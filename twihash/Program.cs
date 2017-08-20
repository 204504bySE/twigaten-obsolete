using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Threading;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Data;
using twitenlib;

namespace twihash
{
    class Program
    {
        static void Main(string[] args)
        {
            Config config = Config.Instance;
            DBHandler db = new DBHandler();
            long NewLastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 900;   //とりあえず15分前

            Stopwatch sw = new Stopwatch();
            Console.WriteLine("{0} Loading hash", DateTime.Now);
            sw.Restart();
            MediaHashArray AllMediaHash = db.AllMediaHash();
            if(AllMediaHash == null) { Console.WriteLine("{0} Hash load failed.", DateTime.Now); Console.ReadKey(); Environment.Exit(1); }
            sw.Stop();
            Console.WriteLine("{0} {1} Hash loaded in {2} ms", DateTime.Now, AllMediaHash.Count, sw.ElapsedMilliseconds);
            Console.WriteLine("{0} {1} New hash", DateTime.Now, AllMediaHash.NewHashes.Count);
            sw.Restart();
            MediaHashSorter media = new MediaHashSorter(AllMediaHash, db, config.hash.MaxHammingDistance,config.hash.ExtraBlocks);
            media.Proceed();
            sw.Stop();
            Console.WriteLine("{0} Multiple Sort, Store: {1}ms", DateTime.Now, sw.ElapsedMilliseconds);
            config.hash.NewLastUpdate(NewLastUpdate);
            Thread.Sleep(5000);
        }
    }

    //とても変なクラスになってしまっためう
    public class MediaHashArray
    {
        public readonly long[] Hashes;
        public readonly HashSet<long> NewHashes;
        public MediaHashArray(int Length)
        {
            Hashes = new long[Length];
            NewHashes = new HashSet<long>();
            ForceInsert = Config.Instance.hash.LastUpdate <= 0;
            if (Config.Instance.hash.KeepDataRAM) { AutoReadAll(); }
        }
        public int Count = 0;  //実際に使ってる個数
        public readonly bool ForceInsert;

        public bool NeedInsert(int Index)
        {
            return ForceInsert || NewHashes.Contains(Hashes[Index]);
        }

        void AutoReadAll()
        {
            Task.Run(() => {
                while (true)
                {
                    for(int i = 0; i < Hashes.Length; i++) { long a = Hashes[i]; }
                    Thread.Sleep(60000);
                }
            });
        }
    }

    //ハミング距離が一定以下のハッシュ値のペア
    public struct MediaPair
    {
        public readonly long media0;
        public readonly long media1;
        public readonly sbyte hammingdistance;
        public MediaPair(long _media0, long _media1, sbyte _ham)
        {
            media0 = _media0;
            media1 = _media1;
            hammingdistance = _ham;
        }

        //media0,media1順で比較
        public class OrderPri : IComparer<MediaPair>
        {
            public int Compare(MediaPair a, MediaPair b)
            {
                if (a.media0 < b.media0) { return -1; }
                else if (a.media0 > b.media0) { return 1; }
                else if (a.media1 < b.media1) { return -1; }
                else if (a.media1 > b.media1) { return 1; }
                else { return 0; }
            }
        }
        //media1,media0順で比較
        public class OrderSub : IComparer<MediaPair>
        {
            public int Compare(MediaPair a, MediaPair b)
            {
                if (a.media1 < b.media1) { return -1; }
                else if (a.media1 > b.media1) { return 1; }
                else if (a.media0 < b.media0) { return -1; }
                else if (a.media0 > b.media0) { return 1; }
                else { return 0; }
            }
        }
    }

    //複合ソート法による全ペア類似度検索 とかいうやつ
    //http://d.hatena.ne.jp/tb_yasu/20091107/1257593519
    class MediaHashSorter
    {
        MediaHashArray media;
        DBHandler db;
        int maxhammingdistance;
        int extrablock;
        Combinations combi;        
        public MediaHashSorter(MediaHashArray media, DBHandler db, int maxhammingdistance, int extrablock)
        {
            this.media = media;
            this.maxhammingdistance = maxhammingdistance;
            this.extrablock = extrablock;
            this.db = db;
            combi = new Combinations(maxhammingdistance + extrablock, extrablock);
        }

        public void Proceed()
        {
            Stopwatch sw = new Stopwatch();
            for (int i = 0; i < combi.Length; i++)
            {
                sw.Restart();
                (int db, int sort) = MultipleSortUnit(media, combi, combi[i]);
                sw.Stop();
                Console.WriteLine("{0} {1}\t{2} / {3}\t{4}\t{5}ms ", DateTime.Now, i, db, sort, combi.CombiString(i), sw.ElapsedMilliseconds);
            }
            //Console.WriteLine("{0} Pairs found", similarmedia.Count);
        }

        const int bitcount = 64;    //longのbit数
        (int db, int sort) MultipleSortUnit(MediaHashArray basemedia, Combinations combi, int[] baseblocks)
        {
            int startblock = baseblocks.Last();
            long fullmask = UnMask(baseblocks, combi.Count);
            QuickSortAll(fullmask, basemedia);

            int ret = 0;
            int dbcount = 0;

            BatchBlock<MediaPair> PairBatchBlock = new BatchBlock<MediaPair>(DBHandler.StoreMediaPairsUnit);
            ActionBlock<MediaPair[]> PairStoreBlock = new ActionBlock<MediaPair[]>(
                (MediaPair[] p) => { Interlocked.Add(ref dbcount, db.StoreMediaPairs(p)); },
                new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });
            PairBatchBlock.LinkTo(PairStoreBlock, new DataflowLinkOptions() { PropagateCompletion = true });

            Parallel.For(0, basemedia.Count,
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                (int i) =>
              {
                  long maskedhash_i = basemedia.Hashes[i] & fullmask;
                  bool NeedInsert_i = basemedia.NeedInsert(i);
                  for (int j = i + 1; j < basemedia.Count; j++)
                  {
                      if (maskedhash_i != (basemedia.Hashes[j] & fullmask)) { break; }
                      if (!NeedInsert_i && !basemedia.NeedInsert(j)) { continue; }
                      //ブロックソートで一致した組のハミング距離を測る
                      int ham = HammingDistance((ulong)basemedia.Hashes[i], (ulong)basemedia.Hashes[j]);
                      if (ham <= maxhammingdistance)
                      {
                          //一致したペアが見つかる最初の組合せを調べる
                          int matchblockindex = 0;
                          int x;
                          for (x = 0; x < startblock && matchblockindex < baseblocks.Length; x++)
                          {
                              if (baseblocks.Contains(x))
                              {
                                  if (x < baseblocks[matchblockindex]) { break; }
                                  matchblockindex++;
                              }
                              else
                              {
                                  long blockmask = UnMask(x, combi.Count);
                                  if ((basemedia.Hashes[i] & blockmask) == (basemedia.Hashes[j] & blockmask))
                                  {
                                      if (x < baseblocks[matchblockindex]) { break; }
                                      matchblockindex++;
                                  }
                              }
                          }
                          //最初の組合せだったときだけ入れる
                          if (x == startblock)
                          {
                              Interlocked.Increment(ref ret);
                              PairBatchBlock.Post(new MediaPair(basemedia.Hashes[i], basemedia.Hashes[j], (sbyte)ham));
                          }
                      }
                  }
              });
            //余りをDBに入れる
            PairBatchBlock.Complete();
            PairStoreBlock.Completion.Wait();
            return (dbcount, ret);
        }

        long UnMask(int block, int blockcount)
        {
            return UnMask(new int[] { block }, blockcount);
        }

        long UnMask(int[] blocks, int blockcount)
        {
            long ret = 0;
            foreach (int b in blocks)
            {
                for (int i = bitcount * b / blockcount; i < bitcount * (b + 1) / blockcount && i < bitcount; i++)
                {
                    ret |= 1L << i;
                }
            }
            return ret;
        }

        void QuickSortAll(long SortMask, MediaHashArray SortList)
        {
            var QuickSortBlock = new TransformBlock<(int Begin, int End), (int Begin1, int End1, int Begin2, int End2)?>
                (((int Begin, int End) SortRange) => {
                    return QuickSortUnit(SortRange, SortMask, SortList);
                }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });

            QuickSortBlock.Post((0, SortList.Count - 1));
            int ProcessingCount = 1;
            do
            {
                (int Begin1, int End1, int Begin2, int End2)? NextSortRange = QuickSortBlock.Receive();
                if (NextSortRange != null)
                {
                    QuickSortBlock.Post((NextSortRange.Value.Begin1, NextSortRange.Value.End1));
                    QuickSortBlock.Post((NextSortRange.Value.Begin2, NextSortRange.Value.End2));
                    ProcessingCount++;  //↑で1個終わって2個始めたから1個増える
                }
                else { ProcessingCount--; } 
            } while (ProcessingCount > 0);
        }

        (int Begin1, int End1, int Begin2, int End2)? QuickSortUnit((int Begin, int End) SortRange, long SortMask, MediaHashArray SortList)
        {
            if (SortRange.Begin >= SortRange.End) { return null; }
            
            //要素数が少なかったら挿入ソートしたい
            if (SortRange.End - SortRange.Begin <= 16)
            {
                for (int k = SortRange.Begin + 1; k <= SortRange.End; k++)
                {
                    long TempHash = SortList.Hashes[k];
                    long TempMasked = SortList.Hashes[k] & SortMask;
                    if ((SortList.Hashes[k - 1] & SortMask) > TempMasked)
                    {
                        int m = k;
                        do
                        {
                            SortList.Hashes[m] = SortList.Hashes[m - 1];
                            m--;
                        } while (m > SortRange.Begin
                        && (SortList.Hashes[m - 1] & SortMask) > TempMasked);
                        SortList.Hashes[m] = TempHash;
                    }
                }
                return null;
            }
            
            long PivotMasked = new long[] { SortList.Hashes[SortRange.Begin] & SortMask,
                        SortList.Hashes[(SortRange.Begin >> 1) + (SortRange.End >> 1)] & SortMask,
                        SortList.Hashes[SortRange.End] & SortMask }
                .OrderBy((long a) => a).Skip(1).First();
            int i = SortRange.Begin; int j = SortRange.End;
            while (true)
            {
                while ((SortList.Hashes[i] & SortMask) < PivotMasked) { i++; }
                while ((SortList.Hashes[j] & SortMask) > PivotMasked) { j--; }
                if (i >= j) { break; }
                long SwapHash = SortList.Hashes[i];
                SortList.Hashes[i] = SortList.Hashes[j];
                SortList.Hashes[j] = SwapHash;
                i++; j--;
            }
            return (SortRange.Begin, i - 1, j + 1, SortRange.End);
        }
        
        //ハミング距離を計算する
        int HammingDistance(ulong a, ulong b)
        {
            //xorしてpopcnt
            ulong value = a ^ b;

            //http://stackoverflow.com/questions/6097635/checking-cpu-popcount-from-c-sharp
            ulong result = value - ((value >> 1) & 0x5555555555555555UL);
            result = (result & 0x3333333333333333UL) + ((result >> 2) & 0x3333333333333333UL);
            return (int)(unchecked(((result + (result >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56);
        }
    }
}