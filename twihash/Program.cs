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
            long NewLastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 600;   //とりあえず10分前

            Console.WriteLine("{0} Loading hash", DateTime.Now);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            MediaHashArray allmediahash =  db.AllMediaHash();
            if(allmediahash == null) { Console.WriteLine("{0} Hash load failed.", DateTime.Now); Thread.Sleep(5000); Environment.Exit(1); }
            sw.Stop();
            Console.WriteLine("{0} {1} hash loaded", DateTime.Now, allmediahash.Count);
            Console.WriteLine("{0} Hash load: {1}ms", DateTime.Now, sw.ElapsedMilliseconds);
            sw.Restart();
            mediahashsorter media = new mediahashsorter(allmediahash, db, config.hash.MaxHammingDistance,config.hash.ExtraBlocks);
            media.Proceed();
            sw.Stop();
            Console.WriteLine("{0} Multiple Sort, Store: {1}ms", DateTime.Now, sw.ElapsedMilliseconds);
            config.hash.NewLastUpdate(NewLastUpdate);
            Thread.Sleep(5000);
        }
    }

    //メモリ使用量を減らすための悪あがき
    public class MediaHashArray
    {
        public long[] Hashes;
        public bool[] NeedstoInsert;
        public MediaHashArray(int Length)
        {
            Hashes = new long[Length];
            NeedstoInsert = new bool[Length];
            AutoReadAll();
        }
        public int Length { get { return Hashes.Length; } }
        public int Count = 0;  //実際に使ってる個数
        public bool EnableAutoRead = true;

        public void AutoReadAll()
        //配列を読み捨てて物理メモリに保持する(つもり
        {
            Task.Run(() =>
            {
                Thread.CurrentThread.Priority = ThreadPriority.Lowest;
                while (true)
                {
                    for (int i = 0; EnableAutoRead && i < Length; i++)
                    {
                        long a = Hashes[i];
                        bool b = NeedstoInsert[i];
                    }
                    Thread.Sleep(30000);
                }
            });
        }

        public void CopyTo(int SourceIndex, MediaHashArray Destination, int DestIndex, int Count)
        {
            Array.Copy(Hashes, SourceIndex, Destination.Hashes, DestIndex, Count);
            Array.Copy(NeedstoInsert, SourceIndex, Destination.NeedstoInsert, DestIndex, Count);
        }
    }
    /*
    public class mediahashlist
    {
        public List<long> Hashes;
        public List<bool> NeedstoInsert;
        public mediahashlist()
        {
            Hashes = new List<long>();
            NeedstoInsert = new List<bool>();
        }
        public mediahashlist(int capacity)
        {
            Hashes = new List<long>(capacity);
            NeedstoInsert = new List<bool>(capacity);
        }
        public void Add(long hash, bool needtoinsert)
        {
            Hashes.Add(hash);
            NeedstoInsert.Add(needtoinsert);
        }
        public void AddRange(mediahashlist he)
        {
            Hashes.AddRange(he.Hashes);
            NeedstoInsert.AddRange(he.NeedstoInsert);
        }
        public void Clear()
        {
            Hashes.Clear();
            NeedstoInsert.Clear();
        }

        public int Count { get { return Hashes.Count(); } }

        public void TrimExcess()
        {
            NeedstoInsert.TrimExcess();
            Hashes.TrimExcess();
        }

        public mediahasharray ToArrayandFree()
        {
            mediahasharray ret = new mediahasharray();
            ret.NeedstoInsert = NeedstoInsert.ToArray();
            NeedstoInsert = null;
            ret.Hashes = Hashes.ToArray();
            Hashes = null;
            return ret;
        }
    }

    public struct mediahash
    {
        public long hash { get; }
        public bool needtoinsert { get; }
        public mediahash(long _hash, bool _needtoinsert)
        {
            hash = _hash;
            needtoinsert = _needtoinsert;
        }
    }
    */
    //ハミング距離が一定以下のハッシュ値のペア
    public struct MediaPair
    {
        public long media0 { get; }
        public long media1 { get; }
        public sbyte hammingdistance { get; }
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
    class mediahashsorter
    {
        MediaHashArray media;
        DBHandler db;
        int maxhammingdistance;
        int extrablock;
        Combinations combi;
        ConcurrentQueue<MediaPair> SimilarMedia = new ConcurrentQueue<MediaPair>();   //media_idのペアとハミング距離(処理結果)
        
        public mediahashsorter(MediaHashArray media, DBHandler db, int maxhammingdistance, int extrablock)
        {
            this.media = media;
            this.maxhammingdistance = maxhammingdistance;
            this.extrablock = extrablock;
            this.db = db;
            combi = new Combinations(maxhammingdistance + extrablock, extrablock);
        }

        public void Proceed()
        {
            //基数ソートを再利用する版
            /*Parallel.For(0, combi.Count - combi.Select + 1, op, (int i) => {
                int[] firstblocks = new int[1];
                firstblocks[0] = i;
                multiplesort(media, combi, firstblocks);
            });
            */
            //基数ソートを再利用しない版
            Stopwatch sw = new Stopwatch();
            for (int i = 0; i < combi.Length; i++)
            {
                sw.Restart();
                long count = multiplesortlast(media, combi, combi[i]);
                sw.Stop();
                Console.WriteLine("{0} {1}\t{2}\t{3}\t{4}ms ", DateTime.Now, i, count, combi.CombiString(i), sw.ElapsedMilliseconds);
            }
            templist = null;
            media = null;
            //Console.WriteLine("{0} Pairs found", similarmedia.Count);
        }

        const int bitcount = 64;    //longのbit数
        int multiplesortlast(MediaHashArray basemedia, Combinations combi, int[] baseblocks)
        {
            int startblock = baseblocks.Last();
            long fullmask = UnMask(baseblocks, combi.Count);
            /*//基数ソートを再利用する版
            long sortmask = UnMask(startblock, combi.Count);
            sortedmedia = radixsortall(sortmask, ref sortedmedia);
            */
            //基数ソートを再利用しない版
            //sortedmedia = radixsortall(fullmask, sortedmedia);
            /*
            mergesortall(fullmask, ref basemediaa);
            mediahasharray basemedia = basemediaa;
            */
            QuickSortAll(fullmask, basemedia);

            int ret = 0;
            int dbcount = 0;
            int dbthreads = 0;
            int InsertUnit = Math.Max(100, 2000 / Environment.ProcessorCount);

            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = Environment.ProcessorCount;
            Parallel.For(0, basemedia.Count, op, (int i) =>
              {
                  long maskedhash_i = basemedia.Hashes[i] & fullmask;
                  for (int j = i + 1; j < basemedia.Count; j++)
                  {
                      if (maskedhash_i != (basemedia.Hashes[j] & fullmask)) { break; }
                      if (!basemedia.NeedstoInsert[i] && !basemedia.NeedstoInsert[j]) { continue; }
                      //ブロックソートで一致した組のハミング距離を測る
                      sbyte ham = hammingdistance((ulong)basemedia.Hashes[i], (ulong)basemedia.Hashes[j]);
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
                              SimilarMedia.Enqueue(new MediaPair(basemedia.Hashes[i], basemedia.Hashes[j], ham));
                              if (SimilarMedia.Count >= (dbthreads + 1) * InsertUnit)
                              {   //溜まったらDBに入れる
                                  Interlocked.Increment(ref dbthreads);
                                  List<MediaPair> PairstoStore = new List<MediaPair>();
                                  MediaPair outpair;
                                  for (int n = 0; n < InsertUnit; n++)
                                  {
                                      if(!SimilarMedia.TryDequeue(out outpair)) { break; }
                                      PairstoStore.Add(outpair);
                                  }
                                  int c = db.StoreMediaPairs(PairstoStore);
                                  Interlocked.Add(ref dbcount, c);
                                  Interlocked.Decrement(ref dbthreads);
                              }
                              //Console.WriteLine("{0} {1} Rows, {2} Pairs\t{3}, {4}", DateTime.Now, dbcount, ret, i, j);
                          }
                      }
                  }
              });
            dbcount += db.StoreMediaPairs(SimilarMedia.ToList());
            Console.WriteLine("{0} {1} Rows affected", DateTime.Now, dbcount);
            return ret;
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
            SortList.EnableAutoRead = false;
            //Key:ソート開始位置 Value:ソート終了位置
            var QuickSortBlock = new TransformBlock<KeyValuePair<int, int>, KeyValuePair<int, int>[]>
                ((KeyValuePair<int, int> SortRange) => {
                    return QuickSortUnit(SortRange.Key, SortRange.Value, SortMask, SortList);
                }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });

            QuickSortBlock.Post(new KeyValuePair<int, int>(0, SortList.Count - 1));
            int ProcessingCount = 1;
            do
            {
                KeyValuePair<int, int>[] NextSortRange = QuickSortBlock.Receive();
                if (NextSortRange != null)
                {
                    foreach(KeyValuePair<int,int> r in NextSortRange)
                    {
                        QuickSortBlock.Post(r);
                        ProcessingCount++;
                    }
                }
                ProcessingCount--;
            } while (ProcessingCount > 0);
            SortList.EnableAutoRead = true;
        }

        KeyValuePair<int,int>[] QuickSortUnit(int Left, int Right, long SortMask, MediaHashArray SortList)
        {
            if (Left >= Right) { return null; }
            
            //要素数が少なかったら挿入ソートしたい
            if (Right - Left <= 16)
            {
                for (int k = Left + 1; k <= Right; k++)
                {
                    long TempHash = SortList.Hashes[k];
                    long TempMasked = SortList.Hashes[k] & SortMask;
                    bool TempNeedtoInsert = SortList.NeedstoInsert[k];
                    if ((SortList.Hashes[k - 1] & SortMask) > TempMasked)
                    {
                        int m = k;
                        do
                        {
                            SortList.Hashes[m] = SortList.Hashes[m - 1];
                            SortList.NeedstoInsert[m] = SortList.NeedstoInsert[m - 1];
                            m--;
                        } while (m > Left
                        && (SortList.Hashes[m - 1] & SortMask) > TempMasked);
                        SortList.Hashes[m] = TempHash;
                        SortList.NeedstoInsert[m] = TempNeedtoInsert;
                    }
                }
                return null;
            }
            
            long PivotMasked = new long[] { SortList.Hashes[Left] & SortMask,
                        SortList.Hashes[(Left >> 1) + (Right >> 1)] & SortMask,
                        SortList.Hashes[Right] & SortMask }
                .OrderBy((long a) => a).Skip(1).First();
            int i = Left; int j = Right;
            while (true)
            {
                while ((SortList.Hashes[i] & SortMask) < PivotMasked) { i++; }
                while ((SortList.Hashes[j] & SortMask) > PivotMasked) { j--; }
                if (i >= j) { break; }
                long SwapHash = SortList.Hashes[i];
                SortList.Hashes[i] = SortList.Hashes[j];
                SortList.Hashes[j] = SwapHash;
                bool SwapNeedtoInsert = SortList.NeedstoInsert[i];
                SortList.NeedstoInsert[i] = SortList.NeedstoInsert[j];
                SortList.NeedstoInsert[j] = SwapNeedtoInsert;
                i++; j--;
            }
            return new KeyValuePair<int, int>[]
            {
                new KeyValuePair<int, int>(Left, i - 1),
                new KeyValuePair<int, int>(j + 1, Right)
            };
        }

        //baselistをsortmaskで破壊的にソートする
        MediaHashArray templist;
        void mergesortall(long sortmask, ref MediaHashArray baselist)
        {
            if (templist == null) { templist = new MediaHashArray(baselist.Count); }
            MediaHashArray sortlist = baselist;
            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = Environment.ProcessorCount;
            for (int sortunit = 2; sortunit <= sortlist.Count; sortunit <<= 1)
            {
                int i_max = sortlist.Count / sortunit - 1;
                Parallel.For(0, i_max + 1, op, (int i) =>
                 {
                     int sortbase = i * sortunit;
                     int count_a_max;
                     int count_b_max;
                     if (i == i_max)
                     {  //最後のグループはうまいことまとめなきゃいけないのであった
                         if ((sortlist.Count / (sortunit >> 1) & 1) != 0)
                         {
                             count_a_max = count_b_max = (sortunit >> 1) - 1;
                             mergesortunit(sortmask, sortbase, sortlist, templist, count_a_max, count_b_max, true);
                             count_a_max = sortunit - 1;
                             count_b_max = sortlist.Count % sortunit - 1;
                         }
                         else
                         {
                             count_a_max = (sortunit >> 1) - 1;
                             count_b_max = (sortunit >> 1) + sortlist.Count % sortunit - 1;
                         }
                     }
                     else
                     {
                         count_a_max = count_b_max = (sortunit >> 1) - 1;
                     }
                     mergesortunit(sortmask, sortbase, sortlist, templist, count_a_max, count_b_max);
                 });
                MediaHashArray swaplist = sortlist;
                sortlist = templist;
                templist = swaplist;
            }
            baselist = sortlist;
        }

        void mergesortunit(long sortmask, int sortbase, MediaHashArray sortlist, MediaHashArray templist, int count_a_max, int count_b_max, bool copyback = false)
        {
            int count_a = 0;  //  sortlist.* [sortbase + count_a]
            int count_b = 0;  //  sortlist.* [sortbase + count_a_max + 1 + count_b]
            while (count_a <= count_a_max || count_b <= count_b_max)
            {
                if (count_a > count_a_max)
                {
                    sortlist.CopyTo(sortbase + count_a_max + 1 + count_b,
                        templist, sortbase + count_a + count_b,
                        count_b_max - count_b + 1);
                    break;
                }
                else if (count_b > count_b_max)
                {
                    sortlist.CopyTo(sortbase + count_a,
                        templist, sortbase + count_a + count_b,
                        count_a_max - count_a + 1);
                    break;
                }
                else
                {
                    if ((sortlist.Hashes[sortbase + count_a] & sortmask) <= (sortlist.Hashes[sortbase + count_a_max + 1 + count_b] & sortmask))
                    {
                        templist.Hashes[sortbase + count_a + count_b] = sortlist.Hashes[sortbase + count_a];
                        templist.NeedstoInsert[sortbase + count_a + count_b] = sortlist.NeedstoInsert[sortbase + count_a];
                        count_a++;
                    }
                    else
                    {
                        templist.Hashes[sortbase + count_a + count_b] = sortlist.Hashes[sortbase + count_a_max + 1 + count_b];
                        templist.NeedstoInsert[sortbase + count_a + count_b] = sortlist.NeedstoInsert[sortbase + count_a_max + 1 + count_b];
                        count_b++;
                    }
                }
            }
            if (copyback)   //ソートした部分をsortlistに書き戻す
            {
                templist.CopyTo(sortbase, sortlist, sortbase, count_a_max + count_b_max + 2);
            }
        }
        /*
        mediahashlist radixsortall(long sortmask, mediahashlist unsorted)
        {
            mediahashlist sortarray_a = new mediahashlist(unsorted.Count);
            mediahashlist sortarray_b = new mediahashlist(unsorted.Count);
            bool sortab = false;    //unsortedとsortedarray_a に交互にソート済みデータが入る
            for(int i = 0; i< bitcount; i++)
            { 
                if ((1L << i & sortmask) != 0)
                {
                    if (sortab) { sortab = false; radixsort(ref sortarray_a, ref unsorted, ref sortarray_b, i); }
                    else { sortab = true; radixsort(ref unsorted, ref sortarray_a, ref sortarray_b, i); }
                }
            }
            if (sortab) { return sortarray_a; }
            else { return unsorted; }
        }
        //下からbit目について基数ソート
        void radixsort(ref mediahashlist unsorted, ref mediahashlist sorted, ref mediahashlist tmp, int bit)
        {
            ulong mask = 1UL << bit;
            int sortedcursor = 0;
            int tmpcursor = 0;
            for(int i = 0;i < unsorted.Count; i++)
            {
                if(((ulong)unsorted.Hashes[i] & mask) == 0)
                {
                    sorted.Hashes[sortedcursor] = unsorted.Hashes[i];
                    sorted.NeedstoInsert[sortedcursor] = unsorted.NeedstoInsert[i];
                    sortedcursor++;
                }
                else
                {
                    tmp.Hashes[tmpcursor] = unsorted.Hashes[i];
                    tmp.NeedstoInsert[tmpcursor] = unsorted.NeedstoInsert[i];
                    tmpcursor++;
                }
            }
            tmp.CopyTo(0, sorted, sortedcursor, tmpcursor);
        }

                //下からbit目について基数ソート
                List<long> radixsort(List<long> list, int bit)
                {
                    List<long> sorted0 = new List<long>(list.Count);
                    List<long> sorted1 = new List<long>(list.Count);
                    long mask = 1L << bit;
                    list.ForEach((long m) => { if ((m & mask) == 0) { sorted0.Add(m); } else { sorted1.Add(m); } });
                    sorted0.AddRange(sorted1);
                    sorted1 = null;
                    GC.Collect();
                    return sorted0;
                }
        */
        //ハミング距離を計算する
        sbyte hammingdistance(ulong a, ulong b)
        {
            //xorしてpopcnt
            ulong value = a ^ b;

            //http://stackoverflow.com/questions/6097635/checking-cpu-popcount-from-c-sharp
            ulong result = value - ((value >> 1) & 0x5555555555555555UL);
            result = (result & 0x3333333333333333UL) + ((result >> 2) & 0x3333333333333333UL);
            return (sbyte)(unchecked(((result + (result >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56);
        }
    }
}