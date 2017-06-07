using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Data;
using MySql.Data.MySqlClient;
using System.Threading.Tasks;
using twitenlib;

namespace twihash
{
    class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("hash", "", Config.Instance.database.Address, 300)
        {
            StoreMediaPairsStrFull = BulkCmdStr(StoreMediaPairsUnit, 3, StoreMediaPairsHead);
        }

        const string StoreMediaPairsHead = @"INSERT IGNORE INTO dcthashpair VALUES";
        readonly string StoreMediaPairsStrFull;
        public const int StoreMediaPairsUnit = 1000;
        public int StoreMediaPairs(MediaPair[] StorePairs)
        //類似画像のペアをDBに保存
        {
            if (StorePairs.Length > StoreMediaPairsUnit) { throw new ArgumentOutOfRangeException(); }
            else if (StorePairs.Length == 0) { return 0; }
            else if (StorePairs.Length == StoreMediaPairsUnit)
            {
                using (MySqlCommand Cmd = new MySqlCommand(StoreMediaPairsStrFull))
                {
                    for (int i = 0; i < StoreMediaPairsUnit; i++)
                    {
                        string numstr = i.ToString();
                        Cmd.Parameters.Add("@a" + numstr, MySqlDbType.Int64);
                        Cmd.Parameters.Add("@b" + numstr, MySqlDbType.Int64);
                        Cmd.Parameters.Add("@c" + numstr, MySqlDbType.Byte);
                    }
                    return StoreMediaPairsInner(Cmd, StorePairs);
                }
            }
            else
            {
                using (MySqlCommand Cmd = new MySqlCommand(BulkCmdStr(StorePairs.Length, 3, StoreMediaPairsHead)))
                { 
                    for (int i = 0; i < StorePairs.Length; i++)
                    {
                        string numstr = i.ToString();
                        Cmd.Parameters.Add("@a" + numstr, MySqlDbType.Int64);
                        Cmd.Parameters.Add("@b" + numstr, MySqlDbType.Int64);
                        Cmd.Parameters.Add("@c" + numstr, MySqlDbType.Byte);
                    }
                    return StoreMediaPairsInner(Cmd, StorePairs);
                }
            }
        }

        MediaPair.OrderPri OrderPri = new MediaPair.OrderPri();
        MediaPair.OrderSub OrderSub = new MediaPair.OrderSub();
        int StoreMediaPairsInner(MySqlCommand Cmd, MediaPair[] StorePairs)
        {
            Array.Sort(StorePairs, OrderPri);   //deadlock防止
            for (int i = 0; i < StorePairs.Length; i++)
            {
                string numstr = i.ToString();
                Cmd.Parameters["@a" + numstr].Value = StorePairs[i].media0;
                Cmd.Parameters["@b" + numstr].Value = StorePairs[i].media1;
                Cmd.Parameters["@c" + numstr].Value = StorePairs[i].hammingdistance;
            }
            int ret = ExecuteNonQuery(Cmd);

            Array.Sort(StorePairs, OrderSub);   //deadlock防止
            for (int i = 0; i < StorePairs.Length; i++)
            {
                string numstr = i.ToString(); 
                Cmd.Parameters["@a" + numstr].Value = StorePairs[i].media1;   //↑とは逆
                Cmd.Parameters["@b" + numstr].Value = StorePairs[i].media0;
                Cmd.Parameters["@c" + numstr].Value = StorePairs[i].hammingdistance;
            }
            return ret + ExecuteNonQuery(Cmd);
        }


        //全mediaのhashを読み込んだりする
        public MediaHashArray AllMediaHash()
        {
            try
            {
                MediaHashArray ret = new MediaHashArray(config.hash.LastHashCount + config.hash.HashCountOffset);
                if(!ret.ForceInsert) { NewerMediaHash(ret); }
                int HashUnitBits = Math.Min(63, 64 + 11 - (int)Math.Log(config.hash.LastHashCount, 2)); //TableがLarge Heapに載らない程度に調整
                Parallel.For(0, 1 << (64 - HashUnitBits),
                    new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                    (long i) =>
                {
                    DataTable Table;
                    do
                    {
                        using (MySqlCommand Cmd = new MySqlCommand(@"SELECT dcthash
FROM media
WHERE dcthash BETWEEN @begin AND @end
GROUP BY dcthash;"))
                        {
                            Cmd.Parameters.AddWithValue("@lastupdate", config.hash.LastUpdate);
                            Cmd.Parameters.AddWithValue("@begin", i << HashUnitBits);
                            Cmd.Parameters.AddWithValue("@end", unchecked(((i + 1) << HashUnitBits) - 1));
                            Table = SelectTable(Cmd, IsolationLevel.ReadUncommitted);
                        }
                    } while (Table == null);    //大変安易な対応
                    int Cursor;
                    lock (ret)
                    {
                        Cursor = ret.Count;
                        ret.Count += Table.Rows.Count;
                    }
                    foreach (DataRow row in Table.Rows)
                    {
                        ret.Hashes[Cursor] = row.Field<long>(0);
                        Cursor++;
                    }
                });
                config.hash.NewLastHashCount(ret.Count);
                return ret;
            }
            catch(Exception e) { Console.WriteLine(e); return null; }
        }

        //dcthashpairに追加する必要があるハッシュを取得するやつ
        //これが始まった後に追加されたハッシュは無視されるが
        //次回の実行で拾われるから問題ない
        public void NewerMediaHash(MediaHashArray ret)
        {
            const int QueryRangeSeconds = 600;
            Parallel.For(0, Math.Max(0, DateTimeOffset.UtcNow.ToUnixTimeSeconds() - config.hash.LastUpdate) / QueryRangeSeconds + 1,
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                (long i) => 
                {
                    DataTable Table;
                    do
                    {
                        using (MySqlCommand Cmd = new MySqlCommand(@"SELECT dcthash
FROM media_downloaded_at
NATURAL JOIN media
WHERE downloaded_at BETWEEN @begin AND @end;"))
                        {
                            Cmd.Parameters.AddWithValue("@begin", config.hash.LastUpdate + QueryRangeSeconds * i);
                            Cmd.Parameters.AddWithValue("@end", config.hash.LastUpdate + QueryRangeSeconds * (i + 1) - 1);
                            Table = SelectTable(Cmd, IsolationLevel.ReadUncommitted);
                        }
                    } while (Table == null);    //大変安易な対応
                    lock (ret.NewHashes)
                    {
                        foreach (DataRow row in Table.Rows)
                        {
                            ret.NewHashes.Add(row.Field<long>(0));
                        }
                    }
                });
        }
    }
}
