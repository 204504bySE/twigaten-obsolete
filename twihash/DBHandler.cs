using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Data;
using MySql.Data.MySqlClient;
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
        const int StoreMediaPairsUnit = 500;
        public int StoreMediaPairs(List<MediaPair> StorePairs)
        //類似画像のペアをDBに保存
        {
            int ret = 0;
            MediaPair[] SortPairs;
            MySqlCommand Cmd;
            if (StorePairs.Count >= StoreMediaPairsUnit)
            {
                Cmd = new MySqlCommand(StoreMediaPairsStrFull);
                for (int i = 0; i < StoreMediaPairsUnit; i++)
                {
                    string numstr = i.ToString();
                    Cmd.Parameters.Add("@a" + numstr, MySqlDbType.Int64);
                    Cmd.Parameters.Add("@b" + numstr, MySqlDbType.Int64);
                    Cmd.Parameters.Add("@c" + numstr, MySqlDbType.Byte);
                }
                SortPairs = new MediaPair[StoreMediaPairsUnit];
                for (int i = 0; i < StorePairs.Count / StoreMediaPairsUnit; i++)
                {
                    StorePairs.CopyTo(i * StoreMediaPairsUnit, SortPairs, 0, StoreMediaPairsUnit);
                    ret += StoreMediaPairsInner(Cmd, SortPairs);
                }
            }
            //余りを処理
            int LastCount = StorePairs.Count % StoreMediaPairsUnit;
            if (LastCount > 0)
            {
                int LastOffset = StorePairs.Count / StoreMediaPairsUnit * StoreMediaPairsUnit;
                Cmd = new MySqlCommand(BulkCmdStr(LastCount, 3, StoreMediaPairsHead));
                for (int i = 0; i < LastCount; i++)
                {
                    string numstr = i.ToString();
                    Cmd.Parameters.Add("@a" + numstr, MySqlDbType.Int64);
                    Cmd.Parameters.Add("@b" + numstr, MySqlDbType.Int64);
                    Cmd.Parameters.Add("@c" + numstr, MySqlDbType.Byte);
                }
                SortPairs = new MediaPair[LastCount];
                StorePairs.CopyTo(LastOffset, SortPairs, 0, LastCount);
                ret += StoreMediaPairsInner(Cmd, SortPairs);
            }
            return ret;
        }

        MediaPair.OrderPri OrderPri = new MediaPair.OrderPri();
        MediaPair.OrderSub OrderSub = new MediaPair.OrderSub();
        int StoreMediaPairsInner(MySqlCommand Cmd, MediaPair[] StorePairs)
        {
            Array.Sort(StorePairs, OrderPri);
            for (int i = 0; i < StorePairs.Length; i++)
            {
                string numstr = i.ToString();
                Cmd.Parameters["@a" + numstr].Value = StorePairs[i].media0;
                Cmd.Parameters["@b" + numstr].Value = StorePairs[i].media1;
                Cmd.Parameters["@c" + numstr].Value = StorePairs[i].hammingdistance;
            }
            int ret = ExecuteNonQuery(Cmd, true);

            Array.Sort(StorePairs, OrderSub);
            for (int i = 0; i < StorePairs.Length; i++)
            {
                string numstr = i.ToString();
                Cmd.Parameters["@a" + numstr].Value = StorePairs[i].media1;   //↑とは逆
                Cmd.Parameters["@b" + numstr].Value = StorePairs[i].media0;
                Cmd.Parameters["@c" + numstr].Value = StorePairs[i].hammingdistance;
            }
            return ret + ExecuteNonQuery(Cmd, true);
        }


        //全mediaのhashを読み込む
        public MediaHashArray AllMediaHash()
        {
            const int selectunit = 1000; //でかくするとGCが捗らない
            DataTable Table;
            MediaHashArray ret = new MediaHashArray(config.hash.LastHashCount + config.hash.HashCountOffset);

            using (MySqlCommand firstcmd = new MySqlCommand(@"SELECT
dcthash, MAX(downloaded_at) >= @lastupdate IS TRUE
FROM media GROUP BY dcthash ORDER BY dcthash LIMIT @selectunit;"))
            {
                firstcmd.Parameters.AddWithValue("@lastupdate", config.hash.LastUpdate);
                firstcmd.Parameters.AddWithValue("@selectunit", selectunit);
                Table = SelectTable(firstcmd, IsolationLevel.ReadUncommitted);
            }
            if (Table == null || Table.Rows.Count < 1) { return null; }

            MySqlCommand cmd = new MySqlCommand(@"SELECT
dcthash, MAX(downloaded_at) >= @lastupdate IS TRUE
FROM media WHERE dcthash > @lasthash
GROUP BY dcthash ORDER BY dcthash LIMIT @selectunit;");
            cmd.Parameters.Add("@lastupdate", MySqlDbType.Int64);
            cmd.Parameters.Add("@lasthash", MySqlDbType.Int64);
            cmd.Parameters.AddWithValue("@selectunit", selectunit);

            int i = 0;
            bool ForceInsert = config.hash.LastUpdate <= 0;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (Table.Rows.Count > 0)
            {
                foreach (DataRow row in Table.Rows)
                {
                    //Console.WriteLine("{0} {1}", (long)row[0], (long)row[1]);
                    ret.Hashes[i] = row.Field<long>(0);
                    ret.NeedstoInsert[i] = ForceInsert || row.Field<long>(1) == 1;
                    i++;
                    if (i >= ret.Length) { return null; }
                }
                long LastHash = Table.Rows[Table.Rows.Count - 1].Field<long>(0);
                cmd.Parameters["@lastupdate"].Value = config.hash.LastUpdate;
                cmd.Parameters["@lasthash"].Value = LastHash;
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
                if (Table == null) { return null; }
            }
            config.hash.NewLastHashCount(i);
            ret.Count = i;
            return ret;
        }
    }

}
