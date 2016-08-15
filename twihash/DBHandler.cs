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
        public DBHandler() : base("similar", "", Config.Instance.database.Address, 300) { }

        //類似画像のペアをDBに保存
        //トランザクションはBulk Insertごとに切っちゃう
        string BulkCmdFull;
        public int StoreMediaPairs(List<mediapair> mediapairs)
        {
            const int BulkUnit = 1000;
            const string head = @"INSERT IGNORE INTO dcthashpair VALUES";
            int ret = 0;
            //Parallel.For(0, mediapairs.Count / BulkUnit, op, (int i) =>
            for (int i = 0; i < mediapairs.Count / BulkUnit; i++)
            {
                if (BulkCmdFull == null) { BulkCmdFull = BulkCmdStr(BulkUnit * 2, 3, head); }
                using (MySqlCommand cmd = new MySqlCommand(BulkCmdFull))
                {
                    for (int j = 0; j < BulkUnit; j++)
                    {
                        string evn = (j << 1).ToString();
                        string odd = (j << 1 | 1).ToString();
                        int num = i * BulkUnit + j;
                        cmd.Parameters.AddWithValue("@a" + evn, mediapairs[num].media0);
                        cmd.Parameters.AddWithValue("@b" + evn, mediapairs[num].media1);
                        cmd.Parameters.AddWithValue("@c" + evn, mediapairs[num].hammingdistance);
                        cmd.Parameters.AddWithValue("@a" + odd, mediapairs[num].media1);    //逆も入れる
                        cmd.Parameters.AddWithValue("@b" + odd, mediapairs[num].media0);
                        cmd.Parameters.AddWithValue("@c" + odd, mediapairs[num].hammingdistance);
                    }
                    ret += ExecuteNonQuery(cmd, true);
                    //Interlocked.Add(ref ret, r);
                }
            }
            //余りを処理
            int LastOffset = mediapairs.Count / BulkUnit * BulkUnit;
            int LastCount = mediapairs.Count % BulkUnit;
            if (LastCount != 0)
            {
                using (MySqlCommand cmdlast = new MySqlCommand(BulkCmdStr(LastCount * 2, 3, head)))
                {
                    for (int j = 0; j < LastCount; j++)
                    {
                        string evn = (j << 1).ToString();
                        string odd = (j << 1 | 1).ToString();
                        int num = LastOffset + j;
                        cmdlast.Parameters.AddWithValue("@a" + evn, mediapairs[num].media0);
                        cmdlast.Parameters.AddWithValue("@b" + evn, mediapairs[num].media1);
                        cmdlast.Parameters.AddWithValue("@c" + evn, mediapairs[num].hammingdistance);
                        cmdlast.Parameters.AddWithValue("@a" + odd, mediapairs[num].media1);    //逆も入れる
                        cmdlast.Parameters.AddWithValue("@b" + odd, mediapairs[num].media0);
                        cmdlast.Parameters.AddWithValue("@c" + odd, mediapairs[num].hammingdistance);
                    }
                    ret += ExecuteNonQuery(cmdlast, true);
                }
            }
            return ret;
        }

        //全mediaのhashを読み込む
        public mediahasharray allmediahash()
        {
            const int selectunit = 1000; //でかくするとGCが捗らない
            DataTable Table;
            mediahasharray ret = new mediahasharray((int)(config.hash.LastHashCount * config.hash.HashCountFactor));

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
