using System;
using System.Collections.Generic;
using System.Threading;

using System.Data;
using MySql.Data.MySqlClient;
using twitenlib;

namespace twidownparent
{

    class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("crawl", "", Config.Instance.database.Address) { }
        public long[] SelectAlltoken()
        //<summary>
        //全tokenを返す 失敗したらnull
        //</summary>
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand("SELECT user_id FROM token;"))
            {
                Table = SelectTable(cmd);
            }
            long[] ret = new long[Table.Rows.Count];
            for (int i = 0; i < Table.Rows.Count; i++)
            {
                ret[i] = Table.Rows[i].Field<long>(0);
            }
            return ret;
        }

        public long[] SelectNewToken()
        {
            //Newというより割り当てがないToken
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM token
WHERE NOT EXISTS (SELECT * FROM crawlprocess WHERE user_id = token.user_id);"))
            {
                Table = SelectTable(cmd);
            }
            if (Table == null) { return new long[0]; }
            long[] ret = new long[Table.Rows.Count];
            for (int i = 0; i < Table.Rows.Count; i++)
            {
                ret[i] = Table.Rows[i].Field<long>(0);
            }
            return ret;
        }

        public int Assigntoken(long user_id, int pid)
        {
            //Console.WriteLine("{0} Assign: {1} to {2}", DateTime.Now, user_id, pid);
            MySqlCommand cmd = new MySqlCommand(@"INSERT IGNORE INTO crawlprocess VALUES(@user_id, @pid)");
            cmd.Parameters.AddWithValue("@user_id", user_id);
            cmd.Parameters.AddWithValue("@pid", pid);
            return ExecuteNonQuery(cmd);
        }


        public KeyValuePair<int, int> SelectBestpid()
        {
            //(pid, アカウント数)
            //一番空いてる子プロセスってわけ 全滅なら負数を返すからつまり新プロセスが必要

            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT pid, COUNT(user_id) FROM crawlprocess
GROUP BY pid HAVING COUNT(user_id) < @count ORDER BY COUNT(user_id) LIMIT 1;"))
            {
                cmd.Parameters.AddWithValue("@count", config.crawlparent.AccountLimit);
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted, true);
            }
            if (Table == null || Table.Rows.Count < 1) { return new KeyValuePair<int, int>(-1, 0); }
            return new KeyValuePair<int, int>(Table.Rows[0].Field<int>(0), (int)Table.Rows[0].Field<long>(1));
        }

        public int SelectBestCPU()
        {
            if (!config.crawlparent.ChildSingleThread) { return -1; }   //CPUコアを制限しない場合用
            //一番空いてるCPUコア
            int CPUCount = Environment.ProcessorCount;
            int ret = 0;
            long retcount = long.MaxValue;  //retのCPUを使ってるプロセス数
            for (int i = 0; i < CPUCount; i++)
            {
                using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(pid) FROM pid WHERE cpu = @cpu;"))
                {
                    cmd.Parameters.AddWithValue("@cpu", i);
                    long tmpcount = SelectCount(cmd, IsolationLevel.ReadCommitted, true);
                    if (retcount > tmpcount)
                    {
                        ret = i;
                        retcount = tmpcount;
                    }
                }
            }
            return ret;
        }

        public int Insertpid(int pid, int cpu)
        {
            if (config.crawlparent.ChildSingleThread) { Console.WriteLine("{0} New PID {1} on CPU {2}", DateTime.Now, pid, cpu); }
            else { Console.WriteLine("{0} New PID {1}", DateTime.Now, pid); }
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT IGNORE INTO pid VALUES(@pid, @cpu)"))
            {
                cmd.Parameters.AddWithValue("@pid", pid);
                cmd.Parameters.AddWithValue("@cpu", cpu);
                return ExecuteNonQuery(cmd);
            }
        }

        public int[] Selectpid()
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT pid FROM pid;"))
            {
                Table = SelectTable(cmd);
            }
            if (Table == null) { return new int[0]; }
            int[] ret = new int[Table.Rows.Count];
            for (int i = 0; i < Table.Rows.Count; i++)
            {
                ret[i] = Table.Rows[i].Field<int>(0);
            }
            return ret;
        }

        public int DeleteDeadpid()
        {
            List<MySqlCommand> cmdList = new List<MySqlCommand>();
            ChildProcessHandler ch = new ChildProcessHandler();
            int DeadCount = 0;
            foreach (int pid in Selectpid())
            {
                if (!ch.isAlive(pid))
                {
                    DeadCount++;
                    Console.WriteLine("{0} Dead PID: {1}", DateTime.Now, pid);
                    List<MySqlCommand> cmd = new List<MySqlCommand>();
                    cmd.Add(new MySqlCommand(@"DELETE FROM crawlprocess WHERE pid = @pid;"));
                    cmd.Add(new MySqlCommand(@"DELETE FROM tweetlock WHERE pid = @pid;"));
                    cmd.Add(new MySqlCommand(@"DELETE FROM pid WHERE pid = @pid;"));
                    foreach (MySqlCommand c in cmd)
                    {
                        c.Parameters.AddWithValue("@pid", pid);
                    }
                    cmdList.AddRange(cmd);
                }
            }
            if (cmdList.Count > 0) { ExecuteNonQuery(cmdList); }
            return DeadCount;
        }

        public int initTruncate()
        {
            List<MySqlCommand> cmd = new List<MySqlCommand>();
            cmd.Add(new MySqlCommand(@"TRUNCATE TABLE crawlprocess;"));
            cmd.Add(new MySqlCommand(@"TRUNCATE TABLE tweetlock;"));
            cmd.Add(new MySqlCommand(@"TRUNCATE TABLE pid;"));
            return ExecuteNonQuery(cmd);
        }
    }
}
