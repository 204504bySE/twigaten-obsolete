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

        public int Assigntoken(long user_id, int pid, bool RestMyTweet)
        {
            //Console.WriteLine("{0} Assign: {1} to {2}", DateTime.Now, user_id, pid);
            MySqlCommand cmd = new MySqlCommand(@"INSERT IGNORE INTO crawlprocess (user_id, pid, rest_needed) VALUES(@user_id, @pid, @rest_needed)");
            cmd.Parameters.AddWithValue("@user_id", user_id);
            cmd.Parameters.AddWithValue("@pid", pid);
            cmd.Parameters.AddWithValue("@rest_needed", RestMyTweet ? 2 : 0);
            return ExecuteNonQuery(cmd);
        }


        public int SelectBestpid()
        {
            //一番空いてる子プロセスってわけ 全滅なら負数を返すからつまり新プロセスが必要

            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT pid.pid, c FROM
pid LEFT JOIN (SELECT pid, COUNT(user_id) as c FROM crawlprocess
GROUP BY pid HAVING COUNT(user_id)) cp ON pid.pid = cp.pid
ORDER BY c LIMIT 1;"))
            {
                cmd.Parameters.AddWithValue("@count", config.crawlparent.AccountLimit);
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
            }
            if (Table == null || Table.Rows.Count < 1
                || (Table.Rows[0].Field<long?>(1) ?? 0) > config.crawlparent.AccountLimit) { return -1; }
            return Table.Rows[0].Field<int>(0);
        }

        public int Insertpid(int pid)
        {
            Console.WriteLine("{0} New PID {1}", DateTime.Now, pid); 
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT IGNORE INTO pid VALUES(@pid)"))
            {
                cmd.Parameters.AddWithValue("@pid", pid);
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

        public int DeleteNotExistpid()
        {
            //MySQLが落ちた後しばらくの間残る古いクローラーが作っちゃうやつ
            return ExecuteNonQuery(new MySqlCommand(@"DELETE FROM tweetlock WHERE pid NOT IN(SELECT pid FROM pid);"));
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
