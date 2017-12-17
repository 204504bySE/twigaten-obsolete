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

        //全tokenを返す 失敗したらnull
        public long[] SelectAlltoken()
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

        public long CountToken()
        {
            using(MySqlCommand cmd = new MySqlCommand("SELECT COUNT(user_id) FROM token;"))
            {
                return SelectCount(cmd, IsolationLevel.ReadUncommitted);
            }
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
            cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
            cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = pid;
            cmd.Parameters.Add("@rest_needed", MySqlDbType.Byte).Value = RestMyTweet ? 2 : 0;
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
                cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = pid;
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
            if (Table == null) { return null; }
            int[] ret = new int[Table.Rows.Count];
            for (int i = 0; i < Table.Rows.Count; i++)
            {
                ret[i] = Table.Rows[i].Field<int>(0);
            }
            return ret;
        }

        public long CountPid()
        {
            using (MySqlCommand cmd = new MySqlCommand("SELECT COUNT(pid) FROM pid;"))
            {
                return SelectCount(cmd);
            }
        }

        public int DeleteDeadpid()
        {
            List<MySqlCommand> CmdList = new List<MySqlCommand>();
            int DeadCount = 0;
            int[] pids = Selectpid();
            if(pids == null) { return 0; }
            foreach (int pid in pids)
            {
                if (!ChildProcessHandler.Alive(pid))
                {
                    DeadCount++;
                    Console.WriteLine("{0} Dead PID: {1}", DateTime.Now, pid);
                    MySqlCommand Cmd = new MySqlCommand(@"DELETE FROM pid WHERE pid = @pid;");
                    Cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = pid;
                    CmdList.Add(Cmd);
                }
            }
            if (CmdList.Count > 0) { ExecuteNonQuery(CmdList); }
            return DeadCount;
        }

        public int InitTruncate()
        {
            return ExecuteNonQuery(new MySqlCommand(@"DELETE FROM pid;"));
        }
    }
}
