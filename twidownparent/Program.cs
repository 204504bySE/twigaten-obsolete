using System;
using System.Collections.Generic;
using System.Threading;

using twitenlib;

namespace twidownparent
{
    class Program
    {
        static void Main(string[] args)
        {
            //多重起動防止
            CheckOldProcess.CheckandExit();

            Config config = Config.Instance;
            DBHandler db = new DBHandler();
            ChildProcessHandler ch = new ChildProcessHandler();
            long[] users;
            int usersIndex = 0;
            int ForceNewChild = 0;

            if (config.crawlparent.InitTruncate)
            {
                db.initTruncate();
                users = db.SelectNewToken();
                for (int i = 0; i <= users.Length / config.crawlparent.AccountLimit; i++)
                {
                    int newpid = ch.StartChild();
                    if (newpid < 0) { continue; }    //雑すぎるエラー処理
                    db.Insertpid(newpid);
                }
            } else { db.DeleteDeadpid(); users = db.SelectNewToken(); }


            bool GetMyTweet = false;    //後から追加されたアカウントはstreamer側で自分のツイートを取得させる
            while (true)
            {
                //子プロセスが死んだならその分だけ先に起動する
                for (int i = 0; i < ForceNewChild; i++)
                {
                    int newpid = ch.StartChild();
                    if (newpid < 0) { continue; }    //雑すぎるエラー処理
                    db.Insertpid(newpid);
                }

                if (users.Length > 0)
                {
                    usersIndex = 0;
                    Console.WriteLine("{0} Assigning {1} accounts.", DateTime.Now, users.Length);
                    for (; usersIndex < users.Length; usersIndex++)
                    {
                        int pid = db.SelectBestpid();
                        if (pid < 0)
                        {
                            int newpid = ch.StartChild();
                            if (newpid < 0) { continue; }    //雑すぎるエラー処理
                            pid = newpid;
                            db.Insertpid(pid);
                        }
                        db.Assigntoken(users[usersIndex], pid, GetMyTweet);
                    }
                }
                GetMyTweet = true;
                Thread.Sleep(60000);
                ForceNewChild = db.DeleteDeadpid();
                db.DeleteNotExistpid();
                users = db.SelectNewToken();
            }
        }
    }
}
