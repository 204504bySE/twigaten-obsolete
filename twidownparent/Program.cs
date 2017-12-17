using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
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

            LockerHandler.CheckAndStart();

            bool GetMyTweet = false;    //後から追加されたアカウントはstreamer側で自分のツイートを取得させる
            Stopwatch LoopWatch = new Stopwatch();           
            while (true)
            {
                LoopWatch.Restart();

                db.DeleteDeadpid();
                long[] users = db.SelectNewToken();
                int NeedProcessCount = (int)(db.CountToken() / config.crawlparent.AccountLimit + 1);
                int CurrentProcessCount = (int)db.CountPid();

                if (users.Length > 0)
                {
                    if (NeedProcessCount > 0 && CurrentProcessCount >= 0)
                    {
                        //アカウント数からして必要な個数のtwidownを起動する
                        for (int i = 0; i < NeedProcessCount - CurrentProcessCount; i++)
                        {
                            int newpid = ChildProcessHandler.Start();
                            if (newpid < 0) { continue; }    //雑すぎるエラー処理
                            db.Insertpid(newpid);
                        }
                    }

                    int usersIndex = 0;
                    Console.WriteLine("{0} Assigning {1} accounts.", DateTime.Now, users.Length);
                    for (; usersIndex < users.Length; usersIndex++)
                    {
                        int pid = db.SelectBestpid();
                        if (pid < 0)
                        {
                            int newpid = ChildProcessHandler.Start();
                            if (newpid < 0) { continue; }    //雑すぎるエラー処理
                            pid = newpid;
                            db.Insertpid(pid);
                        }
                        db.Assigntoken(users[usersIndex], pid, GetMyTweet);
                    }
                }
                GetMyTweet = true;

                //ここでプロセス間通信を監視して返事がなかったら再起動する
                while(LoopWatch.ElapsedMilliseconds < 60000)
                {
                    LockerHandler.CheckAndStart();
                    Thread.Sleep(1000);
                }
                LoopWatch.Stop();
            }
        }
    }
}
