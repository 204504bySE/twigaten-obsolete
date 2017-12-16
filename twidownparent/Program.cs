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
            ChildProcessHandler ch = new ChildProcessHandler();
            long[] users;
            int usersIndex = 0;
            int ForceNewChild = 0;

            if (config.crawlparent.InitTruncate)
            {
                db.InitTruncate();
                users = db.SelectNewToken();
                for (int i = 0; i <= users.Length / config.crawlparent.AccountLimit; i++)
                {
                    int newpid = ch.StartChild();
                    if (newpid < 0) { continue; }    //雑すぎるエラー処理
                    db.Insertpid(newpid);
                }
            } else { db.DeleteDeadpid(); users = db.SelectNewToken(); }


            bool GetMyTweet = false;    //後から追加されたアカウントはstreamer側で自分のツイートを取得させる


            //Lockerの起動と監視用
            Process LockerProcess = ch.StartLocker();
            Stopwatch LoopWatch = new Stopwatch();
            UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, (config.crawl.LockerUdpPort ^ (Process.GetCurrentProcess().Id & 0x3FFF)))) { DontFragment = true };
            Udp.Client.ReceiveTimeout = 1000;
            Udp.Client.SendTimeout = 1000;

            while (true)
            {
                LoopWatch.Restart();
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

                //ここでプロセス間通信を監視して返事がなかったら再起動する
                while(LoopWatch.ElapsedMilliseconds < 60000)
                {
                    const int MaxRetryCount = 2;
                    int RetryCount;
                    for (RetryCount = 0; RetryCount < MaxRetryCount; RetryCount++)
                    {
                        try
                        {
                            IPEndPoint LockerEndPoint = null;
                            Udp.Send(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 8);
                            Udp.Receive(ref LockerEndPoint);
                            break;
                        }
                        catch { }
                    }
                    if(RetryCount >= MaxRetryCount)
                    {
                        if (LockerProcess?.HasExited == false) { LockerProcess.Kill(); }
                        Thread.Sleep(500);  //てきとー
                        LockerProcess = ch.StartLocker();
                    }
                    Thread.Sleep(1000);
                }
                LoopWatch.Stop();
                
                //MySQLが落ちた時はクローラーを必要数新しく起動する それ以外は死んだ分だけ
                if (db.Selectpid()?.Length == 0) { ForceNewChild = users.Length / config.crawlparent.AccountLimit + 1; } 
                else { ForceNewChild = db.DeleteDeadpid(); }

                users = db.SelectNewToken();
            }
        }
    }
}
