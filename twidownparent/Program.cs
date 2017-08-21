using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
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

            TweetLockThread();

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

                //MySQLが落ちた時はクローラーを必要数新しく起動する それ以外は死んだ分だけ
                if (db.Selectpid()?.Length == 0) { ForceNewChild = users.Length / config.crawlparent.AccountLimit + 1; } 
                else { ForceNewChild = db.DeleteDeadpid(); }

                users = db.SelectNewToken();
            }
        }

        static HashSet<long> LockedTweets = new HashSet<long>();
        static Queue<long> LockedTweetsQueue = new Queue<long>(Config.Instance.crawlparent.TweetLockSize);
        ///<summary>ツイートIDを受け取ってプロセスを跨いで排他制御する(DBのdeadlock防止)</summary>
        static void TweetLockThread()
        {
            UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, Config.Instance.crawlparent.UdpPort)) { DontFragment = true};
            Task.Run(() =>
            {
                while (true)
                {
                    IPEndPoint CrawlEndPoint = null;
                    long tweet_id = BitConverter.ToInt64(Udp.Receive(ref CrawlEndPoint), 0);
                    if (LockedTweets.Add(tweet_id))
                    {
                        Udp.Send(BitConverter.GetBytes(true), sizeof(bool), CrawlEndPoint); //Lockできたらtrue
                        while (LockedTweetsQueue.Count >= Config.Instance.crawlparent.TweetLockSize)
                        { LockedTweets.Remove(LockedTweetsQueue.Dequeue()); }
                    }
                    else { Udp.Send(BitConverter.GetBytes(false), sizeof(bool), CrawlEndPoint); }//Lockできなかったらfalse
                }
            });
        }
    }
}
