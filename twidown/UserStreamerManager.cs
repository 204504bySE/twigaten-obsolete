using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Collections.Generic;
using System.Collections.Concurrent;
using CoreTweet;
using twitenlib;

namespace twidown
{
    class UserStreamerManager
    //UserStreamerの追加, 保持, 削除をこれで行う
    {
        ConcurrentDictionary<long, UserStreamer> Streamers = new ConcurrentDictionary<long, UserStreamer>();   //longはUserID
        HashSet<long> RevokeRetryUserID = new HashSet<long>();

        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;
        static readonly StreamerLocker Locker = StreamerLocker.Instance;

        public UserStreamerManager() { AddAll(); }

        public void AddAll()
        {
            Tokens[] token = db.Selecttoken(DBHandler.SelectTokenMode.StreamerAll);
            Tokens[] tokenRest = db.Selecttoken(DBHandler.SelectTokenMode.RestinStreamer);
            setMaxConnections(false, token.Length);
            Console.WriteLine("{0} App: {1} tokens loaded.", DateTime.Now, token.Length);
            foreach(Tokens t in tokenRest)
            {
                if (Add(t) && Streamers.TryGetValue(t.UserId, out UserStreamer s))
                {
                    s.NeedRestMyTweet = true;
                }
            }
            foreach (Tokens t in token)
            {
                Add(t);
            }
            setMaxConnections(true);
        }

        bool Add(Tokens t)
        {
            if (t == null) { return false; }
            if (Streamers.ContainsKey(t.UserId))
            {
                //Console.WriteLine("{0} {1}: Already running.", DateTime.Now, t.UserId);
                return true;
            }
            else
            {
                //Console.WriteLine("{0} {1}: Assigned.", DateTime.Now, t.UserId);
                UserStreamer s = new UserStreamer(t);
                return Streamers.TryAdd(t.UserId, s);
            }
        }

        public int Count { get { return Streamers.Count; } }
        
        //これを定期的に呼んで再接続やFriendの取得をやらせる
        //StreamerLockerのロック解除もここでやる
        public int ConnectStreamers()
        {
            if (!db.ExistThisPid()) { Environment.Exit(1); }

            int ActiveStreamers = 0;  //再接続が不要だったやつの数
            Locker.ActualUnlockAll();
            Counter.Instance.PrintReset();

            TickCount Tick = new TickCount(0);
            SemaphoreSlim UnlockSemaphore = new SemaphoreSlim(1);
            Parallel.ForEach (Streamers,
                new ParallelOptions { MaxDegreeOfParallelism = config.crawl.ReconnectThreads },
                (KeyValuePair<long, UserStreamer> s) =>
            {
                UserStreamer.NeedRetryResult NeedRetry = s.Value.NeedRetry();
                if (NeedRetry != UserStreamer.NeedRetryResult.None)
                {
                    //必要なときだけVerifyCredentials()する
                    switch (NeedRetry == UserStreamer.NeedRetryResult.Verify
                        ? s.Value.VerifyCredentials() : UserStreamer.TokenStatus.Success)
                    {
                        case UserStreamer.TokenStatus.Success:                            
                            s.Value.RecieveStream();
                            s.Value.RecieveRestTimeline();
                            if (s.Value.NeedRestMyTweet)
                            {
                                s.Value.NeedRestMyTweet = false;
                                s.Value.RestMyTweet();
                                db.StoreRestDonetoken(s.Key);
                            }
                            else { db.StoreRestNeedtoken(s.Key); }
                            break;
                        case UserStreamer.TokenStatus.Locked:
                            s.Value.PostponeRetry();
                            break;
                        case UserStreamer.TokenStatus.Revoked:
                            lock (RevokeRetryUserID)
                            {
                                if (RevokeRetryUserID.Contains(s.Key))
                                {
                                    db.DeleteToken(s.Key);
                                    RevokeRetryUserID.Remove(s.Key);
                                    RemoveStreamer(s.Value);
                                }
                                else { RevokeRetryUserID.Add(s.Key); }
                            }
                            break;
                        case UserStreamer.TokenStatus.Failure:
                            break;  //何もしない
                    }
                }
                else { Interlocked.Increment(ref ActiveStreamers); }

                if (Tick.Elasped >= 60000 && UnlockSemaphore.Wait(0))
                {
                    Tick.Update();
                    Locker.ActualUnlockAll();
                    Counter.Instance.PrintReset();
                    UnlockSemaphore.Release();
                }
            });
            return ActiveStreamers;
        }

        void RemoveStreamer(UserStreamer Streamer)
        //Revokeされた後の処理
        {
            Streamer.Dispose();
            Streamers.TryRemove(Streamer.Token.UserId, out UserStreamer z);  //つまり死んだStreamerは除外される
            setMaxConnections(true);
            Console.WriteLine("{0} {1}: Streamer removed", DateTime.Now, Streamer.Token.UserId);
        }

        private void setMaxConnections(bool Force = false, int basecount = 0)
        {
            int MaxConnections = Math.Max(basecount, Streamers.Count) + config.crawl.DefaultConnections;
            if (Force || ServicePointManager.DefaultConnectionLimit < MaxConnections)
            {
                ServicePointManager.DefaultConnectionLimit = MaxConnections;
            }
        }
    }
}
