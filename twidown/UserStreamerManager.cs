using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CoreTweet;
using System.Net;
using System.Collections.Generic;
using System.Collections.Concurrent;
using twitenlib;

namespace twidown
{
    class UserStreamerManager
    //<summary>
    //UserStreamerの追加, 保持, 削除をこれで行う
    //</summary>
    {
        Config config = Config.Instance;
        ConcurrentDictionary<long, UserStreamer> Streamers = new ConcurrentDictionary<long, UserStreamer>();   //longはUserID
        HashSet<long> RevokeRetryUserID = new HashSet<long>();
        DBHandler db = DBHandler.Instance;
        StreamerLocker Locker = new StreamerLocker();

        public UserStreamerManager() { }
        public UserStreamerManager(Tokens t) : this() { Add(t); }
        public UserStreamerManager(Tokens[] t) : this() { AddAll(t); }

        public void AddAll(Tokens[] t)
        {
            setMaxConnections(false, t.Length);
            Console.WriteLine("{0} App: {1} tokens loaded.", DateTime.Now, t.Length);
            foreach (Tokens tt in t)
            {
                Add(tt);
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
                UserStreamer s = new UserStreamer(t, Locker);
                return Streamers.TryAdd(t.UserId, s);
            }
        }

        public int Count
        {
            get
            {
                return Streamers.Count;
            }
        }

        //これを定期的に呼んで再接続やFriendの取得をやらせる
        //StreamerLockerのロック解除もここでやる
        public int ConnectStreamers()
        {
            SemaphoreSlim RestSemaphore = new SemaphoreSlim(config.crawl.RestThreads);
            int ActiveStreamers = 0;  //再接続が不要だったやつの数
            Locker.ActualUnlockAll();
            TickCount Tick = new TickCount(0);

            foreach (KeyValuePair<long, UserStreamer> s in Streamers)
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
                            //TLが遅すぎた時はこっちでTLを取得する
                            if (NeedRetry == UserStreamer.NeedRetryResult.GetTimeline)
                            {
                                s.Value.RecieveRestTimeline();
                                db.StoreRestNeedtoken(s.Key, false);
                            }
                            else { db.StoreRestNeedtoken(s.Key, true); }
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
                                    UserStreamer_Finish(s.Value);
                                }
                                else { RevokeRetryUserID.Add(s.Key); }
                            }
                            break;
                        case UserStreamer.TokenStatus.Failure:
                            break;  //何もしない
                    }
                }
                else { ActiveStreamers++; }

                if(Tick.Elasped >= 60000) { Locker.ActualUnlockAll(); Tick.Update(); }
            }
            return ActiveStreamers;
        }

        void UserStreamer_Finish(UserStreamer set)
        //<summary>
        //UserStreamerがRevokeを検出した時の処理
        //</summary>
        {
            UserStreamer a;  //out用 捨てるだけ
            Streamers.TryRemove(set.Token.UserId, out a);  //つまり死んだStreamerは除外される
            setMaxConnections(true);
            Console.WriteLine("{0} {1}: Streamer removed", DateTime.Now, set.Token.UserId);
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
