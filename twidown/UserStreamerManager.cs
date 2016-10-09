using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CoreTweet;
using System.Net;
using System.Text;
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
        StreamerLocker Locker = StreamerLocker.Instance;

        public UserStreamerManager() {  }
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
                Console.WriteLine("{0} {1}: Assigned.", DateTime.Now, t.UserId);
                UserStreamer s = new UserStreamer(t);
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
        public int ConnectStreamers()
        {
            SemaphoreSlim RestSemaphore = new SemaphoreSlim(config.crawl.RestThreads);
            int ActiveStreamers = 0;  //再接続が不要だったやつの数
            foreach(KeyValuePair<long, UserStreamer> s in Streamers)
            {
                if (s.Value.NeedRetry())
                {
                    s.Value.RecieveStream();    //Tokenの有効性確認を待たずに呼んでしまう
                    switch (s.Value.RecieveRestTimeline())
                    {
                        case UserStreamer.TokenStatus.Success:
                            db.StoreRestNeedtoken(s.Key);
                            break;
                        case UserStreamer.TokenStatus.Locked:
                            s.Value.PostponeRetry();
                            break;
                        case UserStreamer.TokenStatus.Revoked:
                            if (s.Value.VerifyCredentials() == UserStreamer.TokenStatus.Revoked)
                            {
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
                            }
                            break;
                        case UserStreamer.TokenStatus.Failure:
                            break;  //何もしない
                    }
                }
                else { ActiveStreamers++; }
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

        private void setMaxConnections(bool Force = false, int? basecount = null)
        {
            int MaxConnections = (basecount ?? Streamers.Count) * config.crawl.ConnectionCountFactor + config.crawl.DefaultConnections;
            if (Force || ServicePointManager.DefaultConnectionLimit < MaxConnections)
            {
                ServicePointManager.DefaultConnectionLimit = MaxConnections;
            }
        }
    }

}
