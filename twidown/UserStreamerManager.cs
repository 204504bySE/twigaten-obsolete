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

        public UserStreamerManager() { }
        public UserStreamerManager(Tokens t) { Add(t); }

        public UserStreamerManager(Tokens[] t, ref TickCount watcher) { AddAll(t, ref watcher); }
        public void AddAll(Tokens[] t, ref TickCount watcher)
        {
            watcher.update();
            setMaxConnections(false, t.Length);
            bool RestConnected = config.crawl.RestConnected || Streamers.Count != 0;
            Console.WriteLine("{0} App: {1} tokens loaded.", DateTime.Now, t.Length);
            foreach (Tokens tt in t)
            {
                Add(tt, RestConnected);
                watcher.update();
            }
            setMaxConnections(true);
        }

        bool Add(Tokens t, bool RestConnected = true)
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
                if (Streamers.TryAdd(t.UserId, s))
                {
                    try
                    {
                        if (s.Connect() == UserStreamer.ConnectResult.Success)
                        {
                            s.RecieveRestTimeline();
                            s.RecieveStream();
                            if (RestConnected)
                            {
                                s.RestMyTweet();
                                s.RestBlock();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("{0} App:\n{1}", DateTime.Now, e);
                    }
                    return true;
                }
                return false;
            }
        }

        public int Count()
        {
            return Streamers.Count;
        }

        //これを定期的に呼んで再接続やFriendの取得をやらせる
        public int ConnectStreamers(ref TickCount LastProcessTick)
        {
            LastProcessTick.update();
            int ActiveStreams = 0;  //再接続が不要だったやつの数
            foreach (KeyValuePair<long, UserStreamer> s in Streamers)
            {
                if (s.Value.NeedRetry())
                {
                    switch (s.Value.Connect())
                    {
                        case UserStreamer.ConnectResult.Success:
                            s.Value.RecieveStream();
                            s.Value.RecieveRestTimeline();
                            s.Value.RestMyTweet();
                            s.Value.RestBlock();
                            break;
                        case UserStreamer.ConnectResult.Revoked:
                            lock (RevokeRetryUserID)
                            {
                                if (RevokeRetryUserID.Contains(s.Key))
                                {
                                    db.DeleteToken(s.Key);
                                    RevokeRetryUserID.Remove(s.Key);
                                    UserStreamer_Finish(s.Value);
                                }
                                else
                                {
                                    RevokeRetryUserID.Add(s.Key);
                                }
                            }
                            break;
                    }
                }
                else { ActiveStreams++; }
                LastProcessTick.update();
            }
            return ActiveStreams;
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
