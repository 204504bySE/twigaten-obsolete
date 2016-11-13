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
        public int ConnectStreamers()
        {
            SemaphoreSlim RestSemaphore = new SemaphoreSlim(config.crawl.RestThreads);
            int ActiveStreamers = 0;  //再接続が不要だったやつの数
            foreach(KeyValuePair<long, UserStreamer> s in Streamers)
            {
                bool? NeedRetry = s.Value.NeedRetry();
                if (NeedRetry != false)
                {
                    switch (s.Value.VerifyCredentials())
                    {
                        case UserStreamer.TokenStatus.Success:
                            s.Value.RecieveStream();
                            //TLが遅すぎた時はこっちでTLを取得する
                            if (NeedRetry == null) {
                                s.Value.RecieveRestTimeline();
                                db.StoreRestNeedtoken(s.Key, false);
                            }
                            db.StoreRestNeedtoken(s.Key, true);
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

        //全UserStreamerで共有するもの
        public class StreamerLocker
        {
            DBHandler db = DBHandler.Instance;

            public StreamerLocker() { BeginIntervalProcess(); }

            //storetweet用
            ConcurrentDictionary<long, byte> LockedTweets = new ConcurrentDictionary<long, byte>();
            public bool LockTweet(long Id) { return LockedTweets.TryAdd(Id, 0) && db.LockTweet(Id); }
            ConcurrentQueue<long> UnlockTweets = new ConcurrentQueue<long>();
            public void UnlockTweet(long Id) { UnlockTweets.Enqueue(Id); }

            //↓はUnlockはActualUnlockAllでやっちゃうからUnlockメソッドはない
            //storeuser用
            ConcurrentDictionary<long, byte> LockedUsers = new ConcurrentDictionary<long, byte>();
            public bool LockUser(long? Id) { return Id != null && LockedUsers.TryAdd((long)Id, 0); }
            //storedelete用
            ConcurrentDictionary<long, byte> LockedDeletes = new ConcurrentDictionary<long, byte>();
            public bool LockDelete(long Id) { return LockedDeletes.TryAdd(Id, 0); }

            //ツイ消しはここでDBに投げることにした
            void UnlockDelete()
            {
                long[] toDelete = LockedDeletes.Keys.ToArray(); //スナップショットが作成される
                int DeletedCount = db.StoreDelete(toDelete);
                if (DeletedCount >= 0)
                {
                    foreach (long d in toDelete)
                    {
                        byte tmp;
                        LockedDeletes.TryRemove(d, out tmp);
                    }
                }
                if(toDelete.Length > 0) { Console.WriteLine("{0} App: {1} / {2} Tweets Removed", DateTime.Now, DeletedCount, toDelete.Length); }
            }

            //DownloadProfileImage用
            ConcurrentDictionary<long, byte> LockedProfileImages = new ConcurrentDictionary<long, byte>();
            public bool LockProfileImage(long Id) { return LockedProfileImages.TryAdd(Id, 0); }

            List<long> UnlockTweetID = new List<long>();

            //これでUnlockをまとめて呼ぶ
            void ActualUnlockAll()
            {
                LockedUsers.Clear();
                LockedProfileImages.Clear();
                UnlockDelete();

                //UnlockTweetID, DBのtweetlockは1周遅れでロック解除する
                if (db.UnlockTweet(UnlockTweetID) > 0)
                {
                    foreach (long Id in UnlockTweetID) { byte z; LockedTweets.TryRemove(Id, out z); }
                    UnlockTweetID.Clear();
                }
                long tmp;
                while (UnlockTweets.TryDequeue(out tmp)) { UnlockTweetID.Add(tmp); }
            }

            //StreamerLockerのアンロックはここでやる
            void BeginIntervalProcess() { Task.Factory.StartNew(() => IntervalProcess(), TaskCreationOptions.LongRunning); }
            void IntervalProcess()
            {
                Counter counter = Counter.Instance;
                Thread.CurrentThread.Priority = ThreadPriority.Highest;
                while (true)
                {
                    Thread.Sleep(60000);
                    ActualUnlockAll();
                    counter.PrintReset();
                }
            }
        }

    }
}
