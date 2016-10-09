using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using System.Collections.Generic;
using System.Collections.Concurrent;

namespace twidown
{
    //全UserStreamerで共有するもの
    class StreamerLocker
    {
        private static StreamerLocker _Locker = new StreamerLocker();
        //singletonはこれでインスタンスを取得して使う
        public static StreamerLocker Instance { get { return _Locker; } }

        DBHandlerLock db = DBHandlerLock.Instance;

        //storetweet用
        ConcurrentDictionary<long, byte> LockedTweets = new ConcurrentDictionary<long, byte>();
        ConcurrentQueue<long> dbUnlockTweets = new ConcurrentQueue<long>();
        public bool LockTweet(long Id) { return LockedTweets.TryAdd(Id, 0) && db.LockTweet(Id); }
        public void UnlockTweet(long Id) { dbUnlockTweets.Enqueue(Id); }

        //↓はUnlockはActualUnlockAllでやっちゃうからUnlockメソッドはない
        //storeuser用
        ConcurrentDictionary<long, byte> LockedUsers = new ConcurrentDictionary<long, byte>();
        public bool LockUser(long? Id) { return Id != null && LockedUsers.TryAdd((long)Id, 0); }
        //storedelete用
        ConcurrentDictionary<long, byte> LockedDeletes = new ConcurrentDictionary<long, byte>();
        public bool LockDelete(long Id) { return LockedDeletes.TryAdd(Id, 0); }
        //DownloadProfileImage用
        ConcurrentDictionary<long, byte> LockedProfileImages = new ConcurrentDictionary<long, byte>();
        public bool LockProfileImage(long Id) { return LockedProfileImages.TryAdd(Id, 0); }

        List<long> UnlockTweetID = new List<long>();
        //こいつを外から呼ぶと実際にロックが解除される
        public void ActualUnlockAll()
        {
            LockedTweets.Clear();
            LockedUsers.Clear();
            LockedDeletes.Clear();
            LockedProfileImages.Clear();

            //DBからは1周遅れでロック解除する
            db.UnlockTweet(UnlockTweetID);
            UnlockTweetID.Clear();
            long tmp;
            while (dbUnlockTweets.TryDequeue(out tmp)) { UnlockTweetID.Add(tmp); }
        }
    }
}