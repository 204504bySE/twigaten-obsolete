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
        //DownloadProfileImage用
        ConcurrentDictionary<long, byte> LockedProfileImages = new ConcurrentDictionary<long, byte>();
        public bool LockProfileImage(long Id) { return LockedProfileImages.TryAdd(Id, 0); }

        List<long> UnlockTweetID = new List<long>();
        //こいつを外から呼ぶと実際にロックが解除される
        public void ActualUnlockAll()
        {
            LockedUsers.Clear();
            LockedDeletes.Clear();
            LockedProfileImages.Clear();

            //UnlockTweetID, DBのtweetlockは1周遅れでロック解除する
            db.UnlockTweet(UnlockTweetID);
            foreach(long Id in UnlockTweetID) { byte z; LockedTweets.TryRemove(Id, out z); }
            UnlockTweetID.Clear();
            long tmp;
            while (UnlockTweets.TryDequeue(out tmp)) { UnlockTweetID.Add(tmp); }
        }
    }
}