using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections.Concurrent;


namespace twidown
{
    //全UserStreamerで共有するもの
    public class StreamerLocker
    {
        private static StreamerLocker _locker = new StreamerLocker();
        //singletonはこれでインスタンスを取得して使う
        public static StreamerLocker Instance
        {
            get { return _locker; }
        }
        private StreamerLocker() { }    //new()させないだけ

        DBHandler db = DBHandler.Instance;

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
            if(toDelete.Length == 0) { return; }
            
            int DeletedCount = db.StoreDelete(toDelete, out List<long> Deleted);
            foreach (long d in Deleted)
            {
                LockedDeletes.TryRemove(d, out byte z);
            }
            Console.WriteLine("{0} App: {1} / {2} Tweets Removed", DateTime.Now, DeletedCount, toDelete.Length); 
        }

        //DownloadProfileImage用
        ConcurrentDictionary<long, byte> LockedProfileImages = new ConcurrentDictionary<long, byte>();
        public bool LockProfileImage(long Id) { return LockedProfileImages.TryAdd(Id, 0); }

        List<long> UnlockTweetID = new List<long>();

        //これを外から呼び出してロックを解除する
        public void ActualUnlockAll()
        {
            LockedUsers.Clear();
            LockedProfileImages.Clear();
            UnlockDelete();

            //UnlockTweetID, DBのtweetlockは1周遅れでロック解除する
            if (db.UnlockTweet(UnlockTweetID) > 0)
            {
                foreach (long Id in UnlockTweetID) { LockedTweets.TryRemove(Id, out byte z); }
                UnlockTweetID.Clear();
            }
            while (UnlockTweets.TryDequeue(out long tmp)) { UnlockTweetID.Add(tmp); }
        }
    }
}
