using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections.Concurrent;


namespace twidown
{
    //全UserStreamerで共有するもの
    public class StreamerLocker
    {
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
            int DeletedCountTotal = 0;
            const int BulkUnit = 1000;
            for(int i = 0; i <= toDelete.Length / BulkUnit; i++)
            {
                long[] toDeleteTemp = toDelete.Skip(i * BulkUnit).Take(BulkUnit).ToArray();
                int DeletedCount = db.StoreDelete(toDeleteTemp);
                if (DeletedCount >= 0)
                {
                    DeletedCountTotal += DeletedCount;
                    foreach (long d in toDeleteTemp)
                    {
                        byte tmp;
                        LockedDeletes.TryRemove(d, out tmp);
                    }
                }
            }

            if (toDelete.Length > 0) { Console.WriteLine("{0} App: {1} / {2} Tweets Removed", DateTime.Now, DeletedCountTotal, toDelete.Length); }
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
                foreach (long Id in UnlockTweetID) { byte z; LockedTweets.TryRemove(Id, out z); }
                UnlockTweetID.Clear();
            }
            long tmp;
            while (UnlockTweets.TryDequeue(out tmp)) { UnlockTweetID.Add(tmp); }

            Counter.Instance.PrintReset();
        }
    }
}
