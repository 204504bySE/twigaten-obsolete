using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Collections.Generic;
using System.Collections.Concurrent;
using CoreTweet;
using twitenlib;
using System.Threading.Tasks.Dataflow;

namespace twidown
{
    class UserStreamerManager
    //UserStreamerの追加, 保持, 削除をこれで行う
    {
        readonly ConcurrentDictionary<long, UserStreamer> Streamers = new ConcurrentDictionary<long, UserStreamer>();   //longはUserID
        readonly HashSet<long> RevokeRetryUserID = new HashSet<long>();

        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public UserStreamerManager()
        {
            AddAll();
        }

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

        readonly ConcurrentDictionary<long, byte> ConnectWaiting = new ConcurrentDictionary<long, byte>();
        ActionBlock<(long, UserStreamer, UserStreamer.NeedRetryResult)> ConnectBlock;
        void InitConnectBlock()
        {
            ConnectBlock = new ActionBlock<(long Id, UserStreamer Streamer, UserStreamer.NeedRetryResult NeedRetry)>(
            (s) =>
            {
                //必要なときだけVerifyCredentials()する
                switch (s.NeedRetry == UserStreamer.NeedRetryResult.Verify
                    ? s.Streamer.VerifyCredentials() : UserStreamer.TokenStatus.Success)
                {
                    case UserStreamer.TokenStatus.Success:
                        s.Streamer.RecieveStream();
                        s.Streamer.RecieveRestTimeline();
                        if (s.Streamer.NeedRestMyTweet)
                        {
                            s.Streamer.NeedRestMyTweet = false;
                            s.Streamer.RestMyTweet();
                            db.StoreRestDonetoken(s.Id);
                        }
                        else { db.StoreRestNeedtoken(s.Id); }
                        break;
                    case UserStreamer.TokenStatus.Locked:
                        s.Streamer.PostponeRetry();
                        break;
                    case UserStreamer.TokenStatus.Revoked:
                        lock (RevokeRetryUserID)
                        {
                            if (RevokeRetryUserID.Contains(s.Id))
                            {
                                db.DeleteToken(s.Id);
                                RevokeRetryUserID.Remove(s.Id);
                                RemoveStreamer(s.Streamer);
                            }
                            else { RevokeRetryUserID.Add(s.Id); }
                        }
                        break;
                    case UserStreamer.TokenStatus.Failure:
                        break;  //何もしない
                }
                ConnectWaiting.TryRemove(s.Id, out byte gomi);
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.ReconnectThreads,
                MaxMessagesPerTask = 1
            });
        }

        //これを定期的に呼んで再接続やFriendの取得をやらせる
        //StreamerLockerのロック解除もここでやる
        public int ConnectStreamers()
        {
            if (!db.ExistThisPid()) { Environment.Exit(1); }

            int ActiveStreamers = 0;  //再接続が不要だったやつの数
            UserStreamer.StreamerLocker.Unlock();
            UserStreamer.Counter.PrintReset();

            TickCount Tick = new TickCount(0);
            foreach (KeyValuePair<long, UserStreamer> s in Streamers.ToArray())  //ここでスナップショットを作る
            {
                UserStreamer.NeedRetryResult NeedRetry = s.Value.NeedRetry();
                if (NeedRetry != UserStreamer.NeedRetryResult.None)
                {
                    if (ConnectWaiting.TryAdd(s.Key, 0))
                    {
                        if(ConnectBlock == null) { InitConnectBlock(); }
                        ConnectBlock.Post((s.Key, s.Value, NeedRetry));
                    }
                }
                else { Interlocked.Increment(ref ActiveStreamers); }
                if (Tick.Elasped >= 60000)
                {
                    Tick.Update();
                    UserStreamer.StreamerLocker.Unlock();
                    UserStreamer.Counter.PrintReset();
                }
            }
            if (ConnectBlock != null)
            {
                if (ConnectBlock.InputCount > 0) { Console.WriteLine("{0} App: {1} Accounts waiting connect", DateTime.Now, ConnectBlock.InputCount); }
                else { ConnectBlock.Complete(); ConnectBlock.Completion.Wait(); ConnectBlock = null; }
            }
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
