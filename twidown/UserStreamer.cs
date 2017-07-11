using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Reactive.Linq;
using System.Reactive.Concurrency;
using CoreTweet;
using CoreTweet.Streaming;
using twitenlib;
using System.Threading.Tasks.Dataflow;
using System.Collections.Concurrent;
using System.Threading;

namespace twidown
{
    class UserStreamer : IDisposable
    {
        // 各TokenのUserstreamを受信したり仕分けたりする

        public Exception e { get; private set; }
        public Tokens Token { get; }
        public bool NeedRestMyTweet { get; set; }   //次のconnect時にRESTでツイートを取得する
        public bool ConnectWaiting { get; set; }    //UserStreamerManager.ConnectBlockに入っているかどうか
        IDisposable StreamSubscriber;
        DateTimeOffset LastStreamingMessageTime = DateTimeOffset.Now;
        readonly TweetTimeList TweetTime = new TweetTimeList();
        bool isAttemptingConnect = false;

        //Singleton
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public UserStreamer(Tokens t)
        {
            Token = t;
            Token.ConnectionOptions.DisableKeepAlive = false;
            Token.ConnectionOptions.UseCompression = true;
            Token.ConnectionOptions.UseCompressionOnStreaming = true;
        }

        public void Dispose()
        {
            StreamSubscriber?.Dispose();
            StreamSubscriber = null;
            e = null;
        }

        ~UserStreamer()
        {
            Dispose();
        }

        //最近受信したツイートの時刻を一定数保持する
        //Userstreamの場合は実際に受信した時刻を使う
        class TweetTimeList
        {
            readonly SortedSet<DateTimeOffset> TweetTime = new SortedSet<DateTimeOffset>();
            static readonly Config config = Config.Instance;
            public void Add(DateTimeOffset Time)
            {
                lock (TweetTime)
                {
                    TweetTime.Add(Time);
                    while (TweetTime.Count > config.crawl.UserStreamTimeoutTweets)
                    {
                        TweetTime.Remove(TweetTime.Min);
                    }
                }
            }
            //config.crawl.UserStreamTimeoutTweets個前のツイートの時刻を返すってわけ
            public DateTimeOffset Min
            {
                get
                {
                    lock (TweetTime)
                    {
                        if (TweetTime.Count > 0) { return TweetTime.Min; }
                        else { return DateTimeOffset.Now; }
                    }
                }
            }
        }

        DateTimeOffset? PostponedTime;    //ロックされたアカウントが再試行する時刻
        public void PostponeRetry() { PostponedTime = DateTimeOffset.Now.AddSeconds(config.crawl.LockedTokenPostpone); }
        public void PostponeRetry(int Seconds) { PostponedTime = DateTimeOffset.Now.AddSeconds(Seconds); }
        bool isPostponed() {
            if (PostponedTime == null) { return false; }
            else if (DateTimeOffset.Now > PostponedTime.Value) { return true; }
            else { PostponedTime = null; return false; }
        }

        public enum NeedRetryResult
        {
            None,         //不要(Postponedもこれ)
            JustNeeded,       //必要だけど↓の各処理は不要
            Verify       //VerifyCredentialsが必要
        }

        //これを外部から叩いて再接続の必要性を確認
        public NeedRetryResult NeedRetry()
        {
            if (isPostponed()) { return NeedRetryResult.None; }
            else if (e != null)
            {
                if ((e is TwitterException ex) && ex.Status == HttpStatusCode.Unauthorized)
                { return NeedRetryResult.Verify; }
                else { return NeedRetryResult.JustNeeded; }
            }
            else if (!isAttemptingConnect && StreamSubscriber == null) { return NeedRetryResult.JustNeeded; }
            else if ((DateTimeOffset.Now - LastStreamingMessageTime).TotalSeconds
                > Math.Max(config.crawl.UserStreamTimeout, (LastStreamingMessageTime - TweetTime.Min).TotalSeconds))
            {
                //Console.WriteLine("{0} {1}: No streaming message for {2} sec.", DateTime.Now, Token.UserId, (DateTimeOffset.Now - LastStreamingMessageTime).TotalSeconds.ToString("#"));
                return NeedRetryResult.JustNeeded;
            }
            return NeedRetryResult.None;
        }

        //tokenの有効性を確認して自身のプロフィールも取得
        //Revokeの可能性があるときだけ呼ぶ
        public enum TokenStatus { Success, Failure, Revoked, Locked }
        public TokenStatus VerifyCredentials()
        {
            try
            {
                isAttemptingConnect = true;
                 StreamSubscriber?.Dispose(); StreamSubscriber = null; 
                //Console.WriteLine("{0} {1}: Verifying token", DateTime.Now, Token.UserId);
                db.StoreUserProfile(Token.Account.VerifyCredentials());
                Console.WriteLine("{0} {1}: Token verification success", DateTime.Now, Token.UserId);
                return TokenStatus.Success;
            }
            catch (TwitterException ex)
            {
                if (ex.Status == HttpStatusCode.Unauthorized)
                {
                    Console.WriteLine("{0} {1}: Unauthorized", DateTime.Now, Token.UserId);
                    return TokenStatus.Revoked;
                }
                else
                {
                    Console.WriteLine("{0} {1}: {2} {3}", DateTime.Now, Token.UserId, ex.Status, ex.Message);
                    return TokenStatus.Failure;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0} {1}: {2}", DateTime.Now, Token.UserId, ex.Message);
                return TokenStatus.Failure;
            }
            finally { isAttemptingConnect = false; }
        }

        public void RecieveStream()
        {
            StreamSubscriber?.Dispose(); StreamSubscriber = null;
            e = null;
            LastStreamingMessageTime = DateTimeOffset.Now;
            TweetTime.Add(LastStreamingMessageTime);
            StreamSubscriber = Token.Streaming.UserAsObservable()
                .ObserveOn(CurrentThreadScheduler.Instance)
                .Subscribe(
                (StreamingMessage m) =>
                {
                    DateTimeOffset now = DateTimeOffset.Now;
                    LastStreamingMessageTime = now;
                    if (m.Type == MessageType.Create) { TweetTime.Add(now); }
                    HandleStreamingMessage(m);
                },
                (Exception ex) =>
                {
                    if (ex is TaskCanceledException) { Environment.Exit(1); }    //つまり自殺
                    else if (ex is WebException wex) { e = TwitterException.Create(wex); }
                    else { e = ex; }
                    //Console.WriteLine("{0} {1}: {2}", DateTime.Now, Token.UserId, ex.Message);
                },
                () => { e = new Exception("Userstream unexpectedly closed"); } //接続中のRevokeはこれ
                );
        }
        
        public TokenStatus RecieveRestTimeline()
        {
            //RESTで取得してツイートをDBに突っ込む
            //各ツイートの時刻をTweetTimeに格納
            try
            {
                CoreTweet.Core.ListedResponse<Status> Timeline;
                Timeline = Token.Statuses.HomeTimeline(count => 200, tweet_mode => TweetMode.Extended);

                //Console.WriteLine("{0} {1}: Handling {2} RESTed timeline", DateTime.Now, Token.UserId, Timeline.Count);
                DateTimeOffset[] RestTweetTime = new DateTimeOffset[Timeline.Count];
                for (int i = 0; i < Timeline.Count; i++)
                {
                    HandleTweet(Timeline[i], false);
                    RestTweetTime[i] = Timeline[i].CreatedAt;
                }
                for (int i = Math.Max(0, RestTweetTime.Length - config.crawl.UserStreamTimeoutTweets); i < RestTweetTime.Length; i++)
                {
                    TweetTime.Add(RestTweetTime[i]);
                }
                if (Timeline.Count == 0) { TweetTime.Add(DateTimeOffset.Now); }
                //Console.WriteLine("{0} {1}: REST timeline success", DateTime.Now, Token.UserId);
                return TokenStatus.Success;
            }
            catch (TwitterException ex)
            {
                if (ex.Status == HttpStatusCode.Unauthorized)
                {
                    Console.WriteLine("{0} {1}: Unauthorized", DateTime.Now, Token.UserId);
                    return TokenStatus.Revoked;
                }
                else if (ex.Status == HttpStatusCode.Forbidden)
                {
                    Console.WriteLine("{0} {1}: Locked", DateTime.Now, Token.UserId);
                    return TokenStatus.Locked;
                }
                else
                {
                    Console.WriteLine("{0} {1}: {2} {3}", DateTime.Now, Token.UserId, ex.Status, ex.Message);
                    return TokenStatus.Failure;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0} {1}: {2}", DateTime.Now, Token.UserId, ex.Message);
                return TokenStatus.Failure;
            }
        }

        public void RestMyTweet()
        {
            //RESTで取得してツイートをDBに突っ込む
            try
            {
                CoreTweet.Core.ListedResponse<Status> Tweets = Token.Statuses.UserTimeline(user_id => Token.UserId, count => 200, tweet_mode => TweetMode.Extended);

                //Console.WriteLine("{0} {1}: Handling {2} RESTed tweets", DateTime.Now, Token.UserId, Tweets.Count);
                foreach (Status s in Tweets)
                {   //ここでRESTをDBに突っ込む
                    HandleTweet(s, false);
                }
                //Console.WriteLine("{0} {1}: REST tweets success", DateTime.Now, Token.UserId);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} {1}: REST tweets failed: {2}", DateTime.Now, Token.UserId, e.Message);
            }
        }

        public void RestBlock()
        {
            long[] blocks = RestCursored(RestCursorMode.Block);
            if (blocks != null)
            {
                db.StoreBlocks(blocks, Token.UserId);
                //Console.WriteLine("{0} {1}: REST blocks success", DateTime.Now, Token.UserId);
            }
            else { Console.WriteLine("{0} {1}: REST blocks failed", DateTime.Now, Token.UserId); }
        }

        enum RestCursorMode { Friend, Block }
        long[] RestCursored(RestCursorMode Mode)
        {
            try
            {
                switch (Mode)
                {
                    case RestCursorMode.Block:
                        return Token.Blocks.EnumerateIds(EnumerateMode.Next, user_id => Token.UserId).ToArray();
                    case RestCursorMode.Friend:
                        return Token.Friends.EnumerateIds(EnumerateMode.Next, user_id => Token.UserId).ToArray();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} {1}: REST {2}s failed: {3}", DateTime.Now, Token.UserId, Mode.ToString(), e.Message);
            }
            return null;
        }

        void HandleStreamingMessage(StreamingMessage x)
        {
            switch (x.Type)
            {
                case MessageType.Create:
                    HandleTweet((x as StatusMessage).Status);
                    break;
                case MessageType.DeleteStatus:
                    StaticMethods.DeleteTweet((x as DeleteMessage).Id);
                    break;
                case MessageType.Friends:
                    //UserStream接続時に届く(10000フォロー超だと届かない)
                    db.StoreFriends(x as FriendsMessage, Token.UserId);
                    //Console.WriteLine("{0} {1}: Stream connected", DateTime.Now, Token.UserId);
                    break;
                case MessageType.Disconnect:
                    //届かないことの方が多い
                    Console.WriteLine("{0} {1}: DisconnectMessage({2})", DateTime.Now, Token.UserId, (x as DisconnectMessage).Code);
                    break;
                case MessageType.Event:
                    HandleEventMessage(x as EventMessage);
                    break;
                case MessageType.Warning:
                    if ((x as WarningMessage).Code == "FOLLOWS_OVER_LIMIT")
                    {
                        long[] friends = RestCursored(RestCursorMode.Friend);
                        if (friends != null) { db.StoreFriends(friends, Token.UserId); }
                        Console.WriteLine("{0} {1}: REST friends success", DateTime.Now, Token.UserId);
                        //Console.WriteLine("{0} {1}: Stream connected", DateTime.Now, Token.UserId);
                    }
                    break;
            }
        }

        void HandleEventMessage(EventMessage x)
        {
            switch (x.Event)
            {
                case EventCode.Follow:
                case EventCode.Unfollow:
                case EventCode.Unblock:
                    if (x.Source.Id == Token.UserId) { db.StoreEvents(x); }
                    break;
                case EventCode.Block:
                    if (x.Source.Id == Token.UserId || x.Target.Id == Token.UserId) { db.StoreEvents(x); }
                    break;
                case EventCode.UserUpdate:
                    if (x.Source.Id == Token.UserId) { db.StoreUserProfile(Token.Account.VerifyCredentials()); }
                    break;
            }
        }

        //ツイートをDBに保存したりRTを先に保存したりする
        //アイコンを適宜保存する
        int HandleTweet(Status x, bool update = true, bool locked = false)
        {
            //画像なしツイートは捨てる
            if ((x.ExtendedEntities ?? x.Entities).Media == null) { return 0; }
            if (!locked && !StreamerLocker.LockTweet(x.Id)) { return 0; }
            int ret = 0;
            //RTを先にやる(キー制約)
            if (x.RetweetedStatus != null) { ret += HandleTweet(x.RetweetedStatus, update); }
            if (StreamerLocker.LockUser(x.User.Id))
            {
                if (update) { db.StoreUser(x, false); StaticMethods.DownloadStoreProfileImage(x); }
                else { db.StoreUser(x, false, false); }
            }
            int ret2;
            Counter.TweetToStore.Increment();
            if ((ret2 = db.StoreTweet(x, update)) >= 0)
            {
                if (ret2 > 0) { Counter.TweetStored.Increment(); }
                if (x.RetweetedStatus == null) { StaticMethods.DownloadStoreMedia(x); }
            }
            if (!locked) { StreamerLocker.UnlockTweet(x.Id); }
            return ret + ret2;
        }

        DateTimeOffset? OneTweetReset;
        int DownloadOneTweet(long StatusId)
        {
            if (OneTweetReset != null && OneTweetReset > DateTimeOffset.Now) { return 0; }
            OneTweetReset = null;
            if (!StreamerLocker.LockTweet(StatusId)) { return 0; }
            try
            {
                if (db.ExistTweet(StatusId)) { return 0; }
                var res = Token.Statuses.Lookup(id => StatusId, include_entities => true, tweet_mode => TweetMode.Extended);
                if (res.RateLimit.Remaining < 1) { OneTweetReset = res.RateLimit.Reset.AddMinutes(1); }  //とりあえず1分延長奴
                return HandleTweet(res.First(), true, true);
            }
            catch { Console.WriteLine("{0} {1} REST Tweet failed: {2}", DateTime.Now, Token.UserId, StatusId); return 0; }
            finally { StreamerLocker.UnlockTweet(StatusId); }
        }



        static class StaticMethods
        {
            static StaticMethods()
            {
                DeleteTweetBatch.LinkTo(DeleteTweetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            }

            public static void DownloadStoreProfileImage(Status x)
            {
                //アイコンが更新または未保存ならダウンロードする
                //RTは自動でやらない
                //ダウンロード成功してもしなくてもそれなりにDBに反映する
                //(古い奴のURLがDBにあれば古いままになる)
                if (x.User.Id == null) { return; }
                string ProfileImageUrl = x.User.ProfileImageUrlHttps ?? x.User.ProfileImageUrl;
                DBHandler.ProfileImageInfo d = db.NeedtoDownloadProfileImage(x.User.Id.Value, ProfileImageUrl);
                if (!d.NeedDownload || !StreamerLocker.LockProfileImage((long)x.User.Id)) { return; }

                //新しいアイコンの保存先 卵アイコンは'_'をつけただけの名前で保存するお
                string LocalPath = x.User.IsDefaultProfileImage ?
                    config.crawl.PictPathProfileImage + '_' + Path.GetFileName(ProfileImageUrl) :
                    config.crawl.PictPathProfileImage + x.User.Id.ToString() + Path.GetExtension(ProfileImageUrl);

                bool DownloadOK = true; //卵アイコンのダウンロード不要でもtrue
                if (!x.User.IsDefaultProfileImage || !File.Exists(LocalPath))
                {
                    bool RetryFlag = false;
                    RetryLabel:
                    try
                    {
                        HttpWebRequest req = WebRequest.Create(ProfileImageUrl) as HttpWebRequest;
                        req.Referer = StatusUrl(x);
                        using (WebResponse res = req.GetResponse())
                        using (FileStream file = File.Create(LocalPath))
                        {
                            res.GetResponseStream().CopyTo(file);
                        }
                    }
                    catch (Exception ex)
                    {   //404等じゃなければ1回だけリトライする
                        if (!RetryFlag && !(ex is WebException we && we.Status == WebExceptionStatus.ProtocolError))
                        {
                            RetryFlag = true;
                            goto RetryLabel;
                        }
                        else { DownloadOK = false; }
                    }
                }
                if (DownloadOK)
                {
                    string oldext = Path.GetExtension(d.OldProfileImageUrl);
                    string newext = Path.GetExtension(ProfileImageUrl);
                    if (!d.isDefaultProfileImage && oldext != null && oldext != newext)  //卵アイコンはこのパスじゃないしそもそも消さない
                    { File.Delete(config.crawl.PictPathProfileImage + x.User.Id.ToString() + oldext); }
                    db.StoreUser(x, true);
                }
                else { db.StoreUser(x, false); }
            }

            static ActionBlock<Status> DownloadStoreMediaBlock = new ActionBlock<Status>((Status x) =>
            {
                foreach (MediaEntity m in x.ExtendedEntities.Media)
                {
                    Counter.MediaTotal.Increment();
                //画像の情報はあるのにsource_tweet_idがない場合だけここでやる
                switch (db.ExistMedia_source_tweet_id(m.Id))
                    {
                        case null:
                            db.Storetweet_media(x.Id, m.Id);
                            db.UpdateMedia_source_tweet_id(m, x);
                            continue;
                        case true:
                            db.Storetweet_media(x.Id, m.Id);
                            continue;
                    }
                //ハッシュがない時だけ落とす
                string MediaUrl = m.MediaUrlHttps ?? m.MediaUrl;
                    string LocalPaththumb = config.crawl.PictPaththumb + m.Id.ToString() + Path.GetExtension(MediaUrl);  //m.Urlとm.MediaUrlは違う
                string uri = MediaUrl + (MediaUrl.IndexOf("twimg.com") >= 0 ? ":thumb" : "");

                    bool RetryFlag = false;
                    RetryLabel:
                    try
                    {
                        HttpWebRequest req = WebRequest.Create(uri) as HttpWebRequest;
                        req.Referer = StatusUrl(x);
                        using (WebResponse res = req.GetResponse())
                        using (MemoryStream mem = new MemoryStream((int)res.ContentLength))
                        {
                            res.GetResponseStream().CopyTo(mem);
                            res.Close();
                            long? dcthash = PictHash.DCTHash(mem);
                            if (dcthash != null && (db.StoreMedia(m, x, (long)dcthash)) > 0)
                            {
                                using (FileStream file = File.Create(LocalPaththumb))
                                {
                                    mem.Position = 0;   //こいつは必要だった
                                    mem.CopyTo(file);
                                }
                                Counter.MediaSuccess.Increment();
                            }
                        }
                    }
                    catch (Exception ex)
                    {   //404等じゃなければ1回だけリトライする
                    if (!RetryFlag && !(ex is WebException && (ex as WebException).Status == WebExceptionStatus.ProtocolError))
                        {
                            RetryFlag = true;
                            goto RetryLabel;
                        }
                    }
                    Counter.MediaToStore.Increment();
                //URL転載元もペアを記録する
                if (m.SourceStatusId != null && m.SourceStatusId != x.Id)
                    {
                        db.Storetweet_media(m.SourceStatusId.Value, m.Id);
                    }
                }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.MediaDownloadThreads,
                MaxMessagesPerTask = 1
            });

            public static void DownloadStoreMedia(Status x)
            {
                if (x.RetweetedStatus != null)
                {   //そもそもRTに対してこれを呼ぶべきではない
                    DownloadStoreMedia(x.RetweetedStatus);
                    return;
                }
                DownloadStoreMediaBlock.Post(x);
            }
            
            static BatchBlock<long> DeleteTweetBatch = new BatchBlock<long>(config.crawl.TweetDeleteUnit);
            static ActionBlock<long[]> DeleteTweetBlock = new ActionBlock<long[]>
                ((long[] ToDelete) => {
                    //ツイ消しはここでDBに投げることにした
                    int DeletedCount;
                    if (db.StoreDelete(ToDelete, out DeletedCount)) { foreach (long d in ToDelete) { byte gomi; DeleteTweetLock.TryRemove(d, out gomi); } }
                    else { foreach(long d in ToDelete) { DeleteTweetBatch.Post(d); } }
                    Counter.TweetToDelete.Add(ToDelete.Length);
                    Counter.TweetDeleted.Add(DeletedCount);
                }, new ExecutionDataflowBlockOptions()
                {
                    MaxMessagesPerTask = 1
                });

            static ConcurrentDictionary<long, byte> DeleteTweetLock = new ConcurrentDictionary<long, byte>();
            public static void DeleteTweet(long StatusId)
            {
                if (DeleteTweetLock.TryAdd(StatusId, 0)) { DeleteTweetBatch.Post(StatusId); }
            }

            //ツイートのURLを作る
            public static string StatusUrl(Status x)
            {
                return "https://twitter.com/" + x.User.ScreenName + "/status/" + x.Id;
            }
        }



        //ツイートの処理を調停する感じの奴
        public static class StreamerLocker
        {
            //storetweet用
            static ConcurrentDictionary<long, byte> LockedTweets = new ConcurrentDictionary<long, byte>();
            public static bool LockTweet(long Id) { return LockedTweets.TryAdd(Id, 0) && db.LockTweet(Id); }
            static ConcurrentQueue<long> UnlockTweets = new ConcurrentQueue<long>();
            public static void UnlockTweet(long Id) { UnlockTweets.Enqueue(Id); }

            //storeuser用
            //UnlockUser()はない Unlock()で処理する
            static ConcurrentDictionary<long, byte> LockedUsers = new ConcurrentDictionary<long, byte>();
            public static bool LockUser(long? Id) { return Id != null && LockedUsers.TryAdd((long)Id, 0); }

            //DownloadProfileImage用
            static ConcurrentDictionary<long, byte> LockedProfileImages = new ConcurrentDictionary<long, byte>();
            public static bool LockProfileImage(long Id) { return LockedProfileImages.TryAdd(Id, 0); }

            static List<long> UnlockTweetID = new List<long>();

            //これを外から呼び出してロックを解除する
            public static void Unlock()
            {
                LockedUsers.Clear();
                LockedProfileImages.Clear();

                //UnlockTweetID, DBのtweetlockは1周遅れでロック解除する
                if (db.UnlockTweet(UnlockTweetID) > 0)
                {
                    foreach (long Id in UnlockTweetID) { LockedTweets.TryRemove(Id, out byte z); }
                    UnlockTweetID.Clear();
                }
                while (UnlockTweets.TryDequeue(out long tmp)) { UnlockTweetID.Add(tmp); }
            }
        }



        public static class Counter
        {
            //パフォーマンスカウンター的な何か
            public class CounterValue
            {
                int Value = 0;
                public void Increment() { Interlocked.Increment(ref Value); }
                public void Add(int v) { Interlocked.Add(ref Value, v); }
                public int Get() { return Value; }
                public int GetReset() { return Interlocked.Exchange(ref Value, 0); }
            }

            public static CounterValue MediaSuccess = new CounterValue();
            public static CounterValue MediaToStore = new CounterValue();
            public static CounterValue MediaTotal = new CounterValue();
            public static CounterValue TweetStored = new CounterValue();
            public static CounterValue TweetToStore = new CounterValue();
            public static CounterValue TweetToDelete = new CounterValue();
            public static CounterValue TweetDeleted = new CounterValue();
            //ひとまずアイコンは除外しようか
            public static void PrintReset()
            {
                if (MediaToStore.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} / {3} Media Stored", DateTime.Now, MediaSuccess.GetReset(), MediaToStore.GetReset(), MediaTotal.GetReset()); }
                if (TweetToStore.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} Tweet Stored", DateTime.Now, TweetStored.GetReset(), TweetToStore.GetReset()); }
                if (TweetToDelete.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} Tweet Deleted", DateTime.Now, TweetDeleted.GetReset(), TweetToDelete.GetReset()); }
            }
        }
    }
}
