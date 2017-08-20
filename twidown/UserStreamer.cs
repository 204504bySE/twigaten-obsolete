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
using System.Net.Sockets;

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
                    StaticMethods.HandleTweetRest(Timeline[i], Token);
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
                    StaticMethods.HandleTweetRest(s, Token);
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
                    StaticMethods.HandleStatusMessage((x as StatusMessage).Status, Token);
                    break;
                case MessageType.DeleteStatus:
                    StaticMethods.HandleDeleteMessage(x as DeleteMessage);
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


        public static class StaticMethods
        {
            static StaticMethods()
            {
                DeleteTweetBatch.LinkTo(DeleteTweetBlock, new DataflowLinkOptions { PropagateCompletion = true });
                TweetDistinctBlock.LinkTo(HandleTweetBlock, new DataflowLinkOptions { PropagateCompletion = true });
                Udp.Client.ReceiveTimeout = 1000;
            }

            public static void ShowCount()
            {
                int a, b;
                if ((a = TweetDistinctBlock.InputCount) > 0 | (b = HandleTweetBlock.InputCount) > 0)
                { Console.WriteLine("{0} App: {1} -> {2} Tweets in buffer", DateTime.Now, a, b); }
                if ((a = DownloadStoreMediaBlock.InputCount) > 0) { Console.WriteLine("{0} App: {1} Media in buffer", DateTime.Now, a); }
            }

            //ツイートをDBに保存したりRTを先に保存したりする
            //アイコンを適宜保存する
            public static void HandleTweetRest(Status x, Tokens t)   //REST用
            {
                if ((x.ExtendedEntities ?? x.Entities)?.Media == null) { return; }   //画像なしツイートを捨てる
                TweetDistinctBlock.Post(new Tuple<Status, Tokens, bool>(x, t, false));
            }

            public static void HandleStatusMessage(Status x, Tokens t)
            {
                if ((x.ExtendedEntities ?? x.Entities)?.Media != null)  //画像なしツイートを捨てる
                { TweetDistinctBlock.Post(new Tuple<Status, Tokens, bool>(x, t, true)); }
            }

            public static void HandleDeleteMessage(DeleteMessage x)
            {
                //DeleteTweetBufferSizeが小さいとツイートよりツイ消しが先に処理されるかも
                DeleteTweetBatch.Post(x.Id);
            }

            //static HashSet<long> LockedTweets = new HashSet<long>();
            static UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, (config.crawl.ParentUdpPort | (System.Diagnostics.Process.GetCurrentProcess().Id & 0x3FFE)) + 1)) { DontFragment = true };
            static IPEndPoint ParentEndPoint = new IPEndPoint(IPAddress.Loopback, config.crawl.ParentUdpPort);
            static bool LockTweetUdp(long tweet_id)
            {
                try
                {
                    Udp.Send(BitConverter.GetBytes(tweet_id), sizeof(long), ParentEndPoint);
                    IPEndPoint RemoteUdp = null;
                    return BitConverter.ToBoolean(Udp.Receive(ref RemoteUdp), 0);
                }
                catch(Exception e) { Console.WriteLine(e); return false; }
            }

            static TransformBlock<Tuple<Status, Tokens, bool>, Tuple<Status, Tokens, bool>> TweetDistinctBlock
                = new TransformBlock<Tuple<Status, Tokens, bool>, Tuple<Status, Tokens, bool>>(x =>
                {   //ここでLockする(1スレッドなのでHashSetでおｋ
                    //if (StreamerLocker.LockTweetClearFlag) { LockedTweets.Clear(); StreamerLocker.LockTweetClearFlag = false; }
                    //if (LockedTweets.Add(x.Item1.Id) && db.LockTweet(x.Item1.Id)) { return x; }
                    if (LockTweetUdp(x.Item1.Id)) { return x; }
                    else { return null; }
                }, new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = 1
                });
            static ActionBlock<Tuple<Status, Tokens, bool>> HandleTweetBlock = new ActionBlock<Tuple<Status, Tokens, bool>>(x =>
            {   //Tokenを渡すためだけにKeyValuePairにしている #ウンコード
                //画像なしツイートは先に捨ててるのでここでは確認しない
                if (x != null) { HandleTweet(x.Item1, x.Item2, x.Item3); }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.MaxDBConnections, //一応これで
            });

            static void HandleTweet(Status x, Tokens t, bool stream)    //stream(=true)からのツイートならふぁぼRT数を上書きする
            {
                if ((x.ExtendedEntities ?? x.Entities).Media == null) { return; } //画像なしツイートを捨てる
                //RTを先にやる(キー制約)
                if (x.RetweetedStatus != null) { HandleTweet(x.RetweetedStatus, t, stream); }
                if (StreamerLocker.LockUser(x.User.Id))
                {
                    if (stream) { db.StoreUser(x, false); DownloadStoreProfileImage(x); }
                    else { db.StoreUser(x, false, false); }
                }
                if (stream) { Counter.TweetToStoreStream.Increment(); }
                else { Counter.TweetToStoreRest.Increment(); }
                int r;
                if ((r = db.StoreTweet(x, stream)) >= 0)
                {
                    if (r > 0)
                    {
                        if (stream) { Counter.TweetStoredStream.Increment(); }
                        else { Counter.TweetStoredRest.Increment(); }
                    }
                    if (x.RetweetedStatus == null) { DownloadStoreMedia(x, t); }
                }
                 //StreamerLocker.UnlockTweet(x.Id);  //Lockは事前にやっておくこと
            }

            static void DownloadStoreProfileImage(Status x)
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

            static ActionBlock<KeyValuePair<Status,Tokens>> DownloadStoreMediaBlock = new ActionBlock<KeyValuePair<Status, Tokens>>(a =>
            {
                Status x = a.Key;
                Tokens t = a.Value;
                Lazy<HashSet<long>> RestId = new Lazy<HashSet<long>>();   //同じツイートを何度も処理したくない
                foreach (MediaEntity m in x.ExtendedEntities.Media)
                {
                    Counter.MediaTotal.Increment();

                    //URLぶち抜き転載の場合はここでツイートをダウンロード(すでにあればキャンセルされる
                    //x.Idのツイートのダウンロード失敗については何もしない(成功したツイートのみPostするべき
                    bool OtherSourceTweet = m.SourceStatusId.HasValue && m.SourceStatusId.Value != x.Id;    //URLぶち抜きならtrue
                    switch (db.ExistMedia_source_tweet_id(m.Id))
                    {
                        case true:
                            if (OtherSourceTweet) { db.Storetweet_media(x.Id, m.Id); }
                            continue;
                        case null:
                            if (OtherSourceTweet && RestId.Value.Add(x.Id)) { DownloadOneTweet(m.SourceStatusId.Value, t); }
                            db.Storetweet_media(x.Id, m.Id);
                            db.UpdateMedia_source_tweet_id(m, x);
                            continue;
                        case false:
                            if (OtherSourceTweet && RestId.Value.Add(x.Id)) { DownloadOneTweet(m.SourceStatusId.Value, t); }    //コピペつらい
                            break;   //画像の情報がないときだけダウンロードする
                    }
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
                if (OtherSourceTweet) { db.Storetweet_media(m.SourceStatusId.Value, m.Id); }
                }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.MediaDownloadThreads
            });

            public static void DownloadStoreMedia(Status x, Tokens t)
            {
                if (x.RetweetedStatus != null)
                {   //そもそもRTに対してこれを呼ぶべきではない
                    DownloadStoreMedia(x.RetweetedStatus, t);
                    return;
                }
                DownloadStoreMediaBlock.Post(new KeyValuePair<Status, Tokens>(x, t));
            }

            //API制限対策用
            static ConcurrentDictionary<Tokens, DateTimeOffset> OneTweetReset = new ConcurrentDictionary<Tokens, DateTimeOffset>();
            //source_tweet_idが一致しないやつは元ツイートを取得したい
            static void DownloadOneTweet(long StatusId, Tokens Token)
            {
                if (OneTweetReset.ContainsKey(Token) && OneTweetReset[Token] > DateTimeOffset.Now) { return; }
                OneTweetReset.TryRemove(Token, out DateTimeOffset gomi);
                //if (!StreamerLocker.LockTweet(StatusId)) { return; }  //もうチェックしなくていいや(雑
                try
                {
                    if (db.ExistTweet(StatusId)) { return; }
                    var res = Token.Statuses.Lookup(id => StatusId, include_entities => true, tweet_mode => TweetMode.Extended);
                    if (res.RateLimit.Remaining < 1) { OneTweetReset[Token] = res.RateLimit.Reset.AddMinutes(1); }  //とりあえず1分延長奴
                    StaticMethods.HandleTweet(res.First(), Token, true);
                }
                catch { Console.WriteLine("{0} {1} REST Tweet failed: {2}", DateTime.Now, Token.UserId, StatusId); return; }
                //finally { StreamerLocker.UnlockTweet(StatusId); }
            }
            
            static BatchBlock<long> DeleteTweetBatch = new BatchBlock<long>(config.crawl.DeleteTweetBufferSize);
            //ツイ消しはここでDBに投げることにした
            static ActionBlock<long[]> DeleteTweetBlock = new ActionBlock<long[]>
                ((long[] ToDelete) => {
                    foreach (long d in db.StoreDelete(ToDelete.Distinct().ToArray())) { DeleteTweetBatch.Post(d); }
                }, new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = 1
                });
            
            //ツイートのURLを作る
            public static string StatusUrl(Status x)
            {
                return "https://twitter.com/" + x.User.ScreenName + "/status/" + x.Id;
            }
        }



        //ツイートの処理を調停する感じの奴
        public static class StreamerLocker
        {
            //storeuser用
            //UnlockUser()はない Unlock()で処理する
            static ConcurrentDictionary<long, byte> LockedUsers = new ConcurrentDictionary<long, byte>();
            public static bool LockUser(long? Id) { return Id != null && LockedUsers.TryAdd((long)Id, 0); }

            //DownloadProfileImage用
            static ConcurrentDictionary<long, byte> LockedProfileImages = new ConcurrentDictionary<long, byte>();
            public static bool LockProfileImage(long Id) { return LockedProfileImages.TryAdd(Id, 0); }

            //これを外から呼び出してロックを解除する
            public static void Unlock()
            {
                LockedUsers.Clear();
                LockedProfileImages.Clear();
            }
        }

        public static class Counter
        {
            //パフォーマンスカウンター的な何か
            public struct CounterValue
            {
                int Value;
                public void Increment() { Interlocked.Increment(ref Value); }
                public void Add(int v) { Interlocked.Add(ref Value, v); }
                public int Get() { return Value; }
                public int GetReset() { return Interlocked.Exchange(ref Value, 0); }
            }

            public static CounterValue MediaSuccess = new CounterValue();
            public static CounterValue MediaToStore = new CounterValue();
            public static CounterValue MediaTotal = new CounterValue();
            public static CounterValue TweetStoredStream = new CounterValue();
            public static CounterValue TweetStoredRest = new CounterValue();
            public static CounterValue TweetToStoreStream = new CounterValue();
            public static CounterValue TweetToStoreRest = new CounterValue();
            public static CounterValue TweetToDelete = new CounterValue();
            public static CounterValue TweetDeleted = new CounterValue();
            //ひとまずアイコンは除外しようか
            public static void PrintReset()
            {
                if (TweetToStoreStream.Get() > 0 || TweetToStoreRest.Get() > 0)
                {
                    Console.WriteLine("{0} App: {1} + {2} / {3} + {4} Tweet Stored", DateTime.Now,
                    TweetStoredStream.GetReset(), TweetStoredRest.GetReset(),
                    TweetToStoreStream.GetReset(), TweetToStoreRest.GetReset());
                }
                if (TweetToDelete.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} Tweet Deleted", DateTime.Now, TweetDeleted.GetReset(), TweetToDelete.GetReset()); }
                if (MediaToStore.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} / {3} Media Stored", DateTime.Now, MediaSuccess.GetReset(), MediaToStore.GetReset(), MediaTotal.GetReset()); }
            }
        }
    }
}
