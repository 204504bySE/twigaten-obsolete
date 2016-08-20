using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using System.Reactive.Linq;
using CoreTweet;
using CoreTweet.Streaming;
using System.IO;
using System.Net;
using System.Text;
using System.Collections.Generic;
using System.Collections.Concurrent;
using twitenlib;

namespace twidown
{
    class UserStreamer
    {
        // 各TokenのUserstreamを受信したり仕分けたりする

        public Exception e { get; private set; }
        public Tokens Token { get; }
        Config config = Config.Instance;
        DBHandler db = DBHandler.Instance;
        IObservable<StreamingMessage> UserStream;
        IDisposable StreamDisposable = null;
        DateTimeOffset LastStreamingMessageTime = DateTimeOffset.Now;
        DateTimeOffset LatestTweetTime = DateTimeOffset.Now;
        ConcurrentQueue<DateTimeOffset> TweetTimeQueue = new ConcurrentQueue<DateTimeOffset>();
        DateTimeOffset nulloffset;  //outで捨てる用
        bool isAttemptingConnect = false;
        StreamerLocker Locker = StreamerLocker.Instance;

        public UserStreamer(Tokens t)
        {
            Token = t;
            Token.ConnectionOptions.DisableKeepAlive = false;
            Token.ConnectionOptions.UseCompression = true;
            Token.ConnectionOptions.UseCompressionOnStreaming = true;
        }

        //これを外部から叩いてtrueなら再接続
        public bool NeedRetry()
        {
            if (e != null || (!isAttemptingConnect && StreamDisposable == null))
            {
                if (e != null)
                {
                    Console.WriteLine("{0} {1}:\n{2}", DateTime.Now, Token.UserId, e);
                    if (e is TaskCanceledException) { LogFailure.Write("TaskCanceledException"); Environment.Exit(1); }    //つまり自殺
                    e = null;
                }
                if (StreamDisposable != null)
                {
                    StreamDisposable.Dispose();
                    StreamDisposable = null;
                }
                return true;
            }
            DateTimeOffset TimeoutTweetTime;
            if (!TweetTimeQueue.TryPeek(out TimeoutTweetTime)) { TimeoutTweetTime = DateTime.Now; }
            if ((DateTimeOffset.Now - LastStreamingMessageTime).TotalSeconds
                > Math.Max(config.crawl.UserStreamTimeout, (LastStreamingMessageTime - TimeoutTweetTime).TotalSeconds))
            {
                Console.WriteLine("{0} {1}: No streaming message for {2} sec.", DateTime.Now, Token.UserId, (DateTimeOffset.Now - LastStreamingMessageTime).TotalSeconds);
                return true;
            }
            return false;
        }

        public enum ConnectResult { Success, Failure, Revoked }
        public ConnectResult Connect()
        {
            try
            {
                isAttemptingConnect = true;
                if (StreamDisposable != null) { StreamDisposable.Dispose(); StreamDisposable = null; }
                Console.WriteLine("{0} {1}: Attempting to connect", DateTime.Now, Token.UserId);
                db.StoreUserProfile(Token.Account.VerifyCredentials());
                Console.WriteLine("{0} {1}: Token verification success", DateTime.Now, Token.UserId);
                return ConnectResult.Success;
            }
            catch (TwitterException ex)
            {
                if (ex.Status == HttpStatusCode.Unauthorized)
                {
                    Console.WriteLine("{0} {1}: Unauthorized", DateTime.Now, Token.UserId);
                    return ConnectResult.Revoked;
                }
                else
                {
                    Console.WriteLine("{0} {1}:\n{2}", DateTime.Now, Token.UserId, ex);
                    return ConnectResult.Failure;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0} {1}:\n{2}", DateTime.Now, Token.UserId, ex);
                return ConnectResult.Failure;
            }
            finally { isAttemptingConnect = false; }
        }

        public void RecieveRestTimeline()
        {
            DateTimeOffset[] RestTweetTime = RestTimeline();
            for (int i = Math.Max(0, RestTweetTime.Length - config.crawl.UserStreamTimeoutTweets); i < RestTweetTime.Length; i++)
            {
                TweetTimeQueue.Enqueue(RestTweetTime[i]);
                while (TweetTimeQueue.Count > config.crawl.UserStreamTimeoutTweets
                    && TweetTimeQueue.TryDequeue(out nulloffset)) { }
            }
        }

        public void RecieveStream()
        {
            if (StreamDisposable != null) { StreamDisposable.Dispose(); StreamDisposable = null; }

            LastStreamingMessageTime = DateTimeOffset.Now;
            UserStream = Token.Streaming.UserAsObservable();
            StreamDisposable = UserStream.Subscribe(
                (StreamingMessage m) =>
                {
                    DateTimeOffset now = DateTimeOffset.Now;
                    LastStreamingMessageTime = now;
                    if (m.Type == MessageType.Create)
                    {
                        LatestTweetTime = now;
                        TweetTimeQueue.Enqueue(now);
                        while (TweetTimeQueue.Count > config.crawl.UserStreamTimeoutTweets
                            && TweetTimeQueue.TryDequeue(out nulloffset)) { }
                    }
                    HandleStreamingMessage(m);
                },
                (Exception ex) => { StreamDisposable.Dispose(); StreamDisposable = null; e = ex; },
                () => { StreamDisposable.Dispose(); StreamDisposable = null;  //接続中のRevokeはこれ
                    e = new Exception("UserAsObservable unexpectedly finished."); }
                );
        }

        //----------------------------------------
        // ここから下はUtility Classだったやつ
        // つまり具体的な処理が多い(適当
        //----------------------------------------

        public void HandleStreamingMessage(StreamingMessage x)
        {
            switch (x.Type)
            {
                case MessageType.Create:
                    HandleTweet((x as StatusMessage).Status);
                    break;
                case MessageType.DeleteStatus:
                    HandleTweet(x as DeleteMessage);
                    break;
                case MessageType.Friends:
                    //UserStream接続時に届く(10000フォロー超だと届かない)
                    db.StoreFriends(x as FriendsMessage, Token.UserId);
                    Console.WriteLine("{0} {1}: Stream connected", DateTime.Now, Token.UserId);
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
                        Console.WriteLine("{0} {1}: Stream connected", DateTime.Now, Token.UserId);
                    }
                    break;
            }
        }

        public void RestMyTweet()
        {
            //RESTで取得してツイートをDBに突っ込む
            try
            {
                CoreTweet.Core.ListedResponse<Status> Tweets = Token.Statuses.UserTimeline(user_id => Token.UserId, count => 200);

                Console.WriteLine("{0} {1}: Handling {2} RESTed tweets", DateTime.Now, Token.UserId, Tweets.Count);
                foreach (Status s in Tweets)
                {   //ここでRESTをDBに突っ込む
                    HandleTweet(s, false);
                }
                Console.WriteLine("{0} {1}: REST tweets success", DateTime.Now, Token.UserId);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} {1}: REST tweets failed:\n{2}", DateTime.Now, Token.UserId, e);
            }
        }

        DateTimeOffset[] RestTimeline()
        {
            //RESTで取得してツイートをDBに突っ込む 戻り値はツイート時刻の列
            //0ツイートだったら現在時刻を返して1ツイート受信したっぽい戻り値にする
            try
            {
                CoreTweet.Core.ListedResponse<Status> Timeline = Token.Statuses.HomeTimeline(count => 200);
                Console.WriteLine("{0} {1}: Handling {2} RESTed timeline", DateTime.Now, Token.UserId, Timeline.Count);
                DateTimeOffset[] ret = new DateTimeOffset[Timeline.Count];
                for (int i = 0; i < Timeline.Count; i++)
                {
                    HandleTweet(Timeline[i], false);
                    ret[i] = Timeline[i].CreatedAt;
                }
                Console.WriteLine("{0} {1}: REST timeline success", DateTime.Now, Token.UserId);
                if (ret.Length == 0) { return new DateTimeOffset[] { DateTimeOffset.UtcNow }; }
                else { return ret; }
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} {1}: REST timeline failed:\n{2}", DateTime.Now, Token.UserId, e);
                return new DateTimeOffset[0];
            }
        }


        public void RestBlock()
        {
            long[] blocks = RestCursored(RestCursorMode.Block);
            if (blocks != null)
            {
                db.StoreBlocks(blocks, Token.UserId);
                Console.WriteLine("{0} {1}: REST blocks success", DateTime.Now, Token.UserId);
            }
            else { Console.WriteLine("{0} {1}: REST blocks failed", DateTime.Now, Token.UserId); }
        }

        public enum RestCursorMode { Friend, Block }
        public long[] RestCursored(RestCursorMode Mode)
        {
            Cursored<long> CursoredUsers = new Cursored<long>();
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
                Console.WriteLine("{0} {1}: REST {2}s failed:\n{2}", DateTime.Now, Token.UserId, e, Mode.ToString());
            }
            return null;
        }

        void HandleTweet(DeleteMessage x)
        {
            if (Locker.LockDelete(x.Id)) { db.StoreTweet(x); }
        }


        //ツイートをDBに保存したりRTを先に保存したりする
        //アイコンを適宜保存する
        int HandleTweet(Status x, bool update = true, bool locked = false)
        {
            //画像なしツイートは捨てる
            if (x.Entities.Media == null) { return 0; }
            if (!locked && !Locker.LockTweet(x.Id)) { return 0; }
            int ret = 0;
            //RTを先にやる(キー制約)
            if (x.RetweetedStatus != null) { ret += HandleTweet(x.RetweetedStatus, update); }
            if (Locker.LockUser(x.User.Id))
            {
                if (update) { db.StoreUser(x, DownloadProfileImage(x)); }
                else { db.StoreUser(x); }
            }
            int ret2;
            if ((ret2 = db.StoreTweet(x, update)) > 0 && x.RetweetedStatus == null) { DownloadMedia(x); }
            if (!locked) { Locker.UnlockTweet(x.Id); }
            return ret + ret2;
        }

        bool DownloadProfileImage(Status x)
        {
            //<summary>
            //アイコンが更新または未保存ならダウンロードする
            //RTは自動でやらない
            //成功したらtrue, 失敗またはダウンロード不要ならfalse
            //(古い奴のURLがDBにあれば古いままになる)
            //</summary>
            if (x.User.Id == null) { return false; }
            KeyValuePair<bool, string> d = db.NeedtoDownloadProfileImage((long)x.User.Id, x.User.ProfileImageUrl);
            if (d.Key)
            {
                if (Locker.LockProfileImage((long)x.User.Id))
                {
                    //string NewImageLocalPath = string.Format(@"{0}\{1}", config.crawl.PictPathProfileImage, localstrs.localmediapath(x.User.ProfileImageUrl));
                    string oldext = Path.GetExtension(d.Value);
                    string newext = Path.GetExtension(x.User.ProfileImageUrl);
                    string LocalPathnoExt = config.crawl.PictPathProfileImage + @"\" + x.User.Id.ToString();
                    if (downloadFile(x.User.ProfileImageUrl, LocalPathnoExt + newext, StatusUrl(x)))
                    {
                        if (oldext != null && oldext != newext) { File.Delete(LocalPathnoExt + oldext); }
                        //if (OldImageUrl != null && OldImageUrl.IndexOf("default_profile_images") < 0) { File.Delete(string.Format(@"{0}\{1}", config.crawl.PictPathProfileImage, localstrs.localmediapath(OldImageUrl))); }
                        return true;
                    }
                }
            }
            return false;
        }

        DateTimeOffset? OneTweetReset;
        int DownloadOneTweet(long StatusId)
        {
            if (OneTweetReset != null && OneTweetReset > DateTimeOffset.Now) { return 0; }
            OneTweetReset = null;
            if (!Locker.LockTweet(StatusId)) { return 0; }
            try
            {
                if (db.ExistTweet(StatusId)) { return 0; }
                var res = Token.Statuses.Lookup(id => StatusId, include_entities => true);
                if (res.RateLimit.Remaining < 1) { OneTweetReset = res.RateLimit.Reset.AddMinutes(1); }  //とりあえず1分延長奴
                return HandleTweet(res.First(), true, true);
            }
            catch { Console.WriteLine("{0} {1} REST Tweet failed: {2}", DateTime.Now, Token.UserId, StatusId); return 0; }
            finally { Locker.UnlockTweet(StatusId); }
        }

        void DownloadMedia(Status x)
        {
            if (x.RetweetedStatus != null) { DownloadMedia(x.RetweetedStatus); }
            foreach (MediaEntity m in x.ExtendedEntities.Media)
            {
                if (m.SourceStatusId != null && m.SourceStatusId != x.Id)
                {
                    DownloadOneTweet((long)m.SourceStatusId);
                }
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
                string LocalPaththumb = config.crawl.PictPaththumb + @"\" + m.Id.ToString() + Path.GetExtension(m.MediaUrl);  //m.Urlとm.MediaUrlは違う
                string uri = m.MediaUrl.ToString() + (m.MediaUrl.IndexOf("twimg.com") >= 0 ? ":thumb" : "");
                try
                {
                    HttpWebRequest req = (HttpWebRequest)WebRequest.Create(uri);
                    req.Referer = StatusUrl(x);
                    WebResponse res = req.GetResponse();
                    using (Stream httpStream = res.GetResponseStream())
                    using (MemoryStream mem = new MemoryStream())
                    {
                        httpStream.CopyTo(mem); //MemoryStreamはFlush不要(FlushはNOP)
                        mem.Position = 0;
                        long? dcthash = PictHash.dcthash(mem);
                        if (dcthash != null && (db.StoreMedia(m, x, (long)dcthash)) > 0)
                        {
                            using (FileStream fileStream = File.Create(LocalPaththumb))
                            {
                                mem.WriteTo(fileStream);
                                fileStream.Flush();
                            }
                        }
                    }
                }
                catch { }

                //URL転載元もペアを記録する
                if (m.SourceStatusId != null && m.SourceStatusId != x.Id)
                {
                    db.Storetweet_media((long)m.SourceStatusId, m.Id);
                }
            }
        }

        //ツイートのURLを作る
        string StatusUrl(Status x)
        {
            StringBuilder builder = new StringBuilder("https://twitter.com/");
            builder.Append(x.User.ScreenName);
            builder.Append("/status/");
            builder.Append(x.Id);
            return builder.ToString();
        }

        //とりあえずここに置くやつ
        //WebRequestを使うと勝手にプールしてくれるらしい
        bool downloadFile(string uri, string outputPath, string referer = null)
        {
            try
            {
                HttpWebRequest req = WebRequest.Create(uri) as HttpWebRequest;
                if (referer != null) { req.Referer = referer; }
                WebResponse res = req.GetResponse();

                using (FileStream fileStream = File.Create(outputPath))
                using (Stream httpStream = res.GetResponseStream())
                {
                    httpStream.CopyTo(fileStream);
                    fileStream.Flush();
                }
            }
            catch { return false; }
            return true;
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
    }
}
