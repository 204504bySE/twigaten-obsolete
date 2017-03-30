﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Reactive.Linq;
using CoreTweet;
using CoreTweet.Streaming;
using twitenlib;

namespace twidown
{
    class UserStreamer
    {
        // 各TokenのUserstreamを受信したり仕分けたりする

        public Exception e { get; private set; }
        public Tokens Token { get; }
        public bool NeedRestMyTweet { get; set; }   //次のconnect時にRESTでツイートを取得する
        Config config = Config.Instance;
        Counter counter = Counter.Instance;
        DBHandler db = DBHandler.Instance;
        IDisposable StreamDisposable = null;
        DateTimeOffset LastStreamingMessageTime = DateTimeOffset.Now;
        TweetTimeList TweetTime = new TweetTimeList();
        bool isAttemptingConnect = false;
        StreamerLocker Locker = StreamerLocker.Instance;

        public UserStreamer(Tokens t)
        {
            Token = t;
            Token.ConnectionOptions.DisableKeepAlive = false;
            Token.ConnectionOptions.UseCompression = true;
            Token.ConnectionOptions.UseCompressionOnStreaming = true;
        }
        ~UserStreamer()
        {
            if (StreamDisposable != null) { StreamDisposable.Dispose(); }
        }

        //最近受信したツイートの時刻を一定数保持する
        //Userstreamの場合は実際に受信した時刻を使う
        class TweetTimeList
        {
            SortedSet<DateTimeOffset> TweetTime = new SortedSet<DateTimeOffset>();
            Config config = Config.Instance;
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

        DateTimeOffset? PostponedTime = null;    //ロックされたアカウントが再試行する時刻
        public void PostponeRetry()
        {
            PostponedTime = DateTimeOffset.Now.AddSeconds(config.crawl.LockedTokenPostpone);
        }
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
            else if (!isAttemptingConnect && StreamDisposable == null) { return NeedRetryResult.JustNeeded; }
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
                if (StreamDisposable != null) { StreamDisposable.Dispose(); StreamDisposable = null; }
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
            e = null;
            if (StreamDisposable != null) { StreamDisposable.Dispose(); StreamDisposable = null; }
            LastStreamingMessageTime = DateTimeOffset.Now;
            TweetTime.Add(LastStreamingMessageTime);
            StreamDisposable = Token.Streaming.UserAsObservable().Subscribe(
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
                Timeline = Token.Statuses.HomeTimeline(count => 200, tweet_mode => TweetMode.extended);

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
                CoreTweet.Core.ListedResponse<Status> Tweets = Token.Statuses.UserTimeline(user_id => Token.UserId, count => 200, tweet_mode => TweetMode.extended);

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
                    Locker.LockDelete((x as DeleteMessage).Id);    //ここでは削除しないで後でLocker側で消す
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
            if (!locked && !Locker.LockTweet(x.Id)) { return 0; }
            int ret = 0;
            //RTを先にやる(キー制約)
            if (x.RetweetedStatus != null) { ret += HandleTweet(x.RetweetedStatus, update); }
            if (Locker.LockUser(x.User.Id))
            {
                if (update) { DownloadStoreProfileImage(x).Wait(); }
                else { db.StoreUser(x, false, false); }
            }
            int ret2;
            counter.TweetToStore.Increment();
            if ((ret2 = db.StoreTweet(x, update)) > 0)
            {
                counter.TweetStored.Increment();
                if (x.RetweetedStatus == null) { DownloadStoreMedia(x); }
            }
            if (!locked) { Locker.UnlockTweet(x.Id); }
            return ret + ret2;
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
                var res = Token.Statuses.Lookup(id => StatusId, include_entities => true, tweet_mode => TweetMode.extended);
                if (res.RateLimit.Remaining < 1) { OneTweetReset = res.RateLimit.Reset.AddMinutes(1); }  //とりあえず1分延長奴
                return HandleTweet(res.First(), true, true);
            }
            catch { Console.WriteLine("{0} {1} REST Tweet failed: {2}", DateTime.Now, Token.UserId, StatusId); return 0; }
            finally { Locker.UnlockTweet(StatusId); }
        }

        async Task DownloadStoreProfileImage(Status x)
        {
            //アイコンが更新または未保存ならダウンロードする
            //RTは自動でやらない
            //ダウンロード成功してもしなくてもそれなりにDBに反映する
            //(古い奴のURLがDBにあれば古いままになる)
            if (x.User.Id == null) { return; }  
            string ProfileImageUrl = x.User.ProfileImageUrlHttps ?? x.User.ProfileImageUrl;
            DBHandler.ProfileImageInfo d = db.NeedtoDownloadProfileImage(x.User.Id.Value, ProfileImageUrl);
            if (!d.NeedDownload || !Locker.LockProfileImage((long)x.User.Id)) { return; }

            //新しいアイコンの保存先 卵アイコンは'_'をつけただけの名前で保存するお
            string LocalPath = x.User.IsDefaultProfileImage ?
                config.crawl.PictPathProfileImage + '_' + Path.GetFileName(ProfileImageUrl) :
                config.crawl.PictPathProfileImage + x.User.Id.ToString() + Path.GetExtension(ProfileImageUrl);

            bool DownloadOK = true; //卵アイコンのダウンロード不要でもtrue
            if (!x.User.IsDefaultProfileImage || !File.Exists(LocalPath))
            { 
                try
                {
                    HttpWebRequest req = WebRequest.Create(ProfileImageUrl) as HttpWebRequest;
                    req.Referer = StatusUrl(x);
                    using (WebResponse res = await req.GetResponseAsync())
                    using (FileStream file = File.Create(LocalPath))
                    {
                        await res.GetResponseStream().CopyToAsync(file);
                    }
                }
                catch { DownloadOK = false; }
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

        async Task DownloadStoreMedia(Status x)
        {
            if (x.RetweetedStatus != null)
            {   //そもそもRTに対してこれを呼ぶべきではない
                await DownloadStoreMedia(x.RetweetedStatus);
                return;
            }
            foreach (MediaEntity m in x.ExtendedEntities.Media)
            {
                counter.MediaTotal.Increment();
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
                string MediaUrl = m.MediaUrlHttps ?? m.MediaUrl;
                string LocalPaththumb = config.crawl.PictPaththumb + m.Id.ToString() + Path.GetExtension(MediaUrl);  //m.Urlとm.MediaUrlは違う
                string uri = MediaUrl + (MediaUrl.IndexOf("twimg.com") >= 0 ? ":thumb" : "");

                try
                {
                    HttpWebRequest req = WebRequest.Create(uri) as HttpWebRequest;
                    req.Referer = StatusUrl(x);

                    using (WebResponse res = await req.GetResponseAsync())
                    using (MemoryStream mem = new MemoryStream((int)res.ContentLength))
                    {
                        await res.GetResponseStream().CopyToAsync(mem);
                        res.Close();
                        long? dcthash = PictHash.DCTHash(mem);
                        if (dcthash != null && (db.StoreMedia(m, x, (long)dcthash)) > 0)
                        {
                            using (FileStream file = File.Create(LocalPaththumb))
                            {
                                mem.Position = 0;   //こいつは必要だった
                                await mem.CopyToAsync(file);
                            }
                            counter.MediaSuccess.Increment();
                        }
                    }
                }
                catch { }
                counter.MediaToStore.Increment();
                //URL転載元もペアを記録する
                if (m.SourceStatusId != null && m.SourceStatusId != x.Id)
                {
                    db.Storetweet_media(m.SourceStatusId.Value, m.Id);
                }
            }
        }

        //ツイートのURLを作る
        string StatusUrl(Status x)
        {
            return "https://twitter.com/" + x.User.ScreenName + "/status/" + x.Id;
        }
    }
}
