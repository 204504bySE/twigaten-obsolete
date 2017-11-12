using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CoreTweet;
using CoreTweet.Streaming;
using twitenlib;

namespace twidown
{
    class UserStreamerStatic
    {
        //Singleton
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

            static UserStreamerStatic()
            {
                DeleteTweetBatch.LinkTo(DeleteTweetBlock, new DataflowLinkOptions { PropagateCompletion = true });
                TweetDistinctBlock.LinkTo(HandleTweetBlock, new DataflowLinkOptions { PropagateCompletion = true });
                Udp.Client.ReceiveTimeout = 100;
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

            static HashSet<long> TweetLock = new HashSet<long>();
            static UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, (config.crawl.ParentUdpPort ^ (System.Diagnostics.Process.GetCurrentProcess().Id & 0x3FFF)))) { DontFragment = true };
            static IPEndPoint ParentEndPoint = new IPEndPoint(IPAddress.Loopback, config.crawl.ParentUdpPort);
            static bool LockTweet(long tweet_id)
            {
            //雑にプロセス内でもLockしておく
            if (!TweetLock.Add(tweet_id)) { return false; }
            if (TweetLock.Count >= config.crawl.TweetLockSize) { TweetLock.Clear(); }
                //twidownparentでもLockを確認する
                try
                {
                    Udp.Send(BitConverter.GetBytes(tweet_id), sizeof(long), ParentEndPoint);
                    IPEndPoint RemoteUdp = null;
                    return BitConverter.ToBoolean(Udp.Receive(ref RemoteUdp), 0);
                }
                catch { return false; }
            }
            static TransformBlock<Tuple<Status, Tokens, bool>, Tuple<Status, Tokens, bool>> TweetDistinctBlock
                = new TransformBlock<Tuple<Status, Tokens, bool>, Tuple<Status, Tokens, bool>>(x =>
                {   //ここでLockする(1スレッドなのでHashSetでおｋ

                    if (LockTweet(x.Item1.Id)) { return x; }
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
                if ((x.ExtendedEntities ?? x.Entities)?.Media == null) { return; } //画像なしツイートを捨てる
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
                    for (int RetryCount = 0; RetryCount < 2; RetryCount++)
                    {
                        try
                        {
                            HttpWebRequest req = WebRequest.Create(ProfileImageUrl) as HttpWebRequest;
                            req.Referer = StatusUrl(x);
                            using (WebResponse res = req.GetResponse())
                            using (FileStream file = File.Create(LocalPath))
                            {
                                res.GetResponseStream().CopyTo(file);
                            }
                            break;
                        }
                        catch (WebException we)
                        {   //404等じゃなければ1回だけリトライする
                            if (we.Status == WebExceptionStatus.ProtocolError)  { DownloadOK = false; break; }
                        else { continue; }
                        }
                        catch { DownloadOK = false; break; }
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

            static ActionBlock<KeyValuePair<Status, Tokens>> DownloadStoreMediaBlock = new ActionBlock<KeyValuePair<Status, Tokens>>(a =>
            {
                Status x = a.Key;
                Tokens t = a.Value;
                Lazy<HashSet<long>> RestId = new Lazy<HashSet<long>>();   //同じツイートを何度も処理したくない
                foreach (MediaEntity m in x.ExtendedEntities.Media ?? x.Entities.Media)
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
                    HandleTweet(res.First(), Token, true);
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
            //LockedTweetsClearFlag = true;
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


