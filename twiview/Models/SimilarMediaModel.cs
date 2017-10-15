using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using twitenlib;

namespace twiview.Models
{
    public class SimilarMediaModel
    {
        protected System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        protected DBHandlerView db = new DBHandlerView();
        public TweetData._user TargetUser { get; protected set; }
        public SimilarMediaTweet[] Tweets { get; protected set; }
        public long LastTweet { get; protected set; }
        public bool GetRetweet { get; protected set; }
        public long QueryElapsedMilliseconds { get; protected set; }
        public int SimilarLimit { get; protected set; }
        public DateTimeOffset Date { get { return SnowFlake.DatefromSnowFlake(LastTweet).ToLocalTime(); } }
        public bool IsNotFound { get { return Tweets.Length == 0; } }

        public enum RangeModes { None, Date, Before, After }    //なし, 日付指定, 古い, 新しい
        public RangeModes RangeMode { get; protected set; }
        public bool isLatest { get; protected set; }
        public long? NextOld { get {
                switch (RangeMode)
                {
                    case RangeModes.Date:
                    case RangeModes.Before:
                        if (Tweets.Length > 0) { return Tweets.Last().tweet.tweet_id; }
                        else { return null; }
                    case RangeModes.After:
                        if (TargetUser == null) { return null; }
                        else if (Tweets.Length == 0) { return LastTweet + 1; }
                        else { return LastTweet; }
                    default:
                        return null;
                } } }
        public long? NextNew { get {
                switch (RangeMode)
                {
                    case RangeModes.Date:
                    case RangeModes.After:
                        if (Tweets.Length > 0) { return Tweets.First().tweet.tweet_id; }
                        else { return null; }
                    case RangeModes.Before:
                        if (TargetUser == null) { return null; }
                        else if (Tweets.Length == 0) { return LastTweet - 1; }
                        else { return LastTweet; }
                    default:
                        return null;
                } } }
        public int TweetCount { get; protected set; }   //実際の個数じゃなくて件数指定
        public enum ActionNames { Featured, OneTweet, Timeline, UserTweet }
        public ActionNames ActionName;  //ViewでActionNameを取得する方法どこ？
    }

    public class SimilarMediaModelFeatured : SimilarMediaModel
    {
        public DBHandlerView.TweetOrder Order { get; }
        public new DateTimeOffset Date { get; }
        public TimeSpan Span { get; } = new TimeSpan(1, 0, 0);
        
        public SimilarMediaModelFeatured(int SimilarLimit, DateTimeOffset? BeginDate, DBHandlerView.TweetOrder sortOrder)
        {
            //BeginDate == null で最新の時刻用になる
            sw.Start();
            ActionName = ActionNames.Featured;
            this.SimilarLimit = SimilarLimit;
            if (BeginDate.HasValue && BeginDate.Value + Span <= DateTimeOffset.UtcNow) { Date = BeginDate.Value; }
            else { Date = DateTimeOffset.UtcNow - Span; isLatest = true; }
            LastTweet = SnowFlake.SecondinSnowFlake((Date + Span).AddSeconds(-1),true);
            Order = sortOrder;
            Tweets = db.SimilarMediaFeatured(SimilarLimit, SnowFlake.SecondinSnowFlake(Date,false), LastTweet,Order);
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }

    public class SimilarMediaModelOneTweet : SimilarMediaModel
    {
        public long TargetTweetID;
        public bool ViewMoreButton;
        public SimilarMediaModelOneTweet(long tweet_id, long? login_user_id, int SimilarLimit, bool ViewMoreButton)
        {
            sw.Start();
            ActionName = ActionNames.OneTweet;
            TargetTweetID = tweet_id;
            this.SimilarLimit = SimilarLimit;
            this.ViewMoreButton = ViewMoreButton;
            Tweets = db.SimilarMediaTweet(tweet_id, login_user_id, SimilarLimit);

            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }


    public class SimilarMediaModelTimeline : SimilarMediaModel
    {
        public SimilarMediaModelTimeline(long target_user_id, long? login_user_id, int TweetCount, int SimilarLimit, long? LastTweet, bool GetRetweet, RangeModes RangeMode)
        {
            sw.Start();
            ActionName = ActionNames.Timeline;
            TargetUser = db.SelectUser(target_user_id);
            if (LastTweet == null) { this.LastTweet = SnowFlake.Now(true); }
            else { this.LastTweet = (long)LastTweet; }
            this.isLatest = (LastTweet == null);
            this.RangeMode = RangeMode;
            this.TweetCount = TweetCount;
            this.GetRetweet = GetRetweet;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaTimeline(target_user_id, login_user_id, this.LastTweet, TweetCount, SimilarLimit, GetRetweet, RangeMode != RangeModes.After);
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }

    public class SimilarMediaModelUserTweet : SimilarMediaModel
    {
        public SimilarMediaModelUserTweet(long target_user_id, long? login_user_id, int TweetCount, int SimilarLimit, long? LastTweet, bool GetRetweet, RangeModes RangeMode)
        {
            sw.Start();
            ActionName = ActionNames.UserTweet;
            TargetUser = db.SelectUser(target_user_id);
            if (LastTweet == null) { this.LastTweet = SnowFlake.Now(true); }
            else { this.LastTweet = (long)LastTweet; }
            this.isLatest = (LastTweet == null);
            this.RangeMode = RangeMode;
            this.TweetCount = TweetCount;
            this.GetRetweet = GetRetweet;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaUser(target_user_id, login_user_id, this.LastTweet, TweetCount, SimilarLimit, GetRetweet, RangeMode != RangeModes.After);
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }
}
