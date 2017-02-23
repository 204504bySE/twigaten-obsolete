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
        public DateTimeOffset Date { get { return SnowFlake.DatefromSnowFlake(LastTweet); } }
        public bool isNotFound { get { return TargetUser == null || Tweets.Length == 0; } }
    }

    public class SimilarMediaModelFeatured : SimilarMediaModel
    {
        public DBHandlerView.TweetOrder Order { get; }
        public SimilarMediaModelFeatured(int SimilarLimit, DateTimeOffset BeginDate, DBHandlerView.TweetOrder sortOrder)
        {
            sw.Start();
            this.SimilarLimit = SimilarLimit;
            LastTweet = SnowFlake.SecondinSnowFlake(BeginDate.AddDays(1).AddSeconds(-1),true);
            Order = sortOrder;
            Tweets = db.SimilarMediaFeatured(SimilarLimit, SnowFlake.SecondinSnowFlake(BeginDate,false), LastTweet,Order);
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
        public bool? Before { get; }
        public bool isLatest { get; }
        public long? NextOld { get; }
        public long? NextNew { get; }
        public int TweetCount { get; }
        public SimilarMediaModelTimeline(long target_user_id, long? login_user_id, int TweetCount, int SimilarLimit, long? LastTweet, bool GetRetweet, bool? Before)
        {
            sw.Start();
            TargetUser = db.SelectUser(target_user_id);
            if (LastTweet == null) { this.LastTweet = SnowFlake.Now(true); }
            else { this.LastTweet = (long)LastTweet; }
            this.isLatest = (LastTweet == null);
            this.Before = Before;
            this.TweetCount = TweetCount;
            this.GetRetweet = GetRetweet;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaTimeline(target_user_id, login_user_id, this.LastTweet, TweetCount, SimilarLimit, GetRetweet, Before ?? true);
            if (Tweets.Length > 0)
            {
                NextOld = Tweets.Last().tweet.tweet_id;
                NextNew = Tweets.First().tweet.tweet_id;
            }
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }

    public class SimilarMediaModelUserTweet : SimilarMediaModel
    {
        public bool? Before { get; }
        public bool isLatest { get; }
        public long? NextOld { get; }
        public long? NextNew { get; }
        public int TweetCount { get; }
        public SimilarMediaModelUserTweet(long target_user_id, long? login_user_id, int TweetCount, int SimilarLimit, long? LastTweet, bool GetRetweet, bool? Before)
        {
            sw.Start();
            TargetUser = db.SelectUser(target_user_id);
            if (LastTweet == null) { this.LastTweet = SnowFlake.Now(true); }
            else { this.LastTweet = (long)LastTweet; }
            this.isLatest = (LastTweet == null);
            this.Before = Before;
            this.TweetCount = TweetCount;
            this.GetRetweet = GetRetweet;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaUser(target_user_id, login_user_id, this.LastTweet, TweetCount, SimilarLimit, GetRetweet, Before ?? true);
            if (Tweets.Length > 0)
            {
                NextOld = Tweets.Last().tweet.tweet_id;
                NextNew = Tweets.First().tweet.tweet_id;
            }
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }
}
