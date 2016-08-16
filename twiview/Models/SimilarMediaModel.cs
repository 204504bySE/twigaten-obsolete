using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace twiview.Models
{
    public class SimilarMediaModel
    {
        protected System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        protected DBHandlerView db = new DBHandlerView();
        public TweetData._user TargetUser { get; protected set; }
        public SimilarMediaTweet[] Tweets { get; protected set; }
        public DateTimeOffset Date { get; protected set; }
        public bool GetRetweet { get; protected set; }
        public long QueryElapsedMilliseconds { get; protected set; }
        public int SimilarLimit { get; protected set; }
    }

    public class SimilarMediaModelFeatured : SimilarMediaModel
    {
        public DBHandlerView.TweetOrder Order { get; }
        public SimilarMediaModelFeatured(int SimilarLimit, DateTimeOffset BeginDate, DBHandlerView.TweetOrder sortOrder)
        {
            sw.Start();
            this.SimilarLimit = SimilarLimit;
            Date = BeginDate;
            Order = sortOrder;
            Tweets = db.SimilarMediaFeatured(SimilarLimit, BeginDate, BeginDate.AddDays(1),Order);
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }

    public class SimilarMediaModelOneTweet : SimilarMediaModel
    {
        public long TargetTweetID;
        public SimilarMediaModelOneTweet(long tweet_id, long? login_user_id, int SimilarLimit)
        {
            sw.Start();
            TargetTweetID = tweet_id;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaTweet(tweet_id, login_user_id, SimilarLimit);

            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }

    public class SimilarMediaModelTimeline : SimilarMediaModel
    {
        public bool? Before { get; }
        public bool isLatest { get; }
        public DateTimeOffset? NextOld { get; }
        public DateTimeOffset? NextNew { get; }
        public int TweetCount { get; }
        public SimilarMediaModelTimeline(long target_user_id, long? login_user_id, int TweetCount, int SimilarLimit, DateTimeOffset? _Date, bool GetRetweet, bool? Before)
        {
            sw.Start();
            TargetUser = db.SelectUser(target_user_id);
            this.isLatest = (_Date == null);
            if(Before == null)
            {
                if (_Date == null || _Date.Value.AddDays(1) > DateTimeOffset.Now) { Date = DateTimeOffset.Now; }
                else { Date = _Date.Value.AddDays(1).AddSeconds(-1); }
            }
            else
            {
                Date = _Date.Value;
            }
            this.Before = Before;
            this.TweetCount = TweetCount;
            this.GetRetweet = GetRetweet;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaTimeline(target_user_id, login_user_id, Date, TweetCount, SimilarLimit, GetRetweet, Before ?? true);
            if (Tweets.Length > 0)
            {
                NextOld = Tweets.Last().tweet.created_at;
                NextNew = Tweets.First().tweet.created_at;
            }
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }

    public class SimilarMediaModelUserTweet : SimilarMediaModel
    {
        public bool? Before { get; }
        public bool isLatest { get; }
        public DateTimeOffset? NextOld { get; }
        public DateTimeOffset? NextNew { get; }
        public int TweetCount { get; }
        public SimilarMediaModelUserTweet(long target_user_id, long? login_user_id, int TweetCount, int SimilarLimit, DateTimeOffset? _Date, bool GetRetweet, bool? Before)
        {
            sw.Start();
            TargetUser = db.SelectUser(target_user_id);
            if (Before == null)
            {
                if (_Date == null) { this.Date = DateTimeOffset.Now; }
                else
                {
                    this.Date = ((DateTimeOffset)_Date).AddDays(1).AddSeconds(-1);
                    if (this.Date > DateTimeOffset.Now) { this.Date = DateTimeOffset.Now; }
                }
            }
            else { this.Date = (DateTimeOffset)_Date; }
            this.isLatest = (_Date == null);
            this.Before = Before;
            this.TweetCount = TweetCount;
            this.GetRetweet = GetRetweet;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaUser(target_user_id, login_user_id, Date, TweetCount, SimilarLimit, GetRetweet, Before ?? true);
            if (Tweets.Length > 0)
            {
                NextOld = Tweets.Last().tweet.created_at;
                NextNew = Tweets.First().tweet.created_at;
            }
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }
}
