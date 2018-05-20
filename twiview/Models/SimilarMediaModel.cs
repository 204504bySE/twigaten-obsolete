using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using twitenlib;
using twiview.Controllers;

namespace twiview.Models
{
    public class SimilarMediaModel
    {
        protected System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        protected DBHandlerView db = new DBHandlerView();
        public TweetData._user TargetUser { get; protected set; }
        public SimilarMediaTweet[] Tweets { get; protected set; }
        public long LastTweet { get; protected set; }
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
        public enum ActionNames { Featured, OneTweet, Timeline, UserTweet }
        public ActionNames ActionName;  //ViewでActionNameを取得する方法どこ？
    }

    public class SimilarMediaModelFeatured : SimilarMediaModel
    {
        public SimilarMediaController.FeaturedParameters p;
        public new DateTimeOffset Date;
        public TimeSpan Span { get; } = new TimeSpan(1, 0, 0);
        
        public SimilarMediaModelFeatured(SimilarMediaController.FeaturedParameters Validated, int SimilarLimit)
        {
            //BeginDate == null で最新の時刻用になる
            sw.Start();
            p = Validated;
            ActionName = ActionNames.Featured;
            this.SimilarLimit = SimilarLimit;
            DateTimeOffset? BeginDate = SimilarMediaController.StrToDateDay(p.Date);
            if (BeginDate.HasValue && BeginDate.Value + Span <= DateTimeOffset.UtcNow) { Date = BeginDate.Value; }
            else { Date = DateTimeOffset.UtcNow - Span; isLatest = true; }
            LastTweet = SnowFlake.SecondinSnowFlake((Date + Span).AddSeconds(-1),true);
            Tweets = db.SimilarMediaFeatured(SimilarLimit, SnowFlake.SecondinSnowFlake(Date,false), LastTweet, p.Order.Value);
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }

    public class SimilarMediaModelOneTweet : SimilarMediaModel
    {
        public SimilarMediaController.OneTweetParameters p;
        public bool ViewMoreButton;
        public SimilarMediaModelOneTweet(SimilarMediaController.OneTweetParameters Validated, int SimilarLimit, bool ViewMoreButton)
        {
            sw.Start();
            p = Validated;
            ActionName = ActionNames.OneTweet;
            this.SimilarLimit = SimilarLimit;
            this.ViewMoreButton = ViewMoreButton;
            Tweets = db.SimilarMediaTweet(p.TweetID, p.ID, SimilarLimit);

            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }


    public abstract class SimilarMediaModelTLUser : SimilarMediaModel
    {
        public SimilarMediaController.TLUserParameters p;
    }

    public class SimilarMediaModelTimeline : SimilarMediaModelTLUser
    {
        public SimilarMediaModelTimeline(SimilarMediaController.TLUserParameters Validated, int SimilarLimit, long? LastTweet, RangeModes RangeMode)
        {
            sw.Start();
            p = Validated;
            ActionName = ActionNames.Timeline;
            long TargetUserID = (p.UserID ?? p.ID).Value;
            TargetUser = db.SelectUser(TargetUserID);
            if (LastTweet == null) { this.LastTweet = SnowFlake.Now(true); }
            else { this.LastTweet = (long)LastTweet; }
            this.isLatest = (LastTweet == null);
            this.RangeMode = RangeMode;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaTimeline(TargetUserID, p.ID, this.LastTweet, p.Count.Value, SimilarLimit, p.RT.Value, p.Show0.Value, RangeMode != RangeModes.After);
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }

    public class SimilarMediaModelUserTweet : SimilarMediaModelTLUser
    {
        public SimilarMediaModelUserTweet(SimilarMediaController.TLUserParameters Validated, int SimilarLimit, long? LastTweet, RangeModes RangeMode)
        {
            sw.Start();
            p = Validated;
            ActionName = ActionNames.UserTweet;
            long TargetUserID = (p.UserID ?? p.ID).Value;
            TargetUser = db.SelectUser(TargetUserID);
            if (LastTweet == null) { this.LastTweet = SnowFlake.Now(true); }
            else { this.LastTweet = (long)LastTweet; }
            this.isLatest = (LastTweet == null);
            this.RangeMode = RangeMode;
            this.SimilarLimit = SimilarLimit;
            Tweets = db.SimilarMediaUser(TargetUserID, p.ID, this.LastTweet, p.Count.Value, SimilarLimit, p.RT.Value, p.Show0.Value, RangeMode != RangeModes.After);
            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }
}
