using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using twiview.Models;
using twitenlib;
namespace twiview.Controllers
{
    public class SimilarMediaController : Controller
    {
        LoginHandler Login;
        public ActionResult Index()
        {
            return RedirectToAction("Featured", new { Date = DateTimeOffset.Now.ToString("yyyy-MM-dd") });
        }

        [Route("featured/{Date?}")]
        public ActionResult Featured(string Date, DBHandlerView.TweetOrder? Order)
        {
            Login = new LoginHandler(Session, Request, Response);
            DateTimeOffset? DateOffset = StrToDateDay(Date);

            return View(new SimilarMediaModelFeatured(3, DateOffset ?? new DateTimeOffset(DateTimeOffset.Now.Year, DateTimeOffset.Now.Month, DateTimeOffset.Now.Day, 0, 0, 0, new TimeSpan(0)), getTweetOrderPref(Order)));
        }

        [Route("tweet/{TweetID:long}")]
        public ActionResult OneTweet(long TweetID, bool? More)
        {
            Login = new LoginHandler(Session, Request, Response);
            long? SourceTweetID = new DBHandlerView().SourceTweetRT(TweetID);

            //こっちでリダイレクトされるのは直リンだけ(検索側でもリダイレクトする)
            if (SourceTweetID != null) { return RedirectToActionPermanent("OneTweet", new { TweetID = SourceTweetID, More = More }); }

            SimilarMediaModelOneTweet Model;
            if (More ?? false) { Model = new SimilarMediaModelOneTweet(TweetID, Login.UserID, 100, false); }
            else { Model = new SimilarMediaModelOneTweet(TweetID, Login.UserID, 7, true); }
            if(Model.isNotFound) { Response.StatusCode = 404; }
            return View(Model);
        }

        [Route("timeline/{UserID:long?}")]
        public ActionResult Timeline(long? UserID, string Date, int? Count, bool? RT, long? Before, long? After)
        {
            Login = new LoginHandler(Session, Request, Response);
            if (UserID == null && Login.UserID == null) { throw (new ArgumentNullException()); }
            if (UserID == null)
            {
                if (Date == null) { return RedirectToAction("Timeline", new { UserID = UserID ?? Login.UserID, Count = getCountPref(Count), RT = getRetweetPref(RT), Before = Before }); }
                else { return RedirectToAction("Timeline", new { UserID = UserID ?? Login.UserID, Date = Date, Count = getCountPref(Count), RT = getRetweetPref(RT), Before = Before }); }
            }

            long? LastTweet;
            SimilarMediaModel.RangeModes RangeMode;
            if (Before != null) { LastTweet = Before; RangeMode = SimilarMediaModel.RangeModes.Before; }
            else if (After != null) { LastTweet = After; RangeMode = SimilarMediaModel.RangeModes.After; }
            else { LastTweet = null; RangeMode = SimilarMediaModel.RangeModes.Date; }
            DateTimeOffset ParsedDate;
            if (LastTweet != null) { LastTweet = Math.Min(LastTweet.Value, SnowFlake.Now(true)); }
            else if (Date != null && DateTimeOffset.TryParse(Date, out ParsedDate))
            {
                if (ParsedDate <= DateTimeOffset.UtcNow)
                { LastTweet = SnowFlake.SecondinSnowFlake(ParsedDate, true); }
            }

            SimilarMediaModel Model = new SimilarMediaModelTimeline((long)(UserID ?? Login.UserID), Login.UserID, getCountPref(Count), 3, LastTweet, getRetweetPref(RT), RangeMode);
            if (Model.isNotFound) { Response.StatusCode = 404; }
            return View("TLUser", Model);
        }

        [Route("users/{UserID:long?}")]
        public ActionResult UserTweet(long? UserID, string Date, int? Count, bool? RT, long? Before, long? After)
        {
            //Before == nullは日付指定
            Login = new LoginHandler(Session, Request, Response);

            if (UserID == null && Login.UserID == null) { throw new ArgumentNullException(); }
            if (UserID == null) { return RedirectToAction("UserTweet", new { UserID = UserID ?? Login.UserID }); }

            long? LastTweet;
            SimilarMediaModel.RangeModes RangeMode;
            if (Before != null) { LastTweet = Before; RangeMode = SimilarMediaModel.RangeModes.Before; }
            else if (After != null) { LastTweet = After; RangeMode = SimilarMediaModel.RangeModes.After; }
            else { LastTweet = null; RangeMode = SimilarMediaModel.RangeModes.Date; }
            DateTimeOffset ParsedDate;
            if (LastTweet != null) { LastTweet = Math.Min(LastTweet.Value, SnowFlake.Now(true)); }
            else if (Date != null && DateTimeOffset.TryParse(Date, out ParsedDate))
            {
                if (ParsedDate <= DateTimeOffset.UtcNow)
                { LastTweet = SnowFlake.SecondinSnowFlake(ParsedDate, true); }
            }

            SimilarMediaModel Model = new SimilarMediaModelUserTweet((long)UserID, Login.UserID, getCountPref(Count), 3, LastTweet, getRetweetPref(RT), RangeMode);
            if (Model.isNotFound) { Response.StatusCode = 404; }
            return View("TLUser", Model);
        }

        [Route("SimilarMedia/{ActionName}")]
        public ActionResult Redirecter(string ActionName, long? TweetID, long? UserID, string Date, int? Count, bool? RT, long? Before, long? After)
        {
            return RedirectToActionPermanent(ActionName, "SimilarMedia", new { TweetID = TweetID, UserID = UserID, Date=Date,Count = Count, RT =RT,Before= Before, After= After });
        }
        
        int getCountPref(int? QueryPref)
        {
            int? ret = Login.getCookiePref(QueryPref, 10, "TweetCount");
            if(ret > 50) { return 50; } //URL直接入力でも最大数を制限
            if(ret < 10) { return 10; }
            return (int)ret;
        }

        bool getRetweetPref(bool? QueryPref)
        {
            return (bool)Login.getCookiePref(QueryPref, true, "GetRetweet");
        }

        DBHandlerView.TweetOrder getTweetOrderPref(DBHandlerView.TweetOrder? QueryPref)
        {
            return (DBHandlerView.TweetOrder)Login.getCookiePref(QueryPref, DBHandlerView.TweetOrder.Featured, "TweetOrder");
        }

        //"yyyy-MM-dd" を変換する 失敗したらnull
        DateTimeOffset? StrToDateDay(string DateStr)
        {
            if (DateStr == null) { return null; }
            try
            {
                string[] SplitDate = DateStr.Split('-');
                //UTC+9の0時にする
                return new DateTimeOffset(int.Parse(SplitDate[0]), int.Parse(SplitDate[1]), int.Parse(SplitDate[2]), 0, 0, 0, new TimeSpan(9, 0, 0));
            }
            catch { return null; }
        }
    }
}