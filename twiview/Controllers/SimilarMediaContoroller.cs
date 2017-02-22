using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using twiview.Models;
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
            else { Model = new SimilarMediaModelOneTweet(TweetID, Login.UserID, 3, true); }
            if(Model.isNotFound) { Response.StatusCode = 404; }
            return View(Model);
        }

        [Route("timeline/{UserID:long?}")]
        public ActionResult Timeline(long? UserID, string Date, int? Count, bool? RT, long? Before, long? After)
        {
            Login = new LoginHandler(Session, Request, Response);
            if (UserID == null && Login.UserID == null) { throw (new ArgumentNullException()); }

            long? Time = Before ?? After;
            DateTimeOffset? DateOffset = (Time == null ? DateOffset = StrToDateDay(Date) : DateTimeOffset.FromUnixTimeSeconds((long)Time));

            bool? isBefore;
            if (Before != null) { isBefore = true; }
            else if (After != null) { isBefore = false; }
            else { isBefore = null; }

            if (UserID == null)
            {
                if (Date == null) { return RedirectToAction("Timeline", new { UserID = UserID ?? Login.UserID, Count = getCountPref(Count), RT = getRetweetPref(RT), Before = Before }); }
                else { return RedirectToAction("Timeline", new { UserID = UserID ?? Login.UserID, Date = ((DateTimeOffset)DateOffset).ToString("yyyy-MM-dd"), Count = getCountPref(Count), RT = getRetweetPref(RT), Before = Before }); }
            }
            SimilarMediaModelTimeline Model = new SimilarMediaModelTimeline((long)(UserID ?? Login.UserID), Login.UserID, getCountPref(Count), 3, DateOffset, getRetweetPref(RT), isBefore);
            if (Model.isNotFound) { Response.StatusCode = 404; }
            return View(Model);
        }

        [Route("users/{UserID:long?}")]
        public ActionResult UserTweet(long? UserID, string Date, int? Count, bool? RT, long? Before, long? After)
        {
            //Before == nullは日付指定
            Login = new LoginHandler(Session, Request, Response);

            if (UserID == null && Login.UserID == null) { throw new ArgumentNullException(); }
            if (UserID == null) { return RedirectToAction("UserTweet", new { UserID = UserID ?? Login.UserID }); }

            long? Time = Before ?? After;
            bool? isBefore;
            if (Before != null) { isBefore = true; }
            else if (After != null) { isBefore = false; }
            else { isBefore = null; }

            DateTimeOffset? DateOffset = (Time == null ? DateOffset = StrToDateDay(Date) : DateTimeOffset.FromUnixTimeSeconds((long)Time));
            SimilarMediaModelUserTweet Model = new SimilarMediaModelUserTweet((long)UserID, Login.UserID, getCountPref(Count), 3, DateOffset, getRetweetPref(RT), isBefore);
            if (Model.isNotFound) { Response.StatusCode = 404; }
            return View(Model);
        }

        [Route("SimilarMedia/{ActionName}")]
        public ActionResult Redirecter(string ActionName, long? TweetID, long? UserID, string Date, int? Count, bool? RT, long? Before, long? After)
        {
            return RedirectToActionPermanent(ActionName, "SimilarMedia", new { TweetID = TweetID, UserID = UserID, Date=Date,Count = Count, RT =RT,Before= Before, After= After });
        }
        
        int getCountPref(int? QueryPref)
        {
            int? ret = Login.getCookiePref(QueryPref, 20, "TweetCount");
            if(ret > 50) { return 50; } //URL直接入力でも最大数を制限
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
                return new DateTimeOffset(int.Parse(SplitDate[0]), int.Parse(SplitDate[1]), int.Parse(SplitDate[2]), 0, 0, 0, new TimeSpan(9, 0, 0));
            }
            catch { return null; }
        }
    }
}