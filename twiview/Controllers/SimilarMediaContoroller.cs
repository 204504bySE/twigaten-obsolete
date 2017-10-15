using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Text.RegularExpressions;
using twiview;
using twiview.Models;
using twitenlib;
namespace twiview.Controllers
{
    public class SimilarMediaController : Controller
    {
        public abstract class SimilarMediaParameters : LoginParameters
        {
            ///<summary>URL</summary>
            public long? UserID { get; set; }
            ///<summary>URL (Format varies)</summary>
            public string Date { get; set; }
        }
        
        public ActionResult Index()
        {   //誰がアクセスするんだこんなもん
            return RedirectToAction(nameof(Featured), new { Date = DateTimeOffset.Now.ToString("yyyy-MM-dd") });
        }

        public class FeaturedParameters : SimilarMediaParameters
        {
            ///<summary>URL TweetOrder/Cookie</summary>
            public DBHandlerView.TweetOrder? Order { get; set; }

            ///<summary>Cookie</summary>
            public DBHandlerView.TweetOrder? TweetOrder { get; set; }

            protected override void ValidateValues(HttpResponseBase Response)
            {
                TweetOrder = Order = Order ?? TweetOrder ?? DBHandlerView.TweetOrder.Featured;
                SetCookie(nameof(TweetOrder), TweetOrder.ToString(), Response);
            }
        }
        [Route("featured/{Date?}")]
        public ActionResult Featured(FeaturedParameters p)
        {
            p.Validate(Session, Response);
            DateTimeOffset? Date = StrToDateDay(p.Date);
            if (p.Date != null && (!Date.HasValue || Date.Value > DateTimeOffset.UtcNow)) { return RedirectToAction(nameof(Featured)); }
            return View(new SimilarMediaModelFeatured(3, Date, p.Order.Value));
        }

        public class OneTweetParameters : SimilarMediaParameters
        {
            ///<summary>URL</summary>
            public long TweetID { get; set; }
            ///<summary>URL</summary>
            public bool? More { get; set; }
            protected override void ValidateValues(HttpResponseBase Response)
            {
                More = More ?? false;
            }
        }
        [Route("tweet/{TweetID:long}")]
        public ActionResult OneTweet(OneTweetParameters p)
        {
            p.Validate(Session, Response);
            //こっちでリダイレクトされるのは直リンだけ(検索側でもリダイレクトする)
            long? SourceTweetID = new DBHandlerView().SourceTweetRT(p.TweetID);
            if (SourceTweetID != null) { return RedirectToActionPermanent(nameof(OneTweet), new { TweetID = SourceTweetID, More = p.More }); }

            SimilarMediaModelOneTweet Model;
            if (p.More.Value) { Model = new SimilarMediaModelOneTweet(p.TweetID, p.ID, 100, false); }
            else { Model = new SimilarMediaModelOneTweet(p.TweetID, p.ID, 7, true); }
            if(Model.IsNotFound) { Response.StatusCode = 404; }
            return View(Model);
        }

        public class TLUserParameters : SimilarMediaParameters
        {
            ///<summary>URL TweetCount/Cookie</summary>
            public int? Count { get; set; }
            ///<summary>URL GetRetweet/Cookie</summary>
            public bool? RT { get; set; }
            ///<summary>URL</summary>
            public long? Before { get; set; }
            ///<summary>URL</summary>
            public long? After { get; set; }

            ///<summary>Cookie</summary>
            public int? TweetCount { get; set; }
            ///<summary>Cookie</summary>
            public bool? GetRetweet { get; set; }

            protected override void ValidateValues(HttpResponseBase Response)
            {
                Count = Count ?? TweetCount ?? 10;
                if (Count > 50) { Count = 50; }
                if (Count < 10) { Count = 10; }
                TweetCount = Count;
                SetCookie(nameof(TweetCount), TweetCount.ToString(), Response);

                GetRetweet = RT = RT ?? GetRetweet ?? true;
                SetCookie(nameof(GetRetweet), GetRetweet.ToString(), Response);
            }
        }
        readonly Regex OldDateRegex = new Regex(@"^\d{4}-\d{2}-\d{2}$", RegexOptions.Compiled);
        [Route("timeline/{UserID:long?}")]
        public ActionResult Timeline(TLUserParameters p)
        {
            p.Validate(Session, Response);
            if (p.UserID == null && p.ID == null) { throw (new ArgumentNullException()); }
            if (p.UserID == null)
            {
                if (p.Date == null) { return RedirectToAction(nameof(Timeline), new { UserID = p.UserID ?? p.ID, Count = p.Count, RT = p.RT, Before = p.Before }); }
                else { return RedirectToAction(nameof(Timeline), new { UserID = p.UserID ?? p.ID, Date = p.Date, Count = p.Count, RT = p.RT, Before = p.Before }); }
            }

            if (p.Date != null && OldDateRegex.IsMatch(p.Date))
            {   //旧式Dateのリダイレクト(当分の間やっておこう
                return RedirectToActionPermanent(nameof(Timeline), new
                {
                    UserID = p.UserID,
                    Date = DateTime.Parse(p.Date).AddDays(1).AddSeconds(-1).ToString("yyyy/MM/dd HH:mm:ss")
                });
            }
            long? LastTweet;
            SimilarMediaModel.RangeModes RangeMode;
            if (p.Before != null) { LastTweet = p.Before; RangeMode = SimilarMediaModel.RangeModes.Before; }
            else if (p.After != null) { LastTweet = p.After; RangeMode = SimilarMediaModel.RangeModes.After; }
            else { LastTweet = null; RangeMode = SimilarMediaModel.RangeModes.Date; }
            DateTimeOffset ParsedDate;
            if (LastTweet != null) { LastTweet = Math.Min(LastTweet.Value, SnowFlake.Now(true)); }
            else if (p.Date != null && DateTimeOffset.TryParse(p.Date, out ParsedDate))
            {
                if (ParsedDate <= DateTimeOffset.UtcNow)
                { LastTweet = SnowFlake.SecondinSnowFlake(ParsedDate, true); }
            }

            SimilarMediaModel Model = new SimilarMediaModelTimeline((long)(p.UserID ?? p.ID), p.ID, p.Count.Value, 3, LastTweet, p.RT.Value, RangeMode);
            if (Model.IsNotFound) { Response.StatusCode = 404; }
            return View("TLUser", Model);
        }
        [Route("users/{UserID:long?}")]
        public ActionResult UserTweet(TLUserParameters p)
        {
            p.Validate(Session, Response);
            if (p.UserID == null && p.ID == null) { throw (new ArgumentNullException()); }
            if (p.UserID == null)
            {
                if (p.Date == null) { return RedirectToAction(nameof(UserTweet), new { UserID = p.UserID ?? p.ID, Count = p.Count, RT = p.RT, Before = p.Before }); }
                else { return RedirectToAction(nameof(UserTweet), new { UserID = p.UserID ?? p.ID, Date = p.Date, Count = p.Count, RT = p.RT, Before = p.Before }); }
            }

            if (p.Date != null && OldDateRegex.IsMatch(p.Date))
            {   //旧式Dateのリダイレクト(当分の間やっておこう
                return RedirectToActionPermanent(nameof(UserTweet), new
                {
                    UserID = p.UserID,
                    Date = DateTime.Parse(p.Date).AddDays(1).AddSeconds(-1).ToString("yyyy/MM/dd HH:mm:ss")
                });
            }
            long? LastTweet;
            SimilarMediaModel.RangeModes RangeMode;
            if (p.Before != null) { LastTweet = p.Before; RangeMode = SimilarMediaModel.RangeModes.Before; }
            else if (p.After != null) { LastTweet = p.After; RangeMode = SimilarMediaModel.RangeModes.After; }
            else { LastTweet = null; RangeMode = SimilarMediaModel.RangeModes.Date; }
            DateTimeOffset ParsedDate;
            if (LastTweet != null) { LastTweet = Math.Min(LastTweet.Value, SnowFlake.Now(true)); }
            else if (p.Date != null && DateTimeOffset.TryParse(p.Date, out ParsedDate))
            {
                if (ParsedDate <= DateTimeOffset.UtcNow)
                { LastTweet = SnowFlake.SecondinSnowFlake(ParsedDate, true); }
            }

            SimilarMediaModel Model = new SimilarMediaModelUserTweet(p.UserID.Value, p.ID, p.Count.Value, 3, LastTweet, p.RT.Value, RangeMode);
            if (Model.IsNotFound) { Response.StatusCode = 404; }
            return View("TLUser", Model);
        }

        //最初期のURLをリダイレクト もういらない？
        [Route("SimilarMedia/{ActionName}")]
        public ActionResult Redirecter(string ActionName, long? TweetID, long? UserID, string Date, int? Count, bool? RT, long? Before, long? After)
        {
            return RedirectToActionPermanent(ActionName, "SimilarMedia", new { TweetID = TweetID, UserID = UserID, Date=Date,Count = Count, RT =RT,Before= Before, After= After });
        }

        //"yyyy-MM-dd-HH" を変換する 失敗したらnull
        DateTimeOffset? StrToDateDay(string DateStr)
        {
            if (DateStr == null) { return null; }
            try
            {
                string[] SplitDate = DateStr.Split('-');
                if(SplitDate.Length < 4) { return null; }
                //UTC+9の0時にする
                return new DateTimeOffset(int.Parse(SplitDate[0]), int.Parse(SplitDate[1]), int.Parse(SplitDate[2]), int.Parse(SplitDate[3]),
                    0, 0, TimeZoneInfo.Local.BaseUtcOffset);                 
            }
            catch {
                return null;
            }
        }
    }
}