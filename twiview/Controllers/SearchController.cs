using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Text;
using System.Text.RegularExpressions;
using twiview.Models;

namespace twiview.Controllers
{
    public class SearchController : Controller
    {
        DBHandlerView db = new DBHandlerView();
        Regex StatusRegex = new Regex(@"(?<=twitter\.com\/.+?\/status(es)?\/)\d+", RegexOptions.Compiled);
        Regex ScreenNameRegex = new Regex(@"(?<=twitter\.com\/|@|^)[_\w]+(?=[\/_\w]*$)", RegexOptions.Compiled);


        public class SearchParameters : LoginParameters
        {
            ///<summary>URL</summary>
            public string Str { get; set; }
            ///<summary>URL UserLikeMode/Cookie</summary>
            public DBHandlerView.SelectUserLikeMode? Mode { get; set; }
            ///<summary>Cookie(Session only)</summary>
            public DBHandlerView.SelectUserLikeMode? UserLikeMode { get; set; }
            ///<summary>URL</summary>
            public bool? Direct { get; set; }
            ///<summary>URL</summary>
            public HttpPostedFileWrapper File { get; set; }

            protected override void ValidateValues(HttpResponseBase Response)
            {
                Str = twitenlib.CharCodes.KillNonASCII(Str);
                UserLikeMode = Mode = Mode ?? UserLikeMode ?? DBHandlerView.SelectUserLikeMode.Show;
                SetCookie(nameof(UserLikeMode), UserLikeMode.ToString(), Response, true);
                Direct = Direct ?? true;
            }
        }
        [Route("search")]
        public ActionResult Index(SearchParameters p)
        {
            p.Validate(Session, Response);
            if (p.Str == null || p.Str == "") { return View(); }

            //ツイートのURLっぽいならそのツイートのページに飛ばす
            string StatusStr = StatusRegex.Match(p.Str).Value;
            if (StatusStr != "")
            {
                long StatusID = long.Parse(StatusStr);
                return RedirectToRoute(new
                {
                    controller = "SimilarMedia",
                    action = "OneTweet",
                    TweetID = db.SourceTweetRT(StatusID) ?? StatusID   //RTなら元ツイートに飛ばす
            });
            }
            //ユーザー名検索
            string ScreenName = ScreenNameRegex.Match(p.Str).Value;
            if (ScreenName != "")
            {
                if (p.Direct.Value)
                {
                    long? TargetUserID = db.SelectID_Unique_screen_name(ScreenName);
                    if (TargetUserID != null)
                    {
                        return RedirectToRoute(new { controller = "SimilarMedia", action = "UserTweet", UserID = TargetUserID });
                    }
                }
                return RedirectToAction("Users", new { Str = p.Str, Mode = p.Mode });
            }
            else { return RedirectToAction("Index"); }
        }
        
        [Route("search/media")]
        public ActionResult Media(SearchParameters p)
        {
            p.Validate(Session, Response);
            long? hash = twidown.PictHash.DCTHash(p.File?.InputStream, true); 
            if(hash == null) { return View(new SearchModelMedia(SearchModelMedia.FailureType.HashFail)); }
            (long tweet_id, long media_id)? MatchMedia = db.HashtoTweet(hash, p.ID);
            if(MatchMedia == null) { return View(new SearchModelMedia(SearchModelMedia.FailureType.NoTweet)); }
            //その画像を含む最も古いツイートにリダイレクト
            return Redirect(Url.RouteUrl(new { controller = "SimilarMedia", action = "OneTweet", TweetID = MatchMedia.Value.tweet_id }) + "#" + MatchMedia.Value.media_id.ToString());
        }
        

        [Route("search/users")]
        public ActionResult Users(SearchParameters p)
        {
            p.Validate(Session, Response);
            //screen_name 検索
            if(p.Str == null || p.Str == "") { return RedirectToAction("Index"); }
            return View(new SearchModelUsers(p.Str, p.ID, p.Mode.Value));
        }
    }
}