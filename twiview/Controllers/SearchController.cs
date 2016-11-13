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
        Regex StatusRegex = new Regex(@"twitter\.com\/.+?\/status(es)?\/(?<status_id>[0-9]+)", RegexOptions.Compiled);

        public ActionResult Index(string Str, DBHandlerView.SelectUserLikeMode Mode = DBHandlerView.SelectUserLikeMode.Show, bool Direct = true)
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            string QueryStr = twitenlib.CharCodes.KillNonASCII(Str);
            if (Str == null || Str == "") { return View(); }

            //ツイートのURLっぽいならそのツイートのページに飛ばす
            if (StatusRegex.IsMatch(QueryStr))
            {
                return RedirectToRoute(new
                {
                    controller = "SimilarMedia",
                    action = "OneTweet",
                    TweetID = StatusRegex.Match(QueryStr).Groups["status_id"]
                });
            }
            else
            {   //ツイートのURLっぽくなければユーザー名検索とみなす
                if (Direct)
                {
                    long? TargetUserID = db.SelectID_Unique_screen_name(QueryStr.Trim().Replace("@", "").Replace("%", ""));
                    if (TargetUserID != null)
                    {
                        return RedirectToRoute(new { controller = "SimilarMedia", action = "UserTweet", UserID = TargetUserID });
                    }
                }
                return RedirectToAction("Users", new { Str = QueryStr, Mode = Mode });
            }
        }

        public ActionResult Media(HttpPostedFileWrapper File)
        {
            long? hash = null;
            if (File != null) { hash = twidown.PictHash.dcthash(File.InputStream, true); }
            if(hash == null) { return View(new SearchModelMedia(SearchModelMedia.FailureType.HashFail)); }
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            long? tweet_id = db.HashtoTweet(hash, Login.UserID);
            if(tweet_id == null) { return View(new SearchModelMedia(SearchModelMedia.FailureType.NoTweet)); }
            //その画像を含む最も古いツイートにリダイレクト
            return RedirectToRoute(new { controller = "SimilarMedia", action = "OneTweet", TweetID = tweet_id });
        }

        public ActionResult Users(string Str, DBHandlerView.SelectUserLikeMode Mode = DBHandlerView.SelectUserLikeMode.Undefined)
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            //screen_name 検索
            string QueryStr = twitenlib.CharCodes.KillNonASCII(Str);
            if(QueryStr == null || QueryStr == "")
            {
                return RedirectToAction("Index");
            }
            return View(new SearchModelUsers(QueryStr, Login.UserID, getsetUserLikeMode(Mode)));
        }

        DBHandlerView.SelectUserLikeMode getsetUserLikeMode(DBHandlerView.SelectUserLikeMode Mode)
        {
            if(Mode != DBHandlerView.SelectUserLikeMode.Undefined)
            {
                Session["UserLikeMode"] = Mode;
                return Mode;
            }
            else
            {
                //ここでデフォルト値も決める
                DBHandlerView.SelectUserLikeMode newMode = Session["UserLikeMode"] as DBHandlerView.SelectUserLikeMode? ?? DBHandlerView.SelectUserLikeMode.Show;
                Session["UserLikeMode"] = newMode;
                return newMode;
            }
        }
    }
}