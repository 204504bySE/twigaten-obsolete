using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Text;
using System.Text.RegularExpressions;
using twiview.Models;
using System.Net.Http;
using System.IO;
using System.Threading.Tasks;

namespace twiview.Controllers
{
    public class SearchController : Controller
    {
        DBHandlerView db = new DBHandlerView();
        static readonly Regex StatusRegex = new Regex(@"(?<=twitter\.com\/.+?\/status(es)?\/)\d+", RegexOptions.Compiled);
        static readonly Regex ScreenNameRegex = new Regex(@"(?<=twitter\.com\/|@|^)[_\w]+(?=[\/_\w]*$)", RegexOptions.Compiled);


        public class SearchParameters : LoginParameters
        {
            ///<summary>URL</summary>
            public string Str { get; set; }
            ///<summary>URL > Cookie(Session only)</summary>
            public DBHandlerView.SelectUserLikeMode? UserLikeMode { get; set; }
            ///<summary>URL</summary>
            public bool? Direct { get; set; }
            ///<summary>URL</summary>
            public HttpPostedFileWrapper File { get; set; }

            protected override void ValidateValues(HttpResponseBase Response)
            {
                Str = twitenlib.CharCodes.KillNonASCII(Str);
                UserLikeMode = UserLikeMode ?? DBHandlerView.SelectUserLikeMode.Show;
                SetCookie(nameof(UserLikeMode), UserLikeMode.ToString(), Response, true);
                Direct = Direct ?? true;
            }
            public SearchParameters() : base() { }
            ///<summary>コピー用 Fileはコピーしない (LoginParameters分も除く)</summary>
            public SearchParameters(SearchParameters p)
            {
                Str = p.Str;
                UserLikeMode = p.UserLikeMode;
                Direct = p.Direct;
            }
        }
        [Route("search")]
        public ActionResult Index(SearchParameters p)
        {
            p.Validate(Session, Response);
            if (p.Str == null || p.Str == "") { return View(); }

            //ツイートのURLっぽいならそのツイートのページに飛ばす
            string StatusStr = StatusRegex.Match(p.Str).Value;
            if (!string.IsNullOrWhiteSpace(StatusStr))
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
            if (!string.IsNullOrWhiteSpace(ScreenName))
            {
                if (p.Direct.Value)
                {
                    long? TargetUserID = db.SelectID_Unique_screen_name(ScreenName);
                    if (TargetUserID != null)
                    {
                        return RedirectToRoute(new { controller = "SimilarMedia", action = "UserTweet", UserID = TargetUserID });
                    }
                }
                return RedirectToAction("Users", new { Str = p.Str, Mode = p.UserLikeMode });
            }
            else { return RedirectToAction("Index"); }
        }
        
        [Route("search/media")]
        public ActionResult Media(SearchParameters p)
        {
            p.Validate(Session, Response);
            if(p.File == null) { return RedirectToAction("Index"); }
            byte[] mem = new byte[p.File.ContentLength];
            p.File.InputStream.Read(mem, 0, p.File.ContentLength);
            long? hash = PictHash.DCTHash(mem, config.DctHashServerUrl, p.File.FileName).Result;
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
            return View(new SearchModelUsers(p.Str, p.ID, p.UserLikeMode.Value));
        }

        static class PictHash
        {
            readonly static HttpClient Http = new HttpClient(new HttpClientHandler() { UseCookies = false });
            ///<summary>クソサーバーからDCTHashをもらってくる</summary>
            public static async Task<long?> DCTHash(byte[] mem, string ServerUrl, string FileName)
            {
                try
                {
                    MultipartFormDataContent Form = new MultipartFormDataContent();
                    ByteArrayContent File = new ByteArrayContent(mem);
                    File.Headers.ContentDisposition = new System.Net.Http.Headers.ContentDispositionHeaderValue("form-data")
                    {
                        Name = "File",
                        FileName = FileName,
                    };
                    Form.Add(File);
                    using (HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, ServerUrl) { Content = Form })
                    using (HttpResponseMessage res = await Http.SendAsync(req).ConfigureAwait(false))
                    {
                        if (long.TryParse(await res.Content.ReadAsStringAsync().ConfigureAwait(false), out long ret)) { return ret; }
                        else { return null; }
                    }
                }
                catch { return null; }
            }
        }
    }
}