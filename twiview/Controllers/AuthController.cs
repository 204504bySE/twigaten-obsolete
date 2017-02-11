using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using CoreTweet;

namespace twiview.Controllers
{
    public class AuthController : Controller
    {
        // GET: Auth
        //http://nakaji.hatenablog.com/entry/2014/09/19/024341
        [Route("auth/login")]
        public ActionResult Twitter()
        {
            //"{TwitterApiKey}", "{TwitterApiKeySecret}", "http://mydomain.com:63543/AuthCallback/Twitter"
            var oAuthSession = OAuth.Authorize(config.ConsumerKey, config.ConsumerSecret, config.CallBackUrl);

            // セッション情報にOAuthSessionの内容を保存
            TempData["OAuthSession"] = oAuthSession;

            return Redirect(oAuthSession.AuthorizeUri.OriginalString);
        }

        //http://nakaji.hatenablog.com/entry/2014/09/19/024341
        [Route("auth/callback")]
        public ActionResult TwitterCallback(string oauth_token, string oauth_verifier)
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);

            // OAuthSessionインスタンスを復元
            var oAuthSession = TempData["OAuthSession"] as OAuth.OAuthSession;

            try
            {
                var token = oAuthSession.GetTokens(oauth_verifier);
                // token から AccessToken と AccessTokenSecret を永続化しておくとか、
                // セッション情報に格納しておけば毎回認証しなくて良いかも

                DBHandlerToken db = new DBHandlerToken();
                DBHandlerToken.VerifytokenResult vt = Login.StoreNewLogin(token);

                //127.0.0.1だとInvalid Hostnameされる localhostだとおｋ
                if (vt == DBHandlerToken.VerifytokenResult.Exist)
                {
                    return Redirect(@"~/users");
                }
                else
                {
                    return RedirectToAction("Done");
                }
            }
            catch
            {
                //ユーザーが認証を拒否したりするとこっち
                return RedirectToAction("Failure");
            }
        }
        [Route("auth/failure")]
        public ActionResult Failure()
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            return View();
        }
        [Route("auth/done")]
        public ActionResult Done()
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            return View();
        }

        [Route("auth/logout")]
        public ActionResult Logout()
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            Login.Logout(true);
            return Redirect("~/");
        }
    }
}