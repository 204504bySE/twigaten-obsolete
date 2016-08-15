using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

using System.Security.Cryptography;
using CoreTweet;
using twitenlib;

namespace twiview.Controllers
{
    public class AuthCallbackController : Controller
    {

        // GET: AuthCallback
        //http://nakaji.hatenablog.com/entry/2014/09/19/024341
        public ActionResult Twitter(string oauth_token, string oauth_verifier)
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);

            // OAuthSessionインスタンスを復元
            var oAuthSession = Session["OAuthSession"] as OAuth.OAuthSession;

            try {
                var token = oAuthSession.GetTokens(oauth_verifier);
                // token から AccessToken と AccessTokenSecret を永続化しておくとか、
                // セッション情報に格納しておけば毎回認証しなくて良いかも

                dbhandlertoken db = new dbhandlertoken();
                dbhandlertoken.VerifytokenResult vt = Login.StoreNewLogin(token);

                //127.0.0.1だとInvalid Hostnameされる localhostだとおｋ
                //http://localhost:49776/AuthCallback/Twitter?oauth_token=T6976QAAAAAAg7LaAAABUJCk2Pw&oauth_verifier=WhDu2ITngpeH1wjS2PwVYEBdpJyiv5cE
                if (vt == dbhandlertoken.VerifytokenResult.Exist)
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
                //Session["Exception"] = e;
                return RedirectToAction("Failed");
            }
        }
        public ActionResult Failed()
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            return View();
        }
        public ActionResult Done()
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            return View();
        }
    }
}