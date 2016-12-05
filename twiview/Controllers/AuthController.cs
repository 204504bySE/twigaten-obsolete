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
            Session["OAuthSession"] = oAuthSession;

            return Redirect(oAuthSession.AuthorizeUri.OriginalString);
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