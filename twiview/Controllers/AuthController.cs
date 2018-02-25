using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Security.Cryptography;
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
            OAuth.OAuthSession oAuthSession = OAuth.Authorize(config.ConsumerKey, config.ConsumerSecret, config.CallBackUrl);

            // セッション情報にOAuthSessionの内容を保存
            TempData[nameof(TwitterCallbackParameters.OAuthSession)] = oAuthSession;

            return Redirect(oAuthSession.AuthorizeUri.OriginalString);
        }

        public class TwitterCallbackParameters : LoginParameters
        {
            /// <summary>URL (from Twitter)</summary>
            public string oauth_token { get; set; }
            /// <summary>URL (from Twitter)</summary>
            public string oauth_verifier { get; set; }
            /// <summary>TempData</summary>
            public OAuth.OAuthSession OAuthSession { get; set; }

            static RNGCryptoServiceProvider RNG = new RNGCryptoServiceProvider();

            //(新規)ログインの処理
            public DBHandlerToken.VerifytokenResult StoreNewLogin(Tokens Token, HttpSessionStateBase Session, HttpResponseBase Response)
            {
                DBHandlerToken dbToken = new DBHandlerToken();
                DBHandlerToken.VerifytokenResult vt = dbToken.Verifytoken(Token);
                if (vt != DBHandlerToken.VerifytokenResult.Exist)
                {
                    if (dbToken.InsertNewtoken(Token) < 1)
                    {
                        throw (new Exception("トークンの保存に失敗しました"));
                    }
                }
                UserResponse SelfUserInfo = Token.Account.VerifyCredentials();
                dbToken.StoreUserProfile(SelfUserInfo);
                ScreenName = SelfUserInfo.ScreenName;
                Session[nameof(ScreenName)] = ScreenName;

                byte[] random = new byte[64];
                string base64str;
                RNG.GetBytes(random);
                base64str = Convert.ToBase64String(random);
                if (dbToken.StoreUserLoginString(Token.UserId, base64str) < 1) { throw new Exception("トークンの保存に失敗しました"); }

                SetCookie(nameof(ID), Token.UserId.ToString(), Response);
                SetCookie(nameof(LoginToken), base64str, Response);
                ID = Token.UserId;
                LoginToken = base64str;

                return vt;
            }
        }
        //http://nakaji.hatenablog.com/entry/2014/09/19/024341
        [Route("auth/callback")]
        public ActionResult TwitterCallback(TwitterCallbackParameters p)
        {
            try
            {
                Tokens token = p.OAuthSession.GetTokens(p.oauth_verifier);
                // token から AccessToken と AccessTokenSecret を永続化しておくとか、
                // セッション情報に格納しておけば毎回認証しなくて良いかも
                
                DBHandlerToken.VerifytokenResult vt = p.StoreNewLogin(token, Session, Response);

                //127.0.0.1だとInvalid Hostnameされる localhostだとおｋ
                if (vt == DBHandlerToken.VerifytokenResult.Exist)
                {
                    return RedirectToRoute(new { controller = "SimilarMedia", action = "UserTweet", UserID = token.UserId });
                }
                else { return View("Done"); }
            }
            catch { return View("Failure"); }
        }

        [Route("auth/logout")]
        public ActionResult Logout(LoginParameters p)
        {
            p.Logout(Session, Response, true);
            return RedirectToRoute(new { controller = "Home", action = "Index" });
        }
    }
}