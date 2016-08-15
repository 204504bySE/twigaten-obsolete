using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Security.Cryptography;
using CoreTweet;

namespace twiview
{
    public class LoginHandler
    {
        dbhandlertoken dbtoken = new dbhandlertoken();
        dbhandlerview dbview = new dbhandlerview();

        HttpSessionStateBase Session;
        HttpRequestBase Request;
        HttpResponseBase Response;

        public long? UserID { get; }
        public string ScreenName { get; }

        public LoginHandler(HttpSessionStateBase Session, HttpRequestBase Request, HttpResponseBase Response)
        {
            this.Session = Session;
            this.Request = Request;
            this.Response = Response;

            UserID = getLoginUserID();
            ScreenName = Session["LoginUserScreenName"] as string;
        }

        public dbhandlertoken.VerifytokenResult StoreNewLogin(Tokens token)
        {

            dbhandlertoken.VerifytokenResult vt = dbtoken.Verifytoken(token);
            if (vt != dbhandlertoken.VerifytokenResult.Exist)
            {
                if (dbtoken.InsertNewtoken(token) < 1)
                {
                    throw (new Exception("トークンの保存に失敗しました"));
                }
            }
            UserResponse SelfUserInfo = token.Account.VerifyCredentials();
            dbtoken.StoreUserProfile(SelfUserInfo);

            Session["LoginUserID"] = token.UserId;
            Session["LoginUserScreenName"] = SelfUserInfo.ScreenName;

            byte[] random = new byte[64];
            string base64str;
            new RNGCryptoServiceProvider().GetBytes(random);
            base64str = Convert.ToBase64String(random);
            if(dbtoken.StoreUserLoginString(token.UserId, base64str) < 1) { throw new Exception("トークンの保存に失敗しました"); }

            SetCookie("ID", token.UserId.ToString());
            SetCookie("LoginToken", base64str);
            Session["LoginUserToken"] = base64str;

            return vt;
        }

        public void SetCookie(string Key, string Value)
        {
            HttpCookie cookie = new HttpCookie(Key, Value);
            cookie.HttpOnly = true;
            cookie.Secure = true;
            cookie.Expires = DateTime.Now.AddYears(1);  //有効期限
            Response.SetCookie(cookie);
        }

        public void ClearCookie(string Key)
        {
            HttpCookie cookie = new HttpCookie(Key, null);
            cookie.HttpOnly = true;
            cookie.Secure = true;
            cookie.Expires = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            Response.SetCookie(cookie);
        }

        public void Logout(bool Manually = false)
        {
            if(UserID == null) { return; }
            if (Manually)
            {
                dbview.DeleteUserLoginString((long)UserID);
                ClearCookie("TweetCount");
                ClearCookie("GetRetweet");
                ClearCookie("TweetOrder");
            }
            ClearCookie("LoginToken");
            ClearCookie("ID");
            ClearCookie("MediaSize");
            Session.Abandon();
        }

        long? getLoginUserID()
        {
            //ここでログインを捏造できる
            //return 501666898;
            long? ret = Session["LoginUserID"] as long?;
            if (ret != null)
            {
                if(Request.Cookies["LoginToken"] != null && Request.Cookies["LoginToken"].Value == Session["LoginUserToken"] as string)
                {
                    return ret;
                }
                else
                    //新しい端末/ブラウザでログインすると他のログインは無効になる
                {   //あとセッション固定攻撃とかなんとか
                    Logout();
                    return null;
                }
            }
            if(Request.Cookies["ID"] == null || Request.Cookies["LoginToken"] == null) { return null; }
            long UserID;
            if (!long.TryParse(Request.Cookies["ID"].Value, out UserID)) { return null; }
            string LoginToken = Request.Cookies["LoginToken"].Value;
            dbhandlerview db = new dbhandlerview();
            if (db.SelectUserLoginString(UserID) == LoginToken)
            {
                //Cookieの有効期限を延長する
                SetCookie("ID", UserID.ToString());
                SetCookie("LoginToken", LoginToken);
                Session["LoginUserID"] = UserID;
                Session["LoginUserScreenName"] = db.SelectUser(UserID).screen_name;
                Session["LoginUserToken"] = LoginToken;
                return UserID;
            }
            else
            {
                Logout();
                return null;
            }
        }
    }
}