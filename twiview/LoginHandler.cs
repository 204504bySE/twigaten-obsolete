using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Security.Cryptography;
using System.ComponentModel;
using CoreTweet;

namespace twiview
{
    public class LoginHandler
    {
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

        public DBHandlerToken.VerifytokenResult StoreNewLogin(Tokens token)
        {
            DBHandlerToken dbToken = new DBHandlerToken();
            DBHandlerToken.VerifytokenResult vt = dbToken.Verifytoken(token);
            if (vt != DBHandlerToken.VerifytokenResult.Exist)
            {
                if (dbToken.InsertNewtoken(token) < 1)
                {
                    throw (new Exception("トークンの保存に失敗しました"));
                }
            }
            UserResponse SelfUserInfo = token.Account.VerifyCredentials();
            dbToken.StoreUserProfile(SelfUserInfo);

            Session["LoginUserID"] = token.UserId;
            Session["LoginUserScreenName"] = SelfUserInfo.ScreenName;

            byte[] random = new byte[64];
            string base64str;
            new RNGCryptoServiceProvider().GetBytes(random);
            base64str = Convert.ToBase64String(random);
            if(dbToken.StoreUserLoginString(token.UserId, base64str) < 1) { throw new Exception("トークンの保存に失敗しました"); }

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

                new DBHandlerView().DeleteUserLoginString(UserID.Value);
                ClearCookie("TweetCount");
                ClearCookie("GetRetweet");
                ClearCookie("TweetOrder");
            }
            ClearCookie("LoginToken");
            ClearCookie("ID");
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
            DBHandlerView dbView = new DBHandlerView();
            if (dbView.SelectUserLoginString(UserID) == LoginToken)
            {
                //Cookieの有効期限を延長する
                SetCookie("ID", UserID.ToString());
                SetCookie("LoginToken", LoginToken);
                Session["LoginUserID"] = UserID;
                Session["LoginUserScreenName"] = dbView.SelectUser(UserID).screen_name;
                Session["LoginUserToken"] = LoginToken;
                return UserID;
            }
            else
            {
                Logout();
                return null;
            }
        }

        //URL > Cookie > Default の優先順位で値を持ってくるやつ
        //ついでにクエリで指定されてたらCookieにも入れる
        public T getCookiePref<T>(T URLPref, T Default, string CookieName)
        {
            if (URLPref != null)
            {
                SetCookie(CookieName, URLPref.ToString());
                return URLPref;
            }
            //後ろがデフォルト
            if (Request.Cookies[CookieName] != null)
            {
                try
                {
                    TypeConverter Converter = TypeDescriptor.GetConverter(typeof(T));
                    if (Converter != null)
                    {
                        return (T)Converter.ConvertFromString(Request.Cookies[CookieName].Value);
                    }
                }
                catch { }
            }
            return Default;
        }
    }
}