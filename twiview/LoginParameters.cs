using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace twiview
{
    //URLやCookieから引数を受け取ったりする
    //Controllerごとに引数を足したりした派生クラスは各Controllerのファイルでおｋ
    public class LoginParameters
    {
        ///<summary>Cookie (Login user ID)</summary>
        public long? ID { get; set; }
        ///<summary>Cookie</summary>
        public string LoginToken { get; set; }
        ///<summary>Session</summary>
        public string ScreenName { get; set; }

        protected void SetCookie(string Name, string Value, HttpResponseBase Response, bool Ephemeral = false)
        {
            HttpCookie cookie = new HttpCookie(Name, Value)
            {
                HttpOnly = true,
                Secure = true,
                Expires = Ephemeral ? DateTime.MinValue : DateTime.Now.AddYears(1)  //有効期限
            };
            Response.SetCookie(cookie);
        }

        protected void ClearCookie(string Name, HttpResponseBase Response)
        {
            HttpCookie cookie = new HttpCookie(Name)
            {
                HttpOnly = true,
                Secure = true,
                Expires = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)
            };
            Response.SetCookie(cookie);
        }
        
        public void Validate(HttpSessionStateBase Session, HttpResponseBase Response)
        {
            //ログイン確認
            DBHandlerView dbView = new DBHandlerView();
            if (ID != null)
            {
                if(LoginToken != null && LoginToken == dbView.SelectUserLoginString(ID.Value))
                {
                    //Cookieの有効期限を延長する
                    SetCookie(nameof(ID), ID.ToString(), Response);
                    SetCookie(nameof(LoginToken), LoginToken, Response);

                    if (ScreenName == null)
                    {
                        ScreenName = dbView.SelectUser(ID.Value).screen_name;
                        if (ScreenName != null) { Session[nameof(ScreenName)] = ScreenName; }
                    }
                }
                else
                {
                    //新しい端末/ブラウザでログインすると他のログインは無効になる
                    Logout(Session, Response);
                }
            }
            ValidateValues(Response);
        }
        //これをOverrideしてCookieの値を適用したりする
        protected virtual void ValidateValues(HttpResponseBase Response) { }

        //こんなところにLogoutがあるのはクソだと思う
        public void Logout(HttpSessionStateBase Session, HttpResponseBase Response, bool Manually = false)
        {
            Session[nameof(ScreenName)] = null;
            Session.Abandon();
            ClearCookie(nameof(ID), Response);
            ClearCookie(nameof(LoginToken), Response);
            ClearCookie(nameof(ScreenName), Response);
            ID = null;
            LoginToken = null;
            ScreenName = null;
            if (Manually)
            {
                ClearCookie("UserLikeMode", Response);
                ClearCookie("TweetOrder", Response);
                ClearCookie("TweetCount", Response);
                ClearCookie("GetRetweet", Response);
            }
        }
    }
}