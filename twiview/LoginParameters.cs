using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Web;
using System.Web.Mvc;
using twiview.Locale;

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
        ///<summary>Cookie</summary>
        public string Locale { get; set; }

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

                    Session["LoginUserID"] = ID;    //メニューバー表示用
                    if (ScreenName == null)
                    {
                        ScreenName = dbView.SelectUser(ID.Value).screen_name;
                        if (ScreenName != null) { Session[nameof(ScreenName)] = ScreenName; }   //メニューバー表示用
                    }
                }
                else
                {
                    //新しい端末/ブラウザでログインすると他のログインは無効になる
                    Logout(Session, Response);
                }
            }
            ValidateValues(Response);
            ControlLocale(Session, Response);
        }
        //これをOverrideしてCookieの値を適用したりする
        protected virtual void ValidateValues(HttpResponseBase Response) { }

        public static readonly string[] Locales = new string[] { "ja", "en" };

        //言語選択を反映したりする
        void ControlLocale(HttpSessionStateBase Session, HttpResponseBase Response)
        {
            CultureInfo Culture = null;
            if (Locale != null) { try { Culture = CultureInfo.GetCultureInfo(Locale); } catch { } }
            else if (HttpContext.Current.Request.UserLanguages != null)
            {
                foreach(string LangCulture in HttpContext.Current.Request.UserLanguages)
                {
                    string Lang;
                    if(LangCulture.Length >= 2 && Locales.Contains(Lang = LangCulture.Substring(0,2).ToLower()))
                    {
                        try { Culture = CultureInfo.GetCultureInfo(Lang); break; } catch { }
                    }
                }
            }

            if (Culture != null)
            {
                SetCookie(nameof(Locale), Culture.Name, Response);
                Thread.CurrentThread.CurrentCulture = Culture;
                Thread.CurrentThread.CurrentUICulture = Culture;
            }
            else
            {
                ClearCookie(nameof(Locale), Response);
                Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
                Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            }
        }

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
                ClearCookie("Order", Response);
                ClearCookie("Count", Response);
                ClearCookie("RT", Response);
                ClearCookie("Show0", Response);
                ClearCookie(nameof(Locale), Response);
            }
        }
    }
}