using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace twiview.Models
{
    public class SearchModel { }

    public class SearchModelUsers : SearchModel
    {
        public long QueryElapsedMilliseconds { get; protected set; }
        protected System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        dbhandlerview db = new dbhandlerview();
        public string target_screen_name { get; }
        public string ModeStr { get; }
        public int Limit { get; }
        public bool Logined { get; }

        public TweetData._user[] Users { get; }

        public SearchModelUsers(string _target_screen_name, long? LoginUserID, dbhandlerview.SelectUserLikeMode _Mode) {
            sw.Start();
            Limit = 100;
            Logined = (LoginUserID != null);
            target_screen_name = _target_screen_name.Trim().Replace("@", "").Replace("%", "");
            ModeStr = _Mode.ToString();
            Users = db.SelectUserLike(target_screen_name.Trim().Replace(' ', '%').Replace("_", @"\_") + "%", LoginUserID, _Mode, Limit);   //前方一致

            sw.Stop();
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
       }
    }

    public class SearchModelMedia : SearchModel
    {
        public enum FailureType { HashFail, NoTweet }
        public FailureType Mode { get; }
        public SearchModelMedia(FailureType mode)
        {
            Mode = mode;
        }
    }
}