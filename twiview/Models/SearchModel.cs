using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using twiview.Controllers;

namespace twiview.Models
{
    public class SearchModel { }

    public class SearchModelUsers : SearchModel
    {
        public SearchController.SearchParameters p;
        public long QueryElapsedMilliseconds { get; protected set; }
        protected System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        DBHandlerView db = new DBHandlerView();
        public string target_screen_name { get; }
        public int Limit { get; }
        public bool Logined { get { return p.ID.HasValue; } }

        public TweetData._user[] Users { get; }

        public SearchModelUsers(SearchController.SearchParameters Validated) {
            sw.Start();
            p = Validated;
            Limit = 100;
            target_screen_name = p.Str.Trim().Replace("@", "").Replace("%", "");
            Users = db.SelectUserLike(target_screen_name.Trim().Replace(' ', '%').Replace("_", @"\_") + "%", p.ID, p.UserLikeMode.Value, Limit);   //前方一致

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