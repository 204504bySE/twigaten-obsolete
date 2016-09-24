using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MySql.Data.MySqlClient;
using System.Data;
using CoreTweet;
using CoreTweet.Streaming;

using twitenlib;
namespace twidown
{
    class DBHandler : twitenlib.DBHandler
    {
        readonly int pid;
        private DBHandler() : base("crawl", "", Config.Instance.database.Address, 20, (uint)Config.Instance.crawl.MaxDBConnections)
        {
            pid = System.Diagnostics.Process.GetCurrentProcess().Id;
        }
        private static DBHandler _db = new DBHandler();
        //singletonはこれでインスタンスを取得して使う
        public static DBHandler Instance
        {
            get { return _db; }
        }

        int Selfpid = System.Diagnostics.Process.GetCurrentProcess().Id;
        public Tokens[] SelectAlltoken()
        //<summary>
        //全tokenを返す
        //</summary>
        {
            DataTable Table;
            Tokens[] ret;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id, token, token_secret FROM token NATURAL JOIN crawlprocess WHERE pid = @pid;"))
            {
                cmd.Parameters.AddWithValue("@pid", Selfpid);
                Table = SelectTable(cmd);
            }
            if (Table == null) { return new Tokens[0]; }
            ret = new Tokens[Table.Rows.Count];
            for (int i = 0; i < Table.Rows.Count; i++)
            {
                ret[i] = (Tokens.Create(config.token.ConsumerKey,
                    config.token.ConsumerSecret,
                    Table.Rows[i].Field<string>(1),
                    Table.Rows[i].Field<string>(2),
                    Table.Rows[i].Field<long>(0)));
            }
            return ret;
        }

        public int DeleteToken(long user_id)
        //<summary>
        //無効化されたっぽいtokenを消す
        //</summary>
        {
            using (MySqlCommand cmd = new MySqlCommand(@"DELETE FROM token WHERE user_id = @user_id;"))
            {
                cmd.Parameters.AddWithValue("@user_id", user_id);
                return ExecuteNonQuery(cmd);
            }
        }

        string ExpandUrls(Status x)
        {
            string ret;

            if(x.ExtendedTweet == null) { ret = x.FullText; }
            else { ret = x.ExtendedTweet.FullText; }

            foreach (MediaEntity m in x.ExtendedEntities.Media)
            {
                ret = ret.Replace(m.Url, m.ExpandedUrl);
            }
            foreach (UrlEntity u in x.Entities.Urls)
            {
                ret = ret.Replace(u.Url, u.ExpandedUrl);
            }
            return ret;
        }

        public int StoreUserProfile(UserResponse ProfileResponse)
        //ログインユーザー自身のユーザー情報を格納
        //Tokens.Account.VerifyCredentials() の戻り値を投げて使う
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT
INTO user (user_id, name, screen_name, isprotected, location, description) 
VALUES (@user_id, @name, @screen_name, @isprotected, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, location=@location, description=@description;"))
            {
                cmd.Parameters.AddWithValue("@user_id", ProfileResponse.Id);
                cmd.Parameters.AddWithValue("@name", ProfileResponse.Name);
                cmd.Parameters.AddWithValue("@screen_name", ProfileResponse.ScreenName);
                cmd.Parameters.AddWithValue("@isprotected", ProfileResponse.IsProtected);
                //こっちではアイコンはダウンロードしてないし更新もしない
                //cmd.Parameters.AddWithValue("@profile_image_url", ProfileResponse.ProfileImageUrl);
                cmd.Parameters.AddWithValue("@location", ProfileResponse.Location);
                cmd.Parameters.AddWithValue("@description", ProfileResponse.Description);

                return ExecuteNonQuery(cmd);
            }
        }

        //<summary>
        //アイコンを取得する必要があるかどうか返す
        //「保存されている」アイコンの元URLとNewProfileImageUrlが一致しない
        //updated_at IS NULL (アイコンが保存されていない)
        //そもそもアカウントの情報が保存されていない
        //</summary>
        public KeyValuePair<bool, string> NeedtoDownloadProfileImage(long user_id, string NewProfileImageUrl)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT profile_image_url, updated_at FROM user WHERE user_id = @user_id;"))
            {
                cmd.Parameters.AddWithValue("@user_id", user_id);
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted, true);
            }
            if (Table == null || Table.Rows.Count < 1) { return new KeyValuePair<bool, string>(true, null); }
            else if (Table.Rows[0].IsNull(1) || Table.Rows[0].Field<string>(0) != NewProfileImageUrl)
            { return new KeyValuePair<bool, string>(true, Table.Rows[0].Field<string>(0)); }
            else { return new KeyValuePair<bool, string>(false, null); }
        }

        //<summary>
        //DBにユーザーを入れる RTは先にやらないとキー制約が
        //UpdateProfileImageを入れるとprofile_image_urlを更新する
        //</summary>
        public int StoreUser(Status x, bool UpdateProfileImage = false)
        {
            if (x.Entities.Media == null) { return 0; }    //画像なしツイートは捨てる
            using (MySqlCommand cmd = new MySqlCommand())
            {
                if (UpdateProfileImage)
                {
                    cmd.CommandText = @"INSERT
INTO user (user_id, name, screen_name, isprotected, profile_image_url, updated_at, location, description)
VALUES (@user_id, @name, @screen_name, @isprotected, @profile_image_url, @updated_at, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, profile_image_url=@profile_image_url, updated_at=@updated_at, location=@location, description=@description;";
                }
                else
                {
                    //updateはよくやる, insertは未保存アカウントかつアイコン取得失敗時のみ
                    cmd.CommandText = @"INSERT
INTO user (user_id, name, screen_name, isprotected, profile_image_url, location, description)
VALUES (@user_id, @name, @screen_name, @isprotected, @profile_image_url, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, 
updated_at=(CASE
WHEN updated_at IS NOT NULL THEN @updated_at
ELSE NULL
END)
,location=@location, description=@description;";
                }
                cmd.Parameters.AddWithValue("@user_id", x.User.Id);
                cmd.Parameters.AddWithValue("@name", x.User.Name);
                cmd.Parameters.AddWithValue("@screen_name", x.User.ScreenName);
                cmd.Parameters.AddWithValue("@isprotected", x.User.IsProtected);
                cmd.Parameters.AddWithValue("@profile_image_url", x.User.ProfileImageUrl);
                //↓アイコンが保存されている場合だけ更新される
                cmd.Parameters.AddWithValue("@updated_at", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                cmd.Parameters.AddWithValue("@location", x.User.Location);
                cmd.Parameters.AddWithValue("@description", x.User.Description);

                return ExecuteNonQuery(cmd, UpdateProfileImage);
            }
        }

        public int StoreTweet(Status x, bool update)
        //<summary>
        //DBにツイートを入れる 先ににstoreuserしないとキー制約が
        //もちろんRT元→RTの順で呼ばないとキー制約が
        //</summary>
        {
            if (x.Entities.Media == null) { return 0; }    //画像なしツイートは捨てる
            using (MySqlCommand cmd = new MySqlCommand())
            {
                if (update)
                {
                    cmd.CommandText = @"INSERT
INTO tweet (tweet_id, user_id, created_at, text, retweet_id, retweet_count, favorite_count)
VALUES(@tweet_id, @user_id, @created_at, @text, @retweet_id, @retweet_count, @favorite_count)
ON DUPLICATE KEY UPDATE retweet_count=@retweet_count, favorite_count=@favorite_count;";
                }
                else
                {
                    cmd.CommandText = @"INSERT IGNORE
INTO tweet (tweet_id, user_id, created_at, text, retweet_id, retweet_count, favorite_count)
VALUES(@tweet_id, @user_id, @created_at, @text, @retweet_id, @retweet_count, @favorite_count);";
                }
                cmd.Parameters.AddWithValue("@tweet_id", x.Id);
                cmd.Parameters.AddWithValue("@user_id", x.User.Id);
                cmd.Parameters.AddWithValue("@created_at", x.CreatedAt.ToUnixTimeSeconds());
                cmd.Parameters.AddWithValue("@text", x.RetweetedStatus == null ? ExpandUrls(x) : null);
                cmd.Parameters.AddWithValue("@retweet_id", x.RetweetedStatus == null ? null as long? : x.RetweetedStatus.Id);
                cmd.Parameters.AddWithValue("@retweet_count", x.RetweetCount);
                cmd.Parameters.AddWithValue("@favorite_count", x.FavoriteCount);
                return ExecuteNonQuery(cmd);
            }
        }
        //<summary>
        //消されたツイートをDBから消す
        //</summary>
        public int StoreTweet(DeleteMessage x)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"DELETE IGNORE FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.AddWithValue("@tweet_id", x.Id);
                return ExecuteNonQuery(cmd);
            }
        }

        public int StoreFriends(FriendsMessage x, long UserID)
        {
            return StoreFriends(x.Friends, UserID);
        }

        public int StoreFriends(long[] x, long UserID)
        //<summary>
        //UserStream接続時のフォローしている一覧を保存する
        //自分自身も入れる
        {
            const int BulkUnit = 1000;
            const string head = @"INSERT IGNORE INTO friend (user_id, friend_id) VALUES";
            List<MySqlCommand> cmdList = new List<MySqlCommand>(x.Length);
            MySqlCommand cmdtmp;
            int i, j;

            MySqlCommand deletecmd = new MySqlCommand(@"DELETE FROM friend WHERE user_id = @user_id;");
            deletecmd.Parameters.AddWithValue("@user_id", UserID);
            cmdList.Add(deletecmd);

            MySqlCommand selfcmd = new MySqlCommand(@"INSERT IGNORE INTO friend (user_id, friend_id) VALUES (@user, @user);");
            selfcmd.Parameters.AddWithValue("@user", UserID);
            cmdList.Add(selfcmd);

            string BulkInsertCmdFull = "";
            for (i = 0; i < x.Length / BulkUnit; i++)
            {
                if (i == 0) { BulkInsertCmdFull = BulkCmdStr(BulkUnit, 2, head); }
                cmdtmp = new MySqlCommand(BulkInsertCmdFull);
                for (j = 0; j < BulkUnit; j++)
                {
                    cmdtmp.Parameters.AddWithValue("@a" + j.ToString(), UserID);
                    cmdtmp.Parameters.AddWithValue("@b" + j.ToString(), x[BulkUnit * i + j]);
                }
                cmdList.Add(cmdtmp);
            }
            if (x.Length % BulkUnit != 0)
            {
                cmdtmp = new MySqlCommand(BulkCmdStr(x.Length % BulkUnit, 2, head));
                for (j = 0; j < x.Length % BulkUnit; j++)
                {
                    cmdtmp.Parameters.AddWithValue("@a" + j.ToString(), UserID);
                    cmdtmp.Parameters.AddWithValue("@b" + j.ToString(), x[BulkUnit * i + j]);
                }
                cmdList.Add(cmdtmp);
            }
            return ExecuteNonQuery(cmdList);
        }

        public int StoreBlocks(long[] x, long UserID)
        //<summary>
        //ブロックしている一覧を保存する
        {
            const int BulkUnit = 1000;
            const string head = @"INSERT IGNORE INTO block (user_id, target_id) VALUES";
            List<MySqlCommand> cmdList = new List<MySqlCommand>(x.Length);
            MySqlCommand cmdtmp;
            int i, j;

            MySqlCommand deletecmd = new MySqlCommand(@"DELETE FROM block WHERE user_id = @user_id;");
            deletecmd.Parameters.AddWithValue("@user_id", UserID);
            cmdList.Add(deletecmd);

            string BulkInsertCmdFull = "";
            for (i = 0; i < x.Length / BulkUnit; i++)
            {
                if (i == 0) { BulkInsertCmdFull = BulkCmdStr(BulkUnit, 2, head); }
                cmdtmp = new MySqlCommand(BulkInsertCmdFull);
                for (j = 0; j < BulkUnit; j++)
                {
                    cmdtmp.Parameters.AddWithValue("@a" + j.ToString(), UserID);
                    cmdtmp.Parameters.AddWithValue("@b" + j.ToString(), x[BulkUnit * i + j]);
                }
                cmdList.Add(cmdtmp);
            }
            if (x.Length % BulkUnit != 0)
            {
                cmdtmp = new MySqlCommand(BulkCmdStr(x.Length % BulkUnit, 2, head));
                for (j = 0; j < x.Length % BulkUnit; j++)
                {
                    cmdtmp.Parameters.AddWithValue("@a" + j.ToString(), UserID);
                    cmdtmp.Parameters.AddWithValue("@b" + j.ToString(), x[BulkUnit * i + j]);
                }
                cmdList.Add(cmdtmp);
            }
            return ExecuteNonQuery(cmdList);
        }

        public bool ExistTweet(long tweet_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(tweet_id) FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.AddWithValue("@tweet_id", tweet_id);
                if (SelectCount(cmd, IsolationLevel.ReadUncommitted, true) >= 1) { return true; } else { return false; }
            }
        }

        //true→Mediaにmedia_idが載ってる false→載ってない null→source_tweet_idがない
        public bool? ExistMedia_source_tweet_id(long media_id)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT source_tweet_id FROM media WHERE media_id = @media_id;"))
            {
                cmd.Parameters.AddWithValue("@media_id", media_id);
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted, true);
            }
            if (Table == null || Table.Rows.Count < 1) { return false; }
            if (Table.Rows[0].IsNull(0)) { return null; } else { return true; }
        }

        //source_tweet_idを更新するためだけ
        public int UpdateMedia_source_tweet_id(MediaEntity m, Status x)
        {

            using (MySqlCommand cmd = new MySqlCommand(@"UPDATE IGNORE media SET
source_tweet_id = if (EXISTS (SELECT * FROM tweet WHERE tweet_id = @source_tweet_id), @source_tweet_id, source_tweet_id)
WHERE media_id = @media_id;"))
            {
                cmd.Parameters.AddWithValue("@media_id", m.Id);
                cmd.Parameters.AddWithValue("@source_tweet_id", m.SourceStatusId ?? x.Id);

                return ExecuteNonQuery(cmd, true);
            }
        }

        public int StoreMedia(MediaEntity m, Status x, long hash)
        {

            using (MySqlCommand cmd = new MySqlCommand(@"INSERT IGNORE 
INTO media (media_id, source_tweet_id, type, media_url, dcthash, downloaded_at) 
VALUES(@media_id, @source_tweet_id, @type, @media_url, @dcthash, @downloaded_at) 
ON DUPLICATE KEY UPDATE
source_tweet_id = if (EXISTS (SELECT * FROM tweet WHERE tweet_id = @source_tweet_id), @source_tweet_id, source_tweet_id),
dcthash = @dcthash, downloaded_at = @downloaded_at;"))
            {
                cmd.Parameters.AddWithValue("@media_id", m.Id);
                cmd.Parameters.AddWithValue("@source_tweet_id", m.SourceStatusId ?? x.Id);
                cmd.Parameters.AddWithValue("@type", m.Type);
                cmd.Parameters.AddWithValue("@media_url", m.MediaUrl);
                cmd.Parameters.AddWithValue("@dcthash", hash);
                cmd.Parameters.AddWithValue("@downloaded_at", (DateTimeOffset.UtcNow.ToUnixTimeSeconds() as long?));

                int ret = ExecuteNonQuery(cmd, true);
                return ret + Storetweet_media(x.Id, m.Id);
            }
        }

        public int Storetweet_media(long tweet_id, long media_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT IGNORE INTO tweet_media VALUES(@tweet_id, @media_id);"))
            {
                cmd.Parameters.AddWithValue("@tweet_id", tweet_id);
                cmd.Parameters.AddWithValue("@media_id", media_id);
                return ExecuteNonQuery(cmd, true);
            }
        }

        public int StoreEvents(EventMessage x)
        {
            //Eventを問答無用にDBに反映する
            //入れる必要があるイベントの仕分けはstreamer側で

            List<MySqlCommand> cmdList = new List<MySqlCommand>();
            switch (x.Event)
            {
                case EventCode.Follow:
                    cmdList.Add(new MySqlCommand(@"INSERT IGNORE INTO friend VALUES (@source, @target);"));
                    break;
                case EventCode.Unfollow:
                    cmdList.Add(new MySqlCommand(@"DELETE FROM friend WHERE user_id = @source AND friend_id = @target;"));
                    break;
                case EventCode.Block:
                    cmdList.Add(new MySqlCommand(@"DELETE IGNORE FROM friend WHERE (user_id = @source AND friend_id = @target) OR (user_id = @target AND friend_id = @source);"));
                    cmdList.Add(new MySqlCommand(@"INSERT IGNORE INTO block VALUES (@source, @target);"));
                    break;
                case EventCode.Unblock:
                    cmdList.Add(new MySqlCommand(@"DELETE FROM block WHERE user_id = @source AND target_id = @target;"));
                    break;
            }
            if (cmdList.Count < 1) { return 0; }
            foreach (MySqlCommand cmd in cmdList)
            {
                cmd.Parameters.AddWithValue("@source", x.Source);
                cmd.Parameters.AddWithValue("@target", x.Target);
            }
            return ExecuteNonQuery(cmdList);
        }
    }

    class dbhandlerlock : twitenlib.DBHandler
    {
        readonly int pid;
        private dbhandlerlock() : base("crawl", "", Config.Instance.database.AddressLock, 5, (uint)Config.Instance.crawl.MaxDBConnections)
        {
            pid = System.Diagnostics.Process.GetCurrentProcess().Id;
        }
        private static dbhandlerlock _db = new dbhandlerlock();
        //singletonはこれでインスタンスを取得して使う
        public static dbhandlerlock Instance
        {
            get { return _db; }
        }
        //プロセスを跨いだ排他制御用
        public bool LockTweet(long tweet_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT IGNORE INTO tweetlock VALUES (@tweet_id, @pid)"))
            {
                cmd.Parameters.AddWithValue("@tweet_id", tweet_id);
                cmd.Parameters.AddWithValue("@pid", pid);
                return ExecuteNonQuery(cmd) > 0;
            }
        }

        public int UnlockTweet(List<long> tweet_id)
        {
            const int BulkUnit = 1000;
            const string head = @"DELETE FROM tweetlock WHERE tweet_id IN";
            List<MySqlCommand> cmdList = new List<MySqlCommand>(tweet_id.Count);
            MySqlCommand cmdtmp;
            int i, j;

            string BulkCmdFull = "";
            for (i = 0; i < tweet_id.Count / BulkUnit; i++)
            {
                if (i == 0) { BulkCmdFull = BulkCmdStrIn(BulkUnit, head); }
                cmdtmp = new MySqlCommand(BulkCmdFull);
                for (j = 0; j < BulkUnit; j++)
                {
                    cmdtmp.Parameters.AddWithValue("@" + j.ToString(), tweet_id[BulkUnit * i + j]);
                }
                cmdList.Add(cmdtmp);
            }
            if (tweet_id.Count % BulkUnit != 0)
            {
                cmdtmp = new MySqlCommand(BulkCmdStrIn(tweet_id.Count % BulkUnit, head));
                for (j = 0; j < tweet_id.Count % BulkUnit; j++)
                {
                    cmdtmp.Parameters.AddWithValue("@" + j.ToString(), tweet_id[BulkUnit * i + j]);
                }
                cmdList.Add(cmdtmp);
            }
            return ExecuteNonQuery(cmdList);
        }
    }
}
