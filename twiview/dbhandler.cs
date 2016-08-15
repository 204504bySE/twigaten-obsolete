﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MySql.Data.MySqlClient;
using CoreTweet;

namespace twiview
{
    public class dbhandlertoken : twitenlib.DBHandler
    {
        public dbhandlertoken() : base("token", "") { }

        public int InsertNewtoken(Tokens token)
        //<summary>
        //tokenぶっ込むやつ
        //</summary>
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT
INTO token (user_id, token, token_secret) VALUES (@user_id, @token, @token_secret)
ON DUPLICATE KEY UPDATE token=@token, token_secret=@token_secret;"))
            {
                cmd.Parameters.AddWithValue("@user_id", token.UserId);
                cmd.Parameters.AddWithValue("@token", token.AccessToken);
                cmd.Parameters.AddWithValue("@token_secret", token.AccessTokenSecret);

                return ExecuteNonQuery(cmd);
            }
        }

        public enum VerifytokenResult { New, Exist, Modified }
        //revokeされてたものはNewと返す
        public VerifytokenResult Verifytoken(Tokens token, bool StoreifNotExist = true)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand("SELECT * FROM token WHERE user_id = @user_id;"))
            {
                cmd.Parameters.AddWithValue("@user_id", token.UserId);
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count < 1) { return VerifytokenResult.New; }
            else if (Table.Rows[0].Field<string>(1) == token.AccessToken
                && Table.Rows[0].Field<string>(2) == token.AccessTokenSecret)
                { return VerifytokenResult.Exist; }
            else { return VerifytokenResult.Modified; }
        }

        public int StoreUserProfile(UserResponse ProfileResponse)
        //ログインユーザー自身のユーザー情報を格納
        //Tokens.Account.VerifyCredentials() の戻り値を投げて使う
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT INTO user VALUES (@user_id, @name, @screen_name, @isprotected, @profile_image_url, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, location=@location, description=@description;"))
            {
                cmd.Parameters.AddWithValue("@user_id", ProfileResponse.Id);
                cmd.Parameters.AddWithValue("@name", ProfileResponse.Name);
                cmd.Parameters.AddWithValue("@screen_name", ProfileResponse.ScreenName);
                cmd.Parameters.AddWithValue("@isprotected", ProfileResponse.IsProtected);
                cmd.Parameters.AddWithValue("@profile_image_url", ProfileResponse.ProfileImageUrl);
                cmd.Parameters.AddWithValue("@location", ProfileResponse.Location);
                cmd.Parameters.AddWithValue("@description", ProfileResponse.Description);

                return ExecuteNonQuery(cmd);
            }
        }

        public int StoreUserLoginString(long user_id, string base64str)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT INTO viewlogin VALUES (@user_id, @logintoken) ON DUPLICATE KEY UPDATE logintoken=@logintoken;"))
            {
                cmd.Parameters.AddWithValue("@user_id", user_id);
                cmd.Parameters.AddWithValue("@logintoken", base64str);

                return ExecuteNonQuery(cmd);
            }
        }
    }

    public class dbhandlerview : twitenlib.DBHandler
    {
        public dbhandlerview() : base("view", "", twiview.config.DBAddress, 15, 40, 600) { }
        public TweetData._user SelectUser(long user_id)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id, name, screen_name, isprotected, profile_image_url, updated_at, location, description
FROM user WHERE user_id = @user_id"))
            {
                cmd.Parameters.AddWithValue("@user_id", user_id);
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count < 1) { return null; }
            return TableToUser(Table)[0];
        }

        public string SelectUserLoginString(long user_id)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT logintoken FROM viewlogin WHERE user_id = @user_id;"))
            {
                cmd.Parameters.AddWithValue("@user_id", user_id);
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count < 1) { return null; }
            return Table.Rows[0].Field<string>(0);
        }

        public int DeleteUserLoginString(long user_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"DELETE FROM viewlogin WHERE user_id = @user_id"))
            {
                cmd.Parameters.AddWithValue("@user_id", user_id);
                return ExecuteNonQuery(cmd);
            }
        }

        public enum SelectUserLikeMode { Undefined, Show, Following, All }
        public TweetData._user[] SelectUserLike(string Pattern, long? login_user_id, SelectUserLikeMode Mode, int Limit)
        {
            DataTable Table;
            System.Text.StringBuilder cmdBuilder = new System.Text.StringBuilder(@"SELECT
user_id, name, screen_name, isprotected, profile_image_url, updated_at, location, description
FROM user AS u WHERE screen_name LIKE @screen_name ");

            switch (Mode)
            {
                case SelectUserLikeMode.Show:
                    if (login_user_id == null) { cmdBuilder.Append(@"AND isprotected = 0"); }
                    else { cmdBuilder.Append(@"AND (isprotected = 0 OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = u.user_id))"); }
                    break;
                case SelectUserLikeMode.Following:
                    cmdBuilder.Append("AND EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = u.user_id)");
                    break;
                default:
                    break;
            }
            cmdBuilder.Append(" LIMIT @limit;");
            using (MySqlCommand cmd = new MySqlCommand(cmdBuilder.ToString()))
            {
                cmd.Parameters.AddWithValue("@screen_name", Pattern);
                cmd.Parameters.AddWithValue("@login_user_id", login_user_id);
                cmd.Parameters.AddWithValue("@limit", Limit);
                Table = SelectTable(cmd);
            }
            return TableToUser(Table);
        }

        TweetData._user[] TableToUser(DataTable Table)
        {
            if (Table == null) { throw new Exception(); }
            List<TweetData._user> ret = new List<TweetData._user>(Table.Rows.Count);
            foreach (DataRow row in Table.Rows)
            {
                TweetData._user rettmp = new TweetData._user();
                rettmp.user_id = row.Field<long>(0);
                rettmp.name = row.Field<string>(1);
                rettmp.screen_name = row.Field<string>(2);
                rettmp.isprotected = row.Field<bool?>(3) ?? row.Field<sbyte>(3) != 0;
                rettmp.profile_image_url = row.Field<string>(4);
                rettmp.location = row.Field<string>(6);
                rettmp.description = LocalText.TextToLink(row.Field<string>(7));    //htmlにしておく

                rettmp.profile_image_url = LocalText.ProfileImageUrl(rettmp, !row.IsNull(5));
                ret.Add(rettmp);
            }
            return ret.ToArray();
        }

        public long? SelectID_Unique_screen_name(string target_screen_name)
        {
            //<summary>
            //完全一致かつ唯一のscreen_idが見つかればそのIDを返す
            //</summary>

            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM user WHERE screen_name = @screen_name LIMIT 2;"))
            {
                cmd.Parameters.AddWithValue("@screen_name", target_screen_name);
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count == 1) { return Table.Rows[0].Field<long?>(0); }
            else { return null; }
        }

        //特定のハッシュ値の画像を含むツイートのうち、表示可能かつ最も古いやつ
        public long? HashtoTweet(long? dcthash, long? login_user_id)
        {
            if (dcthash == null) { return null; }
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
o.tweet_id
FROM media m
INNER JOIN tweet_media ON m.media_id = tweet_media.media_id
INNER JOIN tweet o ON tweet_media.tweet_id = o.tweet_id
INNER JOIN user ou ON o.user_id = ou.user_id
WHERE m.dcthash = @dcthash
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id))
ORDER BY o.created_at ASC LIMIT 1;"))
            {
                cmd.Parameters.AddWithValue("@dcthash", dcthash);
                cmd.Parameters.AddWithValue("@login_user_id", login_user_id);
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count >= 1) { return Table.Rows[0].Field<long?>(0); }

            using (MySqlCommand cmd = new MySqlCommand(@"SELECT 
o.tweet_id
FROM dcthashpair p INNER JOIN media m ON p.hash_sub = m.dcthash
INNER JOIN tweet_media ON m.media_id = tweet_media.media_id
INNER JOIN tweet o ON tweet_media.tweet_id = o.tweet_id
INNER JOIN user ou ON o.user_id = ou.user_id
WHERE p.hash_pri = @dcthash
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = o.user_id))
ORDER BY p.dcthash_distance, o.created_at LIMIT 1;"))
            {
                cmd.Parameters.AddWithValue("@dcthash", dcthash);
                cmd.Parameters.AddWithValue("@login_user_id", login_user_id);
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count >= 1) { return Table.Rows[0].Field<long?>(0); }
            return null;
        }

        //特定のツイートの各画像とその類似画像
        //鍵かつフォロー外なら何も出ない
        public SimilarMediaTweet[] SimilarMediaTweet(long tweet_id, long? login_user_id, int SimilarLimit)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(SimilarMediaHeadRT + @"
FROM tweet o INNER JOIN user ou ON o.user_id = ou.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
INNER JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
INNER JOIN media m ON t.media_id = m.media_id
WHERE o.tweet_id = @tweet_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id));"))
            {
                cmd.Parameters.AddWithValue("@tweet_id", tweet_id);
                cmd.Parameters.AddWithValue("@login_user_id", login_user_id);
                Table = SelectTable(cmd);
            }
            return TableToTweet(Table, login_user_id, SimilarLimit, true);
        }

        //target_user_idのTL内から類似画像を発見したツイートをずらりと
        public SimilarMediaTweet[] SimilarMediaTimeline(long target_user_id, long? login_user_id, DateTimeOffset time, int TweetCount, int SimilarLimit, bool GetRetweet, bool Before)
        {
            //鍵垢のTLはフォローしてない限り表示しない
            //未登録のアカウントもここで弾かれる
            TweetData._user TargetUserInfo = SelectUser(target_user_id);
            if (TargetUserInfo != null && TargetUserInfo.isprotected && login_user_id != target_user_id)
            {
                if (login_user_id == null) { return new SimilarMediaTweet[0]; }
                using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(*) FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id"))
                {
                    cmd.Parameters.AddWithValue("@login_user_id", login_user_id);
                    cmd.Parameters.AddWithValue("@target_user_id", target_user_id);
                    if (SelectCount(cmd) == 0) { return new SimilarMediaTweet[0]; }
                }
            }

            int ThreadCount = Environment.ProcessorCount;
            DataTable retTable = null;
            const int QueryRangeSeconds = 90;
            long QueryTime = time.ToUnixTimeSeconds();
            long NowTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            int NoTweetSeconds = 0;
            const int NoTweetLimitSeconds = 86400;
            const int GiveupMilliSeconds = 15000;
            int QueryTick = Environment.TickCount;

            CancellationTokenSource CancelToken = new CancellationTokenSource();
            ExecutionDataflowBlockOptions op = new ExecutionDataflowBlockOptions();
            op.CancellationToken = CancelToken.Token;
            op.MaxDegreeOfParallelism = ThreadCount;

            string QueryText;
            if (GetRetweet)
            {
                QueryText = SimilarMediaHeadRT + @"
FROM friend f
INNER JOIN user ou ON f.friend_id = ou.user_id
INNER JOIN tweet o ON ou.user_id = o.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
INNER JOIN tweet_media t ON COALESCE(rt.tweet_id, o.tweet_id) = t.tweet_id
INNER JOIN media m ON t.media_id = m.media_id
WHERE (EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpair WHERE hash_pri = m.dcthash))
AND o.tweet_id BETWEEN " + (Before ? "@time - @timerange AND @time" : "@time AND @time + @timerange") + @"
AND f.user_id = @target_user_id
AND (@login_user_id = @target_user_id
    OR ou.isprotected = 0 
    OR ou.user_id = @login_user_id
    OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id)
)
AND NOT EXISTS (SELECT * FROM block WHERE user_id = @login_user_id AND target_id = rt.user_id)
AND NOT EXISTS(
    SELECT *
    FROM friend fs
    INNER JOIN user ous ON fs.user_id = ous.user_id
    INNER JOIN tweet os ON ous.user_id = os.user_id
    WHERE os.retweet_id = rt.tweet_id
    AND fs.user_id = @target_user_id
    AND (@login_user_id = @target_user_id
        OR (ous.isprotected = 0
        OR ous.user_id = @login_user_id
        OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ous.user_id))
    )
    AND o.tweet_id < os.tweet_id
)
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @tweetcount;";
            }
            else
            {
                QueryText = SimilarMediaHeadnoRT + @"
FROM friend f 
INNER JOIN user ou ON f.friend_id = ou.user_id
INNER JOIN tweet o ON ou.user_id = o.user_id
INNER JOIN tweet_media t ON o.tweet_id = t.tweet_id
INNER JOIN media m ON t.media_id = m.media_id
WHERE (EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpair WHERE hash_pri = m.dcthash))
AND o.tweet_id BETWEEN " + (Before ? "@time - @timerange AND @time" : "@time AND @time + @timerange") + @"
AND f.user_id = @target_user_id
AND (@login_user_id = @target_user_id
    OR ou.user_id = @login_user_id
    OR ou.isprotected = 0
    OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id)
)
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @tweetcount;";
            }

            TransformBlock<int, DataTable> GetTimelineBlock = new TransformBlock<int, DataTable>(
                (int i) =>
                {
                    using (MySqlCommand cmd = new MySqlCommand(QueryText))
                    {
                        cmd.Parameters.AddWithValue("@target_user_id", target_user_id);
                        cmd.Parameters.AddWithValue("@login_user_id", login_user_id);
                        cmd.Parameters.AddWithValue("@time", TimeinSnowFlake((Before ? QueryTime - QueryRangeSeconds * i : QueryTime + QueryRangeSeconds * i), Before));
                        cmd.Parameters.AddWithValue("@timerange", QueryRangeSeconds * msinSnowFlake * 1000 - 1);
                        cmd.Parameters.AddWithValue("@tweetcount", TweetCount);
                        return SelectTable(cmd);
                    }
                }, op);
            int n = 0;
            for (; n <= ThreadCount; n++)
            {
                GetTimelineBlock.Post(n);
            }
            int RecievedCount = 0;
            do
            {
                DataTable Table = GetTimelineBlock.Receive();
                RecievedCount++;

                if (Table.Rows.Count > 0) { NoTweetSeconds = 0; }   //ツイートがない期間が続いたら打ち切る
                else { NoTweetSeconds += QueryRangeSeconds; }

                if (retTable == null)
                {
                    if (Table.Rows.Count > 0) { retTable = Table; }
                }
                else
                {
                    foreach (DataRow row in Table.Rows)
                    {
                        retTable.ImportRow(row);
                        if (retTable.Rows.Count >= TweetCount) { break; }
                    }
                }
                if (Before || QueryTime + QueryRangeSeconds * (n - 1) < NowTime)   //未来は取得しない
                {
                    GetTimelineBlock.Post(n);
                    n++;
                }
            } while (n > RecievedCount && (retTable == null || retTable.Rows.Count < TweetCount)
                && NoTweetSeconds < NoTweetLimitSeconds
                && unchecked(Environment.TickCount - QueryTick) < GiveupMilliSeconds);
            CancelToken.Cancel();
            if (retTable == null) { return new SimilarMediaTweet[0]; }
            if (Before) { return TableToTweet(retTable, login_user_id, SimilarLimit); }
            else { return TableToTweet(retTable, login_user_id, SimilarLimit).Reverse().ToArray(); }
        }

        //target_user_idのツイートから類似画像を発見したツイートをずらりと
        //鍵かつフォロー外なら何も出ない
        public SimilarMediaTweet[] SimilarMediaUser(long target_user_id, long? login_user_id, DateTimeOffset time, int TweetCount, int SimilarLimit, bool GetRetweet, bool Before)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand())
            {
                if (GetRetweet)
                {
                    cmd.CommandText = SimilarMediaHeadRT + @"
FROM tweet o USE INDEX (user_id)
INNER JOIN user ou ON o.user_id = ou.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
INNER JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
INNER JOIN media m ON t.media_id = m.media_id
WHERE ou.user_id = @target_user_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id))
AND o.tweet_id " + (Before ? "<=" : ">=") + @" @time
AND (EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpair WHERE hash_pri = m.dcthash))
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @tweetcount;";
                }
                else
                {
                    cmd.CommandText = SimilarMediaHeadnoRT + @"
FROM tweet o USE INDEX (user_id)
INNER JOIN user ou ON o.user_id = ou.user_id
INNER JOIN tweet_media t ON o.tweet_id = t.tweet_id
INNER JOIN media m ON t.media_id = m.media_id
WHERE ou.user_id = @target_user_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id))
AND o.tweet_id " + (Before ? "<=" : ">=") + @" @time
AND (EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpair WHERE hash_pri = m.dcthash))
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @tweetcount;";
                }
                cmd.Parameters.AddWithValue("@target_user_id", target_user_id);
                cmd.Parameters.AddWithValue("@login_user_id", login_user_id);
                cmd.Parameters.AddWithValue("@time", TimeinSnowFlake(time.ToUnixTimeSeconds(), Before));
                cmd.Parameters.AddWithValue("@tweetcount", TweetCount);
                Table = SelectTable(cmd);
            }
            if (Before) { return TableToTweet(Table, login_user_id, SimilarLimit); }
            else { return TableToTweet(Table, login_user_id, SimilarLimit).Reverse().ToArray(); }
        }

        //時刻→SnowFlake Larger→時刻じゃないビットを1で埋める
        long TimeinSnowFlake(long UnixTimeSeconds, bool Larger)
        {
            const long TwEpoch = 1288834974657L;
            if (Larger) { return (UnixTimeSeconds * 1000 + 999 - TwEpoch) << 22 | 0x3FFFFFL; }
            else { return (UnixTimeSeconds * 1000 - TwEpoch) << 22; }
        }
        const long msinSnowFlake = 0x400000L;   //1msはこれだ

        public enum TweetOrder { New, Featured }
        public SimilarMediaTweet[] SimilarMediaFeatured(int SimilarLimit, DateTimeOffset begin, DateTimeOffset end, TweetOrder Order)
        {
            int ThreadCount = Environment.ProcessorCount;
            DataTable[] Table = new DataTable[ThreadCount];
            DataTable retTable = null;
            long QueryTime = begin.ToUnixTimeSeconds();
            int QueryRangeSeconds = (int)(end - begin).TotalSeconds / ThreadCount;

            string QueryText = SimilarMediaHeadnoRT + @"
FROM tweet o USE INDEX (PRIMARY)
INNER JOIN user ou ON o.user_id = ou.user_id
INNER JOIN tweet_media t ON o.tweet_id = t.tweet_id
INNER JOIN media m ON t.media_id = m.media_id
WHERE (EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
OR EXISTS (SELECT * FROM dcthashpair WHERE hash_pri = m.dcthash))
AND o.tweet_id BETWEEN @begin AND @end
AND (o.favorite_count >= 250 AND o.retweet_count >= 250)
AND ou.isprotected = 0
AND NOT EXISTS (SELECT *
FROM media mm
INNER JOIN tweet_media tt ON mm.media_id = tt.media_id
INNER JOIN tweet oo ON tt.tweet_id = oo.tweet_id
NATURAL LEFT JOIN user oou
WHERE mm.dcthash = m.dcthash
AND oo.tweet_id != o.tweet_id
AND oo.tweet_id BETWEEN @begin AND @end
AND oou.isprotected = 0
AND oo.favorite_count + oo.retweet_count > o.favorite_count + o.retweet_count
)
AND NOT EXISTS (SELECT *
FROM dcthashpair p
INNER JOIN media mm ON p.hash_sub = mm.dcthash
INNER JOIN tweet_media tt ON mm.media_id = tt.media_id
INNER JOIN tweet oo ON tt.tweet_id = oo.tweet_id
NATURAL LEFT JOIN user oou
WHERE p.hash_pri = m.dcthash
AND oo.tweet_id BETWEEN @begin AND @end
AND oou.isprotected = 0
AND oo.favorite_count + oo.retweet_count > o.favorite_count + o.retweet_count
)
ORDER BY (o.favorite_count + o.retweet_count) DESC
LIMIT 50;";
            Parallel.For(0, ThreadCount, (int i) =>
            {
                using (MySqlCommand cmd = new MySqlCommand(QueryText))
                {
                    cmd.Parameters.AddWithValue("@begin", TimeinSnowFlake((QueryTime + QueryRangeSeconds * i), false));
                    cmd.Parameters.AddWithValue("@end", TimeinSnowFlake((QueryTime + QueryRangeSeconds * (i + 1) - 1), true));
                    Table[i] = SelectTable(cmd, IsolationLevel.ReadUncommitted);
                }
            });
            retTable = Table[0];
            for (int i = 1; i < ThreadCount; i++)
            {
                foreach (DataRow row in Table[i].Rows)
                {
                    retTable.ImportRow(row);
                }
            }

            DataRow[] sorted;
            switch (Order)
            {
                case TweetOrder.New:
                    sorted = retTable.Rows.Cast<DataRow>()
                        .OrderByDescending((DataRow row) => row.Field<int>(9) + row.Field<int>(10))
                        .Take(50)
                        .OrderByDescending((DataRow row) => row.Field<long>(7))
                        .ToArray();
                    break;
                case TweetOrder.Featured:
                default:
                    sorted = retTable.Rows.Cast<DataRow>()
                        .OrderByDescending((DataRow row) => row.Field<int>(9) + row.Field<int>(10))
                        .Take(50)
                        .ToArray();
                    break;
            }

            retTable = retTable.Clone();
            foreach (DataRow row in sorted)
            {
                retTable.ImportRow(row);
            }
            return TableToTweet(retTable, null, SimilarLimit);
        }



        //TabletoTweetに渡すリレーションの形式
        const string SimilarMediaHeadRT = @"SELECT
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url, ou.updated_at, ou.isprotected,
o.tweet_id, o.created_at, o.text, o.favorite_count, o.retweet_count,
rt.tweet_id, ru.user_id, ru.name, ru.screen_name, ru.profile_image_url, ru.updated_at, ru.isprotected,
rt.created_at, rt.text, rt.favorite_count, rt.retweet_count,
m.media_id, m.media_url, m.type, m.downloaded_at,
(SELECT COUNT(media_id) FROM media WHERE dcthash = m.dctHash) - 1 +
(SELECT COUNT(media_id) FROM dcthashpair
    INNER JOIN media ON hash_sub = media.dcthash
    WHERE hash_pri = m.dcthash) + 
(SELECT COUNT(tweet_id) FROM tweet_media WHERE media_id = m.media_id) - 1";
        const string SimilarMediaHeadnoRT = @"SELECT
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url, ou.updated_at, ou.isprotected,
o.tweet_id, o.created_at, o.text, o.favorite_count, o.retweet_count,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
m.media_id, m.media_url, m.type, m.downloaded_at,
(SELECT COUNT(media_id) FROM media WHERE dcthash = m.dctHash) - 1 +
(SELECT COUNT(media_id) FROM dcthashpair
    INNER JOIN media ON hash_sub = media.dcthash
    WHERE hash_pri = m.dcthash) + 
(SELECT COUNT(tweet_id) FROM tweet_media WHERE media_id = m.media_id) - 1";

        SimilarMediaTweet[] TableToTweet(DataTable Table, long? login_user_id, int SimilarLimit, bool GetNoSimilar = false, bool GetSimilars = true)
        {
            SimilarMediaTweet[] retArray = new SimilarMediaTweet[Table.Rows.Count];
            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = Environment.ProcessorCount;
            Parallel.For(0, Table.Rows.Count, op, (int i) =>
            //for(int i = 0; i < Table.Rows.Count; i++)
            {
                SimilarMediaTweet rettmp = new SimilarMediaTweet();
                rettmp.tweet.user.user_id = Table.Rows[i].Field<long>(0);
            rettmp.tweet.user.name = Table.Rows[i].Field<string>(1);
                rettmp.tweet.user.screen_name = Table.Rows[i].Field<string>(2);
                //アイコンがローカルにあれば鯖内の絶対パス なければhttpの方のURL
                rettmp.tweet.user.profile_image_url = Table.Rows[i].Field<string>(3);
                rettmp.tweet.user.isprotected = Table.Rows[i].Field<bool?>(5) ?? Table.Rows[i].Field<sbyte>(5) != 0;
                rettmp.tweet.tweet_id = Table.Rows[i].Field<long>(6);
                rettmp.tweet.created_at = DateTimeOffset.FromUnixTimeSeconds(Table.Rows[i].Field<long>(7));
                rettmp.tweet.text = LocalText.TextToLink(Table.Rows[i].Field<string>(8));
            rettmp.tweet.favorite_count = Table.Rows[i].Field<int>(9);
                rettmp.tweet.retweet_count = Table.Rows[i].Field<int>(10);

                rettmp.tweet.user.profile_image_url = LocalText.ProfileImageUrl(rettmp.tweet.user, !Table.Rows[i].IsNull(4));
                if (!Table.Rows[i].IsNull(11)) //今はちゃんとretweetがnullになってくれる
                {
                    rettmp.tweet.retweet = new TweetData._tweet();
                    rettmp.tweet.retweet.user.user_id = Table.Rows[i].Field<long>(12);
                    rettmp.tweet.retweet.user.name = Table.Rows[i].Field<string>(13);
                    rettmp.tweet.retweet.user.screen_name = Table.Rows[i].Field<string>(14);
                    rettmp.tweet.retweet.user.profile_image_url = Table.Rows[i].Field<string>(15);
                    rettmp.tweet.retweet.user.isprotected = Table.Rows[i].Field<bool>(17);
                    rettmp.tweet.retweet.tweet_id = Table.Rows[i].Field<long>(11);
                    rettmp.tweet.retweet.created_at = DateTimeOffset.FromUnixTimeSeconds(Table.Rows[i].Field<long>(18));
                    rettmp.tweet.retweet.text = LocalText.TextToLink(Table.Rows[i].Field<string>(19));
                    rettmp.tweet.retweet.favorite_count = Table.Rows[i].Field<int>(20);
                    rettmp.tweet.retweet.retweet_count = Table.Rows[i].Field<int>(21);

                    rettmp.tweet.retweet.user.profile_image_url = LocalText.ProfileImageUrl(rettmp.tweet.retweet.user, !Table.Rows[i].IsNull(16));
                }
                rettmp.media.media_id = Table.Rows[i].Field<long>(22);
                rettmp.media.orig_media_url = Table.Rows[i].Field<string>(23);
                rettmp.media.media_url = LocalText.MediaUrl(rettmp.media, !Table.Rows[i].IsNull(25));
                rettmp.media.type = Table.Rows[i].Field<string>(24);
                rettmp.SimilarMediaCount = Table.Rows[i].Field<long?>(26) ?? -1;    //COUNTはNOT NULLじゃない

                if (GetSimilars)
                {
                    if (rettmp.tweet.retweet == null)
                    {
                        rettmp.Similars = SimilarMedia(rettmp.media.media_id, SimilarLimit, rettmp.tweet.tweet_id, login_user_id);
                    }
                    else
                    {
                        rettmp.Similars = SimilarMedia(rettmp.media.media_id, SimilarLimit, rettmp.tweet.retweet.tweet_id, login_user_id);
                    }
                    if (GetNoSimilar || rettmp.Similars.Length > 0) { retArray[i] = rettmp; }
                }
                else { retArray[i] = rettmp; }
            });
            List<SimilarMediaTweet> ret = new List<SimilarMediaTweet>(Table.Rows.Count);
            foreach (SimilarMediaTweet r in retArray)
            {
                if (r != null) { ret.Add(r); }
            }
            return ret.ToArray();
        }

        //特定の画像に対する類似画像とそれが含まれるツイートを返す
        //except_tweet_idを除く
        public SimilarMediaTweet[] SimilarMedia(long media_id, int SimilarLimit, long except_tweet_id, long? login_user_id = null)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT 
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url, ou.updated_at, ou.isprotected,
o.tweet_id, o.created_at, o.text, o.favorite_count, o.retweet_count,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
a.media_id, a.media_url, a.type, a.downloaded_at,
NULL
FROM(
    SELECT tweet_media.tweet_id, m.media_id, m.media_url, m.type, m.downloaded_at
    FROM ((
            SELECT media_id FROM media 
            WHERE dcthash = (SELECT dcthash FROM media WHERE media_id = @media_id)
            ORDER BY media_id LIMIT @limitplus
        ) UNION ALL (
            SELECT media.media_id FROM media
            INNER JOIN dcthashpair ON dcthashpair.hash_sub = media.dcthash
            WHERE dcthashpair.hash_pri = (SELECT dcthash FROM media WHERE media_id = @media_id)
            ORDER BY media.media_id LIMIT @limitplus
        ) ORDER BY media_id LIMIT @limitplus
    ) AS i INNER JOIN media m ON i.media_id = m.media_id
    INNER JOIN tweet_media ON m.media_id = tweet_media.media_id
    ORDER BY tweet_media.tweet_id LIMIT @limitplus
) AS a
INNER JOIN tweet o ON a.tweet_id = o.tweet_id
INNER JOIN user ou ON o.user_id = ou.user_id
WHERE (ou.isprotected = 0 OR ou.user_id = @user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @user_id AND friend_id = o.user_id))
AND o.tweet_id != @except_tweet_id
ORDER BY o.tweet_id
LIMIT @limit"))
            {
                cmd.Parameters.AddWithValue("@user_id", login_user_id);
                cmd.Parameters.AddWithValue("@media_id", media_id);
                cmd.Parameters.AddWithValue("@except_tweet_id", except_tweet_id);
                cmd.Parameters.AddWithValue("@limit", SimilarLimit);
                cmd.Parameters.AddWithValue("@limitplus", SimilarLimit << 2);
                Table = SelectTable(cmd);
            }
            return TableToTweet(Table, login_user_id, SimilarLimit, false, false);
        }
    }
}