﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MySql.Data.MySqlClient;
using CoreTweet;
using twitenlib;
using System.Diagnostics;
using System.IO;

namespace twiview
{
    public class DBHandlerToken : DBHandler
    {
        public DBHandlerToken() : base("token", "") { }

        public int InsertNewtoken(Tokens token)
        //<summary>
        //tokenぶっ込むやつ
        //</summary>
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT
INTO token (user_id, token, token_secret) VALUES (@user_id, @token, @token_secret)
ON DUPLICATE KEY UPDATE token=@token, token_secret=@token_secret;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value =  token.UserId;
                cmd.Parameters.Add("@token", MySqlDbType.VarChar).Value = token.AccessToken;
                cmd.Parameters.Add("@token_secret", MySqlDbType.VarChar).Value =  token.AccessTokenSecret;

                return ExecuteNonQuery(cmd);
            }
        }

        public enum VerifytokenResult { New, Exist, Modified }
        //revokeされてたものはNewと返す
        public VerifytokenResult Verifytoken(Tokens token)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand("SELECT * FROM token WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = token.UserId;
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
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT INTO
user (user_id, name, screen_name, isprotected, profile_image_url, is_default_profile_image, location, description)
VALUES (@user_id, @name, @screen_name, @isprotected, @profile_image_url, @is_default_profile_image, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, location=@location, description=@description;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = ProfileResponse.Id;
                cmd.Parameters.Add("@name", MySqlDbType.VarChar).Value = ProfileResponse.Name;
                cmd.Parameters.Add("@screen_name", MySqlDbType.VarChar).Value = ProfileResponse.ScreenName;
                cmd.Parameters.Add("@isprotected", MySqlDbType.Byte).Value = ProfileResponse.IsProtected;
                cmd.Parameters.Add("@profile_image_url", MySqlDbType.Text).Value = ProfileResponse.ProfileImageUrlHttps ?? ProfileResponse.ProfileImageUrl;
                cmd.Parameters.Add("@is_default_profile_image", MySqlDbType.Byte).Value = ProfileResponse.IsDefaultProfileImage;
                cmd.Parameters.Add("@location", MySqlDbType.TinyText).Value = ProfileResponse.Location;
                cmd.Parameters.Add("@description", MySqlDbType.Text).Value = ProfileResponse.Description;

                return ExecuteNonQuery(cmd);
            }
        }

        public int StoreUserLoginToken(long user_id, string logintoken)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT INTO viewlogin (user_id, logintoken) VALUES (@user_id, @logintoken) ON DUPLICATE KEY UPDATE logintoken=@logintoken;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                cmd.Parameters.Add("@logintoken", MySqlDbType.VarChar).Value = logintoken;

                return ExecuteNonQuery(cmd);
            }
        }
    }

    public class DBHandlerView : DBHandler
    {
        public DBHandlerView() : base("view", "", twiview.config.DBAddress, 11, 40, 600) { }
        public TweetData._user SelectUser(long user_id)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id, name, screen_name, isprotected, profile_image_url, is_default_profile_image, location, description
FROM user WHERE user_id = @user_id"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count < 1) { return null; }
            return TableToUser(Table)[0];
        }

        public string SelectUserLoginToken(long user_id)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT logintoken FROM viewlogin WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count < 1) { return null; }
            return Table.Rows[0].Field<string>(0);
        }

        public int DeleteUserLoginString(long user_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"DELETE FROM viewlogin WHERE user_id = @user_id"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                return ExecuteNonQuery(cmd);
            }
        }

        public enum SelectUserLikeMode
        { Show, Following, All }
        public TweetData._user[] SelectUserLike(string Pattern, long? login_user_id, SelectUserLikeMode Mode, int Limit)
        {
            DataTable Table;
            System.Text.StringBuilder cmdBuilder = new System.Text.StringBuilder(@"SELECT
user_id, name, screen_name, isprotected, profile_image_url, is_default_profile_image, location, description
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
                cmd.Parameters.Add("@screen_name", MySqlDbType.VarChar).Value = Pattern;
                cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                cmd.Parameters.Add("@limit", MySqlDbType.Int64).Value = Limit;
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
                TweetData._user rettmp = new TweetData._user()
                {
                    user_id = row.Field<long>(0),
                    name = row.Field<string>(1),
                    screen_name = row.Field<string>(2),
                    isprotected = row.Field<bool?>(3) ?? row.Field<sbyte>(3) != 0,
                    profile_image_url = row.Field<string>(4),
                    location = row.Field<string>(6),
                    description = LocalText.TextToLink(row.Field<string>(7))    //htmlにしておく
                };
                rettmp.profile_image_url = LocalText.ProfileImageUrl(rettmp, row.Field<bool>(5));
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
                cmd.Parameters.Add("@screen_name", MySqlDbType.VarChar).Value = target_screen_name;
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count == 1) { return Table.Rows[0].Field<long?>(0); }
            else { return null; }
        }

        //特定のハッシュ値の画像を含むツイートのうち、表示可能かつ最も古いやつ
        public (long, long)? HashtoTweet(long? dcthash, long? login_user_id)
        {
            if (dcthash == null) { return null; }
            DataTable Table;

            //外側にORDER BYがないので
            //ハッシュ完全一致→距離1→距離2,3くらい の優先度になる(たぶん)
            using (MySqlCommand cmd = new MySqlCommand(@"
(SELECT o.tweet_id, m.media_id
FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
WHERE m.dcthash = @dcthash
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id))
ORDER BY o.created_at LIMIT 1
) UNION ALL (
SELECT o.tweet_id, m.media_id
FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
WHERE m.dcthash IN (@0,@1,@2,@3,@4,@5,@6,@7,@8,@9,@10,@11,@12,@13,@14,@15,@16,@17,@18,@19,@20,@21,@22,@23,@24,@25,@26,@27,@28,@29,@30,@31,@32,@33,@34,@35,@36,@37,@38,@39,@40,@41,@42,@43,@44,@45,@46,@47,@48,@49,@50,@51,@52,@53,@54,@55,@56,@57,@58,@59,@60,@61,@62,@63)
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id))
ORDER BY o.created_at LIMIT 1
) UNION ALL (
SELECT o.tweet_id, m.media_id
FROM dcthashpairslim p 
JOIN media m ON p.hash_large = m.dcthash
JOIN tweet_media USING (media_id)
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
WHERE p.hash_small IN (@dcthash,@0,@1,@2,@3,@4,@5,@6,@7,@8,@9,@10,@11,@12,@13,@14,@15,@16,@17,@18,@19,@20,@21,@22,@23,@24,@25,@26,@27,@28,@29,@30,@31,@32,@33,@34,@35,@36,@37,@38,@39,@40,@41,@42,@43,@44,@45,@46,@47,@48,@49,@50,@51,@52,@53,@54,@55,@56,@57,@58,@59,@60,@61,@62,@63)
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = o.user_id))
ORDER BY o.created_at LIMIT 1
) UNION ALL (
SELECT o.tweet_id, m.media_id
FROM dcthashpairslim p 
JOIN media m ON p.hash_small = m.dcthash
JOIN tweet_media USING (media_id)
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
WHERE p.hash_large IN (@dcthash,@0,@1,@2,@3,@4,@5,@6,@7,@8,@9,@10,@11,@12,@13,@14,@15,@16,@17,@18,@19,@20,@21,@22,@23,@24,@25,@26,@27,@28,@29,@30,@31,@32,@33,@34,@35,@36,@37,@38,@39,@40,@41,@42,@43,@44,@45,@46,@47,@48,@49,@50,@51,@52,@53,@54,@55,@56,@57,@58,@59,@60,@61,@62,@63)
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = o.user_id))
ORDER BY o.created_at LIMIT 1
);"))
            {
                cmd.Parameters.Add("@dcthash", MySqlDbType.Int64).Value = dcthash;
                for(int i = 0; i < 64; i++)
                {
                    cmd.Parameters.Add('@' + i.ToString(), MySqlDbType.Int64).Value = dcthash ^ (1L << i);
                }
                cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                Table = SelectTable(cmd);
            }
            if (Table.Rows.Count >= 1) { return (Table.Rows[0].Field<long>(0), Table.Rows[0].Field<long>(1)); }
            else { return null; }
        }

        public bool ExistTweet(long tweet_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(tweet_id) FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                return SelectCount(cmd, IsolationLevel.ReadUncommitted) >= 1;
            }
        }

        //tweet_idのツイートがRTだったら元ツイートのIDを返す
        public long? SourceTweetRT(long tweet_id)
        {
            DataTable Table;
            using(MySqlCommand cmd = new MySqlCommand(@"SELECT retweet_id FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
            }
            if(Table == null || Table.Rows.Count < 1) { return null; }
            else { return Table.Rows[0].Field<long?>(0); }
        }

        //特定のツイートの各画像とその類似画像
        //鍵かつフォロー外なら何も出ない
        public SimilarMediaTweet[] SimilarMediaTweet(long tweet_id, long? login_user_id, int SimilarLimit)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(SimilarMediaHeadRT + @"
FROM tweet o
JOIN user ou ON o.user_id = ou.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN tweet_text rtt ON rt.tweet_id = rtt.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE o.tweet_id = @tweet_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id));"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                Table = SelectTable(cmd);
            }
            return TableToTweet(Table, login_user_id, SimilarLimit, true);
        }

        const int MultipleMediaOffset = 3;  //複画は今のところ4枚まで これを同ページに収めたいマン
        //target_user_idのTL内から類似画像を発見したツイートをずらりと
        public SimilarMediaTweet[] SimilarMediaTimeline(long target_user_id, long? login_user_id, long LastTweet, int TweetCount, int SimilarLimit, bool GetRetweet, bool ShowNoDup, bool Before)
        {
            //鍵垢のTLはフォローしてない限り表示しない
            //未登録のアカウントもここで弾かれる
            TweetData._user TargetUserInfo = SelectUser(target_user_id);
            if (TargetUserInfo != null && TargetUserInfo.isprotected && login_user_id != target_user_id)
            {
                if (login_user_id == null) { return new SimilarMediaTweet[0]; }
                using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(*) FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id"))
                {
                    cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                    cmd.Parameters.Add("@target_user_id", MySqlDbType.Int64).Value = target_user_id;
                    switch (SelectCount(cmd))
                    {
                        case 0:
                            return new SimilarMediaTweet[0];
                        case -1:
                            throw new Exception("SelectCount() failed.");
                        //それ以外は↓の処理を続行する
                    }
                }
            }

            int ThreadCount = Environment.ProcessorCount;
            DataTable retTable = null;
            const long QueryRangeSnowFlake = 90 * 1000 * SnowFlake.msinSnowFlake;
            long QuerySnowFlake = LastTweet;
            long NowSnowFlake = SnowFlake.Now(true);
            long NoTweetSnowFlake = 0;
            const long NoTweetLimitSnowFlake = 86400 * 1000 * SnowFlake.msinSnowFlake;
            const int GiveupMilliSeconds = 5000;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            CancellationTokenSource CancelToken = new CancellationTokenSource();
            ExecutionDataflowBlockOptions op = new ExecutionDataflowBlockOptions()
            {
                CancellationToken = CancelToken.Token,
                MaxDegreeOfParallelism = ThreadCount,
                SingleProducerConstrained = true
            };
            string QueryText;
            if (GetRetweet)
            {
                QueryText = SimilarMediaHeadRT + @"
FROM friend f 
JOIN user ou ON f.friend_id = ou.user_id
JOIN tweet o USE INDEX (PRIMARY) ON ou.user_id = o.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
JOIN tweet_media t ON COALESCE(rt.tweet_id, o.tweet_id) = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN tweet_text rtt ON rt.tweet_id = rtt.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE " + (ShowNoDup ? "" : @"(
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
) AND") + @"
o.tweet_id BETWEEN " + (Before ? "@time - @timerange AND @time" : "@time AND @time + @timerange") + @"
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
    JOIN user ous ON fs.user_id = ous.user_id
    JOIN tweet os ON ous.user_id = os.user_id
    WHERE os.retweet_id = rt.tweet_id
    AND fs.user_id = @target_user_id
    AND (@login_user_id = @target_user_id
        OR (ous.isprotected = 0
        OR ous.user_id = @login_user_id
        OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ous.user_id))
    )
    AND o.tweet_id < os.tweet_id
)
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @limitplus;";
            }
            else
            {
                QueryText = SimilarMediaHeadnoRT + @"
FROM friend f 
JOIN user ou ON f.friend_id = ou.user_id
JOIN tweet o USE INDEX (PRIMARY) ON ou.user_id = o.user_id
JOIN tweet_media t ON o.tweet_id = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE " + (ShowNoDup ? "" : @"(
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
) AND") + @"
f.user_id = @target_user_id
AND o.tweet_id BETWEEN " + (Before ? "@time - @timerange AND @time" : "@time AND @time + @timerange") + @"
AND o.retweet_id IS NULL
AND (@login_user_id = @target_user_id
    OR ou.user_id = @login_user_id
    OR ou.isprotected = 0
    OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id)
)
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @limitplus;";
            }

            TransformBlock<int, DataTable> GetTimelineBlock = new TransformBlock<int, DataTable>(
                (int i) =>
                {
                    using (MySqlCommand cmd = new MySqlCommand(QueryText))
                    {
                        cmd.Parameters.Add("@target_user_id", MySqlDbType.Int64).Value = target_user_id;
                        cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                        cmd.Parameters.Add("@time", MySqlDbType.Int64).Value = (Before ? QuerySnowFlake - QueryRangeSnowFlake * i : QuerySnowFlake + QueryRangeSnowFlake * i);
                        cmd.Parameters.Add("@timerange", MySqlDbType.Int64).Value = QueryRangeSnowFlake;
                        //類似画像が表示できない画像を弾くときだけ多めに取得する
                        cmd.Parameters.Add("@limitplus", MySqlDbType.Int64).Value = ShowNoDup ? TweetCount : TweetCount + MultipleMediaOffset;
                        return SelectTable(cmd);
                    }
                }, op);
            int PostedCount = 0;
            for (; PostedCount <= ThreadCount; PostedCount++)
            {
                GetTimelineBlock.Post(PostedCount);
            }
            int RecievedCount = 0;
            do
            {
                DataTable Table = GetTimelineBlock.Receive();
                RecievedCount++;

                if (Table.Rows.Count > 0) { NoTweetSnowFlake = 0; }   //ツイートがない期間が続いたら打ち切る
                else { NoTweetSnowFlake += QueryRangeSnowFlake; }

                if (retTable == null)
                {
                    if (Table.Rows.Count > 0) { retTable = Table; }
                }
                else
                {
                    foreach (DataRow row in Table.Rows)
                    {
                        retTable.ImportRow(row);
                        if (retTable.Rows.Count >= TweetCount + MultipleMediaOffset) { break; }
                    }
                }
                if (Before || QuerySnowFlake + QueryRangeSnowFlake * (PostedCount - 1) < NowSnowFlake)   //未来は取得しない
                {
                    GetTimelineBlock.Post(PostedCount);
                    PostedCount++;
                }
            } while (PostedCount > RecievedCount && (retTable == null || retTable.Rows.Count < TweetCount)
                && NoTweetSnowFlake < NoTweetLimitSnowFlake
                && sw.ElapsedMilliseconds < GiveupMilliSeconds);
            CancelToken.Cancel();
            if (retTable == null) { return new SimilarMediaTweet[0]; }

            SimilarMediaTweet[] ret = TableToTweet(retTable, login_user_id, SimilarLimit, ShowNoDup);
            if (!Before) { ret = ret.Reverse().ToArray(); }
            //TableToTweetで類似画像が表示できないやつが削られるので
            //多めに拾ってきて溢れた分を捨てる
            //あと複画は件数超えても同ページに入れる
            for (int i = TweetCount; i < ret.Length; i++)
            {
                if (ret[i].tweet.tweet_id != ret[TweetCount - 1].tweet.tweet_id) { return ret.Take(i - 1).ToArray(); }
            }
            return ret;
        }

        //target_user_idのツイートから類似画像を発見したツイートをずらりと
        //鍵かつフォロー外なら何も出ない
        public SimilarMediaTweet[] SimilarMediaUser(long target_user_id, long? login_user_id, long LastTweet, int TweetCount, int SimilarLimit, bool GetRetweet, bool ShowNoDup, bool Before)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand())
            {
                if (GetRetweet)
                {
                    cmd.CommandText = SimilarMediaHeadRT + @"
FROM tweet o USE INDEX (user_id)
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
JOIN user ou ON o.user_id = ou.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text rtt ON rt.tweet_id = rtt.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE ou.user_id = @target_user_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id))
AND o.tweet_id " + (Before ? "<" : ">") + @" @lasttweet" + (ShowNoDup ? "" : @"
AND (
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
)") + @"
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @limitplus;";
                }
                else
                {
                    cmd.CommandText = SimilarMediaHeadnoRT + @"
FROM tweet o USE INDEX (user_id)
JOIN user ou ON o.user_id = ou.user_id
JOIN tweet_media t ON o.tweet_id = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE ou.user_id = @target_user_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id))
AND o.tweet_id " + (Before ? "<" : ">") + @" @lasttweet
AND o.retweet_id IS NULL" + (ShowNoDup ? "" : @"
AND (
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
)") + @"
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @limitplus;";
                }
                cmd.Parameters.Add("@target_user_id", MySqlDbType.Int64).Value = target_user_id;
                cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                cmd.Parameters.Add("@lasttweet", MySqlDbType.Int64).Value = LastTweet;
                //類似画像が表示できない画像を弾くときだけ多めに取得する
                cmd.Parameters.Add("@limitplus", MySqlDbType.Int64).Value = ShowNoDup ? TweetCount : TweetCount + MultipleMediaOffset;
                Table = SelectTable(cmd);
            }
            SimilarMediaTweet[] ret = TableToTweet(Table, login_user_id, SimilarLimit, ShowNoDup);
            if (!Before) { ret = ret.Reverse().ToArray(); }
            //TableToTweetで類似画像が表示できないやつが削られるので
            //多めに拾ってきて溢れた分を捨てる
            //あと複画は件数超えても同ページに入れる
            for (int i = TweetCount; i < ret.Length; i++)
            {
                if(ret[i].tweet.tweet_id != ret[TweetCount - 1].tweet.tweet_id) { return ret.Take(i - 1).ToArray(); }
            }
            return ret;
        }

        public enum TweetOrder { New, Featured }
        public SimilarMediaTweet[] SimilarMediaFeatured(int SimilarLimit, long BeginSnowFlake, long EndSnowFlake, TweetOrder Order)
        {
            int RangeCount = Environment.ProcessorCount;
            DataTable[] Table = new DataTable[RangeCount];
            DataTable retTable = null;
            long QuerySnowFlake = BeginSnowFlake;
            long QueryRangeSnowFlake = (EndSnowFlake - BeginSnowFlake) / RangeCount;

            string QueryText = SimilarMediaHeadnoRT + @"
FROM tweet o USE INDEX (PRIMARY)
JOIN user ou USING (user_id)
JOIN tweet_media t ON o.tweet_id = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE (
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
)
AND o.tweet_id BETWEEN @begin AND @end
AND (o.favorite_count >= 250 AND o.retweet_count >= 250)
AND ou.isprotected = 0
ORDER BY (o.favorite_count + o.retweet_count) DESC
LIMIT 50;";
            Parallel.For(0, RangeCount,
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                (int i) =>
            {
                using (MySqlCommand cmd = new MySqlCommand(QueryText))
                {
                    cmd.Parameters.Add("@begin", MySqlDbType.Int64).Value = QuerySnowFlake + QueryRangeSnowFlake * i;
                    cmd.Parameters.Add("@end", MySqlDbType.Int64).Value = QuerySnowFlake + QueryRangeSnowFlake * (i + 1) - 1;
                    Table[i] = SelectTable(cmd);
                }
            });
            retTable = Table[0];
            for (int i = 1; i < RangeCount; i++)
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
                        //ふぁぼ+RT数
                        .OrderByDescending((DataRow row) => row.Field<int>(9) + row.Field<int>(10))
                        .Take(50)
                        //ツイの時刻
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



        ///<summary>TabletoTweetに渡すリレーションの形式
        ///ou:user o:tweet ot:tweet_text
        ///rt:RT元ツイのtweet ru:RT元ツイのuser
        ///m:media
        /// </summary>
        const string SimilarMediaHeadRT = @"SELECT
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url, ou.is_default_profile_image, ou.isprotected,
o.tweet_id, o.created_at, ot.text, o.favorite_count, o.retweet_count,
rt.tweet_id, ru.user_id, ru.name, ru.screen_name, ru.profile_image_url, ru.is_default_profile_image, ru.isprotected,
rt.created_at, rtt.text, rt.favorite_count, rt.retweet_count,
m.media_id, mt.media_url, mt.type,
(SELECT COUNT(media_id) FROM media WHERE dcthash = m.dctHash) - 1
    + (SELECT COUNT(media_id) FROM dcthashpairslim
        JOIN media ON hash_large = media.dcthash
        WHERE hash_small = m.dcthash)
    + (SELECT COUNT(media_id) FROM dcthashpairslim
        JOIN media ON hash_small = media.dcthash
        WHERE hash_large = m.dcthash)
    + (SELECT COUNT(tweet_id) FROM tweet_media WHERE media_id = m.media_id) - 1";
        ///<summary>TabletoTweetに渡すリレーションの形式
        ///ou:user o:tweet ot:tweet_text
        ///m:media
        /// </summary>
        const string SimilarMediaHeadnoRT = @"SELECT
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url, ou.is_default_profile_image, ou.isprotected,
o.tweet_id, o.created_at, ot.text, o.favorite_count, o.retweet_count,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
m.media_id, mt.media_url, mt.type,
(SELECT COUNT(media_id) FROM media WHERE dcthash = m.dctHash) - 1
    + (SELECT COUNT(media_id) FROM dcthashpairslim
        JOIN media ON hash_large = media.dcthash
        WHERE hash_small = m.dcthash)
    + (SELECT COUNT(media_id) FROM dcthashpairslim
        JOIN media ON hash_small = media.dcthash
        WHERE hash_large = m.dcthash)
    + (SELECT COUNT(tweet_id) FROM tweet_media WHERE media_id = m.media_id) - 1";

        SimilarMediaTweet[] TableToTweet(DataTable Table, long? login_user_id, int SimilarLimit, bool GetNoSimilar = false, bool GetSimilars = true)
        {
            SimilarMediaTweet[] retArray = new SimilarMediaTweet[Table.Rows.Count];
            Parallel.For(0, Table.Rows.Count, 
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                (int i) =>
            //for(int i = 0; i < Table.Rows.Count; i++)
            {
                SimilarMediaTweet rettmp = new SimilarMediaTweet();
                rettmp.tweet.user.user_id = Table.Rows[i].Field<long>(0);
                rettmp.tweet.user.name = Table.Rows[i].Field<string>(1);
                rettmp.tweet.user.screen_name = Table.Rows[i].Field<string>(2);
                rettmp.tweet.user.profile_image_url = Table.Rows[i].Field<string>(3);  
                rettmp.tweet.user.isprotected = Table.Rows[i].Field<bool>(4);
                rettmp.tweet.tweet_id = Table.Rows[i].Field<long>(6);
                rettmp.tweet.created_at = DateTimeOffset.FromUnixTimeSeconds(Table.Rows[i].Field<long>(7));
                rettmp.tweet.text = LocalText.TextToLink(Table.Rows[i].Field<string>(8));
                rettmp.tweet.favorite_count = Table.Rows[i].Field<int>(9);
                rettmp.tweet.retweet_count = Table.Rows[i].Field<int>(10);

                //アイコンが鯖内にあってもなくてもそれの絶対パスに置き換える
                rettmp.tweet.user.profile_image_url = LocalText.ProfileImageUrl(rettmp.tweet.user, Table.Rows[i].Field<bool>(4));

                if (!Table.Rows[i].IsNull(11)) //RTなら元ツイートが入っている
                {
                    rettmp.tweet.retweet = new TweetData._tweet();
                    rettmp.tweet.retweet.tweet_id = Table.Rows[i].Field<long>(11);
                    rettmp.tweet.retweet.user.user_id = Table.Rows[i].Field<long>(12);
                    rettmp.tweet.retweet.user.name = Table.Rows[i].Field<string>(13);
                    rettmp.tweet.retweet.user.screen_name = Table.Rows[i].Field<string>(14);
                    rettmp.tweet.retweet.user.profile_image_url = Table.Rows[i].Field<string>(15);
                    rettmp.tweet.retweet.user.isprotected = Table.Rows[i].Field<bool>(17);
                    rettmp.tweet.retweet.created_at = DateTimeOffset.FromUnixTimeSeconds(Table.Rows[i].Field<long>(18));
                    rettmp.tweet.retweet.text = LocalText.TextToLink(Table.Rows[i].Field<string>(19));
                    rettmp.tweet.retweet.favorite_count = Table.Rows[i].Field<int>(20);
                    rettmp.tweet.retweet.retweet_count = Table.Rows[i].Field<int>(21);

                    //アイコンが鯖内にあってもなくてもそれの絶対パスに置き換える
                    rettmp.tweet.retweet.user.profile_image_url = LocalText.ProfileImageUrl(rettmp.tweet.retweet.user, Table.Rows[i].Field<bool>(16));
                }
                rettmp.media.media_id = Table.Rows[i].Field<long>(22);
                rettmp.media.orig_media_url = Table.Rows[i].Field<string>(23);
                rettmp.media.type = Table.Rows[i].Field<string>(24);
                rettmp.media.media_url = rettmp.media.local_media_url = LocalText.MediaUrl(rettmp.media);
                rettmp.SimilarMediaCount = Table.Rows[i].Field<long?>(25) ?? -1;    //COUNTはNOT NULLじゃない

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

            //先に画像のハッシュ値を取得する
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT dcthash FROM media WHERE media_id = @media_id"))
            {
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = media_id;
                Table = SelectTable(cmd);
            }
            long media_hash = Table.Rows[0].Field<long>(0);

            //リレーションの形式はSimilarMediaHeadNoRTに準じる
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT 
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url,  ou.is_default_profile_image, ou.isprotected,
o.tweet_id, o.created_at, ot.text, o.favorite_count, o.retweet_count,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
a.media_id, a.media_url, a.type,
NULL
FROM(
    SELECT t.tweet_id, m.media_id, mt.media_url, mt.type
    FROM ((
            SELECT media_id FROM media 
            WHERE dcthash = @media_hash
            ORDER BY media_id LIMIT @limitplus
        ) UNION ALL (
            SELECT media.media_id FROM media
            JOIN dcthashpairslim p on p.hash_large = media.dcthash
            WHERE p.hash_small = @media_hash
            ORDER BY media.media_id LIMIT @limitplus
        ) UNION ALL (
            SELECT media.media_id FROM media
            JOIN dcthashpairslim p on p.hash_small = media.dcthash
            WHERE p.hash_large = @media_hash
            ORDER BY media.media_id LIMIT @limitplus
        ) ORDER BY media_id LIMIT @limitplus
    ) AS i
    JOIN media m ON i.media_id = m.media_id
    JOIN tweet_media t ON m.media_id = t.media_id
    LEFT JOIN media_text mt ON m.media_id = mt.media_id
ORDER BY t.tweet_id LIMIT @limitplus
) AS a
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
LEFT JOIN tweet_text ot USING (tweet_id)
WHERE (ou.isprotected = 0 OR ou.user_id = @user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @user_id AND friend_id = o.user_id))
AND o.tweet_id != @except_tweet_id
ORDER BY o.tweet_id
LIMIT @limit"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = login_user_id;
                cmd.Parameters.Add("@media_hash", MySqlDbType.Int64).Value = media_hash;
                cmd.Parameters.Add("@except_tweet_id", MySqlDbType.Int64).Value = except_tweet_id;
                cmd.Parameters.Add("@limit", MySqlDbType.Int64).Value = SimilarLimit;
                cmd.Parameters.Add("@limitplus", MySqlDbType.Int64).Value = SimilarLimit << 2;
                Table = SelectTable(cmd);
            }
            return TableToTweet(Table, login_user_id, SimilarLimit, false, false);
        }
    }
}