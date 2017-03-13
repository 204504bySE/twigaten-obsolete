using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;
using System.IO;
using System.Data;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;
using System.Threading;
using System.Threading.Tasks.Dataflow;

using CoreTweet;
using twidown;
using twitenlib;

namespace twitool
{
    class Program
    {
        static void Main(string[] args)
        {
            //CheckOldProcess.CheckandExit();
            Config config = Config.Instance;
            DBHandler db = new DBHandler();

            db.RemoveOldMedia();
            db.RemoveOldProfileImage();
            Thread.Sleep(3000);
            return;
        }
    }


    public class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("tool", "", twitenlib.Config.Instance.database.Address, 600) { }

        //ツイートが削除されて参照されなくなった画像を消す
        public int RemoveOrphanMedia()
        {
            int i = 0;
            const int BulkUnit = 1000;
            const string head = @"DELETE FROM media WHERE media_id IN";

            DataTable Table;
            string BulkDeleteCmd = BulkCmdStrIn(BulkUnit, head);
            do
            {
                using (MySqlCommand cmd = new MySqlCommand(@"SELECT media_id, media_url FROM media
WHERE source_tweet_id IS NULL LIMIT @limit;"))
                {
                    cmd.Parameters.AddWithValue("@limit", BulkUnit);
                    Table = SelectTable(cmd);
                }
                if (Table == null || Table.Rows.Count < 1) { break; }

                foreach (DataRow row in Table.Rows)
                {
                    File.Delete(config.crawl.PictPaththumb + '\\'
                        + row.Field<long>(0).ToString() + Path.GetExtension(row.Field<string>(1)));
                }

                if (Table.Rows.Count < BulkUnit)
                {
                    BulkDeleteCmd = BulkCmdStrIn(Table.Rows.Count, head);
                }
                using (MySqlCommand delcmd = new MySqlCommand(BulkDeleteCmd))
                {
                    for (int n = 0; n < Table.Rows.Count; n++)
                    {
                        delcmd.Parameters.AddWithValue('@' + n.ToString(), Table.Rows[n][0]);
                    }
                    i += ExecuteNonQuery(delcmd);
                }
                Console.WriteLine("{0}: {1} Media removed", DateTime.Now, i);
            } while (Table.Rows.Count >= BulkUnit);
            return i;
        }

        public void RemoveOldMedia()
        {
            DriveInfo drive = new DriveInfo(config.crawl.PictPaththumb.Substring(0, 1));
            int RemovedCountDB = 0;
            int RemovedCountFile = 0;
            const int BulkUnit = 1000;
            Console.WriteLine("{0}: {1} / {2} MB Free.", DateTime.Now, drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
            try
            {
                while (drive.TotalFreeSpace < drive.TotalSize / 25 << 2)
                {
                    DataTable Table;
                    using (MySqlCommand cmd = new MySqlCommand(@"SELECT
media_id, media_url FROM media
WHERE downloaded_at IS NOT NULL
ORDER BY downloaded_at LIMIT @limit;"))
                    {
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        Table = SelectTable(cmd);
                    }
                    if (Table == null || Table.Rows.Count < BulkUnit) { break; }

                    foreach (DataRow row in Table.Rows)
                    {
                        File.Delete(config.crawl.PictPaththumb + '\\'
                            + ((long)row[0]).ToString() + Path.GetExtension(row[1] as string));
                    }
                    
                    MySqlCommand[] CmdList = new MySqlCommand[2];
                    CmdList[0] = new MySqlCommand(BulkCmdStrIn(Table.Rows.Count, @"UPDATE media SET downloaded_at = NULL WHERE media_id IN"));
                    CmdList[1] = new MySqlCommand(BulkCmdStrIn(Table.Rows.Count, @"DELETE FROM media WHERE source_tweet_id IS NULL AND media_id IN"));
                    for (int n = 0; n < Table.Rows.Count; n++)
                    {
                        string atNum = '@' + n.ToString();
                        for (int i = 0; i < 2; i++)
                        {
                            CmdList[i].Parameters.AddWithValue(atNum, Table.Rows[n][0]);
                        }
                    }
                    RemovedCountDB += ExecuteNonQuery(CmdList) - Table.Rows.Count;
                    RemovedCountFile += Table.Rows.Count;
                    Console.WriteLine("{0}: {1} / {2} Media removed", DateTime.Now, RemovedCountDB, RemovedCountFile);
                    Console.WriteLine("{0}: {1} / {2} MB Free.", DateTime.Now, drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
                }
            }
            catch (Exception e) { Console.WriteLine(e); return; }
            Console.WriteLine("{0}: {1} Media removal completed.", DateTime.Now, RemovedCountFile);
        }

        //しばらくツイートがないアカウントのprofile_imageを消す
        public void RemoveOldProfileImage()
        {
            DriveInfo drive = new DriveInfo(config.crawl.PictPathProfileImage.Substring(0, 1));
            int RemovedCount = 0;
            const int BulkUnit = 1000;
            const string head = @"UPDATE user SET updated_at = NULL WHERE user_id IN";
            string BulkUpdateCmd = BulkCmdStrIn(BulkUnit, head);
            Console.WriteLine("{0}: {1} / {2} MB Free.", DateTime.Now, drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
            try
            {
                while (drive.TotalFreeSpace < drive.TotalSize / 25 << 2)
                {
                    DataTable Table;
                    using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id, profile_image_url FROM user
WHERE updated_at IS NOT NULL AND profile_image_url IS NOT NULL
ORDER BY updated_at LIMIT @limit;"))
                    {
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        Table = SelectTable(cmd);
                    }
                    foreach (DataRow row in Table.Rows)
                    {
                        File.Delete(config.crawl.PictPathProfileImage + '\\'
                            + row.Field<long>(0).ToString() + Path.GetExtension(row.Field<string>(1)));
                    }
                    if (Table.Rows.Count < BulkUnit) { BulkUpdateCmd = BulkCmdStrIn(Table.Rows.Count, head); }
                    using (MySqlCommand upcmd = new MySqlCommand(BulkUpdateCmd))
                    {
                        for (int n = 0; n < Table.Rows.Count; n++)
                        {
                            upcmd.Parameters.AddWithValue('@' + n.ToString(), Table.Rows[n][0]);
                        }
                        RemovedCount += ExecuteNonQuery(upcmd);
                    }
                    Console.WriteLine("{0}: {1} Icons removed", DateTime.Now, RemovedCount);
                    Console.WriteLine("{0}: {1} / {2} MB Free.", DateTime.Now, drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
                    if (Table.Rows.Count < BulkUnit) { break; }
                }
            }
            catch (Exception e) { Console.WriteLine(e); return; }
            Console.WriteLine("{0}: {1} Icons removal completed.", DateTime.Now, RemovedCount);
        }




        //画像が削除されて意味がなくなったツイートを消す
        //URL転載したやつの転載元ツイートが消された場合
        public int RemoveOrphanTweet()
        {
            const int BulkUnit = 100;
            const string head = @"DELETE FROM tweet WHERE tweet_id IN";
            string BulkDeleteCmd = BulkCmdStrIn(BulkUnit, head);

            TransformBlock<long, DataTable> GetTweetBlock = new TransformBlock<long, DataTable>((long id) =>
            {
                using(MySqlCommand Cmd = new MySqlCommand(@"SELECT tweet_id
FROM tweet
WHERE retweet_id IS NULL
AND NOT EXISTS (SELECT * FROM tweet_media WHERE tweet_media.tweet_id = tweet.tweet_id)
AND tweet_id BETWEEN @begin AND @end
ORDER BY tweet_id DESC;"))
                {
                    Cmd.Parameters.AddWithValue("@begin", id);
                    Cmd.Parameters.AddWithValue("@end", id + SnowFlake.msinSnowFlake * 3600 * 1000 - 1);
                    return SelectTable(Cmd,IsolationLevel.RepeatableRead);
                }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });


            DateTimeOffset date = DateTimeOffset.UtcNow.AddDays(-7);
            for(int i = 0; i < 20; i++)
            {
                GetTweetBlock.Post(SnowFlake.SecondinSnowFlake(date, false));
                date = date.AddHours(-1);
            }
            while(true)
            {
                DataTable Table = GetTweetBlock.Receive();
                using (MySqlCommand delcmd = new MySqlCommand(BulkCmdStrIn(Table.Rows.Count, head)))
                {
                    for (int n = 0; n < Table.Rows.Count; n++)
                    {
                        delcmd.Parameters.AddWithValue("@" + n.ToString(), Table.Rows[n].Field<long>(0));
                    }
                    Console.WriteLine("{0} {1} Tweets removed", date, ExecuteNonQuery(delcmd));
                }
                GetTweetBlock.Post(SnowFlake.SecondinSnowFlake(date, false));
                date = date.AddHours(-1);
            }
        }


        //ツイートが削除されて参照されなくなったユーザーを消す
        public int RemoveOrphanUser()
        {
            int RemovedCount = 0;
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id, profile_image_url FROM user
WHERE NOT EXISTS (SELECT * FROM tweet WHERE tweet.user_id = user.user_id)
AND NOT EXISTS (SELECT user_id FROM token WHERE token.user_id = user.user_id);"))
            {
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
            }
            if (Table == null) { return 0; }
            Console.WriteLine("{0} {1} Users to remove", DateTime.Now, Table.Rows.Count);
            Console.ReadKey();
            Parallel.ForEach(Table.AsEnumerable(), 
                new ParallelOptions { MaxDegreeOfParallelism = 4 },
                (DataRow row) =>
            {
                using (MySqlCommand cmd = new MySqlCommand(@"DELETE FROM user WHERE user_id = @user_id;"))
                {
                    cmd.Parameters.AddWithValue("@user_id", (long)row[0]);
                    if (ExecuteNonQuery(cmd) >= 1)
                    {
                        Interlocked.Increment(ref RemovedCount);
                        if (row[1] as string != null) { File.Delete(config.crawl.PictPathProfileImage + @"\" + ((long)row[0]).ToString() + Path.GetExtension(row[1] as string)); }
                        if (RemovedCount % 1000 == 0) { Console.WriteLine("{0} {1} Users Removed", DateTime.Now, RemovedCount); }
                    }
                }
                if (RemovedCount % 1000 == 0) { Console.WriteLine("{0} {1} Users Removed", DateTime.Now, RemovedCount); }
            });
            return RemovedCount;
        }




        //tweet_mediaが書かれなかったツイ画対応を復元する
        //丸ごとコピペした最低な奴
        public int FixOrphanTweets()
        {
            int ret = 0;
            const int BulkUnit = 8192;
            const string head = @"INSERT IGNORE INTO tweet_media VALUES";
            string BulkInsertCmdFull;

            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT tweet_id, media_id FROM media
INNER JOIN tweet ON tweet_id = media.source_tweet_id
WHERE NOT EXISTS (SELECT * FROM tweet_media WHERE tweet_id = media.source_tweet_id);"))
            {
                Table = SelectTable(cmd);
            }
            if (Table == null) { return 0; }
            Console.WriteLine("{0} Tweets to fix", Table.Rows.Count);


            BulkInsertCmdFull = BulkCmdStr(BulkUnit, 2, head);
            
            object StoreLock = new object();
            Parallel.For(0, Table.Rows.Count / BulkUnit,
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                (int i) =>
            {
                MySqlCommand cmd = new MySqlCommand(BulkInsertCmdFull);
                for (int j = 0; j < BulkUnit; j++)
                {
                    cmd.Parameters.AddWithValue("@a" + j.ToString(), Table.Rows[BulkUnit * i + j][0]);
                    cmd.Parameters.AddWithValue("@b" + j.ToString(), Table.Rows[BulkUnit * i + j][1]);
                }
                lock (StoreLock) { ret += ExecuteNonQuery(cmd); }
            });
            MySqlCommand cmdlast = new MySqlCommand(BulkCmdStr(Table.Rows.Count % BulkUnit, 2, head));

            int iLast = Table.Rows.Count / BulkUnit;
            for (int j = 0; j < Table.Rows.Count % BulkUnit; j++)
            {
                cmdlast.Parameters.AddWithValue("@a" + j.ToString(), Table.Rows[BulkUnit * iLast + j][0]);
                cmdlast.Parameters.AddWithValue("@b" + j.ToString(), Table.Rows[BulkUnit * iLast + j][1]);
            }
            ret += ExecuteNonQuery(cmdlast);
            return ret;
        }

        //とりあえずここに置くやつ #クズ
        bool downloadFile(string uri, string outputPath)
        {
            try
            {
                WebRequest req = WebRequest.Create(uri);
                WebResponse res = req.GetResponse();

                using (FileStream fileStream = File.Create(outputPath))
                using (Stream httpStream = res.GetResponseStream())
                {
                    httpStream.CopyTo(fileStream);
                    fileStream.Flush();
                }
            }
            catch { File.Delete(outputPath); return false; }
            return true;
        }

        public void RemoveOrphanProfileImage()
        {
            int RemoveCount = 0;
            IEnumerable<string> Files = Directory.EnumerateFiles(config.crawl.PictPathProfileImage);
            Parallel.ForEach(Files,
                new ParallelOptions { MaxDegreeOfParallelism = 8 }
                , (string f) =>
             {
                 using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(*) FROM user WHERE user_id = @user_id;"))
                 {
                     cmd.Parameters.AddWithValue("@user_id", Path.GetFileNameWithoutExtension(f));
                     if (SelectCount(cmd, IsolationLevel.ReadUncommitted, true) == 0)
                     {
                         File.Delete(f);
                         Interlocked.Increment(ref RemoveCount);
                         Console.WriteLine("{0} {1} Files Removed. Last: {2}", DateTime.Now, RemoveCount, Path.GetFileName(f)); 
                     }
                 }
             });
        }

        public void Nullify_downloaded_at()
        {
            const int BulkUnit = 10000;
            MySqlCommand cmd = new MySqlCommand(@"UPDATE media SET downloaded_at = null WHERE downloaded_at < @time LIMIT @limit;");
            cmd.Parameters.AddWithValue("@time", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            cmd.Parameters.AddWithValue("@limit", BulkUnit);
            while (ExecuteNonQuery(cmd) > 0) { Console.WriteLine(DateTime.Now); }
        }

        public void Nullify_updated_at()
        {
            const int BulkUnit = 10000;
            MySqlCommand cmd = new MySqlCommand(@"UPDATE user SET updated_at = null WHERE updated_at < @time LIMIT @limit;");
            cmd.Parameters.AddWithValue("@time", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            cmd.Parameters.AddWithValue("@limit", BulkUnit);
            while (ExecuteNonQuery(cmd) > 0) { Console.WriteLine(DateTime.Now); }
        }

        public void Usertohttps()
        {
            //media_urlをhttps://にするだけ
            const int BulkUnit = 1000;
            int LastCount = 0;
            int UpdatedCount = 0;
            long LastMediaId = 336718109;
            string UpdateCmdStr = null;
            try
            {
                DataTable Table;
                do
                {
                    using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id, profile_image_url FROM user
WHERE user_id > @lastid
AND profile_image_url IS NOT NULL
ORDER BY user_id LIMIT @limit;"))
                    {
                        cmd.Parameters.AddWithValue("@lastid", LastMediaId);
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        Table = SelectTable(cmd);
                    }

                    if (LastCount != Table.Rows.Count)
                    {
                        UpdateCmdStr = MediatohttpsCmd(Table.Rows.Count);
                        LastCount = Table.Rows.Count;
                    }

                    using (MySqlCommand cmd = new MySqlCommand(UpdateCmdStr))
                    {
                        for (int i = 0; i < Table.Rows.Count; i++)
                        {
                            cmd.Parameters.AddWithValue('@' + i.ToString(), Table.Rows[i][0]);
                            cmd.Parameters.AddWithValue("@a" + i.ToString(), Table.Rows[i].Field<string>(1).Replace("http://", "https://"));
                            //Console.WriteLine("{0}\t{1}", Table.Rows[i][0], Table.Rows[i].Field<string>(1).Replace("http://pbs.twimg.com/", "https://pbs.twimg.com/"));
                        }
                        UpdatedCount += ExecuteNonQuery(cmd);
                    }
                    LastMediaId = Table.Rows[Table.Rows.Count - 1].Field<long>(0);
                    Console.WriteLine("{0}: {1}, {2}",DateTime.Now, UpdatedCount, LastMediaId);
                } while (Table.Rows.Count > 0);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Console.WriteLine(LastMediaId);
            }
            Console.WriteLine("＼(^o^)／");
            Console.ReadKey();
        }

        string MediatohttpsCmd(int Count)
        {
            StringBuilder BulkCmd = new StringBuilder(@"UPDATE user SET updated_at = null, profile_image_url = ELT(FIELD(user_id,");
            BulkCmd.Append('@');
            for (int i = 0; i < Count; i++)
            {
                BulkCmd.Append(i);
                BulkCmd.Append(",@");
            }
            BulkCmd.Remove(BulkCmd.Length - 2, 2);
            BulkCmd.Append("),@a");
            for (int i = 0; i < Count; i++)
            {
                BulkCmd.Append(i);
                BulkCmd.Append(",@a");
            }
            BulkCmd.Remove(BulkCmd.Length - 3, 3);
            BulkCmd.Append(") WHERE user_id IN(@");
            for (int i = 0; i < Count; i++)
            {
                BulkCmd.Append(i);
                BulkCmd.Append(",@");
            }
            BulkCmd.Remove(BulkCmd.Length - 2, 2);
            BulkCmd.Append(");");
            return BulkCmd.ToString();
        }



        public void UpdateisProtected()
        {
            //動かしたらTwitterに怒られたからやっぱダメ
            ServicePointManager.ReusePort = true;
            const int ConnectionLimit = 10;
            ServicePointManager.DefaultConnectionLimit = ConnectionLimit * 2;
            const int BulkUnit = 100;
            DataTable Table;
            int updated = 0;
            
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id, token, token_secret
FROM token;"))
            {
                Table = SelectTable(cmd);
            }
            if(Table == null) { return; }
            Tokens[] tokens = new Tokens[Table.Rows.Count];
            for(int i = 0; i < Table.Rows.Count; i++)
            {
                tokens[i] = Tokens.Create(config.token.ConsumerKey,
                    config.token.ConsumerSecret,
                    Table.Rows[i].Field<string>(1),
                    Table.Rows[i].Field<string>(2),
                    Table.Rows[i].Field<long>(0)
                    );
                tokens[i].ConnectionOptions.DisableKeepAlive = false;
                tokens[i].ConnectionOptions.UseCompression = true;
                tokens[i].ConnectionOptions.UseCompressionOnStreaming = true;
            }
            Console.WriteLine(tokens.Length);

            int tokenindex = 0;
            object tokenindexlock = new object();
            var UpdateUserBlock = new TransformBlock<long[], int>((long[] user_id) => {
                int i;
                SelectToken:
                lock (tokenindexlock)
                {
                    if (tokenindex >= tokens.Length) { tokenindex = 0; }
                    i = tokenindex;
                    tokenindex++;
                }
                try
                {
                    tokens[i].Account.VerifyCredentials();
                    CoreTweet.Core.ListedResponse<User> users = tokens[i].Users.Lookup(user_id, false);
                    List<MySqlCommand> cmd = new List<MySqlCommand>();
                    foreach (User user in users)
                    {
                        //Console.WriteLine("{0}\t{1}", user.Id, user.IsProtected);                        
                        MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE user SET isprotected = @isprotected WHERE user_id = @user_id;");
                        cmdtmp.Parameters.AddWithValue("@isprotected", user.IsProtected);
                        cmdtmp.Parameters.AddWithValue("@user_id", user.Id);
                        cmd.Add(cmdtmp);                        
                    }
                    return ExecuteNonQuery(cmd);
                }
                catch (Exception e) { Console.WriteLine(e); goto SelectToken; }
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ConnectionLimit });
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM user ORDER BY user_id LIMIT 100 OFFSET 80000;"))
            {
                cmd.Parameters.AddWithValue("@limit", BulkUnit);
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
            }

            int n = 0;
            while (Table != null && Table.Rows.Count > 0)
            {
                long[] user_id = new long[Table.Rows.Count];
                for (int i = 0; i < Table.Rows.Count; i++)
                {
                    user_id[i] = Table.Rows[i].Field<long>(0);
                }
                UpdateUserBlock.Post(user_id);
                if(n > ConnectionLimit)
                {
                    updated += UpdateUserBlock.Receive();
                    Console.WriteLine(updated);
                }
                else { n++; }

                using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM user WHERE user_id > @lastid ORDER BY user_id LIMIT 100;"))
                {
                    cmd.Parameters.AddWithValue("@lastid", Table.Rows[Table.Rows.Count - 1].Field<long>(0));
                    Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
                }
            }
            UpdateUserBlock.Complete();
            UpdateUserBlock.Completion.Wait();
        }


        public void ReHashMedia_Dataflow()
        {
            ServicePointManager.ReusePort = true;
            const int ConnectionLimit = 64;
            ServicePointManager.DefaultConnectionLimit = ConnectionLimit * 4;
            const int BulkUnit = 1000;
            DataTable Table;
            int updated = 0;
            var GetHashBlock = new TransformBlock<KeyValuePair<long, string>, KeyValuePair<long, long?>>(media => {
                return new KeyValuePair<long, long?>(media.Key, downloadforHash(media.Value + ":thumb"));
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ConnectionLimit });
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT media_id, media_url FROM media WHERE media_id < 700000000000000000 ORDER BY media_id DESC LIMIT @limit;"))
            {
                cmd.Parameters.AddWithValue("@limit", BulkUnit);
                Table = SelectTable(cmd,IsolationLevel.ReadUncommitted);
            }
            foreach (DataRow row in Table.Rows)
            {
                GetHashBlock.Post(new KeyValuePair<long, string>((long)row[0], row[1] as string));
            }
            while (Table != null && Table.Rows.Count > 0)
            {
                int LastTableCount = Table.Rows.Count;

                using (MySqlCommand cmd = new MySqlCommand(@"SELECT media_id, media_url FROM media WHERE media_id < @lastid ORDER BY media_id DESC LIMIT @limit;"))
                {
                    cmd.Parameters.AddWithValue("@lastid", (long)Table.Rows[Table.Rows.Count - 1][0]);
                    cmd.Parameters.AddWithValue("@limit", BulkUnit);
                    Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
                }
                foreach (DataRow row in Table.Rows)
                {
                    GetHashBlock.Post(new KeyValuePair<long, string>((long)row[0], row[1] as string));
                }
                KeyValuePair<long, long?> media = new KeyValuePair<long, long?>(0, null);
                for (int i = 0; i < LastTableCount; i++)
                {
                    media = GetHashBlock.Receive();
                    if (media.Value != null)
                    {
                        using (MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE media SET dcthash=@dcthash WHERE media_id = @media_id"))
                        {
                            cmdtmp.Parameters.AddWithValue("@dcthash", media.Value);
                            cmdtmp.Parameters.AddWithValue("@media_id", media.Key);
                            updated += ExecuteNonQuery(cmdtmp);
                        }
                    }
                }
                Console.WriteLine("{0} {1} hashes updated. last: {2}", DateTime.Now, updated, media.Key);
            }
            GetHashBlock.Complete();
            while (GetHashBlock.OutputCount > 0)
            {
                KeyValuePair<long, long?> media = GetHashBlock.Receive();
                if (media.Value != null)
                {
                    using (MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE media SET dcthash=@dcthash WHERE media_id = @media_id"))
                    {
                        cmdtmp.Parameters.AddWithValue("@dcthash", media.Value);
                        cmdtmp.Parameters.AddWithValue("@media_id", media.Key);
                        updated += ExecuteNonQuery(cmdtmp);
                    }
                }
            }
            Console.WriteLine("{0} {1} hashes updated.", DateTime.Now, updated);
        }

        long? downloadforHash(string uri, string referer = null)
        {
            try
            {
                HttpWebRequest req = (HttpWebRequest)WebRequest.Create(uri);
                if (referer != null) { req.Referer = referer; }
                WebResponse res = req.GetResponse();

                using (Stream httpStream = res.GetResponseStream())
                using (MemoryStream mem = new MemoryStream())
                {
                    httpStream.CopyTo(mem); //MemoryStreamはFlush不要(FlushはNOP)
                    mem.Position = 0;
                    return PictHash.DCTHash(mem);
                }
            }
            catch { return null; }
        }
    }
}

