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
using System.Data.HashFunction;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;

using CoreTweet;
using twidown;

namespace twitool
{
    class Program
    {
        static void Main(string[] args)
        {
            twitenlib.Config config = twitenlib.Config.Instance;
            DBHandler db = new DBHandler();
/*            
            db.RemoveOldMedia();
            db.RemoveOldProfileImage();
            Thread.Sleep(3000);
            return;
            */
            //Console.WriteLine("{0} Tweets fixed", db.FixOrphanTweets());
            Console.WriteLine("{0} Media removed", db.RemoveOrphanMedia());
            //Console.WriteLine("{0} Tweets removed", db.RemoveOrphanTweet());
            //Console.WriteLine("{0} Users removed", db.RemoveOrphanUser());
            //Console.WriteLine("{0} Pakuriers stored", db.FindPakurier());
            Thread.Sleep(3000);
        }
    }


    public class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("tool", "", twitenlib.Config.Instance.database.Address, 60) { }

        //ツイートが削除されて参照されなくなった画像を消す
        public int RemoveOrphanMedia()
        {
            int i = 0;
            const int BulkUnit = 1000;
            const string head = @"DELETE FROM media WHERE media_id IN";
/*
            const string head_tweet = @"DELETE FROM tweet WHERE tweet_id IN(
SELECT tweet_id FROM tweet_media
WHERE media_id IN";
*/
            DataTable Table;
            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = 8;
            string BulkDeleteCmd = BulkCmdStrIn(BulkUnit, head);
            //string BulkDeleteCmd_tweet = RemoveOrphanMediaCmd_tweet(BulkUnit);  //作ってみたけどなんか機能しないくさいよねうん
            do
            {
                using (MySqlCommand cmd = new MySqlCommand(@"SELECT media_id, media_url FROM media
WHERE source_tweet_id IS NULL LIMIT @limit;"))
                {
                    cmd.Parameters.AddWithValue("@limit", BulkUnit);
                    Table = SelectTable(cmd);
                }
                if (Table == null || Table.Rows.Count < 1) { break; }
                i += Table.Rows.Count;
                try {
                    Parallel.ForEach(DataTableExtensions.AsEnumerable(Table), op, (DataRow row) =>
                    {
                        File.Delete(config.crawl.PictPaththumb + @"\" + ((long)row[0]).ToString() + Path.GetExtension(row[1] as string));
                    });
                }catch(Exception e) { Console.WriteLine(e);return i; }

                if (Table.Rows.Count < BulkUnit)
                {
                    BulkDeleteCmd = BulkCmdStrIn(Table.Rows.Count, head);
                    //BulkDeleteCmd_tweet = RemoveOrphanMediaCmd_tweet(Table.Rows.Count);
                }
                using (MySqlCommand delcmd = new MySqlCommand(BulkDeleteCmd))
                //using (MySqlCommand delcmd_tweet = new MySqlCommand(BulkDeleteCmd_tweet))
                {
                    for (int n = 0; n < Table.Rows.Count; n++)
                    {
                        delcmd.Parameters.AddWithValue("@" + n.ToString(), Table.Rows[n][0]);
                        //delcmd_tweet.Parameters.AddWithValue("@m" + n.ToString(), Table.Rows[n][0]);
                    }
                    ExecuteNonQuery(delcmd);
                }
                Console.WriteLine("{0}: {1} Media removed", DateTime.Now, i);
            } while (Table.Rows.Count >= BulkUnit);
            return i;
        }

        //画像が削除されて意味がなくなったツイートを消す
        //URL転載したやつの転載元ツイートが消された場合
        public int RemoveOrphanTweet()
        {
            int i = 0;
            const int BulkUnit = 100;
            const string head = @"DELETE FROM tweet WHERE tweet_id IN";
            string BulkDeleteCmd = BulkCmdStrIn(BulkUnit, head);
            DataTable Table;

            MySqlCommand cmd = new MySqlCommand(@"SELECT tweet_id FROM tweet
WHERE retweet_id IS NULL
AND NOT EXISTS (SELECT * FROM tweet_media WHERE tweet_media.tweet_id = tweet.tweet_id)
ORDER BY tweet_id DESC
LIMIT @limit OFFSET @limit;");  //最新の奴は巻き込み防止のためやらない #ウンコード
            cmd.Parameters.AddWithValue("@limit", BulkUnit);
            Table = SelectTable(cmd);
            while(Table != null && Table.Rows.Count > 0)
            {
                if (Table.Rows.Count < BulkUnit) { BulkDeleteCmd = BulkCmdStrIn(Table.Rows.Count,  head); }
                using (MySqlCommand delcmd = new MySqlCommand(BulkDeleteCmd))
                {
                    for (int n = 0; n < Table.Rows.Count; n++)
                    {
                        delcmd.Parameters.AddWithValue("@" + n.ToString(), Table.Rows[n][0]);
                    }
                    i += ExecuteNonQuery(delcmd);

                }
                Console.WriteLine("{0}: {1} Tweets removed", DateTime.Now, i);

                cmd = new MySqlCommand(@"SELECT tweet_id FROM tweet
WHERE tweet_id < @last_tweet_id
AND retweet_id IS NULL
AND NOT EXISTS (SELECT * FROM tweet_media WHERE tweet_media.tweet_id = tweet.tweet_id)
ORDER BY tweet_id DESC
LIMIT @limit;");
                cmd.Parameters.AddWithValue("@last_tweet_id", Table.Rows[Table.Rows.Count - 1][0]);
                cmd.Parameters.AddWithValue("@limit", BulkUnit);
                Table = SelectTable(cmd);
            }
            return i;
        }


        //ツイートが削除されて参照されなくなったユーザーを消す
        public int RemoveOrphanUser()
        {
            int RemovedCount = 0;
            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = 8;
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
            Parallel.ForEach(Table.AsEnumerable(), op, (DataRow row) =>
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

            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = Environment.ProcessorCount;
            object StoreLock = new object();
            Parallel.For(0, Table.Rows.Count / BulkUnit, op, (int i) =>
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

                public void AddSourceTweetID()
                {
                    DataTable Table;
                    using(MySqlCommand cmd = new MySqlCommand(@"SELECT
                tweet.tweet_id, tweet.text, media.media_id
                FROM tweet 
                INNER JOIN tweet_media ON tweet.tweet_id = tweet_media.tweet_id
                INNER JOIN media ON media.media_id = tweet_media.media_id
                WHERE tweet.text IS NOT NULL
                AND source_tweet_id IS NULL;"))
                    {
                        Table = SelectTable(cmd);
                    }

                   List<MySqlCommand> cmdList = new List<MySqlCommand>();

                    //Regex tcoReg = new Regex(@"http(s)?://t\.co/[\da-zA-Z]+",RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                    Regex TweetRegex = new Regex(@"://twitter\.com/([a-z0-9_]+?)/status(es)?/(?<tweet_id>[0-9]+?)/[a-z]+/[0-9]+$",RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    foreach (DataRow row in Table.Rows)
                    {
                        foreach (Match mm in TweetRegex.Matches(row[1] as string))
                        {
                            //Console.WriteLine(mm.Groups["tweet_id"].Value);
                            MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE media SET source_tweet_id=@source_tweet_id WHERE media_id = @media_id;");
                            cmdtmp.Parameters.AddWithValue("@source_tweet_id", long.Parse(mm.Groups["tweet_id"].Value));
                            cmdtmp.Parameters.AddWithValue("@media_id", row[2] as long? ?? 0);
                            cmdList.Add(cmdtmp);
                        }
                    }

                    ExecuteNonQuery(cmdList.ToArray());
                }
        /*
                        public void HashAllText()
                        {
                            DataTable Table;
                            using(MySqlCommand cmd = new MySqlCommand(@"SELECT tweet_id, text FROM tweet WHERE text IS NOT NULL AND relaxedhash IS NULL;"))
                            {
                                Table = SelectTable(cmd);
                            }
                            List<MySqlCommand> cmdList = new List<MySqlCommand>(Table.Rows.Count);
                            foreach(DataRow row in Table.Rows)
                            {

                                MySqlCommand cmd = new MySqlCommand(@"UPDATE tweet SET relaxedhash = @relaxedhash WHERE tweet_id = @tweet_id;");
                                cmd.Parameters.AddWithValue("@relaxedhash", TextHash.RelaxedHash(row[1] as string));
                                cmd.Parameters.AddWithValue("@tweet_id", row[0] as long? ?? 0);
                                cmdList.Add(cmd);

                            }
                            ExecuteNonQuery(cmdList.ToArray());
                        }



                        public struct tweet_media
                        {
                            public long tweet_id { get; }
                            public long media_id { get; }
                            public tweet_media(long _tweet_id, long _media_id)
                            {
                                tweet_id = _tweet_id;
                                media_id = _media_id;
                            }
                        }

                        public void DownloadIcon()
                        {
                            System.Net.ServicePointManager.DefaultConnectionLimit = 20;
                            List<string> IconUrl = new List<string>();
                            DataTable Table;
                            using(MySqlCommand cmd = new MySqlCommand(@"SELECT profile_image_url FROM user;"))
                            {
                                Table = SelectTable(cmd);
                            }
                            foreach(DataRow row in Table.Rows)
                            {
                                IconUrl.Add(row[0] as string);
                            }
                            Table = null;
                            foreach(string u in IconUrl)
                             {
                                 if (!File.Exists(string.Format(@"{0}\{1}", config.crawl.PictPathProfileImage, twitenlib.localstrs.localmediapath(u))))
                                 {
                                     Console.WriteLine(u);
                                     bool nullpo = downloadFileAsync(u, string.Format(@"{0}\{1}", config.crawl.PictPathProfileImage, twitenlib.localstrs.localmediapath(u))).Result;
                                 }
                             }
                        }
            */

        public void RemoveOldMedia()
        {
            DriveInfo drive = new DriveInfo(config.crawl.PictPaththumb.Substring(0, 1));
            int RemovedCount = 0;
            const int BulkUnit = 1000;
            const string head = @"UPDATE media SET downloaded_at = NULL WHERE media_id IN";
            string BulkUpdateCmd = BulkCmdStrIn(BulkUnit, head);
            Console.WriteLine("{0}: {1} / {2} MB Free.",DateTime.Now, drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = 4;

            try {
                while (drive.TotalFreeSpace < drive.TotalSize / 6) {
                    DataTable Table;
                    using (MySqlCommand cmd = new MySqlCommand(@"SELECT
media_id, media_url FROM media
WHERE downloaded_at IS NOT NULL
ORDER BY downloaded_at LIMIT @limit;"))
                    {
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        Table = SelectTable(cmd);
                    }
                    List<KeyValuePair<long, string>> media = new List<KeyValuePair<long, string>>();
                    foreach (DataRow row in Table.Rows)
                    {
                        media.Add(new KeyValuePair<long, string>(row[0] as long? ?? 0, row[1] as string));
                    }
                    Parallel.ForEach(media, op, (KeyValuePair<long, string> m) =>
                    //foreach (KeyValuePair<long, string> m in media)
                    {
                        File.Delete(config.crawl.PictPaththumb + @"\" + m.Key.ToString() + Path.GetExtension(m.Value));
                    });
                    if (media.Count < BulkUnit) { BulkUpdateCmd = BulkCmdStrIn(media.Count, head); }
                    using (MySqlCommand upcmd = new MySqlCommand(BulkUpdateCmd))
                    {
                        for (int n = 0; n < media.Count; n++)
                        {
                            upcmd.Parameters.AddWithValue("@" + n.ToString(), media[n].Key);
                        }
                        RemovedCount += ExecuteNonQuery(upcmd);
                    }
                    Console.WriteLine("{0}: {1} Media removed", DateTime.Now, RemovedCount);
                    Console.WriteLine("{0}: {1} / {2} MB Free.", DateTime.Now, drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
                    if (media.Count < BulkUnit) { break; }
                }
            }
            catch(Exception e) { Console.WriteLine(e); return; }
            Console.WriteLine("{0}: {1} Media removal completed.", DateTime.Now, RemovedCount);
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
            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = 4;

            try
            {
                while (drive.TotalFreeSpace < drive.TotalSize / 6)
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
                    List<KeyValuePair<long, string>> media = new List<KeyValuePair<long, string>>();
                    foreach (DataRow row in Table.Rows)
                    {
                        media.Add(new KeyValuePair<long, string>((long)row[0], row[1] as string));
                    }
                    Parallel.ForEach(media, op, (KeyValuePair<long, string> m) =>
                    //foreach (KeyValuePair<long, string> m in media)
                    {
                        File.Delete(config.crawl.PictPathProfileImage + @"\" + m.Key.ToString() + Path.GetExtension(m.Value));
                    });
                    if (media.Count < BulkUnit) { BulkUpdateCmd = BulkCmdStrIn(media.Count, head); }
                    using (MySqlCommand upcmd = new MySqlCommand(BulkUpdateCmd))
                    {
                        for (int n = 0; n < media.Count; n++)
                        {
                            upcmd.Parameters.AddWithValue("@" + n.ToString(), media[n].Key);
                        }
                        RemovedCount += ExecuteNonQuery(upcmd);
                    }
                    Console.WriteLine("{0}: {1} Icons removed", DateTime.Now, RemovedCount);
                    Console.WriteLine("{0}: {1} / {2} MB Free.", DateTime.Now, drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
                    if (media.Count < BulkUnit) { break; }
                }
            }
            catch (Exception e) { Console.WriteLine(e); return; }
            Console.WriteLine("{0}: {1} Icons removal completed.", DateTime.Now, RemovedCount);
        }

        public void StoreDownloadTimeprofile()
        {
            DataTable Table = SelectTable(new MySqlCommand(@"SELECT user_id, profile_image_url FROM user WHERE profile_image_url IS NOT NULL AND downloaded_at IS NULL;"));
            Console.WriteLine(Table.Rows.Count);
            List<KeyValuePair<long, string>> media = new List<KeyValuePair<long, string>>();
            foreach (DataRow row in Table.Rows)
            {
                media.Add(new KeyValuePair<long, string>(row[0] as long? ?? 0, row[1] as string));
            }
            Table = null;
            GC.Collect();
            int i = 0, k = 0;
            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = 8;
            Parallel.ForEach(media, op, (KeyValuePair<long, string> m) =>
            //foreach (KeyValuePair<long, string> m in media)
            {
                string localurl = string.Format(@"{0}\{1}", config.crawl.PictPathProfileImage, twitenlib.localstrs.localmediapath(m.Value));
                if (File.Exists(localurl))
                {
                    Interlocked.Increment(ref k);
                    DateTime downloadtime = new FileInfo(localurl).CreationTimeUtc;
                    downloadtime = DateTime.SpecifyKind(downloadtime, DateTimeKind.Utc);
                    DateTimeOffset utcdownloadtime = downloadtime;
                    long downloaded_at = utcdownloadtime.ToUnixTimeSeconds();
                    MySqlCommand cmd2 = new MySqlCommand(@"UPDATE user SET downloaded_at = @downloaded_at WHERE user_id = @user_id;");
                    cmd2.Parameters.AddWithValue("@downloaded_at", downloaded_at);
                    cmd2.Parameters.AddWithValue("@user_id", m.Key);
                    ExecuteNonQuery(cmd2);
                }
                /*
                else
                {
                    long? hash = PictHash.dcthash(localurl);
                    MySqlCommand cmd3 = new MySqlCommand(@"UPDATE media SET dcthash = @dcthash WHERE media_id = @media_id;");
                    cmd3.Parameters.AddWithValue("@dcthash", hash);
                    cmd3.Parameters.AddWithValue("@media_id", m.Key);
                    ExecuteNonQuery(cmd3,IsolationLevel.ReadUncommitted);
                }
                */
                Interlocked.Increment(ref i);
                if (i % 1000 == 0) { Console.WriteLine("{0}: {1} / {2}", DateTime.Now, k, i); }
              });
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

/*
        public void ReHashTest()
        {
            List<dbhandler.MediaInfo> media = MediaInfoTest();

            //Parallel.For(0, media.Count - 1, op,(int i) =>
            for (int i = 0; i < media.Count; i++)
            {
                long? newhash = PictHash.dcthash(string.Format(@"{0}\{1}.{2}", config.crawl.PictPathsmall, media[i].media_id, media[i].ext));
                
                if (media[i].dcthash != newhash)
                {
                    Console.WriteLine("{0}: {1}", media[i].media_id, hammingdistance((long)media[i].dcthash, (long)newhash));
                }
                
                if (i % 1000 == 0) { Console.WriteLine("{0}:{1}", DateTime.Now, i); }
            }
        }
*/
/*
        //ReHashTest用
        public List<MediaInfo> MediaInfoTest()
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT media_id, extension, dcthash FROM media WHERE dcthash IS NOT NULL AND downloaded_at IS NOT NULL LIMIT 100000;"))
            {
                Table = SelectTable(cmd);
            }
            List<MediaInfo> ret = new List<MediaInfo>(Table.Rows.Count);
            foreach (DataRow row in Table.Rows)
            {
                if (row[0] != null && row[1] != null)
                {
                    ret.Add(new MediaInfo((long)row[0], row[1] as string, row[2] as long?));
                }
            }
            return ret;
        }
        */

        public int FindPakurier()
        {
            int ret = 0;

            //本文をパクってるやつ(パクツイbot）
            HashSet<long> Pakuriers = FindTextPakurier();
            ExecuteNonQuery(new MySqlCommand(@"TRUNCATE TABLE pakurier;"));
            ret += AddPakuriers(Pakuriers.ToArray());

            //↑から画像をパクってるやつ(パクツイbot2)
            HashSet<long> MediaPakuriers = FindMediaPakurier(Pakuriers);
            ret += AddPakuriers(MediaPakuriers.ToArray());

            //↑から画(同文) (ニュースサイトとパクツイbot3)
            Pakuriers.UnionWith(MediaPakuriers);
            MediaPakuriers = FindMediaPakurier(Pakuriers);
            ret += AddPakuriers(MediaPakuriers.ToArray());
            return ret;
        }

        int AddPakuriers(long[] PakurierArray)
        {
            List<MySqlCommand> cmdList = new List<MySqlCommand>(PakurierArray.Length);
            int ret = 0;
            const int BulkUnit = 1000;
            string BulkInsertCmdFull = AddPakuriersCmd(BulkUnit);
            MySqlCommand cmdtmp;
            int i, j;
            for (i = 0; i < PakurierArray.Length / BulkUnit; i++)
            {
                cmdtmp = new MySqlCommand(BulkInsertCmdFull);
                for (j = 0; j < BulkUnit; j++)
                {
                    cmdtmp.Parameters.AddWithValue(string.Format("@u{0}", j), PakurierArray[BulkUnit * i + j]);
                }
                ret += ExecuteNonQuery(cmdtmp);
            }
            cmdtmp = new MySqlCommand(AddPakuriersCmd(PakurierArray.Length % BulkUnit));
            for (j = 0; j < PakurierArray.Length % BulkUnit; j++)
            {
                cmdtmp.Parameters.AddWithValue(string.Format("@u{0}", j), PakurierArray[BulkUnit * i + j]);
            }
            return ret + ExecuteNonQuery(cmdtmp);
        }

        string AddPakuriersCmd(int count)
        {
            StringBuilder BulkInsertCmd = new StringBuilder("INSERT IGNORE INTO pakurier VALUES");
            for (int i = 0; i < count; i++)
            {
                BulkInsertCmd.AppendFormat("(@u{0}),", i);
            }
            BulkInsertCmd.Remove(BulkInsertCmd.Length - 1, 1);
            BulkInsertCmd.Append(";");
            return BulkInsertCmd.ToString();
        }

        DateTimeOffset NowHour()
        {
            DateTimeOffset now = DateTimeOffset.Now;
            return new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, 0, 0, 0, new TimeSpan(0));
        }

        HashSet<long> FindMediaPakurier(HashSet<long> TextPakuriers)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id
FROM tweet t
NATURAL JOIN user u
NATURAL JOIN tweet_media
NATURAL JOIN media
INNER JOIN mediapair p ON p.media_id_pri = media.media_id
WHERE isprotected = 0
AND created_at >= @created_at
AND (favorite_count >= 100 OR retweet_count >= 100)
AND EXISTS (SELECT *
FROM mediapair pp
INNER JOIN media m ON pp.media_id_sub = m.media_id
INNER JOIN tweet_media on m.media_id = tweet_media.media_id
NATURAL JOIN tweet tt
NATURAL JOIN user uu
WHERE pp.media_id_pri = p.media_id_pri
AND t.created_at > tt.created_at
AND u.user_id <> uu.user_id
AND EXISTS (SELECT * FROM pakurier WHERE user_id = uu.user_id)
);"))
            {
                cmd.Parameters.AddWithValue("@created_at", NowHour().AddDays(-7));
                Table = SelectTable(cmd);
            }
            HashSet<long> Pakuriers = new HashSet<long>();
            foreach (DataRow row in Table.Rows)
            {
                if (!TextPakuriers.Contains(row[0] as long? ?? 0)) { Pakuriers.Add(row[0] as long? ?? 0); }
            }
            return Pakuriers;
        }

        public struct texthashdata : IComparable
        {
            //public long tweet_id { get; }
            public long relaxedhash { get; }
            public long user_id { get; }
            public long created_at { get; }
            public texthashdata(long relaxedhash, long user_id, long created_at)
            {
                //this.tweet_id = tweet_id;
                this.relaxedhash = relaxedhash;
                this.user_id = user_id;
                this.created_at = created_at;
            }

            public int Compare(texthashdata it, texthashdata other)
            {
                if (it.relaxedhash < other.relaxedhash) { return -1; }
                else if (it.relaxedhash > other.relaxedhash) { return 1; }
                else if (it.created_at < other.created_at) { return -1; }
                else if (it.created_at > other.created_at) { return 1; }
                else { return 0; }
            }

            public int CompareTo(object obj)
            {
                return Compare(this, (texthashdata)obj);
            }
        }

        HashSet<long> FindTextPakurier()
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
text, user_id, created_at
FROM tweet
NATURAL JOIN user
WHERE text IS NOT NULL
AND created_at >= @created_at
AND isprotected = 0
AND (favorite_count >= 100 OR retweet_count >= 100)
;"))
            {
                cmd.Parameters.AddWithValue("@created_at", NowHour().AddDays(-7));
                Table = SelectTable(cmd);
            }
            List<texthashdata> hashes = new List<texthashdata>(Table.Rows.Count);
            foreach (DataRow row in Table.Rows)
            {
                string RelaxedText = TextHash.RelaxedText(row[0] as string);
                if (RelaxedText.Length >= 3)
                {
                    hashes.Add(new texthashdata(TextHash.Hash(RelaxedText), row[1] as long? ?? 0, row[2] as long? ?? 0));
                }
            }
            Table = null;
            hashes.Sort();
            HashSet<long> Pakuriers = new HashSet<long>();
            HashSet<long> Users = new HashSet<long>();
            for (int i = 0; i < hashes.Count - 1; i++)
            {
                if (hashes[i].relaxedhash == hashes[i + 1].relaxedhash)
                {
                    Users.Add(hashes[i].user_id);
                    if (!Users.Contains(hashes[i + 1].user_id))
                    {
                        Pakuriers.Add(hashes[i + 1].user_id);
                    }
                }
                else
                {
                    Users.Clear();
                }
            }
            return Pakuriers;
        }

        public static class TextHash
        {
            public static string RelaxedText(string Text)
            {
                string ret;
                ret = Regex.Replace(Text, @"(?<before>^|.*[\s　])(?<hashtag>[#＃][a-z0-9_À-ÖØ-öø-ÿĀ-ɏɓ-ɔɖ-ɗəɛɣɨɯɲʉʋʻ̀-ͯḀ-ỿЀ-ӿԀ-ԧⷠ-ⷿꙀ-֑ꚟ-ֿׁ-ׂׄ-ׇׅא-תװ-״﬒-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﭏؐ-ؚؠ-ٟٮ-ۓە-ۜ۞-۪ۨ-ۯۺ-ۼۿݐ-ݿࢠࢢ-ࢬࣤ-ࣾﭐ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-ﷻﹰ-ﹴﹶ-ﻼ‌ก-ฺเ-๎ᄀ-ᇿ㄰-ㆅꥠ-꥿가-힯ힰ-퟿ﾡ-ￜァ-ヺー-ヾｦ-ﾟｰ０-９Ａ-Ｚａ-ｚぁ-ゖ゙-ゞ㐀-䶿一-鿿꜀-뜿띀-렟-﨟〃々〻]+)", "${before}", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                ret = Regex.Replace(ret, @"s?https?://[-_.!~*'()a-zA-Z0-9;/?:@&=+$,%#]+", "", RegexOptions.Compiled);
                ret = Regex.Replace(ret, @"[\r\n\s　「」｢｣【】()（）『』。、.,!！?？・…'""’”]", "", RegexOptions.Compiled);
                return ret;
            }

            static xxHash xxhasher = new xxHash(64);
            public static long Hash(string Text)
            {
                return BitConverter.ToInt64(xxhasher.ComputeHash(Text), 0);
            }

            public static long RelaxedHash(string Text)
            {
                return Hash(RelaxedText(Text));
            }
        }

        public void RemoveOrphanProfileImage()
        {
            int RemoveCount = 0;
            ParallelOptions op = new ParallelOptions();
            op.MaxDegreeOfParallelism = 8;
            IEnumerable<string> Files = Directory.EnumerateFiles(config.crawl.PictPathProfileImage);
            Parallel.ForEach(Files, op, (string f) =>
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

        public void UpdateisProtected()
        {
            ServicePointManager.ReusePort = true;
            const int ConnectionLimit = 16;
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
                tokens[i] = new Tokens();
                tokens[i].ConsumerKey = config.token.ConsumerKey;
                tokens[i].ConsumerSecret = config.token.ConsumerSecret;
                tokens[i].AccessToken = Table.Rows[i][1] as string;
                tokens[i].AccessTokenSecret = Table.Rows[i][2] as string;
                tokens[i].UserId = (long)Table.Rows[i][0];
                tokens[i].ConnectionOptions.DisableKeepAlive = false;
                tokens[i].ConnectionOptions.UseCompression = true;
                tokens[i].ConnectionOptions.UseCompressionOnStreaming = true;
            }

            int tokenindex = -1;
            object tokenindexlock = new object();
            var UpdateUserBlock = new TransformBlock<long[], int>((long[] user_id) => {
                int i;
                lock (tokenindexlock)
                {
                    if (tokenindex >= tokens.Length) { tokenindex = 0; }
                    i = tokenindex;
                    tokenindex++;
                }
                CoreTweet.Core.ListedResponse<Status> users = tokens[i].Statuses.Lookup(user_id);
                List<MySqlCommand> cmd = new List<MySqlCommand>();
                foreach (Status user in users)
                {
                    Console.WriteLine("{0}\t{1}", user.User.Id, user.User.IsProtected);
                    /*
                    MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE user SET isprotected = @isprotected WHERE user_id = @user_id;");
                    cmdtmp.Parameters.AddWithValue("@isprotected", user.User.IsProtected);
                    cmdtmp.Parameters.AddWithValue("@user_id", user.User.Id);
                    cmd.Add(cmdtmp);
                    */
                }
                return 0;
                //return ExecuteNonQuery(cmd);
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ConnectionLimit });
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM user ORDER BY user_id LIMIT 100;"))
            {
                cmd.Parameters.AddWithValue("@limit", BulkUnit);
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted, true);
            }
            while (Table != null && Table.Rows.Count > 0)
            {
                long[] user_id = new long[Table.Rows.Count];
                for (int i = 0; i < Table.Rows.Count; i++)
                {
                    user_id[i] = (long)Table.Rows[i][0];
                }

                using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM user WHERE user_id > @lastid ORDER BY user_id LIMIT 100;"))
                {
                    cmd.Parameters.AddWithValue("@lastid", (long)Table.Rows[Table.Rows.Count - 1][0]);
                    Table = SelectTable(cmd, IsolationLevel.ReadUncommitted, true);
                }
            }
            UpdateUserBlock.Complete();
            UpdateUserBlock.Completion.Wait();
        }

        public void ReHash()
        {
            int updated = 0;
            ServicePointManager.ReusePort = true;
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT m.media_id, m.media_url, m.dcthash
FROM media m
ORDER BY downloaded_at DESC limit 100;"))
            {
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted, true);
            }
            Console.WriteLine("{0} media to rehash",Table.Rows.Count);
            Console.ReadKey();
            foreach (DataRow row in Table.Rows)
            {
                long? hash = downloadforHash(row[1] as string + ":thumb");
                if(hash == null) { Console.WriteLine("null"); continue; }
                Console.WriteLine("{0:x}\t{1:x}", hash, hash ^ (long)row[2]);
                /*
                using (MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE media SET dcthash=@dcthash WHERE media_id = @media_id"))
                {
                    cmdtmp.Parameters.AddWithValue("@dcthash", hash);
                    cmdtmp.Parameters.AddWithValue("@media_id", (long)row[0]);
                    updated += ExecuteNonQuery(cmdtmp, IsolationLevel.ReadUncommitted, true);
                    Console.WriteLine("{0} {1} hashes updated.", DateTime.Now, updated);
                }
                */
            }
        }

        public void ReHashDataflow()
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
                Table = SelectTable(cmd,IsolationLevel.ReadUncommitted,true);
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
                    Table = SelectTable(cmd, IsolationLevel.ReadUncommitted, true);
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
                            updated += ExecuteNonQuery(cmdtmp, true);
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
                        updated += ExecuteNonQuery(cmdtmp, true);
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
                    return PictHash.dcthash(mem);
                }
            }
            catch { return null; }
        }
    }
}

