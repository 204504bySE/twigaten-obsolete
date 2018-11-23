using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using MySql.Data.MySqlClient;
using System.Text;
using System.Data;
using System.Threading;
using System.IO;
using System.Diagnostics;
using IniParser;
using IniParser.Model;

namespace twitenlib
{
    ///<summary>iniファイル読むやつ</summary>
    public class Config
    {
        private static Config _config = new Config();
        private Config()
        {
            try
            {
                string iniPath = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location), "twiten.ini");
                FileIniDataParser ini = new FileIniDataParser();
                IniData data = ini.ReadFile(iniPath);
                token = new _token(data);
                crawl = new _crawl(data);
                crawlparent = new _crawlparent(data);
                locker = new _locker(data);
                hash = new _hash(iniPath, ini, data);
                database = new _database(data);
            }
            catch { }   //twiviewではこのconfigクラスは使用しない
        }

        //singletonはこれでインスタンスを取得して使う
        public static Config Instance
        {
            get { return _config; }
        }

        public class _token
        {
            public string ConsumerKey { get; }
            public string ConsumerSecret { get; }
            public _token(IniData data)
            {
                ConsumerKey = data["token"]["ConsumerKey"];
                ConsumerSecret = data["token"]["ConsumerSecret"];
            }
        }
        public _token token;

        public class _crawl
        {
            public string PictPathProfileImage { get; }
            public string MountPointProfileImage { get; }
            public string PictPaththumb { get; }
            public string MountPointthumb { get; }
            public int UserStreamTimeout { get; }
            public int UserStreamTimeoutTweets { get; }
            public int DefaultConnections { get; }
            public int MaxDBConnections { get; }
            public int RestTweetThreads { get; }
            public int ReconnectThreads { get; }
            public int MediaDownloadThreads { get; }
            public int DeleteTweetBufferSize { get; }
            public int LockedTokenPostpone { get; }
            public int LockerUdpPort { get; }
            public int TweetLockSize { get; }
            public _crawl(IniData data)
            {
                PictPathProfileImage = data["crawl"][nameof(PictPathProfileImage)] ?? Path.Combine(Directory.GetCurrentDirectory(), @"pict\profile_image\");
                MountPointProfileImage = data["crawl"][nameof(MountPointProfileImage)] ?? PictPathProfileImage?.Substring(0,1) ?? "";
                PictPaththumb = data["crawl"][nameof(PictPaththumb)] ?? Path.Combine(Directory.GetCurrentDirectory(), @"pict\thumb\");
                MountPointthumb = data["crawl"][nameof(MountPointthumb)] ?? PictPaththumb?.Substring(0,1) ?? "";
                UserStreamTimeout = int.Parse(data["crawl"][nameof(UserStreamTimeout)] ?? "180");
                UserStreamTimeoutTweets = int.Parse(data["crawl"][nameof(UserStreamTimeoutTweets)] ?? "50");
                DefaultConnections = int.Parse(data["crawl"][nameof(DefaultConnections)] ?? "100");
                MaxDBConnections = int.Parse(data["crawl"][nameof(MaxDBConnections)] ?? "10");
                RestTweetThreads = int.Parse(data["crawl"][nameof(RestTweetThreads)] ?? Environment.ProcessorCount.ToString());
                ReconnectThreads = int.Parse(data["crawl"][nameof(ReconnectThreads)] ?? Environment.ProcessorCount.ToString());
                MediaDownloadThreads = int.Parse(data["crawl"][nameof(MediaDownloadThreads)] ?? Environment.ProcessorCount.ToString());
                DeleteTweetBufferSize = int.Parse(data["crawl"][nameof(DeleteTweetBufferSize)] ?? "1000");
                LockedTokenPostpone = int.Parse(data["crawl"][nameof(LockedTokenPostpone)] ?? "86400");
                LockerUdpPort = int.Parse(data["crawl"][nameof(LockerUdpPort)] ?? "48250");
                TweetLockSize = int.Parse(data["crawl"][nameof(TweetLockSize)] ?? "10000");
                //http://absg.hatenablog.com/entry/2014/07/03/195043
                //フォロー6000程度でピークは60ツイート/分程度らしい
            }
        }
        public _crawl crawl;

        public class _crawlparent
        {
            public int AccountLimit { get; }
            public string ChildPath { get; }
            public string LockerPath { get; }
            public bool UseDotNet { get; }

            public _crawlparent(IniData data)
            {
                AccountLimit = int.Parse(data["crawlparent"][nameof(AccountLimit)] ?? "250");
                ChildPath = data["crawlparent"][nameof(ChildPath)] ?? "";
                LockerPath = data["crawlparent"][nameof(LockerPath)] ?? "";
                UseDotNet = bool.Parse(data["crawlparent"][nameof(UseDotNet)] ?? "false");

                //http://absg.hatenablog.com/entry/2014/07/03/195043
                //フォロー6000程度でピークは60ツイート/分程度らしい
            }
        }
        public _crawlparent crawlparent;

        public class _locker
        {
            public int UdpPort { get; }
            public int TweetLockSize { get; }

            public _locker(IniData data)
            {
                UdpPort = int.Parse(data["locker"][nameof(UdpPort)] ?? "48250");
                TweetLockSize = int.Parse(data["locker"][nameof(TweetLockSize)] ?? "65536");
            }
        }
        public _locker locker;

        public class _hash
        {
            readonly string iniPath;
            readonly FileIniDataParser ini;
            readonly IniData data;
            public int MaxHammingDistance { get; }
            public int ExtraBlocks { get; }
            public long LastUpdate { get; }
            public long LastHashCount { get; }
            public int HashCountOffset { get; }
            public bool KeepDataRAM { get; }
            public string TempDir { get; }
            public int InitialSortFileSize { get; }
            public _hash(string iniPath, FileIniDataParser ini, IniData data)
            {
                this.iniPath = iniPath; this.ini = ini; this.data = data;
                MaxHammingDistance = int.Parse(data["hash"][nameof(MaxHammingDistance)] ?? "3");
                ExtraBlocks = int.Parse(data["hash"][nameof(ExtraBlocks)] ?? "1");
                LastUpdate = long.Parse(data["hash"][nameof(LastUpdate)] ?? "0");
                LastHashCount = long.Parse(data["hash"][nameof(LastHashCount)] ?? "0");
                HashCountOffset = int.Parse(data["hash"][nameof(HashCountOffset)] ?? "5000000");
                KeepDataRAM = bool.Parse(data["hash"][nameof(KeepDataRAM)] ?? "false");
                TempDir = data["hash"][nameof(TempDir)] ?? "";
                InitialSortFileSize = int.Parse(data["hash"][nameof(InitialSortFileSize)] ?? "16777216");
            }
            public void NewLastUpdate(long time)
            {
                data["hash"][nameof(LastUpdate)] = time.ToString();
                ini.WriteFile(iniPath, data);
            }
            public void NewLastHashCount(long Count)
            {
                data["hash"][nameof(LastHashCount)] = Count.ToString();
                ini.WriteFile(iniPath, data);
            }
        }
        public _hash hash;

        public class _database
        {
            public string Address { get; }
            public _database(IniData data)
            {
                Address = data["database"][nameof(Address)] ?? "localhost";
            }
        }
        public _database database;
    }

    public class DBHandler
    {
        readonly string ConnectionStr;
        public DBHandler(string user, string pass, string server ="localhost", uint timeout = 20, uint poolsize = 40, uint lifetime = 3600)
        {
            if(lifetime < timeout) { throw new ArgumentException("lifetime < timeout"); }
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder()
            {
                Server = server,
                Database = "twiten",
                UserID = user,
                Password = pass,
                MinimumPoolSize = 1,
                MaximumPoolSize = poolsize,    //デフォルトは100
                ConnectionLifeTime = lifetime,
                CharacterSet = "utf8mb4",
                DefaultCommandTimeout = timeout    //デフォルトは20(秒
            };
            ConnectionStr = builder.ToString();            
        }
        MySqlConnection NewConnection()
        {
            return new MySqlConnection(ConnectionStr);
        }

        //"(@a1,@b1),(@a2,@b2)…;" という文字列を出すだけ
        //bulk insertとかこれ使おうな
        protected static string BulkCmdStr(int count, int unit, string head)
        {
            if(26 < unit) { throw new ArgumentOutOfRangeException("26 < unit"); }
            StringBuilder BulkCmd = new StringBuilder(head);
            for (int i = 0; i < count; i++)
            {
                BulkCmd.Append("(@");
                for (int j = 0; j < unit - 1; j++)
                {
                    BulkCmd.Append(Convert.ToChar(0x61 + j));
                    BulkCmd.Append(i);
                    BulkCmd.Append(",@");
                }
                BulkCmd.Append(Convert.ToChar(0x61 + unit - 1));
                BulkCmd.Append(i);
                BulkCmd.Append("),");
            }
            BulkCmd.Remove(BulkCmd.Length - 1, 1)
                .Append(';');
            return BulkCmd.ToString();
        }

        //(@1,@2,@3…);  という文字列
        protected static string BulkCmdStrIn(int count, string head)
        {
            StringBuilder BulkCmd = new StringBuilder(head);
            BulkCmd.Append("(@");
            for (int i = 0; i < count; i++)
            {
                BulkCmd.Append(i);
                BulkCmd.Append(",@");
            }
            BulkCmd.Remove(BulkCmd.Length - 2, 2);
            BulkCmd.Append(");");
            return BulkCmd.ToString();
        }

        protected DataTable SelectTable(MySqlCommand cmd, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted)
        {
            try
            {
                DataTable ret;
                using (MySqlConnection conn = NewConnection())
                {
                    conn.Open();
                    using (MySqlTransaction tran = conn.BeginTransaction(IsolationLevel))
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tran;
                        using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                        {
                            ret = new DataTable();
                            adapter.Fill(ret);
                        }
                        tran.Commit();
                    }
                }
                return ret;
            }
            catch { return null; }
        }

        protected long SelectCount(MySqlCommand cmd, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted)
        {
            //SELECT COUNT() 用
            try
            {
                long? ret;
                using (MySqlConnection conn = NewConnection())
                {
                    conn.Open();
                    using (MySqlTransaction tran = conn.BeginTransaction(IsolationLevel))
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tran;
                        ret = cmd.ExecuteScalar() as long?;
                        tran.Commit();
                    }
                }
                return ret ?? -1;
            }
            catch { return -1; }
        }

        protected int ExecuteNonQuery(MySqlCommand cmd)
        {
            return ExecuteNonQuery(new MySqlCommand[] { cmd });
        }

        protected int ExecuteNonQuery(IEnumerable<MySqlCommand> cmd)
        {
            //<summary>
            //MysqlConnectionとMySQLTransactionを張ってcmdを実行する
            //戻り値はDBの変更された行数
            //失敗したら-1
            //</summary>
            try
            {
                int ret = 0;
                using (MySqlConnection conn = NewConnection())
                {
                    conn.Open();
                    using (MySqlTransaction tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted))
                    {
                        foreach (MySqlCommand c in cmd)
                        {
                            c.Connection = conn;
                            c.Transaction = tran;
                            ret += c.ExecuteNonQuery();
                        }
                        tran.Commit();
                    }
                }
                return ret;
            }
            catch { return -1; }
        }
    }
    
    static class SnowFlake
    {
        public const long msinSnowFlake = 0x400000L;   //1msはこれだ
        const long TwEpoch = 1288834974657L;
        public static long SecondinSnowFlake(long TimeSeconds, bool Larger)
        {
            if (Larger) { return (TimeSeconds * 1000 + 999 - TwEpoch) << 22 | 0x3FFFFFL; }
            else { return (TimeSeconds * 1000 - TwEpoch) << 22; }
        }
        public static long SecondinSnowFlake(DateTimeOffset TimeSeconds, bool Larger)
        {
            return SecondinSnowFlake(TimeSeconds.ToUnixTimeSeconds(), Larger);
        }
        　
        public static long Now(bool Larger)
        {
            if (Larger) { return (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TwEpoch) << 22 | 0x3FFFFFL; }
            else { return (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TwEpoch) << 22; }
        }
        public static DateTimeOffset DatefromSnowFlake(long SnowFlake)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds((SnowFlake >> 22) + TwEpoch);
        }
    }

    static class CharCodes
    {
        static Encoding ascii = Encoding.GetEncoding(1252, new EncoderReplacementFallback(""), DecoderFallback.ReplacementFallback);
        public static string KillNonASCII(string Str)
        {
            if (Str == null) { return null; }
            //ASCII範囲外は消しちゃう(めんどくさい
            byte[] bytes = ascii.GetBytes(Str);
            return ascii.GetString(bytes);
        }
    }

    static class CheckOldProcess
    {
        public static void CheckandExit()
        {   //同名のプロセスがすでに動いていたら終了する
            Process CurrentProc = Process.GetCurrentProcess();
            Process[] proc = Process.GetProcessesByName(CurrentProc.ProcessName);
            foreach (Process p in proc)
            {
                if (p.Id != CurrentProc.Id)
                {
                    Console.WriteLine("{0} Another Instance of {1} is Running.", DateTime.Now, CurrentProc.ProcessName);
                    Thread.Sleep(5000);
                    Environment.Exit(1);
                }
            }
        }
    }

}