﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Runtime.InteropServices;
using MySql.Data.MySqlClient;
using System.Text;
using System.Data;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace twitenlib
{
    public class IniFileHandler
    //http://www.atmarkit.co.jp/fdotnet/dotnettips/039inifile/inifile.html
    {
        [DllImport("KERNEL32.DLL")]
        static extern uint
          GetPrivateProfileString(string lpAppName,
          string lpKeyName, string lpDefault,
          StringBuilder lpReturnedString, uint nSize,
          string lpFileName);

        [DllImport("KERNEL32.DLL",
            EntryPoint = "GetPrivateProfileStringA")]
        static extern uint
          GetPrivateProfileStringByByteArray(string lpAppName,
          string lpKeyName, string lpDefault,
          byte[] lpReturnedString, uint nSize,
          string lpFileName);

        [DllImport("KERNEL32.DLL")]
        static extern uint
           GetPrivateProfileInt(string lpAppName,
           string lpKeyName, int nDefault, string lpFileName);

        [DllImport("KERNEL32.DLL")]
        static extern uint WritePrivateProfileString(
          string lpAppName,
          string lpKeyName,
          string lpString,
          string lpFileName);

        string inipath;

        public IniFileHandler(string path)
        {
            inipath = path;
        }

        public string GetValue(string section, string key, string defvalue = "")
        {
            StringBuilder sb = new StringBuilder(1024);
            GetPrivateProfileString(section, key, defvalue, sb, (uint)sb.Capacity, inipath);
            return sb.ToString();
        }

        public void SetValue(string section, string key, string value)
        {
            WritePrivateProfileString(section, key, value, inipath);
        }
    }

    public class Config
    {
        //<summary>
        //iniファイル読むやつ Singleton
        //</summary>
        private static Config _config = new Config();
        private Config()
        {
            try
            {
                IniFileHandler ini = new IniFileHandler(Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + @"\twiten.ini");
                token = new _token(ini);
                crawl = new _crawl(ini);
                crawlparent = new _crawlparent(ini);
                hash = new _hash(ini);
                database = new _database(ini);
                bot = new _bot(ini);
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
            public _token(IniFileHandler ini)
            {
                ConsumerKey = ini.GetValue("token", "ConsumerKey");
                ConsumerSecret = ini.GetValue("token", "ConsumerSecret");
            }
        }
        public _token token;

        public class _crawl
        {
            public string PictPathProfileImage { get; }
            public string PictPaththumb { get; }
            public int UserStreamTimeout { get; }
            public int UserStreamTimeoutTweets { get; }
            public int DefaultConnections { get; }
            public int MaxDBConnections { get; }
            public int RestTweetThreads { get; }
            public int ReconnectThreads { get; }
            public int LockedTokenPostpone { get; }
            public _crawl(IniFileHandler ini)
            {
                PictPathProfileImage = ini.GetValue("crawl", "PictPathProfileImage", Directory.GetCurrentDirectory() + @"\pict\profile_image\");
                PictPaththumb = ini.GetValue("crawl", "PictPaththumb", Directory.GetCurrentDirectory() + @"\pict\thumb\");
                UserStreamTimeout = int.Parse(ini.GetValue("crawl", "UserStreamTimeout", "180"));
                UserStreamTimeoutTweets = int.Parse(ini.GetValue("crawl", "UserStreamTimeoutTweets", "50"));
                DefaultConnections = int.Parse(ini.GetValue("crawl", "DefaultConnections", "100"));
                MaxDBConnections = int.Parse(ini.GetValue("crawl", "MaxDBConnections", "10"));
                RestTweetThreads = int.Parse(ini.GetValue("crawl", "RestTweetThreads", Environment.ProcessorCount.ToString()));
                ReconnectThreads = int.Parse(ini.GetValue("crawl", "ReconnectThreads", Environment.ProcessorCount.ToString()));
                LockedTokenPostpone = int.Parse(ini.GetValue("crawl", "LockedTokenPostpone", "86400"));
                //http://absg.hatenablog.com/entry/2014/07/03/195043
                //フォロー6000程度でピークは60ツイート/分程度らしい
            }
        }
        public _crawl crawl;

        public class _crawlparent
        {
            public int AccountLimit { get; }
            public string ChildPath { get; }
            public string ChildName { get; }
            public bool InitTruncate { get; }
            public _crawlparent(IniFileHandler ini)
            {
                AccountLimit = int.Parse(ini.GetValue("crawlparent", "AccountLimit", "250"));
                ChildPath = ini.GetValue("crawlparent", "ChildPath", "");
                ChildName = ini.GetValue("crawlparent", "ChildName", "twidown");
                InitTruncate = bool.Parse(ini.GetValue("crawlparent", "InitTruncate", "true"));
                //http://absg.hatenablog.com/entry/2014/07/03/195043
                //フォロー6000程度でピークは60ツイート/分程度らしい
            }
        }
        public _crawlparent crawlparent;

        public class _hash
        {
            IniFileHandler ini;
            public int MaxHammingDistance { get; }
            public int ExtraBlocks { get; }
            public long LastUpdate { get; }
            public int LastHashCount { get; }
            public int HashCountOffset { get; }
            public bool KeepDataRAM { get; }
            public _hash(IniFileHandler ini)
            {
                this.ini = ini;
                MaxHammingDistance = int.Parse(ini.GetValue("hash", "MaxHammingDistance", "3"));
                ExtraBlocks = int.Parse(ini.GetValue("hash", "ExtraBlocks", "1"));
                LastUpdate = long.Parse(ini.GetValue("hash", "LastUpdate", "0"));
                LastHashCount = int.Parse(ini.GetValue("hash", "LastHashCount", "0"));
                HashCountOffset = int.Parse(ini.GetValue("hash", "HashCountOffset", "5000000"));
                KeepDataRAM = bool.Parse(ini.GetValue("hash", "KeepDataRAM", "false"));
            }
            public void NewLastUpdate(long time)
            {
                ini.SetValue("hash", "LastUpdate", time.ToString());
            }
            public void NewLastHashCount(int Count)
            {
                ini.SetValue("hash", "LastHashCount", Count.ToString());
            }
        }
        public _hash hash;

        public class _database
        {
            public string Address { get; }
            public _database(IniFileHandler ini)
            {
                Address = ini.GetValue("database", "Address", "localhost");
            }
        }
        public _database database;

        public class _bot
        {
            IniFileHandler ini;
            public string ConsumerKey { get; }
            public string ConsumerSecret { get; }
            public string AccessToken { get; }
            public string AccessTokenSecret { get; }

            public long LastTweetTime { get; }
            public long LastPakurierTime { get; }

            public _bot(IniFileHandler ini)
            {
                this.ini = ini;
                ConsumerKey = ini.GetValue("bot", "ConsumerKey","");
                ConsumerSecret = ini.GetValue("bot", "ConsumerSecret", "");
                AccessToken = ini.GetValue("bot", "AccessToken", "");
                AccessTokenSecret = ini.GetValue("bot", "AccessTokenSecret", "");
                LastTweetTime = int.Parse(ini.GetValue("bot", "LastTweetTime", "0"));
                LastPakurierTime = int.Parse(ini.GetValue("bot", "LastPakurierTime", "0"));
            }

            public void NewLastTweetTime(long time)
            {
                ini.SetValue("bot", "LastTweetTime", time.ToString());
            }
            public void NewLastPakurierTime(long time)
            {
                ini.SetValue("bot", "LastPakurierTime", time.ToString());
            }
        }
        public _bot bot;
    }

    public class DBHandler
    {
        protected Config config = Config.Instance;
        readonly string ConnectionStr;
        public DBHandler(string user, string pass, string server ="localhost", uint timeout = 20, uint poolsize = 40, uint lifetime = 3600)
        {
            if(lifetime < timeout) { throw new ArgumentException("lifetime < timeout"); }
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder();
            builder.Server = server;
            builder.Database = "twiten";
            builder.UserID = user;
            builder.Password = pass;
            builder.MinimumPoolSize = 1;
            builder.MaximumPoolSize = poolsize;    //デフォルトは100
            builder.ConnectionLifeTime = lifetime;
            builder.CharacterSet = "utf8mb4";
            builder.DefaultCommandTimeout = timeout;    //デフォルトは20(秒
            ConnectionStr = builder.ToString();            
        }
        MySqlConnection NewConnection()
        {
            return new MySqlConnection(ConnectionStr);
        }

        //"(@a1,@b1),(@a2,@b2)…;" という文字列を出すだけ
        //bulk insertとかこれ使おうな
        protected string BulkCmdStr(int count, int unit, string head)
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
        protected string BulkCmdStrIn(int count, string head)
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
                long ret;
                using (MySqlConnection conn = NewConnection())
                {
                    conn.Open();
                    using (MySqlTransaction tran = conn.BeginTransaction(IsolationLevel))
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tran;
                        ret = (long)cmd.ExecuteScalar();
                        tran.Commit();
                    }
                }
                return ret;
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
                    Console.WriteLine("{0} Another Instance of {1} is Runnning.", DateTime.Now, CurrentProc.ProcessName);
                    Thread.Sleep(5000);
                    Environment.Exit(1);
                }
            }
        }
    }

}