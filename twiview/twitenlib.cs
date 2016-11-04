using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Runtime.InteropServices;
using MySql.Data.MySqlClient;
using System.Text;
using System.Data;
using System.Threading;
using System.IO;

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

        public string getvalue(string section, string key, string defvalue = "")
        {
            StringBuilder sb = new StringBuilder(1024);
            GetPrivateProfileString(section, key, defvalue, sb, (uint)sb.Capacity, inipath);
            return sb.ToString();
        }

        public void setvalue(string section, string key, string value)
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
            IniFileHandler ini = new IniFileHandler(Directory.GetCurrentDirectory() + @"\twiten.ini");
            token = new _token(ini);
            crawl = new _crawl(ini);
            crawlparent = new _crawlparent(ini);
            hash = new _hash(ini);
            database = new _database(ini);
            bot = new _bot(ini);
        }

        public class _token
        {
            public string ConsumerKey { get; }
            public string ConsumerSecret { get; }
            public _token(IniFileHandler ini)
            {
                ConsumerKey = ini.getvalue("token", "ConsumerKey");
                ConsumerSecret = ini.getvalue("token", "ConsumerSecret");
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
            public int MediaKeepAlive { get; }
            public int RestThreads { get; }
            public int LockedTokenPostpone { get; }
            public _crawl(IniFileHandler ini)
            {
                PictPathProfileImage = ini.getvalue("crawl", "PictPathProfileImage", Directory.GetCurrentDirectory() + @"\pict\profile_image");
                PictPaththumb = ini.getvalue("crawl", "PictPaththumb", Directory.GetCurrentDirectory() + @"\pict\thumb");
                UserStreamTimeout = int.Parse(ini.getvalue("crawl", "UserStreamTimeout", "180"));
                UserStreamTimeoutTweets = int.Parse(ini.getvalue("crawl", "UserStreamTimeoutTweets", "50"));
                DefaultConnections = int.Parse(ini.getvalue("crawl", "DefaultConnections", "100"));
                MaxDBConnections = int.Parse(ini.getvalue("crawl", "MaxDBConnections", "10"));
                MediaKeepAlive = int.Parse(ini.getvalue("crawl", "MediaKeepAlive", "30")) * 1000;
                RestThreads = int.Parse(ini.getvalue("crawl", "RestThreads", Environment.ProcessorCount.ToString()));
                LockedTokenPostpone = int.Parse(ini.getvalue("crawl", "LockedTokenPostpone", "86400"));
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
                AccountLimit = int.Parse(ini.getvalue("crawlparent", "AccountLimit", "250"));
                ChildPath = ini.getvalue("crawlparent", "ChildPath", "");
                ChildName = ini.getvalue("crawlparent", "ChildName", "twidown");
                InitTruncate = bool.Parse(ini.getvalue("crawlparent", "InitTruncate", "true"));
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
            public _hash(IniFileHandler ini)
            {
                this.ini = ini;
                MaxHammingDistance = int.Parse(ini.getvalue("hash", "MaxHammingDistance", "3"));
                ExtraBlocks = int.Parse(ini.getvalue("hash", "ExtraBlocks", "1"));
                LastUpdate = long.Parse(ini.getvalue("hash", "LastUpdate", "0"));
                LastHashCount = int.Parse(ini.getvalue("hash", "LastHashCount", "0"));
                HashCountOffset = int.Parse(ini.getvalue("hash", "HashCountOffset", "5000000"));
            }
            public void NewLastUpdate(long time)
            {
                ini.setvalue("hash", "LastUpdate", time.ToString());
            }
            public void NewLastHashCount(int Count)
            {
                ini.setvalue("hash", "LastHashCount", Count.ToString());
            }
        }
        public _hash hash;

        public class _database
        {
            public string Address { get; }
            public string AddressLock { get; }
            public _database(IniFileHandler ini)
            {
                Address = ini.getvalue("database", "Address", "localhost");
                AddressLock = ini.getvalue("database", "AddressLock", "localhost");
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
                ConsumerKey = ini.getvalue("bot", "ConsumerKey","");
                ConsumerSecret = ini.getvalue("bot", "ConsumerSecret", "");
                AccessToken = ini.getvalue("bot", "AccessToken", "");
                AccessTokenSecret = ini.getvalue("bot", "AccessTokenSecret", "");
                LastTweetTime = int.Parse(ini.getvalue("bot", "LastTweetTime", "0"));
                LastPakurierTime = int.Parse(ini.getvalue("bot", "LastPakurierTime", "0"));
            }

            public void NewLastTweetTime(long time)
            {
                ini.setvalue("bot", "LastTweetTime", time.ToString());
            }
            public void NewLastPakurierTime(long time)
            {
                ini.setvalue("bot", "LastPakurierTime", time.ToString());
            }
        }
        public _bot bot;

        //singletonはこれでインスタンスを取得して使う
        public static Config Instance
        {
            get { return _config; }
        }
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
                    BulkCmd.Append(Convert.ToChar(0x61 + j))
                        .Append(i)
                        .Append(",@");
                }
                BulkCmd.Append(Convert.ToChar(0x61 + unit - 1))
                    .Append(i)
                    .Append("),");
            }
            BulkCmd.Remove(BulkCmd.Length - 1, 1)
                .Append(";");
            return BulkCmd.ToString();
        }

        //(@1,@2,@3…);  という文字列
        protected string BulkCmdStrIn(int count, string head)
        {
            StringBuilder BulkCmd = new StringBuilder(head);
            BulkCmd.Append("(@");
            for (int i = 0; i < count; i++)
            {
                BulkCmd.Append(i)
                    .Append(",@");
            }
            BulkCmd.Remove(BulkCmd.Length - 2, 2)
                .Append(");");
            return BulkCmd.ToString();
        }

        protected DataTable SelectTable(MySqlCommand cmd, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted, bool NeverSemaphoreTimeout = false)
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

        protected long SelectCount(MySqlCommand cmd, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted, bool NeverSemaphoreTimeout = false)
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

        protected int ExecuteNonQuery(MySqlCommand cmd, bool NeverSemaphoreTimeout = false)
        {
            return ExecuteNonQuery(new MySqlCommand[] { cmd }, NeverSemaphoreTimeout);
        }

        protected int ExecuteNonQuery(IEnumerable<MySqlCommand> cmd, bool NeverSemaphoreTimeout = false)
        {
            //<summary>
            //MysqlConnectionとMySQLTransactionを張ってcmdを実行する
            //戻り値はDBの変更された行数
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

        //時刻→SnowFlake Larger→時刻じゃないビットを1で埋める
        protected long TimeinSnowFlake(long UnixTimeSeconds, bool Larger)
        {
            const long TwEpoch = 1288834974657L;
            if (Larger) { return (UnixTimeSeconds * 1000 + 999 - TwEpoch) << 22 | 0x3FFFFFL; }
            else { return (UnixTimeSeconds * 1000 - TwEpoch) << 22; }
        }
        protected const long msinSnowFlake = 0x400000L;   //1msはこれだ
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
    

    public static class localstrs
    {
        //画像の保存先を雑に返す config.pathはここでは付加しない
        //profile_imageにしか使ってないねきっと
        public static string localmediapath(Uri u)
        {
            return removeinvalidchars(u.Authority + u.LocalPath);
        }
        public static string localmediapath(string s)
        {
            if(s == null) { return null; }
            return localmediapath(new Uri(s));
        }
        static string removeinvalidchars(string s)
        //ファイルに使えない文字とUnicode制御文字を'_'にする
        //http://dobon.net/vb/dotnet/string/removecharacters.html
        {
            StringBuilder buf = new StringBuilder();
            foreach (char c in s)
            {
                if (char.IsControl(c))
                {
                    buf.Append('_');
                    continue;
                }
                else
                {
                    foreach (char i in System.IO.Path.GetInvalidFileNameChars())
                    {
                        if (c == i)
                        {
                            buf.Append('_');
                            goto ICONT;
                        }
                    }
                    buf.Append(c);
                }
                ICONT:;
            }
            return buf.ToString();
        }
    }
}