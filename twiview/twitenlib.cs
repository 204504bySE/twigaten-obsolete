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
            public int StreamersConnectTimeout { get; }
            public int DefaultConnections { get; }
            public int ConnectionCountFactor { get; }
            public int MaxDBConnections { get; }
            public bool RestConnected { get; }
            public _crawl(IniFileHandler ini)
            {
                PictPathProfileImage = ini.getvalue("crawl", "PictPathProfileImage", Directory.GetCurrentDirectory() + @"\pict\profile_image");
                PictPaththumb = ini.getvalue("crawl", "PictPaththumb", Directory.GetCurrentDirectory() + @"\pict\thumb");
                UserStreamTimeout = int.Parse(ini.getvalue("crawl", "UserStreamTimeout", "180"));
                UserStreamTimeoutTweets = int.Parse(ini.getvalue("crawl", "UserStreamTimeoutTweets", "50"));
                StreamersConnectTimeout = 1000 * int.Parse(ini.getvalue("crawl", "StreamersConnectTimeout", "600"));
                DefaultConnections = int.Parse(ini.getvalue("crawl", "DefaultConnections", "10"));
                ConnectionCountFactor = int.Parse(ini.getvalue("crawl", "ConnectionCountFactor", "3"));
                MaxDBConnections = int.Parse(ini.getvalue("crawl", "MaxDBConnections", "10"));
                RestConnected = bool.Parse(ini.getvalue("crawl", "RestConnected", "true"));
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
            public bool ChildSingleThread { get; }
            public bool InitTruncate { get; }
            public _crawlparent(IniFileHandler ini)
            {
                AccountLimit = int.Parse(ini.getvalue("crawlparent", "AccountLimit", "250"));
                ChildPath = ini.getvalue("crawlparent", "ChildPath", "");
                ChildName = ini.getvalue("crawlparent", "ChildName", "twidown");
                ChildSingleThread = bool.Parse(ini.getvalue("crawlparent", "ChildSingleThread", "false"));
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
            public double HashCountFactor { get; }
            public _hash(IniFileHandler ini)
            {
                this.ini = ini;
                MaxHammingDistance = int.Parse(ini.getvalue("hash", "MaxHammingDistance", "3"));
                ExtraBlocks = int.Parse(ini.getvalue("hash", "ExtraBlocks", "1"));
                LastUpdate = long.Parse(ini.getvalue("hash", "LastUpdate", "0"));
                LastHashCount = int.Parse(ini.getvalue("hash", "LastHashCount", "0"));
                HashCountFactor = double.Parse(ini.getvalue("hash", "HashCountFactor", "1.05"));
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
            public string ConsumerKey { get; }
            public string ConsumerSecret { get; }
            public string AccessToken { get; }
            public string AccessTokenSecret { get; }
            public _bot(IniFileHandler ini)
            {
                ConsumerKey = ini.getvalue("bot", "ConsumerKey","");
                ConsumerSecret = ini.getvalue("bot", "ConsumerSecret", "");
                AccessToken = ini.getvalue("bot", "AccessToken", "");
                AccessTokenSecret = ini.getvalue("bot", "AccessTokenSecret", "");
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
        readonly SemaphoreSlim ConnectionSemaphore;
        readonly int ConnectionSemaphoreTimeout;
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

            ConnectionSemaphore = new SemaphoreSlim((int)poolsize);
            ConnectionSemaphoreTimeout = (int)timeout * 1000;
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
            BulkCmd.Remove(BulkCmd.Length - 1, 1);
            BulkCmd.Append(";");
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

        protected DataTable SelectTable(MySqlCommand cmd, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted, bool NeverSemaphoreTimeout = false)
        {
            if (!ConnectionSemaphore.Wait(NeverSemaphoreTimeout ? -1 : ConnectionSemaphoreTimeout)){ return null; }
            DataTable ret = new DataTable();
            try
            {
                MySqlConnection conn = NewConnection();
                conn.Open();
                MySqlTransaction tran = conn.BeginTransaction(IsolationLevel);
                try
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tran;
                    using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                    {
                        adapter.Fill(ret);
                    }
                    tran.Commit();
                }
                catch { tran.Rollback(); ret = null; }
                finally
                {
                    conn.Close();
                }
            }
            catch { ret = null; }
            finally { ConnectionSemaphore.Release(); }
            return ret;
        }

        protected long SelectCount(MySqlCommand cmd, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted, bool NeverSemaphoreTimeout = false)
        {
            //SELECT COUNT(*) 用
            if (!ConnectionSemaphore.Wait(NeverSemaphoreTimeout ? -1 : ConnectionSemaphoreTimeout)) { return -1; }
            long ret;
            try
            {
                MySqlConnection conn = NewConnection();
                conn.Open();
                MySqlTransaction tran = conn.BeginTransaction(IsolationLevel);
                try
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tran;
                    ret = (long)cmd.ExecuteScalar();
                    tran.Commit();
                }
                catch { tran.Rollback(); ret = -1; }
                finally
                {
                    conn.Close();
                }
            }
            catch { ret = -1; }
            finally { ConnectionSemaphore.Release(); }
            return ret;
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
            if (!ConnectionSemaphore.Wait(NeverSemaphoreTimeout ? -1 : ConnectionSemaphoreTimeout)) { return 0; }
            int ret = 0;
            try
            {
                MySqlConnection conn = NewConnection();
                conn.Open();
                MySqlTransaction tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
                try
                {
                    foreach (MySqlCommand c in cmd)
                    {
                        c.Connection = conn;
                        c.Transaction = tran;
                        ret += c.ExecuteNonQuery();
                    }
                    tran.Commit();
                }
                catch
                {
                    //Console.WriteLine("\n{0}\n{1}", DateTime.Now, e);
                    tran.Rollback();
                    ret = 0;
                }
                finally
                {
                    conn.Close();
                }
            }
            catch
            {
                //Console.WriteLine("\n{0}\n{1}", DateTime.Now, e);
            }
            finally { ConnectionSemaphore.Release(); }
            return ret;
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