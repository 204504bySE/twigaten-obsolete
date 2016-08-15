using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;
using MySql.Data.MySqlClient;
using System.Data;
using System.Text.RegularExpressions;
using System.IO;
using CoreTweet;
using twitenlib;


namespace twibot
{
    class Program
    {
        static void Main(string[] args)
        {
            oppai_hantai_bot bot = new oppai_hantai_bot();
            bot.TweetOne();
            Console.WriteLine("＼(^o^)／");
            Thread.Sleep(5000);
            //Console.ReadKey();
        }
    }

    class jsontest
    {
        IniFileHandler ini = new IniFileHandler(Directory.GetCurrentDirectory() + @"\twiten.ini");
        Config config = Config.Instance;
        Tokens Token;
        dbhandler db = new dbhandler();

        public jsontest()
        {
            Token = Tokens.Create(config.bot.ConsumerKey, config.bot.ConsumerSecret, config.bot.AccessToken, config.bot.AccessTokenSecret);
        }

        public void TweetOne()
        {
            try
            {
                StatusResponse s = Token.Statuses.Show(702806139772100608);
                Console.WriteLine(s.Json);
            }
            catch (Exception e) { Console.WriteLine("{0}\n{1}", DateTime.Now, e); }
        }
    }

    class oppai_hantai_bot
    {
        IniFileHandler ini = new IniFileHandler(Directory.GetCurrentDirectory() + @"\twiten.ini");
        Config config = Config.Instance;
        Tokens Token;
        dbhandler db = new dbhandler();

        public oppai_hantai_bot()
        {
            Token = Tokens.Create(config.bot.ConsumerKey, config.bot.ConsumerSecret, config.bot.AccessToken, config.bot.AccessTokenSecret);
        }

        public void TweetOne()
        {
            try
            {
                SearchResult searched = Token.Search.Tweets(new {q = "原発", count = 50});
                if (searched == null) { return; }

                foreach(Status t in searched)
                {
                    Status tweet;
                    if(t.RetweetedStatus != null) { tweet = t.RetweetedStatus; }
                    else{ tweet = t; }
                    if(tweet.InReplyToUserId != null
                       || tweet.InReplyToStatusId != null
                       || tweet.Text.IndexOf("://") >= 0)
                    { continue; }
                    string tweetstr = tweet.Text.Replace("原発", "おっぱい");
                    Token.Statuses.Update(tweetstr);
                    Console.WriteLine(tweetstr);
                    break;
                }
            }
            catch (Exception e) { Console.WriteLine("{0}\n{1}", DateTime.Now, e); }
        }
    }

    class copy_writing_bot
    {
        IniFileHandler ini = new IniFileHandler(Directory.GetCurrentDirectory() + @"\twiten.ini");
        Config config = Config.Instance;
        Tokens Token;
        dbhandler db = new dbhandler();

        public copy_writing_bot()
        {
            Token = Tokens.Create(config.bot.ConsumerKey, config.bot.ConsumerSecret, config.bot.AccessToken, config.bot.AccessTokenSecret);
        }

        public void TweetOne()
        {
            try
            {
                UserResponse prof = Token.Users.Show(344730147);
                if (prof == null) { return; }

                string tweetstr = string.Format("{0} 時点の Copy__writing のフォロワー数: {1} ({2})",
                    DateTime.Now, prof.FollowersCount,
                    prof.FollowersCount - int.Parse(ini.getvalue("copy_writing_bot", "LastFollowers", "0")));
                Token.Statuses.Update(tweetstr);
                Console.WriteLine(tweetstr);
                ini.setvalue("copy_writing_bot", "LastFollowers", prof.FollowersCount.ToString());
            }
            catch (Exception e) { Console.WriteLine("{0}\n{1}", DateTime.Now, e); }
        }
    }

    //NGワードを正規表現で指定できるぞ
    public class NGRegex
    {
        List<Regex> Regexes = new List<Regex>();
        public NGRegex()
        {
            try
            {

                using (StreamReader Reader = new StreamReader(Directory.GetCurrentDirectory() + @"\ngregex.txt"))
                {
                    string line;
                    while ((line = Reader.ReadLine()) != null)
                    {
                        Regexes.Add(new Regex(line));
                    }
                }
            }
            catch (Exception e) { Console.WriteLine(e); }
        }

        public bool isNG(string str)
        {
            foreach (Regex r in Regexes)
            {
                if (r.IsMatch(str)) { return true; }
            }
            return false;
        }
    }

        class Retweeter
        {
        Config config = Config.Instance;
        Tokens Token;
        dbhandler db = new dbhandler();
        
        public Retweeter()
        {
            Token = Tokens.Create(config.bot.ConsumerKey, config.bot.ConsumerSecret, config.bot.AccessToken, config.bot.AccessTokenSecret);
        }

        public void TweetOne()
        {
            List<long> Tweeted = RecentRetweets();
            if(Tweeted == null) { return; }
            List<long> Tweets = db.RecentPakuriedTweet();
            Console.WriteLine(Tweets.Count);
            Status r;
            foreach(long t in Tweets)
            {
                if (!Tweeted.Contains(t))
                {
                    try
                    {
                        r = Token.Statuses.Retweet(t);
                    }
                    catch (Exception e) { Console.WriteLine(e); Thread.Sleep(5000); continue; }
                    try
                    {
                        Console.WriteLine("{0}:{1}\n{2}", r.RetweetedStatus.User.ScreenName, r.RetweetedStatus.User.Name, r.RetweetedStatus.Text);
                        Console.WriteLine("Retweeted: {0}", t);
                    }
                    catch { }
                    return;
                }
            }
        }

        List<long> RecentRetweets()
        {
            List<long> ret = new List<long>();
            try
            {
                foreach (Status x in Token.Statuses.HomeTimeline(count => 200))
                {
                    if(x.RetweetedStatus != null)
                    {
                        ret.Add(x.RetweetedStatus.Id);
                    }
                }
                return ret;
            } catch(Exception e) { Console.WriteLine(e); return null; }
        }
    }

    class dbhandler : twitenlib.DBHandler
    {
        public dbhandler() : base("bot", "") { }

        public List<long> RecentPakuriedTweet()
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
t.tweet_id, t.text FROM tweet t
NATURAL JOIN user u
NATURAL JOIN tweet_media
NATURAL JOIN media m
WHERE u.isprotected = 0
AND t.tweet_id = m.source_tweet_id
AND (t.favorite_count >= 5 OR t.retweet_count >= 5)
AND NOT EXISTS (SELECT * FROM pakurier WHERE user_id = u.user_id)
AND EXISTS (SELECT * FROM 
mediapair pp
INNER JOIN media mm ON pp.media_id_sub = mm.media_id
INNER JOIN tweet_media on mm.media_id = tweet_media.media_id
NATURAL JOIN tweet tt
NATURAL JOIN user uu
WHERE pp.media_id_pri = m.media_id
AND tt.created_at >= @created_at
AND t.created_at < tt.created_at
AND EXISTS (SELECT * FROM pakurier WHERE user_id = uu.user_id)
)
AND NOT EXISTS (SELECT * FROM 
mediapair ppp
INNER JOIN media mmm ON ppp.media_id_sub = mmm.media_id
INNER JOIN tweet_media on mmm.media_id = tweet_media.media_id
NATURAL JOIN tweet ttt
NATURAL JOIN user uuu
WHERE ppp.media_id_pri = m.media_id
AND t.created_at > ttt.created_at
)
ORDER BY (SELECT COUNT(user_id) FROM block WHERE target_id = u.user_id) ASC,
(t.retweet_count + t.favorite_count) DESC LIMIT 10;"))
            {
                DateTimeOffset LastHashDate = DateTimeOffset.Now;
                LastHashDate = LastHashDate.AddMilliseconds(-LastHashDate.Millisecond).AddSeconds(-LastHashDate.Second);
                //毎時15分と45分

                if (LastHashDate.Minute >= 45) { LastHashDate = LastHashDate.AddMinutes(-LastHashDate.Minute + 15); }
                else if(LastHashDate.Minute < 15) { LastHashDate = LastHashDate.AddMinutes(-LastHashDate.Minute + 15).AddHours(-1); }
                else { LastHashDate = LastHashDate.AddMinutes(-LastHashDate.Minute).AddMinutes(45).AddHours(-1); }
                Console.WriteLine(LastHashDate);
                cmd.Parameters.AddWithValue("@created_at", LastHashDate.ToUnixTimeSeconds());
                Table = SelectTable(cmd);
            }
            List<long> Tweets = new List<long>(Table.Rows.Count);
            NGRegex NG = new NGRegex();
            foreach (DataRow row in Table.Rows)
            {
                if (!NG.isNG(row[1] as string)){
                    Tweets.Add(row[0] as long? ?? 0);
                }
            }
            return Tweets;
        }
    }
}