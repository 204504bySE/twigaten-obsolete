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
using CoreTweet.Streaming;
using twitenlib;


namespace twibot
{
    class Program
    {
        static void Main(string[] args)
        {
            Config config = Config.Instance;

            Tokens token = Tokens.Create("8xYrkWXqUhIVeVYVmZxKIzNl5", "sOHcL7wZvwZHE7Cc0HpZWBsmQBUQ80g3f87TP32ivseaZbbcF1", "501666898-6K7fcyAP7auXse67VbaIBoL6pIIrRF1ooEfBTQVQ", "neVFyYfRnVmgf0SvJtehuLBujEtTWePplEM3YlZnnXGas");
            var stream = token.Streaming.User();
            foreach (var message in stream)
            {
                if (message is StatusMessage)
                {
                    var status = (message as StatusMessage).Status;
                    Console.WriteLine(string.Format("{0}:{1}", status.User.ScreenName, status.User.ProfileImageUrlHttps));
                }
                else if (message is EventMessage)
                {
                    var ev = message as EventMessage;
                    Console.WriteLine(string.Format("{0}:{1}->{2}",
                        ev.Event, ev.Source.ScreenName, ev.Target.ScreenName));
                }
            }
            return;

            /*
            if(DateTimeOffset.Now.ToUnixTimeSeconds() - config.bot.LastPakurierTime > 86300)    //100秒だけ削っておく
            {
                config.bot.NewLastPakurierTime(DateTimeOffset.Now.ToUnixTimeSeconds());
                Pakurier pakurier = new Pakurier();
                pakurier.FindPakurier();
            }
            Console.WriteLine("＼(^o^)／");
            Console.ReadKey();
            */
            Retweeter bot = new Retweeter();
            bot.TweetOne();
            config.bot.NewLastTweetTime(DateTimeOffset.Now.ToUnixTimeSeconds());
            Console.WriteLine("＼(^o^)／");
            Thread.Sleep(5000);
            //Console.ReadKey();
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
                    prof.FollowersCount - int.Parse(ini.GetValue("copy_writing_bot", "LastFollowers", "0")));
                Token.Statuses.Update(tweetstr);
                Console.WriteLine(tweetstr);
                ini.SetValue("copy_writing_bot", "LastFollowers", prof.FollowersCount.ToString());
            }
            catch (Exception e) { Console.WriteLine("{0}\n{1}", DateTime.Now, e); }
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
                        //r = Token.Statuses.Retweet(t);
                        r = Token.Statuses.Lookup(id => t, tweet_mode => TweetMode.extended).First();
                    }
                    catch (Exception e) { Console.WriteLine(e); Thread.Sleep(5000); continue; }
                    
                    try
                    {
                        Console.WriteLine("{0}:{1}\n{2}",r.RetweetedStatus.User.ScreenName, r.RetweetedStatus.User.Name, r.RetweetedStatus.Text);
                        Console.WriteLine("Retweeted: {0}", t);
                    }
                    catch { }
                    //return;
                }
            }
        }

        List<long> RecentRetweets()
        {
            List<long> ret = new List<long>();
            try
            {
                foreach (Status x in Token.Statuses.HomeTimeline(count => 200, tweet_mode => TweetMode.extended))
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
        public dbhandler() : base("bot", "", Config.Instance.database.Address, 600) { }

        public List<long> RecentPakuriedTweet()
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"
SELECT tweet_id 
FROM user u
NATURAL JOIN tweet t
NATURAL JOIN tweet_media
NATURAL JOIN media m
INNER JOIN (
    SELECT dcthash, user_id
    FROM tweet tz
    NATURAL JOIN tweet_media
    NATURAL JOIN media mz
    WHERE tweet_id >= @tweet_id
    AND (favorite_count >= 100 AND retweet_count >= 100)
    AND (
        EXISTS (
        SELECT * FROM media ma
        NATURAL JOIN tweet_media
        NATURAL JOIN tweet ta
        WHERE dcthash = m.dcthash
        AND media_id != m.media_id
        AND tz.tweet_id > ta.tweet_id
        AND tz.user_id != ta.user_id
        )
    OR EXISTS (
        SELECT * FROM dcthashpair
        INNER JOIN media ON dcthashpair.hash_sub = media.dcthash
        NATURAL JOIN tweet_media
        NATURAL JOIN tweet tb
        WHERE hash_pri = m.dcthash
        AND tz.tweet_id > tb.tweet_id
        AND tz.user_id != tb.user_id
        )
    )
) h ON m.dcthash = h.dcthash
AND u.isprotected = 0
AND NOT EXISTS (
    SELECT * FROM media mc
    NATURAL JOIN tweet_media
    NATURAL JOIN tweet tc
    WHERE dcthash = m.dcthash
    AND media_id != m.media_id
    AND t.tweet_id > tc.tweet_id
)
AND NOT EXISTS (
    SELECT * FROM dcthashpair
    INNER JOIN media ON dcthashpair.hash_sub = media.dcthash
    NATURAL JOIN tweet_media
    NATURAL JOIN tweet td
    WHERE hash_pri = m.dcthash
    AND t.tweet_id > td.tweet_id
)
ORDER BY (SELECT COUNT(user_id) FROM block WHERE target_id = u.user_id) ASC,
(t.retweet_count + t.favorite_count) DESC;"))
            {
                cmd.Parameters.AddWithValue("@tweet_id", SnowFlake.SecondinSnowFlake(config.bot.LastTweetTime, false));
                Table = SelectTable(cmd);
            }
            List<long> Tweets = new List<long>(Table.Rows.Count);
            NGRegex NG = new NGRegex();
            foreach (DataRow row in Table.Rows)
            {
                if (!NG.isNG(row.Field<string>(1))){
                    Tweets.Add(row.Field<long>(0));
                }
            }
            return Tweets;
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
}