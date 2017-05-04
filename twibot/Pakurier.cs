using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;
using System.Text.RegularExpressions;
using System.Data;
using MySql.Data.MySqlClient;
using System.Data.HashFunction;
using twitenlib;


namespace twibot
{
    class Pakurier : twitenlib.DBHandler
    {
        public Pakurier() : base("bot", "", Config.Instance.database.Address, 3600) { }

        public int FindPakurier()
        {
            int ret = 0;
            
            ExecuteNonQuery(new MySqlCommand(@"TRUNCATE TABLE pakurier;"));
            
            //本文をパクってるやつ(パクツイbot）
            HashSet<long> Pakuriers = FindTextPakurier();
            Console.WriteLine(Pakuriers.Count);
            ret += AddPakuriers(Pakuriers.ToArray());
            
            //画像をパクってるやつ(パクツイbot2)
            FindMediaPakurier(Pakuriers);
            Console.WriteLine(Pakuriers.Count);
            ret += AddPakuriers(Pakuriers.ToArray());
            
            return ret;
        }

        HashSet<long> FindMediaPakurier(HashSet<long> Pakuriers)
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id
FROM user
NATURAL JOIN tweet t
NATURAL JOIN tweet_media
NATURAL JOIN media m
WHERE tweet_id >= @tweet_id
AND isprotected = 0
AND (favorite_count >= 100 OR retweet_count >= 100)
AND (
    EXISTS (
    SELECT * FROM media ma
    NATURAL JOIN tweet_media
    NATURAL JOIN tweet ta
    WHERE dcthash = m.dcthash
    AND media_id != m.media_id
    AND t.tweet_id > ta.tweet_id
    AND t.user_id != ta.user_id
    )
OR EXISTS (
    SELECT * FROM dcthashpair
    INNER JOIN media ON dcthashpair.hash_sub = media.dcthash
    NATURAL JOIN tweet_media
    NATURAL JOIN tweet tb
    WHERE hash_pri = m.dcthash
    AND t.tweet_id > tb.tweet_id
    AND t.user_id != tb.user_id
    )
);"))
            {
                cmd.Parameters.AddWithValue("@tweet_id", SnowFlake.SecondinSnowFlake(NowHour().AddDays(-7).ToUnixTimeSeconds(), false));
                Table = SelectTable(cmd,IsolationLevel.ReadUncommitted);
            }
            foreach (DataRow row in Table.Rows)
            {
                Pakuriers.Add(row.Field<long>(0)); 
            }
            return Pakuriers;
        }

        public struct TextHashData : IComparable<TextHashData>
        {
            //public long tweet_id { get; }
            public long RelaxedHash { get; }
            public long user_id { get; }
            public long created_at { get; }
            public TextHashData(long relaxedhash, long user_id, long created_at)
            {
                //this.tweet_id = tweet_id;
                this.RelaxedHash = relaxedhash;
                this.user_id = user_id;
                this.created_at = created_at;
            }

            public int CompareTo(TextHashData other)
            {
                return RelaxedHash.CompareTo(other.RelaxedHash);
            }
        }

        HashSet<long> FindTextPakurier()
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
text, user_id, created_at
FROM user
NATURAL JOIN tweet
WHERE text IS NOT NULL
AND tweet_id >= @tweet_id
AND (favorite_count >= 100 OR retweet_count >= 100)
AND isprotected = 0
;"))
            {
                cmd.Parameters.AddWithValue("@tweet_id", SnowFlake.SecondinSnowFlake(NowHour().AddDays(-7).ToUnixTimeSeconds(), false));
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
            }
            List<TextHashData> hashes = new List<TextHashData>(Table.Rows.Count);
            foreach (DataRow row in Table.Rows)
            {
                string RelaxedText = TextHash.RelaxedText(row.Field<string>(0));
                if (RelaxedText.Length >= 3)
                {
                    hashes.Add(new TextHashData(TextHash.Hash(RelaxedText), row.Field<long>(1), row.Field<long>(2)));
                }
            }
            Table = null;
            hashes.Sort();
            HashSet<long> Pakuriers = new HashSet<long>();
            HashSet<long> Users = new HashSet<long>();
            for (int i = 0; i < hashes.Count - 1; i++)
            {
                if (hashes[i].RelaxedHash == hashes[i + 1].RelaxedHash)
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

        int AddPakuriers(long[] PakurierArray)
        {
            List<MySqlCommand> cmdList = new List<MySqlCommand>(PakurierArray.Length);
            int ret = 0;
            const int BulkUnit = 1000;
            const string head = "INSERT IGNORE INTO pakurier VALUES";
            string BulkInsertCmdFull = BulkCmdStr(BulkUnit, 1, head);
            MySqlCommand cmdtmp;
            int i, j;
            for (i = 0; i < PakurierArray.Length / BulkUnit; i++)
            {
                cmdtmp = new MySqlCommand(BulkInsertCmdFull);
                for (j = 0; j < BulkUnit; j++)
                {
                    cmdtmp.Parameters.AddWithValue("@a" + j.ToString(), PakurierArray[BulkUnit * i + j]);
                }
                ret += ExecuteNonQuery(cmdtmp);
            }
            cmdtmp = new MySqlCommand(BulkCmdStr(PakurierArray.Length % BulkUnit, 1, head));
            for (j = 0; j < PakurierArray.Length % BulkUnit; j++)
            {
                cmdtmp.Parameters.AddWithValue("@a" + j.ToString(), PakurierArray[BulkUnit * i + j]);
            }
            return ret + ExecuteNonQuery(cmdtmp);
        }

        DateTimeOffset NowHour()
        {
            DateTimeOffset now = DateTimeOffset.Now;
            return new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, 0, 0, 0, new TimeSpan(0));
        }
    }


    public static class TextHash
    {
        public static string RelaxedText(string Text)
        {
            string ret;
            ret = Regex.Replace(Text, @"(?<before>^|.*[\s　])(?<hashtag>[#＃][a-z0-9_À-ÖØ-öø-ÿĀ-ɏɓ-ɔɖ-ɗəɛɣɨɯɲʉʋʻ̀-ͯḀ-ỿЀ-ӿԀ-ԧⷠ-ⷿꙀ-֑ꚟ-ֿׁ-ׂׄ-ׇׅא-תװ-״﬒-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﭏؐ-ؚؠ-ٟٮ-ۓە-ۜ۞-۪ۨ-ۯۺ-ۼۿݐ-ݿࢠࢢ-ࢬࣤ-ࣾﭐ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-ﷻﹰ-ﹴﹶ-ﻼ‌ก-ฺเ-๎ᄀ-ᇿ㄰-ㆅꥠ-꥿가-힯ힰ-퟿ﾡ-ￜァ-ヺー-ヾｦ-ﾟｰ０-９Ａ-Ｚａ-ｚぁ-ゖ゙-ゞ㐀-䶿一-鿿꜀-뜿띀-렟-﨟〃々〻]+)", "${before}", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            ret = Regex.Replace(ret, @"https?://[-_.!~*'()a-zA-Z0-9;/?:@&=+$,%#]+", "", RegexOptions.Compiled);
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
}
