using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using twitenlib;

namespace twiview
{
    public static class LocalText
    {
        static readonly Regex UrlRegex = new Regex(@"https?://[-_.!~*'()a-zA-Z0-9;/?:@&=+$,%#]+", RegexOptions.Compiled);
        static readonly Regex HashtagRegex = new Regex(@"(?<=(?:^|.*?[\s　>])[#＃])[a-z0-9_À-ÖØ-öø-ÿĀ-ɏɓ-ɔɖ-ɗəɛɣɨɯɲʉʋʻ̀-ͯḀ-ỿЀ-ӿԀ-ԧⷠ-ⷿꙀ-֑ꚟ-ֿׁ-ׂׄ-ׇׅא-תװ-״﬒-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﭏؐ-ؚؠ-ٟٮ-ۓە-ۜ۞-۪ۨ-ۯۺ-ۼۿݐ-ݿࢠࢢ-ࢬࣤ-ࣾﭐ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-ﷻﹰ-ﹴﹶ-ﻼ‌ก-ฺเ-๎ᄀ-ᇿ㄰-ㆅꥠ-꥿가-힯ힰ-퟿ﾡ-ￜァ-ヺー-ヾｦ-ﾟｰ０-９Ａ-Ｚａ-ｚぁ-ゖ゙-ゞ㐀-䶿一-鿿꜀-뜿띀-렟-﨟〃々〻]+", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        public static string TextToLink(string Text)
        {
            //URLとハッシュタグをリンクにする rel="nofollow" 付き

            if (Text == null) { return null; }
            StringBuilder Builder = new StringBuilder(Text)
                .Replace("<", "&lt;")
                .Replace(">", "&gt;")
                .Replace("\n", "<br />");

            MatchCollection m = UrlRegex.Matches(Builder.ToString());
            for (int i = m.Count - 1; 0 <= i; i--)
            {
                //後ろから順に挿入する
                Builder.Insert(m[i].Index + m[i].Length, "</a>")
                    .Insert(m[i].Index, @""" rel=""nofollow"">")
                    .Insert(m[i].Index, m[i].Value)
                    .Insert(m[i].Index, @"<a href=""");
            }

            m = HashtagRegex.Matches(Builder.ToString());
            for (int i = m.Count - 1; 0 <= i; i--)
            {
                //後ろから順に挿入する
                Builder.Insert(m[i].Index + m[i].Length, "</a>")
                    .Insert(m[i].Index - 1, @""" rel=""nofollow"">")
                    .Insert(m[i].Index - 1, HttpUtility.UrlEncode(m[i].Value))
                    .Insert(m[i].Index - 1, @"<a href=""https://twitter.com/hashtag/");
            }
            return Builder.ToString();
        }

        //単にhttps://…を必要に応じてつけるだけ
        public static string MediaUrlFull(TweetData._media Media, HttpRequestBase Request)
        {
            if (Media.media_url.IndexOf("://") >= 0) { return Media.media_url; }
            else { return Request.Url.GetLeftPart(UriPartial.Authority) + Media.media_url; }
        }

        public static string MediaUrl(TweetData._media Media, bool IsCached)
        {   //Viewに使う画像のURLを返す
            //orig_media_urlとmedia_idが必要
            if(Media.orig_media_url == null) { return null; }
            if (IsCached) { return config.PictPaththumb + Media.media_id.ToString() + Path.GetExtension(Media.orig_media_url); }
            else if (Media.orig_media_url.IndexOf("twimg.com/") >= 0) { return Media.orig_media_url.Replace("http://", "https://") + ":thumb"; }
            else { return Media.orig_media_url; }
        }

        public static string ProfileImageUrl(TweetData._user User, bool IsCached)
        {   //Viewに使うアイコンのURLを返す
            //user_idとprofile_image_urlが必要
            if (User.profile_image_url == null) { return null; }
            if (IsCached) { return config.PictPathProfileImage + User.user_id.ToString() + Path.GetExtension(User.profile_image_url); }
            else if (User.profile_image_url.IndexOf("twimg.com/") >= 0) { return User.profile_image_url.Replace("http://", "https://"); }
            else { return User.profile_image_url; }
        }

        static int seed = Environment.TickCount;
        static readonly ThreadLocal<Random> rand = new ThreadLocal<Random>(() => 
            { return new Random(Interlocked.Increment(ref seed)); });
        public static string RandomHash()
        {
            return rand.Value.Next().ToString("x8");
        }
    }
}