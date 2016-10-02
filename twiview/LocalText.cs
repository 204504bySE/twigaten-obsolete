using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.IO;
using System.Text.RegularExpressions;
using twitenlib;

namespace twiview
{
    public static class LocalText
    {
        public static string TextToLink(string Text)
        //URLとハッシュタグをリンクにする奴
        {
            if (Text == null) { return null; }
            else
            {
                Text = Text.Replace("<", "&lt;").Replace(">", "&gt;").Replace("\n", "<br />");
                Text = Regex.Replace(Text, @"https?://[-_.!~*'()a-zA-Z0-9;/?:@&=+$,%#]+", @"<a href=""$&"">$&</a>");
                MatchCollection matches = Regex.Matches(Text, @"(^|.*?[\s　>])(?<hashtag>[#＃][a-z0-9_À-ÖØ-öø-ÿĀ-ɏɓ-ɔɖ-ɗəɛɣɨɯɲʉʋʻ̀-ͯḀ-ỿЀ-ӿԀ-ԧⷠ-ⷿꙀ-֑ꚟ-ֿׁ-ׂׄ-ׇׅא-תװ-״﬒-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﭏؐ-ؚؠ-ٟٮ-ۓە-ۜ۞-۪ۨ-ۯۺ-ۼۿݐ-ݿࢠࢢ-ࢬࣤ-ࣾﭐ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-ﷻﹰ-ﹴﹶ-ﻼ‌ก-ฺเ-๎ᄀ-ᇿ㄰-ㆅꥠ-꥿가-힯ힰ-퟿ﾡ-ￜァ-ヺー-ヾｦ-ﾟｰ０-９Ａ-Ｚａ-ｚぁ-ゖ゙-ゞ㐀-䶿一-鿿꜀-뜿띀-렟-﨟〃々〻]+)", RegexOptions.IgnoreCase);

                HashSet<string> matched = new HashSet<string>();
                foreach (Match m in matches)
                {
                    string matchstr = m.Groups["hashtag"].ToString();
                    if (!matched.Contains(matchstr))
                    {
                        matched.Add(matchstr);
                        Text = Text.Replace(matchstr, @"<a href=""https://twitter.com/hashtag/" + HttpUtility.UrlEncode(matchstr.Substring(1)) + @""">" + matchstr + "</a>");
                    }
                }
                return Text;
            }
        }

        //単にhttps://…を必要に応じてつけるだけ
        public static string MediaUrlFull(TweetData._media Media, HttpRequestBase Request)
        {
            if (Media.media_url.IndexOf("://") >= 0) { return Media.media_url; }
            else { return Request.Url.GetLeftPart(UriPartial.Authority) + Media.media_url; }
        }

        public static string MediaUrl(TweetData._media Media, bool iscached)
        {   //Viewに使う画像のURLを返す
            //orig_media_urlとmedia_idが必要
            if(Media.orig_media_url == null) { return null; }
            if (iscached) { return config.PictPaththumb + Media.media_id.ToString() + Path.GetExtension(Media.orig_media_url); }
            else if (Media.orig_media_url.IndexOf("twimg.com") >= 0) { return Media.orig_media_url.Replace("http://", "https://") + ":thumb"; }
            else { return Media.orig_media_url; }
        }

        public static string ProfileImageUrl(TweetData._user User, bool iscached)
        {   //Viewに使うアイコンのURLを返す
            //user_idとprofile_image_urlが必要
            if (User.profile_image_url == null) { return null; }
            if (iscached) { return config.PictPathProfileImage + User.user_id.ToString() + Path.GetExtension(User.profile_image_url); }
            else if (User.profile_image_url.IndexOf("twimg.com") >= 0) { return User.profile_image_url.Replace("http://", "https://"); }  //全部twimg.comだよね
            else { return User.profile_image_url; }
        }
    }
}