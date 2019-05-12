using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Web;

namespace twiview
{
    ///<summary>logintokenテーブルの認証用文字列を暗号化したりする</summary>
    static class LoginTokenEncrypt
    {
        static readonly RNGCryptoServiceProvider RNG = new RNGCryptoServiceProvider();
        static readonly SHA256 SHA = SHA256.Create();
        ///<summary>88文字のCookie用文字列と44文字のDB用文字列を生成</summary>
        public static (string Text88, string Hash44) NewToken()
        {
            byte[] random = new byte[64];
            RNG.GetBytes(random);            
            return (Convert.ToBase64String(random), Convert.ToBase64String(SHA.ComputeHash(random)));
        }
        ///<summary>88文字のCookie用文字列と44文字のDB用文字列を照合</summary>
        public static bool VerifyToken(string Text88, string Hash44)
        {
            if(Text88 == null || Hash44 == null) { return false; }
            return SHA.ComputeHash(Convert.FromBase64String(Text88)).SequenceEqual(Convert.FromBase64String(Hash44));
        }
    }
}