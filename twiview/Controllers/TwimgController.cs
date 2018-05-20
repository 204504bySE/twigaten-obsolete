using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;

namespace twiview.Controllers
{
    /*
    [RoutePrefix("fallback/twimg")]
    public class TwimgController : Controller
    {

        [Route("thumb/{Name}")]
        public ActionResult Thumb(string Name)
        {
            string SourceUrl = new DBHandlerView().SourceUrlThumb(Name);
            if (SourceUrl == null) { return new EmptyResult(); }
            MemoryStream mem = DownloadMedia(SourceUrl.IndexOf("twimg.com/") > 0 ? SourceUrl + ":thumb" : SourceUrl);
            if (mem == null) { return new EmptyResult(); }
            return new EmptyResult();
            return new FileStreamResult(mem, MimeMapping.GetMimeMapping(Path.GetFileName(SourceUrl)));
        }
        [Route("profile_image/{Name}")]
        public ActionResult ProfileImage(string Name)
        {
            string SourceUrl = new DBHandlerView().SourceUrlIcon(Name);
            if (SourceUrl == null) { return new EmptyResult(); }
            MemoryStream mem = DownloadMedia(SourceUrl);
            if(mem == null) { return new EmptyResult(); }
            return new FileStreamResult(mem, MimeMapping.GetMimeMapping(Path.GetFileName(SourceUrl)));
        }

        MemoryStream DownloadMedia(string Url)
        {
            try
            {
                HttpWebRequest req = WebRequest.Create(Url) as HttpWebRequest;
                using (WebResponse res = req.GetResponse())
                {
                    MemoryStream ret = new MemoryStream((int)res.ContentLength);
                    res.GetResponseStream().CopyTo(ret);
                    res.Close();
                    ret.Seek(0, SeekOrigin.Begin);
                    return ret;
                }
            }
            catch { return null; }
        }
    }
    */
}