using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace twiview.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index(LoginParameters p)
        {
            p.Validate(Session, Response);
            return View();
        }

        [Route("about")]
        public ActionResult About(LoginParameters p)
        {
            p.Validate(Session, Response);
            return View();
        }
    }
}