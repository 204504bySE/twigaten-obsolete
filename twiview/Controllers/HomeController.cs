using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace twiview.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            return View();
        }

        [Route("about")]
        public ActionResult About()
        {
            //ViewBag.Message = "Your application description page.";
            LoginHandler Login = new LoginHandler(Session, Request, Response);
            return View();
        }
    }
}