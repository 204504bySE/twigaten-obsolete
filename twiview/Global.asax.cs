using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;
using Microsoft.Web.Mvc;

namespace twiview
{
    public class MvcApplication : System.Web.HttpApplication
    {
        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            BundleConfig.RegisterBundles(BundleTable.Bundles);

            //CookieなどもControllerの引数に渡される
            //ここで上に書いたものが優先(URLは最優先
            ValueProviderFactories.Factories.Add(new CookieValueProviderFactory());
            ValueProviderFactories.Factories.Add(new SessionValueProviderFactory());
            ValueProviderFactories.Factories.Add(new TempDataValueProviderFactory());
        }
    }
}
