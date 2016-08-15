using System.Web;
using System.Web.Optimization;

namespace twiview
{
    public class BundleConfig
    {
        // バンドルの詳細については、http://go.microsoft.com/fwlink/?LinkId=301862  を参照してください
        public static void RegisterBundles(BundleCollection bundles)
        {
            bundles.Add(new ScriptBundle("~/bundles/jquery").Include(
                        "~/Scripts/jquery-{version}.js"));

            // 開発と学習には、Modernizr の開発バージョンを使用します。次に、実稼働の準備が
            // できたら、http://modernizr.com にあるビルド ツールを使用して、必要なテストのみを選択します。
            //bundles.Add(new ScriptBundle("~/bundles/modernizr").Include(
            //            "~/Scripts/modernizr-*"));

            bundles.Add(new ScriptBundle("~/bundles/bootstrap").Include(
                      "~/Scripts/bootstrap.min.js",
                      "~/Scripts/moment.min.js",
                      "~/Scripts/bootstrap-datetimepicker.min.js"));

            bundles.Add(new StyleBundle("~/Content/css").Include(
                      "~/Content/bootstrap.min.css",
                      "~/Content/bootstrap-theme.min.css",
                      "~/Content/site.css"));
            
            bundles.Add(new ScriptBundle("~/bundles/lazyloadxt").Include(
                "~/Scripts/jquery.lazyloadxt.extra.min.js",
                "~/Scripts/jquery.lazyloadxt.widget.js",
                "~/Scripts/jquery.lazyloadxt.print.js"));

            // https://eonasdan.github.io/bootstrap-datetimepicker/

            bundles.Add(new StyleBundle("~/Content/bootstrap-datepicker").Include(
                "~/Content/bootstrap-datetimepicker.min.css"));
        }
    }
}
