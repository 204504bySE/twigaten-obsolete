using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using System.IO;
using System.Net;
using twitenlib;

namespace twidown
{
    class Program
    {
        static void Main(string[] args)
        {
            System.Runtime.GCSettings.LatencyMode = System.Runtime.GCLatencyMode.SustainedLowLatency;
            System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
            Config config = Config.Instance;
            ServicePointManager.ReusePort = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            DBHandler db = DBHandler.Instance;
            Thread.Sleep(10000);
            LastConnectProcessTick.update();
            Task.Factory.StartNew(() => IntervalProcess());
            Task.Factory.StartNew(() => WatchConnect());
            UserStreamerManager manager = new UserStreamerManager(db.SelectAlltoken(), ref LastConnectProcessTick);
            Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;
            manager.AddAll(db.SelectAlltoken(), ref LastConnectProcessTick);

            for (;;)
            {
                int Connected = manager.ConnectStreamers(ref LastConnectProcessTick);
                Console.WriteLine("{0} App: {1} / {2} Streamers active.", DateTime.Now, Connected, manager.Count());
                LastConnectProcessTick.update(60000) ; //Sleep中に自殺するのを防ぐチート
                Thread.Sleep(60000);
                LastConnectProcessTick.update();                
                manager.AddAll(db.SelectAlltoken(), ref LastConnectProcessTick);

                LastFullGCTick.update();
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
                LastFullGCTick.update();
            }
        }

        //MainでフルGCをしてからの経過時間
        static TickCount LastFullGCTick = new TickCount();

        //StreamerLockerのアンロックはここでやる
        static void IntervalProcess()
        {
            StreamerLocker Locker = StreamerLocker.Instance;
            Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;
            for (;;)
            {
                Thread.Sleep(30000);
                Locker.ActualUnlockAll();
                if (LastFullGCTick.Elasped > 70000) { GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, false, false); }
            }
        }

        //詰まって再接続もできないなら自殺ってやつ
        static TickCount LastConnectProcessTick = new TickCount();
        static void WatchConnect()
        {
            Config config = Config.Instance;
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            for (;;)
            {
                //1アカウントの接続処理がStreamersConnectTimeoutよりかかったらアウト
                //ActualUnlockAllも監視してる
                if (LastConnectProcessTick.Elasped  > config.crawl.StreamersConnectTimeout * 1000)
                { LogFailure.Write(string.Format("{0}\t{1}", DateTime.Now, LastConnectProcessTick.Elasped)); Environment.Exit(1); }
                Thread.Sleep(60000);
            }
        }
    }

    //ある処理と現在時刻との差を把握する奴
    struct TickCount
    {
        public int Tick { get; private set; }
        public TickCount(int Offset = 0)
        {
            Tick = unchecked(Environment.TickCount + Offset);
        }
        public void update()
        {
            Tick = Environment.TickCount;
        }
        public void update(int Offset)
        {
            Tick = unchecked(Environment.TickCount + Offset);
        }
        public int Elasped
        {
            get
            {
                return unchecked(Environment.TickCount - Tick);
            }
        }
    }

    static class LogFailure
    {
        public static void Write(string text)
        {
            using (StreamWriter writer = new StreamWriter(Directory.GetCurrentDirectory() + @"\failure.log", true))
            {
                writer.WriteLine(string.Format("{0} {1}:\t{2}", DateTimeOffset.Now.ToString(), System.Diagnostics.Process.GetCurrentProcess().Id, text));
            }
        }
    }
}