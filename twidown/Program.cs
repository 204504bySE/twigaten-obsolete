using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using System.IO;
using System.Net;
using System.Runtime;
using twitenlib;

namespace twidown
{
    class Program
    {
        static void Main(string[] args)
        {
            ServicePointManager.ReusePort = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            Thread.Sleep(10000);

            Task.Factory.StartNew(() => IntervalProcess());
            
            if (args.Length >= 1 && args[0] == "/REST")
            {
                Console.WriteLine("{0} App: Running in REST mode.", DateTime.Now);
                RestManager Rest = new RestManager();
                while (true)
                {
                    if(Rest.Proceed() == 0) { Thread.Sleep(10000); }
                }
            }
            
            GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
            Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;

            Config config = Config.Instance;
            DBHandler db = DBHandler.Instance;
            UserStreamerManager manager = new UserStreamerManager(db.SelectAlltoken());
            manager.AddAll(db.SelectAlltoken());
            while (true)
            {
                int Connected = manager.ConnectStreamers();
                Console.WriteLine("{0} App: {1} / {2} Streamers active.", DateTime.Now, Connected, manager.Count);
                Thread.Sleep(60000);
                manager.AddAll(db.SelectAlltoken());
            }
        }

        //StreamerLockerのアンロックはここでやる
        static void IntervalProcess()
        {
            Config config = Config.Instance;
            StreamerLocker Locker = StreamerLocker.Instance;
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            while (true)
            {
                Thread.Sleep(60000);
                Locker.ActualUnlockAll();
                GC.Collect();
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