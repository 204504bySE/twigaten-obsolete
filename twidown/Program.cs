﻿using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime;
using System.Net;
using System.Diagnostics;
using twitenlib;

namespace twidown
{
    class Program
    {
        static void Main(string[] args)
        {
            ServicePointManager.ReusePort = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            Thread.Sleep(10000);
          
            if (args.Length >= 1 && args[0] == "/REST")
            {
                Console.WriteLine("{0} App: Running in REST mode.", DateTime.Now);
                Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.Idle;
                int RestCount = new RestManager().Proceed();
                Console.WriteLine("{0} App: {1} Accounts REST Tweets Completed.", DateTime.Now, RestCount);
                Thread.Sleep(10000);
                return;
            }

            Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;
            Config config = Config.Instance;
            DBHandler db = DBHandler.Instance;
            
            UserStreamerManager manager = new UserStreamerManager(db.SelectAlltoken());
            manager.AddAll(db.SelectAlltoken());
            while (true)
            {
                int Connected = manager.ConnectStreamers();
                Console.WriteLine("{0} App: {1} / {2} Streamers active.", DateTime.Now, Connected, manager.Count);
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, false, false);
                Thread.Sleep(60000);
                manager.AddAll(db.SelectAlltoken());
            }
        }
    }

    //ある処理と現在時刻との差を把握する奴
    //stopwatchほどの精度はいらない時用
    struct TickCount
    {
        public int Tick { get; private set; }
        public TickCount(int Offset) : this() { Update(Offset); }        
        public int Elasped { get { return unchecked(Environment.TickCount - Tick); } }
        public void Update(int Offset = 0) { Tick = unchecked(Environment.TickCount + Offset); }
    }
}