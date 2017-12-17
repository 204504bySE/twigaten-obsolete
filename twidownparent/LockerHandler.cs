﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using twitenlib;

namespace twidownparent
{
    static class LockerHandler
    {
        static Config config = Config.Instance;
        static Process LockerProcess;

        static UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, (config.crawl.LockerUdpPort ^ (Process.GetCurrentProcess().Id & 0x3FFF)))) { DontFragment = true };
        static IPEndPoint LockerEndPoint = new IPEndPoint(IPAddress.Loopback, config.crawl.LockerUdpPort);

        ///<summary>twilockが反応しないか起動してなかったら動かす</summary>
        static public void CheckAndStart()
        {
            Udp.Client.ReceiveTimeout = 1000;
            Udp.Client.SendTimeout = 1000;

            const int MaxRetryCount = 3;
            int RetryCount;
            for (RetryCount = 0; RetryCount < MaxRetryCount; RetryCount++)
            {
                try
                {
                    IPEndPoint gomi = null;
                    Udp.Send(new byte[] { 255, 255, 255, 255, 255, 255, 255, 255 }, 8, LockerEndPoint);
                    Udp.Receive(ref gomi);
                }
                catch { }
            }
            if (RetryCount >= MaxRetryCount)
            {
                if (LockerProcess?.HasExited == false)
                {
                    LockerProcess.Kill();
                    Console.WriteLine("{0} Locker Killed {1}", DateTime.Now, LockerProcess.Id);
                }
                do { Thread.Sleep(1000); } //てきとー
                while (LockerProcess?.HasExited == false);
                LockerProcess = Start();
                if (LockerProcess == null) { Console.WriteLine("{0} Locker Start Failed"); }
                else { Console.WriteLine("{0} Locker Started {1}", DateTime.Now, LockerProcess.Id); }
            }
            else if(LockerProcess?.HasExited != false)
            {
                //なぜか動いているならそのprocessを取得して使う
                Process[] ps = Process.GetProcessesByName(config.crawlparent.LockerName);
                if(ps.Length == 1) { LockerProcess = ps.First(); }
            }
        }


        static Process Start()
        {
            try
            {
                ProcessStartInfo info = new ProcessStartInfo(config.crawlparent.LockerPath)
                {
                    WorkingDirectory = Path.GetDirectoryName(config.crawlparent.LockerPath),
                    WindowStyle = ProcessWindowStyle.Minimized
                };
                return Process.Start(info);
            }
            catch { return null; }
            finally
            {
                Udp.Client.ReceiveTimeout = 1000;
                Udp.Client.SendTimeout = 1000;
            }
        }

    }
}
