using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using twitenlib;

namespace twilock
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                CheckOldProcess.CheckandExit();

                RemoveOldSet<long> LockedTweets = new RemoveOldSet<long>(Config.Instance.locker.TweetLockSize);

                UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.IPv6Loopback, Config.Instance.locker.UdpPort));

                int ReceiveCount = 0;
                int SuccessCount = 0;
                Stopwatch sw = new Stopwatch();
                byte[] TrueByte = BitConverter.GetBytes(true);
                byte[] FalseByte = BitConverter.GetBytes(false);

                //雑なプロセス間通信

                sw.Start();
                while (true)
                {
                    IPEndPoint CrawlEndPoint = null;
                    var buf = Udp.Receive(ref CrawlEndPoint);

                    long tweet_id = BitConverter.ToInt64(buf, 0);
                    if (LockedTweets.Add(tweet_id))
                    {
                        Udp.Send(TrueByte, sizeof(bool), CrawlEndPoint); //Lockできたらtrue
                        SuccessCount++;
                    }
                    else { Udp.Send(FalseByte, sizeof(bool), CrawlEndPoint); }//Lockできなかったらfalse

                    ReceiveCount++;
                    if (sw.ElapsedMilliseconds >= 60000)
                    {
                        sw.Restart();
                        Console.WriteLine("{0}: {1} / {2} Tweets Locked", DateTime.Now, SuccessCount, ReceiveCount);
                        SuccessCount = 0; ReceiveCount = 0;
                        GC.Collect();
                    }
                }
            }
            catch (Exception e) { Console.WriteLine(e); System.Threading.Thread.Sleep(2000); Environment.Exit(1); }   //何かあったら諦めて死ぬ
        }

        class RemoveOldSet<T>
        {
            HashSet<T> OldSet;
            HashSet<T> NewSet;

            int MaxSize;
            public RemoveOldSet(int MaxSize)
            {
                //各Setあたりのサイズに変換する
                this.MaxSize = Math.Max(MaxSize >> 1, 1);

                OldSet = new HashSet<T>();
                NewSet = new HashSet<T>();
            }

            public bool Add(T Value)
            {
                RemoveOld();
                return !OldSet.Contains(Value) && NewSet.Add(Value);
            }

            void RemoveOld()
            {
                if(NewSet.Count >= MaxSize)
                {
                    OldSet.Clear();
                    var TempSet = OldSet;
                    OldSet = NewSet;
                    NewSet = TempSet;
                }
            }
        }
    }
}
