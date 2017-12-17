using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using twitenlib;

namespace twilock
{
    class Program
    {
        static void Main(string[] args)
        {
            CheckOldProcess.CheckandExit();

            HashSet<long> LockedTweets = new HashSet<long>();
            Queue<long> LockedTweetsQueue = new Queue<long>(Config.Instance.locker.TweetLockSize);
            //ActionBlock<UdpReceiveResult> TweetLockBlock;

            UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, Config.Instance.locker.UdpPort)) { DontFragment = true };

            int ReceiveCount = 0;
            int SuccessCount = 0;
            Stopwatch sw = new Stopwatch();
            /*
            //雑なプロセス間通信
            TweetLockBlock = new ActionBlock<UdpReceiveResult>(r => {
                if (r.Buffer.Length == 8)
                {
                    long tweet_id = BitConverter.ToInt64(r.Buffer, 0);
                    try
                    {
                        if (LockedTweets.Add(tweet_id))
                        {
                            Udp.Send(BitConverter.GetBytes(true), sizeof(bool), r.RemoteEndPoint); //Lockできたらtrue
                            SuccessCount++;
                            while (LockedTweetsQueue.Count >= Config.Instance.locker.TweetLockSize)
                            { LockedTweets.Remove(LockedTweetsQueue.Dequeue()); }
                        }
                        else { Udp.Send(BitConverter.GetBytes(false), sizeof(bool), r.RemoteEndPoint); }//Lockできなかったらfalse
                    }
                    //長時間動かしてるとHashSetがこうなるのでClearしてしのぐ
                    catch (OverflowException) { LockedTweets.Clear(); LockedTweetsQueue.Clear(); }
                    catch (Exception e) { Console.WriteLine(e); }
                }
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 1 });
            */
            sw.Start();
            while (true)
            {
                IPEndPoint CrawlEndPoint = null;
                var buf = Udp.Receive(ref CrawlEndPoint);
                //TweetLockBlock.Post(new UdpReceiveResult(buf, CrawlEndPoint));

                long tweet_id = BitConverter.ToInt64(buf, 0);
                try
                {
                    if (LockedTweets.Add(tweet_id))
                    {
                        Udp.Send(BitConverter.GetBytes(true), sizeof(bool), CrawlEndPoint); //Lockできたらtrue
                        SuccessCount++;
                        while (LockedTweetsQueue.Count >= Config.Instance.locker.TweetLockSize)
                        { LockedTweets.Remove(LockedTweetsQueue.Dequeue()); }
                    }
                    else { Udp.Send(BitConverter.GetBytes(false), sizeof(bool), CrawlEndPoint); }//Lockできなかったらfalse
                }
                //長時間動かしてるとHashSetがこうなるのでClearしてしのぐ
                catch (OverflowException) { LockedTweets.Clear(); LockedTweetsQueue.Clear(); }
                catch (Exception e) { Console.WriteLine(e); }

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
    }
}
