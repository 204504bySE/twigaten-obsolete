using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CoreTweet;
using System.Net;
using System.Collections.Generic;
using twitenlib;

namespace twidown
{
    class RestManager
    {
        Config config = Config.Instance;
        DBHandler db = DBHandler.Instance;
        StreamerLocker Locker = new StreamerLocker();

        public RestManager()
        {
            ServicePointManager.DefaultConnectionLimit = config.crawl.RestThreads * 3;
            Task.Factory.StartNew(() => { IntervalProcess(); }, TaskCreationOptions.LongRunning);
        }

        void IntervalProcess()
        {
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            while (true)
            {
                Locker.ActualUnlockAll();
                Thread.Sleep(60000);
            }
        }

        public int Proceed()
        {
            KeyValuePair<Tokens, bool>[] tokens = db.SelectResttoken();
            if (tokens.Length > 0) { Console.WriteLine("{0} App: {1} Accounts to REST", DateTime.Now, tokens.Length); }
            Parallel.ForEach(tokens,
                new ParallelOptions { MaxDegreeOfParallelism = config.crawl.RestThreads },
                (KeyValuePair<Tokens, bool> t) => 
            {
                UserStreamer s = new UserStreamer(t.Key, Locker);
                s.RestBlock();
                if (t.Value) { s.RecieveRestTimeline(); }
                s.RestMyTweet();
                db.StoreRestDonetoken(t.Key.UserId);
            });
            return tokens.Length;
        }
    }
}
