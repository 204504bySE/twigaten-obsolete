using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CoreTweet;
using System.Net;
using System.Text;
using System.Collections.Generic;
using System.Collections.Concurrent;
using twitenlib;

namespace twidown
{
    class RestManager
    {
        Config config = Config.Instance;
        DBHandler db = DBHandler.Instance;
        UserStreamerManager.StreamerLocker Locker = new UserStreamerManager.StreamerLocker();

        public RestManager()
        {
            ServicePointManager.DefaultConnectionLimit = config.crawl.RestThreads * 3;
        }

        public int Proceed()
        {
            KeyValuePair<Tokens, bool>[] tokens = db.SelectResttoken();
            if (tokens.Length > 0) { Console.WriteLine("{0} App: {1} Accounts to REST", DateTime.Now, tokens.Length); }
            Parallel.ForEach(tokens,
                new ParallelOptions { MaxDegreeOfParallelism = config.crawl.RestThreads },
                (KeyValuePair<Tokens, bool> t) => 
            {
                ThreadPriority priviousePrio = Thread.CurrentThread.Priority;
                Thread.CurrentThread.Priority = ThreadPriority.Lowest;

                UserStreamer s = new UserStreamer(t.Key, Locker);
                s.RestBlock();
                if (t.Value) { s.RecieveRestTimeline(); }
                s.RestMyTweet();
                db.StoreRestDonetoken(t.Key.UserId);

                Thread.CurrentThread.Priority = priviousePrio;
            });
            return tokens.Length;
        }
    }
}
