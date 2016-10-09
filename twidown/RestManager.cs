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
        StreamerLocker Locker = StreamerLocker.Instance;

        public RestManager()
        {
            ServicePointManager.DefaultConnectionLimit = config.crawl.RestThreads * 3;
        }

        public int Proceed()
        {
            Tokens[] tokens = db.SelectResttoken();
            Parallel.ForEach(tokens,
                new ParallelOptions { MaxDegreeOfParallelism = config.crawl.RestThreads },
                (Tokens t) => 
            {
                UserStreamer s = new UserStreamer(t);
                s.RestBlock();
                //s.RecieveRestTimeline();
                s.RestMyTweet();
                db.StoreRestDonetoken(t.UserId);
            });
            return tokens.Length;
        }
    }
}
