using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace twidown
{
    class Counter
    {
        //パフォーマンスカウンター的な何か
        public class CounterValue
        {
            int Value = 0;
            public void Increment()
            {
                Interlocked.Increment(ref Value);
            }
            public int Get()
            {
                return Value;
            }
            public int GetReset()
            {
                return Interlocked.Exchange(ref Value, 0);
            }
        }

        private static Counter _Counter = new Counter();
        //singletonはこれでインスタンスを取得して使う
        public static Counter Instance { get { return _Counter; } }

        public CounterValue MediaSuccess = new CounterValue();
        public CounterValue MediaToStore = new CounterValue();
        public CounterValue MediaTotal = new CounterValue();
        public CounterValue TweetStored = new CounterValue();
        public CounterValue TweetToStore = new CounterValue();
        //ひとまずアイコンは除外しようか
        public void PrintReset()
        {
            if (MediaToStore.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} / {3} Media Stored", DateTime.Now, MediaSuccess.GetReset(), MediaToStore.GetReset(), MediaTotal.GetReset()); }
            if (TweetToStore.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} Tweet Stored", DateTime.Now, TweetStored.GetReset(), TweetToStore.GetReset()); }

        }
    }
}
