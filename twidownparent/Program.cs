using System;
using System.Collections.Generic;
using System.Threading;

using System.Diagnostics;
using twitenlib;

namespace twidownparent
{
    class Program
    {
        static void Main(string[] args)
        {
            //多重起動防止
            CheckOldProcess();

            Config config = Config.Instance;
            DBHandler db = new DBHandler();
            ChildProcessHandler ch = new ChildProcessHandler();
            long[] users;
            int usersIndex = 0;

            if (config.crawlparent.InitTruncate)
            {
                db.initTruncate();
                users = db.SelectAlltoken();
                if (users.Length > 0) { Console.WriteLine("{0} Assigning {1} accounts.", DateTime.Now, users.Length); }

                for (int i = 0; i <= users.Length / config.crawlparent.AccountLimit;)   //i++はここではやらない
                {
                    int cpu = (config.crawlparent.ChildSingleThread ? i % Environment.ProcessorCount : -1);
                    int pid = ch.StartChild(cpu);
                    if (pid >= 0)
                    {
                        db.Insertpid(pid, cpu);
                        for (int j = 0; j < config.crawlparent.AccountLimit && usersIndex < users.Length; j++, usersIndex++)
                        {
                            db.Assigntoken(users[usersIndex], pid);
                        }
                        i++;
                    }
                }
            }

            for (;;)
            {
                int ForceNewChild = db.DeleteDeadpid();
                users = db.SelectNewToken();
                if(users.Length > 0) { Console.WriteLine("{0} Assigning {1} accounts.", DateTime.Now, users.Length); }
                usersIndex = 0;
                if (ForceNewChild > 0)
                {
                    for (int i = 0; i < ForceNewChild && usersIndex < users.Length;)   //i++はここではやらない
                    {
                        int cpu = db.SelectBestCPU();
                        int newpid = ch.StartChild(cpu);
                        if (newpid >= 0)
                        {
                            db.Insertpid(newpid, cpu);
                            for (int j = 0; j < config.crawlparent.AccountLimit && usersIndex < users.Length; j++, usersIndex++)
                            {
                                db.Assigntoken(users[usersIndex], newpid);
                            }
                            i++;
                        }
                    }
                }
                if (users.Length > 0)
                {
                    KeyValuePair<int, int> BestpidInfo = db.SelectBestpid();
                    int pid = BestpidInfo.Key;
                    int count = BestpidInfo.Value;
                    for (; usersIndex < users.Length; usersIndex++)
                    {
                        if (pid < 0)
                        {
                            int cpu = db.SelectBestCPU();
                            int newpid = ch.StartChild(cpu);
                            if (newpid < 0) { continue; }    //雑すぎるエラー処理
                            pid = newpid; count = 0;
                            db.Insertpid(pid, cpu);
                        }
                        db.Assigntoken(users[usersIndex], pid);
                        count++;
                        if (count >= config.crawlparent.AccountLimit)
                        {
                            BestpidInfo = db.SelectBestpid();
                            pid = BestpidInfo.Key;
                            count = BestpidInfo.Value;
                        }
                    }
                }
                Thread.Sleep(60000);
            }
        }

        static void CheckOldProcess()
        {   //多重起動防止
            Process CurrentProc = Process.GetCurrentProcess();
            Process[] proc = Process.GetProcessesByName(CurrentProc.ProcessName);
            foreach(Process p in proc)
            {
                if(p.Id != CurrentProc.Id)
                {
                    Console.WriteLine("{0} Another Instance of {1} is Runnning.", DateTime.Now, CurrentProc.ProcessName);
                    Thread.Sleep(5000);
                    Environment.Exit(1);
                }
            }
        }
    }
}
