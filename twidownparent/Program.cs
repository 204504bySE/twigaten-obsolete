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
                ch.StartChild("/REST");
                users = db.SelectNewToken();
                for (int i = 0; i <= users.Length / config.crawlparent.AccountLimit; i++)
                {
                    int newpid = ch.StartChild();
                    if (newpid < 0) { continue; }    //雑すぎるエラー処理
                    db.Insertpid(newpid);
                }
            } else { db.DeleteDeadpid(); users = db.SelectNewToken(); }

            while (true)
            {
                if (users.Length > 0)
                {
                    usersIndex = 0;
                    Console.WriteLine("{0} Assigning {1} accounts.", DateTime.Now, users.Length);
                    for (; usersIndex < users.Length; usersIndex++)
                    {
                        int pid = db.SelectBestpid();
                        if (pid < 0)
                        {
                            int newpid = ch.StartChild();
                            if (newpid < 0) { continue; }    //雑すぎるエラー処理
                            pid = newpid;
                            db.Insertpid(pid);
                        }
                        db.Assigntoken(users[usersIndex], pid);
                    }
                }
                Thread.Sleep(60000);
                db.DeleteDeadpid();
                users = db.SelectNewToken();
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
