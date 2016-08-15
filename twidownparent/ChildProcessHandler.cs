using System;
using System.Collections.Generic;
using System.Threading;

using System.Diagnostics;
using System.IO;
using twitenlib;


namespace twidownparent
{
    class ChildProcessHandler
    {
        Config config = Config.Instance;

        public int StartChild(int cpu)
        {
            Config config = Config.Instance;
            try
            {
                ProcessStartInfo info = new ProcessStartInfo(config.crawlparent.ChildPath);
                info.WorkingDirectory = Path.GetDirectoryName(config.crawlparent.ChildPath);
                info.WindowStyle = ProcessWindowStyle.Minimized;
                Process retProcess = Process.Start(info);
                if (cpu >= 0) { retProcess.ProcessorAffinity = (IntPtr)(1 << cpu); }  //1プロセス1CPUコア
                return retProcess.Id;
            }
            catch { return -1; }
        }

        public bool isAlive(int pid)
        {
            Process p;
            try
            {
                p = Process.GetProcessById(pid);
                if (p == null) { return false; }
                if (p.ProcessName == config.crawlparent.ChildName) { return true; }
                return false;
            }
            catch { return false; }
        }
    }
}
