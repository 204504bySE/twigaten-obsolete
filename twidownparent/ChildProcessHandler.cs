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

        public int StartChild()
        {
            Config config = Config.Instance;
            try
            {
                ProcessStartInfo info = new ProcessStartInfo(config.crawlparent.ChildPath)
                {
                    WorkingDirectory = Path.GetDirectoryName(config.crawlparent.ChildPath),
                    WindowStyle = ProcessWindowStyle.Minimized
                };
                Process retProcess = Process.Start(info);
                return retProcess.Id;
            }
            catch { return -1; }
        }

        public bool Alive(int pid)
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
