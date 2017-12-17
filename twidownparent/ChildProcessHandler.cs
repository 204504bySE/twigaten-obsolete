using System;
using System.Collections.Generic;
using System.Threading;

using System.Diagnostics;
using System.IO;
using twitenlib;


namespace twidownparent
{
    static class ChildProcessHandler
    {
        static Config config = Config.Instance;

        static public int Start()
        {
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

        static public bool Alive(int pid)
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
