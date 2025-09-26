using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace ProcesosNocturnosSAI
{
    static class Program
    {
        /// <summary>
        /// Punto de entrada principal para la aplicación.
        /// </summary>
        static void Main()
        {
#if DEBUG
            // Modo debug como consola (opcional y súper útil mientras reconstruimos)
            var svc = new Service1();
            svc.DebugRun();
#else
            ServiceBase.Run(new ServiceBase[] { new Service1() });
#endif
        }
    }
}
