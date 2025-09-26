using System.ComponentModel;
using System.ServiceProcess;

namespace ProcesosNocturnosSAI
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : System.Configuration.Install.Installer
    {
        private ServiceProcessInstaller serviceProcessInstaller1;
        private ServiceInstaller serviceInstaller1;

        public ProjectInstaller()
        {
            InitializeComponent();

            serviceProcessInstaller1 = new ServiceProcessInstaller
            {
                Account = ServiceAccount.LocalSystem // o User si necesitas credenciales
            };

            serviceInstaller1 = new ServiceInstaller
            {
                ServiceName = "ProcesosNocturnosSAI",
                DisplayName = "Procesos Nocturnos SAI",
                Description = "Ejecución de procesos nocturnos del sistema SAI.",
                StartType = ServiceStartMode.Automatic
            };

            Installers.Add(serviceProcessInstaller1);
            Installers.Add(serviceInstaller1);
        }
    }
}
