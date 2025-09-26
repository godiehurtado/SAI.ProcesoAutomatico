using System;
using System.ComponentModel;
using System.Configuration;
using System.Diagnostics;
using System.ServiceProcess;
using System.Timers;

namespace ProcesosNocturnosSAI
{
    public partial class Service1 : ServiceBase
    {
        private Timer aTimer;
        private BackgroundWorker backgroundWorker1;
        private EventLog eventLog1;
        private Class1 _classArg;

        public Service1()
        {
            InitializeComponent();

            // Si NO usas el diseñador para EventLog/BackgroundWorker, los inicializamos aquí:
            eventLog1 = new EventLog();
            try
            {
                if (!EventLog.SourceExists("ProcesosNocturnosSAI"))
                    EventLog.CreateEventSource("ProcesosNocturnosSAI", "Application");
            }
            catch { /* permisos en primera ejecución; el instalador puede crear la fuente */ }

            eventLog1.Source = "ProcesosNocturnosSAI";
            eventLog1.Log = "Application";

            backgroundWorker1 = new BackgroundWorker
            {
                WorkerSupportsCancellation = true,
                WorkerReportsProgress = true
            };
            backgroundWorker1.DoWork += backgroundWorker1_DoWork;
            backgroundWorker1.ProgressChanged += backgroundWorker1_ProgressChanged;
            backgroundWorker1.RunWorkerCompleted += backgroundWorker1_RunWorkerCompleted;
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                eventLog1.WriteEntry("ProcesoNocturnoSAI Iniciando");

                // Intervalo desde App.config (fallback 20000 ms)
                int intervaloMs = 20000;
                var v = ConfigurationManager.AppSettings["IntervaloMilisegundos"];
                if (!string.IsNullOrWhiteSpace(v) && int.TryParse(v, out var cfgMs) && cfgMs > 0)
                    intervaloMs = cfgMs;

                aTimer = new Timer(intervaloMs)
                {
                    AutoReset = true,
                    Enabled = true
                };
                aTimer.Elapsed += OnTimedEvent;

                // Instancia ÚNICA de la clase de trabajo
                _classArg = new Class1();

                // Primer disparo inmediato (como el binario original hacía en OnStart)
                if (!backgroundWorker1.IsBusy)
                    backgroundWorker1.RunWorkerAsync(_classArg);

                eventLog1.WriteEntry("ProcesoNocturnoSAI Iniciado");
            }
            catch (Exception ex)
            {
                try { eventLog1.WriteEntry("Error en OnStart: " + ex, EventLogEntryType.Error); } catch { }
                throw;
            }
        }

        protected override void OnStop()
        {
            eventLog1.WriteEntry("ProcesoNocturnoSAI Terminando");

            try
            {
                if (aTimer != null)
                {
                    aTimer.Stop();
                    aTimer.Dispose();
                    aTimer = null;
                }

                if (backgroundWorker1 != null && backgroundWorker1.IsBusy)
                {
                    backgroundWorker1.CancelAsync();
                }
            }
            catch (Exception ex)
            {
                eventLog1.WriteEntry("ProcesoNocturnoSAI Terminando con error: " + ex.Message, EventLogEntryType.Error);
            }

            eventLog1.WriteEntry("ProcesoNocturnoSAI Terminado!");
        }

        private void OnTimedEvent(object source, ElapsedEventArgs e)
        {
            try
            {
                if (!backgroundWorker1.IsBusy)
                {
                    backgroundWorker1.RunWorkerAsync(_classArg);
                }
                // else: hay trabajo en curso; no solapamos
            }
            catch (Exception ex)
            {
                eventLog1.WriteEntry("Error en OnTimedEvent: " + ex, EventLogEntryType.Error);
            }
        }

        private void backgroundWorker1_DoWork(object sender, DoWorkEventArgs e)
        {
            var worker = (BackgroundWorker)sender;
            var arg = (Class1)e.Argument;

            // Llama la lógica real (firma según ILSpy)
            // void iniciaAutomatizacion(BackgroundWorker worker, DoWorkEventArgs e, EventLog log)
            arg.iniciaAutomatizacion(worker, e, eventLog1);
        }

        private void backgroundWorker1_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Error != null)
            {
                eventLog1.WriteEntry("Class1 Error: " + e.Error.Message, EventLogEntryType.Error);
            }
            else if (e.Cancelled)
            {
                eventLog1.WriteEntry("Class1 canceled.");
            }
            else
            {
                eventLog1.WriteEntry("Class1 en proceso.");
            }
        }

        private void backgroundWorker1_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            // Si desde Class1 reportas progreso (% y/o estado), podrías loguearlo:
            // eventLog1.WriteEntry($"Progreso: {e.ProgressPercentage}%");
            // var state = e.UserState as Class1; // si lo usas
        }

#if DEBUG
        // Ejecutar como consola en Debug (útil durante reconstrucción)
        public void DebugRun()
        {
            OnStart(Array.Empty<string>());
            Console.WriteLine("DebugRun: ejecutando, presiona ENTER para detener...");
            Console.ReadLine();
            OnStop();
        }
#endif
    }
}
