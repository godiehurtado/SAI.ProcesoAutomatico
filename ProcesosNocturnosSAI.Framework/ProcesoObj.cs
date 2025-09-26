using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProcesosNocturnosSAI.Framework
{
    public class ProcesoObj
    {
        public int id;

        public string nombre_proceso;

        public int tipo_proceso_id;

        public int max_reintentos;

        public int max_tiempo_ejecucion;

        public int compania_id;

        public int requiere_backup;

        public string tabla;

        public string notificar_inicio;

        public string notificar_error;

        public string notificar_fin;
    }
}
