// ProcesosNocturnosSAI, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// ProcesosNocturnosSAI.Class1
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Net.Sockets;
using System.Threading;
using System.Timers;
using Microsoft.VisualBasic;
using ProcesosNocturnosSAI.Framework;
using Sybase.Data.AseClient;
using WcfPackagesExecution;

internal class Class1
{
    private const int PROCESO_EXTRACCION = 1;

    private const int PROCESO_ETL = 3;

    private const int EXTRACCION_SISE = 1;

    private const int EXTRACCION_CAPI = 2;

    private const int EXTRACCION_BH = 3;

    private const int ESTADO_PROCESO_PENDIENTE_OTROS_PROCESOS = -1;

    private const int ESTADO_PROCESO_NO_HABILITAR = -2;

    private const int ESTADO_PROCESO_EN_EJECUCION = 1;

    private const int ESTADO_PROCESO_TERMINADO = 2;

    private const int ESTADO_PROCESO_CON_ERRORES = 3;

    private const int ESTADO_PROCESO_PENDIENTE_ARCHIVOS = 5;

    private const int ESTADO_PROCESO_NO_EJECUTADO_POR_ERROR_EN_DEPENDENCIA = 6;

    private const int PERIODICIDAD = 30000;

    private const int FILE_BHNEGOCIOS = 1;

    private const int FILE_BHRECAUDOS = 2;

    private const int FILE_CAPINEGOCIOS = 3;

    private const int FILE_CAPIRECAUDOS = 4;

    private string mssqlUser;

    private string mssqlPwd;

    private string mssqlServer;

    private string mssqlDatabase;

    private EventLog eventLog1;

    private System.Timers.Timer aTimer;

    private bool flagProcesando;

    private string ipAddress;

    public Class1()
    {
        mssqlUser = ConfigurationManager.AppSettings["mssqlUser"];
        mssqlPwd = ConfigurationManager.AppSettings["mssqlPwd"];
        mssqlServer = ConfigurationManager.AppSettings["mssqlServer"];
        mssqlDatabase = ConfigurationManager.AppSettings["mssqlDatabase"];
        ipAddress = getMyIPAddress();
        if (ipAddress == null)
        {
            ipAddress = "???";
        }
    }

    public void iniciaAutomatizacion(BackgroundWorker worker, DoWorkEventArgs e, EventLog eventLog1)
    {
        flagProcesando = false;
        this.eventLog1 = eventLog1;
        aTimer = new System.Timers.Timer(30000.0);
        aTimer.Elapsed += OnTimedEvent_Periodico;
        aTimer.Enabled = true;
    }

    private SqlConnection obtenerConexion()
    {
        SqlConnection sqlConnection = null;
        try
        {
            sqlConnection = new SqlConnection("user id=" + mssqlUser + ";password=" + mssqlPwd + ";server=" + mssqlServer + ";database=" + mssqlDatabase + ";Trusted_Connection=no;connection timeout=30");
            sqlConnection.Open();
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR obtenerConexion: " + ex.Message, EventLogEntryType.Error);
            sqlConnection = null;
        }
        return sqlConnection;
    }

    private void OnTimedEvent_Periodico(object source, ElapsedEventArgs e)
    {
        SqlConnection sqlConnection = null;
        DateTime now = DateTime.Now;
        List<ProcesoObj> list = new List<ProcesoObj>();
        string text = "";
        try
        {
            sqlConnection = obtenerConexion();
            bool flag = false;
            text = "SELECT 1 FROM AUT_ALIVE  WHERE ip = '" + ipAddress + "' ";
            SqlCommand sqlCommand = new SqlCommand(text, sqlConnection);
            SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
            if (sqlDataReader.Read())
            {
                flag = true;
            }
            sqlDataReader.Close();
            if (flag)
            {
                text = " UPDATE AUT_ALIVE SET fecha=GETDATE() WHERE ip = '" + ipAddress + "' ";
                sqlCommand = new SqlCommand(text, sqlConnection);
                sqlCommand.CommandTimeout = 0;
                sqlCommand.ExecuteNonQuery();
            }
            else
            {
                text = " INSERT INTO AUT_ALIVE ( ip , fecha ) VALUES (  '" + ipAddress + "' , GETDATE() ) ";
                sqlCommand = new SqlCommand(text, sqlConnection);
                sqlCommand = new SqlCommand(text, sqlConnection);
                sqlCommand.CommandTimeout = 0;
                sqlCommand.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR DB (" + text + ")" + ex.Message, EventLogEntryType.Error);
        }
        finally
        {
            sqlConnection?.Close();
        }
        bool flag2 = false;
        try
        {
            sqlConnection = obtenerConexion();
            text = " SELECT p.ip , p.prioridad ,  isnull( ( SELECT a.fecha FROM AUT_ALIVE a WHERE a.ip = p.ip ) , '2014-01-01') alive , GETDATE() now  FROM AUT_PRIORIDAD p , AUT_PRIORIDAD px  WHERE p.prioridad <= px.prioridad AND px.ip = '" + ipAddress + "' ORDER BY prioridad ";
            SqlCommand sqlCommand2 = new SqlCommand(text, sqlConnection);
            SqlDataReader sqlDataReader2 = sqlCommand2.ExecuteReader();
            while (sqlDataReader2.Read())
            {
                DateTime date = (DateTime)sqlDataReader2["alive"];
                DateTime date2 = (DateTime)sqlDataReader2["now"];
                string text2 = (string)sqlDataReader2["ip"];
                _ = (int)sqlDataReader2["prioridad"];
                long num = DateAndTime.DateDiff(DateInterval.Second, date, date2);
                if (ipAddress.Trim().Equals(text2.Trim()))
                {
                    flag2 = true;
                    break;
                }
                if (num < 240)
                {
                    flag2 = false;
                    break;
                }
            }
            sqlDataReader2.Close();
        }
        catch (Exception ex2)
        {
            eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR DB (" + text + ")" + ex2.Message, EventLogEntryType.Error);
        }
        finally
        {
            sqlConnection?.Close();
        }
        if (!flag2 || flagProcesando)
        {
            return;
        }
        flagProcesando = true;
        try
        {
            sqlConnection = obtenerConexion();
            SqlDataReader sqlDataReader3 = null;
            int id_ejecucion = 0;
            text = "SELECT e.id_ejecucion , e.fecha_hora_inicio , e.fecha_hora_fin , e.estado_ejecucion  FROM AUT_Ejecucion e  WHERE e.estado_ejecucion = 1";
            SqlCommand sqlCommand3 = new SqlCommand(text, sqlConnection);
            sqlDataReader3 = sqlCommand3.ExecuteReader();
            while (sqlDataReader3.Read())
            {
                id_ejecucion = (int)sqlDataReader3["id_ejecucion"];
                _ = (DateTime)sqlDataReader3["fecha_hora_inicio"];
                _ = (int)sqlDataReader3["estado_ejecucion"];
            }
            sqlDataReader3.Close();
            if (id_ejecucion == 0)
            {
                bool flag3 = false;
                text = " SELECT MAX(s.fecha_hora_inicio_ejecucion ) siguiente_ejecucion  FROM AUT_Programacion_Proceso s  WHERE s.fecha_hora_inicio_ejecucion <= GETDATE()  HAVING MAX(s.fecha_hora_inicio_ejecucion ) >  ( ISNULL( (SELECT MAX( e.fecha_hora_inicio ) FROM AUT_Ejecucion e ) , '2014-01-01' ) ) ";
                sqlCommand3 = new SqlCommand(text, sqlConnection);
                sqlDataReader3 = sqlCommand3.ExecuteReader();
                if (sqlDataReader3.Read())
                {
                    flag3 = true;
                }
                sqlDataReader3.Close();
                if (flag3)
                {
                    text = "SELECT ISNULL(MAX(id_ejecucion),0) id_ejecucion FROM AUT_Ejecucion";
                    sqlCommand3 = new SqlCommand(text, sqlConnection);
                    sqlDataReader3 = sqlCommand3.ExecuteReader();
                    if (sqlDataReader3.Read())
                    {
                        id_ejecucion = (int)sqlDataReader3["id_ejecucion"];
                    }
                    sqlDataReader3.Close();
                    id_ejecucion++;
                    text = "INSERT INTO AUT_Ejecucion ( id_ejecucion , fecha_hora_inicio , estado_ejecucion)   VALUES ( " + id_ejecucion + " , GETDATE() ,  1 ) ";
                    sqlCommand3 = new SqlCommand(text, sqlConnection);
                    sqlCommand3.ExecuteNonQuery();
                    Hashtable hashtable = obtenerVariables(sqlConnection, "NOTIFICAR_MAIL_INICIO_PROCESO");
                    enviar_notificacion_mail(sqlConnection, (string)hashtable["NOTIFICAR_MAIL_INICIO_PROCESO"], "[Proceso Automatico SAI] Inicio", "Inicia en este momento el proceso automatico SAI");
                    List<int> list2 = new List<int>();
                    List<int> list3 = new List<int>();
                    List<int> list4 = new List<int>();
                    text = "SELECT compania_id , mesCierre, anioCierre, estado  FROM PeriodoCierre  WHERE anioCierre = 2014 AND estado = 1 AND fechaCierre < GETDATE()  ORDER BY anioCierre , mesCierre , compania_id ";
                    sqlCommand3 = new SqlCommand(text, sqlConnection);
                    sqlDataReader3 = sqlCommand3.ExecuteReader();
                    while (sqlDataReader3.Read())
                    {
                        list2.Add((int)sqlDataReader3["compania_id"]);
                        list3.Add((int)sqlDataReader3["mesCierre"]);
                        list4.Add((int)sqlDataReader3["anioCierre"]);
                    }
                    sqlDataReader3.Close();
                    for (int i = 0; i < list2.Count; i++)
                    {
                        text = " UPDATE Recaudo SET estadoCierre = 2  WHERE mesCierre = " + list3[i] + " AND anioCierre = " + list4[i] + " AND compania_id = " + list2[i];
                        sqlCommand3 = new SqlCommand(text, sqlConnection);
                        sqlCommand3.CommandTimeout = 0;
                        sqlCommand3.ExecuteNonQuery();
                        enviar_notificacion_mail(sqlConnection, (string)hashtable["NOTIFICAR_MAIL_INICIO_PROCESO"], "[Proceso Automatico SAI] Cerrando MES RECAUDO ", text);
                        text = " UPDATE Negocio SET estadoCierre = 2  WHERE mesCierre = " + list3[i] + " AND anioCierre = " + list4[i] + " AND compania_id = " + list2[i];
                        sqlCommand3 = new SqlCommand(text, sqlConnection);
                        sqlCommand3.CommandTimeout = 0;
                        sqlCommand3.ExecuteNonQuery();
                        enviar_notificacion_mail(sqlConnection, (string)hashtable["NOTIFICAR_MAIL_INICIO_PROCESO"], "[Proceso Automatico SAI] Cerrando MES NEGOCIO ", text);
                        text = " UPDATE PeriodoCierre SET estado =  2  WHERE estado = 1 AND  mesCierre = " + list3[i] + " AND anioCierre = " + list4[i] + " AND compania_id = " + list2[i];
                        sqlCommand3 = new SqlCommand(text, sqlConnection);
                        sqlCommand3.CommandTimeout = 0;
                        int num2 = sqlCommand3.ExecuteNonQuery();
                        if (num2 > 0)
                        {
                            enviar_notificacion_mail(sqlConnection, (string)hashtable["NOTIFICAR_MAIL_INICIO_PROCESO"], "[Proceso Automatico SAI] Cerrando PeriodoCierre ", text);
                            text = " UPDATE PeriodoCierre SET estado = 1  WHERE estado = 0  AND compania_id = " + list2[i] + " AND fechaCierre =  ( SELECT MIN(fechaCierre) FROM PeriodoCierre c2      WHERE c2.estado = 0      AND c2.compania_id = PeriodoCierre.compania_id  ) ";
                            sqlCommand3 = new SqlCommand(text, sqlConnection);
                            sqlCommand3.CommandTimeout = 0;
                            num2 = sqlCommand3.ExecuteNonQuery();
                            if (num2 > 0)
                            {
                                enviar_notificacion_mail(sqlConnection, (string)hashtable["NOTIFICAR_MAIL_INICIO_PROCESO"], "[Proceso Automatico SAI] Abriendo PeriodoCierre ", num2 + " Registros afectados \r\n" + text + "\r\n");
                            }
                        }
                    }
                }
            }
            if (id_ejecucion == 0)
            {
                flagProcesando = false;
                return;
            }
            text = "select id, nombre_proceso, tipo_proceso_id, max_reintentos, max_tiempo_ejecucion, compania_id, requiere_backup, tabla,notificar_inicio ,notificar_error ,notificar_fin   FROM AUT_Proceso WHERE habilitado = 1 order by id ";
            sqlCommand3 = new SqlCommand(text, sqlConnection);
            sqlDataReader3 = sqlCommand3.ExecuteReader();
            while (sqlDataReader3.Read())
            {
                ProcesoObj procesoObj = new ProcesoObj();
                procesoObj.id = (int)sqlDataReader3["id"];
                procesoObj.nombre_proceso = (string)sqlDataReader3["nombre_proceso"];
                procesoObj.tipo_proceso_id = (int)sqlDataReader3["tipo_proceso_id"];
                procesoObj.max_reintentos = (int)sqlDataReader3["max_reintentos"];
                procesoObj.max_tiempo_ejecucion = (int)sqlDataReader3["max_tiempo_ejecucion"];
                procesoObj.compania_id = (int)sqlDataReader3["compania_id"];
                procesoObj.requiere_backup = (int)sqlDataReader3["requiere_backup"];
                procesoObj.tabla = (string)sqlDataReader3["tabla"];
                procesoObj.notificar_inicio = (string)sqlDataReader3["notificar_inicio"];
                procesoObj.notificar_error = (string)sqlDataReader3["notificar_error"];
                procesoObj.notificar_fin = (string)sqlDataReader3["notificar_fin"];
                list.Add(procesoObj);
            }
            sqlDataReader3.Close();
            int num3 = 0;
            int num4 = 0;
            for (int j = 0; j < list.Count; j++)
            {
                ProcesoObj proceso = list[j];
                int num5 = proceso_enviado(id_ejecucion, proceso, sqlConnection);
                if (num5 == 1 || num5 == 5 || num5 == 0)
                {
                    num3++;
                }
                if (num5 == 3)
                {
                    num4++;
                }
                if (num5 == 5)
                {
                    Thread thread = new Thread((ThreadStart)delegate
                    {
                        ejecutar_validacion_archivo(id_ejecucion, proceso);
                    });
                    thread.Start();
                }
                if (num5 == 0 && proceso.requiere_backup == 1 && proceso.tabla.Equals("Recaudo"))
                {
                    int anioCierre = 0;
                    int mesCierre = 0;
                    text = "SELECT compania_id , anioCierre, mesCierre , fechaInicio, fechaFin ,fechaCierre  FROM PeriodoCierre WHERE estado = 1  AND compania_id = " + proceso.compania_id;
                    sqlCommand3 = new SqlCommand(text, sqlConnection);
                    sqlDataReader3 = sqlCommand3.ExecuteReader();
                    if (sqlDataReader3.Read())
                    {
                        anioCierre = (int)sqlDataReader3["anioCierre"];
                        mesCierre = (int)sqlDataReader3["mesCierre"];
                    }
                    sqlDataReader3.Close();
                    generarBackupRecaudo(sqlConnection, id_ejecucion, anioCierre, mesCierre, proceso.compania_id);
                }
                if (num5 == 0 && proceso.requiere_backup == 1 && proceso.tabla.Equals("Negocio"))
                {
                    int anioCierre2 = 0;
                    int mesCierre2 = 0;
                    text = "SELECT compania_id , anioCierre, mesCierre , fechaInicio, fechaFin ,fechaCierre  FROM PeriodoCierre WHERE estado = 1  AND compania_id = " + proceso.compania_id;
                    sqlCommand3 = new SqlCommand(text, sqlConnection);
                    sqlDataReader3 = sqlCommand3.ExecuteReader();
                    if (sqlDataReader3.Read())
                    {
                        anioCierre2 = (int)sqlDataReader3["anioCierre"];
                        mesCierre2 = (int)sqlDataReader3["mesCierre"];
                    }
                    sqlDataReader3.Close();
                    generarBackupNegocio(sqlConnection, id_ejecucion, anioCierre2, mesCierre2, proceso.compania_id);
                }
                if (num5 == -2)
                {
                    text = " INSERT INTO AUT_Ejecucion_Proceso  ( id_ejecucion , id_proceso, fecha_inicio, fecha_fin, estado )  VALUES ( @id_ejecucion , @id_proceso , getdate() , getdate() ,  6 )  ";
                    eventLog1.WriteEntry("PE: " + text, EventLogEntryType.Warning);
                    sqlCommand3 = new SqlCommand(text, sqlConnection);
                    sqlCommand3.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                    sqlCommand3.Parameters["@id_ejecucion"].Value = id_ejecucion;
                    sqlCommand3.Parameters.Add("@id_proceso", SqlDbType.Int);
                    sqlCommand3.Parameters["@id_proceso"].Value = proceso.id;
                    sqlCommand3.ExecuteNonQuery();
                }
                if (num5 != 0)
                {
                    continue;
                }
                if (proceso.tipo_proceso_id == 1)
                {
                    Thread thread2 = new Thread((ThreadStart)delegate
                    {
                        ejecutar_proceso_extraccion(id_ejecucion, proceso);
                    });
                    thread2.Start();
                }
                if (proceso.tipo_proceso_id == 3)
                {
                    Thread thread3 = new Thread((ThreadStart)delegate
                    {
                        ejecutar_proceso_etl(id_ejecucion, proceso);
                    });
                    thread3.Start();
                }
            }
            if (num3 == 0)
            {
                text = ((num4 != 0) ? ("UPDATE AUT_Ejecucion SET fecha_hora_fin = GETDATE() , estado_ejecucion = 3 WHERE id_ejecucion = " + id_ejecucion) : ("UPDATE AUT_Ejecucion SET fecha_hora_fin = GETDATE() , estado_ejecucion = 2 WHERE id_ejecucion = " + id_ejecucion));
                sqlCommand3 = new SqlCommand(text, sqlConnection);
                sqlCommand3.ExecuteNonQuery();
                Hashtable hashtable2 = obtenerVariables(sqlConnection, "NOTIFICAR_MAIL_FIN_PROCESO");
                enviar_notificacion_mail(sqlConnection, (string)hashtable2["NOTIFICAR_MAIL_FIN_PROCESO"], "[Proceso Automatico SAI] Fin", "Termina en este momento el proceso automatico SAI");
            }
            sqlConnection.Close();
        }
        catch (Exception ex3)
        {
            eventLog1.WriteEntry("Proceso Automático Error Lectura: ( " + text + " ) \r\n" + ex3.Message, EventLogEntryType.Error);
            try
            {
                sqlConnection?.Close();
            }
            catch (Exception ex4)
            {
                eventLog1.WriteEntry("Proceso Automático Error 2:" + ex4.Message, EventLogEntryType.Error);
            }
        }
        flagProcesando = false;
    }

    private int proceso_enviado(int id_ejecucion, ProcesoObj proceso, SqlConnection myConnection)
    {
        int num = 0;
        int num2 = 0;
        int num3 = 0;
        int num4 = 0;
        int num5 = 0;
        string cmdText = "SELECT id_ejecucion, id_proceso, fecha_inicio, fecha_fin, estado FROM AUT_Ejecucion_Proceso   WHERE id_ejecucion =  " + id_ejecucion + " AND id_proceso = " + proceso.id;
        SqlCommand sqlCommand = new SqlCommand(cmdText, myConnection);
        SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
        while (sqlDataReader.Read())
        {
            switch ((int)sqlDataReader["estado"])
            {
                case 1:
                    num3++;
                    break;
                case 2:
                    num2++;
                    break;
                case 5:
                    num4++;
                    break;
                case 6:
                    num5++;
                    break;
                default:
                    num++;
                    break;
            }
        }
        sqlDataReader.Close();
        if (num2 > 0)
        {
            return 2;
        }
        if (num3 > 0)
        {
            return 1;
        }
        if (num4 > 0)
        {
            return 5;
        }
        if (num5 > 0)
        {
            return 6;
        }
        if (num > proceso.max_reintentos)
        {
            return 3;
        }
        cmdText = " SELECT d.id_proceso_requerido , d.en_error_proceso_requerido, p.habilitado FROM AUT_Proceso_Dependencia d  JOIN AUT_Proceso p ON p.id = d.id_proceso_requerido  WHERE d.id = " + proceso.id;
        sqlCommand = new SqlCommand(cmdText, myConnection);
        bool flag = true;
        sqlDataReader = sqlCommand.ExecuteReader();
        List<int> list = new List<int>();
        List<int> list2 = new List<int>();
        while (sqlDataReader.Read())
        {
            int num6 = (int)sqlDataReader["habilitado"];
            if (num6 == 1)
            {
                list.Add((int)sqlDataReader["id_proceso_requerido"]);
                list2.Add((int)sqlDataReader["en_error_proceso_requerido"]);
            }
        }
        sqlDataReader.Close();
        bool flag2 = false;
        for (int i = 0; i < list.Count; i++)
        {
            int num7 = list[i];
            int num8 = list2[i];
            int num9 = -1;
            bool flag3 = false;
            cmdText = "SELECT TOP 1 id_ejecucion, id_proceso, fecha_inicio, fecha_fin, estado FROM AUT_Ejecucion_Proceso   WHERE id_ejecucion =  " + id_ejecucion + " AND id_proceso = " + num7 + " ORDER BY fecha_inicio DESC ";
            SqlCommand sqlCommand2 = new SqlCommand(cmdText, myConnection);
            SqlDataReader sqlDataReader2 = sqlCommand2.ExecuteReader();
            if (sqlDataReader2.Read())
            {
                num9 = (int)sqlDataReader2["estado"];
            }
            sqlDataReader2.Close();
            if (num9 >= 0)
            {
                if (num9 == 2)
                {
                    flag3 = true;
                }
                switch (num9)
                {
                    case 6:
                        flag3 = true;
                        break;
                    case 2:
                        flag3 = true;
                        break;
                    case 3:
                        {
                            int num10 = 0;
                            int num11 = 0;
                            cmdText = "SELECT COUNT(*) errados FROM AUT_Ejecucion_Proceso   WHERE estado = 3 AND id_ejecucion =  " + id_ejecucion + " AND id_proceso = " + num7;
                            sqlCommand2 = new SqlCommand(cmdText, myConnection);
                            sqlDataReader2 = sqlCommand2.ExecuteReader();
                            if (sqlDataReader2.Read())
                            {
                                num10 = (int)sqlDataReader2["errados"];
                            }
                            sqlDataReader2.Close();
                            cmdText = "SELECT  max_reintentos FROM AUT_Proceso   WHERE id = " + num7;
                            sqlCommand2 = new SqlCommand(cmdText, myConnection);
                            sqlDataReader2 = sqlCommand2.ExecuteReader();
                            if (sqlDataReader2.Read())
                            {
                                num11 = (int)sqlDataReader2["max_reintentos"];
                            }
                            sqlDataReader2.Close();
                            if (num8 == 1 && num10 > num11)
                            {
                                flag3 = true;
                            }
                            if (num8 == 2 && num10 > num11)
                            {
                                flag3 = true;
                                flag2 = true;
                            }
                            if (num8 == 3 && num10 > num11)
                            {
                                flag3 = false;
                            }
                            break;
                        }
                }
            }
            flag = flag && flag3;
        }
        if (!flag)
        {
            return -1;
        }
        if (flag && flag2)
        {
            return -2;
        }
        eventLog1.WriteEntry("PROCESO A INICIAR: " + proceso.id, EventLogEntryType.Information);
        return 0;
    }

    private bool ejecutar_proceso_etl(int id_ejecucion, ProcesoObj proceso)
    {
        string idApp = "";
        string text = "";
        string packageConfigFileName = "";
        bool flag = false;
        SqlConnection sqlConnection = obtenerConexion();
        string text2 = "SELECT idApp, packageFileName, packageConfigFileName FROM AUT_Proceso_ETL   WHERE id_proceso = " + proceso.id;
        SqlCommand sqlCommand = new SqlCommand(text2, sqlConnection);
        SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
        if (sqlDataReader.Read())
        {
            flag = true;
            idApp = (string)sqlDataReader["idApp"];
            text = (string)sqlDataReader["packageFileName"];
            packageConfigFileName = (string)sqlDataReader["packageConfigFileName"];
        }
        sqlDataReader.Close();
        if (flag)
        {
            PackagesExecutionServiceClient packagesExecutionServiceClient = null;
            try
            {
                text2 = " INSERT INTO AUT_Ejecucion_Proceso  ( id_ejecucion , id_proceso, fecha_inicio, estado )  VALUES ( @id_ejecucion , @id_proceso , getdate() , 1 )  ";
                sqlCommand = new SqlCommand(text2, sqlConnection);
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.ExecuteNonQuery();
                enviar_notificacion_mail(sqlConnection, proceso.notificar_inicio, "[Proceso Automatico SAI] Inicia ETL " + proceso.nombre_proceso, "Inicia ETL " + text);
            }
            catch (Exception ex)
            {
                eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR DB (" + text2 + ")" + ex.Message, EventLogEntryType.Error);
            }
            finally
            {
                sqlConnection.Close();
            }
            try
            {
                int num = 2;
                string text3 = "";
                if (proceso.nombre_proceso.Equals("ETL_COMJSN"))
                {
                    try
                    {
                        ejecutaCOMJSN();
                    }
                    catch (Exception ex2)
                    {
                        num = 3;
                        text3 = ex2.Message;
                    }
                }
                else
                {
                    packagesExecutionServiceClient = new PackagesExecutionServiceClient();
                    eventLog1.WriteEntry("Ejecutanto ETL " + text);
                    Variable[] variables = new Variable[0];
                    DTSResponse dTSResponse = packagesExecutionServiceClient.ExecuteFromPackageFile(idApp, text, packageConfigFileName, variables);
                    if (dTSResponse.Fail)
                    {
                        eventLog1.WriteEntry("ETL ejecutada con errores " + text, EventLogEntryType.Error);
                        num = 3;
                        string[] dtsErrors = dTSResponse.DtsErrors;
                        foreach (string text4 in dtsErrors)
                        {
                            eventLog1.WriteEntry("ETL ERROR (" + text + "): " + text4, EventLogEntryType.Error);
                            if (text3.Length > 0)
                            {
                                text3 += "\n";
                            }
                            text3 += text4;
                        }
                    }
                    packagesExecutionServiceClient.Close();
                }
                eventLog1.WriteEntry("ETL ejecutada " + text);
                sqlConnection = obtenerConexion();
                text2 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = getdate() , estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 1 ";
                sqlCommand = new SqlCommand(text2, sqlConnection);
                sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                sqlCommand.Parameters["@estado"].Value = num;
                sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                sqlCommand.Parameters["@detalle"].Value = text3;
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.ExecuteNonQuery();
                if (num == 2)
                {
                    enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] Termina ETL " + proceso.nombre_proceso, "Termina ETL " + text);
                }
                else
                {
                    restaurarPorErrorEnETL(sqlConnection, id_ejecucion, proceso);
                    enviar_notificacion_mail(sqlConnection, proceso.notificar_error, "[Proceso Automatico SAI] Error en ETL " + proceso.nombre_proceso, "Error en ETL " + text + ".\r\n" + text3);
                }
                sqlConnection.Close();
            }
            catch (Exception ex3)
            {
                eventLog1.WriteEntry("Error ejecutando etl (" + text + "): " + ex3.Message, EventLogEntryType.Error);
                sqlConnection = obtenerConexion();
                restaurarPorErrorEnETL(sqlConnection, id_ejecucion, proceso);
                text2 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = getdate() , estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 1 ";
                sqlCommand = new SqlCommand(text2, sqlConnection);
                sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                sqlCommand.Parameters["@estado"].Value = 3;
                sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                sqlCommand.Parameters["@detalle"].Value = ex3.Message;
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.ExecuteNonQuery();
                enviar_notificacion_mail(sqlConnection, proceso.notificar_error, "[Proceso Automatico SAI] Error en ETL " + proceso.nombre_proceso, "Error en ETL " + text + ".\r\n" + ex3.Message);
                sqlConnection.Close();
            }
            finally
            {
                packagesExecutionServiceClient?.Close();
                if (sqlConnection != null)
                {
                    try
                    {
                        sqlConnection.Close();
                    }
                    catch (Exception ex4)
                    {
                        eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR DB " + ex4.Message, EventLogEntryType.Error);
                    }
                }
            }
        }
        return flag;
    }

    private void ejecutar_proceso_extraccion(int id_ejecucion, ProcesoObj proceso)
    {
        string text = "";
        string text2 = "";
        string text3 = "";
        string text4 = "";
        string text5 = "";
        string text6 = "";
        string text7 = "";
        string text8 = "";
        AseConnection aseConnection = null;
        AseCommand aseCommand = null;
        Hashtable hashtable = null;
        string text9 = "";
        string text10 = "";
        string text11 = "";
        SqlConnection sqlConnection = obtenerConexion();
        SqlCommand sqlCommand = null;
        DateTime dateTime = DateTime.Now;
        DateTime dateTime2 = DateTime.Now;
        int num = 0;
        int num2 = 0;
        string text12 = "";
        int num3 = 0;
        string text13 = "";
        string text14 = "";
        string text15 = "";
        string text16 = "";
        string text17 = "";
        string text18 = "10.132.82.215";
        string text19 = "";
        string text20 = "";
        string text21 = "";
        try
        {
            text9 = "SELECT id_proceso, id_tipo_servicio,data_source,company,program,library,parameters,user_envio,archivos  FROM AUT_Proceso_Extraccion WHERE id_proceso = " + proceso.id;
            sqlCommand = new SqlCommand(text9, sqlConnection);
            SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
            if (sqlDataReader.Read())
            {
                num3 = (int)sqlDataReader["id_tipo_servicio"];
                text13 = (string)sqlDataReader["data_source"];
                text14 = (string)sqlDataReader["company"];
                text15 = (string)sqlDataReader["program"];
                text16 = (string)sqlDataReader["library"];
                text17 = (string)sqlDataReader["parameters"];
                text19 = (string)sqlDataReader["user_envio"];
                text21 = (string)sqlDataReader["archivos"];
            }
            sqlDataReader.Close();
            if (proceso.nombre_proceso.Equals("EXTRACCION_GENERALES_NEGOCIO"))
            {
                hashtable = obtenerVariables(sqlConnection, "SISE_HOST", "SISE_PORT", "SISE_USER", "SISE_PASS", "GENERALES_NEGOCIO_DATABASE", "GENERALES_NEGOCIO_SP", "GENERALES_NEGOCIO_REPORTE_TABLA", "GENERALES_NEGOCIO_REPORTE_COLUMNA");
                text = (string)hashtable["SISE_HOST"];
                text2 = (string)hashtable["SISE_PORT"];
                text3 = (string)hashtable["SISE_USER"];
                text4 = (string)hashtable["SISE_PASS"];
                text5 = (string)hashtable["GENERALES_NEGOCIO_DATABASE"];
                text6 = (string)hashtable["GENERALES_NEGOCIO_SP"];
                text7 = (string)hashtable["GENERALES_NEGOCIO_REPORTE_TABLA"];
                text8 = (string)hashtable["GENERALES_NEGOCIO_REPORTE_COLUMNA"];
            }
            if (proceso.nombre_proceso.Equals("EXTRACCION_GENERALES_RECAUDO"))
            {
                hashtable = obtenerVariables(sqlConnection, "SISE_HOST", "SISE_PORT", "SISE_USER", "SISE_PASS", "GENERALES_RECAUDO_DATABASE", "GENERALES_RECAUDO_SP", "GENERALES_RECAUDO_REPORTE_TABLA", "GENERALES_RECAUDO_REPORTE_COLUMNA");
                text = (string)hashtable["SISE_HOST"];
                text2 = (string)hashtable["SISE_PORT"];
                text3 = (string)hashtable["SISE_USER"];
                text4 = (string)hashtable["SISE_PASS"];
                text5 = (string)hashtable["GENERALES_RECAUDO_DATABASE"];
                text6 = (string)hashtable["GENERALES_RECAUDO_SP"];
                text7 = (string)hashtable["GENERALES_RECAUDO_REPORTE_TABLA"];
                text8 = (string)hashtable["GENERALES_RECAUDO_REPORTE_COLUMNA"];
            }
            if (proceso.nombre_proceso.Equals("EXTRACCION_GENERALES_PRODUCTOS"))
            {
                hashtable = obtenerVariables(sqlConnection, "SISE_HOST", "SISE_PORT", "SISE_USER", "SISE_PASS", "GENERALES_PRODUCTO_DATABASE", "GENERALES_PRODUCTO_SP", "GENERALES_PRODUCTO_REPORTE_TABLA");
                text = (string)hashtable["SISE_HOST"];
                text2 = (string)hashtable["SISE_PORT"];
                text3 = (string)hashtable["SISE_USER"];
                text4 = (string)hashtable["SISE_PASS"];
                text5 = (string)hashtable["GENERALES_PRODUCTO_DATABASE"];
                text6 = (string)hashtable["GENERALES_PRODUCTO_SP"];
                text7 = (string)hashtable["GENERALES_PRODUCTO_REPORTE_TABLA"];
                text8 = "";
            }
            if (proceso.nombre_proceso.Equals("EXTRACCION_VIDA_NEGOCIO"))
            {
                hashtable = obtenerVariables(sqlConnection, "SISE_HOST", "SISE_PORT", "SISE_USER", "SISE_PASS", "VIDA_NEGOCIO_DATABASE", "VIDA_NEGOCIO_SP", "VIDA_NEGOCIO_REPORTE_TABLA", "VIDA_NEGOCIO_REPORTE_COLUMNA");
                text = (string)hashtable["SISE_HOST"];
                text2 = (string)hashtable["SISE_PORT"];
                text3 = (string)hashtable["SISE_USER"];
                text4 = (string)hashtable["SISE_PASS"];
                text5 = (string)hashtable["VIDA_NEGOCIO_DATABASE"];
                text6 = (string)hashtable["VIDA_NEGOCIO_SP"];
                text7 = (string)hashtable["VIDA_NEGOCIO_REPORTE_TABLA"];
                text8 = (string)hashtable["VIDA_NEGOCIO_REPORTE_COLUMNA"];
            }
            if (proceso.nombre_proceso.Equals("EXTRACCION_VIDA_RECAUDO"))
            {
                hashtable = obtenerVariables(sqlConnection, "SISE_HOST", "SISE_PORT", "SISE_USER", "SISE_PASS", "VIDA_RECAUDO_DATABASE", "VIDA_RECAUDO_SP", "VIDA_RECAUDO_REPORTE_TABLA", "VIDA_RECAUDO_REPORTE_COLUMNA");
                text = (string)hashtable["SISE_HOST"];
                text2 = (string)hashtable["SISE_PORT"];
                text3 = (string)hashtable["SISE_USER"];
                text4 = (string)hashtable["SISE_PASS"];
                text5 = (string)hashtable["VIDA_RECAUDO_DATABASE"];
                text6 = (string)hashtable["VIDA_RECAUDO_SP"];
                text7 = (string)hashtable["VIDA_RECAUDO_REPORTE_TABLA"];
                text8 = (string)hashtable["VIDA_RECAUDO_REPORTE_COLUMNA"];
            }
            if (proceso.nombre_proceso.Equals("EXTRACCION_VIDA_PRODUCTOS"))
            {
                hashtable = obtenerVariables(sqlConnection, "SISE_HOST", "SISE_PORT", "SISE_USER", "SISE_PASS", "VIDA_PRODUCTO_DATABASE", "VIDA_PRODUCTO_SP", "VIDA_PRODUCTO_REPORTE_TABLA");
                text = (string)hashtable["SISE_HOST"];
                text2 = (string)hashtable["SISE_PORT"];
                text3 = (string)hashtable["SISE_USER"];
                text4 = (string)hashtable["SISE_PASS"];
                text5 = (string)hashtable["VIDA_PRODUCTO_DATABASE"];
                text6 = (string)hashtable["VIDA_PRODUCTO_SP"];
                text7 = (string)hashtable["VIDA_PRODUCTO_REPORTE_TABLA"];
                text8 = "";
            }
            text9 = "SELECT compania_id , anioCierre, mesCierre , fechaInicio, fechaFin ,fechaCierre  FROM PeriodoCierre WHERE estado = 1  AND compania_id = " + proceso.compania_id;
            sqlCommand = new SqlCommand(text9, sqlConnection);
            sqlDataReader = sqlCommand.ExecuteReader();
            while (sqlDataReader.Read())
            {
                dateTime = (DateTime)sqlDataReader["fechaInicio"];
                dateTime2 = (DateTime)sqlDataReader["fechaFin"];
                num2 = (int)sqlDataReader["anioCierre"];
                num = (int)sqlDataReader["mesCierre"];
                text10 = string.Concat(dateTime.Year * 10000 + dateTime.Month * 100 + dateTime.Day);
                text11 = dateTime2.Year * 10000 + dateTime2.Month * 100 + dateTime2.Day + " 23:59";
            }
            sqlDataReader.Close();
            if (text6.Length > 0)
            {
                text12 = text6 + " " + text10 + " , " + text11;
            }
            if (proceso.nombre_proceso.Equals("EXTRACCION_BH_NEGOCIO_RECAUDO"))
            {
                text12 = string.Concat("Params: desde=", dateTime, " hasta=", dateTime2);
            }
            if (num3 == 2)
            {
                eventLog1.WriteEntry("Ejecutanto Extraccion ");
                new WsIntegradorSoapClient();
                if (text17.Equals("yyyymmddyyyymmdd"))
                {
                    text20 = string.Concat(dateTime.Year * 10000 + dateTime.Month * 100 + dateTime.Day, dateTime2.Year * 10000 + dateTime2.Month * 100 + dateTime2.Day);
                }
                if (text17.Equals("yyyymmdd"))
                {
                    text20 = string.Concat(dateTime.Year * 10000 + dateTime.Month * 100 + dateTime.Day);
                }
                if (text17.Equals("yyyymmyyyymm"))
                {
                    text20 = string.Concat(dateTime.Year * 100 + dateTime.Month, dateTime2.Year * 100 + dateTime2.Month);
                }
                if (text17.Equals("yyyymm"))
                {
                    text20 = string.Concat(dateTime.Year * 100 + dateTime.Month);
                }
                text12 = text13 + "," + text14 + "," + text15 + "," + text16 + "," + text20 + "," + text18 + "," + text19;
            }
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("ProcesoNocturnoSAI ejecutar_proceso_extraccion ERRROR DB (" + text9 + ")" + ex.Message, EventLogEntryType.Error);
        }
        try
        {
            text9 = " INSERT INTO AUT_Ejecucion_Proceso  ( id_ejecucion , id_proceso, fecha_inicio, estado , detalle )  VALUES ( @id_ejecucion , @id_proceso , getdate() , 1 , @detalle )  ";
            sqlCommand = new SqlCommand(text9, sqlConnection);
            sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
            sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
            sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
            sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
            sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
            sqlCommand.Parameters["@detalle"].Value = text12;
            sqlCommand.ExecuteNonQuery();
            enviar_notificacion_mail(sqlConnection, proceso.notificar_inicio, "[Proceso Automatico SAI] Inicia extraccion " + proceso.nombre_proceso, "Inicia extraccion ");
            sqlConnection.Close();
        }
        catch (Exception ex2)
        {
            eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR DB (" + text9 + ")" + ex2.Message, EventLogEntryType.Error);
        }
        try
        {
            if (text6.Length > 0 && num3 == 1)
            {
                eventLog1.WriteEntry("Ejecutando proceso: " + proceso.nombre_proceso);
                if (!proceso.nombre_proceso.Equals("EXTRACCION_GENERALES_PRODUCTOS") && !proceso.nombre_proceso.Equals("EXTRACCION_VIDA_PRODUCTOS"))
                {
                    aseConnection = new AseConnection("Data Source='" + text + "';Port='" + text2 + "';UID='" + text3 + "';PWD='" + text4 + "';Database='" + text5 + "';");
                    aseConnection.Open();
                    aseCommand = new AseCommand("USE " + text5, aseConnection);
                    aseCommand.CommandType = CommandType.Text;
                    aseCommand.ExecuteNonQuery();
                    eventLog1.WriteEntry("Armando sp: " + text6);
                    aseCommand = new AseCommand(text6, aseConnection);
                    aseCommand.CommandType = CommandType.StoredProcedure;
                    aseCommand.CommandTimeout = 0;
                    AseParameter aseParameter = aseCommand.Parameters.Add("@fec_ini", AseDbType.VarChar, 125);
                    aseParameter.Direction = ParameterDirection.Input;
                    AseParameter aseParameter2 = aseCommand.Parameters.Add("@fec_fin", AseDbType.VarChar, 125);
                    aseParameter2.Direction = ParameterDirection.Input;
                    aseParameter.Value = text10;
                    aseParameter2.Value = text11;
                    eventLog1.WriteEntry("Ejecutando sentencia: " + text6 + " " + text10 + " , " + text11);
                    aseCommand.ExecuteNonQuery();
                    eventLog1.WriteEntry("Sentencia ejecutada: " + text6 + " " + text10 + " , " + text11, EventLogEntryType.SuccessAudit);
                    text9 = "SELECT  isnull(sum( " + text8 + "  ),0) valor_total, count(*) cantidad_registros  FROM " + text7;
                    aseCommand = new AseCommand("USE reportes", aseConnection);
                    aseCommand.CommandType = CommandType.Text;
                    aseCommand.ExecuteNonQuery();
                    aseCommand = new AseCommand(text9, aseConnection);
                    aseCommand.CommandType = CommandType.Text;
                    AseDataReader aseDataReader = aseCommand.ExecuteReader();
                    int num4 = 0;
                    decimal num5 = 0m;
                    if (aseDataReader.Read())
                    {
                        eventLog1.WriteEntry(string.Concat("cantidad_registros:[", aseDataReader["cantidad_registros"], "]"), EventLogEntryType.SuccessAudit);
                        eventLog1.WriteEntry(string.Concat("valor_total:[", aseDataReader["valor_total"], "]"), EventLogEntryType.SuccessAudit);
                        num4 = (int)aseDataReader["cantidad_registros"];
                        num5 = (decimal)aseDataReader["valor_total"];
                    }
                    aseDataReader.Close();
                    sqlConnection = obtenerConexion();
                    text9 = "SELECT tabla,compania_id,anio_cierre,mes_cierre,cantidad_registros,valor_total FROM AUT_LOG_EXTRACCIONES WHERE tabla = '" + proceso.tabla + "' AND compania_id = " + proceso.compania_id + " AND anio_cierre = " + num2 + " AND mes_cierre = " + num;
                    sqlCommand = new SqlCommand(text9, sqlConnection);
                    SqlDataReader sqlDataReader2 = sqlCommand.ExecuteReader();
                    int num6 = 0;
                    eventLog1.WriteEntry("Reading 2", EventLogEntryType.SuccessAudit);
                    if (sqlDataReader2.Read())
                    {
                        eventLog1.WriteEntry(string.Concat("cantidad_registros anterior:[", sqlDataReader2["cantidad_registros"], "]"), EventLogEntryType.SuccessAudit);
                        eventLog1.WriteEntry(string.Concat("valor_total anterior:[", sqlDataReader2["valor_total"], "]"), EventLogEntryType.SuccessAudit);
                        num6 = (int)sqlDataReader2["cantidad_registros"];
                        decimal.Parse(sqlDataReader2["valor_total"].ToString());
                    }
                    sqlDataReader2.Close();
                    bool flag = false;
                    int num7 = 3;
                    string text22 = "Cantidad o valor de extracción menor a extracción anterior";
                    if (num4 >= num6)
                    {
                        flag = true;
                        num7 = 2;
                        text22 = "";
                    }
                    if (flag)
                    {
                        eventLog1.WriteEntry("Extraccion OK " + proceso.nombre_proceso, EventLogEntryType.SuccessAudit);
                        text9 = "DELETE FROM AUT_LOG_EXTRACCIONES WHERE tabla = '" + proceso.tabla + "' AND compania_id = " + proceso.compania_id + " AND anio_cierre = " + num2 + " AND mes_cierre = " + num;
                        sqlCommand = new SqlCommand(text9, sqlConnection);
                        sqlCommand.ExecuteNonQuery();
                        text9 = "INSERT INTO AUT_LOG_EXTRACCIONES ( tabla,compania_id,anio_cierre,mes_cierre,cantidad_registros,valor_total )  VALUES (  '" + proceso.tabla + "' , " + proceso.compania_id + " , " + num2 + " , " + num + " , " + num4 + " , " + string.Concat(num5).Replace(",", ".") + " )";
                        sqlCommand = new SqlCommand(text9, sqlConnection);
                        sqlCommand.ExecuteNonQuery();
                    }
                    text9 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = getdate() , estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 1 ";
                    eventLog1.WriteEntry("PL: " + text9);
                    sqlCommand = new SqlCommand(text9, sqlConnection);
                    sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                    sqlCommand.Parameters["@estado"].Value = num7;
                    sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                    sqlCommand.Parameters["@detalle"].Value = text12 + " \r\n" + text22;
                    sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                    sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                    sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                    sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                    sqlCommand.ExecuteNonQuery();
                    if (flag)
                    {
                        enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] Termina extraccion " + proceso.nombre_proceso, "Fin extraccion ");
                    }
                    else
                    {
                        enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] ERROR VALIDACION TOTALES extraccion " + proceso.nombre_proceso, text12 + " \r\n" + text22);
                    }
                    sqlConnection.Close();
                    aseConnection.Close();
                }
                else
                {
                    aseConnection = new AseConnection("Data Source='" + text + "';Port='" + text2 + "';UID='" + text3 + "';PWD='" + text4 + "';Database='" + text5 + "';");
                    aseConnection.Open();
                    aseCommand = new AseCommand("USE " + text5, aseConnection);
                    aseCommand.CommandType = CommandType.Text;
                    aseCommand.ExecuteNonQuery();
                    eventLog1.WriteEntry("Armando sp: " + text6);
                    aseCommand = new AseCommand(text6, aseConnection);
                    aseCommand.CommandType = CommandType.StoredProcedure;
                    aseCommand.CommandTimeout = 0;
                    sqlConnection = obtenerConexion();
                    eventLog1.WriteEntry("Ejecutando sentencia: " + text6);
                    aseCommand.ExecuteNonQuery();
                    eventLog1.WriteEntry("Sentencia ejecutada: " + text6, EventLogEntryType.SuccessAudit);
                    bool flag2 = true;
                    int num8 = 2;
                    string text23 = "";
                    text9 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = getdate() , estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 1 ";
                    eventLog1.WriteEntry("PL: " + text9);
                    sqlCommand = new SqlCommand(text9, sqlConnection);
                    sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                    sqlCommand.Parameters["@estado"].Value = num8;
                    sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                    sqlCommand.Parameters["@detalle"].Value = text12 + " \r\n" + text23;
                    sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                    sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                    sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                    sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                    sqlCommand.ExecuteNonQuery();
                    if (flag2)
                    {
                        enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] Termina extraccion " + proceso.nombre_proceso, "Fin extraccion ");
                    }
                    else
                    {
                        enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] ERROR VALIDACION TOTALES extraccion " + proceso.nombre_proceso, text12 + " \r\n" + text23);
                    }
                    sqlConnection.Close();
                    aseConnection.Close();
                }
            }
            if (proceso.nombre_proceso.Equals("EXTRACCION_BH_NEGOCIO_RECAUDO") && num3 == 3)
            {
                eventLog1.WriteEntry("Ejecutanto Extraccion ");
                ServiceSoapClient serviceSoapClient = new ServiceSoapClient();
                dateTime2.AddHours(23.0);
                dateTime2.AddMinutes(59.0);
                string text24 = serviceSoapClient.WSIncentivesAdministrationSystem(dateTime, dateTime2);
                serviceSoapClient.Close();
                int num9 = 2;
                eventLog1.WriteEntry("Extraccion BH ejecutada " + text24);
                int nRecords = 0;
                decimal totalValue = 0m;
                int nRecords2 = 0;
                decimal totalValue2 = 0m;
                bool flag3 = true;
                sqlConnection = obtenerConexion();
                Hashtable hashtable2 = obtenerVariables(sqlConnection, "BH_FTP_SERVER", "BH_FTP_USER", "BH_FTP_CLAVE", "BH_FTP_DIR", "BH_NEGOCIO_FTP_ARCHIVO", "BH_RECAUDO_FTP_ARCHIVO");
                flag3 &= checkFTPFileRecords((string)hashtable2["BH_FTP_SERVER"], (string)hashtable2["BH_FTP_USER"], (string)hashtable2["BH_FTP_CLAVE"], (string)hashtable2["BH_FTP_DIR"], (string)hashtable2["BH_NEGOCIO_FTP_ARCHIVO"], out nRecords, out totalValue, 1);
                flag3 &= checkFTPFileRecords((string)hashtable2["BH_FTP_SERVER"], (string)hashtable2["BH_FTP_USER"], (string)hashtable2["BH_FTP_CLAVE"], (string)hashtable2["BH_FTP_DIR"], (string)hashtable2["BH_RECAUDO_FTP_ARCHIVO"], out nRecords2, out totalValue2, 2);
                text9 = "SELECT tabla,compania_id,anio_cierre,mes_cierre,cantidad_registros,valor_total FROM AUT_LOG_EXTRACCIONES WHERE compania_id = " + proceso.compania_id + " AND anio_cierre = " + num2 + " AND mes_cierre = " + num;
                sqlCommand = new SqlCommand(text9, sqlConnection);
                SqlDataReader sqlDataReader3 = sqlCommand.ExecuteReader();
                int num10 = 0;
                string text25 = "";
                while (flag3 && sqlDataReader3.Read())
                {
                    text25 = (string)sqlDataReader3["tabla"];
                    num10 = (int)sqlDataReader3["cantidad_registros"];
                    decimal.Parse(sqlDataReader3["valor_total"].ToString());
                    if (text25.Equals("Negocio") && nRecords < num10)
                    {
                        num9 = 3;
                        text24 += "\nTotales menores a extracción anterior (Negocio)";
                    }
                    if (text25.Equals("Recaudo") && nRecords2 < num10)
                    {
                        num9 = 3;
                        text24 += "\nTotales menores a extracción anterior (Recaudo)";
                    }
                }
                sqlDataReader3.Close();
                if (num9 == 2)
                {
                    eventLog1.WriteEntry("Extraccion OK " + proceso.nombre_proceso, EventLogEntryType.SuccessAudit);
                    text9 = "DELETE FROM AUT_LOG_EXTRACCIONES WHERE compania_id = " + proceso.compania_id + " AND anio_cierre = " + num2 + " AND mes_cierre = " + num;
                    sqlCommand = new SqlCommand(text9, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    text9 = "INSERT INTO AUT_LOG_EXTRACCIONES ( tabla,compania_id,anio_cierre,mes_cierre,cantidad_registros,valor_total )  VALUES (  'Negocio' , " + proceso.compania_id + " , " + num2 + " , " + num + " , " + nRecords + " , " + string.Concat(totalValue).Replace(",", ".") + " )";
                    sqlCommand = new SqlCommand(text9, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    text9 = "INSERT INTO AUT_LOG_EXTRACCIONES ( tabla,compania_id,anio_cierre,mes_cierre,cantidad_registros,valor_total )  VALUES (  'Recaudo' , " + proceso.compania_id + " , " + num2 + " , " + num + " , " + nRecords2 + " , " + string.Concat(totalValue2).Replace(",", ".") + " )";
                    sqlCommand = new SqlCommand(text9, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                }
                text9 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = getdate() , estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 1 ";
                eventLog1.WriteEntry("PL: " + text9);
                sqlCommand = new SqlCommand(text9, sqlConnection);
                sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                sqlCommand.Parameters["@estado"].Value = num9;
                sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                sqlCommand.Parameters["@detalle"].Value = text12 + " \r\n" + text24;
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.ExecuteNonQuery();
                enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] Termina Extracccion " + proceso.nombre_proceso, "Termina Extraccion BH " + text24);
                sqlConnection.Close();
            }
            if (num3 == 2)
            {
                eventLog1.WriteEntry("Ejecutanto Extraccion CAPI ");
                WsIntegradorSoapClient wsIntegradorSoapClient = new WsIntegradorSoapClient();
                sqlConnection = obtenerConexion();
                string[] array = text21.Split(',');
                for (int i = 0; i < array.Length; i++)
                {
                    text9 = " DELETE FROM AUT_Ejecucion_Proceso_Control_Archivos   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND archivo = @archivo ";
                    eventLog1.WriteEntry("PL: " + text9);
                    sqlCommand = new SqlCommand(text9, sqlConnection);
                    sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                    sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                    sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                    sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                    sqlCommand.Parameters.Add("@archivo", SqlDbType.VarChar);
                    sqlCommand.Parameters["@archivo"].Value = array[i].Trim();
                    sqlCommand.ExecuteNonQuery();
                }
                sqlConnection.Close();
                dateTime2.AddHours(23.0);
                dateTime2.AddMinutes(59.0);
                string text26 = wsIntegradorSoapClient.AS4(text13, text14, text15, text16, text20, text18, text19);
                wsIntegradorSoapClient.Close();
                int num11 = 5;
                eventLog1.WriteEntry("Extraccion CAPI " + proceso.nombre_proceso + " pendiente archivo... respuesta del servicio: " + text26);
                sqlConnection = obtenerConexion();
                text9 = " UPDATE AUT_Ejecucion_Proceso  SET estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 1 ";
                eventLog1.WriteEntry("PL: " + text9);
                sqlCommand = new SqlCommand(text9, sqlConnection);
                sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                sqlCommand.Parameters["@estado"].Value = num11;
                sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                sqlCommand.Parameters["@detalle"].Value = text12 + " \r\n" + text26;
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.ExecuteNonQuery();
                enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] Termina Extracccion (Pendiente archivo) " + proceso.nombre_proceso, "Termina Servicio Extraccion, pendiente archivo. Respuesta del servicio: " + proceso.nombre_proceso + " \r\n" + text26);
                for (int j = 0; j < array.Length; j++)
                {
                    text9 = " INSERT INTO AUT_Ejecucion_Proceso_Control_Archivos   ( id_ejecucion, id_proceso, archivo, fecha_registro, peso )  VALUES ( @id_ejecucion , @id_proceso , @archivo , GETDATE() , -1 ) ";
                    eventLog1.WriteEntry("PL: " + text9);
                    sqlCommand = new SqlCommand(text9, sqlConnection);
                    sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                    sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                    sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                    sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                    sqlCommand.Parameters.Add("@archivo", SqlDbType.VarChar);
                    sqlCommand.Parameters["@archivo"].Value = array[j].Trim();
                    sqlCommand.ExecuteNonQuery();
                }
                sqlConnection.Close();
            }
        }
        catch (Exception ex3)
        {
            eventLog1.WriteEntry("Timer 2 Error extraccion ( " + text9 + " ): " + ex3.Message);
            enviar_notificacion_mail(sqlConnection, proceso.notificar_error, "[Proceso Automatico SAI] Error en extraccion " + proceso.nombre_proceso, "Error en extraccion: ( " + text9 + " ) \r\n " + ex3.Message);
            try
            {
                sqlConnection = obtenerConexion();
                text9 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = getdate() , estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 1 ";
                eventLog1.WriteEntry("PL: " + text9);
                sqlCommand = new SqlCommand(text9, sqlConnection);
                sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                sqlCommand.Parameters["@estado"].Value = 3;
                sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                sqlCommand.Parameters["@detalle"].Value = "[" + text12 + "]\n" + ex3.Message;
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.ExecuteNonQuery();
                sqlConnection.Close();
            }
            catch (Exception ex4)
            {
                eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR DB (" + text9 + ")" + ex4.Message);
            }
            try
            {
                aseConnection?.Close();
            }
            catch (Exception ex5)
            {
                eventLog1.WriteEntry("Timer 2 Error sybase 2:" + ex5.Message);
            }
            try
            {
                sqlConnection?.Close();
            }
            catch (Exception ex6)
            {
                eventLog1.WriteEntry("Timer 2 Error myConnection 2:" + ex6.Message);
            }
        }
    }

    private void ejecutar_validacion_archivo(int id_ejecucion, ProcesoObj proceso)
    {
        string text = "";
        string text2 = "";
        string text3 = "";
        string text4 = "";
        string text5 = "";
        SqlConnection sqlConnection = obtenerConexion();
        SqlCommand sqlCommand = null;
        int num = 0;
        int num2 = 0;
        text5 = "SELECT compania_id , anioCierre, mesCierre , fechaInicio, fechaFin ,fechaCierre  FROM PeriodoCierre WHERE estado = 1  AND compania_id = " + proceso.compania_id;
        sqlCommand = new SqlCommand(text5, sqlConnection);
        SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
        if (sqlDataReader.Read())
        {
            num = (int)sqlDataReader["anioCierre"];
            num2 = (int)sqlDataReader["mesCierre"];
        }
        sqlDataReader.Close();
        Hashtable hashtable = obtenerVariables(sqlConnection, "CAPI_FTP_SERVER", "CAPI_FTP_USER", "CAPI_FTP_CLAVE", "CAPI_FTP_DIR");
        text = (string)hashtable["CAPI_FTP_SERVER"];
        text2 = (string)hashtable["CAPI_FTP_USER"];
        text3 = (string)hashtable["CAPI_FTP_CLAVE"];
        text4 = (string)hashtable["CAPI_FTP_DIR"];
        string text6 = "CAPI_FTP_DIR";
        string text7 = "";
        try
        {
            text5 = "SELECT id_proceso, id_tipo_servicio,data_source,company,program,library,parameters,user_envio,archivos  FROM AUT_Proceso_Extraccion WHERE id_proceso = " + proceso.id;
            sqlCommand = new SqlCommand(text5, sqlConnection);
            SqlDataReader sqlDataReader2 = sqlCommand.ExecuteReader();
            if (sqlDataReader2.Read())
            {
                _ = (int)sqlDataReader2["id_tipo_servicio"];
                _ = (string)sqlDataReader2["data_source"];
                _ = (string)sqlDataReader2["company"];
                _ = (string)sqlDataReader2["program"];
                _ = (string)sqlDataReader2["library"];
                _ = (string)sqlDataReader2["parameters"];
                _ = (string)sqlDataReader2["user_envio"];
                text7 = (string)sqlDataReader2["archivos"];
            }
            sqlDataReader2.Close();
            bool flag = true;
            string[] array = text7.Split(',');
            for (int i = 0; i < array.Length; i++)
            {
                long num3 = -2L;
                long num4 = -1L;
                DateTime date = DateTime.Now;
                DateTime date2 = DateTime.Now;
                text5 = " SELECT TOP  1 peso, fecha_registro, getdate() now FROM AUT_Ejecucion_Proceso_Control_Archivos   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND archivo = @archivo  ORDER BY fecha_registro DESC ";
                eventLog1.WriteEntry("PL: " + text5);
                sqlCommand = new SqlCommand(text5, sqlConnection);
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.Parameters.Add("@archivo", SqlDbType.VarChar);
                sqlCommand.Parameters["@archivo"].Value = array[i].Trim();
                sqlDataReader2 = sqlCommand.ExecuteReader();
                if (sqlDataReader2.Read())
                {
                    num3 = (int)sqlDataReader2["peso"];
                    date = (DateTime)sqlDataReader2["fecha_registro"];
                    date2 = (DateTime)sqlDataReader2["now"];
                }
                sqlDataReader2.Close();
                num4 = getFtpFileSize(text, text2, text3, text4, array[i].Trim());
                if (num4 <= 0)
                {
                    flag = false;
                }
                else if (num4 != num3)
                {
                    text5 = " INSERT INTO AUT_Ejecucion_Proceso_Control_Archivos   ( id_ejecucion, id_proceso, archivo, fecha_registro, peso )  VALUES ( @id_ejecucion , @id_proceso , @archivo , GETDATE() , @peso_actual ) ";
                    eventLog1.WriteEntry("PL: " + text5);
                    sqlCommand = new SqlCommand(text5, sqlConnection);
                    sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                    sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                    sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                    sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                    sqlCommand.Parameters.Add("@archivo", SqlDbType.VarChar);
                    sqlCommand.Parameters["@archivo"].Value = array[i].Trim();
                    sqlCommand.Parameters.Add("@peso_actual", SqlDbType.Int);
                    sqlCommand.Parameters["@peso_actual"].Value = num4;
                    sqlCommand.ExecuteNonQuery();
                    flag = false;
                }
                else
                {
                    long num5 = DateAndTime.DateDiff(DateInterval.Second, date, date2);
                    if (num5 < 120)
                    {
                        flag = false;
                    }
                }
            }
            if (flag && proceso.nombre_proceso.Equals("EXTRACCION_CAPI_NEGOCIO_RECAUDO"))
            {
                string text8 = "";
                int nRecords = 0;
                decimal totalValue = 0m;
                int nRecords2 = 0;
                decimal totalValue2 = 0m;
                bool flag2 = true;
                sqlConnection = obtenerConexion();
                flag2 &= checkFTPFileRecords(text, text2, text3, text4, "GNCSPF00", out nRecords, out totalValue, 3);
                flag2 &= checkFTPFileRecords(text, text2, text3, text4, "GRCSPF00", out nRecords2, out totalValue2, 4);
                text5 = "SELECT tabla,compania_id,anio_cierre,mes_cierre,cantidad_registros,valor_total FROM AUT_LOG_EXTRACCIONES WHERE compania_id = " + proceso.compania_id + " AND anio_cierre = " + num + " AND mes_cierre = " + num2;
                sqlCommand = new SqlCommand(text5, sqlConnection);
                sqlDataReader2 = sqlCommand.ExecuteReader();
                int num6 = 0;
                string text9 = "";
                while (flag2 && sqlDataReader2.Read())
                {
                    text9 = (string)sqlDataReader2["tabla"];
                    num6 = (int)sqlDataReader2["cantidad_registros"];
                    decimal.Parse(sqlDataReader2["valor_total"].ToString());
                    if (text9.Equals("Negocio") && nRecords < num6)
                    {
                        flag = false;
                        string text10 = text8;
                        text8 = text10 + "\nTotales menores a extracción anterior (Negocio) (" + nRecords + " < " + num6 + ")";
                    }
                    if (text9.Equals("Recaudo") && nRecords2 < num6)
                    {
                        flag = false;
                        string text11 = text8;
                        text8 = text11 + "\nTotales menores a extracción anterior (Recaudo) (" + nRecords2 + " < " + num6 + ")";
                    }
                }
                sqlDataReader2.Close();
                if (flag)
                {
                    text5 = "DELETE FROM AUT_LOG_EXTRACCIONES WHERE compania_id = " + proceso.compania_id + " AND anio_cierre = " + num + " AND mes_cierre = " + num2;
                    sqlCommand = new SqlCommand(text5, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    text5 = "INSERT INTO AUT_LOG_EXTRACCIONES ( tabla,compania_id,anio_cierre,mes_cierre,cantidad_registros,valor_total )  VALUES (  'Negocio' , " + proceso.compania_id + " , " + num + " , " + num2 + " , " + nRecords + " , " + string.Concat(totalValue).Replace(",", ".") + " )";
                    sqlCommand = new SqlCommand(text5, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    text5 = "INSERT INTO AUT_LOG_EXTRACCIONES ( tabla,compania_id,anio_cierre,mes_cierre,cantidad_registros,valor_total )  VALUES (  'Recaudo' , " + proceso.compania_id + " , " + num + " , " + num2 + " , " + nRecords2 + " , " + string.Concat(totalValue2).Replace(",", ".") + " )";
                    sqlCommand = new SqlCommand(text5, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                }
                if (!flag)
                {
                    text5 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = GETDATE(), estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 5 ";
                    eventLog1.WriteEntry("PL: " + text5);
                    sqlCommand = new SqlCommand(text5, sqlConnection);
                    sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                    sqlCommand.Parameters["@estado"].Value = 3;
                    sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                    sqlCommand.Parameters["@detalle"].Value = text8;
                    sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                    sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                    sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                    sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                    sqlCommand.ExecuteNonQuery();
                    enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] ERROR GENERACION ARCHIVO " + proceso.nombre_proceso, "Termina Servicio Extraccion, archivo generado con ERRORES.\n" + text8);
                }
            }
            if (flag)
            {
                text5 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = GETDATE(), estado = @estado    WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 5 ";
                eventLog1.WriteEntry("PL: " + text5);
                sqlCommand = new SqlCommand(text5, sqlConnection);
                sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                sqlCommand.Parameters["@estado"].Value = 2;
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.ExecuteNonQuery();
                enviar_notificacion_mail(sqlConnection, proceso.notificar_fin, "[Proceso Automatico SAI] Termina GENERACION ARCHIVO " + proceso.nombre_proceso, "Termina Servicio Extraccion, archivo generado.");
            }
            sqlConnection.Close();
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("Timer 2 Error generacion archivos:" + ex.Message);
            try
            {
                sqlConnection = obtenerConexion();
                text5 = " UPDATE AUT_Ejecucion_Proceso  SET fecha_fin = getdate() , estado = @estado , detalle = @detalle   WHERE id_ejecucion = @id_ejecucion AND id_proceso = @id_proceso AND estado = 1 ";
                eventLog1.WriteEntry("PL: " + text5);
                sqlCommand = new SqlCommand(text5, sqlConnection);
                sqlCommand.Parameters.Add("@estado", SqlDbType.Int);
                sqlCommand.Parameters["@estado"].Value = 3;
                sqlCommand.Parameters.Add("@detalle", SqlDbType.Text);
                sqlCommand.Parameters["@detalle"].Value = "[" + text6 + "]\n" + ex.Message;
                sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
                sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
                sqlCommand.Parameters.Add("@id_proceso", SqlDbType.Int);
                sqlCommand.Parameters["@id_proceso"].Value = proceso.id;
                sqlCommand.ExecuteNonQuery();
                enviar_notificacion_mail(sqlConnection, proceso.notificar_error, "[Proceso Automatico SAI] Error en generacion de archivos " + proceso.nombre_proceso, "Error en generacion de archivos: \r\n " + ex.Message);
                sqlConnection.Close();
            }
            catch (Exception ex2)
            {
                eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR DB (" + text5 + ")" + ex2.Message);
            }
            try
            {
                sqlConnection?.Close();
            }
            catch (Exception ex3)
            {
                eventLog1.WriteEntry("Timer 2 Error ejecutarValidacionArchivo: " + ex3.Message);
            }
        }
    }

    private Hashtable obtenerVariables(SqlConnection myConnection, params string[] values)
    {
        Hashtable hashtable = new Hashtable();
        if (values == null || values.Length == 0)
        {
            return hashtable;
        }
        string text = " SELECT parametro, valor FROM AUT_Configuracion  WHERE parametro IN (  ";
        for (int i = 0; i < values.Length; i++)
        {
            if (i > 0)
            {
                text += ", ";
            }
            text = text + "@p" + i;
        }
        text += " ) ";
        SqlCommand sqlCommand = new SqlCommand(text, myConnection);
        for (int j = 0; j < values.Length; j++)
        {
            sqlCommand.Parameters.Add("@p" + j, SqlDbType.VarChar);
            sqlCommand.Parameters["@p" + j].Value = values[j];
        }
        SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
        while (sqlDataReader.Read())
        {
            hashtable.Add(sqlDataReader["parametro"], sqlDataReader["valor"]);
        }
        sqlDataReader.Close();
        return hashtable;
    }

    private void enviar_notificacion_mail(SqlConnection myConnection, string to, string subject, string message)
    {
        if (to == null || to.Length == 0)
        {
            return;
        }
        try
        {
            string text = "deivid.osorio@ui.colpatria.com";
            string text2 = "relay.ui.colpatria.com";
            int num = 25;
            Hashtable hashtable = obtenerVariables(myConnection, "SMTP_SERVER", "SMTP_PORT", "SMTP_SENDER");
            text = (string)hashtable["SMTP_SENDER"];
            text2 = (string)hashtable["SMTP_SERVER"];
            num = int.Parse((string)hashtable["SMTP_PORT"]);
            MailMessage mailMessage = new MailMessage(text, to);
            SmtpClient smtpClient = new SmtpClient();
            smtpClient.Port = num;
            smtpClient.DeliveryMethod = SmtpDeliveryMethod.Network;
            smtpClient.Host = text2;
            mailMessage.Subject = subject;
            mailMessage.Body = message;
            smtpClient.Send(mailMessage);
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("ERROR Enviando smtp: " + ex.Message, EventLogEntryType.Error);
        }
    }

    private void generarBackupNegocio(SqlConnection myConnection, int id_ejecucion, int anioCierre, int mesCierre, int compania_id)
    {
        try
        {
            string text = "DELETE FROM AUT_BACKUP_Negocio  WHERE anioCierre = @anioCierre AND mesCierre = @mesCierre AND compania_id = @compania_id  ";
            eventLog1.WriteEntry("BACKUP: " + text);
            SqlCommand sqlCommand = new SqlCommand(text, myConnection);
            sqlCommand.CommandTimeout = 0;
            sqlCommand.Parameters.Add("@anioCierre", SqlDbType.Int);
            sqlCommand.Parameters["@anioCierre"].Value = anioCierre;
            sqlCommand.Parameters.Add("@mesCierre", SqlDbType.Int);
            sqlCommand.Parameters["@mesCierre"].Value = mesCierre;
            sqlCommand.Parameters.Add("@compania_id", SqlDbType.Int);
            sqlCommand.Parameters["@compania_id"].Value = compania_id;
            sqlCommand.ExecuteNonQuery();
            text = "INSERT INTO AUT_BACKUP_Negocio  SELECT @id_ejecucion id_ejecucion ,[id] ,[compania_id],[lineaNegocio_id],[ramoDetalle_id],[productoDetalle_id]  ,[planDetalle_id] ,[amparo_id] ,[cobertura_id],[modalidadPago_id],[formaPago_id] ,[numeroNegocio]  ,[numeroSolicitud] ,[codigoAgrupador],[fechaExpedicion],[fechaGrabacion],[fechaRecaudoInicial]  ,[fechaEmisionMaximoEndoso],[fechaCancelacion],[valorAsegurado],[valorProteccion],[valorAhorro]  ,[valorPrimaPensiones],[valorPrimaTotal],[estadoNegocio],[zona_id],[localidad_id],[clave],[participante_id]  ,[porcentajeParticipacion],[segmento_id],[tipoEndoso_id],[grupoEndoso_id],[cuotasPagadas],[cuotasVencidas]  ,[cliente_id],[identificacionSuscriptor],[nombreSuscriptor],[generoSuscriptor],[actividadEconomica_id]  ,[marcaVehiculo_id],[tipoVehiculo_id],[modeloVehiculo],[sistema],[Usuarios],[fechaCarga],[estadoCierre]  ,[mesCierre],[anioCierre],[fechaInicioVigencia],[fechaFinVigencia],[consecutivoEndosoCore],[cuotasTotales]  ,[codigoEmision],[recompra],[numeroRecompra],[fechaContrato],[tipoNovedad],[fechaEfecto]  FROM [SAI].[dbo].[Negocio]  WHERE anioCierre = @anioCierre AND mesCierre = @mesCierre AND compania_id = @compania_id ";
            eventLog1.WriteEntry("BACKUP: " + text);
            eventLog1.WriteEntry("PARAMS: " + id_ejecucion + ", " + anioCierre + ", " + mesCierre + ", " + compania_id);
            sqlCommand = new SqlCommand(text, myConnection);
            sqlCommand.CommandTimeout = 0;
            sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
            sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
            sqlCommand.Parameters.Add("@anioCierre", SqlDbType.Int);
            sqlCommand.Parameters["@anioCierre"].Value = anioCierre;
            sqlCommand.Parameters.Add("@mesCierre", SqlDbType.Int);
            sqlCommand.Parameters["@mesCierre"].Value = mesCierre;
            sqlCommand.Parameters.Add("@compania_id", SqlDbType.Int);
            sqlCommand.Parameters["@compania_id"].Value = compania_id;
            sqlCommand.ExecuteNonQuery();
            eventLog1.WriteEntry("BACKUP TERMINADO ");
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("BACKUP ERROR:" + ex.Message);
        }
    }

    private void generarBackupRecaudo(SqlConnection myConnection, int id_ejecucion, int anioCierre, int mesCierre, int compania_id)
    {
        try
        {
            string text = "DELETE FROM AUT_BACKUP_Recaudo WHERE anioCierre = @anioCierre AND mesCierre = @mesCierre AND compania_id = @compania_id  ";
            eventLog1.WriteEntry("BACKUP: " + text);
            SqlCommand sqlCommand = new SqlCommand(text, myConnection);
            sqlCommand.CommandTimeout = 0;
            sqlCommand.Parameters.Add("@anioCierre", SqlDbType.Int);
            sqlCommand.Parameters["@anioCierre"].Value = anioCierre;
            sqlCommand.Parameters.Add("@mesCierre", SqlDbType.Int);
            sqlCommand.Parameters["@mesCierre"].Value = mesCierre;
            sqlCommand.Parameters.Add("@compania_id", SqlDbType.Int);
            sqlCommand.Parameters["@compania_id"].Value = compania_id;
            sqlCommand.ExecuteNonQuery();
            text = "INSERT INTO AUT_BACKUP_Recaudo  SELECT @id_ejecucion id_ejecucion, [id],[segmento_id],[compania_id],[lineaNegocio_id],[ramoDetalle_id]  ,[productoDetalle_id],[planDetalle_id],[amparo_id],[cobertura_id],[modalidadpago_id],[localidad_id]  ,[zona_id],[formaPago_id],[redDetalle_id],[bancoDetalle_id],[participante_id],[clave],[tipoRecaudo_id]  ,[numeroNegocio],[fechaRecaudo],[fechaGrabacion],[fechaCobranza],[valorRecaudo],[porcentajeParticipacion]  ,[numeroRecibo],[periodoFacturado],[Altura],[Concepto],[porcentajeAhorro_Inversion],[sistema],[codigoOcupacion]  ,[Colquines],[fechaCarga],[estadoCierre],[mesCierre],[anioCierre]  FROM [SAI].[dbo].[Recaudo]  WHERE anioCierre = @anioCierre AND mesCierre = @mesCierre AND compania_id = @compania_id ";
            eventLog1.WriteEntry("BACKUP: " + text);
            eventLog1.WriteEntry("PARAMS: " + id_ejecucion + ", " + anioCierre + ", " + mesCierre + ", " + compania_id);
            sqlCommand = new SqlCommand(text, myConnection);
            sqlCommand.CommandTimeout = 0;
            sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
            sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
            sqlCommand.Parameters.Add("@anioCierre", SqlDbType.Int);
            sqlCommand.Parameters["@anioCierre"].Value = anioCierre;
            sqlCommand.Parameters.Add("@mesCierre", SqlDbType.Int);
            sqlCommand.Parameters["@mesCierre"].Value = mesCierre;
            sqlCommand.Parameters.Add("@compania_id", SqlDbType.Int);
            sqlCommand.Parameters["@compania_id"].Value = compania_id;
            sqlCommand.ExecuteNonQuery();
            eventLog1.WriteEntry("BACKUP TERMINADO ");
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("BACKUP ERROR:" + ex.Message);
        }
    }

    private void generarRestoreNegocio(SqlConnection myConnection, int id_ejecucion, int anioCierre, int mesCierre, int compania_id)
    {
        try
        {
            string text = "DELETE FROM Negocio  WHERE anioCierre = @anioCierre AND mesCierre = @mesCierre AND compania_id = @compania_id  ";
            eventLog1.WriteEntry("RESTORE: " + text);
            SqlCommand sqlCommand = new SqlCommand(text, myConnection);
            sqlCommand.CommandTimeout = 0;
            sqlCommand.Parameters.Add("@anioCierre", SqlDbType.Int);
            sqlCommand.Parameters["@anioCierre"].Value = anioCierre;
            sqlCommand.Parameters.Add("@mesCierre", SqlDbType.Int);
            sqlCommand.Parameters["@mesCierre"].Value = mesCierre;
            sqlCommand.Parameters.Add("@compania_id", SqlDbType.Int);
            sqlCommand.Parameters["@compania_id"].Value = compania_id;
            sqlCommand.ExecuteNonQuery();
            text = "INSERT INTO Negocio ( [compania_id],[lineaNegocio_id],[ramoDetalle_id],[productoDetalle_id]  ,[planDetalle_id] ,[amparo_id] ,[cobertura_id],[modalidadPago_id],[formaPago_id] ,[numeroNegocio]  ,[numeroSolicitud] ,[codigoAgrupador],[fechaExpedicion],[fechaGrabacion],[fechaRecaudoInicial]  ,[fechaEmisionMaximoEndoso],[fechaCancelacion],[valorAsegurado],[valorProteccion],[valorAhorro]  ,[valorPrimaPensiones],[valorPrimaTotal],[estadoNegocio],[zona_id],[localidad_id],[clave],[participante_id]  ,[porcentajeParticipacion],[segmento_id],[tipoEndoso_id],[grupoEndoso_id],[cuotasPagadas],[cuotasVencidas]  ,[cliente_id],[identificacionSuscriptor],[nombreSuscriptor],[generoSuscriptor],[actividadEconomica_id]  ,[marcaVehiculo_id],[tipoVehiculo_id],[modeloVehiculo],[sistema],[Usuarios],[fechaCarga],[estadoCierre]  ,[mesCierre],[anioCierre],[fechaInicioVigencia],[fechaFinVigencia],[consecutivoEndosoCore],[cuotasTotales]  ,[codigoEmision],[recompra],[numeroRecompra],[fechaContrato],[tipoNovedad],[fechaEfecto] )  SELECT [compania_id],[lineaNegocio_id],[ramoDetalle_id],[productoDetalle_id]  ,[planDetalle_id] ,[amparo_id] ,[cobertura_id],[modalidadPago_id],[formaPago_id] ,[numeroNegocio]  ,[numeroSolicitud] ,[codigoAgrupador],[fechaExpedicion],[fechaGrabacion],[fechaRecaudoInicial]  ,[fechaEmisionMaximoEndoso],[fechaCancelacion],[valorAsegurado],[valorProteccion],[valorAhorro]  ,[valorPrimaPensiones],[valorPrimaTotal],[estadoNegocio],[zona_id],[localidad_id],[clave],[participante_id]  ,[porcentajeParticipacion],[segmento_id],[tipoEndoso_id],[grupoEndoso_id],[cuotasPagadas],[cuotasVencidas]  ,[cliente_id],[identificacionSuscriptor],[nombreSuscriptor],[generoSuscriptor],[actividadEconomica_id]  ,[marcaVehiculo_id],[tipoVehiculo_id],[modeloVehiculo],[sistema],[Usuarios],[fechaCarga],[estadoCierre]  ,[mesCierre],[anioCierre],[fechaInicioVigencia],[fechaFinVigencia],[consecutivoEndosoCore],[cuotasTotales]  ,[codigoEmision],[recompra],[numeroRecompra],[fechaContrato],[tipoNovedad],[fechaEfecto]  FROM [SAI].[dbo].[AUT_BACKUP_Negocio]  WHERE anioCierre = @anioCierre AND mesCierre = @mesCierre AND compania_id = @compania_id ";
            eventLog1.WriteEntry("RESTORE: " + text);
            eventLog1.WriteEntry("PARAMS: " + id_ejecucion + ", " + anioCierre + ", " + mesCierre + ", " + compania_id);
            sqlCommand = new SqlCommand(text, myConnection);
            sqlCommand.CommandTimeout = 0;
            sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
            sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
            sqlCommand.Parameters.Add("@anioCierre", SqlDbType.Int);
            sqlCommand.Parameters["@anioCierre"].Value = anioCierre;
            sqlCommand.Parameters.Add("@mesCierre", SqlDbType.Int);
            sqlCommand.Parameters["@mesCierre"].Value = mesCierre;
            sqlCommand.Parameters.Add("@compania_id", SqlDbType.Int);
            sqlCommand.Parameters["@compania_id"].Value = compania_id;
            sqlCommand.ExecuteNonQuery();
            eventLog1.WriteEntry("RESTORE TERMINADO: " + text);
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("RESTORE ERROR:" + ex.Message);
        }
    }

    private void generarRestoreRecaudo(SqlConnection myConnection, int id_ejecucion, int anioCierre, int mesCierre, int compania_id)
    {
        try
        {
            string text = "DELETE FROM Recaudo WHERE anioCierre = @anioCierre AND mesCierre = @mesCierre AND compania_id = @compania_id  ";
            eventLog1.WriteEntry("RECAUDO: " + text);
            SqlCommand sqlCommand = new SqlCommand(text, myConnection);
            sqlCommand.CommandTimeout = 0;
            sqlCommand.Parameters.Add("@anioCierre", SqlDbType.Int);
            sqlCommand.Parameters["@anioCierre"].Value = anioCierre;
            sqlCommand.Parameters.Add("@mesCierre", SqlDbType.Int);
            sqlCommand.Parameters["@mesCierre"].Value = mesCierre;
            sqlCommand.Parameters.Add("@compania_id", SqlDbType.Int);
            sqlCommand.Parameters["@compania_id"].Value = compania_id;
            sqlCommand.ExecuteNonQuery();
            text = "INSERT INTO Recaudo ([segmento_id],[compania_id],[lineaNegocio_id],[ramoDetalle_id]  ,[productoDetalle_id],[planDetalle_id],[amparo_id],[cobertura_id],[modalidadpago_id],[localidad_id]  ,[zona_id],[formaPago_id],[redDetalle_id],[bancoDetalle_id],[participante_id],[clave],[tipoRecaudo_id]  ,[numeroNegocio],[fechaRecaudo],[fechaGrabacion],[fechaCobranza],[valorRecaudo],[porcentajeParticipacion]  ,[numeroRecibo],[periodoFacturado],[Altura],[Concepto],[porcentajeAhorro_Inversion],[sistema],[codigoOcupacion]  ,[Colquines],[fechaCarga],[estadoCierre],[mesCierre],[anioCierre])  SELECT [segmento_id],[compania_id],[lineaNegocio_id],[ramoDetalle_id]  ,[productoDetalle_id],[planDetalle_id],[amparo_id],[cobertura_id],[modalidadpago_id],[localidad_id]  ,[zona_id],[formaPago_id],[redDetalle_id],[bancoDetalle_id],[participante_id],[clave],[tipoRecaudo_id]  ,[numeroNegocio],[fechaRecaudo],[fechaGrabacion],[fechaCobranza],[valorRecaudo],[porcentajeParticipacion]  ,[numeroRecibo],[periodoFacturado],[Altura],[Concepto],[porcentajeAhorro_Inversion],[sistema],[codigoOcupacion]  ,[Colquines],[fechaCarga],[estadoCierre],[mesCierre],[anioCierre]  FROM [SAI].[dbo].[AUT_BACKUP_Recaudo]  WHERE anioCierre = @anioCierre AND mesCierre = @mesCierre AND compania_id = @compania_id ";
            eventLog1.WriteEntry("RESTORE: " + text);
            eventLog1.WriteEntry("PARAMS: " + id_ejecucion + ", " + anioCierre + ", " + mesCierre + ", " + compania_id);
            sqlCommand = new SqlCommand(text, myConnection);
            sqlCommand.CommandTimeout = 0;
            sqlCommand.Parameters.Add("@id_ejecucion", SqlDbType.Int);
            sqlCommand.Parameters["@id_ejecucion"].Value = id_ejecucion;
            sqlCommand.Parameters.Add("@anioCierre", SqlDbType.Int);
            sqlCommand.Parameters["@anioCierre"].Value = anioCierre;
            sqlCommand.Parameters.Add("@mesCierre", SqlDbType.Int);
            sqlCommand.Parameters["@mesCierre"].Value = mesCierre;
            sqlCommand.Parameters.Add("@compania_id", SqlDbType.Int);
            sqlCommand.Parameters["@compania_id"].Value = compania_id;
            sqlCommand.ExecuteNonQuery();
            eventLog1.WriteEntry("RESTORE TERMINADO");
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("RESTORE ERROR:" + ex.Message);
        }
    }

    private long getFtpFileSize(string ftpServer, string ftpUser, string ftpPwd, string ftpDir, string ftpFile)
    {
        long num = -1L;
        try
        {
            string text = ftpServer;
            if (ftpDir.Length > 0 && !ftpDir.Equals("."))
            {
                text = text + "/" + ftpDir;
            }
            text = text + "/" + ftpFile;
            eventLog1.WriteEntry("REVISANDO FTP:" + text);
            FtpWebRequest ftpWebRequest = (FtpWebRequest)WebRequest.Create(new Uri(text));
            ftpWebRequest.Proxy = null;
            ftpWebRequest.Credentials = new NetworkCredential(ftpUser, ftpPwd);
            ftpWebRequest.Method = "SIZE";
            FtpWebResponse ftpWebResponse = (FtpWebResponse)ftpWebRequest.GetResponse();
            num = ftpWebResponse.ContentLength;
            eventLog1.WriteEntry("FTP FILE SIZE:" + num);
            ftpWebResponse.Close();
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("FTP ERROR:" + ex.Message);
        }
        return num;
    }

    private bool checkFTPFileRecords(string ftpServer, string ftpUser, string ftpPwd, string ftpDir, string ftpFile, out int nRecords, out decimal totalValue, int type)
    {
        bool result = false;
        int num = 0;
        decimal num2 = 0m;
        try
        {
            string text = ftpServer;
            if (ftpDir.Length > 0 && !ftpDir.Equals("."))
            {
                text = text + "/" + ftpDir;
            }
            text = text + "/" + ftpFile;
            FtpWebRequest ftpWebRequest = (FtpWebRequest)WebRequest.Create(new Uri(text));
            ftpWebRequest.Proxy = null;
            ftpWebRequest.Credentials = new NetworkCredential(ftpUser, ftpPwd);
            ftpWebRequest.Method = "RETR";
            FtpWebResponse ftpWebResponse = (FtpWebResponse)ftpWebRequest.GetResponse();
            Stream responseStream = ftpWebResponse.GetResponseStream();
            int num3 = 0;
            char c = '\t';
            int num4 = -1;
            if (type == 1)
            {
                num4 = 23;
                c = '|';
            }
            if (type == 2)
            {
                num4 = 12;
                c = '|';
            }
            if (type == 3)
            {
                num4 = 23;
                c = '|';
            }
            if (type == 4)
            {
                num4 = 12;
                c = '|';
            }
            using (StreamReader streamReader = new StreamReader(responseStream))
            {
                while (streamReader.Peek() >= 0)
                {
                    string text2 = streamReader.ReadLine();
                    num3++;
                    if ((num3 == 1 && type == 1) || (num3 == 1 && type == 2))
                    {
                        continue;
                    }
                    string[] array = text2.Split(c);
                    if (num4 >= 0 && array.Length > num4)
                    {
                        try
                        {
                            decimal num5 = decimal.Parse(array[num4], NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, NumberFormatInfo.InvariantInfo);
                            num2 += num5;
                            num++;
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                streamReader.Close();
            }
            ftpWebResponse.Close();
            result = true;
        }
        catch (Exception ex2)
        {
            eventLog1.WriteEntry("FTP ERROR:" + ex2.Message);
        }
        nRecords = num;
        totalValue = num2;
        return result;
    }

    public string getMyIPAddress()
    {
        return Dns.GetHostEntry(Dns.GetHostName()).AddressList.FirstOrDefault((IPAddress ip) => ip.AddressFamily == AddressFamily.InterNetwork).ToString();
    }

    public void restaurarPorErrorEnETL(SqlConnection myConnection, int id_ejecucion, ProcesoObj proceso)
    {
        string text = "";
        try
        {
            if (proceso.requiere_backup == 1 && proceso.tabla.Equals("Recaudo"))
            {
                int anioCierre = 0;
                int mesCierre = 0;
                text = "SELECT compania_id , anioCierre, mesCierre , fechaInicio, fechaFin ,fechaCierre  FROM PeriodoCierre WHERE estado = 1  AND compania_id = " + proceso.compania_id;
                SqlCommand sqlCommand = new SqlCommand(text, myConnection);
                SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
                if (sqlDataReader.Read())
                {
                    anioCierre = (int)sqlDataReader["anioCierre"];
                    mesCierre = (int)sqlDataReader["mesCierre"];
                }
                sqlDataReader.Close();
                generarRestoreRecaudo(myConnection, id_ejecucion, anioCierre, mesCierre, proceso.compania_id);
            }
            if (proceso.requiere_backup == 1 && proceso.tabla.Equals("Negocio"))
            {
                int anioCierre2 = 0;
                int mesCierre2 = 0;
                text = "SELECT compania_id , anioCierre, mesCierre , fechaInicio, fechaFin ,fechaCierre  FROM PeriodoCierre WHERE estado = 1  AND compania_id = " + proceso.compania_id;
                SqlCommand sqlCommand = new SqlCommand(text, myConnection);
                SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
                if (sqlDataReader.Read())
                {
                    anioCierre2 = (int)sqlDataReader["anioCierre"];
                    mesCierre2 = (int)sqlDataReader["mesCierre"];
                }
                sqlDataReader.Close();
                generarRestoreNegocio(myConnection, id_ejecucion, anioCierre2, mesCierre2, proceso.compania_id);
            }
        }
        catch (Exception ex)
        {
            eventLog1.WriteEntry("ProcesoNocturnoSAI ERRROR DB (" + text + ")" + ex.Message);
        }
    }

    private int ejecutaCOMJSN()
    {
        int num = 0;
        OleDbConnection oleDbConnection = null;
        SqlConnection sqlConnection = null;
        try
        {
            string connectionString = ConfigurationManager.AppSettings["as400COMJSNConnectionString"];
            string cmdText = "SELECT COUNT(*) FROM BCPPNMD01.SIGCOMJSN";
            string cmdText2 = "SELECT SNCIA, SNSUC, SECRA, SENNE, SEGMENTO, SNAGE,     DES_JC1, DES_JC2, SESEN     FROM BCPPNMD01.SIGCOMJSN";
            oleDbConnection = new OleDbConnection(connectionString);
            oleDbConnection.Open();
            OleDbCommand oleDbCommand = new OleDbCommand(cmdText, oleDbConnection);
            OleDbDataReader oleDbDataReader = oleDbCommand.ExecuteReader();
            if (oleDbDataReader.Read())
            {
                _ = (int)oleDbDataReader[0];
            }
            oleDbDataReader.Close();
            oleDbCommand = new OleDbCommand(cmdText2, oleDbConnection);
            oleDbDataReader = oleDbCommand.ExecuteReader();
            sqlConnection = obtenerConexion();
            SqlCommand sqlCommand = new SqlCommand("TRUNCATE TABLE SIG.COMJSN", sqlConnection);
            sqlCommand.ExecuteNonQuery();
            SqlTransaction sqlTransaction = sqlConnection.BeginTransaction();
            SqlCommand sqlCommand2 = new SqlCommand();
            sqlCommand2.Connection = sqlConnection;
            sqlCommand2.Transaction = sqlTransaction;
            long num2 = 0L;
            string text = "";
            _ = DateTime.Now;
            while (oleDbDataReader.Read())
            {
                if (num2 % 1000000 == 0)
                {
                    sqlTransaction.Commit();
                    sqlTransaction = (sqlCommand2.Transaction = sqlConnection.BeginTransaction());
                }
                _ = num2 % 100000;
                _ = 0;
                object obj = text;
                text = string.Concat(obj, "INSERT INTO sig.COMJSN (  codCompania , codSucursal , codRamo , numeroNegocio , segmentoNegocio , claveAsesor , nombreDirector , nombreGerente , segmentoNatural , sectorEyE , sectorPyP , sectorCH , microsegmentacion , codigoCIUUSegmento ) VALUES ( '", oleDbDataReader[0], "' , ", oleDbDataReader[1], " , '", oleDbDataReader[2], "' ,'", oleDbDataReader[3], "',", oleDbDataReader[4], ",'", oleDbDataReader[5], "','", oleDbDataReader[6], "','", oleDbDataReader[7], "', ", oleDbDataReader[8], ",0,0,0,'','' ) ; ");
                if (num2 % 200 == 0)
                {
                    sqlCommand2.CommandText = text;
                    sqlCommand2.ExecuteNonQuery();
                    text = "";
                }
                num2++;
            }
            if (text.Length > 0)
            {
                sqlCommand2.CommandText = text;
                sqlCommand2.ExecuteNonQuery();
                text = "";
            }
            sqlTransaction.Commit();
            oleDbDataReader.Close();
            return 1;
        }
        catch (Exception ex)
        {
            num = 0;
            throw ex;
        }
        finally
        {
            sqlConnection?.Close();
            oleDbConnection?.Close();
        }
    }
}
