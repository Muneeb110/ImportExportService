using CustomerBrokerService;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ExcelDataReader;

namespace ImportExportService
{
    public partial class ImportExport : ServiceBase
    {
        static ConcurrentQueue<logger> cq = new ConcurrentQueue<logger>();
        public ImportExport()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            Thread loggingThread = null, ImportThread1 = null, ImportThread2 = null, ImportThread3 = null,
                ExportThread1 = null, ExportThread2 = null, ExportThread3 = null;
            loggingThread = new Thread(() =>
            {
                RunLoggerThread();
            });
            loggingThread.Start();
            try
            {
                #region Export Threads and Configurations
                string storagePathExport_View1, name_View1, IntervalInMinutes_View1, Separator_View1, FilePrefix_View1, FinishStatus_View1, query_View1, ToMailAddresses_View1, Subject_View1, Body_View1,
                    storagePathExport_View2, name_View2, IntervalInMinutes_View2, Separator_View2, FilePrefix_View2, FinishStatus_View2, query_View2, ToMailAddresses_View2, Subject_View2, Body_View2,
                    storagePathExport_View3, name_View3, IntervalInMinutes_View3, Separator_View3, FilePrefix_View3, FinishStatus_View3, query_View3, ToMailAddresses_View3, Subject_View3, Body_View3;
                bool IncludeHeader_View1, IncludeHeader_View2, IncludeHeader_View3, Export_View1, Export_View2, Export_View3, MailFile_View1, MailFile_View2, MailFile_View3;
                #region Export Thread 1
                storagePathExport_View1 = ConfigurationManager.AppSettings.Get("storagePathExport_View1");
                name_View1 = ConfigurationManager.AppSettings.Get("name_View1");
                IntervalInMinutes_View1 = ConfigurationManager.AppSettings.Get("IntervalInMinutes_View1");
                Separator_View1 = ConfigurationManager.AppSettings.Get("Separator_View1");
                FilePrefix_View1 = ConfigurationManager.AppSettings.Get("FilePrefix_View1");
                FinishStatus_View1 = ConfigurationManager.AppSettings.Get("FinishStatus_View1");
                query_View1 = ConfigurationManager.AppSettings.Get("query_View1");
                ToMailAddresses_View1 = ConfigurationManager.AppSettings.Get("ToMailAddresses_View1");
                Subject_View1 = ConfigurationManager.AppSettings.Get("Subject_View1");
                Body_View1 = ConfigurationManager.AppSettings.Get("Body_View1");
                MailFile_View1 = bool.Parse(ConfigurationManager.AppSettings.Get("MailFile_View1"));
                Export_View1 = bool.Parse(ConfigurationManager.AppSettings.Get("Export_View1"));
                IncludeHeader_View1 = bool.Parse(ConfigurationManager.AppSettings.Get("IncludeHeader_View1"));
                if (Export_View1)
                {
                    CreateDirectory(storagePathExport_View1);
                    ExportThread1 = new Thread(() => StartExport(storagePathExport_View1, name_View1, IntervalInMinutes_View1, Separator_View1, FilePrefix_View1, FinishStatus_View1, query_View1, MailFile_View1, ToMailAddresses_View1, Subject_View1, Body_View1, IncludeHeader_View1));
                    ExportThread1.Start();
                }
                #endregion

                #region Export Thread 2
                storagePathExport_View2 = ConfigurationManager.AppSettings.Get("storagePathExport_View2");
                name_View2 = ConfigurationManager.AppSettings.Get("name_View2");
                IntervalInMinutes_View2 = ConfigurationManager.AppSettings.Get("IntervalInMinutes_View2");
                Separator_View2 = ConfigurationManager.AppSettings.Get("Separator_View2");
                FilePrefix_View2 = ConfigurationManager.AppSettings.Get("FilePrefix_View2");
                FinishStatus_View2 = ConfigurationManager.AppSettings.Get("FinishStatus_View2");
                query_View2 = ConfigurationManager.AppSettings.Get("query_View2");
                ToMailAddresses_View2 = ConfigurationManager.AppSettings.Get("ToMailAddresses_View2");
                Subject_View2 = ConfigurationManager.AppSettings.Get("Subject_View2");
                Body_View2 = ConfigurationManager.AppSettings.Get("Body_View2");
                MailFile_View2 = bool.Parse(ConfigurationManager.AppSettings.Get("MailFile_View2"));
                Export_View2 = bool.Parse(ConfigurationManager.AppSettings.Get("Export_View2"));
                IncludeHeader_View2 = bool.Parse(ConfigurationManager.AppSettings.Get("IncludeHeader_View2"));
                if (Export_View2)
                {
                    CreateDirectory(storagePathExport_View2);
                    ExportThread2 = new Thread(() => StartExport(storagePathExport_View2, name_View2, IntervalInMinutes_View2, Separator_View2, FilePrefix_View2, FinishStatus_View2, query_View2, MailFile_View2, ToMailAddresses_View2, Subject_View2, Body_View2, IncludeHeader_View2));
                    ExportThread2.Start();
                }
                #endregion

                #region Export Thread 3
                storagePathExport_View3 = ConfigurationManager.AppSettings.Get("storagePathExport_View3");
                name_View3 = ConfigurationManager.AppSettings.Get("name_View3");
                IntervalInMinutes_View3 = ConfigurationManager.AppSettings.Get("IntervalInMinutes_View3");
                Separator_View3 = ConfigurationManager.AppSettings.Get("Separator_View3");
                FilePrefix_View3 = ConfigurationManager.AppSettings.Get("FilePrefix_View3");
                FinishStatus_View3 = ConfigurationManager.AppSettings.Get("FinishStatus_View3");
                query_View3 = ConfigurationManager.AppSettings.Get("query_View3");
                ToMailAddresses_View3 = ConfigurationManager.AppSettings.Get("ToMailAddresses_View3");
                Subject_View3 = ConfigurationManager.AppSettings.Get("Subject_View3");
                Body_View3 = ConfigurationManager.AppSettings.Get("Body_View3");
                MailFile_View3 = bool.Parse(ConfigurationManager.AppSettings.Get("MailFile_View3"));
                Export_View3 = bool.Parse(ConfigurationManager.AppSettings.Get("Export_View3"));
                IncludeHeader_View3 = bool.Parse(ConfigurationManager.AppSettings.Get("IncludeHeader_View3"));
                if (Export_View3)
                {
                    CreateDirectory(storagePathExport_View3);
                    ExportThread3 = new Thread(() => StartExport(storagePathExport_View3, name_View3, IntervalInMinutes_View3, Separator_View3, FilePrefix_View3, FinishStatus_View3, query_View3, MailFile_View3, ToMailAddresses_View3, Subject_View3, Body_View3, IncludeHeader_View3));
                    ExportThread3.Start();
                }
                #endregion
                #endregion

                #region Import Threads and Configurations
                string FilePath_Import1, Interval_Import1, BackupPath_Import1, SQLStatement_Import1,
                    FilePath_Import2, Interval_Import2, BackupPath_Import2, SQLStatement_Import2,
                    FilePath_Import3, Interval_Import3, BackupPath_Import3, SQLStatement_Import3;
                bool Do_Import1, Do_Import2, Do_Import3, HeadersIncluded_Import1, HeadersIncluded_Import2, HeadersIncluded_Import3;

                #region import thread 1
                FilePath_Import1 = ConfigurationManager.AppSettings.Get("FilePath_Import1");
                Interval_Import1 = ConfigurationManager.AppSettings.Get("Interval_Import1");
                BackupPath_Import1 = ConfigurationManager.AppSettings.Get("BackupPath_Import1");
                SQLStatement_Import1 = ConfigurationManager.AppSettings.Get("SQLStatement_Import1");
                Do_Import1 = bool.Parse(ConfigurationManager.AppSettings.Get("Do_Import1"));
                HeadersIncluded_Import1 = bool.Parse(ConfigurationManager.AppSettings.Get("HeadersIncluded_Import1"));
                if (Do_Import1)
                {
                    CreateDirectory(BackupPath_Import1);
                    CreateDirectory(FilePath_Import1);
                    ImportThread1 = new Thread(() => StartImport(FilePath_Import1, Interval_Import1, BackupPath_Import1, SQLStatement_Import1, HeadersIncluded_Import1) );
                    ImportThread1.Start();
                }
                #endregion

                #region import thread 2
                FilePath_Import2 = ConfigurationManager.AppSettings.Get("FilePath_Import2");
                Interval_Import2 = ConfigurationManager.AppSettings.Get("Interval_Import2");
                BackupPath_Import2 = ConfigurationManager.AppSettings.Get("BackupPath_Import2");
                SQLStatement_Import2 = ConfigurationManager.AppSettings.Get("SQLStatement_Import2");
                Do_Import2 = bool.Parse(ConfigurationManager.AppSettings.Get("Do_Import2"));
                HeadersIncluded_Import2 = bool.Parse(ConfigurationManager.AppSettings.Get("HeadersIncluded_Import2"));
                if (Do_Import2)
                {
                    CreateDirectory(BackupPath_Import2);
                    CreateDirectory(FilePath_Import2);
                    ImportThread2 = new Thread(() => StartImport(FilePath_Import2, Interval_Import2, BackupPath_Import2, SQLStatement_Import2, HeadersIncluded_Import2));
                    ImportThread2.Start();
                }
                #endregion

                #region import thread 3
                FilePath_Import3 = ConfigurationManager.AppSettings.Get("FilePath_Import3");
                Interval_Import3 = ConfigurationManager.AppSettings.Get("Interval_Import3");
                BackupPath_Import3 = ConfigurationManager.AppSettings.Get("BackupPath_Import3");
                SQLStatement_Import3 = ConfigurationManager.AppSettings.Get("SQLStatement_Import3");
                Do_Import3 = bool.Parse(ConfigurationManager.AppSettings.Get("Do_Import3"));
                HeadersIncluded_Import3 = bool.Parse(ConfigurationManager.AppSettings.Get("HeadersIncluded_Import3"));
                if (Do_Import3)
                {
                    CreateDirectory(BackupPath_Import3);
                    CreateDirectory(FilePath_Import3);
                    ImportThread3 = new Thread(() => StartImport(FilePath_Import3, Interval_Import3, BackupPath_Import3, SQLStatement_Import3, HeadersIncluded_Import3));
                    ImportThread3.Start();
                }
                #endregion

                #endregion

            }
            catch (Exception ex)
            {
                log(ex.Message);
                log(ex.StackTrace);
            }
        }

        private void StartImport(string filePath_Import, string interval_Import, string backupPath_Import, string sQLStatement_Import, bool headersIncluded_Import)
        {
            while (true)
            {
                try
                {
                    CreateDirectory(backupPath_Import + "\\" + DateTime.Now.Year + "\\" + DateTime.Now.Month + "\\" + DateTime.Now.Day);
                    log("[Import2csv]: Starting Import from path:" + filePath_Import);
                    string[] files = Directory.GetFiles(filePath_Import);
                    if (files.Count() > 0) //check file exist
                    {
                        foreach (var file in files)
                        {
                            string fileName = Path.GetFileName(file);
                            log("[Import2csv]: file found:" + fileName);
                            string extension = Path.GetExtension(file);
                            DataSet resultTable = null;
                            if (extension == ".csv")
                            {
                                using (var stream = File.Open(file, FileMode.Open, FileAccess.Read))
                                {
                                    using (var reader = ExcelReaderFactory.CreateCsvReader(stream))
                                    {
                                        resultTable = reader.AsDataSet(new ExcelDataSetConfiguration()
                                        {
                                            ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                                            {
                                                UseHeaderRow = headersIncluded_Import
                                            }
                                        });
                                    }
                                }
                                InsertInDatabase(sQLStatement_Import, file, resultTable, headersIncluded_Import);
                                log("[Import2csv]: file processed:" + fileName);
                            }
                            else if (extension == ".xlsx" || extension == ".xls")
                            {
                                using (var stream = File.Open(file, FileMode.Open, FileAccess.Read))
                                {
                                    // Auto-detect format, supports:
                                    //  - Binary Excel files (2.0-2003 format; *.xls)
                                    //  - OpenXml Excel files (2007 format; *.xlsx, *.xlsb)
                                    using (var reader = ExcelReaderFactory.CreateReader(stream))
                                    {
                                        resultTable = reader.AsDataSet(new ExcelDataSetConfiguration()
                                        {
                                            ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                                            {
                                                UseHeaderRow = headersIncluded_Import
                                            }
                                        });
                                    }
                                }
                                InsertInDatabase(sQLStatement_Import, file, resultTable, headersIncluded_Import);
                                
                                log("[Import2csv]: file processed:" + fileName);
                            }
                            //MoveFile(file, backupPath_Import + "\\" + DateTime.Now.Year + "\\" + DateTime.Now.Month + "\\" + DateTime.Now.Day);

                        }
                        Thread.Sleep(1000 * 60 * int.Parse(interval_Import));


                    }
                    else
                    {
                        log("[Import2csv]: No file exist on Path:" + filePath_Import);
                        Thread.Sleep(1000 * 60 * int.Parse(interval_Import));
                    }


                }
                catch (Exception ex)
                {
                    log(ex.Message);
                    log(ex.StackTrace);
                    Thread.Sleep(1000 * 60 * int.Parse(interval_Import));
                }

            }
        }

        private void MoveFile(string file, string backupPath_Import)
        {
            string fileName = Path.GetFileName(file);
            File.Move(file, backupPath_Import + "\\" + fileName);
        }

        private static void InsertInDatabase(string sQLStatement_Import, string file, DataSet resultTable, bool headersIncluded_Import)
        {
            DBManager dBManager = new DBManager();
            log("[Import2csv]: Saving data to temp table for file:" + file);
            dBManager.SaveDataSetInDB(sQLStatement_Import, resultTable, headersIncluded_Import);
            log("[Import2csv]: Calling stored procedure for file:" + file);
            dBManager.CallStoredProcedure(sQLStatement_Import);
            dBManager.InsertInLogTable("Import2DB: " + file + " has been imported into DB");
        }

        private void StartExport(string storagePathExport_View1, string name_View, string intervalInMinutes_View, string separator_View, string filePrefix_View, string finishStatus_View, string query_View, bool MailFile_View, string ToMailAddresses_View, string Subject_View, string Body_View, bool IncludeHeader_View)
        {
            while (true)
            {
                try
                {
                    log("[Export2csv] " + name_View + ": Starting Export.");
                    DBManager dBManager = new DBManager();
                    HashSet<string> references = new HashSet<string>();
                    List<string> csvValueToWrite = dBManager.GetExportData(references, query_View, separator_View, IncludeHeader_View);
                    if (csvValueToWrite.Count > 0)
                    {

                        string data = string.Join(Environment.NewLine, csvValueToWrite);

                        foreach (var reference in references)
                        {
                            log("[Export2csv] " + name_View + ": Updating status to EXP for reference:" + reference);
                            dBManager.UpdateCommericalTable(reference, "EXP");
                        }

                        string filename = filePrefix_View + " " + DateTime.Now.ToString("yyyy-MM-dd HHmmss") + ".csv";
                        using (System.IO.StreamWriter file =
                                           new System.IO.StreamWriter(storagePathExport_View1 + "\\" + filename, false))
                        {
                            file.WriteLine(data);
                        }
                        log("[Export2csv] " + name_View + ": Successfully exported data for file:" + filename);

                        foreach (var reference in references)
                        {
                            log("[Export2csv] " + name_View + ": Updating status to " + finishStatus_View + " for reference:" + reference);
                            dBManager.UpdateCommericalTable(reference, finishStatus_View);
                            log("[Export2csv] " + name_View + ": Updating status in history table for file:" + filename);
                            dBManager.InsertInHistoryTable(reference, "Success", "Record successfully exported to csv:" + filename, name_View);
                        }
                        log("[Export2csv] " + name_View + ": Updating status in log table for file:" + filename);
                        dBManager.InsertInLogTable("Export2DB: " + filename + " has been exported into DB");

                        if (MailFile_View)
                        {
                            var mailMessage = new MailMessage
                            {
                                From = new MailAddress(ConfigurationManager.AppSettings.Get("FromMailAddress")),
                                Subject = Subject_View,
                                Body = Body_View,

                            };
                            try
                            {
                                var smtpClient = new SmtpClient(ConfigurationManager.AppSettings.Get("SMTPServer"));
                                smtpClient.Port = int.Parse(ConfigurationManager.AppSettings.Get("SMTPServerPort"));
                                smtpClient.UseDefaultCredentials = false;
                                smtpClient.Credentials = new NetworkCredential(ConfigurationManager.AppSettings.Get("FromMailAddress"), ConfigurationManager.AppSettings.Get("FromMailAddressPassword"));
                                smtpClient.EnableSsl = true;
                                foreach (var item in ToMailAddresses_View.Split(','))
                                {
                                    mailMessage.To.Add(item);
                                }
                                Attachment attachment = new Attachment(storagePathExport_View1 + "\\" + filename);
                                mailMessage.Attachments.Add(attachment);
                                smtpClient.Send(mailMessage);
                                log("[mailing] " + name_View + ":" + filename + "successfully mailed to:" + ToMailAddresses_View);
                            }
                            catch (Exception ex)
                            {
                                log("[Export2csv] " + name_View + ": Exception in mailing file:" + ex.Message);
                                log("[Export2csv] " + name_View + ": Mail not sent for file:" + filename);
                            }
                            mailMessage.Dispose();
                        }


                        Thread.Sleep(1000);
                    }
                    else
                    {
                        Thread.Sleep(1000 * 60 * int.Parse(intervalInMinutes_View));
                    }


                }
                catch (Exception ex)
                {
                    log(ex.Message);
                    log(ex.StackTrace);
                    Thread.Sleep(1000 * 60 * int.Parse(intervalInMinutes_View));
                }

            }

        }

      

        protected private static void log(string data)
        {
            var logPath = ConfigurationManager.AppSettings.Get("logpath");
            CreateDirectory(logPath);
            cq.Enqueue(new logger(logPath, data));

        }
        private static void CreateDirectory(string path)
        {
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
        }
        void OnStop()
        {
        }
        private static void RunLoggerThread()
        {
            while (true)
            {
                try
                {
                    logger myLogger = null;
                    if (cq.TryDequeue(out myLogger))
                    {
                        using (System.IO.StreamWriter file =
                                       new System.IO.StreamWriter(myLogger.path + "\\AEBCustomsAPIImportExportLog" + DateTime.Now.ToString("yyyyMMdd") + ".txt", true))
                        {
                            file.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + myLogger.data);
                        }
                    }
                    else
                    {
                        Thread.Sleep(1000);
                    }
                }
                catch (Exception ex)
                {
                    Thread.Sleep(1000);
                    using (System.IO.StreamWriter file =
                                     new System.IO.StreamWriter(ConfigurationManager.AppSettings.Get("logpath") + "\\AEBCustomsAPIImportExportLog" + DateTime.Now.ToString("yyyyMMdd") + ".txt", true))
                    {
                        file.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")  + ex.Message);
                    }

                }
            }
        }

        internal void Start()
        {
            this.OnStart(null);
        }
    }
}
