using Microsoft.SqlServer.Server;
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CustomerBrokerService
{
    class DBManager
    {
        public List<string> GetExportData(HashSet<string> OrderReference, string query_View, string separator_View, bool includeHeader_View)
        {
            List<string> values = new List<string>();
            try
            {
                var con = ConfigurationManager.AppSettings["dbConnectionString"].ToString();


                using (SqlConnection myConnection = new SqlConnection(con))
                {
                    string oString = query_View;
                    SqlCommand oCmd = new SqlCommand(oString, myConnection);
                    oCmd.CommandTimeout = 120;
                    myConnection.Open();
                    using (SqlDataReader oReader = oCmd.ExecuteReader())
                    {
                        bool getColumnValues = false;
                        string columns = "";

                        while (oReader.Read())
                        {
                            string rowFields = "";
                            for (int i = 0; i < oReader.FieldCount; i++)
                            {
                                if (!getColumnValues)
                                {
                                    columns += oReader.GetName(i) + separator_View;
                                }
                                rowFields += oReader[i] + separator_View;
                                if (oReader.GetName(i) == "localReference")
                                    OrderReference.Add(oReader[i].ToString());
                            }
                            if (!getColumnValues)
                            {
                                if(includeHeader_View)
                                    values.Add(columns.Trim(separator_View.ToCharArray()));
                                getColumnValues = true;
                            }
                            values.Add(rowFields.Trim(separator_View.ToCharArray()));
                        }

                        myConnection.Close();
                    }
                }
                return values;
            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }

        public void UpdateCommericalTable(string localReference, string status)
        {
            try
            {
                var con = ConfigurationManager.AppSettings["dbConnectionString"].ToString();
                using (SqlConnection myConnection = new SqlConnection(con))
                {
                    SqlCommand SqlComm = new SqlCommand("update commercial set status = @status where localReference = @localReference", myConnection);
                    SqlComm.Parameters.AddWithValue("@localReference", localReference);
                    SqlComm.Parameters.AddWithValue("@status", status);
                    SqlComm.CommandTimeout = 120;
                    myConnection.Open();
                    int i = SqlComm.ExecuteNonQuery();

                    myConnection.Close();
                }

            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }

        public void InsertInHistoryTable(string localReference, string status, string message, string table)
        {
            try
            {
                var con = ConfigurationManager.AppSettings["dbConnectionString"].ToString();
                using (SqlConnection myConnection = new SqlConnection(con))
                {
                    SqlCommand SqlComm = new SqlCommand("insert into dbo.History (dateTime, [key], [table], status, message)values(CURRENT_TIMESTAMP, @key,@table,@status,@message)", myConnection);
                    SqlComm.Parameters.AddWithValue("@key", localReference);
                    SqlComm.Parameters.AddWithValue("@table", table);
                    SqlComm.Parameters.AddWithValue("@status", status);
                    SqlComm.Parameters.AddWithValue("@message", message);
                    SqlComm.CommandTimeout = 120;
                    myConnection.Open();
                    SqlComm.ExecuteNonQuery();

                    myConnection.Close();
                }

            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }

        public void InsertInLogTable(string fileName)
        {
            try
            {
                var con = ConfigurationManager.AppSettings["dbConnectionString"].ToString();
                using (SqlConnection myConnection = new SqlConnection(con))
                {
                    SqlCommand SqlComm = new SqlCommand("insert into dbo.log (dateTime, [key], value) values (CURRENT_TIMESTAMP, 'AEBCustomsAPI', @fileName)", myConnection);
                    SqlComm.Parameters.AddWithValue("@fileName", fileName);
                    SqlComm.CommandTimeout = 120;
                    myConnection.Open();
                    SqlComm.ExecuteNonQuery();

                    myConnection.Close();
                }

            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }

        public void SaveDataSetInDB(string sQLStatement_Import, DataSet resultTable, bool headersIncluded_Import)
        {
            try
            {
                if (headersIncluded_Import)
                {
                    string[] sqlStatementParsing = sQLStatement_Import.Split('(', ')');
                    string columnsInQuery = sQLStatement_Import.Split('(', ')')[1];
                    string[] columns = columnsInQuery.Split(',');
                    string rowsInQuery = sQLStatement_Import.Split('(', ')')[3];
                    string[] rows = rowsInQuery.Split(',');
                    string tableName = sqlStatementParsing[0].Split(' ')[2];
                    int index = 0;



                    var con = ConfigurationManager.AppSettings["dbConnectionString"].ToString();
                    using (SqlConnection myConnection = new SqlConnection(con))
                    {
                        SqlBulkCopy bulk = new SqlBulkCopy(con);
                        bulk.DestinationTableName = tableName;


                        foreach (string column in columns)
                        {
                            bulk.ColumnMappings.Add(rows[index], column);
                            index++;
                        }

                        myConnection.Open();
                        bulk.WriteToServer(resultTable.Tables[0]);
                        myConnection.Close();
                    }
                }
                else
                {
                    string[] sqlStatementParsing = sQLStatement_Import.Split('(', ')');
                    string columnsInQuery = sQLStatement_Import.Split('(', ')')[1];
                    string[] columns = columnsInQuery.Split(',');
                    
                    string tableName = sqlStatementParsing[0].Split(' ')[2];
                    int index = 0;



                    var con = ConfigurationManager.AppSettings["dbConnectionString"].ToString();
                    using (SqlConnection myConnection = new SqlConnection(con))
                    {
                        SqlBulkCopy bulk = new SqlBulkCopy(con);
                        bulk.DestinationTableName = tableName;


                        foreach (string column in columns)
                        {
                            bulk.ColumnMappings.Add(resultTable.Tables[0].Columns[index].ColumnName, column);
                            index++;
                        }

                        myConnection.Open();
                        bulk.WriteToServer(resultTable.Tables[0]);
                        myConnection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }

        public string GetDataType(string tableName, string column)
        {
            string dataType = "";
            try
            {
                var con = ConfigurationManager.AppSettings["dbConnectionString"].ToString();


                using (SqlConnection myConnection = new SqlConnection(con))
                {

                    SqlCommand oCmd = new SqlCommand("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = '" + tableName + "' AND  COLUMN_NAME = '" + column + "'", myConnection);

                    oCmd.CommandTimeout = 120;
                    myConnection.Open();
                    using (SqlDataReader oReader = oCmd.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            dataType = oReader[0].ToString();
                        }

                    }
                    myConnection.Close();
                }
                return dataType;
            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }

        public void CallStoredProcedure(string sQLStatement_Import)
        {
            try
            {
                string[] sqlStatementParsing = sQLStatement_Import.Split('(', ')');
                string from_tableName = sqlStatementParsing[0].Split(' ')[2];
                string[] arr = from_tableName.Split('_');
                string toTableName = "";
                for (int i = 0; i < arr.Length - 1; i++)
                {
                    toTableName += arr[i] + "_";
                }
                toTableName = toTableName.Trim('_');
                var con = ConfigurationManager.AppSettings["dbConnectionString"].ToString();


                using (SqlConnection myConnection = new SqlConnection(con))
                {

                    using (var command = new SqlCommand("InsertFromTempTableToDbTable", myConnection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.Add("@FromTable", SqlDbType.VarChar).Value = from_tableName;
                        command.Parameters.Add("@ToTable", SqlDbType.VarChar).Value = toTableName;
                        command.CommandTimeout = 120;
                        myConnection.Open();
                        command.ExecuteNonQuery();
                    }


                    myConnection.Close();
                }
            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }
    }


}
