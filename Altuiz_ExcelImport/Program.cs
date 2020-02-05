using Altuiz_ExcelImport.Infrastructure;
using Altuiz_ExcelImport.Models;
using CitiBank_Excel_Import.Models;
using LinqToExcel;
using LinqToExcel.Domain;
using LinqToExcel.Query;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Altuiz_ExcelImport
{
    class Program
    {
        public static string connectionString;
        public static List<NumericColumn> integerColumns;
        public static List<NumericColumn> floatColumns;
        public static List<DateColumn> dateColumns;



        static void Main(string[] args)
        {

            GetConnectionString();

            try
            {
                Helpers.ConnectToDb();

                try
                {
                    WriteToLog("", "", "Starting process");

                    var files = GetFiles();

                    ProcessFiles(files);
                }
                catch (Exception ex)
                {
                    WriteToLog("", "", "Error processing files:" + ex.Message);
                    Console.WriteLine(ex.Message);
                }

            }
            catch (Exception ex)
            {
                var date = DateTime.Now.ToString("yyyy-MM-dd-hh-mm-ss");
                File.WriteAllText($"Log_{date}.txt", ex.Message);
            }
            
         

            Console.WriteLine("Process Complete");
        }

   

        private static ImportFile[] GetFiles()
        {
            var files = new List<ImportFile>();

            using (var cn = new SqlConnection(connectionString))
            {
                var sql = @"SELECT DISTINCT file_path, table_name, file_action, m.table_id
                            FROM ExcelFilePaths p LEFT OUTER JOIN ExcelFileToTableMap m ON p.table_id = m.table_id
                            WHERE process = 1 ORDER BY table_id DESC";

                var cmd = new SqlCommand(sql, cn);
                {
                    cn.Open();

                    var reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        files.Add(new ImportFile
                        {
                            FilePath = reader.GetString(0),
                            TableName = reader.GetString(1),
                            Action = reader.GetString(2),
                            ConfigurationNotSet = reader.IsDBNull(3)
                        });
                    }

                    cn.Close();
                }
            }

            return files.ToArray();
        }


        private static void ProcessFiles(ImportFile[] files)
        {

            foreach (var file in files)
            {
                var tableExists = TableExists(file.TableName);
                var fileExists = File.Exists(file.FilePath);


                if (!fileExists)
                {
                    WriteToLog(file.TableName, "No records uploaded", $"File {file.FilePath} does not exist.");
                    continue;
                }

                if (!tableExists)
                {
                    WriteToLog(file.TableName, "No records uploaded", $"Not able to add records to SQL table. Import table {file.TableName} does not exist.");
                    continue;
                }
                

                try
                {
                    if (VerifyColumns(file))
                    {
                        Console.WriteLine("Processing " + file.FilePath + "...");

                        if (ProcessFile(file))
                        {
                            Console.WriteLine("Finished processing " + file.FilePath + ".");
                        }
                        else
                        {
                            Console.WriteLine("Could not process " + file.FilePath + ". See log for details.");
                        }                        
                    }
                }
                catch (Exception ex)
                {
                    var newEx = new Exception("Application error when processing file " + file.FilePath + ": " + ex.Message + ".");

                    throw newEx;
                }
            }
            

        }

   

        private static bool ProcessFile(ImportFile file)
        {
            var sql = "";
            var deleteSql = "";
            var factory = new ExcelQueryFactory(file.FilePath);
            var tableName = file.TableName;
            var columnCount = GetColumnNamesForTable(file.TableName, file.FilePath).Length;
            var rowsDeleted = 0;
            
            DateTime fileUpdateDate;

            integerColumns = Helpers.GetIntegerColumns(file.TableName);
            floatColumns = Helpers.GetFloatColumns(file.TableName);
            dateColumns = Helpers.GetDateColumns(file.TableName);

           
            try
            {
                var d = factory.WorksheetNoHeader(1).Select(x => x).First()[0].Value.ToString();
            }
            catch (Exception)
            {
                WriteToLog("", "No records uploaded", $"Missing second tab for table {file.TableName}");
                return false;
            }

            var d2 = factory.WorksheetNoHeader(1).Select(x => x).First()[0].Value.ToString();

            if (d2.Trim().Length == 0)
            {
                WriteToLog("", "No records uploaded", $"Missing date on second tab for table {file.TableName}");
                return false;
            }

            if (!DateTime.TryParse(d2, out fileUpdateDate))
            {
                WriteToLog("", "No records uploaded", $"Wrong date format in second tab ({d2}) for table {file.TableName}");
                return false;
            }
            
            
            ExcelQueryable<LinqToExcel.Row> excelData = factory.Worksheet(0);

            


            if (excelData.Count() > 0)
            {
                var recordsInserted = 0;
                var insertValues = "";
                

                foreach (var row in excelData)
                {
                    insertValues += $"INSERT INTO {tableName} VALUES (";

                    for (var colIndex = 0; colIndex <= columnCount - 1; colIndex++)
                    {
                        // check if column is numeric to not include single quotes
                        if (Helpers.IsFloatColumn(colIndex))
                        {
                            decimal attempt = 0;

                            var num = Program.floatColumns.FirstOrDefault(c => c.ColumnIndex == colIndex);

                            if (!num.IsNullable)
                            {
                                if (!Decimal.TryParse(row[colIndex], out attempt))
                                {
                                    WriteToLog(tableName, "", $"Invalid decimal format value in row {recordsInserted + 2} column {colIndex + 1}");
                                    return false;
                                }
                                else
                                {
                                    insertValues += $"{row[colIndex]},";
                                }
                            }
                            else
                            {
                                if (String.IsNullOrEmpty(row[colIndex]))
                                {
                                    insertValues += $"null,";
                                }
                                else
                                {
                                    insertValues += $"{row[colIndex]},";
                                }
                            }
                            
                        }
                        else if (Helpers.IsIntegerColumn(colIndex))
                        {
                            long attempt = 0;

                            var num = Program.integerColumns.FirstOrDefault(c => c.ColumnIndex == colIndex);

                            if (!num.IsNullable)
                            {
                                if (!Int64.TryParse(row[colIndex], out attempt))
                                {
                                    WriteToLog(tableName, "", $"Invalid integer format value in row {recordsInserted + 2} column {colIndex + 1}");
                                    return false;                                   
                                }
                                else
                                {
                                    insertValues += $"{row[colIndex]},";
                                }
                            }
                            else
                            {
                                if (String.IsNullOrEmpty(row[colIndex]))
                                {
                                    insertValues += $"null,";
                                }
                                else
                                {
                                    insertValues += $"{row[colIndex]},";
                                }
                            }
                                                                                
                        }
                        else if (Helpers.IsDateColumn(colIndex))
                        {
                            DateTime attempD = new DateTime();

                            if (!DateTime.TryParse(row[colIndex], out attempD))
                            {
                                WriteToLog(tableName, "", $"Invalid date format value in row {recordsInserted + 2} column {colIndex + 1}");
                                return false;
                                //throw new InvalidCastException($"Invalid date format value in row {recordsInserted} column {colIndex}");
                            }
                            else
                            {
                                insertValues += $"'{row[colIndex]}',";
                            }
                        }
                        else
                        {
                            insertValues += $"'{row[colIndex].ToString().Replace("'", "''")}',";
                        }
                    }

                    insertValues = insertValues.Substring(0, insertValues.Length - 1) + ",'" + fileUpdateDate.ToString("MM/dd/yy") + "',  GETDATE());";


                    recordsInserted++;

                    //if (recordsInserted > 300)
                    //    break;
                }

                
                //insertValues += "); ";

                sql = $"{insertValues};";




                // database updates
                using (var cn = new SqlConnection(connectionString))
                {
                    using (var cmd = new SqlCommand(sql, cn))
                    {
                        cn.Open();

                        var trans = cn.BeginTransaction();

                        cmd.Transaction = trans;

                        try
                        {
                            SqlDataReader reader = default(SqlDataReader);

                            if (file.Action == "I")
                            {
                                deleteSql = $"DELETE {tableName} WHERE dttolap = '{fileUpdateDate.ToString("MM/dd/yy")}'; SELECT @@ROWCOUNT";

                                WriteToLog(tableName, "DELETE FROM TABLE", "");

                                cmd.CommandText = deleteSql;

                                reader = cmd.ExecuteReader();
                                reader.ReadAsync();
                                rowsDeleted = reader.GetInt32(0);
                            }
                                                                                                   
                            cmd.CommandText = sql;

                            if (reader != null && !reader.IsClosed)
                            {
                                reader.Close();
                            }

                            reader = cmd.ExecuteReader();
                            reader.Close();

                            WriteToLog(file.TableName, "INSERT", $"INSERTED {recordsInserted} record(s) into {tableName}", cmd);

                            if (rowsDeleted > 0)
                            {
                                WriteToLog(file.TableName, "DELETE", $"DELETED {rowsDeleted} record(s) from {tableName}", cmd);
                            }
                            
                            trans.Commit();
                        }
                        catch (Exception ex)
                        {
                            trans.Rollback();

                            throw ex;
                        }
                    }
                }
            }

            return true;
            
        }


    
        private static bool VerifyColumns(ImportFile file)
        {
            var factory = new ExcelQueryFactory(file.FilePath);

            if (Path.GetExtension(file.FilePath) == ".xls")
                factory.DatabaseEngine = DatabaseEngine.Jet;
            else
                factory.DatabaseEngine = DatabaseEngine.Ace;

            var columnsFromFile = factory.GetColumnNames(factory.GetWorksheetNames().First());
            var columnsFromTableForExcel = GetColumnNamesForTable(file.TableName, file.FilePath);

            var columnNamesFromFile = columnsFromFile.ToList();


            if (VerifyUserColumnConfigMatchesSchema(file.TableName))
            {
                foreach (var colName in columnsFromTableForExcel)
                {
                    if (!columnNamesFromFile.Contains(colName))
                    {
                        WriteToLog("", "No records uploaded", $"XLS fields do not match table config for table {file.TableName}");
                        return false;                        
                    }
                }
                
                
                foreach (var colName in columnNamesFromFile)
                {
                    if (!columnsFromTableForExcel.Contains(colName))
                    {
                        WriteToLog("", "No records uploaded", $"Table config fields do not match XLS file for table {file.TableName}");
                        return false;                        
                    }
                }

                
            }
            else
            {
                WriteToLog("", "No records uploaded", $"Mismatch between table definition and table configuration for table {file.TableName}");                
                return false;
            }
            return true;
        }


      
        private static bool VerifyUserColumnConfigMatchesSchema(string tableName)
        {
            var sql = $@"SELECT*
                            FROM(
                            SELECT column_name
                            FROM ExcelFileToTableMap m JOIN ExcelFilePaths p ON m.table_id = p.table_id
                            WHERE table_name = '{tableName}'
                            ) u FULL OUTER JOIN
                            (SELECT c.name
                                FROM sys.tables t join sys.all_columns c on t.object_id = c.object_id
                                WHERE object_name(t.object_id) =  '{tableName}'
                            AND c.name NOT IN ('dttolap', 'DATETIME_UPDATE')) s ON u.column_name = s.name
                            WHERE s.name IS NULL OR u.column_name IS NULL";

            var r = RunSqlRecordsExist(sql);

            return !r;
        }

        private static string[] GetColumnNamesForTable(string tableName, string filePath)
        {
            var names = new List<string>();
            using (var cn = new SqlConnection(connectionString))
            {
                var sql = $@"SELECT column_name FROM ExcelFileToTableMap WHERE table_id = (
                                SELECT table_id 
                                FROM ExcelFilePaths 
                                WHERE table_name = '{tableName}' AND file_path = '{filePath}')";

                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();

                    var reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        names.Add(reader.GetString(0));
                    }

                    cn.Close();
                }
            }

            return names.ToArray();
        }

        private static bool TableExists(string tableName)
        {
            bool exists = false;

            using (var cn = new SqlConnection(connectionString))
            {
                var sql = $"SELECT * FROM sys.tables WHERE name ='{tableName}'";

                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();

                    var reader = cmd.ExecuteReader();

                    exists = reader.HasRows;

                    cn.Close();
                }
            }

            return exists;
        }

        private static void GetConnectionString()
        {
            connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString.ToString();
        }

        private static void RunSql(string sql)
        {
            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();

                    var trans = cn.BeginTransaction();

                    try
                    {
                        cmd.Transaction = trans;

                        cmd.ExecuteNonQuery();

                        trans.Commit();
                    }
                    catch (Exception ex)
                    {
                        trans.Rollback();

                        throw ex;
                    }
                }
            }
        }

        private static bool RunSqlRecordsExist(string sql)
        {
            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();
                    var reader = cmd.ExecuteReader(System.Data.CommandBehavior.CloseConnection);
                    return reader.HasRows;
                }
            }
        }


        private static void WriteToLog(string table, string action, string message)
        {
            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand())
                {
                    cmd.Connection = cn;

                    cn.Open();

                    cmd.Parameters.Clear();
                    cmd.CommandText = "dbo.DWUS_SP_UPDATE_EVENT_LOG";
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("USER_ID_PARAM", "EVG");
                    cmd.Parameters.AddWithValue("DWH_ACTION_PARAM", action);
                    cmd.Parameters.AddWithValue("DWH_TABLE_RELATED_PARAM", table);
                    cmd.Parameters.AddWithValue("DWH_DETAIL_EVENT", message);
                    cmd.ExecuteNonQuery();
                }
            }

        }

        private static void WriteToLog(string table, string action, string message, SqlCommand cmd)
        {
            cmd.Parameters.Clear();
            cmd.CommandText = "dbo.DWUS_SP_UPDATE_EVENT_LOG";
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("USER_ID_PARAM", "EVG");
            cmd.Parameters.AddWithValue("DWH_ACTION_PARAM", action);
            cmd.Parameters.AddWithValue("DWH_TABLE_RELATED_PARAM", table);
            cmd.Parameters.AddWithValue("DWH_DETAIL_EVENT", message);
            cmd.ExecuteNonQuery();
        }
    }
}
