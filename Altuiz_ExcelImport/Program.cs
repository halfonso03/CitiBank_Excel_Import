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
        private static string connectionString;

        static void Main(string[] args)
        {

            GetConnectionString();

            try
            {
                WriteToLog("", "", "Starting process");

                ProcessFiles(GetFiles());
            }
            catch (Exception ex)
            {
                WriteToLog("", "", "Error processing files:" + ex.Message);
                Console.WriteLine(ex.Message);
            }
            

            Console.WriteLine("Process Complete");
            Console.Read();
        }

        private static ImportFile[] GetFiles()
        {
            List<ImportFile> files = new List<ImportFile>();

            using (var cn = new SqlConnection(connectionString))
            {
                var sql = @"SELECT DISTINCT file_path, table_name, file_action 
                            FROM ExcelFilePaths p JOIN ExcelFileToTableMap m ON p.table_id = m.table_id
                            WHERE process = 1 ";

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
                            Action = reader.GetString(2)
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
                    if (VerifyColumNamesMatch(file))
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

        private static bool FileColumnNamesMatchTableConfig(ImportFile file)
        {
            var factory = new ExcelQueryFactory(file.FilePath);

            if (Path.GetExtension(file.FilePath) == ".xls")
                factory.DatabaseEngine = DatabaseEngine.Jet;
            else
                factory.DatabaseEngine = DatabaseEngine.Ace;

            var columnNamesFromFile = factory.GetColumnNames(factory.GetWorksheetNames().First());


            var columnsFromFile = columnNamesFromFile.Take(columnNamesFromFile.Count() - 2).ToList();

            var columsFromConfig = GetTableColumnNames(file.TableName).ToList();


            return false;
        }

        private static bool ProcessFile(ImportFile file)
        {

            var sql = "";
            var deleteSql = "";
            var factory = new ExcelQueryFactory(file.FilePath);
            var tableName = Path.GetFileNameWithoutExtension(file.FilePath);
            var columnNamesFromMapping = GetTableColumnNames(file.TableName);
            var columnCount = columnNamesFromMapping.Length;
            var rowsDeleted = 0;

            DateTime fileUpdateDate;

            try
            {
                var d = factory.WorksheetNoHeader(1).Select(x => x).First()[0].Value.ToString();
            }
            catch (Exception ex)
            {
                WriteToLog("", "No records uploaded", "Missing second tab");
                return false;
            }

            var d2 = factory.WorksheetNoHeader(1).Select(x => x).First()[0].Value.ToString();

            if (d2.Trim().Length == 0)
            {
                WriteToLog("", "No records uploaded", "Missing date on second tab");
                return false;
            }

            if (!DateTime.TryParse(d2, out fileUpdateDate))
            {
                WriteToLog("", "No records uploaded", $"Wrong date format second tab ({d2})");
                return false;
            }
            
            
            ExcelQueryable<LinqToExcel.Row> excelData = factory.Worksheet(0);

            List<NumericColumn> numberColumns = GetNumericColumns(tableName);


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
                        if (numberColumns
                            .FirstOrDefault(c => c.ColumnIndex == (colIndex + 1)) != null)
                        {
                            insertValues += $"{row[colIndex]},";
                        }
                        else
                        {
                            insertValues += $"'{row[colIndex].ToString().Replace("'", "''")}',";
                        }

                    }

                    insertValues = insertValues.Substring(0, insertValues.Length - 1) + ", '" + fileUpdateDate.ToString("MM/dd/yy") + "', GETDATE());";


                    recordsInserted++;

                    if (recordsInserted > 300)
                        break;
                }

                
                //insertValues += "); ";

                sql = $"{insertValues};";





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

        private static List<NumericColumn> GetNumericColumns(string tableName)
        {
           List<NumericColumn> results = new List<NumericColumn>();

           var sql = $@"SELECT c.column_id, system_type_id
                    FROM sys.all_columns c join sys.tables t ON
	                    t.object_id = c.object_id 
                    WHERE t.name = '{tableName}' AND system_type_id IN (56, 108) ";

            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();
                    var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        results.Add(new NumericColumn
                        {
                            ColumnIndex = reader.GetInt32(0),
                            SqlDataType = (SqlDataType)reader.GetByte(1)
                        });
                    }
                }

            }

            return results;
        }

        private static bool VerifyColumNamesMatch(ImportFile file)
        {
            var factory = new ExcelQueryFactory(file.FilePath);

            if (Path.GetExtension(file.FilePath) == ".xls")
                factory.DatabaseEngine = DatabaseEngine.Jet;
            else
                factory.DatabaseEngine = DatabaseEngine.Ace;

            var columnsFromFile = factory.GetColumnNames(factory.GetWorksheetNames().First());
            var columnsFromTableForExcel = GetTableColumnNames(
                tableName: file.TableName);

            var columnNamesFromFile = columnsFromFile.Take(columnsFromFile.Count() - 2).ToList();


            if (VerifyUserColumnConfigMatchesSchema(file.TableName))
            {
                foreach (var colName in columnsFromTableForExcel)
                {
                    if (!columnNamesFromFile.Contains(colName))
                    {
                        WriteToLog("", "No records uploaded", "XLS fields do not match table config");
                        return false;                        
                    }
                }
                
                
                foreach (var colName in columnNamesFromFile)
                {
                    if (!columnsFromTableForExcel.Contains(colName))
                    {
                        WriteToLog("", "No records uploaded", "Table config fields do not match XLS file");
                        return false;                        
                    }
                }

                
            }
            else
            {
                WriteToLog("", "No records uploaded", "Mismatch between table definition and table configuration");                
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

        private static string[] GetTableColumnNames(string tableName)
        {
            var names = new List<string>();
            using (var cn = new SqlConnection(connectionString))
            {
                var sql = $@"SELECT column_name FROM ExcelFileToTableMap WHERE table_id = (
                                SELECT table_id FROM ExcelFilePaths WHERE table_name = '{tableName}')";

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
