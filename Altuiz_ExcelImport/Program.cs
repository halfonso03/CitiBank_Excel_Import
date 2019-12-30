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

            ReadConfigfile();

            ProcessFiles(GetFiles());

            Console.WriteLine("Process Complete");
            Console.Read();
        }

        private static ImportFile[] GetFiles()
        {
            List<ImportFile> files = new List<ImportFile>();

            using (var cn = new SqlConnection(connectionString))
            {
                var sql = @"SELECT DISTINCT file_path, table_name, file_action 
                            FROM ExcelFilePaths p JOIN ExcelFileToTableMap m ON p.table_id = m.table_id";

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
                var exists = TableExists(file.TableName);

                if (exists)
                {
                    try
                    {
                        if (VerifyColumNamesMatch(file))
                        {
                            ProcessFile(file);
                        }
                        else
                        {
                            // log excel columns and data from columns table not matching
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }

                }
                else
                {
                    // log table does not exist
                }
            }

        }

        private static void ProcessFile(ImportFile file)
        {
            var sql = "";
            var factory = new ExcelQueryFactory(file.FilePath);
            var tableName = Path.GetFileNameWithoutExtension(file.FilePath);
            var columnNamesFromMapping = GetTableColumnNames(file.TableName);
            var columnCount = columnNamesFromMapping.Length;
            var rowsDeleted = 0;
            SqlDataReader reader = default(SqlDataReader);
            var fileUpdateDate = (DateTime)factory.WorksheetNoHeader(1).Select(x => x).First()[0].Value;


            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();

                    var trans = cn.BeginTransaction();

                    try
                    {
                        cmd.Transaction = trans;

                        if (file.Action == "I")
                        {
                            sql = $"DELETE {tableName} WHERE dttolap = '{fileUpdateDate.ToString("MM/dd/yy")}'; SELECT @@ROWCOUNT";

                            cmd.CommandText = sql;

                            reader = cmd.ExecuteReader();
                            reader.ReadAsync();
                            rowsDeleted = reader.GetInt32(0);
                        }

                        var recordsInserted = 0;
                        var insertValues = "";
                        var insertHeader = $"INSERT INTO {tableName} ";
                        // insertHeader += $"({String.Join(",", columnNamesFromMapping)}, DATETIME_UPDATE, dttolap) VALUES ";

                        ExcelQueryable<LinqToExcel.Row> data = factory.Worksheet(0);

                        foreach (var row in data)
                        {
                            insertValues += insertHeader + " VALUES (";

                            for (var x = 0; x <= columnCount - 1; x++)
                            {
                                insertValues += $"'{row[x].ToString().Replace("'", "''")}',";
                            }
                            insertValues = insertValues.Substring(0, insertValues.Length - 1) + ", GETDATE(), '" + fileUpdateDate.ToString("MM/dd/yy") + "');";

                            
                            recordsInserted++;

                            //if (recordsInserted > 300)
                            //    break;
                        }

                        insertValues = insertValues.Substring(0, insertValues.Length - 1);


                        sql = $"{insertValues}; SELECT @@ROWCOUNT";
                        // sql = $"{insertHeader} {insertValues}; SELECT @@ROWCOUNT";

                        cmd.CommandText = sql;

                        if (reader != null && !reader.IsClosed) reader.Close();


                        reader = cmd.ExecuteReader();
                        reader.ReadAsync();

                        // var recordsInserted = reader.GetInt32(0);

                        reader.Close();



                        WriteToLog(file.TableName, "INSERT", $"INSERTED {recordsInserted} record(s)", cmd);

                        if (rowsDeleted > 0)
                        {
                            WriteToLog(file.TableName, "DELETE", $"DELETED {rowsDeleted} record(s)", cmd);
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



        private static bool VerifyColumNamesMatch(ImportFile file)
        {
            var factory = new ExcelQueryFactory(file.FilePath);

            if (Path.GetExtension(file.FilePath) == ".xls")
                factory.DatabaseEngine = DatabaseEngine.Jet;
            else
                factory.DatabaseEngine = DatabaseEngine.Ace;

            var columnNamesFromFile = factory.GetColumnNames(factory.GetWorksheetNames().First());
            var columnsFromTableForExcel = GetTableColumnNames(
                tableName: file.TableName);


            if (VerifyUserColumnEntryMatchesSchema(file.TableName))
            {
                foreach (var colName in columnNamesFromFile)
                {
                    if (!columnsFromTableForExcel.Contains(colName))
                    {
                        throw new Exception("Excel file column does not exist in table definition");
                    }
                }

                foreach (var colName in columnsFromTableForExcel)
                {
                    if (!columnNamesFromFile.Contains(colName))
                    {
                        throw new Exception("Table def column does not exist in excel file");
                    }
                }
            }
            else
            {
                return false;
            }
            return true;
        }

        private static bool VerifyUserColumnEntryMatchesSchema(string tableName)
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

            if (r)
            {
                WriteToLog(tableName, "", "Mismatch between table definition and table column mapping. Data was not imported.");
            }

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

        private static void ReadConfigfile()
        {
            // lines do not have to be in any specific order
            var lines = File.ReadAllLines(AppDomain.CurrentDomain.BaseDirectory + "config.txt");


            //connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString.ToString();


            //  Console.WriteLine( AppDomain.CurrentDomain.BaseDirectory);

            connectionString = lines
                .Where(x => x.ToLower().StartsWith("connectionstring"))
                .First()
                .Split(new char[] { '|' })[1];
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
