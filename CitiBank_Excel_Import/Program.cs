using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CitiBank_Excel_Import.Models;
using LinqToExcel;
using LinqToExcel.Domain;
using LinqToExcel.Query;
using Remotion;

namespace CitiBank_Excel_Import
{
    class Program
    {
        private static string connectionString;        

        static void Main(string[] args)
        {

            ReadConfigfile();

            ProcessFiles(GetFiles()).GetAwaiter().GetResult();

            Console.ReadLine();
        }

        private static ImportFile[] GetFiles()
        {
            List<ImportFile> files = new List<ImportFile>();

            using (var cn = new SqlConnection(connectionString))
            {
                var sql = @"SELECT DISTINCT file_path, table_name, file_action FROM ExcelFilePaths p 
                                JOIN ExcelFileToTableMap m ON p.table_id = m.table_id";

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

        private static async Task ProcessFiles(ImportFile[] files)
        {
            
            foreach (var file in files)
            {
                var exists = await TableExists(file.TableName);

                if (exists) 
                {
                    try
                    {
                        if (await VerifyColumNamesMatch(file))
                        {
                            await ProcessFile(file);
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

        private static async Task ProcessFile(ImportFile file)
        {
            var sql = "";
            var factory = new ExcelQueryFactory(file.FilePath);
            var tableName = Path.GetFileNameWithoutExtension(file.FilePath);
            var columnNamesFromFile = await GetTableColumnNames(file.TableName);
            var columnCount = columnNamesFromFile.Length;
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

                            reader = await cmd.ExecuteReaderAsync();
                            await reader.ReadAsync();
                            rowsDeleted = reader.GetInt32(0);
                        }
                 
 
                        var insertValues = "";
                        var insertHeader = $"INSERT INTO {tableName} ({String.Join(",", columnNamesFromFile)}, [update], dttolap) VALUES ";
                        
                        ExcelQueryable<LinqToExcel.Row> data = factory.Worksheet(0);

                        foreach (var row in data)
                        {
                            insertValues += "(";

                            for (var x = 0; x <= columnCount - 1; x++)
                            {
                                insertValues += $"'{row[x].ToString().Replace("'", "''")}',";
                            }
                            insertValues = insertValues.Substring(0, insertValues.Length - 1) + ", GETDATE(), '" + fileUpdateDate.ToString("MM/dd/yy") + "'),";
                        }

                        insertValues = insertValues.Substring(0, insertValues.Length - 1);


                        sql = $"{insertHeader} {insertValues}; SELECT @@ROWCOUNT";
                                                
                        cmd.CommandText = sql;

                        if (reader != null && !reader.IsClosed) reader.Close();
                                                

                        reader = await cmd.ExecuteReaderAsync();
                        await reader.ReadAsync();

                        var recordsInserted = reader.GetInt32(0);

                        reader.Close();
                        // write to log reord delete count
                        // write to log record insert count

                        
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

        

        private static async Task<bool> VerifyColumNamesMatch(ImportFile file)
        {
            var factory = new ExcelQueryFactory(file.FilePath);

            if (Path.GetExtension(file.FilePath) == ".xls")
                factory.DatabaseEngine = DatabaseEngine.Jet;
            else
                factory.DatabaseEngine = DatabaseEngine.Ace;

            var columnNamesFromFile = factory.GetColumnNames(factory.GetWorksheetNames().First());
            var columnsFromTableForExcel = await GetTableColumnNames( 
                tableName: file.TableName);


            if (await VerifyUserColumnEntryMatchesSchema(file.TableName))
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

        private static async Task<bool> VerifyUserColumnEntryMatchesSchema(string tableName)
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
                            AND c.name NOT IN ('dttolap', 'update')) s ON u.column_name = s.name
                            WHERE s.name IS NULL OR u.column_name IS NULL";

            var r = await RunSqlRecordsExist(sql);

            return !r;
        }

  

        private static async Task<string[]> GetTableColumnNames(string tableName)
        {
            var names = new List<string>();
            using (var cn = new SqlConnection(connectionString))
            {
                var sql = $@"SELECT column_name FROM ExcelFileToTableMap WHERE table_id = (
                                SELECT table_id FROM ExcelFilePaths WHERE table_name = '{tableName}')";

                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();

                    var reader = await cmd.ExecuteReaderAsync();

                    while (reader.Read())
                    {
                        names.Add(reader.GetString(0));
                    }

                    cn.Close();
                }
            }

            return names.ToArray();
        }

       

        private static async Task<bool> TableExists (string tableName)
        {
            bool exists = false;

            using (var cn = new SqlConnection(connectionString))
            {
                var sql = $"SELECT * FROM sys.tables WHERE name ='{tableName}'";
                
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();

                    var reader = await cmd.ExecuteReaderAsync();

                    exists = reader.HasRows;

                    cn.Close();
                }               
            }

            return exists;
        }

        private static void ReadConfigfile()
        {
            // lines do not have to be in any specific order
            var lines = File.ReadAllLines("config.txt");

            connectionString = lines
                .Where(x => x.ToLower().StartsWith("connectionstring"))
                .First()
                .Split(new char[] { '|' })[1];                    
        }

        private static async Task RunSql(string sql)
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

                        await cmd.ExecuteNonQueryAsync();

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

        private async static Task<bool> RunSqlRecordsExist(string sql)
        {
            using (var cn = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();
                    var reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.CloseConnection);
                    return reader.HasRows;
                }
            }
        }

        private static void WriteToLog()
        {

        }
    }
}
