using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LinqToExcel;
using LinqToExcel.Domain;
using LinqToExcel.Query;
using Remotion;

namespace CitiBank_Excel_Import
{
    class Program
    {
        private static string connectionString;
        private static string excelFilePath;

        static void Main(string[] args)
        {

            ReadConfigfile();

            string[] filePaths = GetFilePaths();

            ProcessFiles(filePaths).GetAwaiter().GetResult();

            Console.ReadLine();
        }

        private static string[] GetFilePaths()
        {
            List<string> paths = new List<string>();

            using (var cn = new SqlConnection(connectionString))
            {
                var sql = "SELECT file_path FROM ExcelFilePaths";

                var cmd = new SqlCommand(sql, cn);
                {
                    cn.Open();
                    
                    var reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        paths.Add(reader.GetString(0));
                    }

                    cn.Close();
                }
            }

            return paths.ToArray();
        }

        private static async Task ProcessFiles(string[] filePaths)
        {
            
            foreach (var file in filePaths)
            {
                var exists = await TableExists(Path.GetFileNameWithoutExtension(file));

                if (exists) 
                {
                    try
                    {
                        if (await VerifyColumNamesMatch(file))
                        {
                            ProcessFile(file);
                        }
                    }
                    catch (Exception ex)
                    {
                        var m = ex.Message;
                    }
                    
                }
            }

        }

        private static async Task ProcessFile(string filePath)
        {
            var factory = new ExcelQueryFactory(filePath);
            var tableName = Path.GetFileNameWithoutExtension(filePath);

            if (Path.GetExtension(filePath) == ".xls")
                factory.DatabaseEngine = DatabaseEngine.Jet;
            else
                factory.DatabaseEngine = DatabaseEngine.Ace;

            var columnNamesFromFile = factory.GetColumnNames(factory.GetWorksheetNames().First());
            var columnCount = columnNamesFromFile.Count();

            var insertHeader = $"INSERT INTO {tableName} ({String.Join(",", columnNamesFromFile)}) VALUES ";
            var insertValues = "";

            ExcelQueryable<LinqToExcel.Row> data = factory.Worksheet(0);

            
            foreach (var row in data)
            {
                insertValues += "(";

                for (var x = 0; x <= columnCount - 1; x++)
                {
                    insertValues += $"'{row[x].ToString().Replace("'", "''")}',";
                }
                insertValues = insertValues.Substring(0, insertValues.Length - 1);

                insertValues += "),";
            }

            insertValues = insertValues.Substring(0, insertValues.Length - 1);


            var sql = $"{insertHeader} {insertValues}";

            await RunInsert(sql);
        }

        private static async Task  RunInsert(string sql)
        {

            using (var cn = new SqlConnection(connectionString))
            {               
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();

                    await cmd.ExecuteNonQueryAsync();

                    cn.Close();
                }
            }

        }

        private static async Task<bool> VerifyColumNamesMatch(string filePath)
        {
            var factory = new ExcelQueryFactory(filePath);

            if (Path.GetExtension(filePath) == ".xls")
                factory.DatabaseEngine = DatabaseEngine.Jet;
            else
                factory.DatabaseEngine = DatabaseEngine.Ace;

            var columnNamesFromFile = factory.GetColumnNames(factory.GetWorksheetNames().First());
            var columnsFromTableForExcel = await GetTableColumnNames(Path.GetFileNameWithoutExtension(filePath));


            var date = factory.WorksheetNoHeader(1).Select(x => x).First();


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




            return true;
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

        private static void WriteToLog()
        {

        }

        private static async Task<bool> TableExists (string tableName)
        {
            bool exists = false;

            using (var cn = new SqlConnection(connectionString))
            {
                var sql = $"select * from sys.tables where name ='{tableName}'";
                
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
         
            excelFilePath = lines
                .Where(x => x.ToLower().StartsWith("excefilepath"))
                .First()
                .Split(new char[] { '|' })[1];


        }
    }
}
