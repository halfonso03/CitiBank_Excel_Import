using Altuiz_ExcelImport.Infrastructure;
using Altuiz_ExcelImport.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Altuiz_ExcelImport
{
    public class Helpers
    {
        public static void ConnectToDb()
        {
            using (var cn = new SqlConnection(Program.connectionString))
            {
                using (var cmd = new SqlCommand())
                {
                    cmd.Connection = cn;

                    cn.Open();

                    cmd.Parameters.Clear();
                    cmd.CommandText = "SELECT COUNT(*) FROM ExcelFilePaths";
                    var reader = cmd.ExecuteReader();
                }
            }
        }

        public static bool IsDateColumn(int colIndex)
        {
            return Program.dateColumns.FirstOrDefault(c => c.ColumnIndex == colIndex) != null;
        }

        public static bool IsIntegerColumn(int colIndex)
        {
            return Program.integerColumns.FirstOrDefault(c => c.ColumnIndex == colIndex) != null;
        }

        public static bool IsFloatColumn(int colIndex)
        {
            return Program.floatColumns.FirstOrDefault(c => c.ColumnIndex == colIndex) != null;
        }

        public static List<DateColumn> GetDateColumns(string tableName)
        {
            var results = new List<DateColumn>();

            var sql = $@"SELECT c.column_id, system_type_id
                    FROM sys.all_columns c join sys.tables t ON
	                    t.object_id = c.object_id 
                    WHERE t.name = '{tableName}' 
                        AND system_type_id IN (61, 42, 58) ";

            using (var cn = new SqlConnection(Program.connectionString))
            {
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();
                    var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        results.Add(new DateColumn
                        {
                            ColumnIndex = reader.GetInt32(0) - 1
                        });
                    }
                }

            }

            return results;
        }

        public static List<NumericColumn> GetFloatColumns(string tableName)
        {
            var results = new List<NumericColumn>();

            var sql = $@"SELECT c.column_id, system_type_id, is_nullable
                    FROM sys.all_columns c join sys.tables t ON
	                    t.object_id = c.object_id 
                    WHERE t.name = '{tableName}' 
                        AND system_type_id IN (106, 62, 60, 108, 122)";

            using (var cn = new SqlConnection(Program.connectionString))
            {
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();
                    var reader = cmd.ExecuteReader();

                    try
                    {
                        while (reader.Read())
                        {
                            results.Add(new NumericColumn
                            {
                                ColumnIndex = reader.GetInt32(0) - 1,
                                SqlDataType = (SqlDataType)reader.GetByte(1),
                                IsNullable = Boolean.Parse(reader.GetValue(2).ToString())
                            });
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;

                    }
                    
                }

            }

            return results;
        }



        public static List<NumericColumn> GetIntegerColumns(string tableName)
        {
            var results = new List<NumericColumn>();

            var sql = $@"SELECT c.column_id, system_type_id
                    FROM sys.all_columns c join sys.tables t ON
	                    t.object_id = c.object_id 
                    WHERE t.name = '{tableName}' 
                        AND system_type_id IN (127, 56, 52, 48)";

            using (var cn = new SqlConnection(Program.connectionString))
            {
                using (var cmd = new SqlCommand(sql, cn))
                {
                    cn.Open();
                    var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        results.Add(new NumericColumn
                        {
                            ColumnIndex = reader.GetInt32(0) - 1,
                            SqlDataType = (SqlDataType)reader.GetByte(1)
                        });
                    }
                }

            }

            return results;
        }


    }
}
