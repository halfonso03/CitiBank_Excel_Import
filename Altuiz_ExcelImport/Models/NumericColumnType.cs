using Altuiz_ExcelImport.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Altuiz_ExcelImport.Models
{
    public class NumericColumn
    {
        public int ColumnIndex { get; set; }
        public SqlDataType SqlDataType { get; set; }
        public bool IsNullable { get; set; }
    }


    public class DateColumn
    {
        public int ColumnIndex { get; set; }
        public SqlDataType SqlDataType { get; set; }
    }
}
