using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CitiBank_Excel_Import.Models
{
    public class ImportFile
    {
        public string FilePath { get; set; }
        public string TableName { get; set; }        
        public string Action { get; set; }
        public bool ConfigurationNotSet { get; set; } = false;
        
    }
}
