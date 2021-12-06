using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Repository
{
    public class MSSQLServerSnapshotServiceOptions
    {
        public string TableName { get; set; }
        public JsonSerializerSettings JsonSerializerSettings { get; set; }
        public string SqlConnectionString { get; set; }
    }
}
