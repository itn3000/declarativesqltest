using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using DeclarativeSql.Mapping;
using DeclarativeSql;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using DeclarativeSql.Dapper;
using Dapper;
using System.Linq.Expressions;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace declarativesqltest
{
    using WriteAction = Action<NpgsqlBinaryImporter, MyStructure>;
    [Table("bulktest")]
    class MyStructure
    {
        public string a { get; set; }
        public int b { get; set; }
    }
    static class DbTypeExtension
    {
        //public static NpgsqlTypes.NpgsqlDbType ToNpgsqlDbType(this DbType t)
        //{
        //    switch((int)t)
        //    {
        //        case (int)DbType.AnsiString:
        //        case (int)DbType.String:
        //            return NpgsqlTypes.NpgsqlDbType.Text;
        //        case (int)DbType.Binary:
        //            return NpgsqlTypes.NpgsqlDbType.Bytea;
        //        case (int)DbType.Boolean:
        //            return NpgsqlTypes.NpgsqlDbType.Boolean;
        //        case (int)DbType.Currency:
        //            return NpgsqlTypes.NpgsqlDbType.Money;
        //        case (int)DbType.Date:
        //            return NpgsqlTypes.NpgsqlDbType.Date;
        //        case (int)DbType.DateTime:
        //            return NpgsqlTypes.NpgsqlDbType.Timestamp;
        //        case (int)DbType.DateTime2:
        //            return NpgsqlTypes.NpgsqlDbType.Timestamp;
        //    }
        //}
    }

    class Program
    {
        static ConcurrentDictionary<Type, Dictionary<string, WriteAction>> TypeMap
            = new ConcurrentDictionary<Type, Dictionary<string, WriteAction>>(); 
        static void Main(string[] args)
        {
            var mappinginfo = TableMappingInfo.Create<MyStructure>();
            var cb = new NpgsqlConnectionStringBuilder();
            cb.Host = "localhost";
            cb.Port = 5432;
            cb.Database = "test1";
            cb.Username = "postgres";
            cb.Password = "postgres";
            using (var con = new NpgsqlConnection(cb))
            {
                con.Open();
                con.Truncate<MyStructure>();
                var sw = new Stopwatch();
                sw.Start();
                using (var writer = con.BeginBinaryImport($"copy {mappinginfo.FullName}({string.Join(",", mappinginfo.Columns.Select(x => x.ColumnName))}) from stdin (format binary)"))
                {
                    foreach (var val in Enumerable.Range(0, 1000000).Select(i => new MyStructure() { a = "hoge" + i.ToString(), b = i }))
                    {
                        Dictionary<string, WriteAction> actions;
                        if (!TypeMap.TryGetValue(typeof(MyStructure), out actions))
                        {
                            actions = new Dictionary<string, Action<NpgsqlBinaryImporter, MyStructure>>();
                            foreach (var column in mappinginfo.Columns)
                            {
                                var writerparam = Expression.Parameter(typeof(NpgsqlBinaryImporter), "writer");
                                var lambdaparam = Expression.Parameter(typeof(MyStructure), "val");
                                var call = Expression.Call(writerparam, "Write", new Type[] { column.PropertyType }, Expression.Property(lambdaparam, column.PropertyName));
                                var lambda = Expression.Lambda<WriteAction>(call, writerparam, lambdaparam).Compile();
                                actions[column.ColumnName] = lambda;
                            }
                            TypeMap[typeof(MyStructure)] = actions;
                        }
                        writer.StartRow();
                        foreach (var column in mappinginfo.Columns)
                        {
                            actions[column.ColumnName](writer, val);
                        }
                    }
                }
                sw.Stop();
                Trace.WriteLine($"{sw.Elapsed}");
            }
        }
    }
}
