using DocumentFormat.OpenXml;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.Marshalling.IIUnknownCacheStrategy;

namespace CSV_ML_DataDictionary_Preparing
{
    public class SqlLiteDataDictionary : IDisposable
    {
        private readonly SqliteConnection _connection;
        private SqliteTransaction? _transaction;
        public string DatabasePath => _connection.DataSource;

        public SqlLiteDataDictionary(string? dicPath)
        {
            if (string.IsNullOrEmpty(dicPath))
            {
                dicPath = "dataDictionary.db";
            }

            _connection = new SqliteConnection($"Data Source={Path.GetFullPath(dicPath)}/dataDictionary.db");
            _connection.Open();
        }

        public void CreateAllTables(Dictionary<int, string?> tableInfos)
        {
            foreach (var tableInfo in tableInfos)
            {
                var tableName = $"Column_{tableInfo.Key + 1}{(string.IsNullOrEmpty(tableInfo.Value) ? "" : "_" + tableInfo.Value)}";

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = $@"
                                        CREATE TABLE IF NOT EXISTS {tableName} (
                                            Id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            Value TEXT UNIQUE
                                        );
                                    ";
                    command.ExecuteNonQuery();
                }

                Console.WriteLine($"Table '{tableName}' created successfully. Time..: {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
            }
        }

        public void InsertUniqueValue(int columnIndex, string? columnName, string value)
        {
            var tableName = $"Column_{columnIndex}{(string.IsNullOrEmpty(columnName) ? "" : "_" + columnName)}";

            using (var insertCmd = _connection.CreateCommand())
            {
                insertCmd.CommandText =
                    $"INSERT OR IGNORE INTO {tableName} (Value) VALUES (@Value)";

                insertCmd.Parameters.AddWithValue("@Value", value);
                insertCmd.ExecuteNonQuery();
            }
        }

        public void BeginTransaction()
        {
            _transaction = _connection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            _transaction?.Commit();
            _transaction?.Dispose();
        }

        public void Dispose()
        {
            _connection.Close();
            _connection.Dispose();
        }
    }
}
