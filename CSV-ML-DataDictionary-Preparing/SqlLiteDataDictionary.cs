using Microsoft.Data.Sqlite;
using System.Data;

namespace CSV_ML_DataDictionary_Preparing
{
    public class SqlLiteDataDictionary : IDisposable
    {
        private readonly SqliteConnection _connection;
        private SqliteTransaction? _transaction;

        // SQL Komutlarını tekrar tekrar oluşturmamak için önbellekliyoruz (Prepared Statements)
        private readonly Dictionary<string, SqliteCommand> _insertCommands = new();

        public string DatabasePath => _connection.DataSource;

        public SqlLiteDataDictionary(string? dicPath)
        {
            if (string.IsNullOrEmpty(dicPath))
            {
                dicPath = AppDomain.CurrentDomain.BaseDirectory;
            }

            var dbFilePath = Path.Combine(Path.GetFullPath(dicPath), "dataDictionary.db");

            // Pooling=True: Bağlantı havuzunu açar.
            var connectionString = new SqliteConnectionStringBuilder
            {
                DataSource = dbFilePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Pooling = true,
                Cache = SqliteCacheMode.Shared
            }.ToString();

            _connection = new SqliteConnection(connectionString);
            _connection.Open();

            // --- PERFORMANCE TUNING ---
            // Bu ayarlar 200GB veri için hayatidir. Senkronizasyonu kapatır, hızı 50x artırır.
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                PRAGMA journal_mode = MEMORY; 
                PRAGMA synchronous = OFF; 
                PRAGMA temp_store = MEMORY; 
                PRAGMA cache_size = 10000;
                PRAGMA locking_mode = EXCLUSIVE;";
            cmd.ExecuteNonQuery();
        }

        public void CreateTableIfNotExists(int columnIndex, string? columnName)
        {
            var tableName = GetTableName(columnIndex, columnName);

            using var command = _connection.CreateCommand();
            command.CommandText = $@"
                CREATE TABLE IF NOT EXISTS {tableName} (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Value TEXT UNIQUE
                );";

            if (_transaction != null) command.Transaction = _transaction;
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Tek tek insert atmak yerine, gelen HashSet'i tek döngüde basar.
        /// </summary>
        public void BatchInsert(int columnIndex, string? columnName, HashSet<string> uniqueValues)
        {
            if (uniqueValues == null || uniqueValues.Count == 0) return;

            var tableName = GetTableName(columnIndex, columnName);

            // Komutu cache'den al veya yarat
            if (!_insertCommands.TryGetValue(tableName, out var cmd))
            {
                cmd = _connection.CreateCommand();
                cmd.CommandText = $"INSERT OR IGNORE INTO {tableName} (Value) VALUES (@Value)";
                var param = cmd.CreateParameter();
                param.ParameterName = "@Value";
                cmd.Parameters.Add(param);

                _insertCommands[tableName] = cmd;
            }

            // Aktif transaction'ı komuta ata
            if (_transaction != null && cmd.Transaction != _transaction)
            {
                cmd.Transaction = _transaction;
            }

            // Parametre değerini değiştirip çalıştır (SQL Parsing maliyetini düşürür)
            foreach (var val in uniqueValues)
            {
                cmd.Parameters["@Value"].Value = val;
                cmd.ExecuteNonQuery();
            }
        }

        private string GetTableName(int columnIndex, string? columnName)
        {
            // Tablo ismini sanitize etmeye gerek yok, zaten caller (çağıran) sanitize edilmiş isim yolluyor
            return $"Column_{columnIndex}{(string.IsNullOrEmpty(columnName) ? "" : "_" + columnName)}";
        }

        public void BeginTransaction()
        {
            if (_transaction == null)
            {
                _transaction = _connection.BeginTransaction();
            }
        }

        public void CommitTransaction()
        {
            if (_transaction != null)
            {
                _transaction.Commit();
                _transaction.Dispose();
                _transaction = null;
            }
        }

        public void Dispose()
        {
            // Önce cache'lenen komutları temizle
            foreach (var cmd in _insertCommands.Values)
            {
                cmd.Dispose();
            }
            _insertCommands.Clear();

            _transaction?.Dispose();
            _connection.Close();
            _connection.Dispose();
        }
    }
}