using Microsoft.Data.Sqlite;
using System.Data;

namespace CSV_ML_DataDictionary_Preparing
{
    public class SqlLiteDataDictionary : IDisposable
    {
        private readonly SqliteConnection _connection;
        private SqliteTransaction? _transaction;
        private readonly Dictionary<string, SqliteCommand> _insertCommands = new();

        public string DatabasePath => _connection.DataSource;

        public SqlLiteDataDictionary(string? dicPath)
        {
            if (string.IsNullOrEmpty(dicPath)) dicPath = AppDomain.CurrentDomain.BaseDirectory;
            var dbFilePath = Path.Combine(Path.GetFullPath(dicPath), "dataDictionary.db");

            var connectionString = new SqliteConnectionStringBuilder
            {
                DataSource = dbFilePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Pooling = true,
                Cache = SqliteCacheMode.Shared
            }.ToString();

            _connection = new SqliteConnection(connectionString);
            _connection.Open();

            // Performans ayarları
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                PRAGMA journal_mode = MEMORY; 
                PRAGMA synchronous = OFF; 
                PRAGMA temp_store = MEMORY; 
                PRAGMA locking_mode = EXCLUSIVE;";
            cmd.ExecuteNonQuery();

            // LOG TABLOSUNU OLUŞTUR (Checkpoint için)
            CreateLogTable();
        }

        private void CreateLogTable()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS Process_Logs (
                    FileName TEXT PRIMARY KEY,
                    LastRowProcessed INTEGER,
                    IsFinished BOOLEAN
                );";
            cmd.ExecuteNonQuery();
        }

        public (long LastRow, bool IsFinished) GetFileProgress(string fileName)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT LastRowProcessed, IsFinished FROM Process_Logs WHERE FileName = @Name";
            cmd.Parameters.AddWithValue("@Name", fileName);

            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                return (reader.GetInt64(0), reader.GetBoolean(1));
            }
            return (0, false);
        }

        public void UpdateProgress(string fileName, long lastRow, bool isFinished)
        {
            // Bu metod Transaction içinde çağrılacak!
            var cmdText = @"
                INSERT INTO Process_Logs (FileName, LastRowProcessed, IsFinished) 
                VALUES (@Name, @Row, @Finished)
                ON CONFLICT(FileName) DO UPDATE SET 
                    LastRowProcessed = @Row,
                    IsFinished = @Finished;";

            // Cache mekanizması kullanmıyoruz çünkü burası sık çağrılmayacak
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = cmdText;
            cmd.Parameters.AddWithValue("@Name", fileName);
            cmd.Parameters.AddWithValue("@Row", lastRow);
            cmd.Parameters.AddWithValue("@Finished", isFinished);

            if (_transaction != null) cmd.Transaction = _transaction;

            cmd.ExecuteNonQuery();
        }

        // --- Eski Metodlar Aynen Kalıyor ---
        public void CreateTableIfNotExists(int columnIndex, string? columnName)
        {
            var tableName = GetTableName(columnIndex, columnName);
            using var command = _connection.CreateCommand();
            command.CommandText = $@"CREATE TABLE IF NOT EXISTS {tableName} (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value TEXT UNIQUE);";
            if (_transaction != null) command.Transaction = _transaction;
            command.ExecuteNonQuery();
        }

        public void BatchInsert(int columnIndex, string? columnName, HashSet<string> uniqueValues)
        {
            if (uniqueValues == null || uniqueValues.Count == 0) return;
            var tableName = GetTableName(columnIndex, columnName);

            if (!_insertCommands.TryGetValue(tableName, out var cmd))
            {
                cmd = _connection.CreateCommand();
                cmd.CommandText = $"INSERT OR IGNORE INTO {tableName} (Value) VALUES (@Value)";
                var param = cmd.CreateParameter();
                param.ParameterName = "@Value";
                cmd.Parameters.Add(param);
                _insertCommands[tableName] = cmd;
            }

            if (_transaction != null && cmd.Transaction != _transaction) cmd.Transaction = _transaction;

            foreach (var val in uniqueValues)
            {
                cmd.Parameters["@Value"].Value = val;
                cmd.ExecuteNonQuery();
            }
        }

        private string GetTableName(int columnIndex, string? columnName) => $"Column_{columnIndex}{(string.IsNullOrEmpty(columnName) ? "" : "_" + columnName)}";

        public void BeginTransaction() { if (_transaction == null) _transaction = _connection.BeginTransaction(); }

        public void CommitTransaction() { if (_transaction != null) { _transaction.Commit(); _transaction.Dispose(); _transaction = null; } }

        public void Dispose() { foreach (var cmd in _insertCommands.Values) cmd.Dispose(); _insertCommands.Clear(); _transaction?.Dispose(); _connection.Close(); _connection.Dispose(); }
    }
}