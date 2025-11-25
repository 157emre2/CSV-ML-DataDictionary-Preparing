using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using TextCopy;

namespace CSV_ML_DataDictionary_Preparing
{
    public class CsvtoDataDictionary
    {
        // ... (Değişken tanımları aynı) ...
        private readonly List<ZipArchiveEntry>? _csvFiles;
        private readonly FileInfo? _csvFile;
        private readonly Dictionary<int, string?> _columns;
        private readonly CsvConfiguration _csvConfiguration;
        private readonly HashSet<int> _ignoredColumnIndexes;
        private string? _dbPath;

        private const int CHANNEL_CAPACITY = 500;
        private const int FLUSH_THRESHOLD_ROWS = 100000; // 100k olarak güncelledik

        // Record yapısını güncelledik: Hangi dosya ve hangi satırda olduğumuzu Consumer'a iletiyoruz
        private record ProcessResult(int ColumnIndex, string? ColumnName, HashSet<string> UniqueValues, string FileName, long CurrentRow);

        private readonly Channel<ProcessResult> _dataChannel;

        public CsvtoDataDictionary(List<ZipArchiveEntry>? csvFiles, FileInfo? csvFile, string delimiter, List<int>? ignoredColumnIndexes, string? outputhPath)
        {
            // ... (Constructor başlangıcı aynı) ...
            _csvFiles = csvFiles != null && csvFiles.Count > 0 ? csvFiles : null;
            _csvFile = csvFile;

            _csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false,
                Delimiter = delimiter,
                BufferSize = 65536,
                ProcessFieldBufferSize = 16384,
                MissingFieldFound = null
            };

            _columns = new();
            _ignoredColumnIndexes = ignoredColumnIndexes != null ? new HashSet<int>(ignoredColumnIndexes) : new HashSet<int>();

            var options = new BoundedChannelOptions(CHANNEL_CAPACITY)
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            };
            _dataChannel = Channel.CreateBounded<ProcessResult>(options);

            try { InitializeAndRun(outputhPath).GetAwaiter().GetResult(); }
            catch (Exception ex) { Console.WriteLine($"CRITICAL ERROR: {ex.Message}"); }
        }

        // ... (InitializeAndRun ve ReadColumnsCsv aynı kalacak) ...
        private async Task InitializeAndRun(string? outputPath)
        {
            // ... (Columns okuma mantığı aynı) ...
            // Kodu kısa tutmak için burayı atlıyorum, öncekiyle aynı.
            // Sadece aşağıdaki fonksiyonu çağırırken:

            // Eğer columns boşsa manuel giriş kısmı da aynı...
            if (_columns.Count == 0 && _csvFiles != null && _csvFiles.Any(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase)))
            {
                var temp = _csvFiles.First(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase));
                _csvFiles.Remove(temp);
                ReadColumnsCsv(temp);
            }
            // ...

            await BuildGlobalMappingsAsync(outputPath, _csvFiles != null);
            Console.WriteLine($"\nCompleted. DB: {_dbPath}  -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
            ClipboardService.SetText(_dbPath ?? string.Empty);
        }

        private void ReadColumnsCsv(ZipArchiveEntry entry)
        {
            using var stream = entry.Open();
            using var reader = new StreamReader(stream, Encoding.UTF8);
            using var csv = new CsvReader(reader, _csvConfiguration);
            if (csv.Read())
            {
                for (int i = 0; i < csv.Context.Parser.Count; i++) _columns.Add(i, EditStringForDatabase(csv.GetField<string>(i) ?? ""));
            }
        }


        private async Task BuildGlobalMappingsAsync(string? outputPath, bool isZipFile)
        {
            using (var sqlLiteCon = new SqlLiteDataDictionary(outputPath))
            {
                _dbPath = sqlLiteCon.DatabasePath;
                var activeColumns = _columns.Where(x => !_ignoredColumnIndexes.Contains(x.Key + 1)).ToList();
                foreach (var col in activeColumns) sqlLiteCon.CreateTableIfNotExists(col.Key + 1, col.Value);

                var dbConsumerTask = Task.Run(() => ConsumerWriteToDb(sqlLiteCon));

                try
                {
                    if (isZipFile && _csvFiles != null)
                    {
                        foreach (var entry in _csvFiles)
                        {
                            // RECOVERY KONTROLÜ BURADA
                            var progress = sqlLiteCon.GetFileProgress(entry.Name);
                            if (progress.IsFinished)
                            {
                                Console.WriteLine($"Skipping {entry.Name} (Already Finished).  -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
                                continue;
                            }

                            Console.WriteLine($"\n\tProcessing File: {entry.Name}. Resuming from row: {progress.LastRow} -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
                            using var stream = entry.Open();
                            await ProcessStreamAndFeedChannel(stream, activeColumns, entry.Name, progress.LastRow);
                        }
                    }
                    else if (_csvFile != null)
                    {
                        var progress = sqlLiteCon.GetFileProgress(_csvFile.Name);
                        if (!progress.IsFinished)
                        {
                            Console.WriteLine($"Processing {_csvFile.Name}. Resuming from row: {progress.LastRow} -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
                            using var stream = _csvFile.OpenRead();
                            await ProcessStreamAndFeedChannel(stream, activeColumns, _csvFile.Name, progress.LastRow);
                        }
                        else
                        {
                            Console.WriteLine("File already fully processed.");
                        }
                    }
                }
                finally
                {
                    _dataChannel.Writer.Complete();
                }
                await dbConsumerTask;
            }
        }

        private async Task ProcessStreamAndFeedChannel(Stream stream, List<KeyValuePair<int, string?>> activeColumns, string fileName, long startRowIndex)
        {
            var localCache = new Dictionary<int, HashSet<string>>();
            foreach (var col in activeColumns) localCache[col.Key] = new HashSet<string>();

            long rowCount = 0; // Dosyadaki gerçek satır indeksi
            int bufferCount = 0; // Flush için sayaç

            using (var reader = new StreamReader(stream, Encoding.UTF8))
            using (var csv = new CsvReader(reader, _csvConfiguration))
            {
                // FAST SKIP: Kaldığımız yere kadar boş okuma yapıyoruz
                while (rowCount < startRowIndex && csv.Read())
                {
                    rowCount++;
                    if (rowCount % 100000 == 0) Console.Write($"\rSkipping rows... {rowCount:N0}");
                }
                if (startRowIndex > 0) Console.WriteLine($"\nResumed processing.  -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");

                while (csv.Read())
                {
                    rowCount++;
                    bufferCount++;

                    foreach (var col in activeColumns)
                    {
                        try
                        {
                            var val = csv.GetField<string>(col.Key);
                            if (!string.IsNullOrEmpty(val)) localCache[col.Key].Add(val);
                        }
                        catch { }
                    }

                    if (bufferCount >= FLUSH_THRESHOLD_ROWS)
                    {
                        // Paket içine FileName ve Güncel Satır Sayısını (rowCount) da koyuyoruz
                        await FlushCacheToChannel(localCache, activeColumns, fileName, rowCount);
                        bufferCount = 0;
                    }
                }
            }
            // Kalan son verileri ve "Bitti" bilgisini gönder
            // rowCount'u gönderiyoruz ama bitiş flag'ini Consumer yönetecek
            await FlushCacheToChannel(localCache, activeColumns, fileName, rowCount, true);
        }

        private async Task FlushCacheToChannel(Dictionary<int, HashSet<string>> cache, List<KeyValuePair<int, string?>> activeColumns, string fileName, long currentRow, bool isFinished = false)
        {
            foreach (var col in activeColumns)
            {
                if (cache[col.Key].Count > 0)
                {
                    var dataToSend = new HashSet<string>(cache[col.Key]);
                    await _dataChannel.Writer.WriteAsync(new ProcessResult(col.Key + 1, col.Value, dataToSend, fileName, currentRow));
                    cache[col.Key].Clear();
                }
            }

            // DÜZELTME: Veri gönderilmiş olsa bile, eğer isFinished bayrağı kalktıysa MUTLAKA Bitiş Sinyali gönder.
            if (isFinished)
            {
                var dummySet = new HashSet<string>();
                // ColumnIndex -1 = EOF (End Of File) Sinyali
                await _dataChannel.Writer.WriteAsync(new ProcessResult(-1, "EOF", dummySet, fileName, currentRow));
            }
        }

        private async Task ConsumerWriteToDb(SqlLiteDataDictionary db)
        {
            Console.WriteLine($"-> DB Consumer started.  -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
            db.BeginTransaction();
            int batchesProcessed = 0;

            // Son işlenen dosya ve satırı takip et
            string currentFile = "";
            long currentRow = 0;

            await foreach (var packet in _dataChannel.Reader.ReadAllAsync())
            {
                // Dummy EOF paketi kontrolü (ColumnIndex -1)
                if (packet.ColumnIndex == -1)
                {
                    db.UpdateProgress(packet.FileName, packet.CurrentRow, true); // Dosya Bitti!
                    Console.WriteLine($"\n-> File Completed: {packet.FileName}  -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
                    continue;
                }

                db.BatchInsert(packet.ColumnIndex, packet.ColumnName, packet.UniqueValues);

                // Progress takibi
                currentFile = packet.FileName;
                currentRow = packet.CurrentRow;

                batchesProcessed++;

                // Transaction Commit (Checkpoint)
                if (batchesProcessed >= 50) // Her 50 pakette bir kaydet
                {
                    // VERİYİ KAYDEDERKEN, LOG TABLOSUNU DA GÜNCELLİYORUZ
                    // Böylece transaction commit olduğunda hem veri hem de "Ben buradayım" bilgisi aynı anda diske yazılır.
                    if (!string.IsNullOrEmpty(currentFile))
                    {
                        db.UpdateProgress(currentFile, currentRow, false);
                    }

                    db.CommitTransaction();
                    db.BeginTransaction();
                    Console.Write(".");
                    batchesProcessed = 0;
                }
            }

            db.CommitTransaction();
            Console.WriteLine($"\n-> Database write operations completed.  -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
        }

        private static readonly Regex SafeStringRegex = new Regex(@"[^a-zA-Z0-9_]", RegexOptions.Compiled);
        private string EditStringForDatabase(string inputName)
        {
            if (string.IsNullOrEmpty(inputName)) return string.Empty;
            var sb = new StringBuilder(inputName);
            sb.Replace("ç", "c").Replace("Ç", "C").Replace("ğ", "g").Replace("Ğ", "G").Replace("ı", "i").Replace("İ", "I").Replace("ö", "o").Replace("Ö", "O").Replace("ş", "s").Replace("Ş", "S").Replace("ü", "u").Replace("Ü", "U").Replace(" ", "_");
            string sanitized = sb.ToString().Trim();
            return SafeStringRegex.Replace(sanitized, "");
        }
    }
}