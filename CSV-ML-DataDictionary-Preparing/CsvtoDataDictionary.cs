using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels; // Channel için gerekli
using TextCopy;

namespace CSV_ML_DataDictionary_Preparing
{
    public class CsvtoDataDictionary
    {
        private readonly List<ZipArchiveEntry>? _csvFiles;
        private readonly FileInfo? _csvFile;
        private readonly Dictionary<int, string?> _columns;
        private readonly CsvConfiguration _csvConfiguration;
        private readonly HashSet<int> _ignoredColumnIndexes; // HashSet arama hızı için
        private string? _dbPath;

        // --- AYARLAR ---
        // RAM koruması: Kanalda en fazla 500 paket bekleyebilir. Dolarsa okuma durur.
        private const int CHANNEL_CAPACITY = 500;
        // Her 50.000 satırda bir veriler kanala gönderilir (RAM'i boşaltmak için)
        private const int FLUSH_THRESHOLD_ROWS = 50000;

        // Kanalda taşıyacağımız veri paketi (Record struct hafiftir)
        private record ProcessResult(int ColumnIndex, string? ColumnName, HashSet<string> UniqueValues);

        // Producer-Consumer Kanalı
        private readonly Channel<ProcessResult> _dataChannel;

        public CsvtoDataDictionary(List<ZipArchiveEntry>? csvFiles, FileInfo? csvFile, string delimiter, List<int>? ignoredColumnIndexes, string? outputhPath)
        {
            _csvFiles = csvFiles != null && csvFiles.Count > 0 ? csvFiles : null;
            _csvFile = csvFile;

            // CSV Okuma Ayarları
            _csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false,
                Delimiter = delimiter,
                BufferSize = 65536, // Okuma Buffer'ı artırıldı
                ProcessFieldBufferSize = 16384
            };

            _columns = new();
            // Listeyi HashSet'e çeviriyoruz (O(1) lookup performansı için)
            _ignoredColumnIndexes = ignoredColumnIndexes != null ? new HashSet<int>(ignoredColumnIndexes) : new HashSet<int>();

            // Kanalı oluştur
            var options = new BoundedChannelOptions(CHANNEL_CAPACITY)
            {
                SingleWriter = false,
                SingleReader = true, // Sadece tek bir DB yazıcısı var
                FullMode = BoundedChannelFullMode.Wait // Kanal dolarsa bekle (RAM patlamasın diye)
            };
            _dataChannel = Channel.CreateBounded<ProcessResult>(options);

            // Başlatma mantığı
            InitializeAndRun(outputhPath).Wait(); // Constructor içinde async çağırmak için Wait() (Dikkatli olunmalı)
        }

        private async Task InitializeAndRun(string? outputPath)
        {
            // 1. Columns.csv Okuma Mantığı (Varsa)
            if (_csvFiles != null && _csvFiles.Any(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase)))
            {
                var temp = _csvFiles.First(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase));
                _csvFiles.Remove(temp);
                Console.WriteLine("Reading columns.csv...");
                ReadColumnsCsv(temp); // Aşağıda tanımlı
            }
            else if (_columns.Count == 0) // Eğer kolonlar henüz belirlenmediyse manuel giriş
            {
                Console.Write("Columns.csv not found. Enter number of columns: ");
                if (int.TryParse(Console.ReadLine(), out var result) && result > 0)
                {
                    for (int i = 0; i < result; i++) _columns.Add(i, null);
                }
                else
                {
                    Console.WriteLine("Invalid input. Exiting.");
                    return;
                }
            }

            // 2. Ana İşlemi Başlat
            await BuildGlobalMappingsAsync(outputPath, _csvFiles != null);

            Console.WriteLine($"\n\tAll operations completed. DB Location: {_dbPath}");
            ClipboardService.SetText(_dbPath ?? string.Empty);
            Console.WriteLine("--- App Ended ---");
        }

        private void ReadColumnsCsv(ZipArchiveEntry entry)
        {
            using var stream = entry.Open();
            using var reader = new StreamReader(stream, Encoding.UTF8);
            using var csv = new CsvReader(reader, _csvConfiguration);
            while (csv.Read())
            {
                for (int i = 0; i < csv.ColumnCount; i++)
                {
                    try
                    {
                        var val = csv.GetField<string>(i);
                        if (!string.IsNullOrEmpty(val)) _columns.Add(i, EditStringForDatabase(val));
                    }
                    catch { /* Ignore */ }
                }
            }
        }

        private async Task BuildGlobalMappingsAsync(string? outputPath, bool isZipFile)
        {
            using (var sqlLiteCon = new SqlLiteDataDictionary(outputPath))
            {
                _dbPath = sqlLiteCon.DatabasePath;

                // 1. Tabloları baştan oluştur
                var activeColumns = _columns.Where(x => !_ignoredColumnIndexes.Contains(x.Key + 1)).ToList();
                foreach (var col in activeColumns)
                {
                    sqlLiteCon.CreateTableIfNotExists(col.Key + 1, col.Value);
                }

                Console.WriteLine($"Starting Processing... Mode: {(isZipFile ? "ZIP Multi-File" : "Single File")}");

                // 2. TÜKETİCİ (CONSUMER) TASK BAŞLAT
                // Bu arka planda sürekli kanalı dinleyip veritabanına yazacak.
                var dbConsumerTask = Task.Run(() => ConsumerWriteToDb(sqlLiteCon));

                // 3. ÜRETİCİ (PRODUCER) - Dosyaları Oku ve Kanala At
                try
                {
                    if (isZipFile && _csvFiles != null)
                    {
                        foreach (var entry in _csvFiles)
                        {
                            Console.WriteLine($"\n\tProcessing File: {entry.Name} -- {DateTime.Now:HH:mm:ss}");
                            using var stream = entry.Open();
                            await ProcessStreamAndFeedChannel(stream, activeColumns);
                        }
                    }
                    else if (_csvFile != null)
                    {
                        Console.WriteLine($"\n\tProcessing File: {_csvFile.Name} -- {DateTime.Now:HH:mm:ss}");
                        using var stream = _csvFile.OpenRead();
                        await ProcessStreamAndFeedChannel(stream, activeColumns);
                    }
                }
                finally
                {
                    // Üretim bitti, kanalı kapat. Consumer bunu anlayıp duracak.
                    _dataChannel.Writer.Complete();
                }

                // Consumer'ın işini bitirmesini bekle
                await dbConsumerTask;
            }
        }

        private async Task ProcessStreamAndFeedChannel(Stream stream, List<KeyValuePair<int, string?>> activeColumns)
        {
            // Her dosya için geçici RAM önbelleği
            var localCache = new Dictionary<int, HashSet<string>>();
            foreach (var col in activeColumns) localCache[col.Key] = new HashSet<string>();

            int rowCount = 0;

            using (var reader = new StreamReader(stream, Encoding.UTF8))
            using (var csv = new CsvReader(reader, _csvConfiguration))
            {
                while (csv.Read())
                {
                    rowCount++;

                    foreach (var col in activeColumns)
                    {
                        try
                        {
                            var val = csv.GetField<string>(col.Key);
                            if (!string.IsNullOrEmpty(val))
                            {
                                localCache[col.Key].Add(val);
                            }
                        }
                        catch { /* Log or Ignore */ }
                    }

                    // Belirli satır sayısına gelince kanala boşalt (RAM dolmasın diye)
                    if (rowCount >= FLUSH_THRESHOLD_ROWS)
                    {
                        await FlushCacheToChannel(localCache, activeColumns);
                        rowCount = 0;
                    }
                }
            }
            // Kalan son verileri de gönder
            await FlushCacheToChannel(localCache, activeColumns);
        }

        private async Task FlushCacheToChannel(Dictionary<int, HashSet<string>> cache, List<KeyValuePair<int, string?>> activeColumns)
        {
            foreach (var col in activeColumns)
            {
                if (cache[col.Key].Count > 0)
                {
                    // Verinin kopyasını oluşturup gönderiyoruz (çünkü cache'i temizleyeceğiz)
                    var dataToSend = new HashSet<string>(cache[col.Key]);

                    // Kanala yaz. Eğer DB yavaşsa ve kanal doluysa burada bekler (Backpressure)
                    await _dataChannel.Writer.WriteAsync(new ProcessResult(col.Key + 1, col.Value, dataToSend));

                    cache[col.Key].Clear();
                }
            }
        }

        /// <summary>
        /// Bu metot tek başına ayrı bir thread'de çalışır ve SADECE DB'ye yazar.
        /// Asla kilitlenme (Lock) olmaz çünkü tek erişim noktası burasıdır.
        /// </summary>
        private async Task ConsumerWriteToDb(SqlLiteDataDictionary db)
        {
            db.BeginTransaction();
            int batchesProcessed = 0;

            // Kanal kapanana kadar (Reader.ReadAllAsync) gelen paketleri işle
            await foreach (var packet in _dataChannel.Reader.ReadAllAsync())
            {
                db.BatchInsert(packet.ColumnIndex, packet.ColumnName, packet.UniqueValues);
                batchesProcessed++;

                // Transaction Log şişmesin diye arada bir commit et
                // Her 100 paket (~5 milyon veri) yaklaşık olarak iyi bir noktadır
                if (batchesProcessed >= 100)
                {
                    db.CommitTransaction();
                    db.BeginTransaction();
                    Console.Write("."); // İlerleme çubuğu gibi ekrana nokta koy
                    batchesProcessed = 0;
                }
            }

            // Kalan son işlemi de kaydet
            db.CommitTransaction();
            Console.WriteLine("\nDatabase write operations completed.");
        }

        // Regex derlemesi (CPU optimizasyonu)
        private static readonly Regex SafeStringRegex = new Regex(@"[^a-zA-Z0-9_]", RegexOptions.Compiled);

        private string EditStringForDatabase(string inputName)
        {
            if (string.IsNullOrEmpty(inputName)) return string.Empty;

            var sb = new StringBuilder(inputName);
            sb.Replace("ç", "c").Replace("Ç", "C")
              .Replace("ğ", "g").Replace("Ğ", "G")
              .Replace("ı", "i").Replace("İ", "I")
              .Replace("ö", "o").Replace("Ö", "O")
              .Replace("ş", "s").Replace("Ş", "S")
              .Replace("ü", "u").Replace("Ü", "U")
              .Replace(" ", "_");

            string sanitized = sb.ToString().Trim();
            return SafeStringRegex.Replace(sanitized, "");
        }
    }
}