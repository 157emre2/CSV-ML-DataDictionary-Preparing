using CsvHelper;
using CsvHelper.Configuration;
using System.Formats.Asn1;
using System.Globalization;
using System.IO.Compression;
using System.Text;

namespace CSV_ML_DataDictionary_Preparing
{
    public class CsvtoDataDictionary
    {
        private readonly List<ZipArchiveEntry>? _csvFiles;
        private readonly FileInfo? _csvFile;
        private readonly List<int> _indexOfColumns;

        public CsvtoDataDictionary(List<ZipArchiveEntry>? csvFiles, FileInfo? csvFile)
        {
            _csvFiles = csvFiles != null && csvFiles.Count > 0 ? csvFiles : null;
            _csvFile = csvFile;
            _indexOfColumns = new();

            for (int i = 0; i < 3; i++)
            {
                _indexOfColumns.Add(i);
            }

            if (_csvFiles != null)
            {
                var deneme = BuildGlobalMappings(_csvFiles, _indexOfColumns);

                Console.WriteLine("----------------------------------------");

                Console.WriteLine("Data Dictionary Content:");
                Console.WriteLine("----------------------------------------");

                // 1. Dış Döngü: Ana Dictionary'nin elemanlarında gezer
                // (outerPair.Key bir int, outerPair.Value bir Dictionary<string, int>)
                foreach (var outerPair in deneme)
                {
                    int outerKey = outerPair.Key;
                    Dictionary<string, int> innerDictionary = outerPair.Value;

                    // Dış anahtarı yazdır (Bizim senaryomuzda bu, sütun indeksiydi)
                    Console.WriteLine($"Key (int): {outerKey}");

                    // İç sözlüğün dolu olup olmadığını kontrol et
                    if (innerDictionary == null || innerDictionary.Count == 0)
                    {
                        Console.WriteLine("  -> (The internal dictionary for this key is empty.)");
                    }
                    else
                    {
                        // 2. İç Döngü: İçteki Dictionary'nin elemanlarında gezer
                        // (innerPair.Key bir string, innerPair.Value bir int)
                        foreach (var innerPair in innerDictionary)
                        {
                            string innerKey = innerPair.Key;
                            int innerValue = innerPair.Value;

                            // İç anahtar/değer çiftini girintili olarak yazdır
                            Console.WriteLine($"  -> '{innerKey}' (string)  =>  {innerValue} (int)");
                        }
                    }
                    Console.WriteLine("----------------------------------------"); // Anahtarlar arası ayraç
                }
            }
        }

        /// <summary>
        /// 1. PAS: Sütun İndeksine Göre Eşleme Oluşturur
        /// </summary>
        private static Dictionary<int, Dictionary<string, int>> BuildGlobalMappings(List<ZipArchiveEntry> list, List<int> columnsToEncodeByIndex)
        {
            // [Sütun İndeksi] -> [Değer -> ID]
            var mappings = new Dictionary<int, Dictionary<string, int>>();
            var nextId = new Dictionary<int, int>();

            foreach (var colIndex in columnsToEncodeByIndex)
            {
                mappings[colIndex] = new Dictionary<string, int>();
                nextId[colIndex] = 1;
            }

            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                // EN ÖNEMLİ DEĞİŞİKLİK: Başlık satırı yok
                HasHeaderRecord = false,
                Delimiter = ";"
            };

            foreach (var entry in list)
            {
                Console.WriteLine($"  Scanning: {entry.FullName}");
                using (var stream = entry.Open())
                using (var reader = new StreamReader(stream, Encoding.UTF8))
                using (var csv = new CsvReader(reader, csvConfig))
                {
                    // 'dynamic' yerine satır satır manuel okuma
                    while (csv.Read())
                    {
                        foreach (var colIndex in columnsToEncodeByIndex)
                        {
                            try
                            {
                                // Veriyi isme göre değil, İNDEKSE göre al
                                string stringValue = csv.GetField<string>(colIndex);

                                if (!string.IsNullOrEmpty(stringValue) && !mappings[colIndex].ContainsKey(stringValue))
                                {
                                    mappings[colIndex][stringValue] = nextId[colIndex];
                                    nextId[colIndex]++;
                                }
                            }
                            catch (Exception ex)
                            {
                                // Sütun indeksi satırda bulunamadı (örn: bozuk satır)
                                Console.WriteLine($"Uyarı: {entry.FullName} - İndeks {colIndex} okunamadı. Satır: {csv.Context.Parser.RawRecord?.Trim()}");
                                Console.WriteLine(ex.ToString());
                            }
                        }
                    }
                }
            }
            return mappings;
        }
    }
}
