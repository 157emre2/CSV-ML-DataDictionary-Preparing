using CsvHelper;
using CsvHelper.Configuration;
using DocumentFormat.OpenXml;
using System.Globalization;
using System.IO.Compression;
using System.Text;

namespace CSV_ML_DataDictionary_Preparing
{
    public class DataDictionaryToCSV
    {
        private readonly ZipArchiveEntry _columnNames;
        private readonly List<ZipArchiveEntry> _dataDicFiles;
        private readonly List<ZipArchiveEntry> _inputCsv;
        private readonly string _delimiter;
        private readonly string _outputPath;
        private readonly CsvConfiguration _config;
        private readonly Dictionary<int, Dictionary<string, string>> _columnMappings;
        private readonly List<int> _columnToProccessIndex;
        private readonly Calendar _calendar;
        private static readonly string[] BinaryLookup = { "0", "1" };
        private Dictionary<DateTime, DateFeatures> _dateFeaturesLookup;
        private readonly Dictionary<string, string> _productCategoryMappings;
        private Dictionary<string, string> _categoryMappings;

        public DataDictionaryToCSV(
            List<ZipArchiveEntry> dataDicFiles,
            ZipArchiveEntry columnNames,
            string delimiter,
            string outputPath,
            List<ZipArchiveEntry> inputCsv,
            List<int> columnToProccessIndex)
        {
            _dataDicFiles = dataDicFiles;
            _columnNames = columnNames;
            _delimiter = delimiter;
            _outputPath = outputPath;
            _inputCsv = inputCsv;
            _columnMappings = new Dictionary<int, Dictionary<string, string>>();
            _config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = _delimiter,
                HasHeaderRecord = false,
                BufferSize = 65536,
                MissingFieldFound = null
            };
            _calendar = CultureInfo.CurrentCulture.Calendar;
            _columnToProccessIndex = columnToProccessIndex;
            _productCategoryMappings = new();
            _categoryMappings = new();

            InitializeDateFeatures(2015, 2030);
        }

        public struct DateFeatures
        {
            public string IsBlackFriday;
            public string IsHoliday;
            public string DaysBeforeHoliday;
            public string DaysAfterHoliday;
        }

        public void LoadDictionaries()
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading Dictionaries into RAM...");

            foreach (var entry in _dataDicFiles)
            {
                int colIndex = ExtractColumnIndexFromName(entry.Name);

                if (colIndex == -1)
                {
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Skipping dictionary file: {entry.Name} (Index parse error)");
                    continue;
                }

                if (_columnToProccessIndex.Count > 0 && !_columnToProccessIndex.Contains(colIndex))
                {
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Skipping dictionary for Column {colIndex + 1} as it's not in the processing list.");
                    continue;
                }

                var fileSize = entry.Length;
                var isHugeFile = fileSize > 100 * 1024 * 1024;

                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] -> Loading Column {colIndex + 1} (Size: {fileSize / 1024 / 1024} MB)...");

                var dict = new Dictionary<string, string>(StringComparer.Ordinal);

                var nextCatId = 1;

                using (var stream = entry.Open())
                using (var reader = new StreamReader(stream, Encoding.UTF8))
                using (var csv = new CsvReader(reader, _config))
                {
                    while (csv.Read())
                    {
                        try
                        {
                            var id = csv.GetField<string>(0);
                            var rawValue = csv.GetField<string>(1);

                            if (rawValue != null && !dict.ContainsKey(rawValue))
                            {
                                dict.Add(rawValue.Trim(), id);
                            }

                            if (colIndex == 477)
                            {
                                var catName = csv.GetField<string>(2)?.Trim() ?? "Uncategorized";

                                if (!_categoryMappings.TryGetValue(catName, out string catId))
                                {
                                    catId = nextCatId.ToString();
                                    _categoryMappings[catName.Trim()] = catId;
                                    nextCatId++;
                                }


                                if (!_productCategoryMappings.ContainsKey(rawValue))
                                {
                                    _productCategoryMappings.Add(rawValue.Trim(), catId);
                                }
                            }
                        }
                        catch { }
                    }
                }

                _columnMappings[colIndex] = dict;

                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] -> Loaded Column {colIndex + 1}. Items: {dict.Count:N0}");

                if (isHugeFile)
                {
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] -> Performing GC Cleanup for large file...");
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] All Dictionaries Loaded.");
        }

        public void ProcessAndSave()
        {
            // Output path bir klasör ise dosya adını ekleyelim
            string zipFilePath = _outputPath;
            if (!zipFilePath.EndsWith(".zip"))
            {
                zipFilePath = Path.Combine(_outputPath, "EncodedData.zip");
            }

            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Starting Encoding Process. Output: {zipFilePath}");

            // Direkt ZIP'e yazma akışı
            using var fs = new FileStream(zipFilePath, FileMode.Create);
            using var archive = new ZipArchive(fs, ZipArchiveMode.Create);

            var entry = archive.CreateEntry("EncodedData.csv", CompressionLevel.Fastest);

            using var entryStream = entry.Open();
            using var writer = new StreamWriter(entryStream, Encoding.UTF8, 65536);
            using var csvWriter = new CsvWriter(writer, _config);


            if (_columnNames != null)
            {
                using var readColumns = _columnNames.Open();
                using var reader = new StreamReader(readColumns);
                using var csvReader = new CsvReader(reader, _config);

                while (csvReader.Read())
                {
                    for (int i = 0; i < csvReader.Context.Parser.Count; i++)
                    {
                        var val = csvReader.GetField<string>(i);

                        csvWriter.WriteField(val);
                    }
                }

                csvWriter.NextRecord();
            }

            long totalRows = 0;

            foreach (var inputEntry in _inputCsv)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Processing Input File: {inputEntry.Name}");

                using var readStream = inputEntry.Open();
                using var reader = new StreamReader(readStream, Encoding.UTF8);
                using var csvReader = new CsvReader(reader, _config);

                while (csvReader.Read())
                {
                    totalRows++;

                    for (int i = 0; i < csvReader.Context.Parser.Count; i++)
                    {
                        if (_columnToProccessIndex.Contains(i))
                        {
                            var rawValue = csvReader.GetField<string>(i);
                            var encodedValue = rawValue;

                            if (_columnMappings.TryGetValue(i, out var mappingDict))
                            {
                                var searchKey = rawValue?.Trim() ?? "";
                                if (mappingDict.TryGetValue(searchKey, out var id))
                                {
                                    encodedValue = id;
                                }
                                else
                                {
                                    encodedValue = "-1";
                                }
                            }
                            else if (i == 7)
                            {
                                if (!string.IsNullOrEmpty(rawValue) && DateTime.TryParse(rawValue, new CultureInfo("tr-TR"), DateTimeStyles.None, out var date) && date.Year >= 2022)
                                {
                                    var dayOfWeek = (int)_calendar.GetDayOfWeek(date);
                                    var dayOfMonth = _calendar.GetDayOfMonth(date);
                                    var month = _calendar.GetMonth(date);
                                    var dateFeatures = _dateFeaturesLookup.GetValueOrDefault(date.Date);

                                    csvWriter.WriteField(date);
                                    csvWriter.WriteField(month);
                                    csvWriter.WriteField(dayOfWeek == 0 ? 7 : dayOfWeek);
                                    csvWriter.WriteField(BinaryLookup[dayOfWeek / 6]);
                                    csvWriter.WriteField(dayOfMonth < 10 ? "1" : "0");
                                    csvWriter.WriteField(dayOfMonth > 20 ? "1" : "0");
                                    csvWriter.WriteField(dateFeatures.IsHoliday);
                                    csvWriter.WriteField(dateFeatures.DaysBeforeHoliday);
                                }
                                break;
                            }
                            else if (decimal.TryParse(encodedValue, out decimal numValue))
                            {
                                encodedValue = numValue.ToString("F2", CultureInfo.InvariantCulture);
                            }

                            csvWriter.WriteField(encodedValue);

                            if (i == 477)
                            {
                                var catIdToWrite = "-1";

                                if (rawValue != null && _productCategoryMappings.TryGetValue(rawValue.Trim(), out var foundCatId))
                                {
                                    catIdToWrite = foundCatId;
                                }

                                csvWriter.WriteField(catIdToWrite);
                            }
                        }
                    }

                    csvWriter.NextRecord();

                    // İlerleme çubuğuna da saat ekledim, böylece hızını anlık görebilirsin.
                    if (totalRows % 100000 == 0)
                        Console.Write($"\r[{DateTime.Now:HH:mm:ss}] Encoded Rows: {totalRows:N0}");
                }
                // Bir dosya bittiğinde alt satıra geçsin ki loglar karışmasın
                Console.WriteLine();
            }

            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Process Completed! Saved to ZIP: {zipFilePath}");
        }

        private int ExtractColumnIndexFromName(string fileName)
        {
            try
            {
                var namePart = Path.GetFileNameWithoutExtension(fileName);
                var parts = namePart.Split('_');
                if (int.TryParse(parts[1], out int index))
                {
                    return index > 0 ? index - 1 : index;
                }
            }
            catch { }
            return -1;
        }

        private void InitializeDateFeatures(int startYear, int endYear)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Generating Holiday & Event Lookups...");
            _dateFeaturesLookup = new Dictionary<DateTime, DateFeatures>();

            // 1. TATİLLERİ BELİRLE (HashSet hızlı arama içindir)
            var holidaySet = new HashSet<DateTime>();
            var blackFridaySet = new HashSet<DateTime>();

            for (int year = startYear; year <= endYear; year++)
            {
                // --- Sabit Tatiller (Türkiye Örneği) ---
                holidaySet.Add(new DateTime(year, 1, 1));   // Yılbaşı
                holidaySet.Add(new DateTime(year, 4, 23));  // 23 Nisan
                holidaySet.Add(new DateTime(year, 5, 1));   // 1 Mayıs
                holidaySet.Add(new DateTime(year, 5, 19));  // 19 Mayıs
                holidaySet.Add(new DateTime(year, 7, 15));  // 15 Temmuz
                holidaySet.Add(new DateTime(year, 8, 30));  // 30 Ağustos
                holidaySet.Add(new DateTime(year, 10, 29)); // 29 Ekim

                // Dini bayramlar her yıl değiştiği için buraya manuel veya bir algoritma ile eklenmelidir.
                // Örn: holidaySet.Add(CalculateRamadanFeast(year)); 

                // --- Black Friday Hesabı (Kasım'ın 4. Perşembesini takip eden Cuma) ---
                // Basit kural: Kasım ayının 4. Cuması
                DateTime november1 = new DateTime(year, 11, 1);
                int daysUntilFriday = ((int)DayOfWeek.Friday - (int)november1.DayOfWeek + 7) % 7;
                DateTime firstFriday = november1.AddDays(daysUntilFriday);
                DateTime fourthFriday = firstFriday.AddDays(21); // 1. Cuma + 3 hafta
                blackFridaySet.Add(fourthFriday);
            }

            // 2. GÜNLER ARASI MESAFE HESABI (Performance Trick)
            // 2017-2026 arası her günü oluştur
            var allDates = new List<DateTime>();
            DateTime current = new DateTime(startYear, 1, 1);
            DateTime end = new DateTime(endYear, 12, 31);

            while (current <= end)
            {
                allDates.Add(current);
                current = current.AddDays(1);
            }

            // Tüm günler için boş bir yapı oluştur
            var tempFeatures = new Dictionary<DateTime, (bool isHol, bool isBF, int daysBefore, int daysAfter)>();
            foreach (var d in allDates)
            {
                tempFeatures[d] = (holidaySet.Contains(d), blackFridaySet.Contains(d), 999, 999);
            }

            // A) DaysAfterHoliday (Tatilden sonra kaç gün geçti?) -> İleri doğru tara
            int counter = 999;
            for (int i = 0; i < allDates.Count; i++)
            {
                var d = allDates[i];
                if (holidaySet.Contains(d)) counter = 0;
                else if (counter < 999) counter++;

                var f = tempFeatures[d];
                f.daysAfter = counter;
                tempFeatures[d] = f;
            }

            // B) DaysBeforeHoliday (Tatile kaç gün kaldı?) -> Geriye doğru tara
            counter = 999;
            for (int i = allDates.Count - 1; i >= 0; i--)
            {
                var d = allDates[i];
                if (holidaySet.Contains(d)) counter = 0;
                else if (counter < 999) counter++;

                var f = tempFeatures[d];
                f.daysBefore = counter;
                tempFeatures[d] = f;
            }

            // 3. SONUÇLARI STRING OLARAK SAKLA (CSV Yazarken hız kazanmak için)
            foreach (var kvp in tempFeatures)
            {
                _dateFeaturesLookup[kvp.Key] = new DateFeatures
                {
                    IsHoliday = kvp.Value.isHol ? "1" : "0",
                    IsBlackFriday = kvp.Value.isBF ? "1" : "0",
                    DaysAfterHoliday = kvp.Value.daysAfter > 365 ? "365" : kvp.Value.daysAfter.ToString(),
                    DaysBeforeHoliday = kvp.Value.daysBefore > 365 ? "365" : kvp.Value.daysBefore.ToString()
                };
            }
        }

        public void CategoryCsvMappingFile()
        {
            using (var writer = new StreamWriter(Path.Combine(_outputPath, "Categories.csv"), false, Encoding.UTF8))
            using (var csv = new CsvWriter(writer, _config))
            {
                foreach (var item in _categoryMappings)
                {
                    csv.WriteField(item.Value);
                    csv.WriteField(item.Key);
                    csv.NextRecord();
                }
            }
        }
    }
}