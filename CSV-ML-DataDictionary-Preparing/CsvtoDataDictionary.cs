using ClosedXML.Excel;
using CsvHelper;
using CsvHelper.Configuration;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Formats.Asn1;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using TextCopy;

namespace CSV_ML_DataDictionary_Preparing
{
    public class CsvtoDataDictionary
    {
        private readonly List<ZipArchiveEntry>? _csvFiles;
        private readonly FileInfo? _csvFile;
        private readonly Dictionary<int, string?> _columns;
        private readonly string _delimiter;
        private readonly CsvConfiguration _csvConfiguration;
        private readonly List<int> _ignoredColumnIndexes;
        private string? _dbPath;

        public CsvtoDataDictionary(List<ZipArchiveEntry>? csvFiles, FileInfo? csvFile, string delimiter, List<int>? ignoredColumnIndexes, string? outputhPath)
        {
            _csvFiles = csvFiles != null && csvFiles.Count > 0 ? csvFiles : null;
            _csvFile = csvFile;
            _delimiter = delimiter;
            _csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false,
                Delimiter = _delimiter
            };
            _columns = new();
            _ignoredColumnIndexes = ignoredColumnIndexes;

            if (_csvFiles != null)
            {
                if (_csvFiles.Any(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase)))
                {
                    var temp = _csvFiles.First(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase));
                    _csvFiles.Remove(temp);

                    Console.WriteLine("Reading columns.csv for column indexes...");
                    using (var stream = temp.Open())
                    using (var reader = new StreamReader(stream, Encoding.UTF8))
                    using (var csv = new CsvReader(reader, _csvConfiguration))
                        while (csv.Read())
                        {
                            foreach (var colIndex in Enumerable.Range(0, csv.ColumnCount))
                                try
                                {
                                    var colValue = csv.GetField<string>(colIndex);
                                    if (!string.IsNullOrEmpty(colValue)) _columns.Add(colIndex, EditStringForDatabase(colValue));
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Warning: columns.csv - Could not read index {colIndex}. Row: {csv.Context.Parser.RawRecord?.Trim()}");
                                    Console.WriteLine(ex.ToString());
                                    throw;
                                }
                        }
                }
                else
                {
                    Console.Write("Didn't found any columns.csv. So, Can you entry your number of columns..:");
                    var isNumber = int.TryParse(Console.ReadLine(), out var result);

                    if (!isNumber || result <= 0)
                    {
                        Console.WriteLine("Invalid number of columns. Please restart the application and provide a valid number.");
                        return;
                    }
                    else
                        _columns = Enumerable.Range(0, result).ToDictionary(i => i, i => (string?)null);
                }

                BuildGlobalMappings(outputhPath, true);
            }

            if (_csvFile != null)
            {
                Console.Write("Can you entry your number of columns..:");
                var isNumber = int.TryParse(Console.ReadLine(), out var result);

                if (!isNumber || result <= 0)
                {
                    Console.WriteLine("Invalid number of columns. Please restart the application and provide a valid number.");
                    return;
                }
                else
                    _columns = Enumerable.Range(0, result).ToDictionary(i => i, i => (string?)null);

                BuildGlobalMappings(outputhPath, false);
            }


            Console.WriteLine($"\tAll operations completed successfully aand your database location copied..: {_dbPath}");
            ClipboardService.SetText(_dbPath ?? string.Empty);
            Console.WriteLine("\n\n--- App Ended ---");
        }

        private string EditStringForDatabase(string inputName)
        {
            string text = inputName;
            text = text.Replace("ç", "c").Replace("Ç", "C");
            text = text.Replace("ğ", "g").Replace("Ğ", "G");
            text = text.Replace("ı", "i");
            text = text.Replace("İ", "I");
            text = text.Replace("ö", "o").Replace("Ö", "O");
            text = text.Replace("ş", "s").Replace("Ş", "S");
            text = text.Replace("ü", "u").Replace("Ü", "U");

            string sanitized = text.Trim().Replace(" ", "_");

            sanitized = Regex.Replace(sanitized, @"[^a-zA-Z0-9_]", "");

            return sanitized;
        }

        /// <summary>
        /// Veri Sözlüğü oluşturmak için zip içerisinde bulunan tüm CSV dosyalarını tarar ve belirtilen sütunlardaki benzersiz değerleri toplar.
        /// </summary>
        private void BuildGlobalMappings(string? outputPath, bool isZipFile)
        {
            using (var sqlLiteCon = new SqlLiteDataDictionary(outputPath))
            { 
                sqlLiteCon.CreateAllTables(_columns.Where(x => !_ignoredColumnIndexes.Any(a => a == x.Key + 1)).ToDictionary());

                _dbPath = sqlLiteCon.DatabasePath;

                if (isZipFile)
                {
                    foreach (var entry in _csvFiles)
                    {
                        Console.WriteLine($"\tScanning: {entry.FullName} -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");

                        sqlLiteCon.BeginTransaction();

                        using (var stream = entry.Open())
                            BuildMappings(stream, sqlLiteCon, entry.Name);

                        sqlLiteCon.CommitTransaction();
                        Console.WriteLine($"\t\tCompleted and saved: {entry.FullName} -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
                    }
                }else
                {
                    Console.WriteLine($"\tScanning: {_csvFile.FullName} -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
                    sqlLiteCon.BeginTransaction();

                    using (var stream = _csvFile.OpenRead())
                        BuildMappings(stream, sqlLiteCon, _csvFile.Name);   

                    sqlLiteCon.CommitTransaction();
                    Console.WriteLine($"\t\tCompleted and saved: {_csvFile.FullName} -- {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
                }
            }
        }

        /// <summary>
        /// CSV akışını okuyarak belirtilen sütunlardaki benzersiz değerleri toplar ve eşlemeleri oluşturur.
        /// </summary>
        private void BuildMappings(Stream stream, SqlLiteDataDictionary sqliteDic, string csvName)
        {
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            using (var csv = new CsvReader(reader, _csvConfiguration))
                while(csv.Read())
                    foreach (var colIndex in _columns)
                        try
                        {
                            var colValue = csv.GetField<string>(colIndex.Key);
                            if (_ignoredColumnIndexes.Any(x => x == colIndex.Key + 1)) continue;

                            if (!string.IsNullOrEmpty(colValue))
                                sqliteDic.InsertUniqueValue(colIndex.Key + 1, colIndex.Value, colValue);

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Warning: {csvName} - Could not read index {colIndex}. Row: {csv.Context.Parser.RawRecord?.Trim()}");
                            Console.WriteLine(ex.ToString());
                            throw;
                        }
                
        }
    }
}
