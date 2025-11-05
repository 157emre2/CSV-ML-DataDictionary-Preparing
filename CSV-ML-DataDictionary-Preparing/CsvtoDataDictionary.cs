using ClosedXML.Excel;
using CsvHelper;
using CsvHelper.Configuration;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Formats.Asn1;
using System.Globalization;
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
        private readonly List<int> _indexOfColumns;
        private readonly string _delimiter;
        private readonly List<string> _columns;
        private readonly CsvConfiguration _csvConfiguration;
        private readonly bool _haveColumnsName = false;
        private readonly Dictionary<int, Dictionary<string, int>> _dataDictionaryList;
        private readonly List<int> _ignoredColumnIndexes;

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
            _indexOfColumns = new();
            _columns = new();
            _ignoredColumnIndexes = ignoredColumnIndexes;

            if (_csvFiles != null)
            {
                if (_csvFiles.Any(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase)))
                {
                    var temp = _csvFiles.First(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase));
                    _haveColumnsName = true;
                    _csvFiles.Remove(temp);

                    Console.WriteLine("Reading columns.csv for column indexes...");
                    using (var stream = temp.Open())
                    using (var reader = new StreamReader(stream, Encoding.UTF8))
                    using (var csv = new CsvReader(reader, _csvConfiguration))
                        while (csv.Read())
                        {
                            _indexOfColumns = Enumerable.Range(0, csv.ColumnCount).ToList();

                            foreach (var colIndex in _indexOfColumns)
                                try
                                {
                                    var stringValue = csv.GetField<string>(colIndex);
                                    if (!string.IsNullOrEmpty(stringValue)) _columns.Add(stringValue);
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
                        _indexOfColumns = Enumerable.Range(0, result).ToList();
                }
                _dataDictionaryList = BuildGlobalMappings(true);
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
                    _indexOfColumns = Enumerable.Range(0, result).ToList();

                _dataDictionaryList = BuildGlobalMappings(false);
            }


            if (_dataDictionaryList != null)
                CreateExcelWithMappings(_dataDictionaryList, _haveColumnsName, outputhPath);
        }

        /// <summary>
        /// Veri Sözlüğü oluşturmak için zip içerisinde bulunan tüm CSV dosyalarını tarar ve belirtilen sütunlardaki benzersiz değerleri toplar.
        /// </summary>
        private Dictionary<int, Dictionary<string, int>> BuildGlobalMappings(bool isZipFile)
        {
            var mappings = new Dictionary<int, Dictionary<string, int>>();
            var nextId = new Dictionary<int, int>();

            foreach (var colIndex in _indexOfColumns)
            {
                mappings[colIndex] = new Dictionary<string, int>();
                nextId[colIndex] = 1;
            }

            if (isZipFile)
            {
                foreach (var entry in _csvFiles)
                {
                    Console.WriteLine($"  Scanning: {entry.FullName}");
                    using (var stream = entry.Open())
                        BuildMappings(stream, mappings, nextId, entry.FullName);
                }
            }
            else
            {
                using (var stream = _csvFile.OpenRead())
                    BuildMappings(stream, mappings, nextId, _csvFile.FullName);
            }
            return mappings;
        }

        /// <summary>
        /// Oluşturulan veri sözlüğü eşlemelerini içeren bir Excel dosyası oluşturur.
        /// </summary>
        private void CreateExcelWithMappings(Dictionary<int, Dictionary<string, int>> mappings, bool haveColumnsName, string? outputPath)
        {
            using (var workbook = new XLWorkbook())
            {
                foreach (var firstDic in mappings)
                {
                    var columnNameForSheet = $"Column_{firstDic.Key + 1}";
                    var columnNameForTable = haveColumnsName ? $"Column {firstDic.Key + 1} => {_columns[firstDic.Key]}" : $"Column {firstDic.Key + 1}";

                    var invalidCharsPattern = @"[\\/*?:\[\]]";
                    var cleanSheetName = Regex.Replace(columnNameForSheet, invalidCharsPattern, "");

                    if (cleanSheetName.Length > 31)
                    {
                        cleanSheetName = cleanSheetName.Substring(0, 31);
                    }

                    var ws = workbook.Worksheets.Add(cleanSheetName);

                    ws.Cell(1, 1).Value = columnNameForTable;

                    var mergeRange = ws.Range(1, 1, 1, 2);
                    mergeRange.Merge();
                    mergeRange.Style.Font.Bold = true;
                    mergeRange.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                    mergeRange.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
                    mergeRange.Style.Border.OutsideBorder = XLBorderStyleValues.Medium;

                    if (!_ignoredColumnIndexes.Any(x => x == firstDic.Key + 1))
                    {
                        ws.Cell(2, 1).Value = "Key";
                        ws.Cell(2, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                        ws.Cell(2, 1).Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
                        ws.Cell(2, 1).Style.Border.InsideBorder = XLBorderStyleValues.Thin;

                        ws.Cell(2, 2).Value = "Value";
                        ws.Cell(2, 2).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                        ws.Cell(2, 2).Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
                        ws.Cell(2, 2).Style.Border.InsideBorder = XLBorderStyleValues.Thin;
                    }else
                    {
                        ws.Cell(2, 1).Value = "This column is ignored.";
                        var range = ws.Range(2, 1, 2, 2);
                        range.Merge();
                        range.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                        range.Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
                    }

                        var row = 3;

                    foreach (var secondDic in firstDic.Value)
                    {
                        ws.Cell(row, 1).Value = secondDic.Key;
                        ws.Cell(row, 2).Value = secondDic.Value;

                        ws.Cell(row, 1).Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
                        ws.Cell(row, 1).Style.Border.InsideBorder = XLBorderStyleValues.Thin;

                        ws.Cell(row, 2).Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
                        ws.Cell(row, 2).Style.Border.InsideBorder = XLBorderStyleValues.Thin;

                        row++;
                    }

                    ws.Columns().AdjustToContents();
                }

                var filePath = string.IsNullOrEmpty(outputPath) ? "DataDictionary.xlsx" : Path.Combine(outputPath, "DataDictionary.xlsx");
                workbook.SaveAs(filePath);

                Console.WriteLine($"Data dictionary Excel file created: {Path.GetFullPath(filePath)} \nExcel File Path Copied.");
                ClipboardService.SetText(Path.GetFullPath(filePath));
            }
        }

        /// <summary>
        /// CSV akışını okuyarak belirtilen sütunlardaki benzersiz değerleri toplar ve eşlemeleri oluşturur.
        /// </summary>
        private void BuildMappings(Stream stream, Dictionary<int, Dictionary<string, int>> mappings, Dictionary<int, int> nextId, string csvName)
        {
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            using (var csv = new CsvReader(reader, _csvConfiguration))
                while (csv.Read())
                    foreach (var colIndex in _indexOfColumns)
                        try
                        {
                            if (!_ignoredColumnIndexes.Any(x => x == colIndex + 1))
                            {
                                var stringValue = csv.GetField<string>(colIndex);

                                if (!string.IsNullOrEmpty(stringValue) && !mappings[colIndex].ContainsKey(stringValue))
                                {
                                    mappings[colIndex][stringValue] = nextId[colIndex];
                                    nextId[colIndex]++;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Warning: {csvName} - Index {colIndex} could not be read. Row: {csv.Context.Parser.RawRecord?.Trim()}");
                            Console.WriteLine(ex.ToString());
                            throw;
                        }
        }
    }
}
