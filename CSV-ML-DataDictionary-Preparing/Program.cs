using CSV_ML_DataDictionary_Preparing;
using System.IO.Compression;

Console.WriteLine("--- App Starting ---\n\n\n");

Console.WriteLine("This application is used for machine learning tasks to numerically convert CSV files and create a data dictionary database for the CSV to be numerically converted.\n\n\n");

Console.WriteLine("1 - Create a data dictionary for csv file/s.");
Console.WriteLine("2 - Transform your csv file to numerically with your data dictionary.");
Console.Write("Choose your action (1 or 2) ..:");

int.TryParse(Console.ReadLine(), out var entry);

switch (entry)
{
    case 1:
        Console.WriteLine("What is the path of the CSV file that will be create data dictionary? \n(If there are multiple CSV files, please collect them all in a zip file and write its path.) \n(If you have any CSV file for the columns, please add them to the zip file as columns.csv.)");
        var csvFilePath = Console.ReadLine();

        if (string.IsNullOrWhiteSpace(csvFilePath) || !File.Exists(csvFilePath))
        {
            Console.WriteLine("The provided path is invalid. Please restart the application and provide a valid path.");
            return;
        }

        Console.Write("What is the delimiter of your CSV file? (default is ';')");
        var delimiter = !string.IsNullOrEmpty(Console.ReadLine()) ? Console.ReadLine() : ";";

        Console.WriteLine("If you want ignore some columns. Please entry indexes of columns \n(You can split with ',')\n(First Column Index is 1)");
        var entriedColumnIndexes = Console.ReadLine();
        var ignoredList = new List<int>();

        if (!string.IsNullOrEmpty(entriedColumnIndexes))
            entriedColumnIndexes.Split(',').ToList().ForEach(x => ignoredList.Add(int.Parse(x)));

        Console.Write("Please entry data dictionary database (.db) output path (If you leave it blank, the default file path will be copied automatically.)..:");
        var outputPath = Console.ReadLine();

        if (csvFilePath.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
            using (var archive = ZipFile.OpenRead(csvFilePath))
            {
                var csvFileOnZipList = new List<ZipArchiveEntry>();
                Console.WriteLine("This csv files on your zip..:");
                int i = 0;
                foreach (var csvFileonZip in archive.Entries.Where(e => e.FullName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)))
                {
                    i++;
                    Console.WriteLine($"{i} - {csvFileonZip.Name}");
                    csvFileOnZipList.Add(csvFileonZip);
                }
                Console.WriteLine($"Total File Founds..: {i}");
                new CsvtoDataDictionary(csvFileOnZipList, null, delimiter, ignoredList, outputPath);
            }
        else if (csvFilePath.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            new CsvtoDataDictionary(null, new(csvFilePath), delimiter, ignoredList, outputPath);
        else
            Console.WriteLine("You must give zip or csv file. Please try again.");
        break;
    case 2:
        // 1. AYARLAR: Dosya yollarını burada belirle
        // Sözlüklerin olduğu ZIP (Step 1 çıktısı)
        Console.Write("Where is Data Dictionaries Zip..: ");
        var dictionariesZipPath = Console.ReadLine();

        // 200 GB'lık ham verinin olduğu ZIP (Parçalı CSV'ler)
        Console.Write("Where is Input Raw Data Zip..: ");
        var inputRawDataZipPath = Console.ReadLine();

        // Çıktının nereye kaydedileceği (ZIP olarak oluşacak)
        Console.Write("Where is Output Folder..: ");
        var outputZipPath = Console.ReadLine();

        // CSV Ayırıcı (Noktalı virgül mü, virgül mü? Önemli!)
        Console.Write("What is the delimiter of your CSV file? (default is ';')");
        var delimiterSec = !string.IsNullOrEmpty(Console.ReadLine()) ? Console.ReadLine() : ";";

        //İşlenecek sütun indeksleri (0 tabanlı).
        var columnsToProcess = new List<int> { 7, 460, 477, 482, 489, 491 };

        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Uygulama Başlatılıyor...");

        try
        {
            // 2. ZIP DOSYALARINI AÇ (Okuma Modunda)
            // 'using' bloğu bitene kadar bu dosyalar açık kalır, böylece entry'lere erişebiliriz.
            using var dictArchive = ZipFile.OpenRead(dictionariesZipPath);
            using var inputArchive = ZipFile.OpenRead(inputRawDataZipPath);

            // 3. Dosyaları Listele (Filtreleme yapabilirsin)
            // Sözlük ZIP'indeki tüm dosyaları alıyoruz (veya isme göre filtrele)
            var dictionaryEntries = dictArchive.Entries
                                    .Where(x => !string.IsNullOrEmpty(x.Name) && x.Name.EndsWith(".csv"))
                                    .ToList();

            // Ham veri ZIP'indeki CSV dosyalarını alıyoruz
            var inputEntries = inputArchive.Entries
                                    .Where(x => !string.IsNullOrEmpty(x.Name) && x.Name.EndsWith(".csv") && !x.Name.Contains("columns.csv", StringComparison.OrdinalIgnoreCase))
                                    .OrderBy(x => x.Name) // Sıralı işlemek her zaman iyidir
                                    .ToList();

            var columnNamesEntry = inputArchive.Entries
                                    .FirstOrDefault(x => x.Name.Equals("columns.csv", StringComparison.OrdinalIgnoreCase));

            Console.WriteLine($"-> Bulunan Sözlük Dosyası: {dictionaryEntries.Count}");
            Console.WriteLine($"-> Bulunan Ham Veri Dosyası: {inputEntries.Count}");

            // 4. SINIFI BAŞLAT
            // columnNames kısmına şimdilik null geçiyoruz (Header yazmıyoruz)
            var converter = new DataDictionaryToCSV(
                dictionaryEntries,
                columnNamesEntry,
                delimiterSec,
                outputZipPath,
                inputEntries,
                columnsToProcess
            );

            // 5. ADIM A: Sözlükleri RAM'e Yükle
            // Bu aşamada RAM kullanımı artacak (8-16 GB arası olabilir)
            converter.LoadDictionaries();

            // 6. ADIM B: İşlemi Başlat
            // Bu aşama CPU ve Disk yoğundur, uzun sürecektir.
            converter.ProcessAndSave();

            converter.CategoryCsvMappingFile();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n!!! KRİTİK HATA !!!\n{ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }

        Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss}] Program Sonlandı.");
        // Konsol hemen kapanmasın diye:
        Console.ReadLine();
        break;
    default:
        Console.WriteLine("Invalid entry. Restart the application and try again.");
        break;
}
