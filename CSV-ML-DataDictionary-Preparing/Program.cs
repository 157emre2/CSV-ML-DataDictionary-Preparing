using CSV_ML_DataDictionary_Preparing;
using System.IO.Compression;

Console.WriteLine("--- App Starting ---\n\n\n");

Console.WriteLine("This application is used for machine learning tasks to numerically convert CSV files and create a data dictionary for the CSV to be numerically converted.\n\n\n");

Console.WriteLine("1 - Create a data dictionary for csv file/s.");
Console.WriteLine("2 - Transform your csv file to numerically with your data dictionary.");
Console.Write("Choose your action (1 or 2) ..:");

int.TryParse(Console.ReadLine(), out var entry);

switch (entry)
{
    case 1:
        Console.WriteLine("What is the path of the CSV file that will be create data dictionary? \n(If there are multiple CSV files, please collect them all in a zip file and write its path.) \n(If you have any CSV file for the columns, please add them to the zip file as columns.csv.)");
        var csvFilePath = Console.ReadLine();

        Console.Write("What is the delimiter of your CSV file? (default is ';')");
        var delimiter = !string.IsNullOrEmpty(Console.ReadLine()) ? Console.ReadLine() : ";";

        if (string.IsNullOrWhiteSpace(csvFilePath) || !File.Exists(csvFilePath))
        {
            Console.WriteLine("The provided path is invalid. Please restart the application and provide a valid path.");
            return;
        }

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
                new CsvtoDataDictionary(csvFileOnZipList, null, delimiter);
            }
        else if (csvFilePath.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            new CsvtoDataDictionary(null, new(csvFilePath), delimiter);
        else
            Console.WriteLine("You must give zip or csv file. Please try again.");
        return;
    case 2:
        Console.WriteLine("deneme");
        break;
    default:
        Console.WriteLine("Invalid entry. Restart the application and try again.");
        break;
}
