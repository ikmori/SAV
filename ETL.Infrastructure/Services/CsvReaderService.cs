using CsvHelper;
using System.Globalization;

namespace ETL.Infrastructure.Services
{
    public class CsvReaderService<T>
    {
        private readonly string _filePath;

        public CsvReaderService(string filePath)
        {
            _filePath = filePath;
        }

        public List<T> ReadRecords()
        {
            try
            {
                if (!File.Exists(_filePath))
                {
                    Console.WriteLine("El archivo especificado no existe.");
                    return new List<T>(); 
                }

                using (var reader = new StreamReader(_filePath))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    var records = csv.GetRecords<T>().ToList();
                    return records;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ocurrió un error al leer el archivo CSV: {ex.Message}");
                return new List<T>();
            }
        }
    }
}