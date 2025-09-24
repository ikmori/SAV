
using ETL.Core.Class;
using ETL.Infrastructure;
using ETL.Infrastructure.Services;
using System.Data;
using System.Diagnostics;

public class Program
{
    static void Main(string[] args)
    {
        var stopwatch = Stopwatch.StartNew();

        //configuracion
        string basePath = @"C:\Users\ellia\Desktop\Bigdata\Archivo CSV Análisis de Ventas-20250923";
        string connectionString = "Server=ELLIAM-PC;Database=SAVDB;Trusted_Connection=True;TrustServerCertificate=True;";

        try
        {
            //lectura
            Console.WriteLine("--- Iniciando Fase de Extracción (Lectura de CSVs) ---");

            var customerService = new CsvReaderService<Customers>(Path.Combine(basePath, "customers.csv"));
            var allCustomers = customerService.ReadRecords();

            var productService = new CsvReaderService<Products>(Path.Combine(basePath, "products.csv"));
            var allProducts = productService.ReadRecords();

            var orderService = new CsvReaderService<Orders>(Path.Combine(basePath, "orders.csv"));
            var allOrders = orderService.ReadRecords();

            var orderDetailService = new CsvReaderService<OrderDetails>(Path.Combine(basePath, "order_details.csv"));
            var allOrderDetails = orderDetailService.ReadRecords();

            Console.WriteLine($"Lectura completada: {allCustomers.Count} clientes, {allProducts.Count} productos, {allOrders.Count} ventas, {allOrderDetails.Count} detalles.");

            //transformacion
            Console.WriteLine("\n--- Iniciando Fase de Transformación (Limpieza y Validación) ---");

            //limpiar duplicados por Email, tomando el primero que aparece.
            var cleanedCustomers = allCustomers
                .Where(c => !string.IsNullOrEmpty(c.Email)) 
                .GroupBy(c => c.Email.ToLower()) 
                .Select(g => g.First())          
                .ToList();

            //limpiar productos duplicados por ID
            var cleanedProducts = allProducts.GroupBy(p => p.ProductID).Select(g => g.First()).ToList();

            //validar integridad referencial de las Ventas
            var customerIds = cleanedCustomers.Select(c => c.CustomerID).ToHashSet();
            var validOrders = allOrders.Where(o => customerIds.Contains(o.CustomerID)).ToList();

            //validar y enriquecer Detalles de Venta y prepararlos en un DataTable
            var productDict = cleanedProducts.ToDictionary(p => p.ProductID, p => p.Price);
            var orderIds = validOrders.Select(o => o.OrderID).ToHashSet();

            //creamos un DataTable que coincide con la estructura de la tabla DetallesVenta
            var validOrderDetailsTable = new DataTable();
            validOrderDetailsTable.Columns.Add("IdVenta", typeof(int));
            validOrderDetailsTable.Columns.Add("IdProducto", typeof(int));
            validOrderDetailsTable.Columns.Add("Cantidad", typeof(int));
            validOrderDetailsTable.Columns.Add("PrecioUnitario", typeof(decimal));
            validOrderDetailsTable.Columns.Add("TotalLinea", typeof(decimal));

            foreach (var detail in allOrderDetails)
            {
                if (orderIds.Contains(detail.OrderID) && productDict.TryGetValue(detail.ProductID, out decimal unitPrice))
                {
                    decimal totalRecalculado = detail.Quantity * unitPrice;

                    
                    validOrderDetailsTable.Rows.Add(detail.OrderID, detail.ProductID, detail.Quantity, unitPrice, totalRecalculado);
                }
            }

            Console.WriteLine($"Transformación completada:");
            Console.WriteLine($" - Clientes: {cleanedCustomers.Count} (descartados {allCustomers.Count - cleanedCustomers.Count} por duplicados o email nulo)");
            Console.WriteLine($" - Productos: {cleanedProducts.Count} (descartados {allProducts.Count - cleanedProducts.Count} duplicados)");
            Console.WriteLine($" - Ventas: {validOrders.Count} (descartadas {allOrders.Count - validOrders.Count} por cliente inválido)");
            Console.WriteLine($" - Detalles de Venta: {validOrderDetailsTable.Rows.Count} (descartados {allOrderDetails.Count - validOrderDetailsTable.Rows.Count} por venta o producto inválido)");

            //cargar
            Console.WriteLine("\n--- Iniciando Fase de Carga (Insertar en Base de Datos) ---");
            var dbLoader = new DatabaseLoaderService(connectionString);
            
            dbLoader.BulkLoadData(cleanedCustomers, cleanedProducts, validOrders, validOrderDetailsTable);

        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"\nERROR en el proceso ETL: {ex.Message}");
            
            if (ex.InnerException != null)
            {
                Console.WriteLine($"   Detalle: {ex.InnerException.Message}");
            }
            Console.ResetColor();
        }
        finally
        {
            stopwatch.Stop();
            Console.WriteLine($"\nProceso ETL finalizado en: {stopwatch.Elapsed.TotalSeconds:F2} segundos.");
        }
    }
}