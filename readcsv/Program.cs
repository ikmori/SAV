using readcsv.Class;
using readcsv.Services;
using System.IO;

namespace readcsv
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string basePath = @"C:\Users\ellia\Desktop\Bigdata\Archivo CSV Análisis de Ventas-20250923";

            //costumers
            var customerService = new CsvReaderService<Customers>(Path.Combine(basePath, "customers.csv"));
            var customers = customerService.ReadRecords();
            foreach (var record in customers)
            {
                Console.WriteLine($"ID: {record.CustomerID}, Name: {record.FirstName} {record.LastName}, Country: {record.Country}");
            }


            //order details
            var orderDetailService = new CsvReaderService<OrderDetails>(Path.Combine(basePath, "order_details.csv"));
            var orderDetails = orderDetailService.ReadRecords();
            foreach (var record in orderDetails)
            {
                Console.WriteLine($"OrderID: {record.OrderID}, ProductID: {record.ProductID}, Quantity: {record.Quantity}");
            }

            //orders           
            var orderService = new CsvReaderService<Orders>(Path.Combine(basePath, "orders.csv"));
            var orders = orderService.ReadRecords();
            foreach (var record in orders)
            {
                Console.WriteLine($"OrderID: {record.OrderID}, CustomerID: {record.CustomerID}, Status: {record.Status}");
            }


            //products
            var productService = new CsvReaderService<Products>(Path.Combine(basePath, "products.csv"));
            var products = productService.ReadRecords();
            foreach (var record in products)
            {
                Console.WriteLine($"ProductID: {record.ProductID}, ProductName: {record.ProductName}, Price: {record.Price}");
            }
        }
    }
}