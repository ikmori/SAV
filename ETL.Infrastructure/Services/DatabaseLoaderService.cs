using ETL.Core;
using ETL.Core.Class;
using Microsoft.Data.SqlClient;
using System.Data;

namespace ETL.Infrastructure
{
    public class DatabaseLoaderService
    {
        private readonly string _connectionString;

        public DatabaseLoaderService(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void BulkLoadData(List<Customers> customers, List<Products> products, List<Orders> orders, DataTable validOrderDetailsTable)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                Console.WriteLine("Conexión a la base de datos establecida.");

                Console.WriteLine("Cargando Clientes...");
                BulkInsertCustomers(customers, connection);

                Console.WriteLine("Cargando Productos...");
                BulkInsertProducts(products, connection);

                Console.WriteLine("Cargando Ventas...");
                BulkInsertOrders(orders, connection);

                Console.WriteLine("Cargando Detalles de Venta...");
                BulkInsertOrderDetails(validOrderDetailsTable, connection);

                Console.WriteLine("Carga de datos finalizada con éxito.");
            }
        }

        private void BulkInsertOrderDetails(DataTable orderDetailsTable, SqlConnection connection)
        {
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "DetallesVenta";


                bulkCopy.ColumnMappings.Add("IdVenta", "IdVenta");
                bulkCopy.ColumnMappings.Add("IdProducto", "IdProducto");
                bulkCopy.ColumnMappings.Add("Cantidad", "Cantidad");
                bulkCopy.ColumnMappings.Add("PrecioUnitario", "PrecioUnitario");
                bulkCopy.ColumnMappings.Add("TotalLinea", "TotalLinea");

                bulkCopy.WriteToServer(orderDetailsTable);
            }
        }


        private void BulkInsertCustomers(List<Customers> customers, SqlConnection connection)
        {
            var dt = new DataTable();
            dt.Columns.Add("IdCliente", typeof(int));
            dt.Columns.Add("FirstName", typeof(string));
            dt.Columns.Add("LastName", typeof(string));
            dt.Columns.Add("Email", typeof(string));
            dt.Columns.Add("Phone", typeof(string));
            dt.Columns.Add("City", typeof(string));
            dt.Columns.Add("Country", typeof(string));

            foreach (var cust in customers)
            {
                dt.Rows.Add(cust.CustomerID, cust.FirstName, cust.LastName, cust.Email, cust.Phone, cust.City, cust.Country);
            }

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "Clientes";
                bulkCopy.WriteToServer(dt);
            }
        }

        private void BulkInsertProducts(List<Products> products, SqlConnection connection)
        {
            var dt = new DataTable();
            dt.Columns.Add("IdProducto", typeof(int));
            dt.Columns.Add("NombreProducto", typeof(string));
            dt.Columns.Add("Categoria", typeof(string));
            dt.Columns.Add("Precio", typeof(decimal));
            dt.Columns.Add("Stock", typeof(int));

            foreach (var prod in products)
            {
                dt.Rows.Add(prod.ProductID, prod.ProductName, prod.Category, prod.Price, prod.Stock);
            }

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "Productos";
                bulkCopy.WriteToServer(dt);
            }
        }

        private void BulkInsertOrders(List<Orders> orders, SqlConnection connection)
        {
            var dt = new DataTable();
            dt.Columns.Add("IdVenta", typeof(int));
            dt.Columns.Add("IdCliente", typeof(int));
            dt.Columns.Add("FechaVenta", typeof(DateTime));
            dt.Columns.Add("Status", typeof(string));

            foreach (var order in orders)
            {
                dt.Rows.Add(order.OrderID, order.CustomerID, order.OrderDate, order.Status);
            }

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "Ventas";
                bulkCopy.WriteToServer(dt);
            }
        }
    }
}