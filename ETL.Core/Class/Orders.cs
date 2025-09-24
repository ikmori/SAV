namespace ETL.Core.Class
{
    public class Orders
    {
        public int OrderID { get; set; }
        public int CustomerID { get; set; }
        public DateTime OrderDate { get; set; }
        public string? Status { get; set; }
    }
}
