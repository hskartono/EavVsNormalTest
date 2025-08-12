using NUnit.Framework;
using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace EavVsNormalTest
{
    public class Tests
    {
        private string _connStr = "Server=.;Database=TestDb;Trusted_Connection=True;TrustServerCertificate=True;";

        [SetUp]
        public void Setup()
        {
            InitDatabase();
        }

        private void InitDatabase()
        {
            using (var conn = new SqlConnection(_connStr))
            {
                conn.Open();
                var cmd = conn.CreateCommand();

                // Tabel normal
                cmd.CommandText = @"
                    IF OBJECT_ID('WorkOrders') IS NOT NULL DROP TABLE WorkOrders;
                    CREATE TABLE WorkOrders (
                        Id INT PRIMARY KEY IDENTITY,
                        Line VARCHAR(50),
                        TransDate DATE,
                        WorkNo VARCHAR(50),
                        CustomerName VARCHAR(100)
                    );";
                cmd.ExecuteNonQuery();

                // Tabel EAV
                cmd.CommandText = @"
                    IF OBJECT_ID('WorkOrders_EAV') IS NOT NULL DROP TABLE WorkOrders_EAV;
                    CREATE TABLE WorkOrders_EAV (
                        Id INT,
                        FieldName VARCHAR(50),
                        Value NVARCHAR(200)
                    );
                    CREATE INDEX IX_WorkOrdersEAV_FieldName ON WorkOrders_EAV(FieldName, Value);";
                cmd.ExecuteNonQuery();
            }
        }

        [Test]
        public void Compare_InsertAndRead_Performance()
        {
            var normalTime = InsertAndRead_NormalTable();
            var eavTime = InsertAndRead_EAVTable();

            TestContext.WriteLine($"Normal Table Time: {normalTime.TotalMilliseconds} ms");
            TestContext.WriteLine($"EAV Table Time: {eavTime.TotalMilliseconds} ms");

            Assert.Less(normalTime, eavTime, "Normal table seharusnya lebih cepat dari EAV");
        }

        private TimeSpan InsertAndRead_NormalTable()
        {
            var sw = Stopwatch.StartNew();

            using (var conn = new SqlConnection(_connStr))
            {
                conn.Open();

                for (int i = 0; i < 10000; i++)
                {
                    var cmd = new SqlCommand(@"
                        INSERT INTO WorkOrders (Line, TransDate, WorkNo, CustomerName)
                        VALUES (@Line, @TransDate, @WorkNo, @CustomerName)", conn);

                    cmd.Parameters.AddWithValue("@Line", "LineA");
                    cmd.Parameters.AddWithValue("@TransDate", DateTime.Today);
                    cmd.Parameters.AddWithValue("@WorkNo", $"WO{i:D4}");
                    cmd.Parameters.AddWithValue("@CustomerName", "PT Maju Jaya");
                    cmd.ExecuteNonQuery();
                }

                // Read all
                var readCmd = new SqlCommand("SELECT * FROM WorkOrders", conn);
                using (var rdr = readCmd.ExecuteReader())
                {
                    while (rdr.Read()) { }
                }
            }

            sw.Stop();
            return sw.Elapsed;
        }

        private TimeSpan InsertAndRead_EAVTable()
        {
            var sw = Stopwatch.StartNew();

            using (var conn = new SqlConnection(_connStr))
            {
                conn.Open();

                for (int i = 0; i < 10000; i++)
                {
                    var fields = new[]
                    {
                        new { Field = "Line", Value = "LineA" },
                        new { Field = "TransDate", Value = DateTime.Today.ToString("yyyy-MM-dd") },
                        new { Field = "WorkNo", Value = $"WO{i:D4}" },
                        new { Field = "CustomerName", Value = "PT Maju Jaya" }
                    };

                    foreach (var f in fields)
                    {
                        var cmd = new SqlCommand(@"
                            INSERT INTO WorkOrders_EAV (Id, FieldName, Value)
                            VALUES (@Id, @FieldName, @Value)", conn);

                        cmd.Parameters.AddWithValue("@Id", i + 1);
                        cmd.Parameters.AddWithValue("@FieldName", f.Field);
                        cmd.Parameters.AddWithValue("@Value", f.Value);
                        cmd.ExecuteNonQuery();
                    }
                }

                // Read all with pivot
                var readCmd = new SqlCommand(@"
                    SELECT Id,
                        MAX(CASE WHEN FieldName='Line' THEN Value END) AS Line,
                        MAX(CASE WHEN FieldName='TransDate' THEN Value END) AS TransDate,
                        MAX(CASE WHEN FieldName='WorkNo' THEN Value END) AS WorkNo,
                        MAX(CASE WHEN FieldName='CustomerName' THEN Value END) AS CustomerName
                    FROM WorkOrders_EAV
                    GROUP BY Id", conn);

                using (var rdr = readCmd.ExecuteReader())
                {
                    while (rdr.Read()) { }
                }
            }

            sw.Stop();
            return sw.Elapsed;
        }
    }
}