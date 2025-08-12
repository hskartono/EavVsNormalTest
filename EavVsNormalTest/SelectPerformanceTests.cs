using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EavVsNormalTest
{
    [TestFixture]
    public class SelectPerformanceTests
    {
        private string _connectionString = "Server=.;Database=TestDb;Trusted_Connection=True;TrustServerCertificate=True;Pooling=false";

        [Test]
        public void Compare_ReadNormalTable_Performance()
        {
            ClearSqlServerCache();
            var normalTime = Select_NormalizedTable_Performance();

            TestContext.WriteLine($"Normal Table Time: {normalTime.TotalMilliseconds} ms");
        }

        [Test]
        public void Compare_ReadEav_Performance()
        {
            ClearSqlServerCache();
            var normalTime = Select_NormalizedTable_Performance();

            TestContext.WriteLine($"Normal Table Time: {normalTime.TotalMilliseconds} ms");
        }

        public TimeSpan Select_NormalizedTable_Performance()
        {
            var stopwatch = Stopwatch.StartNew();

            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();

                // Simulasi query di tabel normal
                var sql = @"
                    SELECT Id, Line, TransDate, WorkNo, CustomerName
                    FROM WorkOrders
                    WHERE TransDate >= @FromDate AND TransDate < @ToDate
                      AND Line = @Line
                ";

                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@FromDate", new DateTime(2025, 8, 1));
                    cmd.Parameters.AddWithValue("@ToDate", new DateTime(2025, 9, 1));
                    cmd.Parameters.AddWithValue("@Line", "LINE-01");

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var id = reader["Id"];
                            var line = reader["Line"];
                            var transDate = reader["TransDate"];
                            var workNo = reader["WorkNo"];
                            var customerName = reader["CustomerName"];
                        }
                    }
                }
            }

            stopwatch.Stop();
            // Console.WriteLine($"Normalized Table SELECT Time: {stopwatch.ElapsedMilliseconds} ms");
            return stopwatch.Elapsed;
        }

        public TimeSpan Select_EAVTable_Performance()
        {
            var stopwatch = Stopwatch.StartNew();

            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();

                // Simulasi query EAV, perlu pivot/aggregate untuk dapatkan field
                var sql = @"
                    SELECT pvt.Id, pvt.Line, pvt.TransDate, pvt.WorkNo, pvt.CustomerName
                    FROM
                    (
                        SELECT Id, FieldName, Value
                        FROM WorkOrders_EAV
                        WHERE Id IN (
                            SELECT DISTINCT Id
                            FROM WorkOrders_EAV
                            WHERE FieldName = 'TransDate' AND Value >= @FromDateStr AND Value < @ToDateStr
                        )
                        AND Id IN (
                            SELECT DISTINCT Id
                            FROM WorkOrders_EAV
                            WHERE FieldName = 'Line' AND Value = @Line
                        )
                    ) AS src
                    PIVOT
                    (
                        MAX(Value) FOR FieldName IN ([Line], [TransDate], [WorkNo], [CustomerName])
                    ) AS pvt
                ";

                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@FromDateStr", "2025-08-01");
                    cmd.Parameters.AddWithValue("@ToDateStr", "2025-09-01");
                    cmd.Parameters.AddWithValue("@Line", "LINE-01");

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var id = reader["Id"];
                            var line = reader["Line"];
                            var transDate = reader["TransDate"];
                            var workNo = reader["WorkNo"];
                            var customerName = reader["CustomerName"];
                        }
                    }
                }
            }

            stopwatch.Stop();
            //Console.WriteLine($"EAV Table SELECT Time: {stopwatch.ElapsedMilliseconds} ms");
            return stopwatch.Elapsed;
        }

        private void ClearSqlServerCache()
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("DBCC FREEPROCCACHE; DBCC DROPCLEANBUFFERS;", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
