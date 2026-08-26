"""A small but realistic Spark SQL pipeline used to generate event-log data.

Produces several SQL executions (with plans, shuffles, aggregations and a
broadcast-free join) so every SQL-oriented MCP tool has something to read.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("SalesAnalyticsPipeline")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

out_dir = sys.argv[1] if len(sys.argv) > 1 else "/tmp/shs/output/sales"

# --- SQL execution 1: build the orders fact table -------------------------
orders = (
    spark.range(0, 400000, 1, 8)
    .withColumn("customer_id", (F.col("id") % 5000).cast("int"))
    .withColumn("product_id", (F.col("id") % 300).cast("int"))
    .withColumn("quantity", (F.col("id") % 7 + 1).cast("int"))
    .withColumn("unit_price", (F.col("id") % 97 + 3).cast("double"))
    .withColumn("amount", F.col("quantity") * F.col("unit_price"))
    .withColumn("region", F.concat(F.lit("region-"), (F.col("id") % 4).cast("string")))
)
orders.createOrReplaceTempView("orders")

customers = (
    spark.range(0, 5000, 1, 4)
    .withColumnRenamed("id", "customer_id")
    .withColumn("segment", F.concat(F.lit("segment-"), (F.col("customer_id") % 5).cast("string")))
    .withColumn("country", F.concat(F.lit("country-"), (F.col("customer_id") % 12).cast("string")))
)
customers.createOrReplaceTempView("customers")

print("orders rows:", orders.count())
print("customers rows:", customers.count())

# --- SQL execution 2: shuffle join + aggregation --------------------------
revenue_by_segment = spark.sql(
    """
    SELECT c.segment,
           o.region,
           COUNT(*)            AS order_count,
           SUM(o.amount)       AS revenue,
           AVG(o.unit_price)   AS avg_price
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.segment, o.region
    ORDER BY revenue DESC
    """
)
revenue_by_segment.show(20, truncate=False)

# --- SQL execution 3: window function over a wide shuffle -----------------
top_products = spark.sql(
    """
    SELECT product_id, region, revenue, rank
    FROM (
      SELECT product_id,
             region,
             SUM(amount) AS revenue,
             RANK() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) AS rank
      FROM orders
      GROUP BY product_id, region
    ) ranked
    WHERE rank <= 5
    ORDER BY region, rank
    """
)
top_products.show(30, truncate=False)

# --- SQL execution 4: write results out -----------------------------------
revenue_by_segment.write.mode("overwrite").parquet(f"{out_dir}/revenue_by_segment")
top_products.write.mode("overwrite").parquet(f"{out_dir}/top_products")

print("done")
spark.stop()
