"""A Spark job that fails inside a task, to produce task-level error messages.

Used to exercise `list_stage_task_failures` and the failure-oriented tools.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

spark = (
    SparkSession.builder.appName("FailingSalesJob")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.task.maxFailures", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def risky(value):
    # Blows up on a deterministic subset of rows, after some real work.
    if value is not None and value % 50000 == 49999:
        raise ValueError(f"Corrupt record detected for id={value}: negative quantity")
    return value * 2


risky_udf = F.udf(risky, LongType())

df = (
    spark.range(0, 200000, 1, 4)
    .withColumn("bucket", (F.col("id") % 10).cast("int"))
    .withColumn("doubled", risky_udf(F.col("id")))
)

try:
    result = df.groupBy("bucket").agg(F.sum("doubled").alias("total")).collect()
    print("unexpectedly succeeded:", result)
except Exception as exc:  # noqa: BLE001
    print("job failed as expected:", type(exc).__name__)

spark.stop()
