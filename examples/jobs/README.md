# Spark jobs used to generate the test event logs

Both jobs were submitted with event logging enabled so their applications show
up in the Spark History Server:

```bash
spark-submit --master 'local[4]' \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///path/to/examples/basic/events \
  sales_pipeline.py
```

| Job | Application | What it produces |
| --- | --- | --- |
| `sales_pipeline.py` | `SalesAnalyticsPipeline` (`local-1787666087616`) | 8 SQL executions, 22 jobs, 52 stages: shuffle join, window function, aggregation, Parquet writes |
| `failing_job.py` | `FailingSalesJob` (`local-1787666126593`) | 1 failed job / stage / task with a real Python stack trace, for the failure-analysis tools |
