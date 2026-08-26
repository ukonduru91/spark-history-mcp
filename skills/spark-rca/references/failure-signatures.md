# Spark failure signatures

A lookup table from the exception you found in `list_stage_task_failures` (or in
the driver message) to what actually went wrong and what to change. Find the
innermost `Caused by:` first — that is the one to look up here.

## Contents

- [Memory and container kills](#memory-and-container-kills)
- [Shuffle and fetch failures](#shuffle-and-fetch-failures)
- [Data and schema errors](#data-and-schema-errors)
- [Python / PySpark errors](#python--pyspark-errors)
- [Dependency and classpath errors](#dependency-and-classpath-errors)
- [Storage and permissions](#storage-and-permissions)
- [Driver-side failures](#driver-side-failures)
- [Timeouts, hangs and speculative execution](#timeouts-hangs-and-speculative-execution)

---

## Memory and container kills

### `java.lang.OutOfMemoryError: Java heap space`
The JVM heap genuinely filled. Where it happened decides the fix.

- **In a task, on a wide stage** — usually one oversized partition. Check
  `taskMetricsDistributions.duration` and `peakExecutionMemory`: if max ≫ p75 it
  is skew, and raising memory only postpones the failure. Fix the skew (see the
  optimization skill's skew section) or raise `spark.sql.shuffle.partitions`.
- **In a task, uniformly across tasks** — the partition size is simply too large
  for the executor. Raise `spark.executor.memory`, or raise the partition count
  so each partition is smaller.
- **During a collect/broadcast** — see driver-side failures below.

### `java.lang.OutOfMemoryError: GC overhead limit exceeded`
The JVM spent >98% of its time in GC recovering <2% of the heap. Effectively the
same as heap space, but confirm with `get_executor_summary`: a
`total_gc_time / total_duration` ratio above ~0.10 means GC pressure is real and
chronic, not a one-off.

### `ExecutorLostFailure ... Container killed by YARN for exceeding memory limits`
### `Container killed on request. Exit code is 137`
The **cluster manager** killed the container because its total RSS exceeded the
requested memory — heap + off-heap + PySpark worker memory + native buffers.

This is the one most often mis-diagnosed. Raising `spark.executor.memory` alone
takes memory away from overhead and can make it fail sooner. The knobs:

| Setting | What it covers |
| --- | --- |
| `spark.executor.memoryOverhead` | off-heap: native, PySpark workers, netty buffers. Default `max(384MB, 10% of executor memory)` — routinely too small for PySpark and Arrow |
| `spark.executor.pyspark.memory` | caps the Python worker's own allocation |
| `spark.memory.offHeap.size` | explicit off-heap execution memory when `offHeap.enabled=true` |

For PySpark with pandas UDFs, start at 25% overhead rather than the 10% default.
Confirm the diagnosis by checking `list_executors(include_inactive=true)` for
executors with a `removeReason` — the kill is recorded there.

### `Total size of serialized results ... is bigger than spark.driver.maxResultSize`
A `collect()` (or a Python `toPandas()`) pulled too much back. Either don't
collect (write to storage and read the file), or raise
`spark.driver.maxResultSize` if the volume is genuinely intended. This is a
symptom of an anti-pattern far more often than a config problem.

---

## Shuffle and fetch failures

### `FetchFailedException: Failed to connect to <host>`
### `FetchFailedException: Connection reset by peer`
The reducer could not read map output. **The failing stage is the reducer, but
the problem is in the mapper.** Almost always a downstream effect of the mapper's
executor dying — check `list_executors(include_inactive=true)` for a removed
executor with a matching host, and investigate *its* failure. Spot instance
reclamation and container kills both look like this.

If executors are healthy, suspect network or the external shuffle service.

### `FetchFailedException: Too large frame` / `frame size exceeds`
A single shuffle block exceeded 2GB. Classic severe skew — one key holds a huge
share of the data. Raise `spark.sql.shuffle.partitions`, enable AQE skew join
handling (`spark.sql.adaptive.skewJoin.enabled=true`), or salt the hot key.

### `org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle N`
Map output was lost, usually with the executor that produced it. Same
investigation as fetch failures. Recurring instances on dynamic-allocation
clusters point at executors being decommissioned while their shuffle data is
still needed — enable the external shuffle service or
`spark.decommission.enabled` with shuffle-block migration.

---

## Data and schema errors

### `SparkUpgradeException` / `DateTimeException` on parsing
A datetime or decimal parsed under different calendar/format rules than the
writer used. Compare
`get_environment(section="runtime")` across the working and failing runs — a
Spark version bump is usually behind it. `spark.sql.legacy.timeParserPolicy=LEGACY`
restores the old behaviour as a stopgap; fixing the format is the real answer.

### `ArrayIndexOutOfBoundsException` / `NumberFormatException` inside a parser
Malformed input records. Identify the file: the stage's `name`/`details` and the
plan (`get_sql_execution(..., include_plan=true)`) show the scan and its paths.
`spark.sql.csv.parser.columnPruning.enabled`, `mode=PERMISSIVE` with
`columnNameOfCorruptRecord`, or an explicit schema will quarantine bad rows
rather than fail the job.

### `AnalysisException: cannot resolve '<column>'`
Fails during planning, so there is usually no failed *stage* at all — the job
never started. Compare the schema the code expects with the source. An upstream
schema change is the usual cause; check whether the source table was altered
between the last good run and this one.

### `NullPointerException` inside a UDF
The UDF received a null it did not expect. Spark does not stop nulls reaching
UDFs. Guard inside the UDF or filter beforehand.

---

## Python / PySpark errors

### `org.apache.spark.api.python.PythonException`
The wrapper. The Python traceback inside it is the real error — read it as you
would any Python traceback, bottom line first.

Common ones:

| Python error | Meaning |
| --- | --- |
| `ModuleNotFoundError` | the dependency exists on the driver but not the executors. Ship it with `--py-files`, a packed venv, or bake it into the image |
| `ValueError` / `TypeError` in a UDF | a real data problem — inspect the offending value in the traceback |
| `PicklingError` | the closure captured something unserializable (a DB connection, a file handle, a SparkContext). Build it inside the function instead |
| `MemoryError` | the Python worker, not the JVM — raise `spark.executor.pyspark.memory` and `memoryOverhead` |

### `SparkException: Python worker exited unexpectedly (crashed)`
The Python process died without a traceback — almost always the OOM killer.
Treat it as a container-kill memory problem, on the Python side.

### `PythonException ... pyarrow` / `ArrowInvalid`
A pandas/Arrow UDF type mismatch between the declared return type and what the
function actually returns. The declared schema is the contract; make the function
match it.

---

## Dependency and classpath errors

### `NoSuchMethodError` / `NoClassDefFoundError` / `ClassNotFoundException`
A jar conflict. `get_environment(section="classpath_entries")` lists what was
actually loaded — look for two versions of the same artifact. Comparing the
classpath of the last good run against the failing one
(`compare_job_environments`) finds these fast.

Fixes: align the version with what the Spark distribution ships, shade the
conflicting dependency, or set `spark.driver.userClassPathFirst` /
`spark.executor.userClassPathFirst` (with care — it can break Spark's own
classes).

### `java.lang.UnsupportedClassVersionError`
Code compiled for a newer JDK than the cluster runs. Compare `javaVersion` in
`get_environment(section="runtime")` with the build's target.

---

## Storage and permissions

### `AccessDeniedException` / `403 Forbidden` / `S3Exception`
Credentials or IAM policy, not Spark. Note which path failed — often only one
prefix is unreadable, which points at a specific policy statement.

### `FileNotFoundException: ... It is possible the underlying files have been updated`
The file list was cached and then the underlying data changed — a concurrent
write, or a table overwritten mid-read. `REFRESH TABLE`, or coordinate the
writer. On object stores, eventual consistency of listings can produce this too.

### `org.apache.hadoop.fs.FileAlreadyExistsException`
Two writers targeting the same output path, or a retry after a partial write.
Check whether the application has multiple attempts — attempt 2 tripping over
attempt 1's output is common.

---

## Driver-side failures

The signature is an application that fails with **no failed stage**, or with the
last job never completing.

### Driver `OutOfMemoryError`
Caused by `collect()`, `toPandas()`, a huge broadcast, or accumulating results in
a Python list. Check `spark.driver.memory` in `spark_properties` and look for a
broadcast in the plan (`get_sql_execution(..., include_plan=true)` → a
`BroadcastExchange` over a large table). `spark.sql.autoBroadcastJoinThreshold`
governs the automatic ones; a table that grew past the threshold silently starts
being broadcast and kills the driver.

### `SparkContext was shut down` / `Application killed by user`
External termination — a scheduler timeout, a spot reclamation of the driver
node, or someone killing it. The event log usually ends abruptly. Look at the
wall-clock gap between the last completed stage and the application end time.

---

## Timeouts, hangs and speculative execution

### Job is *stuck*, not failed
The one case where `get_executor_thread_dump` is the right tool — but only while
the application is still **running**, since the History Server does not persist
thread dumps.

```
get_executor_thread_dump(app_id="<id>", executor_id="driver")
get_executor_thread_dump(app_id="<id>", executor_id="3", blocked_only=true)
```

A driver stuck in `listLeafFiles` means file listing on a huge prefix. Executors
blocked on a lock point at a synchronised UDF or a shared resource. All executors
idle with tasks pending means the driver is the bottleneck.

### `TaskCommitDenied` / repeated task attempts
Speculative execution racing on the output committer. Normal in moderation; if it
dominates, the committer is not safe for the storage system — on S3 use a
committer designed for object stores rather than the file-based one.

### `Futures timed out after [N seconds]`
Usually broadcast: the broadcast side took longer to build than
`spark.sql.broadcastTimeout`. Either the table is too big to broadcast (lower
`autoBroadcastJoinThreshold` so it doesn't) or the cluster was starved.
