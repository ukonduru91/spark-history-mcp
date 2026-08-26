# Spark tuning playbook

One section per diagnosis from the SKILL.md decision order. Each gives the
mechanism, the fix, and the way the fix goes wrong when over-applied — because
almost every Spark setting has a cost on the other side.

## Contents

- [Data skew](#data-skew)
- [Spill](#spill)
- [Partition sizing](#partition-sizing)
- [Join strategy](#join-strategy)
- [Small files](#small-files)
- [GC pressure and memory layout](#gc-pressure-and-memory-layout)
- [Executor sizing](#executor-sizing)
- [Scheduler overhead and over-parallelisation](#scheduler-overhead-and-over-parallelisation)
- [Caching](#caching)
- [I/O and file formats](#io-and-file-formats)
- [Adaptive Query Execution](#adaptive-query-execution)
- [Cost, as distinct from speed](#cost-as-distinct-from-speed)

---

## Data skew

**Diagnosis** — `duration` max / p50 > 5 **and** `shuffleReadBytes` max / p50 > 5
in the stage's task distributions. Both matter: uneven time with even bytes is
not skew.

**Mechanism** — one or a few keys hold a disproportionate share of rows, so one
task does most of the work while the rest of the cluster idles. Adding executors
does nothing; the stage is as slow as its slowest task.

**Fixes, best first**

1. **AQE skew join handling** — `spark.sql.adaptive.enabled=true` plus
   `spark.sql.adaptive.skewJoin.enabled=true`. Spark splits oversized partitions
   at runtime. Free, and handles the common case. Tune with
   `skewJoin.skewedPartitionFactor` (default 5) and
   `skewedPartitionThresholdInBytes` (default 256MB).
2. **Salting** — when one key dominates and AQE is not enough: add a random
   suffix to the hot key on the large side, explode the small side across the
   same range, join on the salted key, then aggregate. Costs a widening of the
   small side; worth it when one key is 10×+ the next.
3. **Broadcast the small side** — if the other side fits in memory, the shuffle
   disappears and so does the skew. See join strategy below.
4. **Filter the hot key out and handle it separately** — sometimes the skewed key
   is a null placeholder or a sentinel like `-1`/`UNKNOWN`. Splitting it into its
   own path is simpler than salting and often faster.

**Over-application** — salting everything adds shuffle volume and complexity for
no gain on evenly distributed keys. Salt the specific hot key, not the join.

---

## Spill

**Diagnosis** — `memoryBytesSpilled` or, worse, `diskBytesSpilled` non-zero in the
stage distributions. Disk spill is the expensive one: it turns an in-memory sort
or aggregation into an external one.

**Mechanism** — execution memory could not hold the partition being sorted,
aggregated or joined, so Spark wrote it out and read it back.

**Fixes**

1. **More partitions** — the direct lever. Each partition gets smaller, so it
   fits. Raise `spark.sql.shuffle.partitions` (see partition sizing below). This
   is usually right because it costs nothing but a few more tasks.
2. **More execution memory** — raise `spark.executor.memory`, or shift the split
   with `spark.memory.fraction` (default 0.6 of usable heap for execution +
   storage) and `spark.memory.storageFraction` (default 0.5 of that, the part
   protected for cached blocks). If the job does not cache anything, lowering
   `storageFraction` gives execution more room.
3. **Reduce what has to be held** — project columns earlier, filter earlier,
   aggregate before joining rather than after.

**Over-application** — very large executors GC badly (see below). Prefer more
partitions over more memory unless the partition count is already high.

---

## Partition sizing

The single most valuable number to get right. Target **roughly 100–200 MB of
input per partition** for shuffle stages.

Compute it from the data rather than guessing:

```
partitions ≈ shuffleReadBytes (stage total) / 134217728    # 128 MB
```

`spark.sql.shuffle.partitions` defaults to **200** and is the most common cause
of both spill (too few for big data) and scheduler overhead (too many for small
data). Seeing exactly 200 tasks on every shuffle stage means nobody has tuned it.

Also relevant:

- `spark.sql.files.maxPartitionBytes` (default 128MB) sizes partitions on *read*.
- `spark.default.parallelism` governs RDD operations, not SQL.
- With AQE enabled, `spark.sql.adaptive.coalescePartitions.enabled` merges small
  post-shuffle partitions automatically, so a generous
  `shuffle.partitions` is much safer than it used to be — set it high and let AQE
  coalesce down. `advisoryPartitionSizeInBytes` (default 64MB) is the target it
  coalesces toward.

A useful sanity check: partition count should be a small multiple of total
executor cores, so every core gets several tasks and stragglers can be absorbed.

---

## Join strategy

Read it from the plan (`get_sql_execution(..., include_plan=true)`).

| Operator | When Spark picks it | Cost |
| --- | --- | --- |
| `BroadcastHashJoin` | one side under `spark.sql.autoBroadcastJoinThreshold` (default 10MB) | no shuffle; driver and executors must hold the broadcast |
| `SortMergeJoin` | the default for large-large | shuffles and sorts both sides |
| `ShuffledHashJoin` | one side small enough to hash after shuffling | shuffles both, avoids the sort |
| `BroadcastNestedLoopJoin` | non-equi join conditions | quadratic — a red flag on anything but tiny inputs |
| `CartesianProduct` | no join condition | almost always a bug |

**The classic silent regression**: a dimension table grows past the broadcast
threshold, Spark switches from `BroadcastHashJoin` to `SortMergeJoin`, and the
job doubles in runtime with no code change. `compare_sql_executions(...,
include_plan_diff=true)` between the fast and slow runs catches it immediately.

Fixes: raise `spark.sql.autoBroadcastJoinThreshold` (watch driver memory — the
broadcast is built there), or hint explicitly with
`broadcast(df)` / `/*+ BROADCAST(t) */`. AQE's
`spark.sql.adaptive.autoBroadcastJoinThreshold` applies the same decision at
runtime with real statistics, which is more reliable than planner estimates.

If you see `BroadcastNestedLoopJoin` or `CartesianProduct` on real data, fix the
join condition — no amount of tuning rescues a quadratic join.

---

## Small files

**Diagnosis** — in `nodeMetrics` on a scan node: a large `number of files read`
with a small `size of files read`, or thousands of tasks each reading a few MB.

**Mechanism** — every file costs a listing entry, an open, and often a task.
Object stores make this much worse than HDFS. Ten thousand 1 MB files can take
longer to *list* than to read.

**Fixes**

- On write: `df.coalesce(n)` or `repartition(n)` to target 128–512 MB per output
  file. For partitioned tables, `repartition(<partition columns>)` so each output
  partition is written by one task.
- Compact existing data — for Delta/Iceberg/Hudi use the table's own compaction
  (`OPTIMIZE`, `rewrite_data_files`); for plain Parquet, rewrite the prefix.
- Raise `spark.sql.files.maxPartitionBytes` and
  `spark.sql.files.openCostInBytes` so Spark packs more small files per task.

**The opposite failure** — over-coalescing to a handful of huge files removes
parallelism on read and can make the writing stage a bottleneck. `coalesce(1)` on
a large dataset is nearly always wrong.

---

## GC pressure and memory layout

**Diagnosis** — `total_gc_time / total_duration > 0.10` from
`get_executor_summary`, or `jvmGcTime` > 10% of `executorRunTime` in a stage.

**Fixes**

- Prefer **more, smaller executors** over a few huge ones. Heaps above ~64 GB GC
  poorly; 4–8 cores and 16–32 GB per executor is a good default shape.
- Use the DataFrame/SQL API rather than RDDs of case classes — Tungsten's binary
  format allocates far less.
- `spark.serializer=org.apache.spark.serializer.KryoSerializer` when RDDs are
  unavoidable.
- Cache with `MEMORY_AND_DISK_SER` rather than `MEMORY_AND_DISK` to hold fewer
  live objects.
- Off-heap execution memory (`spark.memory.offHeap.enabled=true` plus a `size`)
  moves execution buffers out of the GC's reach entirely.

---

## Executor sizing

Balance parallelism, GC behaviour and container overhead.

Reasonable starting point: **4–5 cores per executor**. Above that, HDFS/object-
store throughput per executor plateaus and contention rises; below it, per-JVM
overhead multiplies.

```
executors = (total cores available - 1 for the driver) / cores per executor
executor memory = (node memory / executors per node) × 0.9   # leave ~10% for overhead
```

- `spark.executor.memoryOverhead` — off-heap and native. Default
  `max(384MB, 10%)`; PySpark with Arrow or pandas UDFs needs closer to 25%.
- **Dynamic allocation** (`spark.dynamicAllocation.enabled`) is a strong default
  for bursty or multi-tenant workloads; it needs the external shuffle service (or
  shuffle tracking) or executors will be removed while their shuffle data is
  still needed.

Check what the job actually received: `get_resource_usage_timeline` reports
`peak_executors` and `peak_cores`. A job configured for 50 executors that peaked
at 12 was starved, and no amount of code tuning fixes that.

---

## Scheduler overhead and over-parallelisation

**Diagnosis** — `schedulerDelay` > 20% of `duration`, tiny median task durations
(sub-second), very large `numTasks`.

**Mechanism** — each task carries fixed cost: scheduling, serialisation,
launching, result collection. When tasks run for 200 ms, that overhead dominates.

**Fixes** — fewer partitions; or let AQE coalesce
(`spark.sql.adaptive.coalescePartitions.enabled=true` with
`advisoryPartitionSizeInBytes`). Target at least a few seconds of work per task.

A driver that is itself the bottleneck shows the same symptom across every stage
— check the driver thread dump on a live run.

---

## Caching

Cache when a DataFrame is used **more than once** and recomputing it is
expensive. Otherwise it is pure cost: cached blocks take memory away from
execution and cause spill elsewhere.

- `MEMORY_AND_DISK` is the sane default; `MEMORY_ONLY` silently recomputes when
  it does not fit.
- `MEMORY_AND_DISK_SER` trades CPU for a much smaller footprint and less GC.
- Unpersist when done — cached blocks survive until evicted.
- Check `list_executors` for `memoryUsed` vs `maxMemory`: little headroom means
  cache is squeezing execution memory, and the cache may be hurting.

For a DataFrame used once, caching is strictly worse than not caching.

---

## I/O and file formats

- **Columnar formats** (Parquet, ORC) allow column pruning and predicate
  pushdown; CSV and JSON allow neither and must be parsed row by row. Converting
  a repeatedly-read CSV source to Parquet is often the single largest win
  available.
- **Verify pushdown actually happened**: in `nodeMetrics`, compare
  `number of output rows` on the scan with the row count after the filter. If the
  scan emits everything and the filter throws most of it away, the predicate did
  not push down — a UDF in the predicate, or a non-columnar format, is the usual
  reason.
- **Partition pruning**: filter on the table's partition columns and confirm
  `number of files read` drops. Reading every partition to filter one day is a
  common and expensive mistake.
- **Compression**: Snappy (fast, splittable) for intermediate data; ZSTD for
  colder data where size matters more than CPU. Plain gzip Parquet is not
  splittable at the file level and limits read parallelism.

---

## Adaptive Query Execution

On by default from Spark 3.2 and worth confirming
(`get_environment(section="spark_properties")`):

| Setting | Effect |
| --- | --- |
| `spark.sql.adaptive.enabled` | master switch |
| `spark.sql.adaptive.coalescePartitions.enabled` | merges small post-shuffle partitions — makes a high `shuffle.partitions` safe |
| `spark.sql.adaptive.skewJoin.enabled` | splits skewed partitions at runtime |
| `spark.sql.adaptive.localShuffleReader.enabled` | avoids a shuffle after a broadcast join |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | the target size coalescing aims for (default 64MB) |

With AQE on, the plan you read is the *final* plan. The initial plan is stripped
by default in `get_sql_execution`; pass `include_initial_plan=true` to see what
the planner originally chose and what AQE changed — the difference is often the
explanation for a surprising runtime.

---

## Cost, as distinct from speed

A job can be fast and wasteful. When the goal is spend rather than latency:

- **Utilisation** — `get_resource_usage_timeline` and `list_executors`. Executors
  alive but idle are pure cost. Dynamic allocation with a short
  `executorIdleTimeout` reclaims them.
- **Right-size the cluster to the dominant stage**, not the peak. A job that
  needs 100 executors for two minutes and 4 for an hour is paying for 100 the
  whole time unless dynamic allocation is on.
- **Spot/preemptible instances** for executors, on-demand for the driver — but
  only with the external shuffle service, or lost shuffle data will cost more in
  recomputation than the instances saved.
- **Read less.** Partition pruning and column projection reduce both time and
  bytes scanned, which on cloud storage is billed directly.
