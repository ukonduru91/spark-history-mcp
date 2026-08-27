# Reading Spark metrics from the History Server

What the numbers returned by the MCP tools mean, and the thresholds worth
reacting to. Units matter: **times are milliseconds, sizes are bytes**, unless a
field name says otherwise (`duration_seconds`, `memory_spilled_mb`).

## Task metric distributions

`get_stage(app_id, stage_id, with_summaries=true)` returns
`taskMetricsDistributions`: one array per metric, aligned to the `quantiles`
array (default `[0.05, 0.25, 0.5, 0.75, 0.95]`). `compare_stages` returns the
same shape at p25/p50/p75/max.

Read them as ratios, not absolutes. A stage where every task takes 40 seconds is
fine; a stage where the median is 2 seconds and the max is 400 is broken.

| Metric | What it is | React when |
| --- | --- | --- |
| `duration` | wall time per task | max / p50 > 5 → skew. > 10 → severe |
| `executorRunTime` | CPU-side execution time | close to `duration` → CPU-bound; far below → waiting on I/O or shuffle |
| `jvmGcTime` | time in GC | > 10% of `executorRunTime` → memory pressure |
| `schedulerDelay` | queue + serialization overhead | > 20% of `duration` → tasks are too small, or the driver is a bottleneck |
| `peakExecutionMemory` | high-water mark of execution memory | approaching the executor memory fraction → OOM risk |
| `memoryBytesSpilled` | data pushed out of execution memory | any non-zero → the stage needed more memory than it had |
| `diskBytesSpilled` | spill that reached disk | non-zero → real slowdown; this is the expensive one |
| `inputBytes` / `outputBytes` | read from / written to storage | max / p50 > 5 → input file sizes are uneven. For a write stage, also compare stage `outputBytes / duration` against `executorRunTime`: low throughput plus a large `duration − executorRunTime` gap points at a slow sink (HDFS/S3/JDBC), not at Spark compute — see the optimization skill's "Slow writes to a sink" |
| `shuffleReadBytes` | data pulled from the previous stage | max / p50 > 5 → key skew |
| `shuffleWriteBytes` | data written for the next stage | large relative to input → the query is shuffling more than it needs to |
| `resultSize` | serialized result returned to the driver | large → a `collect()` is happening |

### Skew, precisely

Skew is not "one task is slower". It is a partition holding disproportionately
more *data*:

- `shuffleReadBytes` max / p50 > 5 → key skew in the shuffle
- `inputBytes` max / p50 > 5 → uneven source files, not key skew
- `duration` skewed but bytes uniform → not data skew: suspect a slow node, a
  UDF whose cost depends on the value, or lock contention

The distinction decides the fix. Bytes-skew is fixed by repartitioning, salting,
or AQE skew join handling. Duration-skew with uniform bytes is not.

## Stage-level fields

From `list_stages` / `get_stage`, per stage rather than per task:

| Field | Notes |
| --- | --- |
| `status` | `COMPLETE`, `FAILED`, `SKIPPED`, `ACTIVE`, `PENDING`. `SKIPPED` means the results were already available — it is not an error and its duration is meaningless |
| `numTasks` | equals the partition count going in. 200 is the untuned `spark.sql.shuffle.partitions` default |
| `numFailedTasks` | non-zero on a COMPLETE stage means retries succeeded — a warning sign, not a failure |
| `submissionTime` / `completionTime` | wall clock. Their difference is the stage duration; the tools compute it for you |
| `executorRunTime` | summed CPU time across tasks. Compare with `numTasks × wall duration` to see parallelism |
| `shuffleReadBytes` / `shuffleWriteBytes` | stage totals |
| `memoryBytesSpilled` / `diskBytesSpilled` | stage totals — the fastest way to spot memory pressure across a whole application |

A stage with `numTasks` far below the executor core count is under-parallelised —
cores sit idle. A stage with tens of thousands of tiny tasks is over-partitioned
and dominated by scheduling overhead; `schedulerDelay` will show it.

## Executor metrics

`get_executor_summary` aggregates; `list_executors(include_inactive=true)` shows
each one.

| Signal | Meaning |
| --- | --- |
| `total_gc_time / total_duration > 0.10` | chronic GC pressure — under-provisioned memory or too much object churn |
| `failed_tasks` concentrated on one executor | a bad node, not bad code |
| executors with a `removeReason` | container kills, decommissioning or spot reclamation |
| `active_executors ≪ total_executors` | dynamic allocation scaled down — check whether it did so mid-job |
| `total_input_bytes` uneven across executors | data locality problems |
| `maxMemory` vs `memoryUsed` | cache pressure; low headroom means cached blocks are being evicted |

## SQL execution metrics

`get_sql_execution(app_id, execution_id, include_plan=true)` returns
`nodeMetrics` — per plan-node counters that Spark itself collected. These are the
most direct evidence available about a query:

- `number of output rows` on a scan vs on the filter above it shows whether
  predicate pushdown worked
- `number of files read` / `size of files read` shows the small-file problem
- `spill size` on an aggregate or sort names the operator that ran out of memory
- `time in aggregation build` and similar isolate the expensive operator

`include_aggregated_metrics=true` adds the jobs and the aggregated stage totals
scoped to that one query, which is how you attribute cost to a specific SQL
statement in an application that runs many.

## Timeline

`get_resource_usage_timeline` summarises executor add/remove events and stage
starts. `peak_executors` far below the configured maximum means the job never got
the parallelism it asked for — a cluster capacity issue rather than a code issue.

## Sanity checks before drawing conclusions

- **SKIPPED stages have meaningless durations.** Exclude them when ranking.
- **Zero-duration stages** are usually skipped or instantaneous; they are not the
  bottleneck.
- **A single-executor local run** (`total_executors: 1`, executor id `driver`)
  has no shuffle across the network and no container kills — do not generalise
  its metrics to a cluster.
- **Compare like with like.** When comparing two runs, check input volume first
  (`inputBytes`); a run that processed twice the data being twice as slow is not
  a regression.
