---
name: spark-optimization
description: Diagnose and speed up slow or expensive Apache Spark jobs using the Spark History Server MCP tools — finds the real bottleneck (skew, spill, shuffle, small files, GC, bad join strategy, under-parallelism) from task-level metrics and returns ranked, quantified fixes. Use this whenever someone asks why a Spark job is slow, how to make it faster or cheaper, how to tune executor memory / cores / shuffle partitions, why a run suddenly regressed or costs more than it used to, whether a query plan is efficient, or asks to review or optimize a Spark application, pipeline or SQL query. Use it for "this used to finish in 20 minutes" and cost-reduction requests too, even when Spark is not named explicitly but the job runs on Spark.
---

# Spark performance optimization

Most Spark tuning advice on the internet is a list of settings. That is the wrong
shape: the same setting helps one job and hurts the next. What actually works is
finding the one stage that dominates the runtime, learning what it is waiting on,
and changing that.

The History Server has the evidence to do this properly — per-task metric
distributions, per-node SQL plan counters, executor timelines. This skill is
about extracting it in a fixed order so the recommendation is derived rather than
guessed.

## The rule that matters most

**Quantify before recommending.** Every recommendation should carry the number
that justifies it and, where possible, the expected size of the win:

> Stage 14 spilled 8.2 GB to disk across 200 tasks and accounts for 62% of
> wall-clock time. Raising `spark.sql.shuffle.partitions` from 200 to 800 puts
> each partition near 128 MB and should eliminate the spill.

Not:

> Consider increasing shuffle partitions and executor memory.

A recommendation without a measurement is a guess, and engineers can tell.

## Workflow

### 1. Frame the run

```
list_applications(app_id="<id>")
get_executor_summary(app_id="<id>")
```

Get the total duration and the executor totals: task count, GC time, input bytes,
shuffle read/write. This is your denominator — everything later is a share of it.

Two derived numbers to compute immediately:

- **GC ratio** = `total_gc_time / total_duration`. Above ~0.10 means memory
  pressure is a first-order problem and will distort everything else.
- **Shuffle-to-input ratio** = `total_shuffle_write / total_input_bytes`. Well
  above 1 means the query moves more data than it reads — usually a join or
  aggregation strategy problem, and the highest-leverage thing to fix.

### 2. Find where the time actually goes

```
get_job_bottlenecks(app_id="<id>", top_n=10)
```

One call that returns the slowest stages and jobs, stages with significant
memory spill, the GC pressure ratio and executor utilisation. Treat its
`recommendations` as leads, not conclusions — they are threshold-based and do not
know your data.

Then get the ranked stage list yourself so you can compute shares:

```
list_stages(app_id="<id>", sort_by="duration", length=10)
```

Compute each stage's share of total runtime. Spark applications are almost always
dominated by one or two stages; anything below ~10% of runtime is not worth
optimising yet. Ignore `SKIPPED` stages — their durations are meaningless.

### 3. Diagnose the dominant stage

```
get_stage(app_id="<id>", stage_id=<top stage>, with_summaries=true)
```

The `taskMetricsDistributions` block is where the diagnosis comes from. Work
through this decision order — it is ordered by how much the answer changes:

1. **Skew?** `duration` max / p50 > 5, *and* `shuffleReadBytes` (or `inputBytes`)
   max / p50 > 5. Bytes-skew and time-skew together mean data skew. Time-skew
   with uniform bytes means something else — a slow node, or a value-dependent
   UDF.
2. **Spill?** `memoryBytesSpilled` or `diskBytesSpilled` non-zero. Disk spill is
   the expensive one and is usually the single biggest available win.
3. **GC-bound?** `jvmGcTime` > 10% of `executorRunTime`.
4. **Scheduler-bound?** `schedulerDelay` > 20% of `duration` — tasks are too
   small; there are too many partitions.
5. **Under-parallelised?** `numTasks` below the total executor core count — cores
   are idle.
6. **I/O-bound?** `executorRunTime` well below `duration` — waiting on storage or
   shuffle fetch, not computing. When this shows up on the final write stage
   specifically, it is how a slow sink (HDFS, S3, a JDBC target) presents — see
   [Slow writes to a sink](references/tuning-playbook.md#slow-writes-to-a-sink)
   in the playbook.

`references/tuning-playbook.md` has the fix for each of these, with the settings,
the reasoning, and the failure modes of over-applying them. Read it once you know
which branch you are in rather than up front.

### 4. Attribute to the query

For SQL and DataFrame jobs, map the expensive stage back to the query:

```
list_sql_executions(app_id="<id>", sort_by="duration")
get_sql_execution(app_id="<id>", execution_id=<id>, include_plan=true,
                  include_aggregated_metrics=true, include_stages=true)
```

The plan tells you the join strategy and whether AQE rewrote it; `nodeMetrics`
gives per-operator counters Spark collected itself. The specific things to look
for are in `references/plan-reading.md` — it covers what each physical operator
implies, what "number of output rows" on a scan vs a filter reveals about
predicate pushdown, and how to spot the small-file problem.

For RDD-only applications there are no SQL executions; work from stages and job
names instead.

### 5. Check the shape of the cluster

```
list_executors(app_id="<id>", include_inactive=true, sort_by="duration")
get_resource_usage_timeline(app_id="<id>")
```

Look for: executors added late (slow ramp-up wasting the first minutes), a
`peak_executors` far below the configured maximum (the job never got the
parallelism it asked for), one executor doing far more work than the others
(locality skew), or executors removed mid-run.

An under-utilised cluster is a cost problem even when the runtime is acceptable.

### 6. If this is a regression, compare directly

When "it used to be faster" is the complaint, do not analyse the slow run in
isolation — diff it against the fast one:

```
compare_job_environments(app_id1="<fast>", app_id2="<slow>")
compare_job_performance(app_id1="<fast>", app_id2="<slow>")
compare_sql_executions(app_id1="<fast>", app_id2="<slow>",
                       execution_id1=<a>, execution_id2=<b>, include_plan_diff=true)
compare_stages(app_id1="<fast>", stage_id1=<n>, app_id2="<slow>", stage_id2=<m>)
```

Check input volume first: a run that processed 3× the data being 3× slower is not
a regression. Then config (`compare_job_environments` — ignore the noise of ids,
hostnames and ports; look at shuffle partitions, memory, cores, AQE flags,
extensions), then the plan diff, which catches the classic silent regression
where a table grew past `spark.sql.autoBroadcastJoinThreshold` and a broadcast
join quietly became a sort-merge join.

### 7. Report

```markdown
## Summary
Runtime, cost driver, and the single biggest opportunity in two sentences.

## Where the time goes
| Stage | Description | Duration | % of total | Tasks | Spill | Diagnosis |

## Findings
### 1. <Finding> — <expected impact>
Evidence: the specific metrics.
Cause: the mechanism.
Fix: the exact change.

## Recommended changes
```
spark.sql.shuffle.partitions=800     # was 200; targets ~128MB per partition
```

## What to measure after the change
The specific metric that should move, and to roughly what value.
```

Rank findings by expected impact, not by how easy they are to describe. If the
top finding is a code change rather than a config change, say so — telling
someone to add a `repartition` is more useful than a config they can flip that
will not help.

## Judgement calls worth getting right

- **Do not tune a job that is fast enough.** If the dominant stage is 8% of
  runtime, the honest answer is "there is no significant win here", and saying it
  builds more trust than a list of marginal settings.
- **Skew fixes beat memory increases.** Raising executor memory to survive a
  skewed partition works until the data grows. Fixing the distribution scales.
- **More partitions is not always better.** Past a point, scheduling overhead
  dominates — visible as rising `schedulerDelay` and tiny task durations.
- **A local run tells you little about a cluster.** One executor named `driver`
  means no network shuffle and no container limits; do not extrapolate.
- **AQE changes the picture.** With `spark.sql.adaptive.enabled=true`, partition
  counts and join strategies are decided at runtime, so the static config may not
  reflect what ran. The plan's final form is what executed; `include_plan` shows
  it with the initial plan stripped.

## When the job failed rather than ran slowly

Use the `spark-rca` skill — it has the failure-signature catalogue.

## Reference files

- `references/tuning-playbook.md` — the fix for each diagnosis, with settings and
  trade-offs. Read the section matching your diagnosis.
- `references/plan-reading.md` — physical operators, node metrics, join
  strategies, and what a plan reveals about a query.
