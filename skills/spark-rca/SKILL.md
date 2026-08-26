---
name: spark-rca
description: Root-cause a failed, killed, or hung Apache Spark application using the Spark History Server MCP tools — reads the actual task exceptions, stage metrics and runtime config, then names the cause and the fix. Use this whenever someone mentions a Spark job that failed, crashed, errored, was killed, OOMed, got stuck, is retrying, or when they paste a Spark stack trace, an application ID, a YARN/EMR/Databricks/Kubernetes app URL, or ask "why did my job fail" / "what happened to this run". Use it even when they only say a pipeline or DAG task "broke" and the underlying engine is Spark.
---

# Spark failure root-cause analysis

A failed Spark application leaves a complete forensic record in the event log.
The History Server exposes it; the job here is to read it in the right order and
stop at the first piece of evidence that actually explains the failure, rather
than guessing from the driver's top-level error message.

That top-level message is almost always a symptom, not a cause. `Job aborted due
to stage failure` tells you nothing. The real cause sits in a *task* exception,
four levels down. Getting to it reliably is what this skill is for.

## The rule that matters most

**Never name a cause you have not seen in the data.** "Probably a memory issue"
is worse than useless — it sends an engineer to change `executor.memory` on a job
that actually failed on a bad cast. Every claim in the final answer must point at
a specific stage, task, exception, metric or config value that a tool returned.
If the evidence runs out before the cause is clear, say exactly what is missing
and what would reveal it.

## Workflow

Work through these in order. Stop early only when the cause is unambiguous — and
"unambiguous" means you have the exception text or a metric that forces the
conclusion, not a plausible story.

### 1. Establish the application

```
list_applications(app_id="<id>")
```

Note the status, duration and — important — the `attempts` array. A YARN
application with several attempts means the whole application was retried, which
usually indicates an AM/driver-level failure rather than a task failure. If there
is more than one attempt, pass `app_attempt_id` to the later tools so you are
reading one specific run rather than an arbitrary one.

If you only have a name, a time window, or a cluster, find the id first:
`list_applications(status=["completed"], min_date="2026-08-01")`.

### 2. Find the failed unit of work

```
list_jobs(app_id="<id>", status=["FAILED"])
list_stages(app_id="<id>", status=["FAILED"])
```

A subtlety worth knowing: **a Spark application can fail with no job marked
FAILED.** The driver may have died before the failure was recorded, or the job
may have been killed externally. When the status filters come back empty, list
without a filter and look for non-zero `numFailedTasks` / `numFailedStages`, or
for a job that never completed (`completionTime` is null). An application whose
last job is incomplete usually died in the driver — jump to step 5.

### 3. Read the task exceptions — this is the step that finds the cause

```
list_stage_task_failures(app_id="<id>", stage_id=<id>)
```

This returns the untruncated exception and stack trace recorded for each failed
task. Nothing else in the tool set gives you this, and it is where the answer
lives in the large majority of real failures.

Read the *innermost* exception in the chain, not the outermost. Spark wraps
everything: `SparkException` wraps `PythonException` wraps the actual
`ValueError`. The last `Caused by:` is the one that matters.

Then match it against `references/failure-signatures.md`, which maps the
exceptions you actually see in production to their causes and fixes. Load that
file once you have an exception in hand — it is organised by exception class, so
you can go straight to the relevant section.

If several tasks failed with *different* exceptions, that itself is a signal:
one shared root cause (a dependency or a bad node) usually produces one
signature; a data problem produces the same exception with different values.

### 4. Size the failure

Before concluding, check whether this was one unlucky task or a systemic problem:

```
get_stage(app_id="<id>", stage_id=<id>, with_summaries=true)
```

The `taskMetricsDistributions` block gives per-task quantiles. Compare the max to
the median:

- **duration max ≫ p75** → skew; the failure may be a symptom of one giant
  partition, not of the code.
- **`memoryBytesSpilled` or `diskBytesSpilled` non-zero at p75** → the stage was
  already under memory pressure before it failed.
- **`peakExecutionMemory` near the executor's memory** → an OOM is credible.
- Every quantile roughly equal → uniform work; the failure is about *what* the
  task did, not how much.

`references/metric-interpretation.md` explains what each metric means and the
thresholds worth reacting to.

One quirk worth expecting: when a stage failed before *any* task reported
metrics, the History Server answers this call with
`404 ... No tasks reported metrics for <stage> / <attempt> yet`. That is not a
tool problem — it is Spark telling you the stage died early, which is itself
evidence. Fall back to the stage's own counters from `list_stages` and rely on
the task exceptions from step 3.

Whether or not the metrics are available, answer one more question before
moving on: **is this failure deterministic, and how much of the data does it
touch?** A single bad row that one task happened to hit is a very different
problem from a condition that every partition will meet. The exception usually
tells you — a value in the message (`id=99999`), a row count, a file path —
and the partitioning is in the stage's `numTasks` and the job's source. Getting
this wrong sends people to retries and repartitioning for a failure that will
recur identically on every run, so say which one it is and why.

### 5. Rule in or out the environment

Only when the exception points at a version, class, config or resource problem:

```
get_environment(app_id="<id>", section="runtime")           # Java/Scala/Spark versions
get_environment(app_id="<id>", section="classpath_entries") # library versions, conflicts
get_environment(app_id="<id>", section="spark_properties")  # memory, shuffle, serializer, AQE
```

`section` keeps the response small — request only the section your hypothesis
needs. A `NoSuchMethodError` needs the classpath; an OOM needs
`spark_properties`.

For resource failures, confirm with real numbers rather than intuition:

```
get_executor_summary(app_id="<id>")     # aggregate GC time, failed tasks, spill
list_executors(app_id="<id>", include_inactive=true, sort_by="failed-tasks")
```

Executors that were *removed* mid-run are the tell for container kills. If one
executor accounts for nearly all failed tasks, suspect that node, not the code.

### 6. Report

Structure the answer like this. It is the shape an on-call engineer can act on
in under a minute:

```markdown
## Root cause
One or two sentences naming the mechanism, in plain language.

## Evidence
- Application <id>, status FAILED, <duration>
- Job <n> failed → stage <n> (attempt <n>), <x>/<y> tasks failed
- Task <n> on executor <n>: <the innermost exception, quoted>
- <the metric or config value that confirms it>

## Fix
The specific change — a config value, a code change, a data fix.
Include the actual setting and value where there is one.

## Verification
How to confirm the fix worked: the metric to re-check after the next run.
```

Keep "Evidence" literal — quoted exception text and real numbers. If a claim
cannot be traced to a tool response, remove it or mark it explicitly as a
hypothesis with the check that would confirm it.

## Things that mislead people

- **`ExecutorLostFailure` / `Container killed by YARN`** is not "Spark ran out of
  memory" — it is the *cluster manager* killing the container for exceeding its
  total memory request, which includes off-heap and overhead. Raising
  `spark.executor.memory` can make it worse by leaving less overhead. See the
  failure-signatures reference.
- **A `FetchFailedException`** is reported against the *reducer* stage but is
  caused by the *mapper* stage's executor being gone. Investigate the map stage.
- **Thread dumps are unavailable for completed applications.** The History Server
  does not persist them, so `get_executor_thread_dump` returns an error for
  anything that already finished. It is for live hangs only — which is exactly
  the tool you want when a job is *stuck* rather than failed.
- **AQE renames and merges stages.** A stage id in the plan may not exist in the
  stage list; if `get_stage` reports the stage is missing, it was probably
  optimised away, and the work moved to another stage id.

## When the job is slow rather than broken

This skill is for failures. If the job completed but took too long or cost too
much, use the `spark-optimization` skill instead — it has the tuning playbook.

## Related MCP prompt

The MCP server ships an `investigate_failure` prompt that walks the same path
interactively. Use this skill when you want the analysis done and reported; use
the prompt when the engineer wants to drive.
