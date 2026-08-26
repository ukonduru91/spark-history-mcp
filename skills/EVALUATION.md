# How the skills were tested

Both skills were run against a **live Spark History Server** holding real event
logs, and compared against the same model answering the same question with **no
skill loaded**. That baseline matters: if a skill does not beat a capable model
working from the raw tools, it is not earning its place.

## Setup

| | |
| --- | --- |
| Server | Spark 3.5.5 History Server, this MCP over stdio |
| Applications | `SalesAnalyticsPipeline` (8 SQL executions, 22 jobs, 52 stages) and `FailingSalesJob` (a UDF that raises, producing a real failed stage and task) — both in `examples/` |
| Cases | one RCA question, one optimization question, each run with and without the skill |

## What happened

Both skills reached the correct answer, and so did both baselines. Reported
honestly: **the tools are informative enough that a strong model does well
without a skill on a single, well-posed question.**

Where the skills earned their place:

- **Structure.** Both skill runs produced the exact reporting shape the skill
  specifies — root cause, evidence, fix, verification for RCA; time budget,
  findings ranked by impact, recommended changes for optimization. The baselines
  invented their own structure each time. Across a team, one predictable shape is
  worth a lot: a reviewer knows where to look.
- **Evidence discipline.** The skill runs quoted the exception text and the
  metric behind every claim, because the skill makes that a rule rather than a
  habit.
- **Ruling things out explicitly.** Both skill runs included a "what is *not*
  wrong" section — no skew, no spill, no GC pressure, with the numbers. That
  stops the reader from re-investigating what has already been checked.

The optimization skill's run found both real problems in the pipeline: debug
`count()`/`show()` calls forcing the whole pipeline to be computed twice (proved
by byte-identical shuffle sizes between the `show()` and `write` stages), and
`spark.sql.autoBroadcastJoinThreshold=-1` forcing a sort-merge join against a
195 KiB dimension table.

## What the testing changed

Running the skills found two things worth fixing, and both were:

1. **The empty-metrics 404.** When a stage fails before any task reports metrics,
   Spark answers `get_stage(with_summaries=true)` with
   `404 … No tasks reported metrics for N / 0 yet`. That is Spark's own
   behaviour, not a tool failure, but it derailed the workflow. The RCA skill now
   expects it and says what to do instead.
2. **Deterministic vs one-off failures.** The baseline run surfaced an insight
   the skill run missed: the failing condition hit *every* partition, so retries
   and repartitioning could never have helped. The RCA skill now asks that
   question explicitly at the sizing step, because getting it wrong sends people
   to the wrong fix entirely.

That is the loop worth repeating as your team uses these: when a run misses
something a human would have caught, the fix usually belongs in the skill.

## Re-running it

`scripts/mcp-cli.mjs` makes the tools callable from a shell, so a skill run can
be reproduced without an MCP client:

```bash
cd /path/to/spark-history-mcp
SHS_SERVERS__LOCAL__URL=http://localhost:18080 \
  node scripts/mcp-cli.mjs call list_stage_task_failures '{"app_id":"<id>","stage_id":0}'
```

Point a model at `skills/spark-rca/SKILL.md` and that CLI, ask it a real
question about one of your own failed applications, and compare against what it
does without the skill.
