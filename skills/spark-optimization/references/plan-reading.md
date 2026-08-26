# Reading a Spark physical plan from the History Server

`get_sql_execution(app_id, execution_id, include_plan=true)` returns the physical
plan text plus `nodeMetrics` — the counters Spark itself recorded per plan node.
Together they answer questions the stage metrics cannot: *which operator* is
expensive, and *why the planner chose it*.

Read plans **bottom-up**: leaves are scans, the root is the final output.
Indentation shows the tree; `+-` marks a child.

## Physical operators worth recognising

### Scans

| Operator | Meaning |
| --- | --- |
| `FileScan parquet` / `orc` | columnar read; supports pruning and pushdown |
| `FileScan csv` / `json` | row-oriented; no pushdown, full parse of every row |
| `Scan ExistingRDD` | data already in memory |
| `InMemoryTableScan` | a cached DataFrame is being read |

The scan line carries the important attributes: `PartitionFilters`,
`PushedFilters`, `DataFilters` and `ReadSchema`.

- `PartitionFilters: []` on a partitioned table means **no partition pruning** —
  every partition is being read.
- `PushedFilters: []` when the query has a `WHERE` means the predicate is being
  applied after the read instead of during it.
- `ReadSchema` listing every column when the query selects three means **no
  column pruning** — usually caused by a `select *` upstream or a UDF that forces
  the whole row.

### Exchanges (shuffles)

| Operator | Meaning |
| --- | --- |
| `Exchange hashpartitioning(key, N)` | a shuffle keyed on `key` into `N` partitions — the expensive thing in most jobs |
| `Exchange SinglePartition` | everything to one partition; a global sort or an un-keyed aggregate. A serialisation point |
| `Exchange rangepartitioning` | a global `ORDER BY`; requires a sampling pass |
| `BroadcastExchange` | one side broadcast to every executor; no shuffle, but built on the driver |
| `AQEShuffleRead coalesced` | AQE merged small post-shuffle partitions |
| `AQEShuffleRead skewed` | AQE split a skewed partition — a direct statement that skew was detected |

Count the exchanges. Each one is a full pass over the data across the network.
Two aggregations on the same key should share one shuffle; if they do not, the
query is probably re-shuffling unnecessarily.

### Joins

See the join-strategy section of the tuning playbook for the full table. In the
plan, the shape to notice is `SortMergeJoin` sitting above two `Exchange` +
`Sort` pairs — that is two shuffles and two sorts, and it is what a broadcast
join replaces with nothing.

### Aggregates

`HashAggregate` appears twice for a shuffle aggregation: a partial aggregate
below the exchange and a final one above it. That is the efficient shape —
partial aggregation reduces what crosses the network.

A *single* `HashAggregate` above an exchange means no partial aggregation, which
happens with aggregate functions that cannot be split (`collect_list`,
`count(distinct)` without rewriting). Those shuffle raw rows and are expensive.

`SortAggregate` instead of `HashAggregate` means the grouping keys are not
hashable in Tungsten's format — usually a complex type. It sorts instead, which
is slower.

### Other markers

- `WholeStageCodegen (n)` — operators fused into one generated Java method. Its
  absence around a hot operator means the codegen path was not available, often
  because a UDF broke the chain.
- `BatchEvalPython` / `ArrowEvalPython` — a Python UDF. `BatchEvalPython` is the
  slow row-at-a-time path; `ArrowEvalPython` is the vectorised pandas UDF path.
  Seeing `BatchEvalPython` on a hot path is a concrete, actionable finding:
  rewriting as a pandas UDF or a native expression is typically a large win.
- `Generate explode` — row multiplication. Check the output-row metric; explodes
  can silently 100× the data.
- `Window` — requires a sort within each partition; a window without a
  `PARTITION BY` sorts everything into one partition.

## Node metrics

`nodeMetrics` in the response maps each plan node to the counters Spark
collected. The ones that carry the most information:

| Metric | Read it as |
| --- | --- |
| `number of output rows` | the row count leaving that operator. Comparing it across levels shows where data is reduced — or is not |
| `number of files read` | small-file diagnosis |
| `size of files read` | divide by files for the average file size |
| `number of partitions read` | partition-pruning check |
| `spill size` | which specific operator ran out of memory — more precise than the stage total |
| `peak memory` | per-operator high-water mark |
| `data size` | bytes of the broadcast or shuffle at that node |
| `time in aggregation build` / `shuffle write time` | where time went inside the operator |

### The pushdown check

The most valuable single comparison:

```
FileScan parquet ... number of output rows: 2,000,000,000
Filter (event_date = '2026-08-01') ... number of output rows: 4,000,000
```

Two billion rows read to keep four million means the filter did not push down.
Fix the format, remove the UDF from the predicate, or partition the table on the
filtered column — this is often a 100× reduction in bytes scanned.

### The broadcast check

`BroadcastExchange` with a large `data size` is the driver-OOM risk. If it is
close to `spark.sql.autoBroadcastJoinThreshold`, the query is one data-growth
event away from either OOMing the driver or silently falling back to a sort-merge
join.

## AQE and the two plans

With AQE enabled the plan is rewritten during execution.
`get_sql_execution(..., include_plan=true)` strips the `== Initial Plan ==`
sections by default and shows what actually ran. Pass
`include_initial_plan=true` to see both.

The diff between them is diagnostic in itself:

- initial `SortMergeJoin` → final `BroadcastHashJoin`: AQE found the side was
  small at runtime. Good — and a hint that the static statistics are stale.
- final plan contains `AQEShuffleRead skewed`: skew was detected and handled.
  Confirms a skew diagnosis without any metric analysis.
- final plan contains `AQEShuffleRead coalesced N partitions`: `shuffle.partitions`
  was set higher than needed, and AQE cleaned up. Harmless.

## Comparing two plans

`compare_sql_executions(app_id1, app_id2, execution_id1, execution_id2,
include_plan_diff=true)` returns per-side node and edge counts plus the node
types whose counts differ. A join strategy change shows up as
`BroadcastHashJoin` on one side and `SortMergeJoin` + extra `Exchange`/`Sort` on
the other — the fastest way to explain a regression that has no code change
behind it.
