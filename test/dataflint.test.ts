/**
 * Unit tests for the DataFlint tools: metric parsing, DataFlint's resource
 * formulas (idle cores, memory usage, DCU), its alert thresholds and its alert
 * texts.
 *
 * Expected values come from a Spark 3.2.1 History Server running the DataFlint
 * 0.9.9 jar, read off the DataFlint UI for the same applications.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  executorUsage,
  filterCondition,
  humanBytes,
  joinAlerts,
  metricNumber,
  nodeAlerts,
  queryDcu,
  resourceAlerts,
  resourceConfig,
  resourceMetrics,
  sparkFormatBytes,
  sparkSizeBytes,
  stageAlerts,
  writePartitionKeys,
} from "../src/tools/dataflint.js";
import { DATAFLINT_TOOL_CATALOG } from "../src/schemas/dataflint.js";
import { TOOL_CATALOG } from "../src/schemas/generated.js";

const MiB = 1024 ** 2;
const GiB = 1024 ** 3;
const query = { id: 6, description: "many_tiny_tasks" };

/** An 11-quantile distribution with the given median (index 5) and max (index 10). */
const quantiles = (median: number, max: number) => [0, 0, 0, 0, 0, median, median, median, median, median, max];

describe("metric parsing and formatting", () => {
  it("reads plain, unit and multi-line Spark metric strings", () => {
    assert.equal(metricNumber("1,234"), 1234);
    assert.equal(metricNumber("5.4 GiB"), 5.4 * GiB);
    assert.equal(metricNumber("1.2 m"), 72_000);
    assert.equal(metricNumber("total (min, med, max (stageId: taskId))\n7.8 MiB (1 KiB, 2 KiB, 3 KiB (stage 1.0: task 2))"), 7.8 * MiB);
    assert.equal(metricNumber("total (min, med, max (stageId: taskId))\n"), null);
    assert.equal(metricNumber(undefined), null);
  });

  it("reads Spark memory settings the way DataFlint does", () => {
    assert.equal(sparkSizeBytes("4g", "1g"), 4 * GiB);
    assert.equal(sparkSizeBytes("384m", "1g"), 384 * MiB);
    assert.equal(sparkSizeBytes(undefined, "1g"), GiB);
    assert.equal(sparkSizeBytes("2048", "1g"), 2048 * MiB);
  });

  it("formats bytes like the UI (hu) and like a Spark setting (_u)", () => {
    assert.equal(humanBytes(1.05 * GiB), "1.05 GiB");
    assert.equal(humanBytes(7.8 * GiB), "7.8 GiB");
    assert.equal(humanBytes(0), "0 B");
    assert.equal(sparkFormatBytes(4.8 * GiB), "4.8g");
    assert.equal(sparkFormatBytes(384 * MiB), "384m");
  });

  it("parses filter conditions and write partition keys like DataFlint", () => {
    assert.equal(filterCondition("Filter ((page#12 LIKE /a%) OR (page#12 LIKE /b%))"), "(page LIKE /a%) OR (page LIKE /b%)");
    assert.equal(filterCondition("Filter (amount#3L > 150)"), "amount > 150");
    const write = 'Execute InsertIntoHadoopFsRelationCommand file:/out, false, [bucket#494], Parquet, [__partition_columns=["bucket"], path=/out], Overwrite, [a, bucket]';
    assert.deepEqual(writePartitionKeys(write), ["bucket"]);
    assert.equal(writePartitionKeys("Execute InsertIntoHadoopFsRelationCommand file:/out, false, Parquet, [path=/out], Overwrite, [a]"), undefined);
  });
});

describe("resource formulas", () => {
  // nightly_sales_pipeline: local[8], driver 4g, app 05:19:18Z -> 05:25:14Z per DataFlint applicationinfo.
  const start = Date.parse("2026-11-04T05:19:18Z");
  const end = Date.parse("2026-11-04T05:25:14Z");
  const local = [{
    id: "driver", addTime: "2026-11-04T05:19:21.350GMT", totalCores: 8, maxTasks: 8, totalDuration: 1_118_671,
  }];

  it("matches the DataFlint UI's idle cores, DCU and memory usage in local mode", () => {
    const config = resourceConfig([["spark.driver.memory", "4g"], ["spark.master", "local[8]"]]);
    const metrics = resourceMetrics(executorUsage(local, start, end, config.executorMemoryBytes), config);
    assert.equal(metrics.localMode, true);
    assert.equal(metrics.idleCoresPct.toFixed(2), "60.35");
    assert.equal(metrics.dcu.toFixed(4), "0.0411");
    assert.equal(metrics.maxExecutorMemoryPct, 0);
    assert.equal(config.resourceControlType, "local");
  });

  it("matches the UI's per-query DCU (partitioned_export: 0.0140)", () => {
    const config = resourceConfig([["spark.driver.memory", "4g"]]);
    const usage = executorUsage(local, start, end, config.executorMemoryBytes);
    const dcu = queryDcu(usage, config, Date.parse("2026-11-04T05:21:00Z"), 119_700);
    assert.equal(dcu.toFixed(4), "0.0140");
  });

  it("uses real executors in cluster mode: idle cores, heap usage, container memory", () => {
    const cluster = [
      { id: "driver", addTime: "2026-11-04T05:19:21.000GMT", totalCores: 0, maxTasks: 0, totalDuration: 0 },
      {
        id: "1", addTime: "2026-11-04T05:19:30.000GMT", removeTime: "2026-11-04T05:20:30.000GMT",
        totalCores: 4, maxTasks: 4, totalDuration: 120_000, peakMemoryMetrics: { JVMHeapMemory: 4 * GiB },
      },
    ];
    const config = resourceConfig([["spark.executor.memory", "8g"], ["spark.executor.instances", "1"]]);
    assert.equal(config.executorContainerMemoryBytes, 8 * GiB * 1.1);
    assert.equal(config.resourceControlType, "static");
    const metrics = resourceMetrics(executorUsage(cluster, start, end, config.executorMemoryBytes), config);
    assert.equal(metrics.executors, 1);
    assert.equal(metrics.idleCoresPct, 50); // 120 s used of 4 cores x 60 s
    assert.equal(metrics.maxExecutorMemoryPct, 50); // 4g heap of 8g
  });
});

describe("DataFlint alert rules and texts", () => {
  const config = (memory: string) => resourceConfig([["spark.executor.memory", memory]]);
  const metricsWith = (pctUsed: number, idle = 10) => ({
    localMode: false, executors: 2, cores: 4, taskTimeMs: 0, potentialTaskTimeMs: 0, idleCoresPct: idle,
    maxExecutorMemoryPct: pctUsed, maxExecutorMemoryBytes: (pctUsed / 100) * 4 * GiB, driverHeapPeakBytes: 0,
    coreHours: 0, memoryGibHours: 0, dcu: 0,
  });

  it("flags over- and under-provisioned executor memory with DataFlint's wording", () => {
    const low = resourceAlerts(metricsWith(40), config("4g"), null);
    assert.equal(low[0].name, "executorMemoryTooLow");
    assert.equal(low[0].message, "Max executor memory usage is only 40.00%, which means you can provision less memory and save $$$");
    assert.ok(low[0].suggestion.includes('changing "spark.executor.memory" to 1.92g'));
    assert.ok(low[0].suggestion.includes('from current value "4g"'));
    const high = resourceAlerts(metricsWith(97), config("4g"), null);
    assert.equal(high[0].name, "executorMemoryTooHigh");
    assert.equal(high[0].type, "error");
    assert.ok(high[0].suggestion.includes("to 4.8g"));
    assert.deepEqual(resourceAlerts(metricsWith(80), config("4g"), null), []);
  });

  it("uses the idle-cores suggestion for the app's resource mode", () => {
    const idle = (props: Array<[string, string]>) => resourceAlerts(metricsWith(80, 60), resourceConfig(props), null)[0].suggestion;
    assert.equal(idle([["spark.master", "local[8]"]]), "decrease amount of cores or executors");
    assert.ok(idle([["spark.dynamicAllocation.enabled", "true"]]).includes("executorAllocationRatio"));
    assert.ok(idle([["spark.executor.instances", "4"]]).includes("spark.executor.instances"));
  });

  it("flags partition skew only above 5 s and 10x the median", () => {
    const skewed = { stageId: 6, numTasks: 200, taskMetricsDistributions: { executorRunTime: quantiles(362, 23_418) } };
    const alerts = stageAlerts({ id: 0, description: "revenue_by_segment" }, skewed);
    assert.deepEqual(alerts.map((a) => a.name), ["partitionSkew"]);
    assert.equal(alerts[0].message, "Partition skew ratio of 64.7X, median task duration is 362ms and max task duration is 23.4s");
    assert.equal(alerts[0].location, 'In: SQL query "revenue_by_segment" (id: 0), Stage 6');
    assert.deepEqual(stageAlerts(query, { stageId: 7, numTasks: 200, taskMetricsDistributions: { executorRunTime: quantiles(100, 4_000) } }), []);
    assert.deepEqual(stageAlerts(query, { stageId: 8, numTasks: 200, taskMetricsDistributions: { executorRunTime: quantiles(1_000, 9_000) } }), []);
  });

  it("reproduces the UI's small-tasks card verbatim", () => {
    const tiny = { stageId: 21, numTasks: 6000, taskMetricsDistributions: { executorRunTime: quantiles(3, 50) } };
    const [alert] = stageAlerts(query, tiny);
    assert.equal(alert.message, "6000 tasks with median task duration of 3ms, which causes large scheduling overhead for Spark");
    assert.equal(alert.location, 'In: SQL query "many_tiny_tasks" (id: 6), Stage 21');
    assert.equal(
      alert.suggestion,
      "\n  1. Repartition to less tasks, so you will have less overhead, by running .repartition(600)\n" +
        "  2. Instead of repartition, you can run .coallese(600) to decrease the number of tasks without shuffling on the expense of less parallelism\n" +
        '  3. If you need to hash-partition, call repartition like this: .repartition(600, "hash_key1", "hash_key2")\n',
    );
    assert.deepEqual(stageAlerts(query, { ...tiny, numTasks: 5000 }), []);
  });

  it("flags a large partition only above 5 GiB", () => {
    const stage = (max: number) => ({
      stageId: 22, numTasks: 1,
      taskMetricsDistributions: { executorRunTime: quantiles(1, 1), shuffleWriteMetrics: { writeBytes: quantiles(max, max) } },
    });
    assert.deepEqual(stageAlerts(query, stage(5.4 * GiB)).map((a) => a.name), ["maxPartitionToBig"]);
    assert.deepEqual(stageAlerts(query, stage(1.35 * GiB)), []);
  });

  it("splits small-file writes into partitioned and unpartitioned, with DataFlint's texts", () => {
    const write = (metrics: Record<string, string>) => ({
      nodeId: 0, nodeName: "Execute InsertIntoHadoopFsRelationCommand",
      metrics: Object.entries(metrics).map(([name, value]) => ({ name, value })),
    });
    const partitionedPlan = 'Execute InsertIntoHadoopFsRelationCommand file:/out, false, [bucket#494], Parquet, [path=/out], Overwrite, [a, bucket]';
    const flatPlan = "Execute InsertIntoHadoopFsRelationCommand file:/out, false, Parquet, [path=/out], Overwrite, [a]";
    const [partitioned] = nodeAlerts(query, write({ "number of written files": "2,400", "written output": "8.7 MiB", "number of dynamic part": "300" }), partitionedPlan);
    assert.equal(partitioned.name, "writeSmallFilesPartitioned");
    assert.ok(partitioned.message.startsWith("The avg files written for each partition is 8.0"));
    assert.ok(partitioned.suggestion.includes('Choose different partition keys instead of "bucket"'));
    const [flat] = nodeAlerts(query, write({ "number of written files": "187", "written output": "139.5 KiB" }), flatPlan);
    assert.equal(flat.name, "writeSmallUnpartitionedFiles");
    assert.ok(flat.suggestion.includes(".repartition(1). After repartition avg file size should be 139.5 KiB"));
    assert.deepEqual(nodeAlerts(query, write({ "number of written files": "100", "written output": "1 KiB" }), flatPlan), []);
  });

  it("measures filter conditions without attribute ids (>1000 chars)", () => {
    const node = { nodeId: 2, nodeName: "Filter", metrics: [] };
    const long = `Filter (${Array.from({ length: 60 }, (_, i) => `StartsWith(page#12, /page/${i}x)`).join(" OR ")})`;
    const [alert] = nodeAlerts(query, node, long);
    assert.equal(alert.name, "longFilterCondition");
    assert.equal(alert.message, `Condition length is ${filterCondition(long).length}, which is too long and can cause performance issues`);
    assert.deepEqual(nodeAlerts(query, node, "Filter (amount#3 > 150)"), []);
  });

  it("flags a large cross join above 1e10 scanned rows", () => {
    const join = (rows: string) => ({ nodeId: 4, nodeName: "BroadcastNestedLoopJoin", metrics: [{ name: "Cross Join Scanned Rows", value: rows }] });
    assert.deepEqual(nodeAlerts(query, join("12,000,000,000"), undefined).map((a) => a.name), ["largeCrossJoinScan"]);
    assert.deepEqual(nodeAlerts(query, join("9,000,000,000"), undefined), []);
  });

  it("sizes sort-merge join sides by shuffle write, as DataFlint does", () => {
    const graph = (smallShuffleWrite: string) => ({
      nodes: [
        { nodeId: 7, nodeName: "SortMergeJoin", metrics: [] },
        { nodeId: 9, nodeName: "Sort", metrics: [] },
        { nodeId: 10, nodeName: "Exchange", metrics: [
          { name: "data size", value: "total (min, med, max)\n686.6 MiB (1, 2, 3)" },
          { name: "shuffle bytes written", value: "total (min, med, max)\n801.1 MiB (1, 2, 3)" },
        ] },
        { nodeId: 16, nodeName: "Sort", metrics: [] },
        { nodeId: 17, nodeName: "Exchange", metrics: [
          { name: "data size", value: "total (min, med, max)\n7.2 MiB (1, 2, 3)" },
          { name: "shuffle bytes written", value: `total (min, med, max)\n${smallShuffleWrite} (1, 2, 3)` },
        ] },
      ],
      edges: [{ fromId: 9, toId: 7 }, { fromId: 10, toId: 9 }, { fromId: 16, toId: 7 }, { fromId: 17, toId: 16 }],
    });
    const [alert] = joinAlerts({ id: 0, description: "revenue_by_segment" }, graph("7.8 MiB"));
    assert.equal(alert.message, "Broadcast the smaller table (7.8 MiB) to avoid shuffling the larger table (801.1 MiB)");
    assert.deepEqual(joinAlerts(query, graph("200 MiB")), []);
  });
});

describe("catalogue", () => {
  it("adds the DataFlint tools without clashing with upstream tool names", () => {
    const upstream = new Set(TOOL_CATALOG.map((tool) => tool.name));
    for (const tool of DATAFLINT_TOOL_CATALOG) {
      assert.ok(tool.name.startsWith("dataflint_"));
      assert.ok(!upstream.has(tool.name));
    }
  });
});
