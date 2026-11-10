/**
 * DataFlint tools.
 *
 * DataFlint (https://github.com/dataflint/spark) is a Spark plugin whose jar,
 * placed on the History Server classpath, adds JSON endpoints under
 * `/history/{appId}/dataflint/...` and a UI tab. The UI computes its summary
 * page (DCU, idle cores, memory usage...) and its alerts in the browser; no
 * endpoint serves them. These tools recompute them server-side so an agent can
 * use them.
 *
 * Formulas, thresholds and alert texts (location, message, suggestion) follow
 * the DataFlint UI bundle served by `io.dataflint:spark_2.12` 0.9.x, and were
 * checked against that UI on a Spark 3.2.1 History Server. Checks DataFlint
 * does not have are marked `source: "custom"`.
 *
 * These tools are not part of the upstream Python server, so they live outside
 * the generated catalogue and are excluded from the parity harness.
 */

import type { SparkRestClient } from "../api/sparkClient.js";
import { NotFoundException } from "../api/httpClient.js";
import type { AppContext } from "../core/app.js";
import { ValueError } from "../core/errors.js";
import { getClientOrDefault } from "./tools.js";

type Args = Record<string, any>;
type Json = Record<string, any>;

const KiB = 1024;
const MiB = 1024 ** 2;
const GiB = 1024 ** 3;
const TiB = 1024 ** 4;

/* ------------------------------------------------------------------ */
/* DataFlint thresholds (from the DataFlint UI bundle)                 */
/* ------------------------------------------------------------------ */

export const THRESHOLDS = {
  idleCoresPct: 50,
  executorMemoryHighPct: 95,
  executorMemoryLowPct: 70,
  driverMemoryHighPct: 95,
  memoryBuffer: 0.2,
  skewMaxTaskMs: 5_000,
  skewRatio: 10,
  smallTasksMinTasks: 5_000,
  smallTasksMedianMs: 500,
  smallTasksTargetFactor: 10,
  smallFileBytes: 3 * MiB,
  smallFilesMinCount: 100,
  targetWriteFileBytes: 128 * MiB,
  longFilterChars: 1_000,
  largePartitionBytes: 5 * GiB,
  largeBroadcastBytes: 1 * GiB,
  crossJoinScannedRows: 1e10,
  /** Not a DataFlint alert: spill above this is reported as `source: "custom"`. */
  customSpillBytes: 1 * GiB,
} as const;

const QUANTILES = "0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0";
const MEDIAN = 5;
const MAX = 10;

/* ------------------------------------------------------------------ */
/* Parsing and formatting helpers (DataFlint's mu / hu / _u / vu)      */
/* ------------------------------------------------------------------ */

const UNIT_FACTORS: Record<string, number> = {
  B: 1, KiB, MiB, GiB, TiB, KB: KiB, MB: MiB, GB: GiB, TB: TiB,
  ms: 1, s: 1_000, m: 60_000, h: 3_600_000, min: 60_000,
};

/**
 * Total value of a Spark SQL metric string, in bytes, ms or a plain count.
 * Handles "5.4 GiB", "1,234", "1.2 m" and the multi-line
 * "total (min, med, max (stageId: taskId))\n5.4 GiB (...)" form.
 */
export function metricNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  let text = String(value);
  if (text.startsWith("total") && text.includes("\n")) {
    text = text.split("\n")[1];
  }
  const match = /^\s*([\d,.]+)\s*([A-Za-z]+)?/.exec(text);
  if (!match) return null;
  const number = parseFloat(match[1].replace(/,/g, ""));
  if (Number.isNaN(number)) return null;
  return number * (match[2] ? UNIT_FACTORS[match[2]] ?? 1 : 1);
}

/** Bytes from a Spark memory setting ("4g", "384m", "1024"); bare numbers are MiB. */
export function sparkSizeBytes(value: unknown, fallback: string): number {
  const text = String(value ?? fallback).trim().toLowerCase();
  const match = /^([\d.]+)\s*([kmgtp]?)b?$/.exec(text);
  if (!match) return sparkSizeBytes(fallback, fallback);
  const power = { "": 2, k: 1, m: 2, g: 3, t: 4, p: 5 }[match[2]] ?? 2;
  return parseFloat(match[1]) * 1024 ** power;
}

function scaled(bytes: number): [number, number] {
  let value = bytes;
  let unit = 0;
  while (Math.abs(value) >= 1024 && unit < 5) {
    value /= 1024;
    unit += 1;
  }
  return [parseFloat(value.toFixed(2)), unit];
}

/** "1.05 GiB", "7.8 GiB", "0 B": DataFlint's `hu`. */
export function humanBytes(bytes: number): string {
  const [value, unit] = scaled(bytes);
  return `${value} ${["B", "KiB", "MiB", "GiB", "TiB", "PiB"][unit]}`;
}

/** "4.8g", "384m": DataFlint's `_u`, a Spark config value. */
export function sparkFormatBytes(bytes: number): string {
  const [value, unit] = scaled(bytes);
  return `${value}${["B", "k", "m", "g", "t", "PB"][unit]}`;
}

/** "23.4s", "362ms", "2.0m": DataFlint's `vu`. */
export function humanMs(ms: number): string {
  if (ms >= 86_400_000) return `${(ms / 86_400_000).toFixed(1)}d`;
  if (ms >= 3_600_000) return `${(ms / 3_600_000).toFixed(1)}h`;
  if (ms >= 60_000) return `${(ms / 60_000).toFixed(1)}m`;
  if (ms >= 1_000) return `${(ms / 1_000).toFixed(1)}s`;
  return `${Math.round(ms)}ms`;
}

/** `e / t * 100` clamped to [0, 100], 0 when undefined; DataFlint's `xu`. */
function pct(part: number, whole: number): number {
  if (!whole || Number.isNaN(part)) return 0;
  return Math.min(Math.max((part / whole) * 100, 0), 100);
}

function round(value: number, digits: number): number {
  return Number(value.toFixed(digits));
}

/** Spark REST timestamps ("2026-11-04T05:19:21.350GMT") and ISO strings to epoch ms. */
function epochMs(value: unknown): number {
  return Date.parse(String(value).replace("GMT", "Z"));
}

function num(value: unknown): number {
  return value === null || value === undefined ? 0 : Number(value);
}

/* ------------------------------------------------------------------ */
/* Data access                                                         */
/* ------------------------------------------------------------------ */

const DATAFLINT_MISSING =
  "DataFlint endpoints were not found for this application. Is the DataFlint " +
  "jar (io.dataflint:spark_2.12) on the Spark History Server classpath?";

async function dataflint<T = any>(
  client: SparkRestClient,
  appId: string,
  endpoint: string,
  query: Record<string, string | number> = {},
): Promise<T | null> {
  try {
    return await client.getDataflintJson<T>(appId, endpoint, query);
  } catch (error) {
    if (error instanceof NotFoundException) return null;
    throw error;
  }
}

interface AppData {
  appId: string;
  /** YARN attempt id ("1", "2"...) or null for local/standalone apps. */
  attemptId: string | null;
  /** "{appId}" or "{appId}/{attemptId}": the id segment for REST and DataFlint paths. */
  path: string;
  attemptCount: number;
  name: string;
  sparkVersion: string;
  completed: boolean;
  dataflintAvailable: boolean;
  driverXmxBytes: number | null;
  startMs: number;
  endMs: number;
  environment: Json;
  executors: Json[];
  stages: Json[];
  jobs: Json[];
  /** Spark's /sql list, which includes each query's plan graph (nodes with metrics, edges). */
  sql: Json[];
}

async function loadAppData(
  client: SparkRestClient,
  appId: string,
  requestedAttemptId: string | null = null,
): Promise<AppData> {
  // YARN (CDP) applications have attempt ids ("1", "2", ...), and the History
  // Server only serves them at /applications/{appId}/{attemptId}/... and
  // /history/{appId}/{attemptId}/dataflint/...; local/standalone apps have none.
  // The History Server lists attempts newest first.
  const application = await client.getAppJson<Json>(appId, "");
  const attempts: Json[] = application.attempts ?? [];
  const sparkAttempt = requestedAttemptId
    ? attempts.find((a) => String(a.attemptId) === String(requestedAttemptId))
    : attempts[0];
  if (!sparkAttempt) {
    const known = attempts.map((a) => a.attemptId).filter(Boolean).join(", ") || "none";
    throw new ValueError(
      `Application '${appId}' has no attempt '${requestedAttemptId}' (attempts: ${known}).`,
    );
  }
  const attemptId: string | null = sparkAttempt.attemptId ? String(sparkAttempt.attemptId) : null;
  const path = attemptId ? `${appId}/${attemptId}` : appId;

  const [info, environment, executors, stages, jobs, sql] =
    await Promise.all([
      dataflint<Json>(client, path, "applicationinfo"),
      client.getAppJson<Json>(path, "/environment"),
      client.getAppJson<Json[]>(path, "/allexecutors"),
      client.getAppJson<Json[]>(path, "/stages", {
        withSummaries: "true",
        quantiles: QUANTILES,
      }),
      client.getAppJson<Json[]>(path, "/jobs"),
      client.getAppJson<Json[]>(path, "/sql", {
        offset: 0,
        length: 10_000,
        planDescription: "false",
      }),
    ]);

  // DataFlint's UI takes start and end from its applicationinfo endpoint, which
  // has whole-second precision; using it keeps idle cores identical to the UI.
  const dfAttempt = info?.info?.attempts?.[0];
  const attempt = dfAttempt ?? sparkAttempt;
  const completed = Boolean(sparkAttempt.completed);
  const startMs = epochMs(attempt.startTime);
  const rawEnd = String(attempt.endTime ?? "");
  const endMs =
    completed && rawEnd && !rawEnd.startsWith("1969-12-31") ? epochMs(rawEnd) : Date.now();

  return {
    appId,
    attemptId,
    path,
    attemptCount: attempts.length,
    name: application.name,
    sparkVersion: sparkAttempt.appSparkVersion,
    completed,
    dataflintAvailable: info !== null,
    driverXmxBytes: info?.environmentInfo?.driverXmxBytes ?? null,
    startMs,
    endMs,
    environment,
    executors,
    stages: stages.filter((stage) => stage.status !== "SKIPPED"),
    jobs,
    sql,
  };
}

/* ------------------------------------------------------------------ */
/* Resources: idle cores, memory usage, DCU                            */
/* ------------------------------------------------------------------ */

export interface ExecutorUsage {
  id: string;
  isDriver: boolean;
  addMs: number;
  endMs: number;
  durationMs: number;
  totalCores: number;
  maxTasks: number;
  totalTaskDurationMs: number;
  heapPeakBytes: number;
  memoryUsagePct: number;
}

export type ResourceControlType = "databricks" | "local" | "dynamic" | "static" | "unknown";

export interface ResourceConfig {
  executorMemoryBytes: number;
  executorMemoryOverheadBytes: number;
  executorContainerMemoryBytes: number;
  driverMemoryBytes: number;
  /** spark.executor.memory as configured ("1g" if unset); DataFlint quotes it verbatim. */
  executorMemoryConfigValue: string;
  resourceControlType: ResourceControlType;
}

/** DataFlint's memory model: executor memory + max(memory x factor, memoryOverhead). */
export function resourceConfig(sparkProperties: Array<[string, string]>): ResourceConfig {
  const props = Object.fromEntries(sparkProperties ?? []);
  const executorMemory = sparkSizeBytes(props["spark.executor.memory"], "1g");
  const factor = parseFloat(
    props["spark.kubernetes.memoryOverheadFactor"] ??
      props["spark.executor.memoryOverheadFactor"] ??
      "0.1",
  );
  const overhead = Math.max(
    executorMemory * factor,
    sparkSizeBytes(props["spark.executor.memoryOverhead"], "384m"),
  );
  const master = props["spark.master"];
  const resourceControlType: ResourceControlType =
    props["spark.databricks.clusterUsageTags.cloudProvider"] !== undefined ? "databricks"
      : master !== undefined && master.startsWith("local") ? "local"
        : props["spark.dynamicAllocation.enabled"] === "true" ? "dynamic"
          : props["spark.executor.instances"] !== undefined ? "static"
            : "unknown";
  return {
    executorMemoryBytes: executorMemory,
    executorMemoryOverheadBytes: overhead,
    executorContainerMemoryBytes: executorMemory + overhead,
    driverMemoryBytes: sparkSizeBytes(props["spark.driver.memory"], "1g"),
    executorMemoryConfigValue: props["spark.executor.memory"] ?? "1g",
    resourceControlType,
  };
}

/**
 * Executors with lifetimes ending at removeTime, or at the app end if still
 * registered, and heap usage as peak JVM heap / spark.executor.memory.
 */
export function executorUsage(
  executors: Json[],
  appStartMs: number,
  appEndMs: number,
  executorMemoryBytes = GiB,
): ExecutorUsage[] {
  const usage: ExecutorUsage[] = executors.map((executor) => {
    const add = epochMs(executor.addTime);
    const end = executor.removeTime ? epochMs(executor.removeTime) : appEndMs;
    const heap = num(executor.peakMemoryMetrics?.JVMHeapMemory);
    return {
      id: String(executor.id),
      isDriver: executor.id === "driver",
      addMs: add,
      endMs: end,
      durationMs: end - add,
      totalCores: num(executor.totalCores),
      maxTasks: num(executor.maxTasks),
      totalTaskDurationMs: num(executor.totalDuration),
      heapPeakBytes: heap,
      memoryUsagePct: pct(heap, executorMemoryBytes),
    };
  });
  if (!usage.some((executor) => executor.isDriver)) {
    usage.push({
      id: "driver", isDriver: true, addMs: appStartMs, endMs: appEndMs,
      durationMs: appEndMs - appStartMs, totalCores: 0, maxTasks: 0,
      totalTaskDurationMs: 0, heapPeakBytes: 0, memoryUsagePct: 0,
    });
  }
  return usage;
}

const hours = (ms: number) => ms / 3_600_000;

/**
 * Idle cores %, memory usage % and DCU exactly as DataFlint's UI computes them.
 * Idle cores and memory usage count real executors; the driver is used only
 * when there are none (local mode), where memory usage is reported as 0.
 */
export function resourceMetrics(usage: ExecutorUsage[], config: ResourceConfig) {
  const workers = usage.filter((executor) => !executor.isDriver);
  const driver = usage.find((executor) => executor.isDriver)!;
  const localMode = workers.length === 0;
  const taskMs = localMode
    ? driver.totalTaskDurationMs
    : workers.reduce((sum, executor) => sum + executor.totalTaskDurationMs, 0);
  const potentialMs = localMode
    ? driver.durationMs * driver.maxTasks
    : workers.reduce((sum, executor) => sum + executor.durationMs * executor.maxTasks, 0);
  const coreHours = usage.reduce(
    (sum, executor) => sum + executor.totalCores * hours(executor.durationMs),
    0,
  );
  const executorMemoryGibHours = workers.reduce(
    (sum, executor) =>
      sum + (config.executorContainerMemoryBytes / GiB) * hours(executor.durationMs),
    0,
  );
  const driverMemoryGibHours = (config.driverMemoryBytes / GiB) * hours(driver.durationMs);
  const memoryGibHours = executorMemoryGibHours + driverMemoryGibHours;
  return {
    localMode,
    executors: workers.length,
    cores: localMode ? driver.totalCores : workers.reduce((s, e) => s + e.totalCores, 0),
    taskTimeMs: taskMs,
    potentialTaskTimeMs: potentialMs,
    idleCoresPct: 100 - pct(taskMs, potentialMs),
    maxExecutorMemoryPct: workers.length ? Math.max(...workers.map((e) => e.memoryUsagePct)) : 0,
    maxExecutorMemoryBytes: workers.length ? Math.max(...workers.map((e) => e.heapPeakBytes)) : 0,
    driverHeapPeakBytes: driver.heapPeakBytes,
    coreHours,
    memoryGibHours,
    dcu: 0.05 * coreHours + 0.005 * memoryGibHours,
  };
}

/** DCU of one SQL query: each executor's lifetime overlap with the query x cores / memory (DataFlint's `Up`). */
export function queryDcu(
  usage: ExecutorUsage[],
  config: ResourceConfig,
  startMs: number,
  durationMs: number,
): number {
  const endMs = startMs + durationMs;
  let coreHours = 0;
  let memoryHours = 0;
  for (const executor of usage) {
    const overlap = Math.max(0, Math.min(executor.endMs, endMs) - Math.max(executor.addMs, startMs));
    const memoryGib = (executor.isDriver ? config.driverMemoryBytes : config.executorContainerMemoryBytes) / GiB;
    coreHours += hours(overlap * executor.totalCores);
    memoryHours += hours(overlap * memoryGib);
  }
  return 0.05 * coreHours + 0.005 * memoryHours;
}

/* ------------------------------------------------------------------ */
/* Summary page                                                        */
/* ------------------------------------------------------------------ */

function sqlStageMap(sql: Json[], jobs: Json[]): Map<number, number[]> {
  const stagesByJob = new Map<number, number[]>(
    jobs.map((job) => [num(job.jobId), (job.stageIds ?? []).map(num)]),
  );
  const result = new Map<number, number[]>();
  for (const query of sql) {
    const jobIds = [
      ...(query.successJobIds ?? []),
      ...(query.failedJobIds ?? []),
      ...(query.runningJobIds ?? []),
    ].map(num);
    const stageIds = new Set<number>();
    for (const jobId of jobIds) {
      for (const stageId of stagesByJob.get(jobId) ?? []) stageIds.add(stageId);
    }
    result.set(num(query.id), [...stageIds].sort((a, b) => a - b));
  }
  return result;
}

/** Latest attempt of each stage, by stage id. */
function latestStages(stages: Json[]): Map<number, Json> {
  const byId = new Map<number, Json>();
  for (const stage of stages) {
    const previous = byId.get(num(stage.stageId));
    if (!previous || num(stage.attemptId) > num(previous.attemptId)) {
      byId.set(num(stage.stageId), stage);
    }
  }
  return byId;
}

function summarize(data: AppData) {
  const config = resourceConfig(data.environment.sparkProperties);
  const usage = executorUsage(data.executors, data.startMs, data.endMs, config.executorMemoryBytes);
  const resources = resourceMetrics(usage, config);
  const sum = (field: string) => data.stages.reduce((total, stage) => total + num(stage[field]), 0);
  const totals = {
    input: sum("inputBytes"),
    output: sum("outputBytes"),
    shuffleRead: sum("shuffleReadBytes"),
    shuffleWrite: sum("shuffleWriteBytes"),
    spill: sum("diskBytesSpilled"),
    tasks: sum("numTasks"),
    failed: sum("numFailedTasks"),
  };
  const durationMs = data.endMs - data.startMs;
  const stageIdsByQuery = sqlStageMap(data.sql, data.jobs);
  const allStages = data.stages;

  const queries = [...data.sql]
    .sort((a, b) => num(b.duration) - num(a.duration))
    .map((query) => {
      const ids = new Set(stageIdsByQuery.get(num(query.id)) ?? []);
      const stages = allStages.filter((stage) => ids.has(num(stage.stageId)));
      const dcu = queryDcu(usage, config, epochMs(query.submissionTime), num(query.duration));
      return {
        id: query.id,
        status: query.status,
        description: query.description,
        duration: humanMs(num(query.duration)),
        duration_pct: round(pct(num(query.duration), durationMs), 1),
        dcu: round(dcu, 4),
        dcu_pct: round(Math.min(100, resources.dcu ? (dcu / resources.dcu) * 100 : 0), 1),
        input: humanBytes(stages.reduce((s, stage) => s + num(stage.inputBytes), 0)),
        output: humanBytes(stages.reduce((s, stage) => s + num(stage.outputBytes), 0)),
      };
    });

  return {
    config,
    usage,
    resources,
    summary: {
      app_id: data.appId,
      app_attempt_id: data.attemptId,
      attempts: data.attemptCount,
      name: data.name,
      spark_version: data.sparkVersion,
      completed: data.completed,
      dataflint_available: data.dataflintAvailable,
      // The ten tiles of DataFlint's summary page, in UI order.
      duration: humanMs(durationMs),
      dcu: round(resources.dcu, 4),
      input: humanBytes(totals.input),
      output: humanBytes(totals.output),
      memory_usage_pct: round(resources.maxExecutorMemoryPct, 2),
      shuffle_read: humanBytes(totals.shuffleRead),
      shuffle_write: humanBytes(totals.shuffleWrite),
      spill_to_disk: humanBytes(totals.spill),
      idle_cores_pct: round(resources.idleCoresPct, 2),
      task_error_rate_pct: round(pct(totals.failed, totals.tasks), 2),
      // Details behind the tiles.
      details: {
        duration_s: round(durationMs / 1000, 1),
        resource_control: config.resourceControlType,
        executors: resources.executors,
        cores: resources.cores,
        core_hours: round(resources.coreHours, 4),
        memory_gib_hours: round(resources.memoryGibHours, 4),
        executor_memory: sparkFormatBytes(config.executorMemoryBytes),
        executor_memory_overhead: sparkFormatBytes(config.executorMemoryOverheadBytes),
        executor_container_memory: sparkFormatBytes(config.executorContainerMemoryBytes),
        driver_memory: sparkFormatBytes(config.driverMemoryBytes),
        max_executor_heap_used: humanBytes(resources.maxExecutorMemoryBytes),
        driver_heap_used: humanBytes(resources.driverHeapPeakBytes),
        driver_heap_usage_pct:
          data.driverXmxBytes ? round(pct(resources.driverHeapPeakBytes, data.driverXmxBytes), 2) : null,
        total_tasks: totals.tasks,
        failed_tasks: totals.failed,
        memory_usage_note: resources.localMode
          ? "local mode: DataFlint reports executor memory usage only for real executors, so it is 0"
          : undefined,
      },
      // The SQL table under the tiles: id, status, description, duration, DCU, input, output.
      sql_queries: queries,
    },
  };
}

/* ------------------------------------------------------------------ */
/* Alerts                                                              */
/* ------------------------------------------------------------------ */

export interface Alert {
  name: string;
  title: string;
  type: "warning" | "error";
  source: "dataflint" | "custom";
  location: string;
  message: string;
  suggestion: string;
  short_suggestion?: string;
  evidence: Record<string, unknown>;
}

const dist = (stage: Json, ...path: string[]): number[] | undefined => {
  let node: any = stage.taskMetricsDistributions;
  for (const key of path) node = node?.[key];
  return Array.isArray(node) ? node.map(num) : undefined;
};

const stageLocation = (query: Json, stageId: unknown) =>
  `In: SQL query "${query.description}" (id: ${query.id}), Stage ${stageId}`;
const nodeLocation = (query: Json, nodeName: string) =>
  `In: SQL query "${query.description}" (id: ${query.id}) and node "${nodeName}"`;

/** Application-level rules: idle cores and memory provisioning. */
export function resourceAlerts(
  resources: ReturnType<typeof resourceMetrics>,
  config: ResourceConfig,
  driverXmxBytes: number | null,
): Alert[] {
  const alerts: Alert[] = [];
  const buffer = 1 + THRESHOLDS.memoryBuffer;

  if (resources.maxExecutorMemoryBytes !== 0) {
    const usage = resources.maxExecutorMemoryPct;
    const current = config.executorMemoryConfigValue;
    if (usage > THRESHOLDS.executorMemoryHighPct) {
      const target = sparkFormatBytes(config.executorMemoryBytes * buffer);
      alerts.push({
        name: "executorMemoryTooHigh",
        title: "Executor Memory Under-Provisioned",
        type: "error",
        source: "dataflint",
        location: "In: Summery Page -> Memory Utilization",
        message: `Max Executor Memory usage is ${usage.toFixed(2)}% which is too high, and can cause spills and OOMs`,
        suggestion: `\n        1. Increase executor memory provisioning by changing "spark.executor.memory" to ${target}\n           (the current usage is ${usage.toFixed(2)}% but set to higher as it needs some buffer)\n           from current value "${current}"`,
        evidence: { max_executor_memory_pct: round(usage, 2), max_heap_bytes: resources.maxExecutorMemoryBytes, spark_executor_memory: current },
      });
    } else if (usage < THRESHOLDS.executorMemoryLowPct) {
      const target = sparkFormatBytes(resources.maxExecutorMemoryBytes * buffer);
      alerts.push({
        name: "executorMemoryTooLow",
        title: "Executor Memory Over-Provisioned",
        type: "warning",
        source: "dataflint",
        location: "In: Summery Page -> Memory Utilization",
        message: `Max executor memory usage is only ${usage.toFixed(2)}%, which means you can provision less memory and save $$$`,
        suggestion: `\n        1. Decrease executor memory provisioning by changing "spark.executor.memory" to ${target}\n           (the current usage is ${usage.toFixed(2)}% but set to higher as it needs some buffer)\n           from current value "${current}"`,
        evidence: { max_executor_memory_pct: round(usage, 2), max_heap_bytes: resources.maxExecutorMemoryBytes, spark_executor_memory: current },
      });
    }
  }

  if (driverXmxBytes && resources.driverHeapPeakBytes) {
    const usage = pct(resources.driverHeapPeakBytes, driverXmxBytes);
    if (usage > THRESHOLDS.driverMemoryHighPct) {
      alerts.push({
        name: "driverMemoryTooHigh",
        title: "Driver Memory Under-Provisioned",
        type: "error",
        source: "dataflint",
        location: "In: Summery Page -> Memory Utilization",
        message: `Max Driver Memory usage is ${usage.toFixed(2)}% which is too high, and can cause spills and OOMs`,
        suggestion: `\n        1. Increase driver memory provisioning by changing "spark.driver.memory" to ${sparkFormatBytes(driverXmxBytes * buffer)}\n           (the current usage is ${usage.toFixed(2)}% but set to higher as it needs some buffer)\n           from current value "${sparkFormatBytes(driverXmxBytes)}"`,
        evidence: { driver_memory_pct: round(usage, 2), driver_heap_bytes: resources.driverHeapPeakBytes, driver_xmx_bytes: driverXmxBytes },
      });
    }
  }

  if (resources.idleCoresPct > THRESHOLDS.idleCoresPct) {
    const idle = resources.idleCoresPct;
    const suggestion =
      config.resourceControlType === "databricks"
        ? "Reduce your cluster size or machine type via databricks cluster UI"
        : config.resourceControlType === "static"
          ? "1. decrease amount of cores per executor by lowering spark.executor.cores\n      2. decrease amount of executors by lowering spark.executor.instances OR if using dynamic allocation by tuning  ."
          : config.resourceControlType === "dynamic"
            ? "1. decrease amount of cores per executor by lowering spark.executor.cores\n      2. tune your Dynamic Allocation config, specifically lower spark.dynamicAllocation.executorAllocationRatio or increase spark.dynamicAllocation.schedulerBacklogTimeout"
            : "decrease amount of cores or executors";
    alerts.push({
      name: "idleCoresTooHigh",
      title: "Idle Cores Too High",
      type: "warning",
      source: "dataflint",
      location: "In: Summery Page -> Idle Cores",
      message: `Idle Cores is ${idle.toFixed(2)}% which is too high, and suggest your cluster is over-provisioned on cores or executors`,
      suggestion,
      evidence: { idle_cores_pct: round(idle, 2), cores: resources.cores, resource_control: config.resourceControlType },
    });
  }
  return alerts;
}

/** Stage-level rules: partition skew, small tasks, large partition, and (custom) spill. */
export function stageAlerts(query: Json, stage: Json): Alert[] {
  const alerts: Alert[] = [];
  const where = stageLocation(query, stage.stageId);

  const runTime = dist(stage, "executorRunTime");
  if (runTime && runTime.length > MAX) {
    const median = runTime[MEDIAN];
    const max = runTime[MAX];
    if (max > THRESHOLDS.skewMaxTaskMs && median !== 0 && max / median > THRESHOLDS.skewRatio) {
      alerts.push({
        name: "partitionSkew",
        title: "Partition Skew",
        type: "warning",
        source: "dataflint",
        location: where,
        message: `Partition skew ratio of ${(max / median).toFixed(1)}X, median task duration is ${humanMs(median)} and max task duration is ${humanMs(max)}`,
        suggestion:
          "\n  1. Fix the partition skew, by changing the repartition your data differently\n" +
          "  2. Do not fix the partition skew, and instead decrease number of executors/cores, so you will have less resource waste\n",
        evidence: { median_task_ms: median, max_task_ms: max, num_tasks: num(stage.numTasks) },
      });
    }
    if (num(stage.numTasks) > THRESHOLDS.smallTasksMinTasks && median < THRESHOLDS.smallTasksMedianMs) {
      const target = Math.ceil(num(stage.numTasks) / THRESHOLDS.smallTasksTargetFactor);
      alerts.push({
        name: "smallTasks",
        title: "Large Number Of Small Tasks",
        type: "warning",
        source: "dataflint",
        location: where,
        message: `${num(stage.numTasks)} tasks with median task duration of ${humanMs(median)}, which causes large scheduling overhead for Spark`,
        suggestion:
          `\n  1. Repartition to less tasks, so you will have less overhead, by running .repartition(${target})\n` +
          `  2. Instead of repartition, you can run .coallese(${target}) to decrease the number of tasks without shuffling on the expense of less parallelism\n` +
          `  3. If you need to hash-partition, call repartition like this: .repartition(${target}, "hash_key1", "hash_key2")\n`,
        short_suggestion: `.repartition(${target}) before this transformation`,
        evidence: { num_tasks: num(stage.numTasks), median_task_ms: median },
      });
    }
  }

  const candidates: Array<[string, number[] | undefined]> = [
    ["output", dist(stage, "outputMetrics", "bytesWritten")],
    ["input", dist(stage, "inputMetrics", "bytesRead")],
    ["shuffle write", dist(stage, "shuffleWriteMetrics", "writeBytes")],
    ["shuffle read", dist(stage, "shuffleReadMetrics", "readBytes")],
  ];
  for (const [kind, values] of candidates) {
    if (values && values.length > MAX && values[MAX] > THRESHOLDS.largePartitionBytes) {
      alerts.push({
        name: "maxPartitionToBig",
        title: "Large Partition Size",
        type: "warning",
        source: "dataflint",
        location: where,
        message: `Maximum ${kind} partition size in stage ${stage.stageId} is ${humanBytes(values[MAX])}, which is too big and can cause performance issues or OOM errors`,
        suggestion:
          "\n  1. Increase the number of partitions to reduce the size of each partition\n" +
          "  2. Use more specific partitioning keys to distribute data more evenly\n",
        evidence: { kind, max_task_bytes: values[MAX], median_task_bytes: values[MEDIAN] },
      });
      break;
    }
  }

  const spill = num(stage.diskBytesSpilled);
  if (spill > THRESHOLDS.customSpillBytes) {
    alerts.push({
      name: "diskSpill",
      title: "Spill To Disk",
      type: "warning",
      source: "custom",
      location: where,
      message: `${humanBytes(spill)} spilled to disk`,
      suggestion: "Usually a symptom of skew or large partitions; otherwise raise partitions or executor memory",
      evidence: { disk_spill_bytes: spill, memory_spill_bytes: num(stage.memoryBytesSpilled) },
    });
  }
  return alerts;
}

const metricsOf = (node: Json): Record<string, string> =>
  Object.fromEntries((node.metrics ?? []).filter((m: Json) => m.value).map((m: Json) => [m.name, m.value]));

/** Drop Spark attribute ids ("page#12", "id#3L"); DataFlint's `Pd`. */
export function stripAttributeIds(text: string): string {
  return text.replace(/#\d+L/g, "").replace(/#\d+/g, "");
}

/** Split on top-level commas, outside [...] and "..."; DataFlint's write-plan tokenizer. */
function splitTopLevel(text: string): string[] {
  const parts: string[] = [];
  let current = "";
  let depth = 0;
  let quoted = false;
  for (const ch of text) {
    if (ch === "[") depth++;
    if (ch === "]") depth--;
    if (ch === '"') quoted = !quoted;
    if (ch === "," && depth === 0 && !quoted) {
      parts.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }
  if (current) parts.push(current.trim());
  return parts;
}

/**
 * Partition columns of an InsertIntoHadoopFsRelationCommand plan line, or
 * undefined if unpartitioned. Mirrors DataFlint's `ep` exactly, including not
 * trimming keys after splitting on ",".
 */
export function writePartitionKeys(plan: string): string[] | undefined {
  const tokens = splitTopLevel(plan.replace("Execute InsertIntoHadoopFsRelationCommand", "").trim());
  if (tokens.length < 3 || !tokens[2].includes("[")) return undefined;
  return stripAttributeIds(tokens[2].slice(1, -1)).split(",");
}

/** The filter condition DataFlint measures; its `Yd` + `Pd`. */
export function filterCondition(plan: string): string {
  let text = plan;
  for (const prefix of ["Filter ", "PhotonFilter ", "GpuFilter ", "CometFilter ", "FilterExecTransformer "]) {
    if (text.startsWith(prefix)) text = text.slice(prefix.length);
  }
  if (text.startsWith("(")) {
    text = text.slice(1);
    if (text.endsWith(")")) text = text.slice(0, -1);
  }
  return stripAttributeIds(text);
}

/**
 * DataFlint shows (and its rules read) node metrics under friendlier names,
 * e.g. "shuffle bytes written" -> "shuffle write", "data size" -> "shuffle data size".
 */
const METRIC_RENAMES: Record<string, string> = {
  "number of output rows": "rows", "output rows": "rows", "number of written files": "files written",
  "written output": "bytes written", "number of files read": "files read", "size of files read": "bytes read",
  "num bytes read": "bytes read", "num bytes written": "bytes written", "number of partitions read": "partitions read",
  "number of dynamic part": "partitions written", "number of partitions": "partitions",
  "shuffle bytes written": "shuffle write", "partition data size": "data size", "data size": "shuffle data size",
  "records read": "shuffle records read", "local bytes read": "shuffle read (local)",
  "remote bytes read": "shuffle read (remote)",
};

/** A node's metric looked up by DataFlint's (renamed) metric name. */
function renamedMetric(node: Json, ...names: string[]): string | undefined {
  const metric = (node.metrics ?? []).find((m: Json) => names.includes(METRIC_RENAMES[m.name] ?? m.name));
  return metric?.value;
}

/**
 * Node-level rules on one SQL query's plan graph: small files read/written,
 * long filter conditions, large broadcasts and large cross joins.
 * `sparkNode` comes from Spark's /sql graph (name + metrics); `plan` is
 * DataFlint's plan text for the same node id, when available.
 */
export function nodeAlerts(query: Json, sparkNode: Json, plan: string | undefined): Alert[] {
  const alerts: Alert[] = [];
  const name = String(sparkNode.nodeName);
  const where = nodeLocation(query, name);
  const metrics = metricsOf(sparkNode);

  const filesRead = metricNumber(metrics["number of files read"]);
  const bytesRead = metricNumber(metrics["size of files read"]);
  if (filesRead && bytesRead && filesRead > THRESHOLDS.smallFilesMinCount &&
      bytesRead / filesRead < THRESHOLDS.smallFileBytes) {
    alerts.push({
      name: "readSmallFiles",
      title: "Reading Small Files",
      type: "warning",
      source: "dataflint",
      location: where,
      message: `The average file size is ${humanBytes(bytesRead / filesRead)}, which is too small and can cause performance issues`,
      suggestion:
        "\n    1. Ask the data owner to increase the file size\n" +
        "    2. Decrease number of executors, so each executor will read more small files and reduce the resource overhead\n                        ",
      short_suggestion: "Ask the data owner to increase the file size",
      evidence: { files_read: filesRead, bytes_read: bytesRead },
    });
  }

  const filesWritten = metricNumber(metrics["number of written files"]);
  const bytesWritten = metricNumber(metrics["written output"]);
  if (filesWritten && bytesWritten && filesWritten > THRESHOLDS.smallFilesMinCount &&
      bytesWritten / filesWritten < THRESHOLDS.smallFileBytes) {
    const avg = humanBytes(bytesWritten / filesWritten);
    const keys = plan ? writePartitionKeys(plan) : undefined;
    if (keys !== undefined && keys.length > 0) {
      const partitions = metricNumber(metrics["number of dynamic part"]) ?? 0;
      const perPartition = partitions ? (filesWritten / partitions).toFixed(1) : "0";
      const sizeAfter = partitions ? humanBytes(bytesWritten / partitions) : "0";
      const quoted = keys.map((key) => `"${key}"`).join(",");
      alerts.push({
        name: "writeSmallFilesPartitioned",
        title: "Writing Small Partitioned Files",
        type: "warning",
        source: "dataflint",
        location: where,
        message: `The avg files written for each partition is ${perPartition}, and the avg file size is ${avg}, which is too small and can cause performance issues`,
        suggestion:
          `\n        1. Do a repartition by your partition key before writing the data, by running .repartition(${quoted}) before writing the dataframe. Avg file size after this change should be ${sizeAfter}\n` +
          `        2. Choose different partition key${quoted.length === 1 ? "" : "s"} instead of ${quoted} with lower cardinality`,
        short_suggestion: `.repartition(${quoted}) before writing`,
        evidence: { files_written: filesWritten, bytes_written: bytesWritten, partitions_written: partitions, partition_keys: keys },
      });
    } else {
      const target = Math.ceil(bytesWritten / THRESHOLDS.targetWriteFileBytes);
      alerts.push({
        name: "writeSmallUnpartitionedFiles",
        title: "Writing Small Unpartitioned Files",
        type: "warning",
        source: "dataflint",
        location: where,
        message: `The average file size is ${avg}, which is too small and can cause performance issues`,
        suggestion: `\n        1. Do a repartition before writing the data, by running .repartition(${target}). After repartition avg file size should be ${humanBytes(bytesWritten / target)}`,
        short_suggestion: `.repartition(${target}) before write`,
        evidence: { files_written: filesWritten, bytes_written: bytesWritten },
      });
    }
  }

  const condition = name === "Filter" && plan ? filterCondition(plan) : null;
  if (condition !== null && condition.length > THRESHOLDS.longFilterChars) {
    alerts.push({
      name: "longFilterCondition",
      title: "Long Filter Condition",
      type: "warning",
      source: "dataflint",
      location: where,
      message: `Condition length is ${condition.length}, which is too long and can cause performance issues`,
      suggestion:
        "\n    1. Try to convert your filter condition to a join statement, by creating a DF of your filter condition and inner joining it with your main DF\n" +
        "    2. Consider rewriting your filter condition to be shorter\n                        ",
      evidence: { condition_chars: condition.length },
    });
  }

  if (name === "BroadcastExchange") {
    // DataFlint reads its renamed "data size", i.e. Spark's "partition data size".
    const size = metricNumber(renamedMetric(sparkNode, "data size")) ?? 0;
    if (size > THRESHOLDS.largeBroadcastBytes) {
      alerts.push({
        name: "largeBroadcast",
        title: "Large data Broadcast",
        type: "warning",
        source: "dataflint",
        location: where,
        message: `The data broadcast size is ${humanBytes(size)}, which exceeds the 1GB threshold and can cause performance issues`,
        suggestion:
          "\n    1. spark.sql.autoBroadcastJoinThreshold config might be set to a large number which is not optimal\n" +
          "    2. The broadcast hint is applied on a large dataframe which is not optimal",
        evidence: { broadcast_bytes: size },
      });
    }
  }

  if (name === "BroadcastNestedLoopJoin" || name === "CartesianProduct") {
    const rows = metricNumber(metrics["Cross Join Scanned Rows"]);
    if (rows !== null && rows > THRESHOLDS.crossJoinScannedRows) {
      alerts.push({
        name: "largeCrossJoinScan",
        title: "Large Cross Join Scan",
        type: "error",
        source: "dataflint",
        location: where,
        message: `Cross join is scanning ${rows.toLocaleString("en-US")} rows, which is too large and can cause performance issues or query failure`,
        suggestion:
          "\n  1. Add specific join conditions to convert the cross join to a more efficient join type\n" +
          "  2. Avoid using cross joins for large datasets\n",
        evidence: { scanned_rows: rows },
      });
    }
  }
  return alerts;
}

const SIZE_METRICS = ["bytes written", "shuffle write", "partition size", "data size"];
const PASS_THROUGH = new Set(["AQEShuffleRead", "CustomShuffleReader", "Coalesce", "Sort", "Exchange", "Filter"]);

/**
 * Size flowing into a plan node, DataFlint's `Xu`: look at the node's single
 * child; use the child's size metric (`Ku`, by renamed name), or keep walking
 * down while the child is a pass-through node.
 */
function inputSize(nodeId: number, nodes: Map<number, Json>, children: Map<number, number[]>): number | null {
  const kids = children.get(nodeId) ?? [];
  if (kids.length !== 1) return null;
  const child = nodes.get(kids[0]);
  if (!child) return null;
  const value = metricNumber(renamedMetric(child, ...SIZE_METRICS));
  if (value) return value;
  return PASS_THROUGH.has(child.nodeName) ? inputSize(kids[0], nodes, children) : null;
}

/** DataFlint's "Broadcast small table in Sort Merge join" rule on Spark's SQL graph. */
export function joinAlerts(query: Json, details: Json): Alert[] {
  const nodes = new Map<number, Json>((details.nodes ?? []).map((n: Json) => [num(n.nodeId), n]));
  const children = new Map<number, number[]>();
  for (const edge of details.edges ?? []) {
    const parent = num(edge.toId);
    children.set(parent, [...(children.get(parent) ?? []), num(edge.fromId)]);
  }
  const alerts: Alert[] = [];
  for (const node of nodes.values()) {
    if (node.nodeName !== "SortMergeJoin") continue;
    const kids = children.get(num(node.nodeId)) ?? [];
    if (kids.length !== 2) continue;
    const sizes = kids.map((kid) => inputSize(kid, nodes, children));
    if (sizes.some((size) => size === null)) continue;
    const small = Math.min(sizes[0]!, sizes[1]!);
    const large = Math.max(sizes[0]!, sizes[1]!);
    if (small < 10 * MiB || (small < 100 * MiB && large > 10 * GiB) ||
        (small < 1000 * MiB && large > 300 * GiB) || (small < 5 * GiB && large > TiB)) {
      alerts.push({
        name: "broadcastSmallTable",
        title: "Broadcast small table in Sort Merge join",
        type: "warning",
        source: "dataflint",
        location: nodeLocation(query, node.nodeName),
        message: `Broadcast the smaller table (${humanBytes(small)}) to avoid shuffling the larger table (${humanBytes(large)})`,
        suggestion:
          "\n    1. Use broadcast(small_table_df) hint on the smaller table\n" +
          `    2. Set spark.sql.autoBroadcastJoinThreshold to a value larger than ${humanBytes(small)}`,
        short_suggestion: "broadcast(small_table_df small table before join",
        evidence: { small_side_bytes: small, large_side_bytes: large, node_id: num(node.nodeId) },
      });
    }
  }
  return alerts;
}

/**
 * DataFlint's own view of a query graph. With spark.dataflint.instrument.* on,
 * Spark reports nodes as "DataFlintFilter", "DataFlintScan parquet", ...; the
 * UI folds a "DataFlint<X>" wrapper into its single child of the same operator
 * (merging metrics, rewiring edges), then strips the "DataFlint" prefix (`op`).
 */
export function normalizeGraph(query: Json): { nodes: Json[]; edges: Json[] } {
  let nodes: Json[] = (query.nodes ?? []).map((n: Json) => ({ ...n, metrics: [...(n.metrics ?? [])] }));
  let edges: Json[] = (query.edges ?? []).map((e: Json) => ({ ...e }));
  const removed = new Set<number>();
  for (const wrapper of nodes) {
    if (!String(wrapper.nodeName).startsWith("DataFlint")) continue;
    const operator = String(wrapper.nodeName).slice(9);
    const inbound = edges.filter((e) => e.toId === wrapper.nodeId);
    if (inbound.length !== 1) continue;
    const child = nodes.find((n) => n.nodeId === inbound[0].fromId);
    if (!child || String(child.nodeName).replace(/ /g, "") !== operator.replace(/ /g, "")) continue;
    const known = new Set(child.metrics.map((m: Json) => m.name));
    child.metrics = [...child.metrics, ...wrapper.metrics.filter((m: Json) => !known.has(m.name))];
    child.nodeName = `DataFlint${child.nodeName}`;
    removed.add(wrapper.nodeId);
    for (const edge of edges) if (edge.fromId === wrapper.nodeId) edge.fromId = child.nodeId;
  }
  if (removed.size > 0) {
    nodes = nodes.filter((n) => !removed.has(n.nodeId));
    edges = edges.filter((e) => !removed.has(e.toId) && !removed.has(e.fromId));
  }
  nodes = nodes.map((n) => {
    const instrumented = String(n.nodeName).startsWith("DataFlint");
    return { ...n, instrumented, nodeName: instrumented ? String(n.nodeName).slice(9) : n.nodeName };
  });
  return { nodes, edges };
}

async function computeAlerts(client: SparkRestClient, data: AppData) {
  const { summary, resources, config } = summarize(data);
  const alerts: Alert[] = [...resourceAlerts(resources, config, data.driverXmxBytes)];

  const failed = data.stages.filter((stage) => stage.status === "FAILED");
  const failedTasks = data.stages.reduce((sum, stage) => sum + num(stage.numFailedTasks), 0);
  if (failedTasks > 0) {
    alerts.push({
      name: "taskFailures",
      title: "Task Failures",
      type: "error",
      source: "custom",
      location: "Application",
      message: `${failedTasks} failed tasks, ${failed.length} failed stages (${failed.map((s) => s.stageId).join(", ") || "none"})`,
      suggestion: "Call list_stage_task_failures for the error messages",
      evidence: { failed_tasks: failedTasks, failed_stage_ids: failed.map((s) => s.stageId) },
    });
  }

  const stagesById = latestStages(data.stages);
  const byQuery = sqlStageMap(data.sql, data.jobs);
  const queries = new Map<number, Json>(data.sql.map((q) => [num(q.id), q]));
  for (const [queryId, stageIds] of byQuery) {
    for (const stageId of stageIds) {
      const stage = stagesById.get(stageId);
      if (stage) alerts.push(...stageAlerts(queries.get(queryId)!, stage));
    }
  }

  // DataFlint's plan text gives filter conditions and write partition keys.
  const plans = data.dataflintAvailable
    ? (await dataflint<Json[]>(client, data.path, "sqlplan", { offset: 0, length: 10_000 })) ?? []
    : [];
  const planText = new Map<string, string>();
  for (const plan of plans) {
    for (const node of plan.nodesPlan ?? []) {
      planText.set(`${num(plan.executionId)}:${num(node.id)}`, String(node.planDescription ?? ""));
    }
  }
  for (const query of data.sql) {
    const graph = normalizeGraph(query);
    for (const node of graph.nodes) {
      const raw = planText.get(`${num(query.id)}:${num(node.nodeId)}`);
      const plan = raw !== undefined && node.instrumented ? raw.replace(`DataFlint${node.nodeName} `, "") : raw;
      alerts.push(...nodeAlerts(query, node, plan));
    }
    alerts.push(...joinAlerts(query, graph));
  }

  const counts: Record<string, number> = {};
  for (const alert of alerts) counts[alert.name] = (counts[alert.name] ?? 0) + 1;
  const dataflintAlerts = alerts.filter((a) => a.source === "dataflint");
  return {
    app_id: data.appId,
    app_attempt_id: data.attemptId,
    attempts: data.attemptCount,
    name: data.name,
    dataflint_available: data.dataflintAvailable,
    errors: dataflintAlerts.filter((a) => a.type === "error").length,
    warnings: dataflintAlerts.filter((a) => a.type === "warning").length,
    custom_alerts: alerts.length - dataflintAlerts.length,
    counts,
    alerts,
    idle_cores_pct: summary.idle_cores_pct,
    memory_usage_pct: summary.memory_usage_pct,
    not_covered:
      "DataFlint's Delta Lake and Iceberg table alerts" +
      (data.dataflintAvailable ? "" : "; long filter and partitioned small-file detail (need the DataFlint jar)"),
  };
}

/* ------------------------------------------------------------------ */
/* SQL plan                                                            */
/* ------------------------------------------------------------------ */

async function sqlPlan(client: SparkRestClient, data: AppData, executionId: number) {
  const query = data.sql.find((q) => num(q.id) === executionId);
  if (!query) {
    throw new ValueError(`SQL execution ${executionId} not found in application ${data.appId}.`);
  }
  if (!data.dataflintAvailable) throw new ValueError(DATAFLINT_MISSING);
  const plans = (await dataflint<Json[]>(client, data.path, "sqlplan", { offset: 0, length: 10_000 })) ?? [];
  const plan = plans.find((p) => num(p.executionId) === executionId) ?? { nodesPlan: [] };
  const metrics = (await dataflint<Json[]>(client, data.path, "sqlmetrics", { executionId })) ?? [];
  const metricsById = new Map<number, Json>(metrics.map((m) => [num(m.id), m]));
  const stageIds = sqlStageMap([query], data.jobs).get(executionId) ?? [];
  const stages = data.stages
    .filter((stage) => stageIds.includes(num(stage.stageId)))
    .map((stage) => {
      const runTime = dist(stage, "executorRunTime");
      return {
        stage_id: stage.stageId,
        attempt_id: stage.attemptId,
        status: stage.status,
        num_tasks: num(stage.numTasks),
        duration: stage.submissionTime && stage.completionTime
          ? humanMs(epochMs(stage.completionTime) - epochMs(stage.submissionTime))
          : null,
        median_task: runTime ? humanMs(runTime[MEDIAN]) : null,
        max_task: runTime ? humanMs(runTime[MAX]) : null,
        input: humanBytes(num(stage.inputBytes)),
        shuffle_read: humanBytes(num(stage.shuffleReadBytes)),
        shuffle_write: humanBytes(num(stage.shuffleWriteBytes)),
        spill_to_disk: humanBytes(num(stage.diskBytesSpilled)),
      };
    });
  return {
    app_id: data.appId,
    app_attempt_id: data.attemptId,
    attempts: data.attemptCount,
    execution_id: executionId,
    description: query.description,
    status: query.status,
    duration: humanMs(num(query.duration)),
    nodes: (plan.nodesPlan ?? []).map((node: Json) => ({
      id: node.id,
      plan: String(node.planDescription).slice(0, 1500),
      metrics: Object.fromEntries(
        (metricsById.get(num(node.id))?.metrics ?? [])
          .filter((m: Json) => m.value)
          .map((m: Json) => [m.name, m.value]),
      ),
    })),
    edges: (query.edges ?? []).map((edge: Json) => `${edge.fromId} -> ${edge.toId}`),
    stages,
  };
}

/* ------------------------------------------------------------------ */
/* Handlers                                                            */
/* ------------------------------------------------------------------ */

async function resolve(ctx: AppContext, args: Args): Promise<[SparkRestClient, AppData]> {
  const appId = String(args.app_id);
  const client = await getClientOrDefault(ctx, args.server ?? null, appId);
  return [client, await loadAppData(client, appId, args.app_attempt_id ?? null)];
}

export async function dataflintGetAppSummary(ctx: AppContext, args: Args) {
  const [, data] = await resolve(ctx, args);
  return summarize(data).summary;
}

export async function dataflintGetAlerts(ctx: AppContext, args: Args) {
  const [client, data] = await resolve(ctx, args);
  return computeAlerts(client, data);
}

export async function dataflintGetSqlPlan(ctx: AppContext, args: Args) {
  const [client, data] = await resolve(ctx, args);
  return sqlPlan(client, data, num(args.execution_id));
}

export const DATAFLINT_TOOL_HANDLERS = {
  dataflint_get_app_summary: dataflintGetAppSummary,
  dataflint_get_alerts: dataflintGetAlerts,
  dataflint_get_sql_plan: dataflintGetSqlPlan,
};
