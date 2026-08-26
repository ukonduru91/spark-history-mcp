/**
 * MCP tools for the Spark History Server.
 *
 * Port of `spark_history_mcp/tools/tools.py`. Every tool keeps the Python
 * function's name, argument names, defaults, ordering rules and return shape so
 * responses are interchangeable with the original server's.
 */

import type { SparkRestClient } from "../api/sparkClient.js";
import type { AppContext } from "../core/app.js";
import { ValueError, errorText } from "../core/errors.js";
import { modelCopyDeep } from "../models/deserialize.js";
import type {
  Application,
  Environment,
  Executor,
  Job,
  SQLExecution,
  SQLPlanNode,
  StageData,
  TaskMetricsSummary,
  ThreadStackTrace,
} from "../models/generated.js";
import {
  failedTask,
  sqlCompareSide,
  sqlExecutionComparison,
  sqlExecutionDetail,
  sqlExecutionSummary,
  sqlJobSummary,
  sqlNodeMetrics,
  sqlNodeTypeDiff,
  sqlPlanComparison,
  sqlStageSummary,
  stageCompareSide,
  stageMetricsAggregation,
  stageTaskQuantiles,
  type FailedTask,
  type SqlExecutionComparison,
  type SqlExecutionDetail,
  type SqlExecutionSummary,
  type SqlJobSummary,
  type SqlNodeMetrics,
  type SqlPlanComparison,
  type StageComparison,
  type StageCompareSide,
  type StageMetricsAggregation,
  type StageTaskQuantiles,
} from "../models/mcpTypes.js";
import { parallelExecute } from "../utils/utils.js";
import { pyDiv, pyFloat, pySum, type PyFloat } from "../core/pyfloat.js";

/* ------------------------------------------------------------------ */
/* Timestamp helpers                                                   */
/* ------------------------------------------------------------------ */

const GMT_TIMESTAMP =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{1,6})GMT$/;

/**
 * Parse a Spark REST API timestamp into epoch milliseconds.
 *
 * The models expose timestamps as ISO strings (e.g.
 * `"2025-08-05T00:52:08.178GMT"`). This helper normalises the representations
 * the API may produce:
 *
 * * `null`/`undefined` -> `null`
 * * epoch milliseconds (number) -> used directly
 * * `"...GMT"` strings -> parsed as UTC
 * * other ISO-8601 strings -> parsed as ISO
 *
 * Returns `null` when the value cannot be parsed, so callers can treat an
 * unparseable timestamp the same as a missing one.
 */
export function parseSparkDatetime(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const gmt = GMT_TIMESTAMP.exec(value);
    if (value.endsWith("GMT")) {
      if (!gmt) return null;
      const [, year, month, day, hour, minute, second, fraction] = gmt;
      const micros = fraction.padEnd(6, "0");
      const millis = Number(micros.slice(0, 3));
      const extra = Number(micros.slice(3)) / 1000;
      return (
        Date.UTC(
          Number(year),
          Number(month) - 1,
          Number(day),
          Number(hour),
          Number(minute),
          Number(second),
          millis,
        ) + extra
      );
    }
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

/**
 * Duration in seconds between two Spark timestamps.
 *
 * Returns the literal int `0` when either endpoint is missing or unparseable —
 * Python's early `return 0` — and a float otherwise, since `timedelta`
 * arithmetic there yields `float`. The distinction is visible in the rendered
 * JSON (`0` vs `0.0`).
 */
export function durationSeconds(start: unknown, end: unknown): number | PyFloat {
  const startMs = parseSparkDatetime(start);
  const endMs = parseSparkDatetime(end);
  if (startMs !== null && endMs !== null) {
    return pyFloat((endMs - startMs) / 1000);
  }
  return 0;
}

/** Duration in whole milliseconds between two Spark timestamps. */
export function durationMs(start: unknown, end: unknown): number {
  return Math.trunc(Number(durationSeconds(start, end)) * 1000);
}

/* ------------------------------------------------------------------ */
/* Sorting helpers                                                     */
/* ------------------------------------------------------------------ */

// Lower number = shown first. Anything not listed sorts last, then by duration
// descending.
const SQL_STATUS_PRIORITY: Record<string, number> = {
  FAILED: 0,
  RUNNING: 1,
  COMPLETED: 2,
};
const JOB_STATUS_PRIORITY: Record<string, number> = {
  FAILED: 0,
  RUNNING: 1,
  UNKNOWN: 2,
  SUCCEEDED: 3,
};
const STAGE_STATUS_PRIORITY: Record<string, number> = {
  FAILED: 0,
  COMPLETE: 1,
  ACTIVE: 2,
  PENDING: 3,
  SKIPPED: 4,
};

/**
 * Stable sort by a key vector, comparing element-wise — the semantics of
 * Python's `sorted(items, key=...)` with a tuple key.
 */
function sortedBy<T>(items: T[], key: (item: T) => Array<number | string>): T[] {
  return items
    .map((item, index) => ({ item, index, key: key(item) }))
    .sort((left, right) => {
      const length = Math.max(left.key.length, right.key.length);
      for (let position = 0; position < length; position++) {
        const a = left.key[position];
        const b = right.key[position];
        if (a === b) continue;
        if (typeof a === "string" || typeof b === "string") {
          return String(a) < String(b) ? -1 : 1;
        }
        return (a as number) - (b as number);
      }
      return left.index - right.index;
    })
    .map((entry) => entry.item);
}

/** Remove `== Initial Plan ==` sections from an AQE plan description. */
export function stripInitialPlans(plan: string): string {
  const lines = plan.split("\n");
  const out: string[] = [];
  let i = 0;
  while (i < lines.length) {
    if (lines[i].trim().startsWith("+- == Initial Plan ==")) {
      const markerIndent = lines[i].length - lines[i].replace(/^ +/, "").length;
      i += 1;
      while (i < lines.length) {
        const lineIndent = lines[i].length - lines[i].replace(/^ +/, "").length;
        if (lines[i] === "" || lineIndent > markerIndent) {
          i += 1;
        } else {
          break;
        }
      }
      continue;
    }
    out.push(lines[i]);
    i += 1;
  }
  return out.join("\n").replace(/\n+$/, "");
}

/** All job IDs (success + failed + running) for a SQL execution. */
function collectSqlJobIds(execution: SQLExecution): number[] {
  const ids: number[] = [];
  for (const group of [
    execution.successJobIds,
    execution.failedJobIds,
    execution.runningJobIds,
  ]) {
    if (group) ids.push(...group);
  }
  return ids;
}

/** The set of stage IDs referenced by the given jobs. */
function stageIdsFromJobs(jobs: Job[]): Set<number> {
  const stageIds = new Set<number>();
  for (const job of jobs) {
    for (const stageId of job.stageIds ?? []) {
      stageIds.add(stageId);
    }
  }
  return stageIds;
}

/**
 * Aggregate stage metrics, optionally scoped to a set of stage IDs.
 *
 * De-duplicates by stage ID (keeping the first attempt seen).
 */
function aggregateStages(
  stages: StageData[],
  stageIds: Set<number> | null,
): StageMetricsAggregation {
  const agg = stageMetricsAggregation();
  const seen = new Set<number>();
  for (const stage of stages) {
    const sid = stage.stageId as number;
    if ((stageIds !== null && !stageIds.has(sid)) || seen.has(sid)) continue;
    seen.add(sid);
    agg.stageCount += 1;
    agg.tasks += stage.numTasks ?? 0;
    agg.duration += durationMs(stage.submissionTime, stage.completionTime);
    agg.inputBytes += stage.inputBytes ?? 0;
    agg.shuffleReadBytes += stage.shuffleReadBytes ?? 0;
    agg.shuffleWriteBytes += stage.shuffleWriteBytes ?? 0;
    agg.diskBytesSpilled += stage.diskBytesSpilled ?? 0;
    agg.jvmGcTime += stage.jvmGcTime ?? 0;
  }
  return agg;
}

function sortSqlExecutions(
  executions: SQLExecution[],
  sortBy: string | null,
): SQLExecution[] {
  const duration = (execution: SQLExecution) => execution.duration ?? 0;
  if (sortBy === "duration") {
    return sortedBy(executions, (e) => [-duration(e)]);
  }
  if (sortBy === "id") {
    return sortedBy(executions, (e) => [-(e.id ?? 0)]);
  }
  // default: failed first, then by duration descending
  return sortedBy(executions, (e) => [
    SQL_STATUS_PRIORITY[e.status ?? ""] ?? 99,
    -duration(e),
  ]);
}

/** Sort jobs failed-first, then by duration descending. */
function defaultSortJobs(jobs: Job[]): Job[] {
  return sortedBy(jobs, (job) => [
    JOB_STATUS_PRIORITY[job.status ?? ""] ?? 99,
    -durationMs(job.submissionTime, job.completionTime),
  ]);
}

function sortJobs(jobs: Job[], sortBy: string | null): Job[] {
  if (sortBy === "duration") {
    return sortedBy(jobs, (job) => [
      -durationMs(job.submissionTime, job.completionTime),
    ]);
  }
  if (sortBy === "failed-tasks") {
    return sortedBy(jobs, (job) => [-(job.numFailedTasks ?? 0)]);
  }
  if (sortBy === "id") {
    return sortedBy(jobs, (job) => [-(job.jobId ?? 0)]);
  }
  if (sortBy !== null && sortBy !== undefined) {
    throw new ValueError(
      `invalid sort_by '${sortBy}'; expected 'duration', 'failed-tasks', or 'id'`,
    );
  }
  return defaultSortJobs(jobs);
}

/** Sort stages by status priority, then by duration descending. */
function defaultSortStages(stages: StageData[]): StageData[] {
  return sortedBy(stages, (stage) => [
    STAGE_STATUS_PRIORITY[stage.status ?? ""] ?? 99,
    -durationMs(stage.submissionTime, stage.completionTime),
  ]);
}

function sortStages(stages: StageData[], sortBy: string | null): StageData[] {
  if (sortBy === "duration") {
    return sortedBy(stages, (stage) => [
      -durationMs(stage.submissionTime, stage.completionTime),
    ]);
  }
  if (sortBy === "failed-tasks") {
    return sortedBy(stages, (stage) => [-(stage.numFailedTasks ?? 0)]);
  }
  if (sortBy === "id") {
    return sortedBy(stages, (stage) => [-(stage.stageId ?? 0)]);
  }
  if (sortBy !== null && sortBy !== undefined) {
    throw new ValueError(
      `invalid sort_by '${sortBy}'; expected 'duration', 'failed-tasks', or 'id'`,
    );
  }
  return defaultSortStages(stages);
}

function sortExecutors(executors: Executor[], sortBy: string | null): Executor[] {
  if (sortBy === "failed-tasks") {
    return sortedBy(executors, (e) => [-(e.failedTasks ?? 0)]);
  }
  if (sortBy === "duration") {
    return sortedBy(executors, (e) => [-(e.totalDuration ?? 0)]);
  }
  if (sortBy === "gc") {
    return sortedBy(executors, (e) => [-(e.totalGCTime ?? 0)]);
  }
  if (sortBy === "id") {
    // Ascending string comparison (IDs are "driver", "1", "2", ...).
    return sortedBy(executors, (e) => [e.id ?? ""]);
  }
  if (sortBy !== null && sortBy !== undefined) {
    throw new ValueError(
      `invalid sort_by '${sortBy}'; expected 'failed-tasks', 'duration', 'gc', or 'id'`,
    );
  }
  // default: active executors first, then by duration descending
  return sortedBy(executors, (e) => [
    e.isActive ? 0 : 1,
    -(e.totalDuration ?? 0),
  ]);
}

/** Count plan nodes by node name. */
function countNodeTypes(nodes: SQLPlanNode[] | null | undefined): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const node of nodes ?? []) {
    const name = node.nodeName as string;
    counts[name] = (counts[name] ?? 0) + 1;
  }
  return counts;
}

/* ------------------------------------------------------------------ */
/* Client resolution                                                   */
/* ------------------------------------------------------------------ */

/**
 * Get a client by server name, app discovery, or default client.
 *
 * @throws ValueError If no client is found.
 */
export async function getClientOrDefault(
  ctx: AppContext,
  serverName?: string | null,
  appId?: string | null,
): Promise<SparkRestClient> {
  // If app_id provided, use discovery
  if (appId && !serverName) {
    const [client] = await ctx.appDiscovery.getClientForApp(appId, serverName);
    return client;
  }

  const clients = ctx.clients;

  // An explicitly requested server must exist; do not silently fall back to the
  // default, or callers can't tell which server actually answered.
  if (serverName) {
    const client = clients[serverName];
    if (!client) {
      const available = Object.keys(clients).sort().join(", ") || "none";
      throw new ValueError(
        `No Spark server named '${serverName}' is configured ` +
          `(available servers: ${available}).`,
      );
    }
    return client;
  }

  if (ctx.defaultClient) {
    return ctx.defaultClient;
  }

  throw new ValueError(
    "No Spark client found. Please specify a valid server name or set a default server.",
  );
}

/* ------------------------------------------------------------------ */
/* Tool argument helpers                                               */
/* ------------------------------------------------------------------ */

type Args = Record<string, any>;

function optString(args: Args, name: string): string | null {
  const value = args[name];
  return value === undefined || value === null ? null : String(value);
}

function optNumber(args: Args, name: string): number | null {
  const value = args[name];
  return value === undefined || value === null ? null : Number(value);
}

function optBool(args: Args, name: string, fallback: boolean | null = null) {
  const value = args[name];
  return value === undefined || value === null ? fallback : Boolean(value);
}

function optStringList(args: Args, name: string): string[] | null {
  const value = args[name];
  if (value === undefined || value === null) return null;
  return Array.isArray(value) ? value.map(String) : [String(value)];
}

function numberOr(args: Args, name: string, fallback: number): number {
  const value = args[name];
  return value === undefined || value === null ? fallback : Number(value);
}

function boolOr(args: Args, name: string, fallback: boolean): boolean {
  const value = args[name];
  return value === undefined || value === null ? fallback : Boolean(value);
}

function requiredString(args: Args, name: string): string {
  const value = args[name];
  if (value === undefined || value === null) {
    throw new ValueError(`Field required: ${name}`);
  }
  return String(value);
}

function requiredNumber(args: Args, name: string): number {
  const value = args[name];
  if (value === undefined || value === null) {
    throw new ValueError(`Field required: ${name}`);
  }
  return Number(value);
}

/* ------------------------------------------------------------------ */
/* Tools                                                               */
/* ------------------------------------------------------------------ */

export async function listApplications(
  ctx: AppContext,
  args: Args,
): Promise<Application[]> {
  const server = optString(args, "server");
  const appId = optString(args, "app_id");
  const status = optStringList(args, "status");
  const minDate = optString(args, "min_date");
  const maxDate = optString(args, "max_date");
  const minEndDate = optString(args, "min_end_date");
  const maxEndDate = optString(args, "max_end_date");
  const limit = optNumber(args, "limit");

  if (appId) {
    const client = await getClientOrDefault(ctx, server, appId);
    return [await client.getApplication(appId)];
  }

  if (server) {
    // Return from specific server
    const client = await getClientOrDefault(ctx, server);
    return client.listApplications({
      status,
      minDate,
      maxDate,
      minEndDate,
      maxEndDate,
      limit,
    });
  }

  // Return from all servers
  const allApps: Application[] = [];
  for (const [serverName, client] of Object.entries(ctx.clients)) {
    try {
      const apps = await client.listApplications({
        status,
        minDate,
        maxDate,
        minEndDate,
        maxEndDate,
        limit,
      });
      allApps.push(...apps);
    } catch (error) {
      console.error(
        `WARNING: Failed to get applications from server '${serverName}': ${errorText(error)}`,
      );
      continue; // Skip unreachable servers
    }
  }
  return allApps;
}

export async function listJobs(ctx: AppContext, args: Args): Promise<Job[]> {
  const appId = requiredString(args, "app_id");
  const server = optString(args, "server");
  const status = optStringList(args, "status");
  const jobId = optNumber(args, "job_id");
  const sortBy = optString(args, "sort_by");
  const appAttemptId = optString(args, "app_attempt_id");
  const offset = numberOr(args, "offset", 0);
  const length = optNumber(args, "length");

  const client = await getClientOrDefault(ctx, server, appId);

  if (offset < 0) throw new ValueError("offset must be non-negative");
  if (length !== null && length < 0) {
    throw new ValueError("length must be non-negative");
  }

  let jobs = await client.listJobs({ appId, status, appAttemptId });

  if (jobId !== null) {
    jobs = jobs.filter((job) => job.jobId === jobId);
  }

  jobs = sortJobs(jobs, sortBy);

  if (offset) jobs = jobs.slice(offset);
  if (length !== null) jobs = jobs.slice(0, length);

  return jobs;
}

export async function listStages(ctx: AppContext, args: Args): Promise<StageData[]> {
  const appId = requiredString(args, "app_id");
  const server = optString(args, "server");
  const status = optStringList(args, "status");
  const sortBy = optString(args, "sort_by");
  const withSummaries = boolOr(args, "with_summaries", false);
  const appAttemptId = optString(args, "app_attempt_id");
  const offset = numberOr(args, "offset", 0);
  const length = optNumber(args, "length");

  const client = await getClientOrDefault(ctx, server, appId);

  if (offset < 0) throw new ValueError("offset must be non-negative");
  if (length !== null && length < 0) {
    throw new ValueError("length must be non-negative");
  }

  let stages = await client.listStages({
    appId,
    status,
    withSummaries,
    appAttemptId,
  });

  stages = sortStages(stages, sortBy);

  if (offset) stages = stages.slice(offset);
  if (length !== null) stages = stages.slice(0, length);

  return stages;
}

export async function getStage(ctx: AppContext, args: Args): Promise<StageData> {
  const appId = requiredString(args, "app_id");
  const stageId = requiredNumber(args, "stage_id");
  const attemptId = optNumber(args, "attempt_id");
  const server = optString(args, "server");
  const withSummaries = boolOr(args, "with_summaries", false);
  const quantiles = optString(args, "quantiles");

  const client = await getClientOrDefault(ctx, server, appId);

  let stageData: StageData;
  if (attemptId !== null) {
    // Get specific attempt
    stageData = await client.getStageAttempt({
      appId,
      stageId,
      attemptId,
      details: false,
      withSummaries,
    });
  } else {
    // Get all attempts and use the latest one
    const stages = await client.listStageAttempts({
      appId,
      stageId,
      details: false,
      withSummaries,
    });

    if (stages.length === 0) {
      throw new ValueError(`No stage found with ID ${stageId}`);
    }

    // If multiple attempts exist, get the one with the highest attempt_id
    stageData = stages.reduce((best, stage) =>
      (stage.attemptId ?? 0) > (best.attemptId ?? 0) ? stage : best,
    );
  }

  // If summaries were requested but metrics distributions are missing, fetch
  // them separately.
  if (
    withSummaries &&
    (stageData.taskMetricsDistributions === undefined ||
      stageData.taskMetricsDistributions === null)
  ) {
    const taskSummary = await client.getStageTaskSummary({
      appId,
      stageId,
      attemptId: stageData.attemptId as number,
      ...(quantiles ? { quantiles } : {}),
    });
    stageData.taskMetricsDistributions = taskSummary;
  }

  return stageData;
}

/** The highest attempt id for a stage; raises if the stage is absent. */
async function resolveLatestStageAttemptId(
  client: SparkRestClient,
  appId: string,
  stageId: number,
): Promise<number> {
  const attempts = await client.listStageAttempts({
    appId,
    stageId,
    details: false,
    withSummaries: false,
  });
  if (attempts.length === 0) {
    throw new ValueError(
      `No stage found with ID ${stageId} in application ${appId} ` +
        "(it may have been skipped by AQE)",
    );
  }
  return attempts.reduce(
    (best, stage) => Math.max(best, stage.attemptId ?? 0),
    0,
  );
}

export async function listStageTaskFailures(
  ctx: AppContext,
  args: Args,
): Promise<FailedTask[]> {
  const appId = requiredString(args, "app_id");
  const stageId = requiredNumber(args, "stage_id");
  const server = optString(args, "server");
  let stageAttemptId = optNumber(args, "stage_attempt_id");
  const appAttemptId = optString(args, "app_attempt_id");

  const client = await getClientOrDefault(ctx, server, appId);

  if (stageAttemptId === null) {
    stageAttemptId = await resolveLatestStageAttemptId(client, appId, stageId);
  }

  const tasks = await client.listStageTasks({
    appId,
    stageId,
    attemptId: stageAttemptId,
    status: "failed",
    appAttemptId,
  });

  return tasks.map((task) =>
    failedTask({
      taskId: task.taskId,
      attempt: task.attempt,
      executorId: task.executorId,
      host: task.host,
      status: task.status,
      errorMessage: task.errorMessage,
    }),
  );
}

// Environment sections that can be requested individually via `section`, and the
// model property each maps to.
const ENVIRONMENT_SECTION_FIELDS: Array<[string, keyof Environment]> = [
  ["runtime", "runtime"],
  ["spark_properties", "sparkProperties"],
  ["system_properties", "systemProperties"],
  ["hadoop_properties", "hadoopProperties"],
  ["metrics_properties", "metricsProperties"],
  ["classpath_entries", "classpathEntries"],
];

function resolveEnvironmentSection(section: string): keyof Environment {
  const keepField = section.trim().toLowerCase();
  const match = ENVIRONMENT_SECTION_FIELDS.find(([name]) => name === keepField);
  if (!match) {
    const valid = ENVIRONMENT_SECTION_FIELDS.map(([name]) => `'${name}'`).join(", ");
    throw new ValueError(`invalid section '${section}'; expected one of ${valid}`);
  }
  return match[1];
}

/**
 * A copy of `env` with only the requested section populated. Fields for the
 * other sections — and `resourceProfiles` — are cleared.
 */
function filterEnvironmentSection(env: Environment, section: string): Environment {
  const keepField = resolveEnvironmentSection(section);
  const filtered = modelCopyDeep(env);
  for (const [, field] of ENVIRONMENT_SECTION_FIELDS) {
    if (field !== keepField) {
      (filtered as Record<string, unknown>)[field as string] = null;
    }
  }
  filtered.resourceProfiles = null;
  return filtered;
}

export async function getEnvironment(
  ctx: AppContext,
  args: Args,
): Promise<Environment> {
  const appId = requiredString(args, "app_id");
  const server = optString(args, "server");
  const appAttemptId = optString(args, "app_attempt_id");
  const section = optString(args, "section");

  const client = await getClientOrDefault(ctx, server, appId);

  let env = await client.getEnvironment({ appId, appAttemptId });

  if (section !== null) {
    env = filterEnvironmentSection(env, section);
  }

  return env;
}

export async function listExecutors(
  ctx: AppContext,
  args: Args,
): Promise<Executor[]> {
  const appId = requiredString(args, "app_id");
  const server = optString(args, "server");
  const executorId = optString(args, "executor_id");
  const sortBy = optString(args, "sort_by");
  const includeInactive = boolOr(args, "include_inactive", false);
  const appAttemptId = optString(args, "app_attempt_id");
  const offset = numberOr(args, "offset", 0);
  const length = optNumber(args, "length");

  const client = await getClientOrDefault(ctx, server, appId);

  if (offset < 0) throw new ValueError("offset must be non-negative");
  if (length !== null && length < 0) {
    throw new ValueError("length must be non-negative");
  }

  if (executorId !== null) {
    // Single-executor lookup: search all executors, including inactive.
    const all = await client.listAllExecutors({ appId, appAttemptId });
    return all.filter((executor) => executor.id === executorId);
  }

  let executors = includeInactive
    ? await client.listAllExecutors({ appId, appAttemptId })
    : await client.listExecutors({ appId, appAttemptId });

  executors = sortExecutors(executors, sortBy);

  if (offset) executors = executors.slice(offset);
  if (length !== null) executors = executors.slice(0, length);

  return executors;
}

export interface ExecutorMetricsSummary {
  total_executors: number;
  active_executors: number;
  memory_used: number;
  disk_used: number;
  completed_tasks: number;
  failed_tasks: number;
  total_duration: number;
  total_gc_time: number;
  total_input_bytes: number;
  total_shuffle_read: number;
  total_shuffle_write: number;
}

/** Calculate executor summary metrics from an executor list. */
function calculateExecutorMetrics(executors: Executor[]): ExecutorMetricsSummary {
  const memoryUsed = (executor: Executor): number => {
    const metrics = executor.memoryMetrics;
    if (metrics === null || metrics === undefined) return 0;
    return (
      (metrics.usedOnHeapStorageMemory ?? 0) +
      (metrics.usedOffHeapStorageMemory ?? 0)
    );
  };
  const sum = (pick: (executor: Executor) => number | null | undefined): number =>
    executors.reduce((total, executor) => total + (pick(executor) as number), 0);

  return {
    total_executors: executors.length,
    active_executors: executors.filter((executor) => executor.isActive).length,
    memory_used: executors.reduce((total, e) => total + memoryUsed(e), 0),
    disk_used: sum((e) => e.diskUsed),
    completed_tasks: sum((e) => e.completedTasks),
    failed_tasks: sum((e) => e.failedTasks),
    total_duration: sum((e) => e.totalDuration),
    total_gc_time: sum((e) => e.totalGCTime),
    total_input_bytes: sum((e) => e.totalInputBytes),
    total_shuffle_read: sum((e) => e.totalShuffleRead),
    total_shuffle_write: sum((e) => e.totalShuffleWrite),
  };
}

export async function getExecutorSummary(
  ctx: AppContext,
  args: Args,
): Promise<ExecutorMetricsSummary> {
  const appId = requiredString(args, "app_id");
  const server = optString(args, "server");
  const client = await getClientOrDefault(ctx, server, appId);
  const executors = await client.listAllExecutors({ appId });
  return calculateExecutorMetrics(executors);
}

/** Helper to calculate an executor summary without MCP context. */
async function calcExecutorSummaryFromClient(
  client: SparkRestClient,
  appId: string,
): Promise<ExecutorMetricsSummary> {
  const executors = await client.listAllExecutors({ appId });
  return calculateExecutorMetrics(executors);
}

/** Filter thread-dump entries by state, name substring, and blocked status. */
function filterThreads(
  threads: ThreadStackTrace[],
  state: string | null,
  name: string | null,
  blockedOnly: boolean,
): ThreadStackTrace[] {
  if (!state && !name && !blockedOnly) {
    return [...threads];
  }
  const nameLow = name ? name.toLowerCase() : null;
  const result: ThreadStackTrace[] = [];
  for (const thread of threads) {
    if (state && (thread.threadState ?? "").toUpperCase() !== state.toUpperCase()) {
      continue;
    }
    if (nameLow && !(thread.threadName ?? "").toLowerCase().includes(nameLow)) {
      continue;
    }
    if (
      blockedOnly &&
      (thread.blockedByThreadId === null || thread.blockedByThreadId === undefined) &&
      (thread.lockName === null || thread.lockName === undefined)
    ) {
      continue;
    }
    result.push(thread);
  }
  return result;
}

export async function getExecutorThreadDump(
  ctx: AppContext,
  args: Args,
): Promise<ThreadStackTrace[]> {
  const appId = requiredString(args, "app_id");
  const executorId = requiredString(args, "executor_id");
  const server = optString(args, "server");
  const state = optString(args, "state");
  const name = optString(args, "name");
  const blockedOnly = boolOr(args, "blocked_only", false);
  const appAttemptId = optString(args, "app_attempt_id");

  const client = await getClientOrDefault(ctx, server, appId);

  let threads = await client.getExecutorThreadDump({
    appId,
    executorId,
    appAttemptId,
  });

  threads = filterThreads(threads, state, name, blockedOnly);
  threads = sortedBy(threads, (thread) => [thread.threadId ?? 0]);
  return threads;
}

export async function compareJobEnvironments(
  ctx: AppContext,
  args: Args,
): Promise<Record<string, unknown>> {
  const appId1 = requiredString(args, "app_id1");
  const appId2 = requiredString(args, "app_id2");
  const server = optString(args, "server");

  const client1 = await getClientOrDefault(ctx, server, appId1);
  const client2 = await getClientOrDefault(ctx, server, appId2);

  const env1 = await client1.getEnvironment({ appId: appId1 });
  const env2 = await client2.getEnvironment({ appId: appId2 });

  const propsToDict = (
    props: Array<Array<string>> | null | undefined,
  ): Record<string, string> => {
    const out: Record<string, string> = {};
    for (const entry of props ?? []) {
      out[entry[0]] = entry[1];
    }
    return out;
  };

  const sparkProps1 = propsToDict(env1.sparkProperties);
  const sparkProps2 = propsToDict(env2.sparkProperties);
  const systemProps1 = propsToDict(env1.systemProperties);
  const systemProps2 = propsToDict(env2.systemProperties);

  const common: Record<string, unknown> = {};
  const different: Record<string, unknown> = {};
  const onlyInApp1: Record<string, string> = {};
  const onlyInApp2: Record<string, string> = {};

  for (const [key, value] of Object.entries(sparkProps1)) {
    if (key in sparkProps2 && value === sparkProps2[key]) {
      common[key] = { app1: value, app2: sparkProps2[key] };
    }
  }
  for (const [key, value] of Object.entries(sparkProps1)) {
    if (key in sparkProps2 && value !== sparkProps2[key]) {
      different[key] = { app1: value, app2: sparkProps2[key] ?? "NOT_SET" };
    }
  }
  for (const [key, value] of Object.entries(sparkProps1)) {
    if (!(key in sparkProps2)) onlyInApp1[key] = value;
  }
  for (const [key, value] of Object.entries(sparkProps2)) {
    if (!(key in sparkProps1)) onlyInApp2[key] = value;
  }

  const keyDifferences: Record<string, unknown> = {};
  for (const key of [
    "java.version",
    "java.runtime.version",
    "os.name",
    "os.version",
    "user.timezone",
    "file.encoding",
  ]) {
    if (systemProps1[key] !== systemProps2[key]) {
      keyDifferences[key] = {
        app1: systemProps1[key] ?? "NOT_SET",
        app2: systemProps2[key] ?? "NOT_SET",
      };
    }
  }

  return {
    applications: { app1: appId1, app2: appId2 },
    runtime_comparison: {
      app1: {
        java_version: env1.runtime?.javaVersion ?? null,
        java_home: env1.runtime?.javaHome ?? null,
        scala_version: env1.runtime?.scalaVersion ?? null,
      },
      app2: {
        java_version: env2.runtime?.javaVersion ?? null,
        java_home: env2.runtime?.javaHome ?? null,
        scala_version: env2.runtime?.scalaVersion ?? null,
      },
    },
    spark_properties: {
      common,
      different,
      only_in_app1: onlyInApp1,
      only_in_app2: onlyInApp2,
    },
    system_properties: { key_differences: keyDifferences },
  };
}

export async function compareJobPerformance(
  ctx: AppContext,
  args: Args,
): Promise<Record<string, unknown>> {
  const appId1 = requiredString(args, "app_id1");
  const appId2 = requiredString(args, "app_id2");
  const server = optString(args, "server");

  const client1 = await getClientOrDefault(ctx, server, appId1);
  const client2 = await getClientOrDefault(ctx, server, appId2);

  // Define API calls for parallel execution
  const apiCalls = [
    { name: "app1", run: () => client1.getApplication(appId1) },
    { name: "app2", run: () => client2.getApplication(appId2) },
    {
      name: "exec_summary1",
      run: () => calcExecutorSummaryFromClient(client1, appId1),
    },
    {
      name: "exec_summary2",
      run: () => calcExecutorSummaryFromClient(client2, appId2),
    },
    { name: "jobs1", run: () => client1.listJobs({ appId: appId1 }) },
    { name: "jobs2", run: () => client2.listJobs({ appId: appId2 }) },
  ];

  // Execute all API calls in parallel; a generous timeout for large-scale apps.
  const executionResult = await parallelExecute(apiCalls, 6, 300);

  // If parallel execution fails entirely, try sequential as a fallback.
  if (
    executionResult.errors.length > 0 &&
    Object.keys(executionResult.results).length === 0
  ) {
    try {
      const app1 = await client1.getApplication(appId1);
      const app2 = await client2.getApplication(appId2);
      const errorSummary = executionResult.errors.join("; ");
      return {
        error: `Parallel execution failed: ${errorSummary}. Falling back to basic app info only.`,
        partial_data: {
          app1: { id: appId1, name: app1.name },
          app2: { id: appId2, name: app2.name },
        },
      };
    } catch (error) {
      const allErrors = [
        ...executionResult.errors,
        `Sequential fallback failed: ${errorText(error)}`,
      ];
      return { error: `Complete failure: ${allErrors.join("; ")}` };
    }
  }

  if (executionResult.errors.length > 0) {
    return { error: `API failures: ${executionResult.errors.join("; ")}` };
  }

  const results = executionResult.results;
  const app1 = results.app1 as Application;
  const app2 = results.app2 as Application;
  const execSummary1 = results.exec_summary1 as ExecutorMetricsSummary;
  const execSummary2 = results.exec_summary2 as ExecutorMetricsSummary;
  const jobs1 = results.jobs1 as Job[];
  const jobs2 = results.jobs2 as Job[];

  const calcJobStats = (jobs: Job[]): Record<string, number | PyFloat> => {
    if (jobs.length === 0) {
      return { count: 0, total_duration: 0, avg_duration: 0 };
    }
    const completedJobs = jobs.filter(
      (job) => job.completionTime && job.submissionTime,
    );
    if (completedJobs.length === 0) {
      return { count: jobs.length, total_duration: 0, avg_duration: 0 };
    }
    const durations = completedJobs.map((job) =>
      durationSeconds(job.submissionTime, job.completionTime),
    );
    // `sum()` stays an int only if every duration was an int; `/` always
    // produces a float; `min`/`max` return one of the original values.
    const total = pySum(durations);
    const smallest = durations.reduce((best, value) =>
      Number(value) < Number(best) ? value : best,
    );
    const largest = durations.reduce((best, value) =>
      Number(value) > Number(best) ? value : best,
    );
    return {
      count: jobs.length,
      completed_count: completedJobs.length,
      total_duration: total,
      avg_duration: pyDiv(total, durations.length),
      min_duration: smallest,
      max_duration: largest,
    };
  };

  const jobStats1 = calcJobStats(jobs1);
  const jobStats2 = calcJobStats(jobs2);

  return {
    applications: {
      app1: { id: appId1, name: app1.name },
      app2: { id: appId2, name: app2.name },
    },
    resource_allocation: {
      app1: {
        cores_granted: app1.coresGranted ?? null,
        max_cores: app1.maxCores ?? null,
        cores_per_executor: app1.coresPerExecutor ?? null,
        memory_per_executor_mb: app1.memoryPerExecutorMB ?? null,
      },
      app2: {
        cores_granted: app2.coresGranted ?? null,
        max_cores: app2.maxCores ?? null,
        cores_per_executor: app2.coresPerExecutor ?? null,
        memory_per_executor_mb: app2.memoryPerExecutorMB ?? null,
      },
    },
    executor_metrics: {
      app1: execSummary1,
      app2: execSummary2,
      comparison: {
        executor_count_ratio: pyDiv(
          execSummary2.total_executors,
          Math.max(execSummary1.total_executors, 1),
        ),
        memory_usage_ratio: pyDiv(
          execSummary2.memory_used,
          Math.max(execSummary1.memory_used, 1),
        ),
        task_completion_ratio: pyDiv(
          execSummary2.completed_tasks,
          Math.max(execSummary1.completed_tasks, 1),
        ),
        gc_time_ratio: pyDiv(
          execSummary2.total_gc_time,
          Math.max(execSummary1.total_gc_time, 1),
        ),
      },
    },
    job_performance: {
      app1: jobStats1,
      app2: jobStats2,
      comparison: {
        job_count_ratio: pyDiv(
          Number(jobStats2.count),
          Math.max(Number(jobStats1.count), 1),
        ),
        // The `else` branch is a literal `0` in Python, i.e. an int.
        avg_duration_ratio:
          Number(jobStats1.avg_duration) > 0
            ? pyDiv(
                Number(jobStats2.avg_duration),
                Math.max(Number(jobStats1.avg_duration), 1),
              )
            : 0,
        total_duration_ratio:
          Number(jobStats1.total_duration) > 0
            ? pyDiv(
                Number(jobStats2.total_duration),
                Math.max(Number(jobStats1.total_duration), 1),
              )
            : 0,
      },
    },
  };
}

/** Resolve a SQL execution id, defaulting to the longest-running execution. */
async function resolveLongestSqlId(
  client: SparkRestClient,
  appId: string,
  executionId: number | null,
): Promise<number> {
  if (executionId !== null) return executionId;
  const sqlList = await client.getSqlList({ appId, details: false });
  if (sqlList.length > 0) {
    return sqlList.reduce((best, execution) =>
      (execution.duration ?? 0) > (best.duration ?? 0) ? execution : best,
    ).id as number;
  }
  throw new ValueError(`No SQL executions found in application ${appId}`);
}

/** Build a plan-structure diff between two (already resolved) SQL executions. */
async function compareSqlPlans(
  client1: SparkRestClient,
  appId1: string,
  execId1: number,
  client2: SparkRestClient,
  appId2: string,
  execId2: number,
): Promise<SqlPlanComparison> {
  const exec1 = await client1.getSqlExecution({
    appId: appId1,
    executionId: execId1,
    details: true,
    planDescription: false,
  });
  const exec2 = await client2.getSqlExecution({
    appId: appId2,
    executionId: execId2,
    details: true,
    planDescription: false,
  });

  const nodes1 = countNodeTypes(exec1.nodes);
  const nodes2 = countNodeTypes(exec2.nodes);

  const nodeTypes = Array.from(
    new Set([...Object.keys(nodes1), ...Object.keys(nodes2)]),
  ).sort();

  const diffs = nodeTypes
    .filter((nodeType) => (nodes1[nodeType] ?? 0) !== (nodes2[nodeType] ?? 0))
    .map((nodeType) =>
      sqlNodeTypeDiff({
        nodeType,
        a: nodes1[nodeType] ?? 0,
        b: nodes2[nodeType] ?? 0,
      }),
    );

  return sqlPlanComparison({
    appA: appId1,
    appB: appId2,
    execIdA: execId1,
    execIdB: execId2,
    nodeCountA: (exec1.nodes ?? []).length,
    nodeCountB: (exec2.nodes ?? []).length,
    edgeCountA: (exec1.edges ?? []).length,
    edgeCountB: (exec2.edges ?? []).length,
    nodeTypeDiffs: diffs,
  });
}

/** Truncate a plan description while preserving structure. */
export function truncatePlanDescription(
  planDesc: string,
  maxLength: number,
): string {
  if (!planDesc || planDesc.length <= maxLength) {
    return planDesc;
  }
  let truncated = planDesc.slice(0, maxLength);
  const lastNewline = truncated.lastIndexOf("\n");
  if (lastNewline > maxLength * 0.8) {
    truncated = truncated.slice(0, lastNewline);
  }
  return truncated + "\n... [truncated]";
}

function buildSqlExecutionSummary(execution: SQLExecution): SqlExecutionSummary {
  return sqlExecutionSummary({
    id: execution.id,
    status: execution.status,
    description: execution.description,
    submissionTime: execution.submissionTime,
    duration: execution.duration,
    successJobIds: execution.successJobIds ?? [],
    failedJobIds: execution.failedJobIds ?? [],
    runningJobIds: execution.runningJobIds ?? [],
  });
}

function buildSqlJobSummary(job: Job): SqlJobSummary {
  return sqlJobSummary({
    jobId: job.jobId,
    status: job.status,
    description: job.description || job.name,
    duration: durationMs(job.submissionTime, job.completionTime),
    numTasks: job.numTasks,
    numFailedTasks: job.numFailedTasks,
    stageIds: job.stageIds ?? [],
  });
}

function buildSqlStageSummary(stage: StageData) {
  return sqlStageSummary({
    stageId: stage.stageId,
    attemptId: stage.attemptId,
    status: stage.status,
    description: stage.description || stage.name,
    numTasks: stage.numTasks,
    numFailedTasks: stage.numFailedTasks,
    duration: durationMs(stage.submissionTime, stage.completionTime),
    inputBytes: stage.inputBytes,
    shuffleReadBytes: stage.shuffleReadBytes,
    shuffleWriteBytes: stage.shuffleWriteBytes,
  });
}

/** Curated per-node plan metrics, skipping nodes without metrics. */
function buildNodeMetrics(
  nodes: SQLPlanNode[] | null | undefined,
): SqlNodeMetrics[] {
  const result: SqlNodeMetrics[] = [];
  for (const node of nodes ?? []) {
    if (!node.metrics || node.metrics.length === 0) continue;
    const metrics: Record<string, string> = {};
    for (const metric of node.metrics) {
      // Collapse internal whitespace in values.
      metrics[metric.name as string] = (metric.value ?? "").split(/\s+/).filter(Boolean).join(" ");
    }
    if (Object.keys(metrics).length > 0) {
      result.push(
        sqlNodeMetrics({
          nodeId: node.nodeId,
          nodeName: node.nodeName,
          metrics,
        }),
      );
    }
  }
  return result;
}

export async function listSqlExecutions(
  ctx: AppContext,
  args: Args,
): Promise<SqlExecutionSummary[]> {
  const appId = requiredString(args, "app_id");
  const server = optString(args, "server");
  const appAttemptId = optString(args, "app_attempt_id");
  const status = optString(args, "status");
  const description = optString(args, "description");
  const sortBy = optString(args, "sort_by");
  const limit = numberOr(args, "limit", 20);
  const pageSize = numberOr(args, "page_size", 100);

  const client = await getClientOrDefault(ctx, server, appId);

  if (limit < 0) throw new ValueError("limit must be non-negative");

  // Fetch all executions (lightweight: no plan text, no node details).
  let allExecutions: SQLExecution[] = [];
  let offset = 0;
  for (;;) {
    const page = await client.getSqlList({
      appId,
      appAttemptId,
      details: false,
      planDescription: false,
      offset,
      length: pageSize,
    });
    if (page.length === 0) break;
    allExecutions.push(...page);
    offset += pageSize;
    if (page.length < pageSize) break;
  }

  if (description) {
    const needle = description.toLowerCase();
    allExecutions = allExecutions.filter((execution) =>
      (execution.description ?? "").toLowerCase().includes(needle),
    );
  }
  if (status) {
    const wanted = status.toUpperCase();
    allExecutions = allExecutions.filter(
      (execution) => (execution.status ?? "") === wanted,
    );
  }

  allExecutions = sortSqlExecutions(allExecutions, sortBy);
  if (limit) {
    allExecutions = allExecutions.slice(0, limit);
  }

  return allExecutions.map(buildSqlExecutionSummary);
}

export async function getSqlExecution(
  ctx: AppContext,
  args: Args,
): Promise<SqlExecutionDetail> {
  const appId = requiredString(args, "app_id");
  const executionId = requiredNumber(args, "execution_id");
  const server = optString(args, "server");
  const appAttemptId = optString(args, "app_attempt_id");
  let includePlan = optBool(args, "include_plan", null);
  const includeInitialPlan = boolOr(args, "include_initial_plan", false);
  const includeAggregatedMetrics = boolOr(args, "include_aggregated_metrics", false);
  const includeStages = boolOr(args, "include_stages", false);
  const planMaxLength = optNumber(args, "plan_max_length");

  const client = await getClientOrDefault(ctx, server, appId);

  // Resolve whether to include the plan. Explicit arg wins; otherwise fall back
  // to the server's include_plan_description config (default False).
  if (includeInitialPlan) {
    includePlan = true;
  } else if (includePlan === null) {
    const configValue = client.config.include_plan_description;
    includePlan = configValue !== null && configValue !== undefined
      ? Boolean(configValue)
      : false;
  }

  // Node-level metrics require details=true; only fetch them when needed.
  const execution = await client.getSqlExecution({
    appId,
    executionId,
    appAttemptId,
    details: includePlan,
    planDescription: includePlan,
  });

  const detail = sqlExecutionDetail(buildSqlExecutionSummary(execution));

  if (includePlan) {
    let plan = execution.planDescription ?? "";
    if (!includeInitialPlan) {
      plan = stripInitialPlans(plan);
    }
    if (planMaxLength !== null) {
      plan = truncatePlanDescription(plan, planMaxLength);
    }
    detail.planDescription = plan;
    detail.nodeMetrics = buildNodeMetrics(execution.nodes);
  }

  if (includeAggregatedMetrics || includeStages) {
    const jobIds = new Set(collectSqlJobIds(execution));
    let jobs: Job[] = [];
    let stageList: StageData[] = [];
    if (jobIds.size > 0) {
      const allJobs = await client.listJobs({ appId, appAttemptId });
      jobs = defaultSortJobs(
        allJobs.filter((job) => jobIds.has(job.jobId as number)),
      );
      const stageIds = stageIdsFromJobs(jobs);
      if (stageIds.size > 0) {
        const allStages = await client.listStages({ appId, appAttemptId });
        const seen = new Set<number>();
        const scoped: StageData[] = [];
        for (const stage of allStages) {
          const sid = stage.stageId as number;
          if (stageIds.has(sid) && !seen.has(sid)) {
            seen.add(sid);
            scoped.push(stage);
          }
        }
        stageList = defaultSortStages(scoped);
      }
    }

    detail.jobs = jobs.map(buildSqlJobSummary);
    if (includeAggregatedMetrics) {
      detail.stageMetrics = aggregateStages(stageList, null);
    }
    if (includeStages) {
      detail.stages = stageList.map(buildSqlStageSummary);
    }
  }

  return detail;
}

export async function compareSqlExecutions(
  ctx: AppContext,
  args: Args,
): Promise<SqlExecutionComparison> {
  const appId1 = requiredString(args, "app_id1");
  const appId2 = requiredString(args, "app_id2");
  const server = optString(args, "server");
  const includePlanDiff = boolOr(args, "include_plan_diff", false);

  const client1 = await getClientOrDefault(ctx, server, appId1);
  const client2 = await getClientOrDefault(ctx, server, appId2);

  const executionId1 = await resolveLongestSqlId(
    client1,
    appId1,
    optNumber(args, "execution_id1"),
  );
  const executionId2 = await resolveLongestSqlId(
    client2,
    appId2,
    optNumber(args, "execution_id2"),
  );

  const collectSide = async (
    client: SparkRestClient,
    appId: string,
    executionId: number,
  ) => {
    const execution = await client.getSqlExecution({
      appId,
      executionId,
      details: false,
      planDescription: false,
    });
    const jobIds = new Set(collectSqlJobIds(execution));
    const allJobs = await client.listJobs({ appId });
    const jobs = allJobs.filter((job) => jobIds.has(job.jobId as number));
    const stageIds = stageIdsFromJobs(jobs);
    const allStages = await client.listStages({ appId });
    const agg = aggregateStages(allStages, stageIds);

    return sqlCompareSide({
      app: appId,
      sqlId: execution.id,
      description: execution.description,
      status: execution.status,
      duration: execution.duration,
      jobs: jobs.length,
      stages: agg.stageCount,
      tasks: agg.tasks,
      stageTime: agg.duration,
      inputBytes: agg.inputBytes,
      shuffleReadBytes: agg.shuffleReadBytes,
      shuffleWriteBytes: agg.shuffleWriteBytes,
      diskBytesSpilled: agg.diskBytesSpilled,
      jvmGcTime: agg.jvmGcTime,
    });
  };

  const comparison = sqlExecutionComparison(
    await collectSide(client1, appId1, executionId1),
    await collectSide(client2, appId2, executionId2),
  );

  if (includePlanDiff) {
    comparison.planComparison = await compareSqlPlans(
      client1,
      appId1,
      executionId1,
      client2,
      appId2,
      executionId2,
    );
  }

  return comparison;
}

// Quantile points fetched for stage task-metric distributions: p25/p50/p75/max.
const STAGE_COMPARE_QUANTILES = "0.25,0.5,0.75,1.0";

/** Map a `TaskMetricsSummary` into the curated stage quantiles model. */
function buildStageTaskQuantiles(
  summary: TaskMetricsSummary | null,
): StageTaskQuantiles | null {
  if (summary === null || summary === undefined) return null;
  const nested = <T>(obj: T | null | undefined, attr: keyof NonNullable<T>) =>
    obj === null || obj === undefined ? null : ((obj as NonNullable<T>)[attr] as any);

  return stageTaskQuantiles({
    quantiles: summary.quantiles ?? [],
    duration: summary.duration ?? null,
    gcTime: summary.jvmGcTime ?? null,
    schedulerDelay: summary.schedulerDelay ?? null,
    peakExecutionMemory: summary.peakExecutionMemory ?? null,
    inputBytes: nested(summary.inputMetrics, "bytesRead"),
    outputBytes: nested(summary.outputMetrics, "bytesWritten"),
    shuffleReadBytes: nested(summary.shuffleReadMetrics, "readBytes"),
    shuffleWriteBytes: nested(summary.shuffleWriteMetrics, "writeBytes"),
    diskBytesSpilled: summary.diskBytesSpilled ?? null,
    memoryBytesSpilled: summary.memoryBytesSpilled ?? null,
  });
}

/** Build one side of a stage comparison from the latest stage attempt. */
async function collectStageSide(
  client: SparkRestClient,
  appId: string,
  stageId: number,
): Promise<StageCompareSide> {
  const attempts = await client.listStageAttempts({
    appId,
    stageId,
    details: false,
    withSummaries: false,
  });
  if (attempts.length === 0) {
    throw new ValueError(
      `No stage found with ID ${stageId} in application ${appId} ` +
        "(it may have been skipped by AQE)",
    );
  }

  const stage = attempts.reduce((best, candidate) =>
    (candidate.attemptId ?? 0) > (best.attemptId ?? 0) ? candidate : best,
  );
  const attemptId = stage.attemptId ?? 0;

  // Task-metric quantiles are best-effort; a missing summary just omits them.
  let summary: TaskMetricsSummary | null = null;
  try {
    summary = await client.getStageTaskSummary({
      appId,
      stageId,
      attemptId,
      quantiles: STAGE_COMPARE_QUANTILES,
    });
  } catch {
    summary = null;
  }

  return stageCompareSide({
    app: appId,
    stageId: stage.stageId,
    attemptId,
    status: stage.status,
    description: stage.description || stage.name,
    duration: durationMs(stage.submissionTime, stage.completionTime),
    tasks: stage.numTasks ?? 0,
    failedTasks: stage.numFailedTasks ?? 0,
    inputBytes: stage.inputBytes ?? 0,
    outputBytes: stage.outputBytes ?? 0,
    shuffleReadBytes: stage.shuffleReadBytes ?? 0,
    shuffleWriteBytes: stage.shuffleWriteBytes ?? 0,
    diskBytesSpilled: stage.diskBytesSpilled ?? 0,
    memoryBytesSpilled: stage.memoryBytesSpilled ?? 0,
    jvmGcTime: stage.jvmGcTime ?? 0,
    taskQuantiles: buildStageTaskQuantiles(summary),
  });
}

export async function compareStages(
  ctx: AppContext,
  args: Args,
): Promise<StageComparison> {
  const appId1 = requiredString(args, "app_id1");
  const stageId1 = requiredNumber(args, "stage_id1");
  const appId2 = requiredString(args, "app_id2");
  const stageId2 = requiredNumber(args, "stage_id2");
  const server = optString(args, "server");

  const client1 = await getClientOrDefault(ctx, server, appId1);
  const client2 = await getClientOrDefault(ctx, server, appId2);

  return {
    a: await collectStageSide(client1, appId1, stageId1),
    b: await collectStageSide(client2, appId2, stageId2),
  };
}

export async function getJobBottlenecks(
  ctx: AppContext,
  args: Args,
): Promise<Record<string, unknown>> {
  const appId = requiredString(args, "app_id");
  const server = optString(args, "server");
  const topN = numberOr(args, "top_n", 5);

  const client = await getClientOrDefault(ctx, server, appId);

  const slowestStages = await listStages(ctx, {
    app_id: appId,
    server,
    sort_by: "duration",
    length: topN,
  });

  const slowestJobs = await listJobs(ctx, {
    app_id: appId,
    server,
    sort_by: "duration",
    length: topN,
  });

  const execSummary = await getExecutorSummary(ctx, { app_id: appId, server });

  const allStages = await client.listStages({ appId });

  // Identify stages with high spill
  let highSpillStages: Array<Record<string, unknown>> = [];
  for (const stage of allStages) {
    if (stage.memoryBytesSpilled && stage.memoryBytesSpilled > 100 * 1024 * 1024) {
      highSpillStages.push({
        stage_id: stage.stageId,
        attempt_id: stage.attemptId,
        name: stage.name,
        memory_spilled_mb: pyFloat(stage.memoryBytesSpilled / (1024 * 1024)),
        disk_spilled_mb: stage.diskBytesSpilled
          ? pyFloat(stage.diskBytesSpilled / (1024 * 1024))
          : 0,
      });
    }
  }

  highSpillStages = sortedBy(highSpillStages, (entry) => [
    -Number(entry.memory_spilled_mb),
  ]);

  // Identify GC pressure
  const gcPressure =
    execSummary.total_duration > 0
      ? execSummary.total_gc_time / Math.max(execSummary.total_duration, 1)
      : 0;
  // Python yields a float from the division and an int `0` otherwise.
  const gcPressureValue =
    execSummary.total_duration > 0 ? pyFloat(gcPressure) : 0;

  const recommendations: Array<Record<string, string>> = [];

  const bottlenecks: Record<string, unknown> = {
    application_id: appId,
    performance_bottlenecks: {
      slowest_stages: slowestStages.slice(0, topN).map((stage) => ({
        stage_id: stage.stageId,
        attempt_id: stage.attemptId,
        name: stage.name,
        duration_seconds: durationSeconds(stage.submissionTime, stage.completionTime),
        task_count: stage.numTasks,
        failed_tasks: stage.numFailedTasks,
      })),
      slowest_jobs: slowestJobs.slice(0, topN).map((job) => ({
        job_id: job.jobId,
        name: job.name,
        duration_seconds: durationSeconds(job.submissionTime, job.completionTime),
        failed_tasks: job.numFailedTasks,
        status: job.status,
      })),
    },
    resource_bottlenecks: {
      memory_spill_stages: highSpillStages.slice(0, topN),
      gc_pressure_ratio: gcPressureValue,
      executor_utilization: {
        total_executors: execSummary.total_executors,
        active_executors: execSummary.active_executors,
        utilization_ratio: pyFloat(
          execSummary.active_executors / Math.max(execSummary.total_executors, 1),
        ),
      },
    },
    recommendations,
  };

  // Generate recommendations
  if (gcPressure > 0.1) {
    recommendations.push({
      type: "memory",
      priority: "high",
      issue: `High GC pressure (${formatPercent(gcPressure)})`,
      suggestion:
        "Consider increasing executor memory or reducing memory usage",
    });
  }

  if (highSpillStages.length > 0) {
    recommendations.push({
      type: "memory",
      priority: "high",
      issue: `Memory spilling detected in ${highSpillStages.length} stages`,
      suggestion: "Increase executor memory or optimize data partitioning",
    });
  }

  if (execSummary.failed_tasks > 0) {
    recommendations.push({
      type: "reliability",
      priority: "medium",
      issue: `${execSummary.failed_tasks} failed tasks`,
      suggestion:
        "Investigate task failures and consider increasing task retry settings",
    });
  }

  return bottlenecks;
}

/** Python's `f"{value:.1%}"` — percentage with one decimal place. */
function formatPercent(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

export async function getResourceUsageTimeline(
  ctx: AppContext,
  args: Args,
): Promise<Record<string, unknown>> {
  const appId = requiredString(args, "app_id");
  const server = optString(args, "server");

  const client = await getClientOrDefault(ctx, server, appId);

  // Get application info
  const app = await client.getApplication(appId);

  // Get all executors
  const executors = await client.listAllExecutors({ appId });

  // Get stages
  const stages = await client.listStages({ appId });

  // Create timeline events
  const timelineEvents: Array<Record<string, any>> = [];

  // Add executor events
  for (const executor of executors) {
    if (executor.addTime) {
      timelineEvents.push({
        timestamp: parseSparkDatetime(executor.addTime),
        type: "executor_add",
        executor_id: executor.id,
        cores: executor.totalCores,
        memory_mb: executor.maxMemory
          ? pyFloat(executor.maxMemory / (1024 * 1024))
          : 0,
      });
    }
    if (executor.removeTime) {
      timelineEvents.push({
        timestamp: parseSparkDatetime(executor.removeTime),
        type: "executor_remove",
        executor_id: executor.id,
        reason: executor.removeReason,
      });
    }
  }

  // Add stage events
  for (const stage of stages) {
    if (stage.submissionTime) {
      timelineEvents.push({
        timestamp: parseSparkDatetime(stage.submissionTime),
        type: "stage_start",
        stage_id: stage.stageId,
        attempt_id: stage.attemptId,
        name: stage.name,
        task_count: stage.numTasks,
      });
    }
    if (stage.completionTime) {
      timelineEvents.push({
        timestamp: parseSparkDatetime(stage.completionTime),
        type: "stage_end",
        stage_id: stage.stageId,
        attempt_id: stage.attemptId,
        status: stage.status,
        duration_seconds: durationSeconds(stage.submissionTime, stage.completionTime),
      });
    }
  }

  // Sort events by timestamp
  const sortedEvents = sortedBy(timelineEvents, (event) => [
    event.timestamp ?? 0,
  ]);

  // Calculate resource utilization over time
  let activeExecutors = 0;
  let totalCores = 0;
  let totalMemory = 0;

  const resourceTimeline: Array<Record<string, unknown>> = [];

  for (const event of sortedEvents) {
    if (event.type === "executor_add") {
      activeExecutors += 1;
      totalCores += event.cores;
      totalMemory += event.memory_mb;
    } else if (event.type === "executor_remove") {
      activeExecutors -= 1;
      // Note: We don't have cores/memory info in remove events
    }

    resourceTimeline.push({
      timestamp: event.timestamp,
      active_executors: activeExecutors,
      total_cores: totalCores,
      total_memory_mb: totalMemory,
      event,
    });
  }

  return {
    application_id: appId,
    application_name: app.name,
    summary: {
      total_events: sortedEvents.length,
      executor_additions: sortedEvents.filter((e) => e.type === "executor_add").length,
      executor_removals: sortedEvents.filter((e) => e.type === "executor_remove")
        .length,
      stage_executions: sortedEvents.filter((e) => e.type === "stage_start").length,
      peak_executors: Math.max(
        ...resourceTimeline.map((row) => row.active_executors as number),
        0,
      ),
      peak_cores: Math.max(
        ...resourceTimeline.map((row) => row.total_cores as number),
        0,
      ),
    },
  };
}

/* ------------------------------------------------------------------ */
/* Registry                                                            */
/* ------------------------------------------------------------------ */

export type ToolHandler = (ctx: AppContext, args: Args) => Promise<unknown>;

export const TOOL_HANDLERS: Record<string, ToolHandler> = {
  list_applications: listApplications,
  list_jobs: listJobs,
  list_stages: listStages,
  get_stage: getStage,
  list_stage_task_failures: listStageTaskFailures,
  get_environment: getEnvironment,
  list_executors: listExecutors,
  get_executor_summary: getExecutorSummary,
  get_executor_thread_dump: getExecutorThreadDump,
  compare_job_environments: compareJobEnvironments,
  compare_job_performance: compareJobPerformance,
  list_sql_executions: listSqlExecutions,
  get_sql_execution: getSqlExecution,
  compare_sql_executions: compareSqlExecutions,
  compare_stages: compareStages,
  get_job_bottlenecks: getJobBottlenecks,
  get_resource_usage_timeline: getResourceUsageTimeline,
};
