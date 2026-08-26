/**
 * Curated, LLM-friendly output models for MCP tools.
 *
 * Port of `spark_history_mcp/models/mcp_types.py`. These intentionally expose a
 * small, summarized subset of the raw Spark History Server payloads.
 *
 * The factory functions below build plain objects whose keys are the pydantic
 * *aliases*, inserted in field-definition order, so `JSON.stringify` reproduces
 * the Python server's `model_dump(by_alias=True)` output.
 */

/** Header-level summary of a single SQL execution. */
export interface SqlExecutionSummary {
  id: number | null;
  status: string | null;
  description: string | null;
  submissionTime: string | null;
  /** milliseconds, as reported by SHS */
  duration: number | null;
  successJobIds: number[];
  failedJobIds: number[];
  runningJobIds: number[];
}

export function sqlExecutionSummary(
  init: Partial<SqlExecutionSummary> = {},
): SqlExecutionSummary {
  return {
    id: init.id ?? null,
    status: init.status ?? null,
    description: init.description ?? null,
    submissionTime: init.submissionTime ?? null,
    duration: init.duration ?? null,
    successJobIds: init.successJobIds ?? [],
    failedJobIds: init.failedJobIds ?? [],
    runningJobIds: init.runningJobIds ?? [],
  };
}

/** Per-node plan metrics, keyed name -> value. */
export interface SqlNodeMetrics {
  nodeId: number | null;
  nodeName: string | null;
  metrics: Record<string, string>;
}

export function sqlNodeMetrics(init: Partial<SqlNodeMetrics> = {}): SqlNodeMetrics {
  return {
    nodeId: init.nodeId ?? null,
    nodeName: init.nodeName ?? null,
    metrics: init.metrics ?? {},
  };
}

/** Curated job row associated with a SQL execution. */
export interface SqlJobSummary {
  jobId: number | null;
  status: string | null;
  description: string | null;
  /** milliseconds */
  duration: number | null;
  numTasks: number | null;
  numFailedTasks: number | null;
  stageIds: number[];
}

export function sqlJobSummary(init: Partial<SqlJobSummary> = {}): SqlJobSummary {
  return {
    jobId: init.jobId ?? null,
    status: init.status ?? null,
    description: init.description ?? null,
    duration: init.duration ?? null,
    numTasks: init.numTasks ?? null,
    numFailedTasks: init.numFailedTasks ?? null,
    stageIds: init.stageIds ?? [],
  };
}

/** Curated stage row associated with a SQL execution. */
export interface SqlStageSummary {
  stageId: number | null;
  attemptId: number | null;
  status: string | null;
  description: string | null;
  numTasks: number | null;
  numFailedTasks: number | null;
  /** milliseconds */
  duration: number | null;
  inputBytes: number | null;
  shuffleReadBytes: number | null;
  shuffleWriteBytes: number | null;
}

export function sqlStageSummary(init: Partial<SqlStageSummary> = {}): SqlStageSummary {
  return {
    stageId: init.stageId ?? null,
    attemptId: init.attemptId ?? null,
    status: init.status ?? null,
    description: init.description ?? null,
    numTasks: init.numTasks ?? null,
    numFailedTasks: init.numFailedTasks ?? null,
    duration: init.duration ?? null,
    inputBytes: init.inputBytes ?? null,
    shuffleReadBytes: init.shuffleReadBytes ?? null,
    shuffleWriteBytes: init.shuffleWriteBytes ?? null,
  };
}

/** Aggregated stage metrics scoped to a SQL execution. */
export interface StageMetricsAggregation {
  stageCount: number;
  tasks: number;
  /** milliseconds (sum of per-stage wall durations) */
  duration: number;
  inputBytes: number;
  shuffleReadBytes: number;
  shuffleWriteBytes: number;
  diskBytesSpilled: number;
  jvmGcTime: number;
}

export function stageMetricsAggregation(
  init: Partial<StageMetricsAggregation> = {},
): StageMetricsAggregation {
  return {
    stageCount: init.stageCount ?? 0,
    tasks: init.tasks ?? 0,
    duration: init.duration ?? 0,
    inputBytes: init.inputBytes ?? 0,
    shuffleReadBytes: init.shuffleReadBytes ?? 0,
    shuffleWriteBytes: init.shuffleWriteBytes ?? 0,
    diskBytesSpilled: init.diskBytesSpilled ?? 0,
    jvmGcTime: init.jvmGcTime ?? 0,
  };
}

/**
 * Detailed view of a SQL execution.
 *
 * By default, only `execution` (the header) is populated. The optional sections
 * are filled in when the corresponding `include_*` flag is set on
 * `get_sql_execution`.
 */
export interface SqlExecutionDetail {
  execution: SqlExecutionSummary;
  planDescription: string | null;
  nodeMetrics: SqlNodeMetrics[] | null;
  jobs: SqlJobSummary[] | null;
  stageMetrics: StageMetricsAggregation | null;
  stages: SqlStageSummary[] | null;
}

export function sqlExecutionDetail(
  execution: SqlExecutionSummary,
): SqlExecutionDetail {
  return {
    execution,
    planDescription: null,
    nodeMetrics: null,
    jobs: null,
    stageMetrics: null,
    stages: null,
  };
}

/** One side of a SQL execution metrics comparison. */
export interface SqlCompareSide {
  app: string;
  sqlId: number | null;
  description: string | null;
  status: string | null;
  /** milliseconds */
  duration: number | null;
  jobs: number;
  stages: number;
  tasks: number;
  /** milliseconds */
  stageTime: number;
  inputBytes: number;
  shuffleReadBytes: number;
  shuffleWriteBytes: number;
  diskBytesSpilled: number;
  jvmGcTime: number;
}

export function sqlCompareSide(
  init: Partial<SqlCompareSide> & { app: string },
): SqlCompareSide {
  return {
    app: init.app,
    sqlId: init.sqlId ?? null,
    description: init.description ?? null,
    status: init.status ?? null,
    duration: init.duration ?? null,
    jobs: init.jobs ?? 0,
    stages: init.stages ?? 0,
    tasks: init.tasks ?? 0,
    stageTime: init.stageTime ?? 0,
    inputBytes: init.inputBytes ?? 0,
    shuffleReadBytes: init.shuffleReadBytes ?? 0,
    shuffleWriteBytes: init.shuffleWriteBytes ?? 0,
    diskBytesSpilled: init.diskBytesSpilled ?? 0,
    jvmGcTime: init.jvmGcTime ?? 0,
  };
}

/** Count of a single plan node type on each side of a plan comparison. */
export interface SqlNodeTypeDiff {
  nodeType: string;
  a: number;
  b: number;
}

export function sqlNodeTypeDiff(
  init: Partial<SqlNodeTypeDiff> & { nodeType: string },
): SqlNodeTypeDiff {
  return { nodeType: init.nodeType, a: init.a ?? 0, b: init.b ?? 0 };
}

/** Plan-structure diff between two SQL executions. */
export interface SqlPlanComparison {
  appA: string;
  appB: string;
  execIdA: number | null;
  execIdB: number | null;
  nodeCountA: number;
  nodeCountB: number;
  edgeCountA: number;
  edgeCountB: number;
  nodeTypeDiffs: SqlNodeTypeDiff[];
}

export function sqlPlanComparison(
  init: Partial<SqlPlanComparison> & { appA: string; appB: string },
): SqlPlanComparison {
  return {
    appA: init.appA,
    appB: init.appB,
    execIdA: init.execIdA ?? null,
    execIdB: init.execIdB ?? null,
    nodeCountA: init.nodeCountA ?? 0,
    nodeCountB: init.nodeCountB ?? 0,
    edgeCountA: init.edgeCountA ?? 0,
    edgeCountB: init.edgeCountB ?? 0,
    nodeTypeDiffs: init.nodeTypeDiffs ?? [],
  };
}

/** Metrics diff between two SQL executions, with an optional plan-structure diff. */
export interface SqlExecutionComparison {
  a: SqlCompareSide;
  b: SqlCompareSide;
  planComparison: SqlPlanComparison | null;
}

export function sqlExecutionComparison(
  a: SqlCompareSide,
  b: SqlCompareSide,
): SqlExecutionComparison {
  return { a, b, planComparison: null };
}

/**
 * Per-task metric distributions for a stage at the requested quantiles.
 *
 * Each metric is a list aligned with `quantiles` (e.g. `[p25, p50, p75, max]`).
 * Time metrics are milliseconds; byte metrics are bytes.
 */
export interface StageTaskQuantiles {
  quantiles: number[];
  duration: number[] | null;
  gcTime: number[] | null;
  schedulerDelay: number[] | null;
  peakExecutionMemory: number[] | null;
  inputBytes: number[] | null;
  outputBytes: number[] | null;
  shuffleReadBytes: number[] | null;
  shuffleWriteBytes: number[] | null;
  diskBytesSpilled: number[] | null;
  memoryBytesSpilled: number[] | null;
}

export function stageTaskQuantiles(
  init: Partial<StageTaskQuantiles> = {},
): StageTaskQuantiles {
  return {
    quantiles: init.quantiles ?? [],
    duration: init.duration ?? null,
    gcTime: init.gcTime ?? null,
    schedulerDelay: init.schedulerDelay ?? null,
    peakExecutionMemory: init.peakExecutionMemory ?? null,
    inputBytes: init.inputBytes ?? null,
    outputBytes: init.outputBytes ?? null,
    shuffleReadBytes: init.shuffleReadBytes ?? null,
    shuffleWriteBytes: init.shuffleWriteBytes ?? null,
    diskBytesSpilled: init.diskBytesSpilled ?? null,
    memoryBytesSpilled: init.memoryBytesSpilled ?? null,
  };
}

/** One side of a stage comparison: stage-level metrics and task quantiles. */
export interface StageCompareSide {
  app: string;
  stageId: number | null;
  attemptId: number | null;
  status: string | null;
  description: string | null;
  /** milliseconds (wall time, submission -> completion) */
  duration: number;
  tasks: number;
  failedTasks: number;
  inputBytes: number;
  outputBytes: number;
  shuffleReadBytes: number;
  shuffleWriteBytes: number;
  diskBytesSpilled: number;
  memoryBytesSpilled: number;
  /** milliseconds */
  jvmGcTime: number;
  taskQuantiles: StageTaskQuantiles | null;
}

export function stageCompareSide(
  init: Partial<StageCompareSide> & { app: string },
): StageCompareSide {
  return {
    app: init.app,
    stageId: init.stageId ?? null,
    attemptId: init.attemptId ?? null,
    status: init.status ?? null,
    description: init.description ?? null,
    duration: init.duration ?? 0,
    tasks: init.tasks ?? 0,
    failedTasks: init.failedTasks ?? 0,
    inputBytes: init.inputBytes ?? 0,
    outputBytes: init.outputBytes ?? 0,
    shuffleReadBytes: init.shuffleReadBytes ?? 0,
    shuffleWriteBytes: init.shuffleWriteBytes ?? 0,
    diskBytesSpilled: init.diskBytesSpilled ?? 0,
    memoryBytesSpilled: init.memoryBytesSpilled ?? 0,
    jvmGcTime: init.jvmGcTime ?? 0,
    taskQuantiles: init.taskQuantiles ?? null,
  };
}

/** Metrics and task-quantile diff between two stages. */
export interface StageComparison {
  a: StageCompareSide;
  b: StageCompareSide;
}

/**
 * A single failed task with its error message.
 *
 * Curated subset of the raw task payload focused on failure investigation; the
 * heavy per-task metrics are intentionally omitted.
 */
export interface FailedTask {
  taskId: number | null;
  attempt: number | null;
  executorId: string | null;
  host: string | null;
  status: string | null;
  errorMessage: string | null;
}

export function failedTask(init: Partial<FailedTask> = {}): FailedTask {
  return {
    taskId: init.taskId ?? null,
    attempt: init.attempt ?? null,
    executorId: init.executorId ?? null,
    host: init.host ?? null,
    status: init.status ?? null,
    errorMessage: init.errorMessage ?? null,
  };
}
