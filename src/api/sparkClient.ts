/**
 * Thin facade over the Spark History Server REST API.
 *
 * Port of `spark_history_mcp/api/spark_client.py`. Methods return the generated
 * model shapes from {@link ../models/generated.js}. The facade adds client-side
 * `offset`/`length` pagination and list-of-string status filtering on top of the
 * REST surface.
 *
 * A specific YARN application attempt is addressed by embedding the attempt id
 * in the application path (`/applications/{base-app-id}/{attempt-id}/...`).
 * Every application-scoped method takes an optional `appAttemptId` that is
 * composed into the application id; omitting it selects the latest/only attempt.
 * There is no implicit retry against a hard-coded attempt id.
 *
 * **Authentication.** Username/password (basic) and bearer tokens are applied as
 * an `Authorization` header. EMR persistent-UI servers authenticate with a
 * session cookie instead: {@link SparkRestClient.configureCookies} installs a
 * `Cookie` header and an optional re-auth callback that refreshes it when a
 * request is rejected with 401/403.
 */

import type { ServerConfig } from "../config/config.js";
import { fromDict, fromDictList } from "../models/deserialize.js";
import type {
  Application,
  Environment,
  Executor,
  Job,
  SQLExecution,
  StageData,
  Task,
  TaskMetricsSummary,
  ThreadStackTrace,
} from "../models/generated.js";
import {
  ForbiddenException,
  RestClient,
  UnauthorizedException,
  NotFoundException,
  type QueryValue,
} from "./httpClient.js";

const DEFAULT_QUANTILES = "0.05, 0.25, 0.5, 0.75, 0.95";
const PROXY_URL = "socks5h://localhost:8157";

/**
 * Raised when an application has multiple attempts and none was specified.
 *
 * Replaces the History Server's misleading `404 no such app` for a multi-attempt
 * YARN application with an actionable message listing the available attempts. No
 * data is returned; the caller must retry with `appAttemptId`.
 */
export class AttemptRequiredError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AttemptRequiredError";
  }
}

export interface PaginationOptions {
  offset?: number;
  length?: number | null;
}

export class SparkRestClient {
  readonly config: ServerConfig;
  readonly baseUrl: string;
  readonly useProxy: boolean;
  readonly verifySsl: boolean;
  readonly sslCaCert: string | null;
  readonly timeout: number;

  private readonly rest: RestClient;
  /** Optional callback returning a fresh Cookie header (EMR re-auth). */
  private reauth: (() => string | Promise<string>) | null = null;

  constructor(serverConfig: ServerConfig) {
    this.config = serverConfig;
    this.baseUrl = (serverConfig.url ?? "").replace(/\/+$/, "") + "/api/v1";
    this.useProxy = serverConfig.use_proxy;
    this.verifySsl = serverConfig.verify_ssl;
    this.sslCaCert = serverConfig.ssl_ca_cert ?? null;
    this.timeout = serverConfig.timeout;

    this.rest = new RestClient(this.baseUrl, {
      verifySsl: this.verifySsl,
      sslCaCert: this.sslCaCert,
      proxy: this.useProxy ? PROXY_URL : null,
      timeout: this.timeout,
    });

    const auth = serverConfig.auth;
    if (auth) {
      if (auth.username && auth.password) {
        const token = Buffer.from(`${auth.username}:${auth.password}`).toString(
          "base64",
        );
        this.rest.setDefaultHeader("Authorization", `Basic ${token}`);
      } else if (auth.token) {
        this.rest.setDefaultHeader("Authorization", `Bearer ${auth.token}`);
      }
    }
  }

  /* ---------------------------------------------------------------- */
  /* Auth / transport configuration                                    */
  /* ---------------------------------------------------------------- */

  /**
   * Authenticate via a `Cookie` header (used for EMR persistent UI).
   *
   * @param cookieHeader Serialized `name=value; ...` cookie string.
   * @param reauth Optional callable returning a fresh cookie header, invoked
   *   once to recover from a 401/403 (e.g. cookie rotation).
   */
  configureCookies(
    cookieHeader: string,
    reauth: (() => string | Promise<string>) | null = null,
  ): void {
    this.applyCookie(cookieHeader);
    this.reauth = reauth;
  }

  private applyCookie(cookieHeader: string): void {
    this.rest.setDefaultHeader("Cookie", cookieHeader);
  }

  /**
   * Cross-cutting error handling for application-scoped methods.
   *
   * * On 401/403, if a re-auth callback is configured, refresh the cookie and
   *   retry once (covers EMR session-cookie rotation).
   * * On 404 with no `appAttemptId` for an app that has named attempts, re-raise
   *   as {@link AttemptRequiredError}; otherwise propagate. The attempt lookup
   *   only happens on the failure path.
   */
  private async resilient<T>(
    appId: string | undefined,
    appAttemptId: string | null | undefined,
    call: () => Promise<T>,
  ): Promise<T> {
    try {
      return await call();
    } catch (error) {
      if (
        error instanceof UnauthorizedException ||
        error instanceof ForbiddenException
      ) {
        if (!this.reauth) throw error;
        this.applyCookie(await this.reauth());
        return await call();
      }
      if (error instanceof NotFoundException) {
        if (appAttemptId) throw error;
        const attempts = appId ? await this.attemptIds(appId) : [];
        if (attempts.length > 0) {
          throw new AttemptRequiredError(
            `Application '${appId}' has multiple attempts ` +
              `(${attempts.join(", ")}); retry with app_attempt_id ` +
              `(e.g. app_attempt_id='${attempts[0]}').`,
          );
        }
        throw error;
      }
      throw error;
    }
  }

  /** Compose the application path segment (`{appId}/{attempt}` or bare). */
  private static appPath(appId: string, appAttemptId?: string | null): string {
    return appAttemptId ? `${appId}/${appAttemptId}` : appId;
  }

  /** Named attempt ids for an application; `[]` on any failure. */
  private async attemptIds(appId: string): Promise<string[]> {
    try {
      const application = await this.getApplication(appId);
      const attempts = application?.attempts ?? [];
      return attempts
        .map((attempt) => attempt.attemptId)
        .filter((id): id is string => Boolean(id));
    } catch {
      return [];
    }
  }

  private static paginate<T>(
    items: T[],
    offset: number,
    length: number | null | undefined,
  ): T[] {
    if (length !== null && length !== undefined) {
      return items.slice(offset, offset + length);
    }
    return offset ? items.slice(offset) : items;
  }

  private request<T>(path: string, query: Record<string, QueryValue> = {}) {
    return this.rest.get<T>(path, query, this.timeout);
  }

  /* ---------------------------------------------------------------- */
  /* Applications                                                      */
  /* ---------------------------------------------------------------- */

  /** List applications, optionally filtered by status/date and limited. */
  async listApplications(options: {
    status?: string[] | null;
    minDate?: string | null;
    maxDate?: string | null;
    minEndDate?: string | null;
    maxEndDate?: string | null;
    limit?: number | null;
  } = {}): Promise<Application[]> {
    // The generated client accepts a single status string.
    const statusParam = options.status?.length ? options.status[0] : null;
    const raw = await this.request<unknown>("/applications", {
      status: statusParam,
      minDate: options.minDate ?? null,
      maxDate: options.maxDate ?? null,
      minEndDate: options.minEndDate ?? null,
      maxEndDate: options.maxEndDate ?? null,
      limit: options.limit ?? null,
    });
    return fromDictList<Application>("Application", raw);
  }

  /** Get a single application (with all its attempts). */
  async getApplication(appId: string): Promise<Application> {
    const raw = await this.request<unknown>(
      `/applications/${encodeAppPath(appId)}`,
    );
    return fromDict<Application>("Application", raw) as Application;
  }

  /* ---------------------------------------------------------------- */
  /* Jobs                                                              */
  /* ---------------------------------------------------------------- */

  /** List jobs for an application, with optional status filter/pagination. */
  async listJobs(options: {
    appId: string;
    status?: string[] | null;
    appAttemptId?: string | null;
    offset?: number;
    length?: number | null;
  }): Promise<Job[]> {
    const { appId, status, appAttemptId } = options;
    const offset = options.offset ?? 0;
    const length = options.length ?? null;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/jobs`,
      );
      let jobs = fromDictList<Job>("Job", raw);

      if (status && status.length > 0) {
        const wanted = new Set(status.map((value) => value.toUpperCase()));
        jobs = jobs.filter((job) => job.status && wanted.has(job.status));
      }

      return SparkRestClient.paginate(jobs, offset, length);
    });
  }

  /* ---------------------------------------------------------------- */
  /* Stages                                                            */
  /* ---------------------------------------------------------------- */

  /** List stages for an application, with optional status filter/pagination. */
  async listStages(options: {
    appId: string;
    status?: string[] | null;
    details?: boolean;
    withSummaries?: boolean;
    quantiles?: string;
    taskStatus?: string[] | null;
    appAttemptId?: string | null;
    offset?: number;
    length?: number | null;
  }): Promise<StageData[]> {
    const {
      appId,
      status,
      details = false,
      withSummaries = false,
      quantiles = DEFAULT_QUANTILES,
      taskStatus,
      appAttemptId,
    } = options;
    const offset = options.offset ?? 0;
    const length = options.length ?? null;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const taskStatusParam =
        taskStatus && taskStatus.length > 0 && details ? taskStatus[0] : null;
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/stages`,
        {
          details,
          taskStatus: taskStatusParam,
          withSummaries,
          quantiles,
        },
      );
      let stages = fromDictList<StageData>("StageData", raw);

      if (status && status.length > 0) {
        const wanted = new Set(status.map((value) => value.toUpperCase()));
        stages = stages.filter((stage) => stage.status && wanted.has(stage.status));
      }

      return SparkRestClient.paginate(stages, offset, length);
    });
  }

  /** Get all attempts for a specific stage. */
  async listStageAttempts(options: {
    appId: string;
    stageId: number;
    details?: boolean;
    taskStatus?: string[] | null;
    withSummaries?: boolean;
    quantiles?: string;
    appAttemptId?: string | null;
  }): Promise<StageData[]> {
    const {
      appId,
      stageId,
      details = false,
      taskStatus,
      withSummaries = true,
      quantiles = DEFAULT_QUANTILES,
      appAttemptId,
    } = options;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const taskStatusParam =
        taskStatus && taskStatus.length > 0 ? taskStatus[0] : null;
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/stages/${stageId}`,
        { details, taskStatus: taskStatusParam, withSummaries, quantiles },
      );
      return fromDictList<StageData>("StageData", raw);
    });
  }

  /** Get a specific stage attempt (`attemptId` is the *stage* attempt). */
  async getStageAttempt(options: {
    appId: string;
    stageId: number;
    attemptId: number;
    details?: boolean;
    taskStatus?: string[] | null;
    withSummaries?: boolean;
    quantiles?: string;
    appAttemptId?: string | null;
  }): Promise<StageData> {
    const {
      appId,
      stageId,
      attemptId,
      details = true,
      taskStatus,
      withSummaries = false,
      quantiles = DEFAULT_QUANTILES,
      appAttemptId,
    } = options;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const taskStatusParam =
        taskStatus && taskStatus.length > 0 ? taskStatus[0] : null;
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/stages/${stageId}/${attemptId}`,
        { details, taskStatus: taskStatusParam, withSummaries, quantiles },
      );
      return fromDict<StageData>("StageData", raw) as StageData;
    });
  }

  /** Get task summary metrics for a specific stage attempt. */
  async getStageTaskSummary(options: {
    appId: string;
    stageId: number;
    attemptId: number;
    quantiles?: string;
    appAttemptId?: string | null;
  }): Promise<TaskMetricsSummary> {
    const {
      appId,
      stageId,
      attemptId,
      quantiles = DEFAULT_QUANTILES,
      appAttemptId,
    } = options;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(
          appPath,
        )}/stages/${stageId}/${attemptId}/taskSummary`,
        { quantiles },
      );
      return fromDict<TaskMetricsSummary>(
        "TaskMetricsSummary",
        raw,
      ) as TaskMetricsSummary;
    });
  }

  /** List tasks for a specific stage attempt, optionally filtered by status. */
  async listStageTasks(options: {
    appId: string;
    stageId: number;
    attemptId: number;
    status?: string | null;
    sortBy?: string | null;
    offset?: number | null;
    length?: number | null;
    appAttemptId?: string | null;
  }): Promise<Task[]> {
    const { appId, stageId, attemptId, appAttemptId } = options;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(
          appPath,
        )}/stages/${stageId}/${attemptId}/taskList`,
        {
          offset: options.offset ?? null,
          length: options.length ?? null,
          sortBy: options.sortBy ?? null,
          status: options.status ?? null,
        },
      );
      return fromDictList<Task>("Task", raw);
    });
  }

  /* ---------------------------------------------------------------- */
  /* Executors                                                         */
  /* ---------------------------------------------------------------- */

  /** List active executors for an application. */
  async listExecutors(options: {
    appId: string;
    appAttemptId?: string | null;
    offset?: number;
    length?: number | null;
  }): Promise<Executor[]> {
    const { appId, appAttemptId } = options;
    const offset = options.offset ?? 0;
    const length = options.length ?? null;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/executors`,
      );
      return SparkRestClient.paginate(
        fromDictList<Executor>("Executor", raw),
        offset,
        length,
      );
    });
  }

  /** List all executors (active and inactive) for an application. */
  async listAllExecutors(options: {
    appId: string;
    appAttemptId?: string | null;
    offset?: number;
    length?: number | null;
  }): Promise<Executor[]> {
    const { appId, appAttemptId } = options;
    const offset = options.offset ?? 0;
    const length = options.length ?? null;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/allexecutors`,
      );
      return SparkRestClient.paginate(
        fromDictList<Executor>("Executor", raw),
        offset,
        length,
      );
    });
  }

  /**
   * Get the JVM thread dump for a driver or executor.
   *
   * Use `"driver"` for the driver. The application must be running; completed
   * applications return 404 because the History Server does not persist thread
   * dumps.
   */
  async getExecutorThreadDump(options: {
    appId: string;
    executorId: string;
    appAttemptId?: string | null;
  }): Promise<ThreadStackTrace[]> {
    const { appId, executorId, appAttemptId } = options;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/executors/${encodeURIComponent(
          executorId,
        )}/threads`,
      );
      return fromDictList<ThreadStackTrace>("ThreadStackTrace", raw);
    });
  }

  /* ---------------------------------------------------------------- */
  /* Environment                                                       */
  /* ---------------------------------------------------------------- */

  /** Get environment/configuration information for an application. */
  async getEnvironment(options: {
    appId: string;
    appAttemptId?: string | null;
  }): Promise<Environment> {
    const { appId, appAttemptId } = options;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/environment`,
      );
      return fromDict<Environment>("Environment", raw) as Environment;
    });
  }

  /* ---------------------------------------------------------------- */
  /* SQL                                                               */
  /* ---------------------------------------------------------------- */

  /** List SQL executions for an application (pagination is server-side). */
  async getSqlList(options: {
    appId: string;
    appAttemptId?: string | null;
    details?: boolean;
    planDescription?: boolean;
    offset?: number;
    length?: number;
  }): Promise<SQLExecution[]> {
    const {
      appId,
      appAttemptId,
      details = true,
      planDescription = false,
      offset = 0,
      length = 20,
    } = options;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/sql`,
        { details, planDescription, offset, length },
      );
      return fromDictList<SQLExecution>("SQLExecution", raw);
    });
  }

  /** Get a specific SQL execution. */
  async getSqlExecution(options: {
    appId: string;
    executionId: number;
    appAttemptId?: string | null;
    details?: boolean;
    planDescription?: boolean;
  }): Promise<SQLExecution> {
    const {
      appId,
      executionId,
      appAttemptId,
      details = true,
      planDescription = true,
    } = options;

    return this.resilient(appId, appAttemptId, async () => {
      const appPath = SparkRestClient.appPath(appId, appAttemptId);
      const raw = await this.request<unknown>(
        `/applications/${encodeAppPath(appPath)}/sql/${executionId}`,
        { details, planDescription },
      );
      return fromDict<SQLExecution>("SQLExecution", raw) as SQLExecution;
    });
  }
}

/**
 * Percent-encode an application path while leaving `/` intact, mirroring the
 * Python client's `safe_chars_for_path_param = "/"`.
 */
function encodeAppPath(appPath: string): string {
  return appPath
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}
