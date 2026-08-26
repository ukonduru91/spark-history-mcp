/**
 * Simple timeout handling and parallel execution for MCP tools.
 *
 * Port of `spark_history_mcp/utils/utils.py`.
 */

import type { SparkRestClient } from "../api/sparkClient.js";
import { ApiException } from "../api/httpClient.js";
import { errorText } from "../core/errors.js";

export interface ParallelResult {
  results: Record<string, unknown>;
  errors: string[];
}

export interface NamedCall {
  name: string;
  run: () => Promise<unknown>;
}

/**
 * Execute multiple API calls in parallel with error handling.
 *
 * @param apiCalls List of `{name, run}` pairs
 * @param maxWorkers Maximum number of parallel workers
 * @param timeout Total timeout, in seconds, for all operations
 */
export async function parallelExecute(
  apiCalls: NamedCall[],
  maxWorkers = 6,
  timeout = 180,
): Promise<ParallelResult> {
  const results: Record<string, unknown> = {};
  const errors: string[] = [];

  const queue = [...apiCalls];
  const deadline = Date.now() + timeout * 1000;

  async function worker(): Promise<void> {
    for (;;) {
      const call = queue.shift();
      if (!call) return;
      if (Date.now() > deadline) {
        errors.push(`${call.name} failed: timed out after ${timeout}s`);
        continue;
      }
      try {
        results[call.name] = await call.run();
      } catch (error) {
        errors.push(describeFailure(call.name, error));
      }
    }
  }

  const workers = Array.from(
    { length: Math.max(1, Math.min(maxWorkers, apiCalls.length)) },
    () => worker(),
  );
  await Promise.all(workers);

  return { results, errors };
}

function describeFailure(name: string, error: unknown): string {
  if (error instanceof ApiException) {
    // Detect out-of-memory from the response body on a 5xx.
    const status = error.status;
    const body = error.body ?? "";
    if (
      status !== null &&
      status >= 500 &&
      status <= 599 &&
      (body.includes("OutOfMemoryError") || body.includes("Java heap space"))
    ) {
      return `${name} failed: Spark History Server out of memory (increase SPARK_DAEMON_MEMORY)`;
    }
    return `${name} failed: HTTP ${status} - ${errorText(error)}`;
  }
  return `${name} failed: ${errorText(error)}`;
}

/**
 * Application discovery with TTL cache and collision handling.
 */
export class ApplicationDiscovery {
  private readonly clients: Record<string, SparkRestClient>;
  private readonly ttl: number;
  private readonly cache = new Map<string, { servers: string[]; lastUpdated: number }>();

  constructor(clients: Record<string, SparkRestClient>, ttl = 300) {
    this.clients = clients;
    this.ttl = ttl;
  }

  private isExpired(entry: { lastUpdated: number }): boolean {
    return Date.now() / 1000 - entry.lastUpdated > this.ttl;
  }

  async findApplicationServers(appId: string): Promise<string[]> {
    const cached = this.cache.get(appId);
    if (cached && !this.isExpired(cached)) {
      return cached.servers;
    }

    const servers: string[] = [];
    for (const [serverName, client] of Object.entries(this.clients)) {
      try {
        await client.getApplication(appId);
        servers.push(serverName);
      } catch {
        continue;
      }
    }

    this.cache.set(appId, { servers, lastUpdated: Date.now() / 1000 });
    return servers;
  }

  async getClientForApp(
    appId: string,
    serverName?: string | null,
  ): Promise<[SparkRestClient, string]> {
    if (serverName) {
      const client = this.clients[serverName];
      if (!client) {
        throw new Error(`Server '${serverName}' not found`);
      }
      return [client, serverName];
    }

    const servers = await this.findApplicationServers(appId);
    if (servers.length === 0) {
      throw new Error(`Application '${appId}' not found on any server`);
    }

    const chosenServer = servers[0];
    return [this.clients[chosenServer], chosenServer];
  }
}
