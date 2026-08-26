/**
 * MCP server wiring.
 *
 * Port of `spark_history_mcp/core/app.py`. Uses the low-level `Server` class
 * from the TypeScript SDK rather than the `McpServer` helper, because the tool
 * results must match FastMCP's exactly: one text block per list element, and
 * `structuredContent` only for the tools whose Python signature declares a
 * concrete return type.
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import {
  CallToolRequestSchema,
  GetPromptRequestSchema,
  ListPromptsRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

import { SparkRestClient } from "../api/sparkClient.js";
import type { Config } from "../config/config.js";
import { PROMPT_HANDLERS } from "../prompts/prompts.js";
import { PROMPT_CATALOG, TOOL_CATALOG } from "../schemas/generated.js";
import { TOOL_HANDLERS } from "../tools/tools.js";
import { ApplicationDiscovery } from "../utils/utils.js";
import { errorText } from "./errors.js";
import { validatePromptArguments, validateToolArguments } from "./validation.js";
import { pythonJsonDumps } from "./json.js";

export const SERVER_NAME = "Spark Events";
export const SERVER_VERSION = "0.1.0";

export interface AppContext {
  clients: Record<string, SparkRestClient>;
  defaultClient: SparkRestClient | null;
  appDiscovery: ApplicationDiscovery;
}

/**
 * How a tool's return value maps onto `structuredContent`.
 *
 * FastMCP builds an output schema from the Python return annotation. When that
 * annotation is a pydantic model the dump is used as-is (`"object"`); for
 * `list[...]` and `Dict[str, Any]` it is wrapped in `{"result": ...}`
 * (`"wrapped"`); an unannotated `-> list` produces no schema at all
 * (`"none"`).
 */
type StructuredMode = "none" | "wrapped" | "object";

const STRUCTURED_MODE: Record<string, StructuredMode> = {
  list_applications: "wrapped",
  list_jobs: "none",
  list_stages: "none",
  get_stage: "object",
  list_stage_task_failures: "none",
  get_environment: "none",
  list_executors: "none",
  get_executor_summary: "none",
  get_executor_thread_dump: "none",
  compare_job_environments: "wrapped",
  compare_job_performance: "wrapped",
  list_sql_executions: "wrapped",
  get_sql_execution: "object",
  compare_sql_executions: "object",
  compare_stages: "object",
  get_job_bottlenecks: "wrapped",
  get_resource_usage_timeline: "wrapped",
};

/**
 * Build the per-server Spark clients described by the configuration.
 *
 * Equivalent to the Python `app_lifespan` context manager.
 */
export function buildAppContext(config: Config): AppContext {
  const clients: Record<string, SparkRestClient> = {};
  let defaultClient: SparkRestClient | null = null;

  for (const [name, serverConfig] of Object.entries(config.servers)) {
    if (serverConfig.emr_cluster_arn) {
      // EMR persistent-UI support (AWS SDK, presigned URLs, cookie re-auth) is
      // outside the scope of this port; fail loudly rather than silently
      // querying an unauthenticated endpoint.
      throw new Error(
        `Server '${name}' is configured with emr_cluster_arn, but EMR ` +
          "persistent-UI support is not included in this TypeScript port. " +
          "Point the server at a directly reachable Spark History Server URL " +
          "instead.",
      );
    }
    clients[name] = new SparkRestClient(serverConfig);
    if (serverConfig.default) {
      defaultClient = clients[name];
    }
  }

  return {
    clients,
    defaultClient,
    appDiscovery: new ApplicationDiscovery(clients),
  };
}

/** Turn a tool's return value into MCP content blocks, the way FastMCP does. */
function toContent(result: unknown): Array<{ type: "text"; text: string }> {
  if (Array.isArray(result)) {
    return result.map((item) => ({
      type: "text" as const,
      text: pythonJsonDumps(item),
    }));
  }
  return [{ type: "text" as const, text: pythonJsonDumps(result) }];
}

export function createServer(context: AppContext): Server {
  const server = new Server(
    { name: SERVER_NAME, version: SERVER_VERSION },
    { capabilities: { tools: {}, prompts: {} } },
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOL_CATALOG.map((tool) => ({
      name: tool.name,
      description: tool.description,
      inputSchema: tool.inputSchema as { type: "object" },
      ...(tool.outputSchema ? { outputSchema: tool.outputSchema as { type: "object" } } : {}),
    })),
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const name = request.params.name;
    const handler = TOOL_HANDLERS[name];
    const entry = TOOL_CATALOG.find((tool) => tool.name === name);
    if (!handler || !entry) {
      return {
        content: [{ type: "text" as const, text: `Unknown tool: ${name}` }],
        isError: true,
      };
    }

    try {
      // Validated and coerced up front, the way FastMCP validates against the
      // pydantic model built from the Python signature.
      const args = validateToolArguments(
        name,
        entry.inputSchema,
        request.params.arguments ?? {},
      );
      const result = await handler(context, args);
      const mode = STRUCTURED_MODE[name] ?? "none";
      const payload: Record<string, unknown> = { content: toContent(result) };
      if (mode === "wrapped") {
        payload.structuredContent = { result };
      } else if (mode === "object") {
        payload.structuredContent = result;
      }
      return payload;
    } catch (error) {
      return {
        content: [
          {
            type: "text" as const,
            text: `Error executing tool ${name}: ${errorText(error)}`,
          },
        ],
        isError: true,
      };
    }
  });

  server.setRequestHandler(ListPromptsRequestSchema, async () => ({
    prompts: PROMPT_CATALOG.map((prompt) => ({
      name: prompt.name,
      ...(prompt.title ? { title: prompt.title } : {}),
      description: prompt.description,
      arguments: prompt.arguments.map((argument) => ({
        name: argument.name,
        ...(argument.description ? { description: argument.description } : {}),
        required: argument.required,
      })),
    })),
  }));

  server.setRequestHandler(GetPromptRequestSchema, async (request) => {
    const name = request.params.name;
    const handler = PROMPT_HANDLERS[name];
    const entry = PROMPT_CATALOG.find((prompt) => prompt.name === name);
    if (!handler || !entry) {
      throw new Error(`Unknown prompt: ${name}`);
    }

    const args = request.params.arguments ?? {};
    try {
      validatePromptArguments(name, entry.arguments, args);
    } catch (error) {
      if ((error as Error).name === "ValidationError") {
        throw new Error(
          `Error rendering prompt ${name}: ${(error as Error).message}`,
        );
      }
      throw error;
    }

    const text = handler(args);
    return {
      description: entry.description,
      messages: [{ role: "user" as const, content: { type: "text" as const, text } }],
    };
  });

  return server;
}
