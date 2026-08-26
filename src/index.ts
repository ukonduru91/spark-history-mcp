#!/usr/bin/env node
/**
 * Main entry point for the Spark History Server MCP (TypeScript port).
 *
 * Port of `spark_history_mcp/core/main.py` plus the transport wiring from
 * `core/app.py`.
 */

import { randomUUID } from "node:crypto";
import process from "node:process";

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import express from "express";

import { buildAppContext, createServer } from "./core/app.js";
import {
  dumpConfig,
  loadConfig,
  resolveConfigPath,
  type Config,
  type TransportName,
} from "./config/config.js";

let debugEnabled = false;

function log(level: "INFO" | "DEBUG" | "WARNING" | "ERROR", message: string): void {
  if (level === "DEBUG" && !debugEnabled) return;
  const now = new Date();
  const stamp =
    `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ` +
    `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())},` +
    String(now.getMilliseconds()).padStart(3, "0");
  process.stderr.write(`${stamp} - spark_history_mcp - ${level} - ${message}\n`);
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}

interface CliArgs {
  config: string | null;
}

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = { config: null };
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === "--config" || token === "-c") {
      args.config = argv[++i] ?? null;
    } else if (token.startsWith("--config=")) {
      args.config = token.slice("--config=".length);
    } else if (token === "--help" || token === "-h") {
      process.stdout.write(
        "usage: spark-mcp [-h] [--config CONFIG]\n\n" +
          "Spark History Server MCP\n\n" +
          "options:\n" +
          "  -h, --help            show this help message and exit\n" +
          "  --config CONFIG, -c CONFIG\n" +
          "                        Path to config file. If omitted, the server searches\n" +
          "                        $SHS_MCP_CONFIG, then ./config.yaml, then\n" +
          "                        ~/.config/spark-mcp/config.yaml (env: SHS_MCP_CONFIG).\n",
      );
      process.exit(0);
    }
  }
  return args;
}

function resolveTransport(config: Config): TransportName {
  let transport: TransportName | null =
    (process.env.SHS_MCP_TRANSPORT as TransportName | undefined) ??
    config.mcp.transport ??
    null;

  if (
    !transport &&
    config.mcp.__fields_set__.has("transports") &&
    config.mcp.transports &&
    config.mcp.transports.length > 0
  ) {
    transport = config.mcp.transports[0];
  }

  return transport ?? "streamable-http";
}

async function runStdio(config: Config): Promise<void> {
  const context = buildAppContext(config);
  const server = createServer(context);
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

async function runStreamableHttp(config: Config): Promise<void> {
  const context = buildAppContext(config);
  const security = config.mcp.transport_security;

  const app = express();
  app.use(express.json());

  const transports = new Map<string, StreamableHTTPServerTransport>();

  app.post("/mcp", async (req, res) => {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    let transport = sessionId ? transports.get(sessionId) : undefined;

    if (!transport && isInitializeRequest(req.body)) {
      transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
        enableDnsRebindingProtection:
          security?.enable_dns_rebinding_protection ?? false,
        allowedHosts: security?.allowed_hosts,
        allowedOrigins: security?.allowed_origins,
        onsessioninitialized: (id: string) => {
          transports.set(id, transport as StreamableHTTPServerTransport);
        },
      });
      transport.onclose = () => {
        if (transport?.sessionId) transports.delete(transport.sessionId);
      };
      const server = createServer(context);
      await server.connect(transport);
    }

    if (!transport) {
      res.status(400).json({
        jsonrpc: "2.0",
        error: { code: -32000, message: "Bad Request: no valid session ID provided" },
        id: null,
      });
      return;
    }

    await transport.handleRequest(req, res, req.body);
  });

  const sessionRoute = async (
    req: express.Request,
    res: express.Response,
  ): Promise<void> => {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    const transport = sessionId ? transports.get(sessionId) : undefined;
    if (!transport) {
      res.status(400).send("Invalid or missing session ID");
      return;
    }
    await transport.handleRequest(req, res);
  };

  app.get("/mcp", sessionRoute);
  app.delete("/mcp", sessionRoute);

  const port = Number(config.mcp.port ?? 18888);
  const address = config.mcp.address ?? "localhost";

  await new Promise<void>((resolve) => {
    app.listen(port, address, () => {
      log("INFO", `Streamable HTTP transport listening on http://${address}:${port}/mcp`);
      resolve();
    });
  });
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  try {
    log("INFO", "Starting Spark History Server MCP...");

    // An explicit --config flag wins over a pre-existing SHS_MCP_CONFIG; if
    // omitted, leave the env untouched so discovery can run.
    if (args.config !== null) {
      process.env.SHS_MCP_CONFIG = args.config;
    }

    const [configPath] = resolveConfigPath();
    if (configPath !== null) {
      log("INFO", `Using config file: ${configPath}`);
    } else {
      log("INFO", "No config file found; using built-in defaults and SHS_* env vars");
    }

    const config = loadConfig();
    debugEnabled = Boolean(config.mcp.debug);
    log("DEBUG", JSON.stringify(dumpConfig(config), null, 4));

    const transport = resolveTransport(config);
    if (transport === "stdio") {
      await runStdio(config);
    } else {
      await runStreamableHttp(config);
    }
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    if (err.code === "ENOENT") {
      log("ERROR", `Configuration error: ${err.message}`);
      process.exit(1);
    }
    log("ERROR", `Failed to start MCP server: ${(error as Error).message}`);
    process.exit(1);
  }
}

void main();
