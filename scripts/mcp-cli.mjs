#!/usr/bin/env node
/**
 * A tiny command-line client for this MCP server.
 *
 * Useful for smoke-testing a configuration without wiring up an LLM client, and
 * for scripting one-off queries against a Spark History Server.
 *
 * Usage:
 *   node scripts/mcp-cli.mjs list-tools
 *   node scripts/mcp-cli.mjs list-prompts
 *   node scripts/mcp-cli.mjs call <tool> '<json arguments>'
 *   node scripts/mcp-cli.mjs prompt <name> '<json arguments>'
 *
 * Examples:
 *   node scripts/mcp-cli.mjs call list_applications '{"limit": 5}'
 *   node scripts/mcp-cli.mjs call list_stages '{"app_id":"app-1","sort_by":"duration","length":5}'
 *
 * The server is launched over stdio with the same configuration resolution as
 * normal (SHS_MCP_CONFIG / ./config.yaml / ~/.config/spark-mcp/config.yaml, and
 * SHS_* environment variables), so `--config` and env overrides work here too:
 *
 *   SHS_SERVERS__LOCAL__URL=http://spark-history:18080 \
 *     node scripts/mcp-cli.mjs call list_applications '{}'
 */

import path from "node:path";
import { fileURLToPath } from "node:url";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const serverEntry = path.join(here, "..", "dist", "index.js");

function usage(message) {
  if (message) console.error(`error: ${message}\n`);
  console.error(
    [
      "usage:",
      "  node scripts/mcp-cli.mjs list-tools",
      "  node scripts/mcp-cli.mjs list-prompts",
      "  node scripts/mcp-cli.mjs call <tool> '<json arguments>'",
      "  node scripts/mcp-cli.mjs prompt <name> '<json arguments>'",
    ].join("\n"),
  );
  process.exit(message ? 2 : 0);
}

async function main() {
  const [command, name, rawArgs] = process.argv.slice(2);
  if (!command || command === "--help" || command === "-h") usage();

  let args = {};
  if (rawArgs) {
    try {
      args = JSON.parse(rawArgs);
    } catch (error) {
      usage(`arguments must be valid JSON: ${error.message}`);
    }
  }

  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [serverEntry],
    env: { ...process.env, SHS_MCP__TRANSPORT: "stdio" },
    stderr: "inherit",
  });

  const client = new Client({ name: "spark-mcp-cli", version: "1.0.0" });
  await client.connect(transport);

  try {
    switch (command) {
      case "list-tools": {
        const { tools } = await client.listTools();
        for (const tool of tools) {
          const required = tool.inputSchema?.required ?? [];
          console.log(`${tool.name}(${required.join(", ")})`);
          console.log(`    ${(tool.description ?? "").trim().split("\n")[0]}`);
        }
        break;
      }
      case "list-prompts": {
        const { prompts } = await client.listPrompts();
        for (const prompt of prompts) {
          const names = (prompt.arguments ?? []).map(
            (argument) => `${argument.name}${argument.required ? "" : "?"}`,
          );
          console.log(`${prompt.name}(${names.join(", ")})`);
        }
        break;
      }
      case "call": {
        if (!name) usage("a tool name is required");
        const result = await client.callTool({ name, arguments: args });
        for (const block of result.content ?? []) {
          if (block.type === "text") console.log(block.text);
        }
        if (result.isError) process.exitCode = 1;
        break;
      }
      case "prompt": {
        if (!name) usage("a prompt name is required");
        const result = await client.getPrompt({ name, arguments: args });
        for (const message of result.messages ?? []) {
          console.log(message.content.text);
        }
        break;
      }
      default:
        usage(`unknown command: ${command}`);
    }
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error(error.message ?? String(error));
  process.exit(1);
});
