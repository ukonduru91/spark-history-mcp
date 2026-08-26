/**
 * Unit tests for the pieces of the port whose behaviour must match Python
 * exactly: timestamp parsing, plan text handling, model deserialization,
 * JSON rendering and configuration precedence.
 *
 * Run with `npm test`.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, before, describe, it } from "node:test";

import {
  durationMs,
  durationSeconds,
  parseSparkDatetime,
  stripInitialPlans,
  truncatePlanDescription,
} from "../src/tools/tools.js";
import { fromDict, fromDictList, modelCopyDeep } from "../src/models/deserialize.js";
import { pythonJsonDumps } from "../src/core/json.js";
import { parseJsonPreservingFloats, pyFloat } from "../src/core/pyfloat.js";
import { investigateFailure, compareApplications } from "../src/prompts/prompts.js";
import {
  validatePromptArguments,
  validateToolArguments,
} from "../src/core/validation.js";
import { TOOL_CATALOG } from "../src/schemas/generated.js";

describe("timestamp parsing", () => {
  it("parses Spark GMT timestamps", () => {
    const ms = parseSparkDatetime("2025-06-27T03:56:52.238GMT");
    assert.equal(ms, Date.UTC(2025, 5, 27, 3, 56, 52, 238));
  });

  it("returns null for malformed GMT timestamps", () => {
    assert.equal(parseSparkDatetime("not-a-date-GMT"), null);
  });

  it("accepts epoch milliseconds and ISO strings", () => {
    assert.equal(parseSparkDatetime(1750996612238), 1750996612238);
    assert.equal(
      parseSparkDatetime("2025-06-27T03:56:52.238Z"),
      Date.UTC(2025, 5, 27, 3, 56, 52, 238),
    );
  });

  it("treats a missing endpoint as a zero duration, as an int", () => {
    // Python's early `return 0` yields an int, which renders as `0` not `0.0`.
    assert.equal(Number(durationSeconds(null, "2025-06-27T03:56:52.238GMT")), 0);
    assert.equal(pythonJsonDumps(durationSeconds(null, "x")), "0");
    assert.equal(durationMs("2025-06-27T03:56:52.238GMT", null), 0);
  });

  it("computes durations in seconds as a float and milliseconds as an int", () => {
    const start = "2025-06-27T03:56:52.238GMT";
    const end = "2025-06-27T03:56:53.006GMT";
    assert.equal(Number(durationSeconds(start, end)), 0.768);
    assert.equal(durationMs(start, end), 768);
    // A whole-second duration still renders as a float, like Python's timedelta.
    assert.equal(
      pythonJsonDumps(durationSeconds(start, "2025-06-27T03:56:53.238GMT")),
      "1.0",
    );
  });
});

describe("plan text handling", () => {
  it("strips AQE initial plans and keeps the final plan", () => {
    const plan = [
      "AdaptiveSparkPlan isFinalPlan=true",
      "+- == Final Plan ==",
      "   *(3) HashAggregate",
      "+- == Initial Plan ==",
      "   SortMergeJoin",
      "   +- Exchange hashpartitioning",
      "",
      "TrailingLine",
    ].join("\n");
    assert.equal(
      stripInitialPlans(plan),
      [
        "AdaptiveSparkPlan isFinalPlan=true",
        "+- == Final Plan ==",
        "   *(3) HashAggregate",
        "TrailingLine",
      ].join("\n"),
    );
  });

  it("leaves a plan without initial sections untouched", () => {
    const plan = "Project\n+- Scan parquet";
    assert.equal(stripInitialPlans(plan), plan);
  });

  it("truncates at a line boundary when close enough to the limit", () => {
    // Expectations cross-checked against the Python implementation.
    const plan = "aaaa\nbbbb\ncccc\ndddd";
    assert.equal(truncatePlanDescription(plan, 100), plan);
    // Last newline at index 9 is below 0.8 * 12, so the cut is mid-line.
    assert.equal(truncatePlanDescription(plan, 12), "aaaa\nbbbb\ncc\n... [truncated]");
    // Last newline at index 10 is above 0.8 * 12, so it snaps to the boundary.
    assert.equal(
      truncatePlanDescription("aaaaaaaaaa\nbb", 12),
      "aaaaaaaaaa\n... [truncated]",
    );
  });
});

describe("model deserialization", () => {
  it("fills known fields, nulls missing ones and collects extras", () => {
    const job = fromDict<Record<string, unknown>>("Job", {
      jobId: 7,
      status: "SUCCEEDED",
      stageIds: [1, 2],
      somethingNew: { a: 1 },
    });
    assert.ok(job);
    assert.equal(job.jobId, 7);
    assert.equal(job.description, null);
    assert.deepEqual(job.stageIds, [1, 2]);
    assert.deepEqual(job.additional_properties, { somethingNew: { a: 1 } });
    // Field order matches the Python model's declaration order.
    assert.equal(Object.keys(job)[0], "jobId");
    assert.equal(Object.keys(job).at(-1), "additional_properties");
  });

  it("recurses into nested models", () => {
    const app = fromDict<Record<string, any>>("Application", {
      id: "app-1",
      attempts: [{ attemptId: "1", completed: true, extra: 5 }],
    });
    assert.equal(app!.attempts[0].attemptId, "1");
    assert.equal(app!.attempts[0].sparkUser, null);
    assert.deepEqual(app!.attempts[0].additional_properties, { extra: 5 });
  });

  it("returns null for null input and an empty array for a non-list", () => {
    assert.equal(fromDict("Job", null), null);
    assert.deepEqual(fromDictList("Job", null), []);
  });

  it("preserves float boxes through a deep copy", () => {
    const copy = modelCopyDeep({ nested: { value: pyFloat(1) } });
    assert.equal(pythonJsonDumps(copy), '{\n  "nested": {\n    "value": 1.0\n  }\n}');
  });
});

describe("python-compatible JSON rendering", () => {
  it("keeps float literals from the wire as floats", () => {
    const parsed = parseJsonPreservingFloats('{"a": 114.0, "b": 114}');
    assert.equal(pythonJsonDumps(parsed), '{\n  "a": 114.0,\n  "b": 114\n}');
  });

  it("renders computed floats the way Python does", () => {
    assert.equal(pythonJsonDumps(pyFloat(1)), "1.0");
    assert.equal(pythonJsonDumps(pyFloat(0.5)), "0.5");
    assert.equal(pythonJsonDumps(2), "2");
  });

  it("escapes non-ASCII characters like ensure_ascii=True", () => {
    assert.equal(pythonJsonDumps("café"), '"caf\\u00e9"');
  });

  it("renders empty containers and nesting with two-space indent", () => {
    assert.equal(pythonJsonDumps({ a: [], b: {} }), '{\n  "a": [],\n  "b": {}\n}');
    assert.equal(pythonJsonDumps([1, [2]]), "[\n  1,\n  [\n    2\n  ]\n]");
  });
});

describe("prompts", () => {
  it("targets a named server when one is given", () => {
    const text = investigateFailure({ app_id: "app-1", server: "prod" });
    assert.match(text, /Target the `prod` server on every call below\./);
    assert.match(text, /list_applications\(app_id="app-1", server="prod"\)/);
  });

  it("explains discovery when no server is given", () => {
    const text = investigateFailure({ app_id: "app-1" });
    assert.match(text, /Omitting `server` makes each tool search every configured server/);
  });

  it("includes free-text context in the comparison prompt", () => {
    const text = compareApplications({
      app_a: "a",
      app_b: "b",
      context: "before and after",
    });
    assert.match(text, /Context: before and after/);
  });
});

describe("configuration", () => {
  let workdir: string;
  let previousCwd: string;
  const savedEnv = { ...process.env };

  before(() => {
    previousCwd = process.cwd();
    workdir = fs.mkdtempSync(path.join(os.tmpdir(), "spark-mcp-config-"));
    process.chdir(workdir);
  });

  after(() => {
    process.chdir(previousCwd);
    fs.rmSync(workdir, { recursive: true, force: true });
    for (const key of Object.keys(process.env)) {
      if (key.startsWith("SHS_")) delete process.env[key];
    }
    Object.assign(process.env, savedEnv);
  });

  function clearEnv(): void {
    for (const key of Object.keys(process.env)) {
      if (key.startsWith("SHS_")) delete process.env[key];
    }
  }

  it("falls back to built-in defaults", async () => {
    clearEnv();
    const { loadConfig } = await import("../src/config/config.js");
    const config = loadConfig();
    assert.deepEqual(Object.keys(config.servers), ["local"]);
    assert.equal(config.servers.local.url, "http://localhost:18080");
    assert.equal(config.servers.local.default, true);
    assert.equal(config.servers.local.timeout, 30);
    assert.equal(config.mcp.port, "18888");
  });

  it("reads the YAML file named by SHS_MCP_CONFIG", async () => {
    clearEnv();
    const file = path.join(workdir, "custom.yaml");
    fs.writeFileSync(
      file,
      "servers:\n  prod:\n    url: https://shs.example.com\n    verify_ssl: false\nmcp:\n  transport: stdio\n",
    );
    process.env.SHS_MCP_CONFIG = file;
    const { loadConfig } = await import("../src/config/config.js");
    const config = loadConfig();
    assert.deepEqual(Object.keys(config.servers), ["prod"]);
    assert.equal(config.servers.prod.verify_ssl, false);
    assert.equal(config.mcp.transport, "stdio");
  });

  it("lets environment variables override the YAML file", async () => {
    clearEnv();
    const file = path.join(workdir, "custom.yaml");
    process.env.SHS_MCP_CONFIG = file;
    process.env.SHS_SERVERS__PROD__URL = "http://override:18080";
    process.env.SHS_SERVERS__PROD__AUTH__TOKEN = "secret";
    process.env.SHS_MCP__PORT = "19999";
    const { loadConfig } = await import("../src/config/config.js");
    const config = loadConfig();
    assert.equal(config.servers.prod.url, "http://override:18080");
    assert.equal(config.servers.prod.auth.token, "secret");
    assert.equal(config.servers.prod.verify_ssl, false); // still from YAML
    assert.equal(config.mcp.port, "19999");
  });

  it("parses JSON-valued environment variables", async () => {
    clearEnv();
    process.env.SHS_MCP__TRANSPORT_SECURITY__ENABLE_DNS_REBINDING_PROTECTION = "true";
    process.env.SHS_MCP__TRANSPORT_SECURITY__ALLOWED_HOSTS = '["localhost:*"]';
    const { loadConfig } = await import("../src/config/config.js");
    const config = loadConfig();
    assert.equal(config.mcp.transport_security?.enable_dns_rebinding_protection, true);
    assert.deepEqual(config.mcp.transport_security?.allowed_hosts, ["localhost:*"]);
  });

  it("detects the deprecated single-underscore delimiter", async () => {
    clearEnv();
    process.env.SHS_MCP_DEBUG = "true";
    const { legacyEnvMode } = await import("../src/config/config.js");
    assert.equal(legacyEnvMode(), true);
    clearEnv();
    process.env.SHS_MCP__DEBUG = "true";
    assert.equal(legacyEnvMode(), false);
  });
});

describe("argument validation", () => {
  const schemaFor = (name: string) =>
    TOOL_CATALOG.find((tool) => tool.name === name)!.inputSchema;

  it("reports a missing required field the way pydantic does", () => {
    assert.throws(
      () => validateToolArguments("list_jobs", schemaFor("list_jobs"), {}),
      (error: Error) =>
        error.message ===
        "1 validation error for list_jobsArguments\n" +
          "app_id\n" +
          "  Field required [type=missing, input_value={}, input_type=dict]\n" +
          "    For further information visit https://errors.pydantic.dev/2.13/v/missing",
    );
  });

  it("rejects an int where a string is expected", () => {
    assert.throws(
      () => validateToolArguments("list_jobs", schemaFor("list_jobs"), { app_id: 12345 }),
      /Input should be a valid string \[type=string_type, input_value=12345, input_type=int\]/,
    );
  });

  it("applies pydantic's coercions", () => {
    const args = validateToolArguments("get_stage", schemaFor("get_stage"), {
      app_id: "a",
      stage_id: "20",        // numeric string -> int
      with_summaries: "false", // string -> bool, and "false" means false
    });
    assert.equal(args.stage_id, 20);
    assert.equal(args.with_summaries, false);
  });

  it("rejects a fractional float for an integer field", () => {
    assert.throws(
      () =>
        validateToolArguments("get_stage", schemaFor("get_stage"), {
          app_id: "a",
          stage_id: 20.5,
        }),
      /got a number with a fractional part \[type=int_from_float/,
    );
  });

  it("reports several errors in schema order", () => {
    assert.throws(
      () =>
        validateToolArguments("get_stage", schemaFor("get_stage"), { stage_id: "x" }),
      (error: Error) => {
        assert.match(error.message, /^2 validation errors for get_stageArguments/);
        assert.ok(error.message.indexOf("app_id") < error.message.indexOf("stage_id"));
        return true;
      },
    );
  });

  it("ignores unknown arguments", () => {
    const args = validateToolArguments("list_jobs", schemaFor("list_jobs"), {
      app_id: "a",
      bogus: 1,
    });
    assert.equal(args.app_id, "a");
  });

  it("rejects an unexpected prompt argument", () => {
    assert.throws(
      () =>
        validatePromptArguments(
          "investigate_failure",
          [
            { name: "app_id", required: true },
            { name: "server", required: false },
          ],
          { app_id: "a", bogus: "y" },
        ),
      /Unexpected keyword argument \[type=unexpected_keyword_argument/,
    );
  });

  it("reports missing prompt arguments as a set", () => {
    assert.throws(
      () =>
        validatePromptArguments(
          "compare_applications",
          [
            { name: "app_a", required: true },
            { name: "app_b", required: true },
          ],
          {},
        ),
      /Missing required arguments: \{'app_a', 'app_b'\}/,
    );
  });
});
