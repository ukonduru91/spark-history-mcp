/**
 * Catalogue entries for the DataFlint tools.
 *
 * Hand-written (unlike generated.ts): these tools are an extension of this
 * port and have no upstream Python counterpart. The schema shape follows the
 * generated entries so the same validator handles them.
 */

import type { ToolCatalogEntry } from "./generated.js";

const appId = { title: "App Id", type: "string" };
const server = {
  anyOf: [{ type: "string" }, { type: "null" }],
  default: null,
  title: "Server",
};
const appAttemptId = {
  anyOf: [{ type: "string" }, { type: "null" }],
  default: null,
  title: "App Attempt Id",
};

const REQUIRES =
  "Requires the DataFlint jar (io.dataflint:spark_2.12) on the Spark History " +
  "Server classpath; without it the Spark-API parts still work and the result " +
  "says dataflint_available: false.";

export const DATAFLINT_TOOL_CATALOG: ToolCatalogEntry[] = [
  {
    name: "dataflint_get_app_summary",
    description:
      "\n    DataFlint summary page for a Spark application, computed with DataFlint's own formulas.\n\n" +
      "    Returns duration, DCU (DataFlint compute units: 0.05 x core-hours + 0.005 x memory GiB-hours),\n" +
      "    idle cores % (share of allocated core time with no task running; over 50% means over-provisioned),\n" +
      "    task error rate, input/output/shuffle/spill volumes, and SQL queries ranked by duration.\n\n" +
      `    ${REQUIRES}\n\n` +
      "    Args:\n        app_id: The Spark application ID\n" +
      "        server: Optional server name to use (uses default if not specified)\n" +
      "        app_attempt_id: Optional YARN application attempt ID (latest if omitted)\n    ",
    inputSchema: {
      properties: { app_id: appId, server, app_attempt_id: appAttemptId },
      required: ["app_id"],
      title: "dataflint_get_app_summaryArguments",
      type: "object",
    },
    outputSchema: null,
  },
  {
    name: "dataflint_get_alerts",
    description:
      "\n    DataFlint performance alerts for a Spark application, as the DataFlint UI's Alerts page shows them:\n" +
      "    same rules, thresholds, titles, locations, messages and suggestions (verbatim). Covers\n" +
      "    idleCoresTooHigh, executorMemoryTooHigh, executorMemoryTooLow, driverMemoryTooHigh, partitionSkew,\n" +
      "    smallTasks, maxPartitionToBig, readSmallFiles, writeSmallUnpartitionedFiles, writeSmallFilesPartitioned,\n" +
      "    longFilterCondition, broadcastSmallTable, largeBroadcast, largeCrossJoinScan (not Delta/Iceberg alerts).\n" +
      "    Extra checks DataFlint does not have (diskSpill, taskFailures) are marked source: \"custom\".\n" +
      "    Each alert has type (warning/error), location (SQL query id / stage / node), message, suggestion\n" +
      "    and evidence (the numbers behind it).\n\n" +
      `    ${REQUIRES}\n\n` +
      "    Args:\n        app_id: The Spark application ID\n" +
      "        server: Optional server name to use (uses default if not specified)\n" +
      "        app_attempt_id: Optional YARN application attempt ID (latest if omitted)\n    ",
    inputSchema: {
      properties: { app_id: appId, server, app_attempt_id: appAttemptId },
      required: ["app_id"],
      title: "dataflint_get_alertsArguments",
      type: "object",
    },
    outputSchema: null,
  },
  {
    name: "dataflint_get_sql_plan",
    description:
      "\n    DataFlint's enriched plan for one SQL query: each plan node with its metrics (rows, bytes,\n" +
      "    files read/written, partitions written, spill, time), the plan edges (child -> parent), and the\n" +
      "    query's stages with task duration median/max, shuffle and spill. Use after dataflint_get_alerts\n" +
      "    to see the plan behind an alert.\n\n" +
      `    ${REQUIRES}\n\n` +
      "    Args:\n        app_id: The Spark application ID\n" +
      "        execution_id: The SQL execution ID (from dataflint_get_app_summary or list_sql_executions)\n" +
      "        server: Optional server name to use (uses default if not specified)\n" +
      "        app_attempt_id: Optional YARN application attempt ID (latest if omitted)\n    ",
    inputSchema: {
      properties: {
        app_id: appId,
        execution_id: { title: "Execution Id", type: "integer" },
        server,
        app_attempt_id: appAttemptId,
      },
      required: ["app_id", "execution_id"],
      title: "dataflint_get_sql_planArguments",
      type: "object",
    },
    outputSchema: null,
  },
];
