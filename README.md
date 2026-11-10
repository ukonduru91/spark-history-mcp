# Spark History Server MCP (TypeScript)

Give an LLM read access to your Spark History Server so it can do the tedious
part of Spark work: finding why a job failed, and finding where a slow job spends
its time.

It is a TypeScript port of
[kubeflow/mcp-apache-spark-history-server](https://github.com/kubeflow/mcp-apache-spark-history-server),
verified response-for-response against the Python original — see
[PARITY.md](PARITY.md). On top of the port it ships two **agent skills** that
turn the raw tools into an expert workflow for root-cause analysis and
performance tuning.

```
                    ┌──────────────────┐
  data engineer ──▶ │  LLM client      │   Claude Code / Claude Desktop / any MCP client
                    │  + skills        │   ← skills/ supply the method
                    └────────┬─────────┘
                             │ MCP (stdio or streamable-http)
                    ┌────────▼─────────┐
                    │  this server     │   20 tools, 2 prompts
                    └────────┬─────────┘
                             │ HTTP  GET /api/v1/...
                    ┌────────▼─────────┐
                    │ Spark History    │   your existing one, or the bundled demo
                    │ Server           │
                    └────────┬─────────┘
                             │ reads
                    ┌────────▼─────────┐
                    │ event logs       │   s3://…, hdfs://…, file://…
                    └──────────────────┘
```

The server only ever issues **GET** requests to the History Server's REST API. It
cannot modify anything.

---

## Contents

0. [Prerequisites](#0-prerequisites)
1. [Quick start](#1-quick-start)
2. [Pointing it at your Spark History Server](#2-pointing-it-at-your-spark-history-server)
3. [Connecting your LLM client](#3-connecting-your-llm-client)
4. [Installing the skills](#4-installing-the-skills)
5. [The tools](#5-the-tools)
6. [How it works](#6-how-it-works)
7. [Deployment](#7-deployment)
8. [Troubleshooting](#8-troubleshooting)
9. [Development](#9-development)

---

## 0. Prerequisites

Pick a route below and you only need what that route lists — you don't need both.

| Route | You need | Notes |
| --- | --- | --- |
| **Option A — Docker** | [Git](https://git-scm.com/), [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + the Compose plugin on Linux) | Nothing else — Node, TypeScript and all npm dependencies are installed *inside* the image during `docker compose up --build`. Make sure the Docker daemon is actually running before you run the command (on Windows/Mac, start Docker Desktop first). |
| **Option B — from source** | Git, [Node.js 20+](https://nodejs.org/) (22 recommended — npm ships with it, nothing separate to install), and the packages `npm install` pulls from `package.json`: `@modelcontextprotocol/sdk`, `express`, `socks-proxy-agent`, `yaml`, `zod` | You do not install these by hand — `npm install` reads `package.json` and fetches all of them (plus dev dependencies: `typescript`, `tsx`, `@types/*`) into `node_modules/`. |

You do **not** need a Java/Spark install yourself in either route — Option A's Docker image bundles a real Spark distribution to serve the sample event logs; Option B assumes you already have a Spark History Server running somewhere (yours, or the one from Option A) and just points at its URL.

---

## 1. Quick start

### Option A — Docker (nothing to install but Docker)

Starts a Spark History Server loaded with sample event logs *and* this MCP, in
one command — this is the fastest path and needs nothing installed except Git
and Docker (see [Prerequisites](#0-prerequisites)):

```bash
git clone https://github.com/ukonduru91/spark-history-mcp.git
cd spark-history-mcp
docker compose up --build -d      # -d = detached; drop it to watch the logs
```

That single command does three things: builds this MCP server's Docker image
(installing its npm dependencies and compiling TypeScript to `dist/` *inside*
the image), starts the bundled Spark History Server container, and starts this
MCP server container in `streamable-http` mode, already configured (via
`docker-compose.yml`'s env vars) to point at that History Server.

| | |
| --- | --- |
| Spark History Server UI | http://localhost:18080 |
| Spark History REST API | http://localhost:18080/api/v1/applications |
| MCP endpoint | http://localhost:18888/mcp |

Check both containers came up healthy with `docker compose ps`. The bundled
logs include a healthy pipeline and a deliberately failed job, so the tools
have something real to show before you point them at your own cluster.

To run only the History Server (no MCP container):

```bash
./start_local_spark_history.sh          # macOS / Linux / Git Bash
.\start_local_spark_history.ps1         # Windows PowerShell
```

### Option B — from source

Requires Node.js 20+ (22 recommended) — see [Prerequisites](#0-prerequisites).
Use this when you want the MCP server itself running as a local process (e.g.
to register it with an LLM client over **stdio** — see
[§3](#3-connecting-your-llm-client)) rather than as a Docker container, or when
you're developing against it.

```bash
git clone https://github.com/ukonduru91/spark-history-mcp.git
cd spark-history-mcp
npm install                 # installs the packages from package.json into node_modules/
npm run build                # compiles src/ → dist/index.js
```

Before starting it, point it at your Spark History Server — edit
[`config.yaml`](config.yaml) (see [§2](#2-pointing-it-at-your-spark-history-server)
for every option). The file ships pointing at `http://localhost:18080`, which
matches Option A's Docker demo server if you have that running.

Then start it:

```bash
npm start                    # runs `node dist/index.js`, transport from config.yaml/env (default streamable-http, port 18888)
```

or, to have an LLM client (Claude Code, Claude Desktop, ...) launch it itself
over stdio instead of running it standalone, skip `npm start` and register it
as in [§3](#3-connecting-your-llm-client).

### Verify it works

```bash
node scripts/mcp-cli.mjs list-tools
node scripts/mcp-cli.mjs call list_applications '{"limit": 5}'
```

If applications come back, you are connected.

---

## 2. Pointing it at your Spark History Server

**This is the one thing you must configure.** Three ways, highest precedence
first — environment variables win over the `.env` file, which wins over YAML.

### a. Environment variables (best for containers and CI)

Nesting uses a **double** underscore. `LOCAL` below is just a name you choose for
the server:

```bash
export SHS_SERVERS__LOCAL__URL=http://spark-history.internal:18080
export SHS_SERVERS__LOCAL__DEFAULT=true
```

### b. A YAML config file

**This is [`config.yaml`](config.yaml) at the repo root** — it ships pre-filled
with a `local` server pointing at `http://localhost:18080` (the Docker demo
History Server from Option A), set as the default. Edit the `url:` under
`servers:` to point at your own Spark History Server, or add a new named
server block (e.g. `production:`) as shown below and set its `default: true`.

The server looks for a config file in this order:

1. the path given to `--config`, or `$SHS_MCP_CONFIG`
2. `./config.yaml` in the working directory
3. `~/.config/spark-mcp/config.yaml`

```yaml
servers:
  prod:
    url: "https://spark-history.company.com:18080"
    default: true          # used when a tool call omits `server`
    verify_ssl: true
    ssl_ca_cert: "/etc/ssl/custom-ca/ca-bundle.pem"   # private CA
    timeout: 30            # seconds
    auth:
      username: admin
      password: ${SPARK_PASSWORD}   # see the note below
      # token: <bearer token>       # or a bearer token instead

  staging:
    url: "https://spark-history-staging.company.com:18080"
```

> **On secrets**: values in YAML are literal — `${SPARK_PASSWORD}` is *not*
> expanded. Keep credentials in environment variables
> (`SHS_SERVERS__PROD__AUTH__PASSWORD`), which override the file. This matches
> the upstream project's behaviour.

### c. A `.env` file

Same variable names as (a), read from `.env` in the working directory.

### Multiple servers

Configure as many as you like. Tools take an optional `server` argument; when it
is omitted the server **discovers** which configured History Server has that
application and uses it (cached for 5 minutes). An engineer can therefore ask
about an application id without knowing which cluster ran it.

### Every setting

| Setting | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `servers.<n>.url` | `SHS_SERVERS__<N>__URL` | `http://localhost:18080` | History Server base URL |
| `servers.<n>.default` | `SHS_SERVERS__<N>__DEFAULT` | `false` | use when no `server` is given |
| `servers.<n>.auth.username` | `SHS_SERVERS__<N>__AUTH__USERNAME` | — | basic auth |
| `servers.<n>.auth.password` | `SHS_SERVERS__<N>__AUTH__PASSWORD` | — | basic auth |
| `servers.<n>.auth.token` | `SHS_SERVERS__<N>__AUTH__TOKEN` | — | bearer token |
| `servers.<n>.verify_ssl` | `SHS_SERVERS__<N>__VERIFY_SSL` | `true` | TLS verification |
| `servers.<n>.ssl_ca_cert` | `SHS_SERVERS__<N>__SSL_CA_CERT` | — | PEM bundle for a private CA |
| `servers.<n>.timeout` | `SHS_SERVERS__<N>__TIMEOUT` | `30` | request timeout, seconds |
| `servers.<n>.use_proxy` | `SHS_SERVERS__<N>__USE_PROXY` | `false` | route via `socks5h://localhost:8157` |
| `servers.<n>.include_plan_description` | `SHS_SERVERS__<N>__INCLUDE_PLAN_DESCRIPTION` | `false` | default for `get_sql_execution`'s plan text |
| `mcp.transport` | `SHS_MCP__TRANSPORT` | `streamable-http` | `stdio` or `streamable-http` |
| `mcp.address` | `SHS_MCP__ADDRESS` | `localhost` | bind address for HTTP |
| `mcp.port` | `SHS_MCP__PORT` | `18888` | bind port for HTTP |
| `mcp.debug` | `SHS_MCP__DEBUG` | `false` | verbose logging |

Single-underscore variables (`SHS_MCP_PORT`) still work but log a deprecation
warning, exactly as upstream.

### Reaching a History Server you cannot route to

An SSH tunnel plus `use_proxy: true` covers the common locked-down-cluster case:

```bash
ssh -D 8157 -N user@bastion    # SOCKS5 proxy on :8157
```

---

## 3. Connecting your LLM client

### stdio (Claude Code, Claude Desktop, most clients)

```json
{
  "mcpServers": {
    "spark-history": {
      "command": "node",
      "args": ["/absolute/path/to/spark-history-mcp/dist/index.js"],
      "env": {
        "SHS_MCP__TRANSPORT": "stdio",
        "SHS_SERVERS__PROD__URL": "https://spark-history.company.com:18080",
        "SHS_SERVERS__PROD__DEFAULT": "true"
      }
    }
  }
}
```

Claude Code users can do the same in one line:

```bash
claude mcp add spark-history \
  --env SHS_MCP__TRANSPORT=stdio \
  --env SHS_SERVERS__PROD__URL=https://spark-history.company.com:18080 \
  --env SHS_SERVERS__PROD__DEFAULT=true \
  -- node /absolute/path/to/spark-history-mcp/dist/index.js
```

### streamable-http (one shared server for a team)

Run it once, point everyone at it:

```bash
SHS_MCP__TRANSPORT=streamable-http SHS_MCP__ADDRESS=0.0.0.0 npm start
```

Clients connect to `http://<host>:18888/mcp`. The server is read-only, but it is
also unauthenticated — put it behind your normal internal ingress, and enable DNS
rebinding protection if it is reachable from a browser:

```yaml
mcp:
  transport_security:
    enable_dns_rebinding_protection: true
    allowed_hosts: ["spark-mcp.internal:*"]
    allowed_origins: ["https://spark-mcp.internal"]
```

---

## 4. Installing the skills

The tools give the model access to the data. The skills give it the method — the
order to gather evidence in, the thresholds that separate a finding from noise,
and the rule that it must not name a cause it has not seen in the data.

```bash
# per project
mkdir -p .claude/skills
cp -r skills/spark-rca skills/spark-optimization .claude/skills/

# or for every project
mkdir -p ~/.claude/skills
cp -r skills/spark-rca skills/spark-optimization ~/.claude/skills/
```

| Skill | Handles | Triggers on |
| --- | --- | --- |
| `spark-rca` | failed, killed or hung jobs | "why did it fail", a stack trace, an app id, "OOM", "stuck" |
| `spark-optimization` | slow, expensive or regressed jobs | "why is this slow", "tune", "it used to take 20 minutes", "reduce cost" |

They trigger on their own from a normal question — nobody has to remember a
command:

> "the 2am load failed again, app_1724… — can you look?"

See [skills/README.md](skills/README.md) for what is inside each one and how to
extend them with your team's own knowledge.

---

## 5. The tools

The 17 upstream tools live in [`src/tools/tools.ts`](src/tools/tools.ts); their JSON
schemas are in [`src/schemas/generated.ts`](src/schemas/generated.ts). The 3 DataFlint
tools live in [`src/tools/dataflint.ts`](src/tools/dataflint.ts) and
[`src/schemas/dataflint.ts`](src/schemas/dataflint.ts). Run
`node scripts/mcp-cli.mjs list-tools` to see them with their arguments.

### Finding things

| Tool | Returns |
| --- | --- |
| `list_applications` | applications, filterable by status and date, or one by `app_id` |
| `list_jobs` | jobs for an application — failed first by default; `sort_by` `duration` / `failed-tasks` / `id` |
| `list_stages` | stages, same ordering options, optional summary metrics |
| `list_executors` | executors, active by default, `include_inactive` for the full history |
| `list_sql_executions` | curated SQL execution summaries, filterable by description |

### Going deep

| Tool | Returns |
| --- | --- |
| `get_stage` | one stage with per-task metric distributions at your quantiles |
| `list_stage_task_failures` | **the per-task exceptions and stack traces** — where root causes live |
| `get_sql_execution` | one query: header, physical plan, per-node metrics, jobs, stages |
| `get_environment` | runtime versions, Spark/system/Hadoop properties, classpath — filter by `section` |
| `get_executor_summary` | aggregated executor metrics for the application |
| `get_executor_thread_dump` | JVM thread dump — **running applications only** |

### Diagnosing

| Tool | Returns |
| --- | --- |
| `get_job_bottlenecks` | slowest stages and jobs, spill, GC pressure, utilisation, recommendations |
| `get_resource_usage_timeline` | executor add/remove and stage timeline summary |

### Comparing two runs

| Tool | Returns |
| --- | --- |
| `compare_job_environments` | config diff — what changed between two runs |
| `compare_job_performance` | resource and duration diff |
| `compare_sql_executions` | metrics diff for two queries, plus an optional plan-structure diff |
| `compare_stages` | stage metrics and task quantiles side by side |

### DataFlint (extension, not in upstream)

For History Servers with the [DataFlint](https://github.com/dataflint/spark) jar on
the classpath (`SPARK_DAEMON_CLASSPATH=/path/to/spark_2.12-<version>.jar`). DataFlint's
UI computes its summary and alerts in the browser (no endpoint serves them); these
tools recompute them from the same REST data with DataFlint's own formulas, thresholds,
metric renames and alert texts, ported from the UI bundle of `io.dataflint:spark_2.12`
0.9.9. Checked against that UI on Spark 3.2.1: 16 applications (local, real executors
with static and dynamic allocation, YARN-style attempt ids), 607 checks, 0 differences.

| Tool | Returns |
| --- | --- |
| `dataflint_get_app_summary` | DataFlint's summary page: duration, **DCU**, input, output, **memory usage %**, shuffle read/write, spill, **idle cores %**, task error rate, and the SQL table (duration %, DCU %, input, output); plus memory settings and heap details |
| `dataflint_get_alerts` | DataFlint's Alerts page, verbatim (title, location, message, suggestion): idle cores, executor/driver memory, partition skew, small tasks, large partitions, small files read/written, long filters, broadcast-small-table, large broadcast, large cross join. Extra checks (`diskSpill`, `taskFailures`) are marked `source: "custom"` |
| `dataflint_get_sql_plan` | one query's DataFlint plan nodes and metrics (files written, partitions, spill…), plan edges, and its stages |

All three take `app_id`, optional `server` and optional `app_attempt_id`. YARN
applications are served only under `/{appId}/{attemptId}/`, so the latest attempt is
used when none is given.

Not computed: DataFlint's Delta Lake and Iceberg alerts. Without the jar the
Spark-API parts still work and results say `dataflint_available: false`. The parity
harness ignores `dataflint_*` tools.

Notes for YARN clusters:

* The driver-memory alert needs the driver's peak heap, which Spark only logs with
  `spark.eventLog.logStageExecutorMetrics=true`; executor memory usage works without it.
* On Spark 3.2 with DataFlint 0.9.9 the large-cross-join alert cannot fire (the metric
  it reads is not recorded), in the UI or here.
* The History Server answers requests for running applications with a 302 to the
  same URL when its cache is stale; the HTTP client follows redirects (as the Python
  server's urllib3 does), which all tools benefit from.

### Prompts

`investigate_failure(app_id, server?)` and
`compare_applications(app_a, app_b, server?, context?)` — interactive walkthroughs
from the upstream project, for when the engineer wants to drive instead of
handing the analysis over.

---

## 6. How it works

A tool call becomes one or more GETs against `/api/v1/...`, and the JSON comes
back shaped exactly as the Python original shaped it.

```
src/
  index.ts                 CLI entry, transport selection (stdio | streamable-http)
  config/config.ts         YAML + .env + SHS_* resolution and precedence
  core/
    app.ts                 MCP request handlers; maps results to content blocks
    validation.ts          pydantic-compatible argument validation and messages
    json.ts                Python-compatible JSON rendering
    pyfloat.ts             int/float fidelity across the JSON round-trip
    pyrepr.ts              Python repr() for validation messages
    errors.ts              error text shaping
  api/
    httpClient.ts          HTTP transport, ApiException taxonomy, auth, TLS, SOCKS
    sparkClient.ts         Spark REST facade: pagination, attempts, status filters
  models/
    generated.ts           model shapes, generated from the upstream OpenAPI models
    deserialize.ts         from_dict / model_dump equivalents
    mcpTypes.ts            curated LLM-facing output models
  tools/tools.ts           the 17 upstream tools
  tools/dataflint.ts       the 3 DataFlint tools (summary, alerts, SQL plan)
  prompts/prompts.ts       the 2 prompts
  schemas/generated.ts     tool + prompt catalogue (names, descriptions, schemas)
  schemas/dataflint.ts     catalogue entries for the DataFlint tools (hand-written)
```

Three details worth knowing if you plan to modify it:

- **`models/generated.ts` and `schemas/generated.ts` are generated**, by
  `tools/gen_models.py` and `tools/gen_schemas.py`, from the upstream Python
  project. Regenerate rather than hand-edit — that is what keeps the catalogue and
  the response shapes identical to the original.
- **The low-level `Server` API is used, not `McpServer`**, because the result
  shape has to match FastMCP's: one text block per list element, and
  `structuredContent` only for the tools whose Python signature declared a
  concrete return type.
- **Application discovery** lets tools omit `server`. `ApplicationDiscovery`
  probes each configured server for the application id and caches the answer for
  5 minutes.

---

## 7. Deployment

### Docker

```bash
docker build -t spark-history-mcp .
docker run -p 18888:18888 \
  -e SHS_SERVERS__PROD__URL=https://spark-history.company.com:18080 \
  -e SHS_SERVERS__PROD__DEFAULT=true \
  -e SHS_MCP__ADDRESS=0.0.0.0 \
  spark-history-mcp
```

### Kubernetes

Run it as a normal Deployment with the URL in the env and credentials from a
Secret:

```yaml
env:
  - name: SHS_MCP__TRANSPORT
    value: streamable-http
  - name: SHS_MCP__ADDRESS
    value: "0.0.0.0"
  - name: SHS_SERVERS__PROD__URL
    value: http://spark-history-server.spark.svc.cluster.local:18080
  - name: SHS_SERVERS__PROD__DEFAULT
    value: "true"
  - name: SHS_SERVERS__PROD__AUTH__TOKEN
    valueFrom:
      secretKeyRef: { name: spark-history-auth, key: token }
```

The process is stateless apart from the 5-minute discovery cache, so it scales
horizontally without coordination.

---

## 8. Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `connect ECONNREFUSED` | wrong URL or port, or the History Server is down. Check `curl $URL/api/v1/applications` from the same host |
| `Application '<id>' not found on any server` | the id is not on any configured server, or the event log has not been picked up yet — `spark.history.fs.update.interval` controls the scan |
| `No Spark server named 'x' is configured` | the `server` argument does not match a key under `servers:` |
| `404 … No tasks reported metrics for N / 0 yet` | Spark's own answer for a stage that failed before any task finished. Not a tool problem — read the task exceptions instead |
| `get_executor_thread_dump` errors on a finished app | expected: the History Server does not persist thread dumps. They work only while the app is running |
| Empty `list_applications` | check `spark.history.fs.logDirectory` points where your jobs actually write event logs, and that `spark.eventLog.enabled=true` on the jobs |
| Very large responses | narrow with `length`, `limit` and `section`. `get_stage(with_summaries=false)` is much smaller |
| `emr_cluster_arn … not included in this TypeScript port` | EMR persistent-UI auth is not ported; point at a directly reachable URL instead |

Set `SHS_MCP__DEBUG=true` for verbose logs.

---

## 9. Development

```bash
npm install
npm run build        # compile to dist/
npm run dev          # run from source, no build step
npm test             # unit tests
npm run typecheck    # tsc --noEmit
```

Cross-implementation parity testing lives in [`parity/`](parity/) — it runs the
same MCP calls against this server and the Python original and diffs every
response. [PARITY.md](PARITY.md) records the results and the exact differences
that remain.

### Not ported from upstream

| Upstream module | Status |
| --- | --- |
| `api/emr_persistent_ui_client.py` | not ported — a server configured with `emr_cluster_arn` fails fast with an explanatory error |
| `tools/aws_troubleshooting.py` | not ported — proxies to an AWS-hosted MCP endpoint, registered only when AWS credentials are present |
| `api/spark_html_client.py` | not ported — a Playwright screenshot helper no tool calls |

---

## License

Apache-2.0, as with the upstream project.
