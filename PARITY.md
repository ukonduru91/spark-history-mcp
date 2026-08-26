# Parity report — Python original vs TypeScript port

"Functionality is unchanged" is only worth as much as the evidence behind it, so
the port was verified by running **both servers against the same live Spark
History Server** and diffing every response.

## Result

| Comparison | Result |
| --- | --- |
| Tool catalogue — 17 tools, names, descriptions, input schemas, ~52 KB of output JSON Schema | **identical** |
| Prompt catalogue — 2 prompts, descriptions, argument lists | **identical** |
| Tool results — 135 calls across three suites | **135/135 identical**; 125 byte-for-byte, the other 10 identical once the live HTTP `Date` header is masked |
| Prompt renders — 13 cases including error paths | **12/13 identical** (the 13th is Python set-ordering, below) |
| Total comparisons | **154 items, 0 differences** |

## What was compared

Three suites, run against both servers with the same client and the same call
sequence:

| Suite | Cases | Covers |
| --- | --- | --- |
| `parity/cases.json` | 69 tools + 4 prompts | every tool several times, reaching its internal branches: each `sort_by`, each `section`, pagination, single-item lookup, status filters, the `include_*` flags on `get_sql_execution`, and 10 error paths |
| `parity/edge_cases.json` | 42 tools + 4 prompts | boundaries: empty filters, zero and negative lengths, offsets past the end, unknown ids, unknown servers, mixed-case section names, missing stages and executions, filter combinations |
| `parity/validation_cases.json` | 26 tools + 5 prompts | malformed arguments: missing required fields, every wrong-type combination, coercions pydantic accepts (`"20"`→int, `"yes"`→bool, `20.0`→int) and rejects (`20.5`→int, int→str), unknown arguments, multi-error ordering |

For every call the comparison covers `isError`, the number of content blocks, the
text of each block, and `structuredContent`.

The differ compares parsed JSON rather than raw text, so `114.0` and `114` would
count as equal — but in practice they are byte-equal too (see below). It was
itself validated by mutating a copy of the TypeScript capture (one changed field,
one changed prompt character, one changed tool description) and confirming it
reported exactly those three regressions.

## Test environment

| Component | Detail |
| --- | --- |
| Spark | 3.5.5, History Server on port 18080 |
| Event logs | 3 sample applications from upstream + 2 generated for this test |
| Python server | upstream `main`, `mcp[cli]~=1.26`, pydantic 2.13, over stdio |
| TypeScript server | this project, `@modelcontextprotocol/sdk` 1.30, over stdio |

Two Spark jobs were submitted to produce realistic event logs
(`examples/jobs/`):

| Application | What it exercises |
| --- | --- |
| `SalesAnalyticsPipeline` | 8 SQL executions, 22 jobs, 52 stages — shuffle join, window function, aggregation, Parquet writes |
| `FailingSalesJob` | a UDF that raises on specific rows: 1 failed job, stage and task with a real Python stack trace |

The failing job matters: it is the only way to exercise
`list_stage_task_failures` and the failure-ordering branches against real data.

## The differences that remain

Four, and they are all named here rather than buried.

### 1. Three upstream modules are not ported

| Upstream module | Status |
| --- | --- |
| `api/emr_persistent_ui_client.py` | not ported. A server configured with `emr_cluster_arn` fails fast with an explanatory error rather than silently querying an unauthenticated endpoint |
| `tools/aws_troubleshooting.py` | not ported. Registers `aws_analyze_spark_workload` and `aws_spark_code_recommendation` only when AWS credentials are detected; both proxy to an AWS-hosted MCP endpoint |
| `api/spark_html_client.py` | not ported. A Playwright screenshot helper that no tool calls |

This is the only difference that changes what the server can do. Everything else
below is cosmetic.

### 2. The HTTP `Date` header inside error messages

The generated Python client embeds the raw response headers in its exception
text:

```
(404)
Reason: Not Found
HTTP response headers: HTTPHeaderDict({'Date': 'Tue, 25 Aug 2026 13:57:59 GMT', …})
```

`Date` is the moment the Spark History Server answered, so it cannot match across
two runs. Masking that one field makes all 135 tool results byte-identical.

### 3. Python set ordering in one prompt error

When a prompt is called with two required arguments missing, FastMCP renders a
Python set:

```
Missing required arguments: {'app_b', 'app_a'}
```

The order comes from Python's hash randomisation and is not stable across Python
runs either. This port emits them sorted (`{'app_a', 'app_b'}`) — same content,
deterministic order.

### 4. Transport-error wording for an unreachable server

Node's socket errors are reshaped into the wording `requests`/`urllib3` produce,
so the common cases (connection refused, DNS failure, reset, timeout) match
exactly. An unusual network failure could word itself differently.

## Details that had to be reproduced deliberately

Byte-level equality required matching behaviours that are implicit in the Python
stack rather than stated anywhere:

- **Field order, nulls and `additional_properties`.** FastMCP serializes the
  OpenAPI-generated pydantic models with `model_dump(by_alias=True)`: every known
  field present (`null` when absent), in declaration order, unknown keys collected
  under `additional_properties`. `src/models/deserialize.ts` reproduces this from
  a schema table generated out of the upstream models.
- **One content block per list element.** A tool returning 22 jobs produces 22
  text blocks, not one JSON array.
- **`structuredContent` only where the Python signature is annotated**, wrapped in
  `{"result": …}` for `list[...]` and `Dict[str, Any]` returns but not for
  pydantic-model returns. Encoded per tool in `src/core/app.ts`.
- **`int` vs `float` rendering.** Python prints `114.0` where JavaScript prints
  `114`. Float-ness is preserved from the wire using `JSON.parse` source-text
  access, and propagated through the arithmetic the way Python's `/` and `sum()`
  do — including that `_duration_seconds` returns the *int* `0` on its
  early-return path and a float otherwise (`src/core/pyfloat.ts`).
- **`ensure_ascii=True`.** Python escapes non-ASCII as `\uXXXX`.
- **pydantic validation messages**, including the coercion rules, the per-field
  error ordering, `repr()` of the offending value, and the `errors.pydantic.dev`
  documentation URL (`src/core/validation.ts`, `src/core/pyrepr.ts`).
- **Exception text** from the generated client's `ApiException.__str__` — status,
  reason, header dict (in the server's original header casing), body.

### Bugs this testing found

The validation suite was written after the first "all green" run, and it found
three real defects that are now fixed:

1. Malformed arguments produced this port's own error text rather than pydantic's
   `1 validation error for list_jobsArguments…`.
2. `with_summaries: "false"` was read as **true** — JavaScript's
   `Boolean("false")` is `true`. A genuine behavioural bug, not a cosmetic one.
3. An unexpected prompt argument was ignored instead of raising.

That is the argument for testing error paths and not just happy paths.

## Reproducing

With a Spark History Server on `localhost:18080` and the upstream Python project
checked out:

```bash
# 1. Reference outputs from the Python server
PYTHONPATH=<upstream>/src python3 parity/run_cases.py out/py \
    python3 -m spark_history_mcp.core.main --config config.yaml

# 2. The same calls against this server
npm run build
python3 parity/run_cases.py out/ts node dist/index.js --config config.yaml

# 3. Diff
python3 parity/diff_results.py out/py out/ts
```

`CASES_FILE=edge_cases.json` (or `validation_cases.json`) selects the other
suites. Update the application ids in the `vars` block of each case file to match
your own event logs.

The captures and diff reports from the recorded run are in `parity/results/`;
`full-captures.tar.gz` holds every response from every suite for both servers.
