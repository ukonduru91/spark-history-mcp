# Parity harness

Runs an identical sequence of MCP calls against two servers and diffs the
results. Used to verify that this TypeScript port behaves like the upstream
Python server. See [../PARITY.md](../PARITY.md) for the recorded results.

## Files

| File | Purpose |
| --- | --- |
| `cases.json` | The main matrix: 69 tool calls across all 17 tools plus 4 prompt renders. Application ids live in the `vars` block. |
| `edge_cases.json` | Boundary conditions: empty filters, zero/negative lengths, unknown ids and servers, missing stages, filter combinations. |
| `validation_cases.json` | Malformed arguments: wrong types, missing required fields, pydantic's coercion rules, multi-error ordering. |
| `run_cases.py` | Launches an MCP server over stdio, runs every case, writes one JSON file per case plus the tool and prompt catalogues. |
| `diff_results.py` | Compares two capture directories and prints the differences. |
| `check_http.py` | Smoke-tests a server over the streamable-http transport. |
| `results/` | The captures and diff report from the recorded run. |

## Requirements

```bash
pip install "mcp[cli]~=1.26"
```

## Usage

```bash
# Reference capture from the Python server
PYTHONPATH=<upstream>/src python3 run_cases.py out/py \
    python3 -m spark_history_mcp.core.main --config ../config.yaml

# Capture from this server
python3 run_cases.py out/ts node ../dist/index.js --config ../config.yaml

# Compare
python3 diff_results.py out/py out/ts --json report.json
```

`run_cases.py` reads `cases.json` by default; set `CASES_FILE` to run another
suite:

```bash
CASES_FILE=validation_cases.json python3 run_cases.py out/ts node ../dist/index.js
```

`diff_results.py` compares parsed JSON rather than raw text, so `114.0` and
`114` count as equal, and it masks the `Date` HTTP header that appears inside
error messages from the Python client. Everything else — content block counts,
field values, `structuredContent`, `isError`, catalogue entries — must match
exactly.

## Adapting to your own Spark History Server

Edit the `vars` block in `cases.json`:

```json
"vars": {
  "APP_OK":   "an application with SQL executions",
  "APP_FAIL": "an application with a failed stage",
  "TAXI_A":   "any other application",
  "TAXI_B":   "any other application",
  "PI":       "an application with no SQL executions"
}
```

Stage and execution ids referenced in individual cases may need updating too.
