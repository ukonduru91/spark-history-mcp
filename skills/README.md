# Skills

Two agent skills that turn the raw MCP tools into an expert workflow. The tools
alone give a model access to the data; these give it the method — the order to
gather evidence in, the thresholds that separate a real finding from noise, and
the discipline to name a cause only when the data supports it.

| Skill | Use it for | Triggers on |
| --- | --- | --- |
| [`spark-rca`](spark-rca/) | a job that failed, was killed, or is hung | "why did my job fail", a stack trace, an app id, "it crashed", "OOM", "stuck" |
| [`spark-optimization`](spark-optimization/) | a job that is slow or expensive, or has regressed | "why is this slow", "make it faster", "tune", "it used to take 20 minutes", "reduce cost" |

Both are written against the 17 tools this MCP server exposes, and both assume
the model can call those tools directly.

## What is in each skill

```
spark-rca/
├── SKILL.md                              the workflow and reporting format
└── references/
    ├── failure-signatures.md             exception → cause → fix, by exception class
    └── metric-interpretation.md          what each metric means and when to react

spark-optimization/
├── SKILL.md                              the workflow and reporting format
└── references/
    ├── tuning-playbook.md                the fix for each diagnosis, with trade-offs
    └── plan-reading.md                   physical operators, node metrics, join strategy
```

The reference files are loaded on demand — the model reads the section that
matches what it found, not the whole catalogue.

## Installing

**Claude Code** — copy into either location:

```bash
# available in one project
mkdir -p .claude/skills && cp -r skills/spark-rca skills/spark-optimization .claude/skills/

# available everywhere
mkdir -p ~/.claude/skills && cp -r skills/spark-rca skills/spark-optimization ~/.claude/skills/
```

**Claude Desktop / claude.ai** — zip each skill folder and upload it in the
skills settings, or share the `.skill` bundle with your team.

**Any other MCP client with skill support** — the format is a folder containing
`SKILL.md` with YAML frontmatter (`name`, `description`). Follow that client's
installation path.

Once installed, they trigger on their own from a normal question. Nobody has to
remember a command:

> "the 2am load failed again, app_1724... — can you look?"

## Using them without an LLM client

The workflows are readable on their own. `scripts/mcp-cli.mjs` runs the same
tools from a shell, so an engineer can walk the RCA steps by hand:

```bash
node scripts/mcp-cli.mjs call list_jobs '{"app_id":"<id>","status":["FAILED"]}'
node scripts/mcp-cli.mjs call list_stage_task_failures '{"app_id":"<id>","stage_id":7}'
```

## Relationship to the MCP prompts

The server also ships two prompts, `investigate_failure` and
`compare_applications`, which are part of the upstream project. They cover
similar ground interactively — the engineer drives, step by step. The skills are
for the other mode: the model does the analysis and hands back a report. Both
work; they suit different moments.

## Extending them

The reference files are the place to put what your team learns. A failure
signature specific to your platform, a threshold that differs for your data, the
config your cluster actually needs — adding those to
`references/failure-signatures.md` or `references/tuning-playbook.md` makes every
future investigation better, and costs nothing at runtime because the files load
only when relevant.

Keep `SKILL.md` itself short. It is loaded in full whenever the skill triggers;
the references are not.
