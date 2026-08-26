#!/usr/bin/env python3
"""Run every case in cases.json against an MCP server over stdio and dump results.

Usage:
    run_cases.py <out_dir> <command> [args...]

The case file defaults to cases.json; set CASES_FILE to run another suite:
    CASES_FILE=validation_cases.json run_cases.py out/ts node ../dist/index.js

Writes:
    <out_dir>/_tools.json       tool catalogue (name, description, schemas)
    <out_dir>/_prompts.json     prompt catalogue
    <out_dir>/tool__<case>.json result of each tool call
    <out_dir>/prompt__<case>.json result of each prompt render
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

HERE = Path(__file__).parent
CASES = json.loads((HERE / (os.environ.get("CASES_FILE") or "cases.json")).read_text())


def substitute(value, variables):
    if isinstance(value, str) and value.startswith("$"):
        return variables.get(value[1:], value)
    if isinstance(value, dict):
        return {k: substitute(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute(v, variables) for v in value]
    return value


def dump(obj):
    """Normalise an MCP result object into plain JSON."""
    return json.loads(json.dumps(obj, default=lambda o: getattr(o, "model_dump", lambda **_: str(o))(mode="json")))


async def main() -> None:
    out_dir = Path(sys.argv[1])
    command, *args = sys.argv[2:]
    out_dir.mkdir(parents=True, exist_ok=True)

    variables = CASES["vars"]
    env = dict(os.environ)

    server = StdioServerParameters(command=command, args=args, env=env)

    async with stdio_client(server) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            (out_dir / "_tools.json").write_text(
                json.dumps(
                    [
                        {
                            "name": t.name,
                            "title": t.title,
                            "description": t.description,
                            "inputSchema": t.inputSchema,
                            "outputSchema": t.outputSchema,
                        }
                        for t in sorted(tools.tools, key=lambda t: t.name)
                    ],
                    indent=2,
                    sort_keys=True,
                )
            )

            prompts = await session.list_prompts()
            (out_dir / "_prompts.json").write_text(
                json.dumps(
                    [
                        {
                            "name": p.name,
                            "title": p.title,
                            "description": p.description,
                            "arguments": [
                                {
                                    "name": a.name,
                                    "description": a.description,
                                    "required": a.required,
                                }
                                for a in (p.arguments or [])
                            ],
                        }
                        for p in sorted(prompts.prompts, key=lambda p: p.name)
                    ],
                    indent=2,
                    sort_keys=True,
                )
            )

            for case_name, tool_name, raw_args in CASES["tools"]:
                call_args = substitute(raw_args, variables)
                record = {"case": case_name, "tool": tool_name, "arguments": call_args}
                try:
                    result = await session.call_tool(tool_name, call_args)
                    record["isError"] = bool(result.isError)
                    record["content"] = [dump(c) for c in result.content]
                    record["structuredContent"] = result.structuredContent
                except Exception as exc:  # noqa: BLE001
                    record["exception"] = f"{type(exc).__name__}: {exc}"
                (out_dir / f"tool__{case_name}.json").write_text(
                    json.dumps(record, indent=2, sort_keys=True)
                )
                print(f"  tool {case_name}: {'ERROR' if record.get('isError') else 'ok'}")

            for case_name, prompt_name, raw_args in CASES["prompts"]:
                call_args = substitute(raw_args, variables)
                record = {"case": case_name, "prompt": prompt_name, "arguments": call_args}
                try:
                    result = await session.get_prompt(prompt_name, call_args)
                    record["description"] = result.description
                    record["messages"] = [dump(m) for m in result.messages]
                except Exception as exc:  # noqa: BLE001
                    record["exception"] = f"{type(exc).__name__}: {exc}"
                (out_dir / f"prompt__{case_name}.json").write_text(
                    json.dumps(record, indent=2, sort_keys=True)
                )
                print(f"  prompt {case_name}: ok")


if __name__ == "__main__":
    asyncio.run(main())
