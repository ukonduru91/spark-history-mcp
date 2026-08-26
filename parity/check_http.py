#!/usr/bin/env python3
"""Smoke-test a running MCP server over the streamable-http transport."""

import asyncio
import sys

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


async def main() -> None:
    url = sys.argv[1]
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            print("server:", init.serverInfo.name, init.serverInfo.version)
            tools = await session.list_tools()
            print("tools:", len(tools.tools))
            prompts = await session.list_prompts()
            print("prompts:", len(prompts.prompts))
            result = await session.call_tool("list_applications", {"limit": 2})
            print("list_applications content blocks:", len(result.content))
            for block in result.content:
                first_line = block.text.splitlines()[1]
                print("   ", first_line.strip())
            failures = await session.call_tool(
                "list_stage_task_failures",
                {"app_id": "local-1787666126593", "stage_id": 0},
            )
            print("failed tasks:", len(failures.content))
            print(
                "   error head:",
                failures.content[0].text.splitlines()[6].strip()[:100],
            )
            prompt = await session.get_prompt(
                "investigate_failure", {"app_id": "local-1787666126593"}
            )
            print("prompt chars:", len(prompt.messages[0].content.text))


asyncio.run(main())
