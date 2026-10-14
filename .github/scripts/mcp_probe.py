"""Start hpc-bridge's MCP server with the given command and ask it for its tools: does the server itself run here?

Usage: python mcp_probe.py <command> [args...]   (e.g. `uv run --locked --extra integration hpc-bridge`)
Exits 0 when the server answers `initialize` and lists at least one tool."""
import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def probe(command: str, args: list[str]) -> int:
    params = StdioServerParameters(command=command, args=args,
                                   env={**os.environ, "HPC_BRIDGE_OMIT_INSTRUCTIONS": "1"})  # as the plugin starts it
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        init = await session.initialize()
        tools = await session.list_tools()
    names = sorted(t.name for t in tools.tools)
    print(f"server {init.serverInfo.name} {init.serverInfo.version}: {len(names)} tools: {', '.join(names)}")
    return 0 if names else 1


if __name__ == "__main__":
    command, *rest = sys.argv[1:]
    sys.exit(asyncio.run(asyncio.wait_for(probe(command, rest), 300)))
