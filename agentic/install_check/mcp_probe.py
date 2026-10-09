"""Two probes for the install check, each printing one JSON line.

    python mcp_probe.py direct  <command...>   # spawn the server with the documented command; list tools, read the
                                                # guidance resource, call list_facilities (the anonymous registry)
    python mcp_probe.py codex   <server-name>  # ask a running `codex app-server` which tools that MCP server gave it
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time


async def direct(cmd: list[str]) -> dict:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    out: dict = {"probe": "direct"}
    t0 = time.monotonic()
    params = StdioServerParameters(command=cmd[0], args=cmd[1:])
    async with stdio_client(params) as (r, w), ClientSession(r, w) as s:
        init = await s.initialize()
        out["startup_s"] = round(time.monotonic() - t0, 1)
        out["server"] = init.serverInfo.name
        out["instructions"] = bool(init.instructions)
        tools = (await s.list_tools()).tools
        out["tools"] = sorted(t.name for t in tools)
        res = await s.read_resource("hpcbridge://guidance/operations")
        text = res.contents[0].text if res.contents else ""
        out["guidance_chars"] = len(text)
        out["guidance_is_skill"] = text.startswith("---\nname: driving-hpc")
        fac = await s.call_tool("list_facilities", {})
        payload = fac.structuredContent or {}
        items = payload.get("result", payload) if isinstance(payload, dict) else payload
        out["facilities"] = sorted(e.get("id") for e in items) if isinstance(items, list) else items
    return out


def codex(server: str) -> dict:
    p = subprocess.Popen(["codex", "app-server"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, text=True, bufsize=1)

    def send(msg: dict) -> None:
        assert p.stdin is not None
        p.stdin.write(json.dumps(msg) + "\n")
        p.stdin.flush()

    def wait_for(rid: int, timeout: float = 240) -> dict:
        assert p.stdout is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = p.stdout.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            if msg.get("id") == rid:
                return msg
        return {"error": "no reply"}

    try:
        send({"id": 0, "method": "initialize", "params": {"clientInfo": {"name": "install-check", "version": "1"}}})
        wait_for(0, 30)
        send({"method": "initialized"})
        out: dict = {"probe": "codex"}
        for attempt in range(12):  # the server starts asynchronously; poll until it reports tools or a failure
            send({"id": 1 + attempt, "method": "mcpServerStatus/list", "params": {}})
            reply = wait_for(1 + attempt, 60)
            data = ((reply.get("result") or {}).get("data")) or []
            st = next((d for d in data if d.get("name") == server), None)
            if st and (st.get("tools") or st.get("runtimeStatus") not in (None, "starting", "Starting")):
                out["status"] = st.get("runtimeStatus")
                out["tools"] = sorted((st.get("tools") or {}).keys())
                out["error"] = st.get("error") or st.get("toolsError")
                return out
            out["last"] = reply.get("error") or (st and st.get("runtimeStatus"))
            time.sleep(10)
        return out
    finally:
        p.kill()


def main() -> int:
    kind = sys.argv[1]
    result = asyncio.run(direct(sys.argv[2:])) if kind == "direct" else codex(sys.argv[2])
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
