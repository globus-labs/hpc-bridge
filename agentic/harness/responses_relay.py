"""A local relay that lets Codex talk to an OpenAI-compatible endpoint that serves the Responses API only partly — the
ALCF inference gateway (vLLM), which answers it NON-streamed and knows only plain `function` tools:

- "Streaming is not supported for the 'responses' API on this endpoint" — Codex always streams. The relay forwards
  with `"stream": false` and replays the complete response as the server-sent events Codex's parser consumes
  (codex-rs/codex-api/src/sse/responses.rs): `response.created`, one `response.output_item.done` per output item,
  then `response.completed` (or `response.incomplete`).
- An assistant message in the history is refused unless it carries an id, a status and annotations, which Codex
  doesn't send back: it is resent in the plain `{role, content}` form.
- "tool type namespace not supported" — Codex 0.16x groups tools into `namespace` tools (MCP servers, its own
  `functions`) and offers freeform `custom` tools (apply_patch). The relay flattens every one into a plain function
  tool (a custom tool takes one string, `input`), translates the conversation history the same way, and restores
  Codex's shape on the way back: a call to a flattened name returns as `{name, namespace}`, a call to a custom tool as
  a `custom_tool_call`. Tool types vLLM cannot run at all (web_search, tool_search) are dropped.

So the model sees the names Codex itself would show it (`mcp__hpc_bridge__list_facilities`), and Codex's own MCP
dispatch, approvals and tool handling run unchanged — only the wire format is adapted.

The relay is also the run's most complete RECORD of the agent (its log, $HPCB_RELAY_LOG): every call the model asked
for — including the ones `codex exec --json` never reports (typing into a running command with `write_stdin`, a
sub-agent's calls, a patch's content) — and every output Codex sent back, once each. cli_runner grades from it.

    relay = start_relay("https://inference-api.alcf.anl.gov/resource_server/sophia/vllm/v1")
    base_url = relay.base_url   # http://127.0.0.1:<port>/v1
    relay.shutdown()
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

_DEFAULT_NS = "functions"  # codex_protocol::DEFAULT_FUNCTION_NAMESPACE — its own tools, unprefixed
_DROP_KEYS = ("client_metadata", "include", "prompt_cache_key")  # OpenAI-only request fields
_UNSUPPORTED_ITEMS = ("tool_search_call", "tool_search_output", "web_search_call")


def _flat(ns: str | None, name: str) -> str:
    if not ns or ns == _DEFAULT_NS:
        return name
    return ns + name if ns.endswith("_") else f"{ns}__{name}"


def _as_function(tool: dict, flat: str) -> dict:
    if tool.get("type") == "custom":
        fmt = tool.get("format") or {}
        hint = f" The input is free text in {fmt.get('syntax', 'its')} syntax." if fmt else ""
        return {"type": "function", "name": flat, "description": (tool.get("description") or "") + hint,
                "parameters": {"type": "object", "properties": {"input": {"type": "string"}}, "required": ["input"]}}
    return {"type": "function", "name": flat, "description": tool.get("description") or "",
            "parameters": tool.get("parameters") or {"type": "object", "properties": {}},
            **({"strict": tool["strict"]} if "strict" in tool else {})}


def adapt_request(req: dict) -> tuple[dict, dict[str, tuple[str | None, str, str]]]:
    """Codex's request → one vLLM accepts, plus the map {flat name: (namespace, name, "function"|"custom")} that
    `adapt_response` needs to restore Codex's shape."""
    out = {k: v for k, v in req.items() if k not in _DROP_KEYS}
    names: dict[str, tuple[str | None, str, str]] = {}
    tools: list[dict] = []
    for t in req.get("tools") or []:
        kind = t.get("type")
        inner = [(t.get("name"), x) for x in t.get("tools") or []] if kind == "namespace" else [(None, t)]
        for ns, tool in inner:
            if tool.get("type") not in ("function", "custom") or not tool.get("name"):
                continue  # web_search / tool_search / local_shell: nothing vLLM can run
            flat = _flat(ns, tool["name"])
            names[flat] = (ns, tool["name"], tool["type"])
            tools.append(_as_function(tool, flat))
    if "tools" in req:
        out["tools"] = tools
    items = req.get("input")
    if isinstance(items, list):
        conv: list[Any] = []
        for it in items:
            kind = it.get("type") if isinstance(it, dict) else None
            if kind in _UNSUPPORTED_ITEMS:
                continue
            if kind == "function_call":
                it = {k: v for k, v in it.items() if k != "namespace"} | {"name": _flat(it.get("namespace"), it["name"])}
            elif kind == "custom_tool_call":
                it = {"type": "function_call", "call_id": it["call_id"],
                      "name": _flat(it.get("namespace"), it["name"]), "arguments": json.dumps({"input": it["input"]})}
            elif kind in ("function_call_output", "custom_tool_call_output"):
                it = {"type": "function_call_output", "call_id": it.get("call_id"), "output": it.get("output")}
            elif kind == "message" and it.get("role") == "assistant":
                # vLLM validates an assistant message as OpenAI's output message (id, status, annotations required)
                # and Codex replays it without them; the plain {role, content} form carries the same text
                it = {"role": "assistant", "content": _output_text(it.get("content"))}
            conv.append(it)
        out["input"] = conv
    return out, names


def adapt_response(resp: dict, names: dict[str, tuple[str | None, str, str]]) -> dict:
    """vLLM's response → Codex's item shapes: a flattened call returns under its namespace, a custom tool's call as a
    `custom_tool_call` carrying the raw input."""
    output = []
    for it in resp.get("output") or []:
        if isinstance(it, dict) and it.get("type") == "function_call" and it.get("name") in names:
            ns, name, kind = names[it["name"]]
            if kind == "custom":
                try:
                    text = json.loads(it.get("arguments") or "{}").get("input", "")
                except (ValueError, AttributeError):
                    text = it.get("arguments") or ""
                it = {"type": "custom_tool_call", "call_id": it.get("call_id"), "name": name, "input": text,
                      **({"namespace": ns} if ns and ns != _DEFAULT_NS else {}), "status": "completed"}
            else:
                it = {**it, "name": name, **({"namespace": ns} if ns and ns != _DEFAULT_NS else {})}
        output.append(it)
    return {**resp, "output": output}


def _sse(kind: str, payload: dict) -> bytes:
    return f"event: {kind}\ndata: {json.dumps({'type': kind, **payload})}\n\n".encode()


def replay_as_events(resp: dict) -> bytes:
    """A complete Responses API object → the event stream Codex reads."""
    out = [_sse("response.created", {"response": {"id": resp.get("id")}})]
    for item in resp.get("output") or []:
        out.append(_sse("response.output_item.done", {"item": item}))
    completed = {k: resp.get(k) for k in ("id", "usage", "status", "incomplete_details", "error") if k in resp}
    kind = {"incomplete": "response.incomplete", "failed": "response.failed"}.get(resp.get("status") or "",
                                                                                   "response.completed")
    out.append(_sse(kind, {"response": completed}))
    return b"".join(out)


def _log(record: dict) -> None:
    """One JSON line per relayed request to $HPCB_RELAY_LOG (the run bundle keeps it) — what Codex asked for and what
    the endpoint said, so a refusal is diagnosable rather than Codex's generic "high demand" retry message."""
    path = os.environ.get("HPCB_RELAY_LOG")
    if path:
        with _LOG_LOCK, open(path, "a") as fh:  # sub-agents make concurrent requests
            fh.write(json.dumps(record, default=str) + "\n")


_LOG_LOCK = threading.Lock()
_OUT_MAX = 8000


def _output_text(out: Any) -> str:
    if isinstance(out, list):  # structured content items
        return "\n".join(str(x.get("text") or "") for x in out if isinstance(x, dict))
    return out if isinstance(out, str) else json.dumps(out, default=str)


def new_outputs(req: dict, seen: set[str]) -> list[dict]:
    """The tool outputs Codex sends back for the first time in this request (each request carries the whole history)."""
    out = []
    for it in req.get("input") or []:
        if isinstance(it, dict) and it.get("type") in ("function_call_output", "custom_tool_call_output"):
            cid = str(it.get("call_id"))
            if cid not in seen:
                seen.add(cid)
                out.append({"call_id": cid, "output": _output_text(it.get("output"))[-_OUT_MAX:]})
    return out


def model_calls(resp: dict) -> tuple[list[dict], list[str]]:
    """(the tool calls the model asked for, in Codex's shapes; its prose) from one adapted response."""
    calls, texts = [], []
    for it in resp.get("output") or []:
        if not isinstance(it, dict):
            continue
        if it.get("type") == "function_call":
            calls.append({"call_id": it.get("call_id"), "name": it.get("name"), "namespace": it.get("namespace"),
                          "arguments": it.get("arguments")})
        elif it.get("type") == "custom_tool_call":
            calls.append({"call_id": it.get("call_id"), "name": it.get("name"), "namespace": it.get("namespace"),
                          "input": it.get("input")})
        elif it.get("type") == "message":
            texts += [str(c.get("text")) for c in it.get("content") or []
                      if isinstance(c, dict) and c.get("type") == "output_text" and c.get("text")]
    return calls, texts


class _Handler(BaseHTTPRequestHandler):
    upstream = ""
    seen: ClassVar[set[str]] = set()  # replaced per relay (start_relay)

    def log_message(self, *args) -> None:  # quiet
        pass

    def _forward(self, method: str) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        path = self.path.split("/v1", 1)[-1] if "/v1" in self.path else self.path
        streamed, names, tools, outputs, problem = False, {}, None, [], None
        req: Any = None
        if method == "POST" and path.rstrip("/").endswith("/responses") and body:
            try:
                req = json.loads(body)
            except ValueError:
                req = None
            if isinstance(req, dict):
                streamed = bool(req.get("stream"))
                try:
                    outputs = new_outputs(req, self.seen)
                    req, names = adapt_request(req)
                    tools = sorted(names)
                except Exception as e:  # noqa: BLE001 - forward unadapted; the endpoint's refusal is then logged
                    problem = f"adapt_request: {type(e).__name__}: {e}"
                req["stream"] = False
                body = json.dumps(req).encode()
        headers = {k: v for k, v in self.headers.items() if k.lower() in ("authorization", "openai-beta")}
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"
        up = urllib.request.Request(self.upstream.rstrip("/") + path, data=body or None, headers=headers,
                                    method=method)
        t0 = time.time()
        try:
            with urllib.request.urlopen(up, timeout=600) as r:
                status, data, ctype = r.status, r.read(), r.headers.get("Content-Type", "application/json")
        except urllib.error.HTTPError as e:
            status, data, ctype = e.code, e.read(), e.headers.get("Content-Type", "application/json")
        except Exception as e:  # noqa: BLE001 - surface it to Codex as an HTTP error
            status, data, ctype = 502, json.dumps({"error": {"message": f"relay: {e}"}}).encode(), "application/json"
        record: dict[str, Any] = {"t": t0, "s": round(time.time() - t0, 1), "method": method, "path": path,
                                  "status": status, "tools": tools, "outputs": outputs}
        if problem:
            record["problem"] = problem
        if status >= 400:
            record["error"] = data[:1500].decode(errors="replace")
            if isinstance(req, dict):  # which history shapes the endpoint saw, to find the one it refused
                record["input_shapes"] = sorted({f"{i.get('type', '-')}/{i.get('role', '-')}" for i in
                                                 req.get("input") or [] if isinstance(i, dict)})
        elif tools is not None:
            try:
                resp = adapt_response(json.loads(data), names)
            except ValueError:
                resp = None
            if not isinstance(resp, dict):
                status, ctype = 502, "application/json"
                record["error"] = "relay: the endpoint's answer was not a JSON object: " + data[:300].decode(errors="replace")
                data = json.dumps({"error": {"message": record["error"]}}).encode()
            else:
                record["output"] = [o.get("type") for o in resp.get("output") or [] if isinstance(o, dict)]
                record["calls"], record["texts"] = model_calls(resp)
                data = json.dumps(resp).encode()
                if streamed:
                    data, ctype = replay_as_events(resp), "text/event-stream"
        _log(record)
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        self._forward("POST")

    def do_GET(self) -> None:
        self._forward("GET")


class Relay:
    def __init__(self, server: ThreadingHTTPServer) -> None:
        self._server = server
        self.base_url = f"http://127.0.0.1:{server.server_address[1]}/v1"

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def start_relay(upstream: str) -> Relay:
    handler = type("Handler", (_Handler,), {"upstream": upstream, "seen": set()})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return Relay(server)
