"""A minimal, AGENT-AGNOSTIC ACP (Agent Client Protocol) client for the benchmark harness.

Drives any ACP-speaking agent harness (hermes `hermes acp`, and — the plan — Claude Code's ACP mode, Gemini CLI,
…) over one persistent session, with the persona'd human-sim answering the agent's asks and hpc-bridge registered
as an MCP server via `session/new`. This replaces the fragile, token-heavy `hermes -z` transcript-replay
(`Reference/Cross-harness study …` Follow-up 5; `Planned/ACP interactive benchmark driver.md`): one session (no
per-turn re-send), clean turn boundaries (`prompt()` returns when the turn ends), and a structured update stream.

v1 scope: a proof-of-connection + a single-turn `prompt`. The multi-turn human-sim loop + Trace-from-updates land
next. Requires `agent-client-protocol` (the `acp` package — hermes' `[acp]` extra; installed in the jail image).
Runs only in the harness image, never imported by the hermetic `pytest -q`.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import sys
from dataclasses import dataclass, field
from typing import Any

import acp
from acp import PROTOCOL_VERSION, Client
from acp.schema import (
    ClientCapabilities,
    EnvVariable,
    FileSystemCapabilities,
    Implementation,
    McpServerStdio,
    RequestPermissionResponse,
    SelectedPermissionOutcome,
)


@dataclass
class AcpCapture:
    """What the client observed over one ACP session: the agent's prose, the tool calls it made, and the
    permission requests it raised. NOT a grading source — hermes' adapter drops structured results (raw_output is
    None for any JSON result; a truncated rendering goes into content) and Zed's claude-agent-acp sets neither
    rawInput nor rawOutput (both verified in source, 2026-09-08); the graded Trace comes from each agent's own
    post-run store (hermes state.db / the Claude CLI transcript). It IS the live, ordered record of what the client
    saw: `events` is persisted into the bundle (`acp-updates.jsonl`) and cross-checked against the graded trace's
    hpc-bridge call count (`harness:acp_capture`) so a lagging or truncated trace source is caught."""
    texts: list[str] = field(default_factory=list)                 # AgentMessageChunk content, in order
    tool_calls: list[dict] = field(default_factory=list)           # {tool_call_id, title, kind, raw_input}
    tool_results: dict[str, Any] = field(default_factory=dict)     # tool_call_id -> raw_output (final)
    permissions: list[str] = field(default_factory=list)           # titles of tool calls we auto-approved
    events: list[dict] = field(default_factory=list)               # the flat, JSON-able event log, in order
    turn: int = 0                                                  # prompt turn the events belong to (1-based)


def _jsonable(o: Any, depth: int = 0) -> Any:
    """A JSON-able copy of an ACP payload (pydantic models → their dict; unknowns → str)."""
    if depth > 6:
        return str(o)
    if o is None or isinstance(o, (str, int, float, bool)):
        return o
    if isinstance(o, dict):
        return {str(k): _jsonable(v, depth + 1) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v, depth + 1) for v in o]
    dump = getattr(o, "model_dump", None)
    if callable(dump):
        with contextlib.suppress(Exception):
            return _jsonable(dump(), depth + 1)
    return str(o)


def _selected_outcome(option_id: str) -> Any:
    """The 'selected' permission outcome in whatever shape this `agent-client-protocol` release serializes.
    0.12.x types `RequestPermissionResponse.outcome` as `AllowedOutcome | DeniedOutcome` (`AllowedOutcome(option_id,
    outcome="selected")`); the older `SelectedPermissionOutcome` still imports but is no longer in that union, so
    pydantic keeps it as an opaque object and the response dies in json.dumps — "Object of type
    SelectedPermissionOutcome is not JSON serializable" — and the agent waits forever for an answer (found live on
    the first claude-acp cell, 2026-09-08, after the 0.9.0 → 0.12.1 bump). 0.9.x has only the older class."""
    allowed = getattr(__import__("acp.schema", fromlist=["AllowedOutcome"]), "AllowedOutcome", None)
    if allowed is not None:
        return allowed(option_id=option_id, outcome="selected")
    return SelectedPermissionOutcome(option_id=option_id)


def _chunk_text(content: Any) -> str:
    """Pull plain text out of an ACP content block / list of blocks."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(_chunk_text(c) for c in content)
    return str(getattr(content, "text", "") or "")


def _fmt_call(title: Any, kind: Any, raw_input: Any) -> str:
    """A compact `[kind] name(args)` line for the live stderr play-by-play — the ACP counterpart of the
    Claude-SDK operator's `  → tool(args)` (runner.py). The ACP `title` is ALREADY the tool name for MCP calls
    (e.g. `connect_facility`) and a short label for the agent's own file/terminal/search tools, so it is printed
    VERBATIM (never parsed — an earlier split on ':'/'__' mangled path- and glob-shaped titles); `raw_input`
    supplies the args; `kind` (read/execute/search/…) gives context for terse titles like `src` or `*.py`."""
    name = str(title or "?").strip() or "?"
    if isinstance(raw_input, dict):
        args = ", ".join(f"{k}={str(v)[:40]}" for k, v in raw_input.items())
    elif raw_input is None:
        args = ""
    else:
        args = str(raw_input)[:80]
    k = str(kind or "").rpartition(".")[2].lower()   # 'ToolKind.READ' -> 'read'; '' stays ''
    body = f"{name}({args})" if args else name
    s = f"[{k}] {body}" if k and k != "other" and k != name.lower() else body
    return s if len(s) <= 160 else s[:159] + "…"


class BenchClient(Client):
    """Auto-approves every permission (the disposable jail IS the sandbox, like Claude's bypassPermissions),
    records the update stream, and stubs the fs/terminal client methods (the agent drives HPC via the hpc-bridge
    MCP server, not the ACP client's own file/terminal channels)."""

    def __init__(self) -> None:
        self.capture = AcpCapture()

    def _event(self, event: str, **fields: Any) -> None:
        self.capture.events.append({"event": event, "turn": self.capture.turn, **{k: _jsonable(v) for k, v in fields.items()}})

    async def request_permission(self, options: list, session_id: str, tool_call: Any, **kwargs: Any):
        title = str(getattr(tool_call, "title", "") or getattr(tool_call, "tool_call_id", "") or "?")
        self.capture.permissions.append(title)
        allow = next((o for o in options if str(getattr(o, "kind", "")) in ("allow_once", "allow_always")), None)
        chosen = allow or (options[0] if options else None)
        self._event("permission", title=title, tool_call_id=getattr(tool_call, "tool_call_id", None),
                    raw_input=getattr(tool_call, "raw_input", None),
                    options=[{"option_id": getattr(o, "option_id", None), "kind": str(getattr(o, "kind", "")),
                              "name": getattr(o, "name", None)} for o in options],
                    chosen=(getattr(chosen, "option_id", None) if chosen is not None else None))
        if chosen is None:                                          # no options offered → nothing to select
            raise acp.RequestError(-32603, "no permission options offered")  # type: ignore[attr-defined]
        return RequestPermissionResponse(outcome=_selected_outcome(chosen.option_id))

    async def session_update(self, session_id: str, update: Any, **kwargs: Any) -> None:
        kind = str(getattr(update, "session_update", "") or type(update).__name__)
        if "agent_message" in kind or type(update).__name__ == "AgentMessageChunk":
            text = _chunk_text(getattr(update, "content", None))
            self.capture.texts.append(text)
            self._event("message_chunk", text=text)
        elif type(update).__name__ == "ToolCallStart" or kind == "tool_call":
            title = getattr(update, "title", None)
            raw_input = getattr(update, "raw_input", None)
            call_kind = str(getattr(update, "kind", "") or "")
            self.capture.tool_calls.append({
                "tool_call_id": getattr(update, "tool_call_id", None),
                "title": title,
                "kind": call_kind,
                "raw_input": raw_input,
            })
            self._event("tool_call", tool_call_id=getattr(update, "tool_call_id", None), title=title, kind=call_kind,
                        raw_input=raw_input, content=_chunk_text(getattr(update, "content", None)) or None)
            # Live legibility: stream each operator tool call to stderr, so the hermes/ACP docker log gets the
            # same `  → tool(args)` play-by-play the Claude-SDK operator prints (runner.py). Without it the log
            # shows only the human-sim's replies — the operator's list_facilities/connect/run_shell steps land
            # only in the post-hoc Trace. Best-effort; logging must never break the run.
            with contextlib.suppress(Exception):
                print(f"  → {_fmt_call(title, call_kind, raw_input)}", file=sys.stderr, flush=True)
        elif type(update).__name__ == "ToolCallProgress" or "tool_call_update" in kind:
            tid = getattr(update, "tool_call_id", None)
            raw_output = getattr(update, "raw_output", None)
            if tid is not None and raw_output is not None:
                self.capture.tool_results[tid] = raw_output
            self._event("tool_call_update", tool_call_id=tid, status=str(getattr(update, "status", "") or ""),
                        raw_output=raw_output, content=_chunk_text(getattr(update, "content", None)) or None)
        else:
            self._event("other", kind=kind)

    # --- fs/terminal: not used for HPC-driving (hpc-bridge tools ride the MCP server); safe stubs ---
    async def write_text_file(self, content: str, path: str, session_id: str, **kwargs: Any):
        raise acp.RequestError(-32601, "write_text_file not supported by the benchmark client")  # type: ignore[attr-defined]

    async def read_text_file(self, path: str, session_id: str, **kwargs: Any):
        raise acp.RequestError(-32601, "read_text_file not supported by the benchmark client")  # type: ignore[attr-defined]

    async def create_terminal(self, command: str, session_id: str, **kwargs: Any):
        raise acp.RequestError(-32601, "terminals not supported by the benchmark client")  # type: ignore[attr-defined]

    async def terminal_output(self, session_id: str, terminal_id: str, **kwargs: Any):
        raise acp.RequestError(-32601, "terminals not supported")  # type: ignore[attr-defined]

    async def release_terminal(self, session_id: str, terminal_id: str, **kwargs: Any):
        return None

    async def wait_for_terminal_exit(self, session_id: str, terminal_id: str, **kwargs: Any):
        raise acp.RequestError(-32601, "terminals not supported")  # type: ignore[attr-defined]

    async def kill_terminal(self, session_id: str, terminal_id: str, **kwargs: Any):
        return None


def hpc_bridge_mcp(repo_root: str, env: dict[str, str]) -> McpServerStdio:
    """The hpc-bridge MCP server as an ACP stdio server (registered via session/new)."""
    passthrough = ("HOME", "HPC_BRIDGE_USER_DIR", "GLOBUS_COMPUTE_USER_DIR", "HPC_BRIDGE_SSH_USER",
                   "HPC_BRIDGE_SSH_KEY", "HPC_BRIDGE_SSH_HOST", "HPC_BRIDGE_ENDPOINT_NAME", "HPC_BRIDGE_MACHINE",
                   "HPC_BRIDGE_SEARCH_INDEX", "HPC_BRIDGE_CATALOG_FILE", "HPCB_HARNESS_SSH_PORT")
    ev = [EnvVariable(name=k, value=env[k]) for k in passthrough if k in env]
    return McpServerStdio(
        name="hpc-bridge", command="/usr/local/bin/uv",
        args=["run", "--directory", repo_root, "--extra", "integration", "hpc-bridge"], env=ev,
    )


def _join_chunks(chunks: list[str]) -> str:
    """Reassemble a turn's AgentMessageChunk texts. Over a STREAMING provider (the Argo tunnel) the chunks are token
    DELTAS — joining them with ' ' put spaces inside words ("part ition", "sp ending"; sonnet-5 via Argo, 2026-09-08)
    and the human-sim read that garble as the operator's ask. Deltas concatenate verbatim. Over a non-streaming
    provider (ALCF) a chunk is a whole message; when one ends a sentence and the next starts a new one with no
    whitespace between them, a newline keeps them apart. (Grading never reads this text — the graded question comes
    from state.db post-run — but the sim's judgement does.)"""
    out = ""
    for c in chunks:
        if not c:
            continue
        if out and out.rstrip()[-1:] in ".!?" and not out[-1].isspace() and c[0].isalpha() and c[0].isupper():
            out += "\n"
        out += c
    return out


@dataclass
class AcpTurn:
    """One turn of an ACP session: the operator's closing prose for that turn (used to decide whether it asked
    the user something), and the running tool-call count at that point (so a human reply can be stamped in order)."""
    text: str
    calls_so_far: int


async def run_session(command: str, args: list[str], task: str, *, cwd: str, env: dict[str, str],
                      mcp_servers: list[McpServerStdio], respond=None, max_turns: int = 1,
                      **session_kwargs: Any) -> tuple[Any, AcpCapture]:
    """Drive an ACP agent over ONE persistent session, up to ``max_turns`` prompt turns. ``respond`` (async,
    optional) is the interactive hook: ``respond(AcpTurn) -> str | None`` — return the user's next message to send
    it as another prompt in the SAME session, or None/"" to stop. This is the clean multi-turn the transcript-replay
    faked: no per-turn conversation re-send (the agent keeps its context), and ``prompt()`` returning IS the turn
    boundary (no `ends_with_question` guessing needed to detect turn end — only to decide whether to reply)."""
    client = BenchClient()
    resp: Any = None
    async with acp.spawn_agent_process(client, command, *args, env=env, cwd=cwd) as (conn, _proc):
        await conn.initialize(
            protocol_version=PROTOCOL_VERSION,
            client_capabilities=ClientCapabilities(fs=FileSystemCapabilities(read_text_file=False,
                                                                             write_text_file=False), terminal=False),
            client_info=Implementation(name="hpc-bridge-bench", version="0.1"),
        )
        # session_kwargs: agent-specific extras, e.g. `field_meta` (ACP `_meta`) — claude-agent-acp reads
        # `_meta.claudeCode.options.model` to pin the model.
        sess = await conn.new_session(cwd=cwd, mcp_servers=mcp_servers, **session_kwargs)
        prompt_text = task
        for _turn in range(max(1, max_turns)):
            client.capture.turn += 1
            client._event("user_prompt", text=prompt_text)
            seen = len(client.capture.texts)
            resp = await conn.prompt(prompt=[acp.text_block(prompt_text)], session_id=sess.session_id)
            client._event("turn_end", stop_reason=str(getattr(resp, "stop_reason", "") or ""))
            if respond is None:
                break
            turn_text = _join_chunks(client.capture.texts[seen:]).strip()
            reply = await respond(AcpTurn(text=turn_text, calls_so_far=len(client.capture.tool_calls)))
            if not reply:
                break
            prompt_text = reply
    return resp, client.capture


async def run_once(command: str, args: list[str], task: str, *, cwd: str, env: dict[str, str],
                   mcp_servers: list[McpServerStdio]) -> tuple[Any, AcpCapture]:
    """Drive one ACP agent through a single prompt turn; return (PromptResponse, capture)."""
    return await run_session(command, args, task, cwd=cwd, env=env, mcp_servers=mcp_servers, max_turns=1)


async def _probe() -> int:
    """Proof of connection: drive `hermes acp`, register hpc-bridge, ask it to list facilities."""
    env = dict(os.environ)
    repo = "/work/hpc-bridge"
    task = ("Call the hpc-bridge list_facilities tool once, then reply with ONLY the facility ids, "
            "comma-separated.")
    resp, cap = await run_once("hermes", ["acp"], task, cwd=repo, env=env,
                               mcp_servers=[hpc_bridge_mcp(repo, env)])
    print(f"stop_reason: {getattr(resp, 'stop_reason', None)}", file=sys.stderr)
    print(f"tool calls: {[c.get('title') or (c.get('raw_input') or {}) for c in cap.tool_calls]}", file=sys.stderr)
    print("AGENT SAID:", _join_chunks(cap.texts).strip()[-400:])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_probe()))
