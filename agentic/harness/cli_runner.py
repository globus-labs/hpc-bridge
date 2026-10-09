"""Drive a non-Claude agent HARNESS — Codex, Pi, or Hermes at its latest release — through a scenario, headlessly, and
grade the run. The graded trace has two sources: the harness's OWN record of the run (Codex: the relay's log of the
model's wire; Pi: `--mode json` events; Hermes: its state.db) fixes the order of every call and carries the agent's
own shell / read / search calls, and hpc-bridge's tool-call journal (HPC_BRIDGE_JOURNAL, written by the server)
carries exactly what each hpc-bridge tool received and returned.

Each harness is configured with the settings docs/user/other-hosts.md gives (Codex: startup timeout + approval mode;
Pi: `exposure: direct` + a 300 s timeout; Hermes: a long connect timeout, the env it filters passed explicitly), the
`driving-hpc` skill installed where it looks, and the model pointed at the ALCF OpenAI-compatible endpoint
(HPCB_ALCF_BASE_URL, bearer ALCF_INFERENCE_TOKEN) — the same open model under every harness, so a difference is the
harness's, not the model's. The server itself runs from this checkout (`uv run`), not the users' `uvx --from git+…`
— that install path is what agentic/install_check/ covers. The agent works in an empty directory (no
CLAUDE.md/AGENTS.md of ours leaks in), beside — not inside — the record that grades it.

AUTONOMOUS scenarios only for now (one turn: the prompt authorizes the spend). A persona'd scenario needs the harness's
long-lived session mode (an MCP server that lives across turns, as in real use) — raised as unsupported.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from invariants import ToolCall, Trace
from responses_relay import start_relay

HARNESSES = ("codex", "pi", "hermes21")
_UV = "/usr/local/bin/uv"
# What the hpc-bridge server needs from the jail's env; Codex and Hermes filter the child env, so it is named explicitly.
_PASSTHROUGH = (
    "HOME", "HPC_BRIDGE_USER_DIR", "GLOBUS_COMPUTE_USER_DIR", "HPC_BRIDGE_SSH_USER", "HPC_BRIDGE_SSH_KEY",
    "HPC_BRIDGE_SSH_HOST", "HPC_BRIDGE_ENDPOINT_NAME", "HPC_BRIDGE_MACHINE", "HPC_BRIDGE_SEARCH_INDEX",
    "HPC_BRIDGE_CATALOG_FILE", "HPCB_HARNESS_SSH_PORT", "HPC_BRIDGE_JOURNAL", "HPC_BRIDGE_OMIT_INSTRUCTIONS",
)
TURN_TIMEOUT_S = float(os.environ.get("HPCB_CLI_TURN_TIMEOUT_S", "2700"))


@dataclass
class CliFinal:
    """The run's last word, shaped like the SDK ResultMessage the graders read (`result`, `is_error`)."""
    result: str
    is_error: bool = False
    session_id: str | None = None
    total_cost_usd: float | None = None


def _server_cmd(repo: str) -> list[str]:
    return [_UV, "run", "--directory", repo, "--extra", "integration", "hpc-bridge"]


def _install_skill(repo: Path, *dirs: Path) -> None:
    src = repo / "skills" / "driving-hpc" / "SKILL.md"
    for d in dirs:
        (d / "driving-hpc").mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, d / "driving-hpc" / "SKILL.md")


def _model() -> tuple[str, str]:
    base = os.environ.get("HPCB_ALCF_BASE_URL", "").strip()
    if not base:
        raise SystemExit("cli_runner: HPCB_ALCF_BASE_URL is unset")
    return base, os.environ.get("HPCB_ALCF_MODEL", "openai/gpt-oss-120b")


# ---------------------------------------------------------------- per-harness setup + one turn

def setup_codex(work: Path, repo: str, env: dict[str, str], base_url: str | None = None) -> dict[str, str]:
    base, model = _model()
    base = base_url or base  # the local relay (responses_relay) when the endpoint cannot stream Responses
    home = work / "codex"
    home.mkdir(parents=True, exist_ok=True)
    names = sorted(k for k in _PASSTHROUGH if k in env)
    lines = [
        f"model = {json.dumps(model)}", 'model_provider = "alcf"', 'approval_policy = "never"',
        'sandbox_mode = "danger-full-access"', "tool_output_token_limit = 20000", "",
        "[model_providers.alcf]", 'name = "ALCF"', f"base_url = {json.dumps(base)}",
        'env_key = "ALCF_INFERENCE_TOKEN"', 'wire_api = "responses"', "",
        "[mcp_servers.hpc-bridge]", f"command = {json.dumps(_server_cmd(repo)[0])}",
        f"args = {json.dumps(_server_cmd(repo)[1:])}", f"env_vars = {json.dumps(names)}",
        "startup_timeout_sec = 180", "tool_timeout_sec = 600", "required = true",
        'default_tools_approval_mode = "approve"',
    ]
    (home / "config.toml").write_text("\n".join(lines) + "\n")
    return {"CODEX_HOME": str(home)}


def setup_pi(work: Path, repo: str, env: dict[str, str]) -> dict[str, str]:
    base, model = _model()
    home = work / "pi"
    home.mkdir(parents=True, exist_ok=True)
    (home / "models.json").write_text(json.dumps({"providers": {"alcf": {
        "baseUrl": base, "api": "openai-completions", "apiKey": "$ALCF_INFERENCE_TOKEN",
        "compat": {"supportsDeveloperRole": False, "supportsStore": False, "maxTokensField": "max_tokens"},
        "models": [{"id": model, "reasoning": True, "contextWindow": 131072, "maxTokens": 32768}]}}}, indent=1))
    cmd = _server_cmd(repo)
    (home / "mcp.json").write_text(json.dumps({"mcpServers": {"hpc-bridge": {
        "command": cmd[0], "args": cmd[1:], "exposure": "direct", "timeout": 300}}}, indent=1))
    return {"PI_CODING_AGENT_DIR": str(home), "PI_OFFLINE": "1", "PI_SKIP_VERSION_CHECK": "1", "PI_TELEMETRY": "0"}


def setup_hermes21(work: Path, repo: str, env: dict[str, str]) -> dict[str, str]:
    base, model = _model()
    home = work / "hermes"
    home.mkdir(parents=True, exist_ok=True)
    cmd = _server_cmd(repo)
    cfg = {
        "model": {"provider": "custom", "base_url": base, "api_key": "${ALCF_INFERENCE_TOKEN}", "default": model,
                  "context_length": 131072, "max_tokens": 4096},
        "approvals": {"mode": "off"},
        "mcp_servers": {"hpc-bridge": {"command": cmd[0], "args": cmd[1:],
                                       "env": {k: env[k] for k in _PASSTHROUGH if k in env},
                                       "connect_timeout": 180, "timeout": 600}},
    }
    (home / "config.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False))
    return {"HERMES_HOME": str(home)}


def _turn_argv(harness: str, prompt: str, ws: Path, rec: Path, session: str) -> list[str]:
    _, model = _model()
    if harness == "codex":
        return ["codex", "exec", "--json", "--skip-git-repo-check", "-s", "danger-full-access", "-C", str(ws),
                "-o", str(rec / "last-message.txt"), prompt]
    if harness == "pi":
        return ["pi", "--mode", "json", "--provider", "alcf", "--model", model, "--session-dir",
                str(rec / "pi-sessions"), "--session-id", session, prompt]
    return [os.environ.get("HPCB_HERMES21_BIN", str(Path.home() / ".local/bin/hermes")), "-z", prompt, "--yolo"]


def _native(harness: str, rec: Path, env: dict[str, str], stdout: str,
            hops: list[dict]) -> tuple[list[Native], list[str], str]:
    """(the harness's own calls in order, the agent's prose, its final message)."""
    if harness == "codex":
        # the relay saw the model's whole wire; `exec --json` omits write_stdin, sub-agents and patch content. The
        # agent's TEXT is what the user saw: `exec --json`'s main-thread messages, not sub-agents' or compaction's
        events, said = codex_events(stdout)
        texts = said
        if any("calls" in h for h in hops):
            events, relayed = relay_events(hops)
            texts = said or relayed
        f = rec / "last-message.txt"
        return events, texts, f.read_text().strip() if f.exists() else (texts[-1] if texts else "")
    if harness == "pi":
        events, texts = pi_events(stdout)
        return events, texts, texts[-1] if texts else ""
    events, texts = hermes_events(Path(env["HERMES_HOME"]) / "state.db")
    return events, texts, stdout.strip()


# ---------------------------------------------------------------- the trace: native order + the journal's record

# hpc-bridge's tools (test_cli_runner checks this against the server). A native call whose name ENDS with one of them
# is an hpc-bridge call under the harness's own prefix (Pi `mcp__hpc-bridge__…`, Hermes `mcp__hpc_bridge__…`).
HPCB_TOOLS = ("authenticate", "complete_login", "complete_preauth", "connect_facility", "ensure_endpoint_up",
              "list_facilities", "login_shell", "poll_task", "reset_session", "run_shell", "stop_endpoint",
              "teardown_endpoint")
# Each harness's OWN tools → the names the graders read (Claude's): the safety floor, no_ssh_workaround and the
# introspection report inspect the agent's own shell / read / search calls, which no MCP journal can see.
_NATIVE = {
    "bash": "Bash", "exec_command": "Bash", "shell": "Bash", "local_shell": "Bash", "terminal": "Bash",
    "execute_code": "Bash", "process": "Bash", "write_stdin": "Bash",
    "read": "Read", "read_file": "Read", "view_image": "Read",
    "grep": "Grep", "search_files": "Grep", "find": "Glob", "ls": "Glob",
    "write": "Write", "write_file": "Write", "edit": "Edit", "patch": "Edit", "apply_patch": "Edit",
}
# what reaches a shell, under each harness's key: Codex exec_command `cmd` / write_stdin `chars`, Hermes
# execute_code `code` / process(action=write|submit) `data`
_SHELL_TEXT_KEYS = ("cmd", "code", "chars", "data", "input")
_NOT_INVOKED = "The tool was NOT invoked"  # Hermes, when it refuses a deferred tool's arguments itself


@dataclass
class Native:
    """One tool call as the harness itself recorded it, in the order it made them."""
    name: str            # an hpc-bridge tool name, or the harness's own tool name
    args: dict[str, Any]
    result: dict[str, Any] | None = None
    hpcb: bool = False
    ran: bool | None = None   # False: the harness says the tool never ran (refused before dispatch); None: unknown
    call_id: str | None = None


def _hpcb_name(name: str) -> str | None:
    if name in HPCB_TOOLS:
        return name
    if not re.search(r"hpc[-_]bridge", name):  # another server's `…_run_shell` is not ours
        return None
    return next((t for t in HPCB_TOOLS if name.endswith(("_" + t, "." + t))), None)


def _native_call(n: Native) -> ToolCall:
    """A harness's own call, under the grader's name. For a shell-like call, every piece of text that reaches the shell
    lands in `command` (what _command_of and the floor read), whatever key the harness used."""
    graded = _NATIVE.get(n.name, n.name)
    args = dict(n.args)
    if graded == "Bash":
        cmd = args.get("command")
        if isinstance(cmd, list):
            cmd = " ".join(map(str, cmd))
        parts = [cmd, *(args.get(k) for k in _SHELL_TEXT_KEYS)]
        args["command"] = " ".join(p for p in parts if isinstance(p, str) and p)
    return ToolCall.of(graded, args, n.result)


def _journal_call(row: dict) -> ToolCall:
    res = row.get("result")
    if "error" in row:
        result: dict[str, Any] | None = {"text": row["error"], "is_error": True}
    elif isinstance(res, dict):
        result = res
    elif isinstance(res, str):
        try:
            obj = json.loads(res)
            result = obj if isinstance(obj, dict) else {"value": obj}
        except ValueError:
            result = {"text": res}
    else:
        result = {"value": res}
    return ToolCall.of(f"mcp__hpc-bridge__{row.get('tool')}", row.get("args") or {}, result)


def _loose(v: Any) -> Any:
    """A value as a harness may have coerced it before the call (Pi, Hermes): "true" → True, "3" → 3, a JSON string
    → the object it encodes."""
    if isinstance(v, str):
        try:
            return _loose(json.loads(v))
        except ValueError:
            return v
    if isinstance(v, dict):
        return {k: _loose(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_loose(x) for x in v]
    return v


def _same_args(native: dict, row: dict) -> bool:
    got = row.get("args") or {}
    return all(_loose(native.get(k)) == _loose(got.get(k))
               for k in set(native) | set(got) if got.get(k) != "<redacted>")


def _align(events: list[Native], rows: list[dict]) -> dict[int, int]:
    """Native hpc-bridge calls ↔ journal rows, ORDER-PRESERVING (calls start in the order the harness made them), the
    alignment matching the most calls — same arguments counting above same tool only. A call with no partner never
    reached the server; a row with none is one the harness's record cannot account for. Returns {event: row}."""
    n, m = len(events), len(rows)

    def score(e: Native, r: dict) -> int:
        if e.ran is False or e.name != str(r.get("tool")):
            return 0
        return 2 if _same_args(e.args, r) else 1

    best = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(n - 1, -1, -1):
        for j in range(m - 1, -1, -1):
            s = score(events[i], rows[j])
            best[i][j] = max(best[i + 1][j], best[i][j + 1], s + best[i + 1][j + 1] if s else 0)
    pairs: dict[int, int] = {}
    i = j = 0
    while i < n and j < m:
        s = score(events[i], rows[j])
        if s and best[i][j] == s + best[i + 1][j + 1]:
            pairs[i] = j
            i, j = i + 1, j + 1
        elif best[i][j] == best[i + 1][j]:
            i += 1
        else:
            j += 1
    return pairs


def build_trace(rows: list[dict], events: list[Native], texts: list[str]) -> tuple[Trace, dict[str, int]]:
    """The harness's own record fixes the ORDER of every call; hpc-bridge's journal is the record of what its tools
    received and returned. The harness's hpc-bridge calls and the journal rows (in the order the calls STARTED — `ts`,
    then `seq`; a row is written when its call ends) are aligned order-preservingly (`_align`). A call the harness
    itself refused (`ran is False`) takes no row; a call with no row is kept as an error; rows no native event
    accounts for (a reader blind spot) are appended and counted — run.py GATES on that count."""
    ordered = sorted(rows, key=lambda r: (r.get("ts") is None, r.get("ts") or 0, r.get("seq") or 0))
    hp = [k for k, e in enumerate(events) if e.hpcb]
    pairs = {hp[a]: b for a, b in _align([events[k] for k in hp], ordered).items()}
    calls: list[ToolCall] = []
    stats = {"native": 0, "hpcb_matched": 0, "harness_rejected": 0, "hpcb_not_reaching_server": 0,
             "journal_unplaced": 0}
    for k, ev in enumerate(events):
        if not ev.hpcb:
            calls.append(_native_call(ev))
            stats["native"] += 1
        elif k in pairs:
            calls.append(_journal_call(ordered[pairs[k]]))
            stats["hpcb_matched"] += 1
        else:
            refused = ev.ran is False
            why = ("refused by the harness; the call never reached hpc-bridge" if refused
                   else "the call never reached hpc-bridge (no journal row)")
            calls.append(ToolCall.of(f"mcp__hpc-bridge__{ev.name}", ev.args,
                                     {**(ev.result or {}), "text": why, "is_error": True}))
            stats["harness_rejected" if refused else "hpcb_not_reaching_server"] += 1
    used = set(pairs.values())
    left = [r for j, r in enumerate(ordered) if j not in used]
    calls.extend(_journal_call(r) for r in left)
    stats["journal_unplaced"] = len(left)
    return Trace(calls, texts), stats


def trace_from_journal(path: Path, texts: list[str]) -> Trace:
    """The journal alone (no native events): hpc-bridge's calls, in the order the server saw them."""
    return build_trace(_journal_rows(path), [], texts)[0]


def _bundle_native(n: Native) -> dict:
    return {"name": n.name, "hpcb": n.hpcb, "args": n.args, "result": n.result, "ran": n.ran, "call_id": n.call_id}


def trace_from_bundle_messages(messages: list[dict]) -> Trace:
    """Rebuild the graded Trace from a bundle this operator wrote (regrade.py): the `native`, `text`, `final` and
    `journal` entries are exactly what build_trace was given live."""
    events = [Native(m["native"]["name"], m["native"].get("args") or {}, m["native"].get("result"),
                     bool(m["native"].get("hpcb")), m["native"].get("ran"), m["native"].get("call_id"))
              for m in messages if "native" in m]
    texts = [m["text"] for m in messages if "text" in m]
    final = next((m["final"] for m in messages if "final" in m), "")
    return build_trace([m["journal"] for m in messages if "journal" in m], events,
                       texts or ([final] if final else []))[0]


def _journal_rows(path: Path) -> list[dict]:
    return _jsonl(path.read_text()) if path.exists() else []


def _clip(text: str, keep: int = 16000) -> str:
    """Head AND tail of a long output: a secret printed early must still reach the floor graders."""
    return text if len(text) <= keep else text[:keep // 2] + "\n…\n" + text[-keep // 2:]


def _jsonl(text: str) -> list[dict]:
    """Strict JSONL: records end at LF only — str.splitlines() would also split on U+2028/U+2029/U+0085, which
    JSON encoders leave raw inside strings, and silently drop the record (a command with one hides from the floor)."""
    out = []
    for line in text.split("\n"):
        line = line.strip("\r").strip()
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return out


def relay_events(hops: list[dict]) -> tuple[list[Native], list[str]]:
    """Codex, from the relay's record of the model's own wire (responses_relay): every call the model asked for, in
    order — write_stdin, sub-agents' calls and patch contents included — with the output Codex returned for each."""
    events: list[Native] = []
    by_id: dict[str, Native] = {}
    texts: list[str] = []
    for h in hops:
        for o in h.get("outputs") or []:  # this request's new outputs answer calls from earlier responses
            n = by_id.get(str(o.get("call_id")))
            if n is not None and n.result is None:
                n.result = {"text": str(o.get("output") or "")}
        for c in h.get("calls") or []:
            name, ns = str(c.get("name") or ""), str(c.get("namespace") or "")
            if "input" in c:
                args: Any = {"input": c.get("input")}
            else:
                try:
                    args = json.loads(c.get("arguments") or "{}")
                except ValueError:
                    args = {"arguments": c.get("arguments")}
            if not isinstance(args, dict):
                args = {"value": args}
            hp = name if ns.startswith("mcp__hpc_bridge") and name in HPCB_TOOLS else None
            n = Native(hp or name, args, hpcb=hp is not None, call_id=str(c.get("call_id")))
            events.append(n)
            by_id[str(n.call_id)] = n
        texts += [t for t in h.get("texts") or [] if t]
    return events, texts


def codex_events(stdout: str) -> tuple[list[Native], list[str]]:
    """`codex exec --json` (the fallback when there is no relay record): item.started / item.completed for
    command_execution, mcp_tool_call, file_change; agent_message items are the agent's prose. It does NOT report
    write_stdin or a sub-agent's items — the relay record does."""
    order: list[str] = []
    by_id: dict[str, Native] = {}
    texts: list[str] = []
    for ev in _jsonl(stdout):
        item = ev.get("item") if ev.get("type") in ("item.started", "item.completed") else None
        if not isinstance(item, dict):
            continue
        kind, iid = item.get("type"), str(item.get("id"))
        if kind == "agent_message" and ev["type"] == "item.completed" and item.get("text"):
            texts.append(item["text"])
            continue
        if kind == "command_execution":
            n = by_id.get(iid) or Native("exec_command", {"command": item.get("command", "")})
            if ev["type"] == "item.completed":
                n.result = {"text": _clip(str(item.get("aggregated_output") or "")), "exit_code": item.get("exit_code")}
        elif kind == "mcp_tool_call":
            tool = str(item.get("tool") or "")
            hp = item.get("server") == "hpc-bridge" and tool in HPCB_TOOLS
            n = by_id.get(iid) or Native(tool, item.get("arguments") or {}, hpcb=hp)
            if ev["type"] == "item.completed" and item.get("error"):
                err = item["error"]
                n.result = {"text": str(err.get("message") if isinstance(err, dict) else err), "is_error": True}
        elif kind == "file_change":
            n = by_id.get(iid) or Native("apply_patch", {"changes": item.get("changes") or []})
        else:
            continue
        if iid not in by_id:
            by_id[iid] = n
            order.append(iid)
    return [by_id[i] for i in order], texts


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    return "\n".join(str(b.get("text") or "") for b in content or [] if isinstance(b, dict) and b.get("type") == "text")


def pi_events(stdout: str) -> tuple[list[Native], list[str]]:
    """`pi --mode json`: tool_execution_start/end per call (an end without `durationMs` = the tool never ran);
    message_end carries each assistant message."""
    order: list[str] = []
    by_id: dict[str, Native] = {}
    texts: list[str] = []
    for ev in _jsonl(stdout):
        kind = ev.get("type")
        if kind == "tool_execution_start":
            name = str(ev.get("toolName") or "")
            hp = _hpcb_name(name)
            cid = str(ev.get("toolCallId"))
            if cid not in by_id:
                by_id[cid] = Native(hp or name, ev.get("args") or {}, hpcb=hp is not None, call_id=cid)
                order.append(cid)
        elif kind == "tool_execution_end":
            n = by_id.get(str(ev.get("toolCallId")))
            if n is not None:
                n.ran = "durationMs" in ev
                res = ev.get("result") or {}
                n.result = {"text": _clip(_content_text(res.get("content"))), "is_error": bool(ev.get("isError"))}
        elif kind == "message_end":
            msg = ev.get("message") or {}
            if msg.get("role") == "assistant" and (t := _content_text(msg.get("content")).strip()):
                texts.append(t)
    return [by_id[i] for i in order], texts


def hermes_events(db: Path) -> tuple[list[Native], list[str]]:
    """Hermes' state.db, in row order: assistant rows carry tool_calls (a deferred tool may go through the `tool_call`
    dispatcher, `{"calls": [{name, arguments}, …]}`, or its legacy single form); tool rows carry results ("The tool was
    NOT invoked" = Hermes refused it itself). A compaction summary row is Hermes' text, not the agent's, and is skipped;
    compaction also copies the kept tail into a child session, so a call id (or a text) seen before is not counted
    twice."""
    if not db.exists():
        return [], []
    import sqlite3
    con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(messages)")}
        skip = " WHERE _compressed_summary = 0" if "_compressed_summary" in cols else ""
        rows = [dict(r) for r in con.execute(f"SELECT * FROM messages{skip} ORDER BY id")]
    finally:
        con.close()
    events: list[Native] = []
    by_id: dict[str, Native] = {}
    seen_ids: set[str] = set()
    texts: list[str] = []
    texts_from: dict[str, Any] = {}  # text → the session it first appeared in
    for r in rows:
        if r.get("role") == "assistant":
            try:
                tcs = json.loads(r.get("tool_calls") or "[]")
            except ValueError:
                tcs = []
            for tc in tcs if isinstance(tcs, list) else []:
                tid = str(tc.get("id") or tc.get("call_id") or "")
                if tid and tid in seen_ids:
                    continue
                seen_ids.add(tid)
                fn = tc.get("function") or {}
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except ValueError:
                    args = {}
                name = str(fn.get("name") or "")
                inner: list[tuple[str, Any]] = [(name, args)] if name else []
                if name == "tool_call" and isinstance(args, dict):
                    raw = args.get("calls")
                    if isinstance(raw, str):
                        try:
                            raw = json.loads(raw)
                        except ValueError:
                            raw = []
                    raw = [raw] if isinstance(raw, dict) else raw
                    if raw is None and args.get("name"):
                        raw = [{"name": args.get("name"), "arguments": args.get("arguments")}]
                    inner = [(str(c.get("name") or ""), c.get("arguments") or {}) for c in raw or []
                             if isinstance(c, dict)]
                for nm, a in inner:
                    if isinstance(a, str):
                        try:
                            a = json.loads(a)
                        except ValueError:
                            a = {"value": a}
                    hp = _hpcb_name(nm)
                    n = Native(hp or nm, a if isinstance(a, dict) else {"value": a}, hpcb=hp is not None,
                               call_id=tid or None)
                    events.append(n)
                    if tid and len(inner) == 1:
                        by_id[tid] = n
            t = (r.get("content") or "").strip()
            if t and texts_from.get(t, r.get("session_id")) == r.get("session_id"):
                texts.append(t)
                texts_from.setdefault(t, r.get("session_id"))
        elif r.get("role") == "tool":
            n = by_id.get(str(r.get("tool_call_id")))
            if n is not None and n.result is None:
                content = str(r.get("content") or "")
                n.result = {"text": _clip(content)}
                if _NOT_INVOKED in content:
                    n.ran = False
    return events, texts


# ---------------------------------------------------------------- the operator

async def run_scenario(prompt: str, *, repo_root: Path, model: str = "default", effort: str | None = None,
                       persona: str | None = None, user_goal: str = "", ablate_skill: bool = False,
                       max_turns: int = 40, extra_env: dict[str, str] | None = None,
                       midrun_hooks: list[dict] | None = None, hook_runner=None,
                       harness: str = "codex"):
    if persona:
        raise NotImplementedError(f"the {harness} operator drives autonomous scenarios only (no persona) for now")
    if midrun_hooks:
        raise NotImplementedError(f"the {harness} operator does not support mid-run chaos hooks yet")
    if harness not in HARNESSES:
        raise ValueError(f"unknown harness {harness!r}")
    from runner import RunResult  # jail-only import chain (the Claude Agent SDK); kept off module import
    repo = str(repo_root)
    # under HOME, not /tmp: Codex refuses to put its helper binaries under a temporary dir. The RECORD (journal, relay
    # log, harness configs) lives in `rec`; the agent works in an empty ~/project-* — not in, or above, what grades it.
    runs = Path.home() / ".hpcb-cli-runs"
    runs.mkdir(parents=True, exist_ok=True)
    rec = Path(tempfile.mkdtemp(prefix=f"{harness}-", dir=runs))
    ws = Path(tempfile.mkdtemp(prefix="project-", dir=Path.home()))
    journal = rec / "journal.jsonl"
    env = dict(os.environ)
    env.update({k: v for k, v in (extra_env or {}).items() if v is not None})
    env["HPC_BRIDGE_JOURNAL"] = str(journal)
    if ablate_skill:
        env["HPC_BRIDGE_OMIT_INSTRUCTIONS"] = "1"
    relay = None
    if harness == "codex":  # Codex speaks only streamed Responses with namespaced tools; ALCF serves neither
        os.environ["HPCB_RELAY_LOG"] = str(rec / "relay.jsonl")
        relay = start_relay(_model()[0])
        env.update(setup_codex(rec, repo, env, base_url=relay.base_url))
    else:
        env.update({"pi": setup_pi, "hermes21": setup_hermes21}[harness](rec, repo, env))
    if not ablate_skill:  # where each looks: ~/.agents/skills (Codex, Pi), $HERMES_HOME/skills (Hermes)
        _install_skill(repo_root, Path.home() / ".agents" / "skills",
                       Path(env.get("HERMES_HOME", rec / "hermes")) / "skills")
    for k in ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"):  # the operator runs on the open model only
        env.pop(k, None)

    argv = _turn_argv(harness, prompt, ws, rec, str(uuid.uuid4()))
    print(f"  → {harness}: {' '.join(a if len(a) < 80 else a[:77] + '…' for a in argv[:8])} …", flush=True)
    t0 = time.monotonic()
    try:
        proc = await asyncio.to_thread(subprocess.run, argv, cwd=ws, env=env, stdin=subprocess.DEVNULL,
                                       capture_output=True, text=True, timeout=TURN_TIMEOUT_S)
        rc, out, err = proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        rc, out, err = 124, (exc.stdout or b"").decode() if isinstance(exc.stdout, bytes) else (exc.stdout or ""), \
            f"timed out after {TURN_TIMEOUT_S:.0f} s"
    finally:
        if relay:
            relay.shutdown()
    (rec / "harness-stdout.txt").write_text(out or "")
    (rec / "harness-stderr.txt").write_text(err or "")
    hops = _journal_rows(rec / "relay.jsonl")
    events, texts, text = _native(harness, rec, env, out or "", hops)
    print(f"  ← {harness}: rc={rc} in {time.monotonic() - t0:.0f}s; final: {text[:240]!r}", flush=True)
    if rc != 0:
        print(f"  stderr tail: {(err or '').strip()[-600:]}", flush=True)
    rows = _journal_rows(journal)
    trace, stats = build_trace(rows, events, texts or ([text] if text else []))
    print(f"  trace: {len(trace.calls)} calls {stats}", flush=True)
    final = CliFinal(result=text or (err or "")[-400:], is_error=rc != 0)
    # the run dir dies with the jail: carry the journal, the harness's own record and the relay log into the bundle
    messages = [{"harness": harness, "argv": argv[:6], "rc": rc, "seconds": round(time.monotonic() - t0, 1),
                 "trace_stats": stats, "stdout_tail": (out or "")[-4000:], "stderr_tail": (err or "")[-4000:]},
                *({"native": _bundle_native(n)} for n in events), *({"text": t} for t in texts), {"final": text},
                *({"journal": r} for r in rows), *({"relay": h} for h in hops)]
    return RunResult(trace=trace, final=final, messages=messages)
