"""The SCRIPTED operator — `HPCB_OPERATOR=script`: no model. The scenario's own SCRIPT drives hpc-bridge, one tool call
after another, so a cell can provoke ONE exact sequence at the moment it matters — queued_check_relaunch stops the
endpoint the instant its pilot is in the queue, before the canary can run — which no model's choices would time.

It runs where an agent CLI would (the jail, under run.py) and starts the hpc-bridge MCP server exactly as the CLI
operators do: this checkout through `uv run`, behind the stderr wrapper (cli_runner._server_cmd → server-stderr.log), with
the env Codex and Hermes pass it (cli_runner._server_env_names: the fixed passthrough plus the scenario's EXTRA_ENV) and
HPC_BRIDGE_JOURNAL. It speaks to it with the `mcp` stdio client, as agentic/install_check/mcp_probe.py does. The steps
(each tool call, its arguments, what came back, when it started and returned) are the script's own record, as a CLI's
event stream is its own, so the graded trace is built the way theirs is (cli_runner.build_trace): the steps fix the
order, hpc-bridge's journal says what each tool received and returned, and `harness:trace_complete` gates on every
journal row being one of the steps. The bundle holds the same rows (`native`, with the step's timestamps; `text`;
`final`; `journal`), so regrade.py replays it through cli_runner.trace_from_bundle_messages. The world checks, the
teardown and the cleanup are run.py's, unchanged.

A scenario's SCRIPT (imports nothing from here — scenario_knobs.py imports scenarios on the host, where cli_runner's
dependencies may be missing):

    async def SCRIPT(ops) -> str:                         # returns the run's final message
        r = await ops.call("connect_facility", facility=ops.facility, ssh_host=ops.ssh_host)
        ops.say("what the script saw")                    # the trace's texts: the script's own words
        ops.abort("why")                                  # a premise unmet: the run ends errored (run_completed fails)

`ops.call` never raises for a tool's failure: an error result, a refused call or a closed channel comes back as
{"is_error": True, "text": …} for the script to judge. run.py refuses a scenario without a SCRIPT (SETUP FAILED), and
this operator takes no persona, PHASES or MIDRUN_HOOKS (midrun_hooks.operator_refusal: SKIPPED).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tempfile
import time
import traceback
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, NoReturn

from cli_runner import CliFinal, Native, _bundle_native, _journal_rows, _server_cmd, _server_env_names, build_trace

OPERATOR = "script"
# One tool call: Codex's tool_timeout_sec and Hermes's timeout (cli_runner). hpc-bridge bounds its own waits (the
# sync-wait, the heartbeat), so this only ends a call to a wedged server.
CALL_TIMEOUT_S = 600.0
STARTUP_TIMEOUT_S = 180.0   # the server's start (`uv run` + imports): Codex's startup_timeout_sec
SCRIPT_TIMEOUT_S = float(os.environ.get("HPCB_SCRIPT_TIMEOUT_S", "1800"))


class ScriptAbort(Exception):
    """The script cannot go on — a premise of the cell is unmet (the login shape never came up, no pilot appeared). The
    run ends errored with this reason as its final message, so `run_completed` fails rather than graders passing on a
    sequence that never happened."""


Script = Callable[["ScriptOps"], Awaitable[Any]]
SessionFactory = Callable[[list[str], dict[str, str], str], contextlib.AbstractAsyncContextManager[Any]]


def script_refusal(scen: Any) -> str | None:
    """Why the script operator cannot drive `scen` (run.py: RESULT: SETUP FAILED), or None when it can."""
    if callable(getattr(scen, "SCRIPT", None)):
        return None
    name = getattr(scen, "__name__", "?")
    return (f"scenario {name!r} defines no SCRIPT — the script operator (HPCB_OPERATOR=script) drives only a scenario "
            "that scripts its own tool calls (`async def SCRIPT(ops)`); run it under an agent operator")


def tool_result(res: Any) -> dict:
    """A CallToolResult as the graders' dict — what cli_runner._journal_call makes of the journal's row: the tool's own
    model (structuredContent; a bare {"result": x} unwrapped, as the journal does), else its text, parsed when it is
    JSON; an error result as {"text", "is_error": True}."""
    text = "\n".join(str(t) for b in (getattr(res, "content", None) or []) if (t := getattr(b, "text", None)))
    if getattr(res, "isError", False):
        return {"text": text, "is_error": True}
    sc = getattr(res, "structuredContent", None)
    if isinstance(sc, dict):
        inner = sc.get("result", sc) if set(sc) == {"result"} else sc
        return inner if isinstance(inner, dict) else {"value": inner}
    try:
        obj = json.loads(text)
    except ValueError:
        return {"text": text}
    return obj if isinstance(obj, dict) else {"value": obj}


@dataclass
class Step:
    """One tool call the script made: the record that fixes the trace's order (a CLI's own event, for this operator)."""
    n: int
    tool: str
    args: dict[str, Any]
    result: dict[str, Any]
    t_start: float      # epoch seconds — the jail's clock, the server journal's too
    t_end: float


def _short(obj: Any, n: int = 110) -> str:
    s = json.dumps(obj, default=str)
    return s if len(s) <= n else s[: n - 1] + "…"


class ScriptOps:
    """What a SCRIPT drives hpc-bridge with. `facility` / `ssh_host`: this run's (run.py: `{facility}`, `{ssh_host}`)."""

    def __init__(self, session: Any, *, facility: str, ssh_host: str) -> None:
        self._session = session
        self.facility = facility
        self.ssh_host = ssh_host
        self.steps: list[Step] = []
        self.texts: list[str] = []

    async def call(self, tool: str, **args: Any) -> dict:
        """One hpc-bridge tool call, recorded as a step. Never raises for the tool's failure (see the module doc)."""
        t_start = time.time()
        print(f"  → {tool}({_short(args, 90)})", flush=True)
        try:
            res = await self._session.call_tool(tool, args, read_timeout_seconds=timedelta(seconds=CALL_TIMEOUT_S))
            out = tool_result(res)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a closed channel or a timeout is the script's to judge, as a result
            out = {"text": f"{type(exc).__name__}: {exc}", "is_error": True}
        step = Step(len(self.steps), tool, dict(args), out, round(t_start, 3), round(time.time(), 3))
        self.steps.append(step)
        said = out.get("status") or out.get("phase") or ("ERROR" if out.get("is_error") else "")
        print(f"  ← {tool}: {said} ({step.t_end - step.t_start:.1f}s) {_short(out.get('notice') or out.get('text') or '', 120)}",
              flush=True)
        return out

    def say(self, text: str) -> None:
        """The script's own words: the trace's `texts`, as an agent's prose would be."""
        self.texts.append(text)
        print(f"  ✎ {text}", flush=True)

    def abort(self, reason: str) -> NoReturn:
        raise ScriptAbort(reason)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


@contextlib.asynccontextmanager
async def stdio_session(cmd: list[str], env: dict[str, str], cwd: str) -> AsyncIterator[Any]:
    """The hpc-bridge server on stdio, initialised. mcp's client layers `env` over its own minimal environment (HOME,
    PATH, …), as the MCP clients of Codex and Hermes do with theirs; the wrapper in `cmd` is called by absolute path."""
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(command=cmd[0], args=cmd[1:], env=env, cwd=cwd)
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await asyncio.wait_for(session.initialize(), timeout=STARTUP_TIMEOUT_S)
        yield session


async def _drive(script: Script, ops: ScriptOps) -> tuple[str, str | None, str]:
    """Run the script to its end: (its final message, why it did not finish — None when it did, a traceback). Nothing
    but a cancellation escapes, so the stdio client's task group never wraps the script's own failure."""
    try:
        return str(await asyncio.wait_for(script(ops), timeout=SCRIPT_TIMEOUT_S) or ""), None, ""
    except ScriptAbort as exc:
        return "", f"script aborted: {exc}", ""
    except TimeoutError:
        return "", f"script timed out after {SCRIPT_TIMEOUT_S:.0f} s (HPCB_SCRIPT_TIMEOUT_S)", ""
    except Exception as exc:  # noqa: BLE001 - a script bug is the run's error, never a crashed cell
        return "", f"script failed: {type(exc).__name__}: {exc}", traceback.format_exc()


async def run_scenario(prompt: str, *, repo_root: Path, model: str = OPERATOR, effort: str | None = None,
                       persona: str | None = None, user_goal: str = "", ablate_skill: bool = False,
                       max_turns: int = 40, extra_env: dict[str, str] | None = None,
                       midrun_hooks: list[dict] | None = None, hook_runner=None,
                       script: Script | None = None, facility: str = "", ssh_host: str = "",
                       session_factory: SessionFactory | None = None):
    """Drive `script` against a fresh hpc-bridge server and return the RunResult run.py grades. `prompt`, `model`,
    `effort`, `user_goal` and `max_turns` mean nothing here (run.py passes every operator the same arguments)."""
    if script is None:
        raise ValueError("the script operator needs the scenario's SCRIPT")
    if persona or midrun_hooks:   # run.py reports such a cell SKIPPED first; this keeps a direct caller honest too
        raise NotImplementedError("the script operator takes no persona and no mid-run hooks")
    from runner import RunResult  # jail-only import chain (the Claude Agent SDK), as cli_runner keeps it

    runs = Path.home() / ".hpcb-cli-runs"
    runs.mkdir(parents=True, exist_ok=True)
    rec = Path(tempfile.mkdtemp(prefix=f"{OPERATOR}-", dir=runs))
    ws = Path(tempfile.mkdtemp(prefix="project-", dir=Path.home()))
    journal = rec / "journal.jsonl"
    env = dict(os.environ)
    env.update({k: v for k, v in (extra_env or {}).items() if v is not None})
    env["HPC_BRIDGE_JOURNAL"] = str(journal)
    if ablate_skill:   # parity with the CLI operators: the server then omits its instructions (nothing here reads them)
        env["HPC_BRIDGE_OMIT_INSTRUCTIONS"] = "1"
    extra = tuple(k for k, v in (extra_env or {}).items() if v is not None)
    server_env = {k: env[k] for k in _server_env_names(env, extra)}
    cmd = _server_cmd(str(repo_root), OPERATOR)
    print(f"  → {OPERATOR}: hpc-bridge over stdio ({' '.join(a if len(a) < 60 else a[:57] + '…' for a in cmd[:6])} …)",
          flush=True)
    t0 = time.monotonic()
    ops = ScriptOps(None, facility=facility, ssh_host=ssh_host)
    text, error, tb = "", None, ""
    drove = False
    try:
        async with (session_factory or stdio_session)(cmd, server_env, str(ws)) as session:
            ops._session = session
            text, error, tb = await _drive(script, ops)
            drove = True
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - the server never started (graded: agent_engaged), or closing it failed
        if not drove:
            error = f"the hpc-bridge MCP server failed: {type(exc).__name__}: {exc}"
            tb = traceback.format_exc()
        else:   # the script ran to its end; only the server's shutdown complained — evidence, not the run's verdict
            tb = (tb + "\n" if tb else "") + f"closing the server: {type(exc).__name__}: {exc}"
    final_text = text or error or ""
    print(f"  ← {OPERATOR}: {len(ops.steps)} step(s) in {time.monotonic() - t0:.0f}s; "
          f"{'ERROR: ' + error if error else 'final: ' + repr(final_text[:240])}", flush=True)
    rows = _journal_rows(journal)
    events = [Native(s.tool, s.args, s.result, hpcb=True, call_id=f"step-{s.n}") for s in ops.steps]
    trace, stats = build_trace(rows, events, ops.texts or ([final_text] if final_text else []))
    print(f"  trace: {len(trace.calls)} calls {stats}", flush=True)
    # The run dir dies with the jail: the steps (with their timestamps), the script's words and the journal ride in the
    # bundle — the cli_runner layout, which regrade.py and infra.py already read (the head row is `harness` + `rc`).
    messages = [{"harness": OPERATOR, "server": cmd[:8], "rc": 1 if error else 0,
                 "seconds": round(time.monotonic() - t0, 1), "trace_stats": stats, "stdout_tail": "",
                 "stderr_tail": (tb or error or "")[-4000:]},
                *({"native": {**_bundle_native(e), "step": s.n, "t_start": s.t_start, "t_end": s.t_end}}
                  for s, e in zip(ops.steps, events, strict=True)),
                *({"text": t} for t in ops.texts), {"final": final_text},
                *({"journal": r} for r in rows)]
    return RunResult(trace=trace, final=CliFinal(result=final_text, is_error=error is not None), messages=messages)
