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
        rc, out = await ops.world("cmd", host=None)       # the harness' world channel (not in the trace)
        ops.abort("why")                                  # a premise unmet: the run ends errored (run_completed fails)

`ops.call` never raises for a tool's failure: an error result, a refused call or a closed channel comes back as
{"is_error": True, "text": …} for the script to judge. run.py refuses a scenario without a SCRIPT (SETUP FAILED), and
this operator takes no persona; PHASES only when every phase is scripted; and MIDRUN_HOOKS that run a `cmd` after a tool
result (polling ones too), fed from the journal as for the CLI operators (midrun_hooks.follow_journal).

Batch phase 1 adds two server-lifecycle steps (design §10's harness work):
- `await ops.restart_server()` KILLS the server (SIGKILL: no shutdown, no flush — a crash) and starts a new one on the
  same state directory and journal; the script's steps go on against the new process.
- `b = await ops.open_server("B")` starts a SECOND server on the same state directory and journal, beside the first;
  `b.call(...)` is a step of the same run, labelled with its server (`ToolCall.server`: "A", "B").
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import tempfile
import time
import traceback
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, NoReturn

from cli_runner import (
    CliFinal,
    Native,
    _bundle_native,
    _journal_call,
    _journal_rows,
    _server_cmd,
    _server_env_names,
    build_trace,
    label_servers,
)
from invariants import ToolCall, Trace
from midrun_hooks import HookWatcher, fired_record, follow_journal, hook_label, trigger_text
from server_stderr import wrap

OPERATOR = "script"
# One tool call: Codex's tool_timeout_sec and Hermes's timeout (cli_runner). hpc-bridge bounds its own waits (the
# sync-wait, the heartbeat), so this only ends a call to a wedged server.
CALL_TIMEOUT_S = 600.0
STARTUP_TIMEOUT_S = 180.0   # the server's start (`uv run` + imports): Codex's startup_timeout_sec
SCRIPT_TIMEOUT_S = float(os.environ.get("HPCB_SCRIPT_TIMEOUT_S", "1800"))


def _read_timeout(session: Any) -> float | timedelta:
    """CALL_TIMEOUT_S in the form this `mcp` takes. The jail's newer client types `read_timeout_seconds` as a float and
    adds it to one (a timedelta raised `TypeError: float + timedelta` before the first call left — live 2026-10-15, the
    first queued_check_relaunch run); the host's older client wants a timedelta. The annotation decides, read as text so
    both a real type (`datetime.timedelta | None`) and a postponed one (`"timedelta | None"`) match; a parameter that is
    renamed or gone, or a signature that cannot be read, falls back to the float (never a KeyError on every call)."""
    import inspect
    try:
        param = inspect.signature(session.call_tool).parameters.get("read_timeout_seconds")
    except (TypeError, ValueError, AttributeError):
        param = None
    ann = "" if param is None or param.annotation is inspect.Parameter.empty else str(param.annotation)
    return timedelta(seconds=CALL_TIMEOUT_S) if "timedelta" in ann else CALL_TIMEOUT_S


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
    phases = getattr(scen, "PHASES", None) or []
    if phases and all(isinstance(p, dict) and callable(p.get("script")) for p in phases):
        return None   # a chain of scripted sessions (run.py `_phase_spec`): every phase brings its own
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
    server: str = "A"   # which server process answered (open_server labels a second one)


def _short(obj: Any, n: int = 110) -> str:
    s = json.dumps(obj, default=str)
    return s if len(s) <= n else s[: n - 1] + "…"


World = Callable[..., tuple[int, str]]   # run.py's `_ssh_run(cmd, timeout=…, host=…)`


Opener = Callable[[], contextlib.AbstractAsyncContextManager[Any]]


class ScriptOps:
    """What a SCRIPT drives hpc-bridge with. `facility` / `ssh_host`: this run's (run.py: `{facility}`, `{ssh_host}`).
    `label`: the server this handle talks to ("A"; `open_server` makes "B"…), whose steps share one record."""

    def __init__(self, session: Any, *, facility: str, ssh_host: str, world: World | None = None, label: str = "A",
                 opener: Opener | None = None, journal: Path | None = None, parent: ScriptOps | None = None) -> None:
        self._session = session
        self._world = world
        self.facility = facility
        self.ssh_host = ssh_host
        self.label = label
        self._opener = opener
        self._journal = journal
        self._stack: asyncio.Task[BaseException | None] | None = None   # the server's host task (`_open`)
        self._closing = asyncio.Event()
        self.server_pid: int | None = None   # learnt from the journal rows this server wrote (its `pid`)
        self.restarts: list[dict] = []
        root = parent if parent is not None else self
        self._root = root
        if parent is None:
            self.steps: list[Step] = []
            self.texts: list[str] = []
            self.text_at: list[int] = []   # how many steps came before each text (the Trace's text_at: cli_runner)
            self.servers: list[ScriptOps] = [self]
        else:
            self.steps, self.texts, self.text_at, self.servers = root.steps, root.texts, root.text_at, root.servers

    async def call(self, tool: str, **args: Any) -> dict:
        """One hpc-bridge tool call, recorded as a step. Never raises for the tool's failure (see the module doc)."""
        t_start = time.time()
        print(f"  → {tool}({_short(args, 90)})", flush=True)
        try:
            res = await self._session.call_tool(tool, args, read_timeout_seconds=_read_timeout(self._session))
            out = tool_result(res)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a closed channel or a timeout is the script's to judge, as a result
            out = {"text": f"{type(exc).__name__}: {exc}", "is_error": True}
        step = Step(len(self.steps), tool, dict(args), out, round(t_start, 3), round(time.time(), 3), self.label)
        self.steps.append(step)
        self._learn_pid(tool, t_start)
        said = out.get("status") or out.get("phase") or ("ERROR" if out.get("is_error") else "")
        print(f"  ← {tool}: {said} ({step.t_end - step.t_start:.1f}s) {_short(out.get('notice') or out.get('text') or '', 120)}",
              flush=True)
        return out

    def say(self, text: str) -> None:
        """The script's own words: the trace's `texts`, as an agent's prose would be — placed after the steps so far."""
        self.texts.append(text)
        self.text_at.append(len(self.steps))
        print(f"  ✎ {text}", flush=True)

    async def world(self, cmd: str, *, host: str | None = None, timeout: int = 60) -> tuple[int, str]:
        """One command over the harness' WORLD channel (run.py `_ssh_run`: ssh as the pool user, on the harness' own
        port; `host` = one login node of a pool) — how a script changes the world mid-run, as a SETUP or a mid-run hook
        would. Not a tool call: nothing of it is in the trace. (rc, stdout+stderr); 255 without a channel."""
        if self._world is None:
            return 255, "no world channel (run.py passes one)"
        print(f"  ⚙ world{f' [{host}]' if host else ''}: {cmd[:100]}", flush=True)
        return await asyncio.to_thread(self._world, cmd, timeout=timeout, host=host)

    def abort(self, reason: str) -> NoReturn:
        raise ScriptAbort(reason)

    # -- the server's lifecycle (batch phase 1) ------------------------------------------------------------------------
    def _learn_pid(self, tool: str, t_start: float) -> None:
        """This server's pid: the journal row of the call just made (the server writes `pid` on every row)."""
        if self._journal is None:
            return
        for row in reversed(_journal_rows(self._journal)):
            if row.get("tool") == tool and isinstance(row.get("ts"), int | float) and row["ts"] >= t_start - 1.0:
                if isinstance(row.get("pid"), int) and row["pid"] not in {o.server_pid for o in self.servers
                                                                          if o is not self}:
                    self.server_pid = row["pid"]
                return

    async def _open(self) -> None:
        """Start the server in a task of its own, which enters the stdio session's context and later leaves it. anyio
        cancel scopes must be left in the task that entered them (the script runs in another task: `_drive`'s
        wait_for), and a server that dies makes the stdio client cancel the scope it was entered in — its host task
        here, never the script (live 2026-10-31: the first restart's SIGKILL cancelled the script and run.py read the
        cell as INTERRUPTED)."""
        if self._opener is None:
            raise RuntimeError("this ScriptOps cannot start a server (no opener)")
        loop = asyncio.get_running_loop()
        ready: asyncio.Future[Any] = loop.create_future()
        closing = asyncio.Event()
        opener = self._opener

        async def host() -> BaseException | None:
            try:
                async with opener() as session:
                    if not ready.done():
                        ready.set_result(session)
                    await closing.wait()
            except BaseException as exc:  # noqa: BLE001 - a dead server's teardown is this task's, reported to the closer
                if not ready.done():
                    ready.set_exception(exc if isinstance(exc, Exception) else RuntimeError(f"the server ended: {exc!r}"))
                return exc
            return None

        self._closing = closing
        self._stack = asyncio.create_task(host())
        self._session = await ready

    async def _close(self) -> None:
        with contextlib.suppress(Exception):
            await self._close_raising()

    async def _close_raising(self) -> None:
        """End the server's host task (it leaves the session's context itself); a server that already died returns its
        ending, which is evidence, not this close's failure — only a close that could not run raises."""
        host, self._stack = self._stack, None
        if host is None:
            return
        self._closing.set()
        try:
            ended = await asyncio.wait_for(asyncio.shield(host), timeout=60)
        except TimeoutError:
            host.cancel()
            raise RuntimeError(f"server {self.label} did not close within 60 s") from None
        if ended is not None:   # it died before this close (a restart's kill): its session's ending, for the log
            print(f"  ⚠ server {self.label} had ended: {type(ended).__name__}: {str(ended)[:160]}", flush=True)

    async def restart_server(self, *, kill: bool = True, wait_s: float = 10.0) -> dict:
        """Kill this server (SIGKILL by default: a crash, nothing flushed) and start a new one on the same state
        directory and journal. Returns what was done; the script's own words record it too."""
        pid = self.server_pid
        killed = False
        if kill and pid:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(pid, signal.SIGKILL)
                killed = True
            deadline = time.monotonic() + wait_s
            while killed and time.monotonic() < deadline:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break
                except PermissionError:
                    break
                await asyncio.sleep(0.1)
        await self._close()
        await self._open()
        self.server_pid = None
        rec = {"server": self.label, "killed_pid": pid if killed else None, "at": round(time.time(), 3),
               "after_step": len(self.steps)}
        self.restarts.append(rec)
        self.say(f"server {self.label} {'killed (SIGKILL, pid ' + str(pid) + ')' if killed else 'closed'} and "
                 "started again on the same state directory")
        return rec

    async def open_server(self, label: str = "B") -> ScriptOps:
        """A second server process on the same state directory and journal, beside this one."""
        other = ScriptOps(None, facility=self.facility, ssh_host=self.ssh_host, world=self._world, label=label,
                          opener=self._opener, journal=self._journal, parent=self._root)
        await other._open()
        self.servers.append(other)
        self.say(f"server {label} started beside {self.label} on the same state directory")
        return other

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


def steps_trace(steps: list[Step], texts: list[str], text_at: list[int]) -> tuple[Trace, dict[str, Any]]:
    """The trace of a server that keeps no journal — an hpc-bridge older than HPC_BRIDGE_JOURNAL (0.1.20: the
    returning-user cells' first session, `server_cmd` = the previous release). The script's own steps are then the
    record of what each tool got and returned (the MCP client's view of the same answers); nothing is unaccounted for."""
    calls = [ToolCall.of(f"mcp__hpc-bridge__{s.tool}", s.args, s.result) for s in steps]
    placed = list(text_at) if len(text_at) == len(texts) else []
    return Trace(calls, list(texts), placed), {"native": 0, "hpcb_matched": 0, "steps_as_record": len(steps),
                                               "journal_unplaced": 0}


async def run_scenario(prompt: str, *, repo_root: Path, model: str = OPERATOR, effort: str | None = None,
                       persona: str | None = None, user_goal: str = "", ablate_skill: bool = False,
                       max_turns: int = 40, extra_env: dict[str, str] | None = None,
                       midrun_hooks: list[dict] | None = None, hook_runner=None,
                       script: Script | None = None, facility: str = "", ssh_host: str = "",
                       session_factory: SessionFactory | None = None, world: World | None = None,
                       server_cmd: list[str] | None = None, journal_kept: bool = True):
    """Drive `script` against a fresh hpc-bridge server and return the RunResult run.py grades. `prompt`, `model`,
    `effort`, `user_goal` and `max_turns` mean nothing here (run.py passes every operator the same arguments).
    `world`: the harness' world channel the script's `ops.world` uses (run.py's `_ssh_run`). `server_cmd`: another
    hpc-bridge server than this checkout's (the previous release, prev_release.server_argv), started behind the same
    stderr wrapper; `journal_kept=False` says it writes no HPC_BRIDGE_JOURNAL, so the steps are the record
    (`steps_trace`)."""
    if script is None:
        raise ValueError("the script operator needs the scenario's SCRIPT")
    if persona:   # run.py reports such a cell SKIPPED first; this keeps a direct caller honest too
        raise NotImplementedError("the script operator takes no persona")
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
    cmd = wrap(list(server_cmd), OPERATOR) if server_cmd else _server_cmd(str(repo_root), OPERATOR)
    print(f"  → {OPERATOR}: hpc-bridge over stdio ({' '.join(a if len(a) < 60 else a[:57] + '…' for a in cmd[:6])} …)",
          flush=True)
    t0 = time.monotonic()
    factory = session_factory or stdio_session
    ops = ScriptOps(None, facility=facility, ssh_host=ssh_host, world=world,
                    opener=lambda: factory(cmd, server_env, str(ws)), journal=journal)
    # MIDRUN_HOOKS (`cmd` after a tool result, polling ones too): fed from the server's journal while the script runs,
    # as cli_runner does for the CLI operators
    watcher = HookWatcher(midrun_hooks)
    fired: list[dict] = []
    stop = asyncio.Event()

    async def act(hook: dict) -> None:
        print(f"  💥 hook: {hook_label(hook)} {trigger_text(hook)} (journal row {hook['call_index']})", flush=True)
        try:
            outcome = (await hook_runner(hook) if hook_runner else None) or {"rc": None, "out": "(no hook runner)"}
        except Exception as exc:  # noqa: BLE001 - recorded as a failed hook (harness:midrun_hooks gates it), never lost
            outcome = {"rc": 1, "out": f"the hook failed: {type(exc).__name__}: {exc}"}
        fired.append(fired_record(hook, outcome))

    follower = (asyncio.create_task(follow_journal(journal, watcher, act, stop=stop, to_call=_journal_call,
                                                   record=fired)) if midrun_hooks else None)
    text, error, tb = "", None, ""
    drove = False
    try:
        try:
            await ops._open()
            text, error, tb = await _drive(script, ops)
            drove = True
        finally:
            closing = [o for o in ops.servers if o._stack is not None]
            for o in closing:
                try:
                    await o._close_raising()
                except Exception as exc:
                    if drove:   # the script ran to its end; only a server's shutdown complained — evidence
                        tb = (tb + "\n" if tb else "") + f"closing server {o.label}: {type(exc).__name__}: {exc}"
                    else:
                        raise
    except asyncio.CancelledError:
        if follower is not None:
            follower.cancel()
        raise
    except Exception as exc:  # noqa: BLE001 - the server never started (graded: agent_engaged)
        if not drove:
            error = f"the hpc-bridge MCP server failed: {type(exc).__name__}: {exc}"
            tb = traceback.format_exc()
    if follower is not None:
        stop.set()   # a hook acting now finishes first; the follower reads the journal once more, then returns
        try:
            await follower
        except Exception as exc:  # noqa: BLE001 - the hooks it never acted on read NEVER FIRED, which gates the cell
            print(f"  ⚠ journal hooks stopped: {type(exc).__name__}: {exc}", flush=True)
    final_text = text or error or ""
    print(f"  ← {OPERATOR}: {len(ops.steps)} step(s) in {time.monotonic() - t0:.0f}s; "
          f"{'ERROR: ' + error if error else 'final: ' + repr(final_text[:240])}", flush=True)
    rows = _journal_rows(journal)
    several = len({s.server for s in ops.steps}) > 1
    events = [Native(s.tool, s.args, s.result, hpcb=True, call_id=f"step-{s.n}", server=s.server if several else "")
              for s in ops.steps]
    texts, text_at = (ops.texts, ops.text_at) if ops.texts else (([final_text], [len(events)]) if final_text else ([], []))
    if journal_kept:
        trace, stats = build_trace(rows, events, texts, text_at)
        if several:
            label_servers(trace, events)
    else:
        trace, stats = steps_trace(ops.steps, texts, text_at)
    print(f"  trace: {len(trace.calls)} calls {stats}", flush=True)
    # The run dir dies with the jail: the steps (with their timestamps), the script's words and the journal ride in the
    # bundle — the cli_runner layout, which regrade.py and infra.py already read (the head row is `harness` + `rc`).
    # the server's WHOLE command line (it was cut to 8 words, which dropped the older release's path behind the wrapper's
    # own: nothing in the bundle said which server ran — the review of 2026-10-30); run.py adds what that server is
    messages = [{"harness": OPERATOR, "server": list(cmd), "rc": 1 if error else 0,
                 "seconds": round(time.monotonic() - t0, 1), "trace_stats": stats, "stdout_tail": "",
                 "stderr_tail": (tb or error or "")[-4000:],
                 **({"restarts": [r for o in ops.servers for r in o.restarts]} if any(o.restarts for o in ops.servers)
                    else {}),
                 **({"servers": [o.label for o in ops.servers]} if len(ops.servers) > 1 else {})},
                *({"native": {**_bundle_native(e), "step": s.n, "t_start": s.t_start, "t_end": s.t_end}}
                  for s, e in zip(ops.steps, events, strict=True)),
                *({"text": t, "at": a} for t, a in zip(ops.texts, ops.text_at, strict=True)), {"final": final_text},
                *({"journal": r} for r in rows)]
    return RunResult(trace=trace, final=CliFinal(result=final_text, is_error=error is not None), messages=messages,
                     hooks_fired=(fired + watcher.unfired_records()) if midrun_hooks else None)
