"""Mid-run hooks (a scenario's MIDRUN_HOOKS): act on the world, or on the agent's session, at a chosen point of a run.

The harness has no other channel into a run between SETUP and POSTCHECKS, so a dead endpoint under a polled task, a
stop that must come back `draining`, a reaped block or an Esc in the middle of a tool call could never be provoked on
purpose (review 2026-09-05, N6–N8; itinerary 2026-10-13, §2.1).

A hook has ONE trigger:
- `after_tool` — fires after the Nth result of that tool (`nth`, default 1), optionally only when the call's input
  matches (`when_input`) or the result's phase does (`when_phase`).
- `on_tool` — a DURING hook: fires `after_s` seconds after a matching call STARTED, if that call is still in flight
  then (`nth` counts the matching calls that were). This is how a scenario presses Esc in the middle of a call.

`when_input` is a dict (each key equal to the call's argument, case-insensitively: True and "true" alike) or a string
(a regex searched in the call's argument values, e.g. a marker in the command).

And one or more ACTIONS — what the hook carries:
- `cmd` — a shell command run on the cluster as the pool user (run.py `_run_hook`; `"on": "each_login"` fans it out).
  `expect_any_host` (a substring) makes it report its REAL outcome: the hook fails unless some node printed it.
- `world` — a world action the harness builds itself. `"stop_manager"`: stop THIS run's endpoint manager where it runs
  (cluster_ops.stop_here_cmd on every login node, each acting only if the manager is in its own `ps`); it fails unless
  some node actually stopped it. A plain `cmd` sent to a round-robin alias can land on the other node, act on the
  NFS-shared daemon.pid there and leave the manager running — while its own echo says it worked (orphaned_task,
  live 2026-10-13).
- `interject` — the user's text: the SDK interrupt (what Esc does in Claude Code), then that text as the user's next
  turn of the same session. An MCP call in flight is cancelled with it: the server got notifications/cancelled and
  kept the command tracked in the first live esc_mid_call (2026-10-13); its graders stay tolerant if it ever does not.
- `"action": "interrupt"` — the interrupt alone: Esc with nothing said after it (the run ends there).

Which operator can do what (`OPERATOR_HOOK_ACTIONS`; run.py reports a cell SKIPPED rather than grade it vacuously):
- `claude` (the Agent SDK) — everything. The interrupt needs the streaming client, so `interject` and `interrupt` need
  a run with a user (a persona); a DURING hook is timed from the ToolUseBlock's arrival on the stream.
- `codex`, `pi`, `hermes21` (one-shot CLI turns) — `cmd` / `world` after a tool result only. They are fed from hpc-bridge's own
  journal (HPC_BRIDGE_JOURNAL), which the server writes when a call ENDS, so the hook fires after a matching result;
  nothing can see a call in flight (no DURING hook), and a one-shot CLI turn cannot take a user message mid-turn (no
  `interject`, no `interrupt`).
- `hermes`, `claude-acp` (ACP) — none yet.

Pure and SDK-free (cli_runner and the hermetic tests import it; runner.py re-exports HookWatcher).
"""
from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path
from typing import Any

from invariants import ToolCall, logical_name
from trace_adapter import _result_to_dict

ACTIONS = ("cmd", "interrupt", "interject")
WORLD_ACTIONS = ("stop_manager",)   # `world` hooks: the harness builds the command (run.py _run_hook); they count as cmd
DURING_TOOL = "during_tool"   # a need, beside the actions: a trigger only a live view of call STARTS can serve

# What each operator can serve: the actions, and DURING_TOOL for a hook timed from a call's start.
OPERATOR_HOOK_ACTIONS: dict[str, frozenset[str]] = {
    "claude": frozenset({"cmd", "interrupt", "interject", DURING_TOOL}),
    "codex": frozenset({"cmd"}),
    "pi": frozenset({"cmd"}),
    "hermes21": frozenset({"cmd"}),
    "hermes": frozenset(),
    "claude-acp": frozenset(),
}
CLI_OPERATORS = ("codex", "pi", "hermes21")


def is_during(h: dict) -> bool:
    return "on_tool" in h


def trigger_tool(h: dict) -> str:
    return str(h.get("on_tool") or h.get("after_tool") or "")


def hook_label(h: dict) -> str:
    return str(h.get("name") or trigger_tool(h) or "hook")


def trigger_text(h: dict) -> str:
    """`after run_shell #2` / `during run_shell +10s` — for logs and the bundle."""
    nth = int(h.get("nth", 1))
    if is_during(h):
        return f"during {h['on_tool']} +{float(h.get('after_s', 0)):g}s" + (f" #{nth}" if nth != 1 else "")
    return f"after {h.get('after_tool')} #{nth}"


def hook_actions(h: dict) -> set[str]:
    """What a hook does: `cmd` if it carries one (or a `world` action, a command the harness builds); `interject`
    (which interrupts first); `action` adds `interrupt`."""
    acts: set[str] = set()
    if "cmd" in h or "world" in h:
        acts.add("cmd")
    if "interject" in h:
        acts |= {"interrupt", "interject"}
    a = h.get("action")
    if a:
        acts |= {a} if isinstance(a, str) else set(a)
    return acts


def hook_needs(hooks: Iterable[dict] | None) -> set[str]:
    """Everything a scenario's hooks need from the operator: their actions, plus DURING_TOOL for a during hook."""
    need: set[str] = set()
    for h in hooks or []:
        need |= hook_actions(h)
        if is_during(h):
            need.add(DURING_TOOL)
    return need


def hook_problems(hooks: Iterable[dict] | None) -> list[str]:
    """What is wrong with a scenario's hooks (empty: nothing) — test_chaos_scenarios checks every scenario with it."""
    out: list[str] = []
    for k, h in enumerate(hooks or []):
        who = f"hook {k} ({hook_label(h)})"
        if ("after_tool" in h) == ("on_tool" in h):
            out.append(f"{who}: needs exactly one trigger, after_tool or on_tool")
        acts = hook_actions(h)
        if not acts:
            out.append(f"{who}: does nothing (no cmd, interject or action)")
        if acts - set(ACTIONS):
            out.append(f"{who}: unknown action(s) {sorted(acts - set(ACTIONS))}")
        if "interject" in acts and not str(h.get("interject") or "").strip():
            out.append(f"{who}: an interject needs the user's text")
        if "world" in h and h["world"] not in WORLD_ACTIONS:
            out.append(f"{who}: unknown world action {h['world']!r} (known: {list(WORLD_ACTIONS)})")
        if "world" in h and "cmd" in h:
            out.append(f"{who}: carries both a cmd and a world action — one command per hook")
        if "expect_any_host" in h and (not str(h["expect_any_host"] or "").strip() or "cmd" not in h):
            out.append(f"{who}: expect_any_host is a non-empty substring, for a cmd hook")
        if is_during(h):
            try:
                ok = math.isfinite(float(h.get("after_s", -1))) and float(h.get("after_s", -1)) >= 0
            except (TypeError, ValueError):
                ok = False
            if not ok:
                out.append(f"{who}: a during hook needs after_s >= 0")
        elif "after_s" in h:
            out.append(f"{who}: after_s applies to a during hook (on_tool) only")
        want = h.get("when_input")
        if isinstance(want, str):
            try:
                re.compile(want)
            except re.error as exc:
                out.append(f"{who}: when_input is not a regex ({exc})")
        elif want is not None and not isinstance(want, dict):
            out.append(f"{who}: when_input must be a dict or a regex string")
    return out


def unsupported_hook_needs(operator: str, hooks: Iterable[dict] | None) -> set[str]:
    return hook_needs(hooks) - OPERATOR_HOOK_ACTIONS.get(operator, frozenset())


def operator_refusal(operator: str, *, persona: str | None, phases: bool, hooks: Iterable[dict] | None) -> str | None:
    """Why `operator` cannot drive a scenario (a SKIPPED cell, not a vacuous grade) — None when it can."""
    hooks = list(hooks or [])
    if operator in CLI_OPERATORS and persona:
        return "an interactive persona"
    if operator != "claude" and phases:
        return "cross-restart chains (PHASES)"
    missing = unsupported_hook_needs(operator, hooks)
    if missing:
        return f"mid-run hooks that need {', '.join(sorted(missing))}"
    return None


def _input_matches(want: Any, inp: dict) -> bool:
    if not want:
        return True
    if isinstance(want, dict):
        return all(str(inp.get(k)).lower() == str(v).lower() for k, v in want.items())   # True/"true" alike
    blob = "\n".join(v if isinstance(v, str) else json.dumps(v, default=str) for v in (inp or {}).values())
    return re.search(str(want), blob) is not None


def fired_record(hook: dict, outcome: dict | None) -> dict:
    """The bundle's record of a hook that fired (`events`; run.py's `harness:midrun_hooks` row reads it)."""
    rec = {"name": hook_label(hook), "trigger": trigger_text(hook), "nth": int(hook.get("nth", 1)),
           "call_index": hook.get("call_index")}
    rec["on_tool" if is_during(hook) else "after_tool"] = trigger_tool(hook)
    return {**rec, **(outcome or {})}


class HookWatcher:
    """Decides when MIDRUN_HOOKS are due, from the calls of a run. Pure: the caller acts on what it returns.

    Two feeds. `observe(msg)` reads the Claude SDK's message stream: a ToolUseBlock is a call STARTING, a
    ToolResultBlock its END. `observe_call(name, input, result)` takes a call that has already ENDED — a journal row
    (cli_runner), which hpc-bridge writes when a call returns. After-hooks are returned when due; a during hook is
    ARMED when a matching call starts (`take_armed`), and the caller asks `due_during(armed)` once `after_s` has passed.

    `call_index` is the call's index in the run's trace for the SDK feed (every tool call counts, as in the trace), and
    the journal row's ordinal (hpc-bridge calls only) for the journal feed."""

    def __init__(self, hooks: list[dict] | None) -> None:
        self.hooks = [dict(h) for h in (hooks or [])]
        self._uses: dict[str, tuple[str, dict, int]] = {}   # call id -> (logical name, input, call index)
        self._open: set[str] = set()                        # call ids started and not yet ended
        self._counts: dict[int, int] = {}
        self._fired: set[int] = set()
        self._armed: list[dict] = []
        self.calls = 0                                      # calls started (the trace's call index)
        self.endpoint_ids: list[str] = []
        self._rows = 0

    # -- the feeds -------------------------------------------------------------------------------------------------
    def call_started(self, call_id: str, raw_name: str, inp: dict | None) -> None:
        name, args, index = logical_name(raw_name or ""), dict(inp or {}), self.calls
        self.calls += 1
        self._uses[call_id] = (name, args, index)
        self._open.add(call_id)
        for k, h in enumerate(self.hooks):
            if k in self._fired or not is_during(h) or h.get("on_tool") != name:
                continue
            if _input_matches(h.get("when_input"), args):
                self._armed.append({**h, "index": k, "call_index": index, "call_id": call_id})

    def call_ended(self, call_id: str, result: dict | None) -> list[dict]:
        """The after-hooks due now that this call returned `result`."""
        self._open.discard(call_id)
        use = self._uses.get(call_id)
        if use is None:
            return []
        name, inp, index = use
        result = result or {}
        eid = str(result.get("endpoint_id") or "")
        if eid and eid not in self.endpoint_ids:
            self.endpoint_ids.append(eid)
        due: list[dict] = []
        for k, h in enumerate(self.hooks):
            if k in self._fired or is_during(h) or h.get("after_tool") != name:
                continue
            if not _input_matches(h.get("when_input"), inp):
                continue
            if h.get("when_phase") and str(result.get("phase")) != str(h["when_phase"]):
                continue
            self._counts[k] = self._counts.get(k, 0) + 1
            if self._counts[k] == int(h.get("nth", 1)):
                self._fired.add(k)
                due.append({**h, "index": k, "call_index": index, "result": result,
                            "endpoint_ids": list(self.endpoint_ids)})
        return due

    def observe(self, msg: Any) -> list[dict]:
        """The SDK stream: the after-hooks due after this message (during hooks it starts are armed)."""
        due: list[dict] = []
        content = getattr(msg, "content", None)
        if not isinstance(content, list):
            return due
        for b in content:
            if hasattr(b, "name") and hasattr(b, "input") and hasattr(b, "id"):
                self.call_started(str(b.id), getattr(b, "name", "") or "", getattr(b, "input", {}) or {})
            elif hasattr(b, "tool_use_id"):
                due += self.call_ended(str(b.tool_use_id), _result_to_dict(getattr(b, "content", None)) or {})
        return due

    def observe_call(self, call: ToolCall) -> list[dict]:
        """A call that has already ENDED (a journal row): started and ended at once — so it can fire after-hooks only."""
        cid = f"journal-{self._rows}"
        self._rows += 1
        self.call_started(cid, call.name, call.input)
        self._armed = [a for a in self._armed if a["call_id"] != cid]   # never in flight as far as anyone can see
        return self.call_ended(cid, call.result)

    # -- during hooks ----------------------------------------------------------------------------------------------
    def take_armed(self) -> list[dict]:
        armed, self._armed = self._armed, []
        return armed

    def in_flight(self, call_id: str) -> bool:
        return call_id in self._open

    def due_during(self, armed: dict) -> dict | None:
        """`after_s` after an armed call started: the hook, if that call is still in flight and it is the Nth such."""
        k = int(armed["index"])
        if k in self._fired or not self.in_flight(str(armed["call_id"])):
            return None
        self._counts[k] = self._counts.get(k, 0) + 1
        if self._counts[k] != int(armed.get("nth", 1)):
            return None
        self._fired.add(k)
        return {**armed, "endpoint_ids": list(self.endpoint_ids)}

    # -- reporting -------------------------------------------------------------------------------------------------
    @property
    def unfired(self) -> list[dict]:
        return [h for k, h in enumerate(self.hooks) if k not in self._fired]

    def unfired_records(self) -> list[dict]:
        return [fired_record(h, {"rc": None, "out": "NEVER FIRED"}) for h in self.unfired]


class JsonlTail:
    """Read a JSONL file as it grows: each `read_new()` returns the records completed since the last one. A record ends
    at LF only (as cli_runner._jsonl), and a partly written last line waits for its LF."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._pos = 0
        self._buf = b""

    def read_new(self) -> list[dict]:
        try:
            with self.path.open("rb") as fh:
                fh.seek(self._pos)
                data = fh.read()
        except FileNotFoundError:
            return []
        self._pos += len(data)
        *lines, self._buf = (self._buf + data).split(b"\n")
        out = []
        for ln in lines:
            ln = ln.strip()
            if ln.startswith(b"{"):
                try:
                    out.append(json.loads(ln))
                except ValueError:
                    continue
        return out


async def follow_journal(path: Path, watcher: HookWatcher, act: Callable[[dict], Awaitable[None]], *,
                         stop: asyncio.Event, to_call: Callable[[dict], ToolCall], poll_s: float = 0.5,
                         record: list[dict] | None = None) -> None:
    """Feed hpc-bridge's journal to `watcher` while an operator runs, and `act` on each hook as it falls due, in order
    (an action is awaited before the next row is read: a `cmd` that sleeps holds the later hooks, as in the SDK path).
    `act` records its own outcome (cli_runner's records a failure as rc 1). One that raises anyway does not end the
    follower: the failure goes into `record` (when given) and the next hooks still act — and a hook in no record at all
    is reported MISSING by run.py's `harness:midrun_hooks`. Once `stop` is set (the operator has ended) it reads the
    journal ONE last time — a row the server wrote in the last poll window is a call the agent made, and its hook acts
    as the SDK path's would — and returns."""
    tail = JsonlTail(path)

    async def feed() -> None:
        for row in tail.read_new():
            for hook in watcher.observe_call(to_call(row)):
                try:
                    await act(hook)
                except Exception as exc:  # noqa: BLE001 - a failed hook is a recorded outcome, never a dead follower
                    if record is not None:
                        record.append(fired_record(hook, {"rc": 1, "out": f"the hook failed: {type(exc).__name__}: {exc}"}))

    while not stop.is_set():
        await feed()
        try:
            await asyncio.wait_for(stop.wait(), timeout=poll_s)
        except TimeoutError:
            pass
    await feed()
