"""Mid-run hooks (a scenario's MIDRUN_HOOKS): act on the world, or on the agent's session, at a chosen point of a run.

The harness has no other channel into a run between SETUP and POSTCHECKS, so a dead endpoint under a polled task, a
stop that must come back `draining`, a reaped block or an Esc in the middle of a tool call could never be provoked on
purpose (review 2026-09-05, N6–N8; itinerary 2026-10-13, §2.1).

A hook has ONE trigger:
- `after_tool` — fires after the Nth result of that tool (`nth`, default 1), optionally only when the call's input
  matches (`when_input`), the result's phase does (`when_phase`), the result names an endpoint (`when_endpoint`), or
  another hook has already fired (`after_hook`).
- `on_tool` — a DURING hook: fires `after_s` seconds after a matching call STARTED, if that call is still in flight
  then (`nth` counts the matching calls that were). This is how a scenario presses Esc in the middle of a call.

`when_input` is a dict (each key equal to the call's EFFECTIVE argument, case-insensitively: True and "true" alike) or a
string (a regex searched in the argument values the call sent, e.g. a marker in the command). Effective: an argument
the call left out takes the tool's own default (`tool_defaults`, read from hpc-bridge's source), so `{"shape":
"compute"}` matches a `run_shell` that sends only its command — the product runs it on the compute shape. hermes21
sent every run_shell as `{"command": "hostname"}` (draining_restop, live 2026-10-16) and the freeze, matched against
the arguments as sent, NEVER FIRED. A regex reads only what the call sent: a default is no marker the agent wrote.

A call that does not match is not counted, so `nth` counts only the calls a hook is about. That matters because a
model may open with a blind parallel batch before anything is connected — hermes21 on gpt-oss, live 2026-10-15:
`ensure_endpoint_up` → down, `run_shell` → failed, `poll_task(task_id="PLACEHOLDER")` → failed, `stop_endpoint` → "no
endpoint was up". `orphaned_task`'s "after poll_task #1" fired on that placeholder poll, with no manager to stop, and
the cell never met its orphan. Each answer is a no-op the result itself shows, and both feeds below see the result:
- `when_phase` — a run_shell / poll_task / reset_session answer's `phase`: `"running"` is a task that really runs (a
  placeholder poll answers `failed`), `"complete"` a command that ran (one before any facility answers `failed`, one
  refused for spend `needs_confirmation`). For a hook whose premise IS that answer;
- `when_endpoint: True` — an ensure_endpoint_up / stop_endpoint / teardown_endpoint answer that names the endpoint it
  acted on (`endpoint_id`). Made while nothing was connected, they answer `endpoint_id: null` ("no endpoint was up",
  "No facility connected yet"). A real answer names it whenever the server held one — not always: a confirm whose
  endpoint configure failed ("hpc-bridge error: … configure failed") carries none, and such a call simply does not
  count (the next one may). For a hook that must see the real call whatever it answered (evidence at a stop, a lying
  `down` included);
- `after_hook: "<name>"` — only calls that END after the named hook of the same scenario fired: a hook that undoes
  another. draining_restop's thaw, "after stop_endpoint #1", fired on a stop hermes21 made before its compute runs
  were done, before the freeze; the freeze then stayed, and the real stops met a frozen worker (`draining` three
  times, the cell failed — live 2026-10-15).
They read the call's RESULT (or the hooks fired before it), so they apply to an `after_tool` hook only — and only to
a tool whose answer carries the field: `when_phase` on the tools that answer a `phase` (with one of its values),
`when_endpoint` on the three that answer `status` + `endpoint_id` (`ANSWER_PHASES`, `ENDPOINT_ANSWERS`; a hook that
could never fire is refused by `hook_problems`, review of e126f41).

And one or more ACTIONS — what the hook carries:
- `cmd` — a shell command run on the cluster as the pool user (run.py `_run_hook`; `"on": "each_login"` fans it out).
  `expect_any_host` (a substring) makes it report its REAL outcome: the hook fails unless some node printed it.
- `poll` (with a `cmd`) — a POLLING hook: once its trigger fires, run.py runs the `poll` command on the cluster every
  `every_s` seconds (default 5) until its output matches the regex `until`, then runs the `cmd`; past `poll_timeout_s`
  (default 900) the hook fails with the last output, and the `cmd` never runs. Mid-run hooks fire only on tool results,
  and an agent may sit in one long call (`status(wait_s=60)`) while the world it waits on changes — the poll is what
  times a hook by the world ("after 5 results": count the result files), not by the next call (batch phase 1,
  `batch_block_reaped_midrun`). `poll`, `cmd` and `until` may name the trigger call's result fields as
  `{result:<key>}` (e.g. `{result:results_dir}`), beside `{eid}` and `{endpoint_name}`.
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
- `script` (no model: the scenario's own SCRIPT makes the calls, script_runner) — `cmd` after a tool result (polling
  ones too), fed from hpc-bridge's journal as for the CLI operators, so one scenario's hook serves a scripted and an
  agent cell alike; no persona, no DURING hook, no interject.

Pure and SDK-free (cli_runner and the hermetic tests import it; runner.py re-exports HookWatcher).
"""
from __future__ import annotations

import ast
import asyncio
import functools
import json
import math
import operator
import re
import sys
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path
from typing import Any

from invariants import ToolCall, logical_name
from trace_adapter import _result_to_dict

# hpc-bridge's tools, as the product defines them: the `@mcp.tool` functions of its server. Read from the source, not
# imported — the harness' interpreter in the jail has no Globus Compute SDK, which importing the server pulls in (the
# repo is in the jail: the server runs from it). test_midrun_hooks pins what this reads to the server's MCP schema.
_SERVER_SRC = Path(__file__).resolve().parents[2] / "src" / "hpc_bridge" / "server.py"
_LITERAL_ERRORS = (ValueError, TypeError, SyntaxError, MemoryError, RecursionError)
_ARITHMETIC: dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv, ast.Mod: operator.mod, ast.Pow: operator.pow}
_MAX_HOPS = 8   # constant → constant → module: how far a default is followed before it is called unresolved


class _Unresolved(Exception):
    """A default the source does not state as a literal, arithmetic on literals, or a module constant."""


def _warn(msg: str) -> None:
    print(f"midrun_hooks: {msg}", file=sys.stderr, flush=True)


def _package_module(path: Path, module: str | None, level: int) -> Path | None:
    """The source file of `from <module> import …` in `path`: relative (`level` dots), or absolute inside the same
    package (`hpc_bridge.context`); None for a module outside it (another distribution's: never read), and for
    `from . import binding` (a submodule, not a constant)."""
    if not module:
        return None
    if level:
        base = path.parent
        for _ in range(level - 1):
            base = base.parent
        parts = module.split(".")
    else:
        top = path.parent
        while (top.parent / "__init__.py").exists():
            top = top.parent
        parts = module.split(".")
        if parts[0] != top.name:
            return None
        base, parts = top, parts[1:]
    target = base.joinpath(*parts)
    for src in (target.parent / f"{target.name}.py", target / "__init__.py"):
        if src.exists():
            return src
    return None


@functools.cache
def _module_names(path: Path) -> tuple[dict[str, ast.expr], dict[str, tuple[Path, str]]]:
    """A module's top-level `NAME = <expr>` (the expression, resolved when asked for), and the names it imports from
    its own package: `from .context import DEFAULT_SHAPE as SHAPE` → {"SHAPE": (<…>/context.py, "DEFAULT_SHAPE")}."""
    assigned: dict[str, ast.expr] = {}
    imported: dict[str, tuple[Path, str]] = {}
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for t in targets:
                if isinstance(t, ast.Name):
                    assigned[t.id] = node.value
        elif isinstance(node, ast.ImportFrom):
            src = _package_module(path, node.module, node.level)
            for alias in node.names if src is not None else ():
                imported[alias.asname or alias.name] = (src, alias.name)
    return assigned, imported


def _value(node: ast.expr, path: Path, hops: int = 0) -> Any:
    """What a default expression in `path` evaluates to, read from the source: a literal, arithmetic on literals
    (`2 * 60`), or a module constant — this module's, or one it imports from its own package, under any alias
    (`DEFAULT_SHAPE`, which server.py imports from context.py). Raises _Unresolved for anything else."""
    if hops > _MAX_HOPS:
        raise _Unresolved("too many hops")
    try:
        return ast.literal_eval(node)
    except _LITERAL_ERRORS:
        pass
    if isinstance(node, ast.BinOp) and type(node.op) in _ARITHMETIC:
        return _ARITHMETIC[type(node.op)](_value(node.left, path, hops + 1), _value(node.right, path, hops + 1))
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        v = _value(node.operand, path, hops + 1)
        return -v if isinstance(node.op, ast.USub) else +v
    if isinstance(node, ast.Name):
        assigned, imported = _module_names(path)
        if node.id in assigned:
            return _value(assigned[node.id], path, hops + 1)
        if node.id in imported:
            src, original = imported[node.id]
            return _value(ast.Name(id=original), src, hops + 1)
    raise _Unresolved(ast.unparse(node))


def _is_tool_decorator(d: ast.expr) -> bool:
    f = d.func if isinstance(d, ast.Call) else d
    return isinstance(f, ast.Attribute) and f.attr == "tool" and isinstance(f.value, ast.Name) and f.value.id == "mcp"


def _tool_name(deco: ast.expr, fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """The tool's name: `@mcp.tool("x")` or `@mcp.tool(name="x")` (FastMCP's first parameter), else the function's."""
    if isinstance(deco, ast.Call):
        given = deco.args[0] if deco.args else next((k.value for k in deco.keywords if k.arg == "name"), None)
        if isinstance(given, ast.Constant) and isinstance(given.value, str):
            return given.value
    return fn.name


def _read_tool_defaults(server_src: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for node in ast.parse(server_src.read_text()).body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        deco = next((d for d in node.decorator_list if _is_tool_decorator(d)), None)
        if deco is None:
            continue
        name = _tool_name(deco, node)
        a = node.args
        positional = [*a.posonlyargs, *a.args]
        pairs = [*zip(positional[len(positional) - len(a.defaults):], a.defaults, strict=True),
                 *((k, d) for k, d in zip(a.kwonlyargs, a.kw_defaults, strict=True) if d is not None)]
        out[name] = {}
        for arg, default in pairs:
            try:
                out[name][arg.arg] = _value(default, server_src)
            except Exception as exc:  # noqa: BLE001 - one default the reader cannot follow leaves only that one out
                why = str(exc) if isinstance(exc, _Unresolved) else f"{type(exc).__name__}: {exc}"
                _warn(f"{name}({arg.arg}=…): default not read ({why}) — when_input matches it only as sent")
    if not out:
        _warn(f"no @mcp.tool function in {server_src} — when_input matches the arguments as sent")
    return out


@functools.cache
def tool_defaults(server_src: Path = _SERVER_SRC) -> dict[str, dict[str, Any]]:
    """Each hpc-bridge tool's defaults, {tool: {argument: default}}, as its `@mcp.tool` signature in server.py states
    them — what the server takes for an argument a call leaves out (run_shell's `shape` is "compute"). An argument with
    no default is left out; so is a default the source does not state as a literal, arithmetic on literals or a
    package constant, with a warning on stderr. Never raises: a source that cannot be read gives {} and a warning, and
    `when_input` then matches the arguments as sent, as it did before — a hook watcher that raised would end the SDK
    feed's run, or stop the journal follower (every hook then NEVER FIRED)."""
    try:
        return _read_tool_defaults(server_src)
    except Exception as exc:  # noqa: BLE001 - see above: never a reason a run or its hooks fail
        _warn(f"could not read the tools' defaults from {server_src} ({type(exc).__name__}: {exc}) — when_input "
              "matches the arguments as sent")
        return {}


def effective_input(tool: str, inp: dict | None) -> dict:
    """A call's arguments with the tool's own defaults filled in for those it left out (`tool_defaults`)."""
    return {**tool_defaults().get(tool, {}), **(inp or {})}


ACTIONS = ("cmd", "interrupt", "interject")
# What each hpc-bridge tool's answer carries (server.py's return types; test_midrun_hooks pins both to the product's
# models): a `phase` with these values (ShellOutcome, ConnectResult, LoginStatus, PreauthStatus) — or, for the
# EndpointStatus tools, a `status` and an `endpoint_id` and no phase.
_SHELL_PHASES = ("complete", "cold_start", "failed", "needs_confirmation", "needs_account", "running")
_LOGIN_PHASES = ("logged_in", "needs_login", "failed")
ANSWER_PHASES: dict[str, tuple[str, ...]] = {
    "run_shell": _SHELL_PHASES, "poll_task": _SHELL_PHASES, "reset_session": _SHELL_PHASES,
    "connect_facility": ("needs_account", "provisioning", "needs_facility_details", "proposed_facility_details",
                         "needs_preauth", "needs_login", "unsupported", "failed"),
    "authenticate": _LOGIN_PHASES, "complete_login": _LOGIN_PHASES,
    "complete_preauth": ("opened", "failed", "needs_terminal"),
}
ENDPOINT_ANSWERS = ("ensure_endpoint_up", "stop_endpoint", "teardown_endpoint")
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
    "script": frozenset({"cmd"}),
}
CLI_OPERATORS = ("codex", "pi", "hermes21")
NO_USER_OPERATORS = (*CLI_OPERATORS, "script")   # nobody to answer a persona's questions: one-shot turns, or no model


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
    hooks = list(hooks or [])
    for k, h in enumerate(hooks):
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
        if "poll" in h:
            out += _poll_problems(who, h)
        elif any(key in h for key in ("until", "every_s", "poll_timeout_s")):
            out.append(f"{who}: until / every_s / poll_timeout_s belong to a polling hook (poll)")
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
        if is_during(h) and any(key in h for key in ("when_phase", "when_endpoint", "after_hook")):
            out.append(f"{who}: when_phase / when_endpoint / after_hook decide at a call's END — an after_tool hook only")
        if "when_endpoint" in h and h["when_endpoint"] is not True:
            out.append(f"{who}: when_endpoint is True or absent")
        tool = trigger_tool(h)
        if "when_endpoint" in h and tool not in ENDPOINT_ANSWERS:
            out.append(f"{who}: when_endpoint on {tool!r}, whose answer names no endpoint_id — it could never fire")
        if "when_phase" in h:
            if tool in ENDPOINT_ANSWERS:
                out.append(f"{who}: when_phase on {tool!r}, whose answer carries a status, not a phase — it could never fire")
            elif tool in ANSWER_PHASES and h["when_phase"] not in ANSWER_PHASES[tool]:
                out.append(f"{who}: when_phase {h['when_phase']!r} is no phase {tool} answers {ANSWER_PHASES[tool]}")
            elif tool not in ANSWER_PHASES:
                out.append(f"{who}: when_phase on {tool!r}, whose answer has no phase")
        if "after_hook" in h and (h["after_hook"] == hook_label(h)
                                  or h["after_hook"] not in [hook_label(o) for o in hooks if o is not h]):
            out.append(f"{who}: after_hook names no other hook of this scenario ({h['after_hook']!r})")
        want = h.get("when_input")
        if isinstance(want, str):
            try:
                re.compile(want)
            except re.error as exc:
                out.append(f"{who}: when_input is not a regex ({exc})")
        elif want is not None and not isinstance(want, dict):
            out.append(f"{who}: when_input must be a dict or a regex string")
    return out


def _poll_problems(who: str, h: dict) -> list[str]:
    out: list[str] = []
    if not str(h.get("poll") or "").strip():
        out.append(f"{who}: poll is the command to repeat (non-empty)")
    if "cmd" not in h:
        out.append(f"{who}: a polling hook acts with a cmd once its poll matches")
    if is_during(h):
        out.append(f"{who}: a polling hook is armed by a tool result (after_tool), not during a call")
    until = h.get("until")
    if not isinstance(until, str) or not until:
        out.append(f"{who}: a polling hook needs `until`, the regex its poll's output must match")
    else:
        try:
            re.compile(until)
        except re.error as exc:
            out.append(f"{who}: until is not a regex ({exc})")
    for key, low in (("every_s", 0.5), ("poll_timeout_s", 1.0)):
        if key in h:
            try:
                ok = math.isfinite(float(h[key])) and float(h[key]) >= low
            except (TypeError, ValueError):
                ok = False
            if not ok:
                out.append(f"{who}: {key} must be a number >= {low:g}")
    return out


_RESULT_FIELD = re.compile(r"\{result:([A-Za-z_][A-Za-z0-9_]*)\}")


def hook_text(hook: dict, text: str, *, endpoint_name: str = "") -> str:
    """A hook's command (`cmd` or `poll`, or its `until`) with this run's values in: `{endpoint_name}`, `{eid}` (the
    latest endpoint uuid the tool results showed) and `{result:<key>}`, the trigger call's result field (empty when the
    result lacks it — the command then sees an empty word, and a poll simply never matches)."""
    eids = hook.get("endpoint_ids") or []
    out = str(text).replace("{endpoint_name}", endpoint_name).replace("{eid}", eids[-1] if eids else "")
    result = hook.get("result") or {}

    def field(m: re.Match) -> str:
        v = result.get(m.group(1))
        return "" if v is None else str(v)

    return _RESULT_FIELD.sub(field, out)


def unsupported_hook_needs(operator: str, hooks: Iterable[dict] | None) -> set[str]:
    return hook_needs(hooks) - OPERATOR_HOOK_ACTIONS.get(operator, frozenset())


def seeded_chain(phases: Any) -> bool:
    """A chain whose FIRST phase is scripted (`{"script": …}`: run.py `_phase_spec`) — the world before the agent made by
    a script, on the plugin the user had before (the returning-user cells). The cell's own operator then runs only
    fresh one-shot sessions after it, which every CLI operator can (run.py's `_run_chain` runs each phase with the
    cell's runner); a plain prompt chain stays the Claude operator's, the only one its cells were proven under."""
    if not isinstance(phases, list | tuple) or not phases:
        return False
    first = phases[0]
    return isinstance(first, dict) and first.get("script") is not None


def operator_refusal(operator: str, *, persona: str | None, phases: Any, hooks: Iterable[dict] | None) -> str | None:
    """Why `operator` cannot drive a scenario (a SKIPPED cell, not a vacuous grade) — None when it can. `phases`: the
    scenario's PHASES (or a bool, as before: a plain chain)."""
    hooks = list(hooks or [])
    if operator in NO_USER_OPERATORS and persona:
        return "an interactive persona"
    if operator != "claude" and phases and not (operator in NO_USER_OPERATORS and seeded_chain(phases)):
        return "cross-restart chains (PHASES)"
    missing = unsupported_hook_needs(operator, hooks)
    if missing:
        return f"mid-run hooks that need {', '.join(sorted(missing))}"
    return None


def _input_matches(want: Any, inp: dict, tool: str = "") -> bool:
    """Does a call of `tool` with arguments `inp` match `when_input`? A dict compares the call's EFFECTIVE arguments
    (an argument left out is the tool's default); a regex searches the values the call sent."""
    if not want:
        return True
    if isinstance(want, dict):
        args = effective_input(tool, inp) if tool else (inp or {})
        return all(str(args.get(k)).lower() == str(v).lower() for k, v in want.items())   # True/"true" alike
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
            if _input_matches(h.get("when_input"), args, name):
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
        fired_before = {hook_label(self.hooks[j]) for j in self._fired}   # a hook due on THIS call is not "before" it
        for k, h in enumerate(self.hooks):
            if k in self._fired or is_during(h) or h.get("after_tool") != name:
                continue
            if not _input_matches(h.get("when_input"), inp, name):
                continue
            if h.get("after_hook") and h["after_hook"] not in fired_before:
                continue
            if h.get("when_phase") and str(result.get("phase")) != str(h["when_phase"]):
                continue
            if h.get("when_endpoint") and not result.get("endpoint_id"):
                continue   # a no-op answer: nothing was connected, nothing was acted on (see the module docstring)
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
            if "tool" not in row:   # hpc-bridge's own event rows (the batch engine's): no call, no hook trigger
                continue
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
