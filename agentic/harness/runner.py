"""Per-scenario runner: drive a headless agent over hpc-bridge, capture the Trace.

Registers the hpc-bridge MCP stdio server via the Agent SDK under the key
``endpoint`` (tools surface as ``mcp__endpoint__*``, matching the .mcp.json rename),
injecting **scoped** test credentials via the server's own ``env`` — never the admin
key. Runs the scripted prompt non-interactively (``bypassPermissions`` — the
disposable container IS the sandbox), then returns the normalised Trace + the
ResultMessage for grading by ``invariants.py`` (+ the judge).

Credentials come from THIS process's environment, which in the container holds only
the scoped test key + test Globus identity. See Plan B (runtime sandbox).

Requires ``claude-agent-sdk`` (not a hpc-bridge dependency — installed only in the
agentic harness image). The hermetic ``pytest -q`` never imports this module
(testpaths = ["tests"]).
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from claude_agent_sdk import (  # type: ignore[import-not-found]
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    query,
)

# MAX_PROSE_FOLLOWUPS: used by the prose loop below, re-exported to hermes_runner
from human_sim import MAX_PROSE_FOLLOWUPS, HumanSim, ends_with_question
from invariants import Trace, logical_name
from trace_adapter import _result_to_dict, build_trace, insert_interjections

# Logical hpc-bridge tool names (see Modules/server.md). Registered under SDK key
# "endpoint" -> the agent sees them as mcp__endpoint__<tool>.
HPC_BRIDGE_TOOLS = (
    "list_facilities",
    "connect_facility",
    "ensure_endpoint_up",
    "run_shell",
    "poll_task",          # the #21 item-2 handle path (long_task_via_handle)
    "reset_session",
    "stop_endpoint",
    "teardown_endpoint",  # the rare full pull-down (SKILL.md: only on an explicit ask / a real wedge)
    "login_shell",
)

# Scoped creds the harness injects into the MCP server's env. The first three are
# required (fail fast if the container didn't inject them); the rest are optional.
# HPC_BRIDGE_SEARCH_INDEX is the Globus Search catalog: passed through ONLY when the host sets it
# (run_smoke.sh forwards it) — the mounted storage.db must then already hold the search scope
# (granted once via `hpc-bridge-catalog`); unset, the suite stays on the BYO/discovery path.
_REQUIRED_ENV = ("HPC_BRIDGE_USER_DIR", "HPC_BRIDGE_SSH_USER", "HPC_BRIDGE_SSH_KEY")
# MAX_PROSE_FOLLOWUPS (the prose-answer cap) lives in human_sim — shared with the hermes/ACP driver's turn policy.
# Passed EXPLICITLY to the MCP server. The CLI spawns the server with its own env layered UNDER this dict, so
# the server also inherits the jail's environment (run_smoke.sh has relied on that for GLOBUS_COMPUTE_USER_DIR
# and HPC_BRIDGE_ENDPOINT_NAME since the first harness commit). Naming them here makes the dependency visible
# and recorded (review 2026-09-05): what the server gets is inherited-plus-overridden, not this allowlist.
# The human-sim's authenticator secret (fake `totp` profile). Read at IMPORT — before _scrubbed_agent_env strips every
# HPCB_* knob for the agent's lifetime — and handed to HumanSim only; the MCP server's env never carries it.
_SIM_TOTP_SECRET = os.environ.get("HPCB_SIM_TOTP_SECRET") or None

_OPTIONAL_ENV = ("HPC_BRIDGE_SSH_HOST", "HPC_BRIDGE_MACHINE", "HPC_BRIDGE_SEARCH_INDEX", "HPC_BRIDGE_CATALOG_FILE",
                 "HPC_BRIDGE_ENDPOINT_NAME", "GLOBUS_COMPUTE_USER_DIR", "HPC_BRIDGE_STATE_DIR")
# Harness plumbing the AGENT under test has no business seeing: the CLI inherits this process's env and hands it
# to the agent's Bash, so these are removed from os.environ for the client's lifetime (the SDK layers
# options.env OVER the inherited env; it cannot remove keys). Kept: the auth token (the CLI needs it) — so a
# Bash `env` still shows it; the REPORTED `no_harness_introspection` grader makes that visible.
_SCRUB_PREFIXES = ("HPCB_",)
_SCRUB_KEYS = ("PYTHONPATH",)


@dataclass
class RunResult:
    trace: Trace
    final: Any            # the SDK ResultMessage (cost, is_error, session_id, ...)
    messages: list[Any]   # raw messages, kept for the LLM-judge / debugging
    dialogue: list[Any] = None  # interactive mode: the human-sim's Q&A Exchanges
    prose_followups: int = 0          # interactive: prose questions the sim answered (client.query follow-ups)
    followups_capped: bool = False    # the run ended because MAX_PROSE_FOLLOWUPS was hit — the agent kept asking
    nudges: int = 0                   # ACP driver: mid-task pauses the sim told the operator to carry on from
    nudges_capped: bool = False       # the nudge budget ran out — the operator kept pausing without finishing (diagnostic)
    human_sim_model: str | None = None  # interactive: which model played the user (bundle provenance)
    hooks_fired: list[dict] = None    # chaos: the MIDRUN_HOOKS that fired (tool, nth, call index, rc, output)
    interjections: list[dict] = None  # `interject` hooks: what the USER said mid-run, and after which call (stamped into the trace)
    acp_events: list[dict] = None     # ACP operators: the client's event log (persisted as acp-updates.jsonl; cross-checked)


class HookWatcher:
    """Chaos hooks: a scenario's MIDRUN_HOOKS fire at a chosen point in the agent's tool sequence — after the Nth
    result of a given tool (optionally only when the call's input matches, e.g. shape="compute", or the result's
    phase matches) — and run a command on the cluster while the agent is mid-task. The harness had no channel into
    the world between SETUP and POSTCHECKS, so a dead endpoint under a polled task, a stop that must come back
    `draining`, or a stop while a task runs could never be provoked on purpose (review 2026-09-05, N6–N8).

    Pure over the message stream: `observe(msg)` returns the hooks due after this message. The runner awaits the
    executor for each; the fake cluster is where these run (a hook kills things)."""

    def __init__(self, hooks: list[dict] | None) -> None:
        self.hooks = [dict(h) for h in (hooks or [])]
        self._uses: dict[str, tuple[str, dict]] = {}   # tool_use_id -> (logical name, input)
        self._counts: dict[str, int] = {}
        self._fired: set[int] = set()
        self.calls = 0                                  # tool_use blocks seen (the trace's call index)
        self.endpoint_ids: list[str] = []

    def observe(self, msg: Any) -> list[dict]:
        due: list[dict] = []
        content = getattr(msg, "content", None)
        if not isinstance(content, list):
            return due
        for b in content:
            if hasattr(b, "name") and hasattr(b, "input") and hasattr(b, "id"):
                self._uses[b.id] = (logical_name(getattr(b, "name", "") or ""), dict(getattr(b, "input", {}) or {}))
                self.calls += 1
            elif hasattr(b, "tool_use_id"):
                use = self._uses.get(b.tool_use_id)
                if use is None:
                    continue
                name, inp = use
                result = _result_to_dict(getattr(b, "content", None)) or {}
                eid = str(result.get("endpoint_id") or "")
                if eid and eid not in self.endpoint_ids:
                    self.endpoint_ids.append(eid)
                for k, h in enumerate(self.hooks):
                    if k in self._fired or h.get("after_tool") != name:
                        continue
                    want = h.get("when_input") or {}
                    if any(str(inp.get(kk)).lower() != str(vv).lower() for kk, vv in want.items()):  # True/"true" alike
                        continue
                    if h.get("when_phase") and str(result.get("phase")) != str(h["when_phase"]):
                        continue
                    self._counts[k] = self._counts.get(k, 0) + 1
                    if self._counts[k] == int(h.get("nth", 1)):
                        self._fired.add(k)
                        due.append({**h, "index": k, "call_index": self.calls - 1, "result": result,
                                    "endpoint_ids": list(self.endpoint_ids)})
        return due

    @property
    def unfired(self) -> list[dict]:
        return [h for k, h in enumerate(self.hooks) if k not in self._fired]


@dataclass
class _AbortedResult:
    """Stand-in `final` when the SDK stream dies mid-run (max_turns / budget / transport error)
    instead of yielding a ResultMessage. `is_error=True` routes it to run.py's completion gate as a
    hard FAIL while the PARTIAL trace is still graded + recorded — an overrun must not crash the
    harness or vanish without evidence. `result` carries the reason (the rate-limit sniff reads it).
    Downstream reads every ResultMessage attr via getattr(default=None), so absent fields are fine."""
    result: str
    is_error: bool = True


def _server_env() -> dict[str, str]:
    missing = [k for k in _REQUIRED_ENV if k not in os.environ]
    if missing:
        raise RuntimeError(
            f"agentic harness: missing scoped credential env {missing} — the container "
            "must inject the test SSH user/key + a fresh HPC_BRIDGE_USER_DIR (never the admin key)."
        )
    env = {k: os.environ[k] for k in _REQUIRED_ENV}
    env.update({k: os.environ[k] for k in _OPTIONAL_ENV if k in os.environ})
    return env


@contextlib.contextmanager
def _scrubbed_agent_env():
    """Temporarily remove harness plumbing from os.environ while the CLI (and so the agent's shell) is alive.
    Restored afterwards: run.py reads HPCB_* for the bundle and teardown after run_scenario returns."""
    saved = {k: v for k, v in os.environ.items() if k.startswith(_SCRUB_PREFIXES) or k in _SCRUB_KEYS}
    for k in saved:
        del os.environ[k]
    try:
        yield sorted(saved)
    finally:
        os.environ.update(saved)


def _mcp_servers(repo_root: Path, extra_env: dict[str, str] | None = None) -> dict:
    # Direct registration (not as a plugin) — matches the .mcp.json launch command
    # (the trailing "hpc-bridge" is the console script, unchanged by the server rename).
    return {
        "endpoint": {
            "type": "stdio",
            "command": "uv",
            "args": ["run", "--directory", str(repo_root), "--extra", "integration", "hpc-bridge"],
            # extra_env: per-scenario overrides for the SERVER only (e.g. a bogus SSH login name to
            # play a stranger) — the harness process keeps its own pool credentials for postchecks/teardown.
            "env": {**_server_env(), **(extra_env or {})},
        }
    }


def _system_prompt(repo_root: Path, interactive: bool, include_skill: bool = True) -> str:
    # First cut: inject the driving-hpc skill as standing guidance. (Faithful on-demand
    # Skill-tool loading is a later refinement; here we test whether the guidance CONTENT
    # drives the right behaviour.)
    if interactive:
        lead = (
            "You are driving real HPC through the hpc-bridge tools on behalf of a user. "
            "The user is present and answers questions via the AskUserQuestion tool — use it "
            "for the consequential choices"
        )
        lead += " exactly as the guidance instructs. " if include_skill else ". "
    else:
        lead = (
            "You are driving real HPC through the hpc-bridge tools in an automated test. "
            "There is no human to answer follow-up questions — act on the instructions you "
            "are given. "
        )
    if not include_skill:
        # Skill ablation: the agent gets only the tools' own descriptions — the measured
        # pass-rate delta vs baseline IS the causal value of SKILL.md.
        return lead + "Use the tools' own descriptions to decide how to proceed."
    skill = (repo_root / "skills" / "driving-hpc" / "SKILL.md").read_text()
    return lead + "Follow this operational guidance:\n\n" + skill


def _live(msg: Any) -> None:
    """Stream a one-line marker per tool call as the agent works — to stderr, so progress
    interleaves with container logs while the final structured trace goes to stdout."""
    content = getattr(msg, "content", None)
    if not isinstance(content, list):
        return
    for b in content:
        if hasattr(b, "name") and hasattr(b, "input") and hasattr(b, "id"):  # ToolUseBlock
            inp = json.dumps(getattr(b, "input", {}) or {}, default=str)
            if len(inp) > 90:
                inp = inp[:89] + "…"
            print(f"  → {logical_name(getattr(b, 'name', '') or '')}({inp})", file=sys.stderr, flush=True)


async def run_scenario(
    prompt: str,
    *,
    repo_root: Path,
    model: str = "claude-opus-4-8",
    effort: str | None = None,
    persona: str | None = None,
    user_goal: str = "",
    ablate_skill: bool = False,
    max_turns: int = 40,
    max_budget_usd: float = 2.0,
    extra_env: dict[str, str] | None = None,
    midrun_hooks: list[dict] | None = None,
    hook_runner=None,
) -> RunResult:
    """Run one scripted scenario end-to-end and return the captured Trace + result.

    Two modes:
    - autonomous (persona=None): one-shot `query()` under bypassPermissions — the prompt
      pre-authorises everything (no human exists).
    - interactive (persona set): a simulated human ([[human_sim]]) answers the agent's REAL
      AskUserQuestion calls. Mechanics (spike-proven): `permission_mode="default"` with
      everything pre-allowed EXCEPT AskUserQuestion, so it — and only it — falls through to
      `can_use_tool`, which injects the human-sim's answers via `updated_input`. The
      callback needs the streaming control channel, hence ClaudeSDKClient.
    """
    interactive = persona is not None
    opts: dict[str, Any] = dict(
        model=model,
        allowed_tools=[f"mcp__endpoint__{t}" for t in HPC_BRIDGE_TOOLS] + ["Bash", "Read", "Write"],
        mcp_servers=_mcp_servers(repo_root, extra_env),
        system_prompt=_system_prompt(repo_root, interactive, include_skill=not ablate_skill),
        setting_sources=[],  # SDK isolation: ignore host ~/.claude + project settings
        cwd=str(repo_root),
        max_turns=max_turns,            # safety rail: bound a runaway agent
        max_budget_usd=max_budget_usd,  # safety rail: bound cost per scenario
    )
    if effort:
        # Reasoning level (low..max): effort guides adaptive thinking DEPTH, so pair it with
        # adaptive thinking to make the level bite. Unset ⇒ the model's default effort.
        opts["effort"] = effort
        opts["thinking"] = {"type": "adaptive"}

    human: HumanSim | None = None
    injected_answers: dict[str, dict[str, str]] = {}  # tool_use_id -> answers (structural record)
    if interactive:
        human = HumanSim(persona=persona, goal=user_goal, totp_secret=_SIM_TOTP_SECRET)

        async def _gatekeeper(tool_name: str, tool_input: dict, ctx: Any):
            if tool_name == "AskUserQuestion":
                print(f"  ? gate: {[q.get('question') for q in tool_input.get('questions', [])]}",
                      file=sys.stderr, flush=True)
                answers = await human.answer(tool_input)
                print(f"  ! human({persona}): {answers}", file=sys.stderr, flush=True)
                # Stamp the answers structurally by tool_use_id so grading never depends on
                # how the CLI renders them into result text (format drift => vacuous passes).
                tid = getattr(ctx, "tool_use_id", None)
                if tid:
                    injected_answers[tid] = answers
                return PermissionResultAllow(updated_input={**tool_input, "answers": answers})
            # Anything else that falls through (not in allowed_tools) is fine in the jail.
            return PermissionResultAllow()

        opts["permission_mode"] = "default"   # bypass would skip can_use_tool entirely
        opts["can_use_tool"] = _gatekeeper
    else:
        opts["permission_mode"] = "bypassPermissions"  # the disposable container is the sandbox

    options = ClaudeAgentOptions(**opts)
    messages: list[Any] = []
    final: Any = None
    turn_final: Any = None   # the CURRENT turn's ResultMessage — never a previous turn's (review 2026-09-05, 2.4)
    followups = 0
    capped = False
    watcher = HookWatcher(midrun_hooks)
    fired: list[dict] = []
    interjections: list[dict] = []   # `interject` hooks that fired: what the user said, after which call
    pending: list[str] = []          # an interjection to send once the interrupted turn has ended
    live: dict[str, Any] = {}        # the ClaudeSDKClient while an interactive run is alive (interject hooks need it)

    async def _observe(msg: Any) -> None:
        for hook in watcher.observe(msg):
            label = hook.get("name", hook["after_tool"])
            print(f"  💥 hook: {label} after {hook['after_tool']} #{hook.get('nth', 1)} (call {hook['call_index']})",
                  file=sys.stderr, flush=True)
            if "interject" in hook:
                # The USER interrupts (Esc in Claude Code) and speaks. The SDK interrupt rejects the in-flight tool use and
                # ends the turn with an error-subtype result (live-verified 2026-09-06); the interactive loop below then
                # sends the text as the next turn of the SAME session. Only a run with a user (a persona) can interject.
                client = live.get("client")
                if client is None:
                    outcome = {"rc": 1, "out": "interject hook needs an interactive run (a persona): an autonomous run has no user to interrupt"}
                else:
                    text = str(hook["interject"])
                    await client.interrupt()
                    pending.append(text)
                    interjections.append({"name": label, "text": text, "call_index": hook["call_index"]})
                    outcome = {"rc": 0, "out": "interrupted the turn; the user's message follows it", "interject": text[:300]}
            else:
                outcome = await hook_runner(hook) if hook_runner else {"rc": None, "out": "(no hook runner)"}
            fired.append({"name": label, "after_tool": hook["after_tool"],
                          "nth": int(hook.get("nth", 1)), "call_index": hook["call_index"], **(outcome or {})})
    try:
        with _scrubbed_agent_env():
            if interactive:
                async with ClaudeSDKClient(options=options) as client:
                    live["client"] = client
                    await client.query(prompt)
                    while True:
                        last_text, last_had_tool = "", False
                        turn_final = None
                        async for msg in client.receive_response():
                            messages.append(msg)
                            _live(msg)
                            await _observe(msg)
                            kind = type(msg).__name__
                            if kind == "ResultMessage":
                                turn_final = msg
                            elif kind == "AssistantMessage":
                                content = getattr(msg, "content", None) or []
                                last_had_tool = any(hasattr(b, "name") and hasattr(b, "input") for b in content)
                                last_text = "".join(getattr(b, "text", "") or "" for b in content)
                        if turn_final is None:
                            # the stream ended (EOF, CLI death) without this turn's ResultMessage: the run is
                            # TRUNCATED. The previous turn's result must not stand in for it and pass the gate.
                            final = _AbortedResult(result="stream ended without a ResultMessage for this turn")
                            break
                        final = turn_final
                        if pending:
                            # the turn was cut by an interject hook's interrupt (its error-subtype result is expected and
                            # is not the run's verdict): the user now speaks, and the same session continues
                            text = pending.pop(0)
                            print(f"  ✋ user({persona}) INTERJECTS: {text[:160]}", file=sys.stderr, flush=True)
                            await client.query(text)
                            continue
                        # The turn ended on a text-only question (no tool call): a real user would answer in
                        # chat, so the sim does — one more `query` continues the SAME session (SDK multi-turn).
                        if last_had_tool or getattr(final, "is_error", False) or not ends_with_question(last_text):
                            break
                        if followups == MAX_PROSE_FOLLOWUPS:
                            capped = True  # the agent is still asking in prose; recorded, not hidden
                            break
                        reply = await human.reply(last_text)
                        followups += 1
                        print(f"  ! human({persona}) answers a PROSE question: {reply[:160]}", file=sys.stderr, flush=True)
                        await client.query(reply)
                live.pop("client", None)
            else:
                async for msg in query(prompt=prompt, options=options):
                    messages.append(msg)
                    _live(msg)
                    await _observe(msg)
                    if type(msg).__name__ == "ResultMessage":
                        final = msg
    except Exception as e:  # noqa: BLE001 — max_turns / budget / transport death mid-stream
        # The SDK RAISES (e.g. "Reached maximum number of turns") rather than yielding a final
        # ResultMessage. Swallow it here so the PARTIAL trace collected so far is returned, graded,
        # and recorded — an overrun becomes a graceful FAIL (run_completed), not a lost bundle plus a
        # top-level traceback. A ResultMessage that arrived for the turn that died stands; a PREVIOUS
        # turn's must not (the interactive loop already reset `turn_final`; `final` may be stale).
        print(f"  ⚠ run aborted mid-stream: {e}", file=sys.stderr, flush=True)
        if (interactive and turn_final is None) or final is None:
            final = _AbortedResult(result=str(e))
    return RunResult(
        trace=insert_interjections(build_trace(messages, injected_answers=injected_answers), interjections),
        final=final, messages=messages,
        dialogue=(human.dialogue if human else []),
        prose_followups=followups, followups_capped=capped,
        human_sim_model=(human.model if human else None),
        interjections=interjections,
        hooks_fired=fired + [{"name": h.get("name", h["after_tool"]), "after_tool": h["after_tool"],
                              "nth": int(h.get("nth", 1)), "call_index": None, "rc": None, "out": "NEVER FIRED"}
                             for h in watcher.unfired],
    )
