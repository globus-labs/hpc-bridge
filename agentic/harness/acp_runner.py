"""Operator: drive CLAUDE CODE over ACP — the second harness on the cross-harness benchmark's harness axis.

Zed's `@zed-industries/claude-agent-acp` wraps the Claude Agent SDK as an ACP agent (installed in the jail image,
bin `claude-agent-acp`). The SAME agent-agnostic client (`acp_client.run_session`), the SAME persona'd human-sim
turn policy (`AcpResponder` / `HumanSim.move`) and the SAME graders drive it as drive hermes — so a run here differs
from a hermes run ONLY in the harness, which is the point ([[Planned/ACP interactive benchmark driver]]).

Facts this operator is built on (verified against the published 0.23.1 build + a local probe, 2026-09-08):
- The adapter DISALLOWS `AskUserQuestion` unconditionally ("not a great way to expose this over ACP at the
  moment"), so Claude Code asks in PROSE — the same prose loop hermes uses without `clarify`. Native structured
  asks (ACP elicitation, present on the adapter's main branch) can be routed to the human-sim later.
- The ACP update stream carries no tool I/O for MCP calls, so the graded Trace comes from the CLI's own session
  transcript (`$HOME/.claude/projects/<cwd-slug>/<session>.jsonl`, `claude_transcript`) — read POST-RUN, like
  hermes' state.db — with the human-sim's prose replies stamped by prompt order (`exchanges_from_transcript`).
  The jail's entrypoint harvests the same files into the bundle.
- hpc-bridge is registered at `session/new` (`mcp_servers`), guidance arrives over MCP (`instructions` pointer +
  the resource) — the SAME channel as hermes, not the Claude Code plugin skill: the cell measures the harness
  driving an MCP server, with guidance delivery held constant. `_meta.claudeCode.options.model` pins the model
  (HPCB_CLAUDE_ACP_MODEL; the adapter's default is the CLI's default model).
- Auth: the Claude subscription token (`CLAUDE_CODE_OAUTH_TOKEN`, forwarded by run_smoke.sh) — the adapter's
  child CLI reads it; `ANTHROPIC_API_KEY` is passed EMPTY to block the precedence trap. The jail's fresh HOME has
  no `permissions.defaultMode` setting (a host `auto` mode makes the adapter's `session/new` fail).
"""
from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from claude_transcript import (
    exchanges_from_transcript,
    find_session_files,
    load_lines,
    select_operator_session,
    trace_from_transcript,
)
from hermes_runner import AcpResponder, _lead
from hermes_trace import stamp_exchanges
from human_sim import MAX_NUDGES, MAX_PROSE_FOLLOWUPS
from invariants import Trace
from runner import RunResult

ADAPTER_BIN = "claude-agent-acp"
# Harness plumbing the operator's child must not see; the Claude auth token is KEPT (the adapter's CLI needs it).
_SCRUB_PREFIXES = ("HPCB_",)
_SCRUB_KEYS = ("PYTHONPATH", "ANTHROPIC_API_KEY")


@dataclass
class AcpFinal:
    """Stand-in `final` for an ACP-driven run (run.py reads is_error / result / session_id via getattr)."""
    result: str
    is_error: bool = False
    total_cost_usd: float | None = None
    session_id: str | None = None
    api_error_status: int | None = None


def _child_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(_SCRUB_PREFIXES) and k not in _SCRUB_KEYS}
    env["ANTHROPIC_API_KEY"] = ""          # the subscription token must win (precedence trap)
    return env


def session_meta(model: str | None) -> dict | None:
    """`session/new` `_meta` for the adapter: pins the SDK model when one is requested."""
    return {"claudeCode": {"options": {"model": model}}} if model else None


def claude_projects_dir() -> Path:
    return Path(os.environ.get("CLAUDE_CONFIG_DIR", "").strip() or (Path.home() / ".claude")) / "projects"


def assemble(projects: Path, task: str, replies: list[dict]) -> tuple[Trace, list[dict], Path | None]:
    """POST-RUN: the operator's transcript → Trace, with the human-sim's replies stamped as synthetic
    AskUserQuestion calls (nudges as `user_nudge`). Returns (trace, transcript lines, source file)."""
    src = select_operator_session(find_session_files(projects), task=task) if projects.is_dir() else None
    if src is None:
        return Trace([], []), [], None
    lines = load_lines(src)
    trace = stamp_exchanges(trace_from_transcript(lines), exchanges_from_transcript(lines, replies))
    return trace, lines, src


async def run_scenario(
    prompt: str,
    *,
    repo_root: Path,
    model: str = "default",
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
    if midrun_hooks:
        raise NotImplementedError("the claude-acp operator does not support mid-run chaos hooks yet")
    import acp_client  # jail-only (needs agent-client-protocol)

    repo = str(repo_root)
    server_env = dict(os.environ)
    if extra_env:
        server_env.update({k: v for k, v in extra_env.items() if v is not None})
    if ablate_skill:
        server_env["HPC_BRIDGE_OMIT_INSTRUCTIONS"] = "1"     # the guidance ablation: no pointer, no resource nudge
    mcp = [acp_client.hpc_bridge_mcp(repo, server_env)]
    interactive = persona is not None
    full_prompt = _lead(ablate_skill, interactive=interactive) + "\n\n" + prompt
    model_id = os.environ.get("HPCB_CLAUDE_ACP_MODEL", "").strip() or (model if model and model != "default" else None)

    human = None
    respond = None
    if interactive:
        from human_sim import HumanSim
        human = HumanSim(persona=persona, goal=user_goal, totp_secret=os.environ.get("HPCB_SIM_TOTP_SECRET") or None)
        respond = AcpResponder(human, persona)

    print(f"  claude-ACP: model={model_id or 'default'} projects={claude_projects_dir()} "
          f"({'interactive ' + str(persona) if interactive else 'autonomous'})", file=sys.stderr, flush=True)
    t0 = time.time()
    err = False
    stop = "?"
    cap = None
    kwargs = {"field_meta": session_meta(model_id)} if model_id else {}
    try:
        resp, cap = await acp_client.run_session(ADAPTER_BIN, [], full_prompt, cwd=repo, env=_child_env(),
                                                 mcp_servers=mcp, respond=respond,
                                                 max_turns=1 + MAX_PROSE_FOLLOWUPS + MAX_NUDGES, **kwargs)
        stop = str(getattr(resp, "stop_reason", "") or "")
    except Exception as e:  # noqa: BLE001 - transport/agent death: grade the partial transcript as a fail
        print(f"  claude-ACP aborted: {e}", file=sys.stderr, flush=True)
        err = True
    print(f"  claude-ACP: stop={stop} ({time.time() - t0:.0f}s)", file=sys.stderr, flush=True)

    replies = respond.replies if respond is not None else []
    trace, lines, src = assemble(claude_projects_dir(), full_prompt, replies)
    if src is None:
        print("  claude-ACP: no operator transcript found under the projects dir — empty trace", file=sys.stderr, flush=True)
    answer = trace.texts[-1] if trace.texts else ""
    sid = next((str(line.get("sessionId")) for line in lines if line.get("sessionId")), None)
    return RunResult(
        trace=trace,
        final=AcpFinal(result=answer, is_error=err, session_id=sid),
        messages=lines, dialogue=(human.dialogue if human else []),
        prose_followups=(human.answers if human else 0), followups_capped=(human.followups_capped if human else False),
        nudges=(human.nudges if human else 0), nudges_capped=(human.nudges_capped if human else False),
        human_sim_model=(human.model if human else None),
        acp_events=(list(cap.events) if cap is not None else None),
    )
