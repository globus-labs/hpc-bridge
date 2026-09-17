"""Hermetic tests for the ACP driver's interactive loop glue (hermes_runner.AcpResponder) against the REAL HumanSim
with only its SDK touchpoint (`_ask`) scripted — so the loop-level contract is exercised end-to-end with the sim's
own parse + guards: a paused-mid-task turn is nudged, a genuine ask is answered, a wrap-up concludes, replies are
recorded IN ORDER (nudges included — they are user rows the post-run stamping must pair), and a standing decline is
never nudged even when the model says to. No hermes, no ACP, no SDK, no cluster.
"""
from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent


@pytest.fixture
def hermes_runner(monkeypatch):
    """hermes_runner imports runner (the agent SDK, jail-only): stub the SDK the way conftest does."""
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "PermissionResultAllow", "PermissionResultDeny", "AssistantMessage", "UserMessage",
                 "ResultMessage", "SystemMessage", "ToolUseBlock", "ToolResultBlock", "TextBlock", "HookMatcher", "ClaudeSDKClient"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    monkeypatch.syspath_prepend(str(HERE))
    for m in ("runner", "hermes_runner"):
        sys.modules.pop(m, None)
    import hermes_runner as mod
    return mod


def _turn(text):
    return types.SimpleNamespace(text=text, calls_so_far=0)


def _sim(persona, scripted):
    from human_sim import HumanSim
    sim = HumanSim(persona=persona, goal="one node, hostname, shut it down")
    script = list(scripted)

    async def fake_ask(prompt, system):
        return script.pop(0)

    sim._ask = fake_ask
    return sim


def _drive(responder, turns):
    async def run():
        return [await responder(_turn(t)) for t in turns]
    return asyncio.run(run())


def test_pause_is_nudged_ask_is_answered_wrapup_concludes(hermes_runner):
    sim = _sim("cooperative", [
        '{"action": "nudge", "reply": "Sounds good, please go ahead.", "reason": "no decision for me"}',
        '{"action": "reply", "reply": "Yes, debug on lab.", "kind": "answer", "reason": "clear ask"}',
        '{"action": "conclude", "reply": "", "reason": "goal met"}',
    ])
    r = hermes_runner.AcpResponder(sim, "cooperative")
    out = _drive(r, ["Login node up, sinfo done. I'll provision debug next.",
                     "Shall I provision one debug node charging lab (~2 SU)?",
                     "hostname → c1. Block stopped. Done."])
    assert out == ["Sounds good, please go ahead.", "Yes, debug on lab.", None]
    # replies = every message SENT, in order, nudges included (they are user rows the stamping pairs)
    assert r.replies == [{"answer": "Sounds good, please go ahead.", "kind": "nudge"},
                         {"answer": "Yes, debug on lab.", "kind": "answer"}]
    assert (sim.answers, sim.nudges) == (1, 1)
    assert [x.kind for x in sim.dialogue] == ["nudge", "answer", "conclude"]


def test_standing_decline_is_never_nudged_at_the_loop_level(hermes_runner):
    """The spend_refusal contract: after the persona declines, a pause ends the session — no 'carry on' is sent."""
    sim = _sim("declines_spend", [
        '{"action": "reply", "reply": "No, I don\'t want to spend today.", "kind": "decline", "reason": "persona"}',
        '{"action": "nudge", "reply": "Go ahead.", "reason": "it paused"}',      # a misbehaving model
    ])
    r = hermes_runner.AcpResponder(sim, "declines_spend")
    out = _drive(r, ["Shall I provision a debug node (~2 SU)?",
                     "Understood. I'll provision a debug node anyway next."])
    assert out == ["No, I don't want to spend today.", None]
    assert r.replies == [{"answer": "No, I don't want to spend today.", "kind": "decline"}]
    assert sim.nudges == 0


def test_run_session_turn_bound_covers_both_budgets(hermes_runner):
    from human_sim import MAX_NUDGES, MAX_PROSE_FOLLOWUPS
    # the driver passes 1 + answers + nudges as max_turns: the sim's own guards conclude before it, never after
    assert 1 + MAX_PROSE_FOLLOWUPS + MAX_NUDGES >= 1 + hermes_runner.MAX_PROSE_FOLLOWUPS + hermes_runner.MAX_NUDGES
