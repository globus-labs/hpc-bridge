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
    from human_sim import MAX_NUDGES, MAX_PROSE_FOLLOWUPS, MAX_SCRIPTED_CODE_ANSWERS
    # the driver passes 1 + answers + nudges + scripted code answers as max_turns: the sim's own guards conclude before
    # it, never after (a scripted answer spends no answer budget, but it is a prompt all the same)
    assert hermes_runner.ACP_MAX_TURNS == 1 + MAX_PROSE_FOLLOWUPS + MAX_NUDGES + MAX_SCRIPTED_CODE_ANSWERS


def test_the_transcript_replay_loop_does_not_count_scripted_code_answers(hermes_runner, monkeypatch, tmp_path):
    # the non-ACP hermes loop used to cap at MAX_PROSE_FOLLOWUPS + 1 turns counting EVERY reply: password_pusher's
    # three scripted answers (password, password, code) used it all up, so its next ordinary question capped the run
    import human_sim
    monkeypatch.delenv("HPCB_HERMES_ACP", raising=False)
    monkeypatch.setenv("HPCB_HERMES_NO_CLARIFY", "1")
    monkeypatch.setattr(hermes_runner, "_prepare", lambda ablate, extra: (tmp_path, {}))
    turns = ["What is the current one-time code from your authenticator?"] * 3 + ["Shall I continue?"] * 9

    async def fake_turn(prompt, env, timeout):
        return 0, False
    monkeypatch.setattr(hermes_runner, "_run_turn", fake_turn)
    monkeypatch.setattr(hermes_runner, "_turn_final_text", lambda rows, after: turns.pop(0))
    monkeypatch.setenv("HPCB_SIM_TOTP_SECRET", "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ")

    async def fake_ask(self, prompt, system):
        return '{"reply": "Yes, go on.", "kind": "answer", "reason": "asked"}'
    monkeypatch.setattr(human_sim.HumanSim, "_ask", fake_ask)
    r = asyncio.run(hermes_runner.run_scenario("connect", repo_root=HERE.parents[1], persona="password_pusher"))
    scripted = [x for x in r.dialogue if x.scripted]
    assert len(scripted) == 3 and r.prose_followups == 3 + human_sim.MAX_PROSE_FOLLOWUPS and r.followups_capped
    # an ordinary persona is capped exactly as before: MAX_PROSE_FOLLOWUPS answers, then the cap
    turns[:] = ["Shall I continue?"] * 9
    r = asyncio.run(hermes_runner.run_scenario("connect", repo_root=HERE.parents[1], persona="cooperative"))
    assert r.prose_followups == human_sim.MAX_PROSE_FOLLOWUPS and r.followups_capped


def _replay(hermes_runner, monkeypatch, tmp_path, turns, outputs, persona="cooperative"):
    """Drive hermes' transcript-replay loop: `turns` are the operator's closing texts, turn by turn; `outputs` the
    human-sim model's, in order. Returns (result, the number of sim model calls, the number of hermes turns)."""
    import human_sim
    monkeypatch.delenv("HPCB_HERMES_ACP", raising=False)
    monkeypatch.setenv("HPCB_HERMES_NO_CLARIFY", "1")
    monkeypatch.setattr(hermes_runner, "_prepare", lambda ablate, extra: (tmp_path, {}))
    turns, outputs, n = list(turns), list(outputs), {"ask": 0, "turn": 0}

    async def fake_turn(prompt, env, timeout):
        n["turn"] += 1
        return 0, False

    async def fake_ask(self, prompt, system):
        n["ask"] += 1
        return outputs.pop(0)
    monkeypatch.setattr(hermes_runner, "_run_turn", fake_turn)
    monkeypatch.setattr(hermes_runner, "_turn_final_text", lambda rows, after: turns.pop(0))
    monkeypatch.setattr(human_sim.HumanSim, "_ask", fake_ask)
    r = asyncio.run(hermes_runner.run_scenario("connect", repo_root=HERE.parents[1], persona=persona))
    return r, n["ask"], n["turn"]


def test_the_transcript_replay_loop_lets_the_sim_answer_a_hand_over_without_a_question_mark(hermes_runner, monkeypatch,
                                                                                          tmp_path):
    # it used a regex on the turn's tail, which missed "Say the word once it's open and I'll …" (2026-10-15)
    r, asks, turns = _replay(hermes_runner, monkeypatch, tmp_path,
                             ["Run that in your own terminal. Say the word once it's open and I'll reconnect.",
                              "`hostname -f` printed login01.hpcb.test."],
                             ['{"action": "reply", "reply": "It is open.", "kind": "answer", "reason": "it waits"}',
                              '{"action": "conclude", "reply": "", "reason": "done"}'])
    assert (turns, asks, r.prose_followups) == (2, 2, 1)
    assert [x.kind for x in r.dialogue] == ["answer", "conclude"]
    assert [c.name for c in r.trace.calls] == ["AskUserQuestion"]   # the prose exchange, stamped for the graders


def test_the_transcript_replay_loop_nudges_a_pause_and_stops_on_a_tool_ending(hermes_runner, monkeypatch, tmp_path):
    r, asks, turns = _replay(hermes_runner, monkeypatch, tmp_path,
                             ["The login node is up. Next I'll check the partitions.", ""],
                             ['{"action": "nudge", "reply": "Please carry on.", "reason": "it paused"}'])
    assert (turns, asks, r.nudges, r.prose_followups) == (2, 1, 1, 0)   # the empty (tool-call) ending asks nothing
    assert [c.name for c in r.trace.calls] == ["user_nudge"]           # a nudge is never a question answered
