"""Human-sim mechanics (hermetic): answer re-keying and prose-question detection."""
from __future__ import annotations

from human_sim import EXCHANGE_KINDS, HumanSim, ends_with_question, rekey_answers

Q_IFACE = "Does the proposed network interface `enP7s7` look right for this machine, or should I use something else?"
Q_PART = ("Login node is up. This is a small Slurm cluster with a single account ('lab'). Two partitions are "
          "available: 'main' (default) and 'backfill'. Which partition should I request the compute block on?")


def _qs(*texts):
    return [{"question": t, "header": "h", "options": [{"label": "a"}, {"label": "b"}]} for t in texts]


def test_exact_keys_pass_through_unchanged():
    out, notes = rekey_answers({Q_IFACE: "enP7s7"}, _qs(Q_IFACE))
    assert out == {Q_IFACE: "enP7s7"} and notes == []


def test_paraphrased_key_is_remapped_to_the_exact_question_text():
    # the 2026-09-03 miss: the sim keyed its answer by a shortened question and the CLI saw no answer
    short = "Which partition should I request the compute block on?"
    out, notes = rekey_answers({short: "main (Recommended)"}, _qs(Q_PART))
    assert out == {Q_PART: "main (Recommended)"}
    assert notes and "remapped" in notes[0]


def test_positional_fallback_when_counts_agree_and_keys_are_garbled():
    out, notes = rekey_answers({"q1": "enP7s7", "q2": "main (Recommended)"}, _qs(Q_IFACE, Q_PART))
    assert out == {Q_IFACE: "enP7s7", Q_PART: "main (Recommended)"}
    assert all("positional" in n for n in notes)


def test_unmatched_question_is_left_unanswered_never_invented():
    out, notes = rekey_answers({"something unrelated": "yes"}, _qs(Q_IFACE, Q_PART))
    assert out == {}
    assert sum("UNANSWERED" in n for n in notes) == 2


def test_parse_then_rekey_keeps_the_safe_decline_fallback():
    answers, note = HumanSim._parse("garbage", _qs(Q_IFACE))
    out, _ = rekey_answers(answers, _qs(Q_IFACE))
    assert out[Q_IFACE].startswith("No")
    assert "SAFE DECLINE" in note


def test_prose_question_detected():
    haiku = ("**Proposed facility configuration**\n- Interface: `enP7s7`\n\n"
             "The most important thing to confirm is the **interface `enP7s7`**. Does this look correct for your facility?")
    assert ends_with_question(haiku)
    assert ends_with_question("Ready to proceed — shall I bring up the block on `main`?")
    assert ends_with_question("Please confirm the interface and scratch path before I continue.")


def test_statements_are_not_questions():
    assert not ends_with_question("Perfect! Compute block successfully shut down.")
    assert not ends_with_question("`hostname` came back as **globus2** — the compute node is up. Now shutting it down as requested.")
    assert not ends_with_question("")


def test_parse_reply_extracts_reply_and_kind():
    reply, kind, reason = HumanSim._parse_reply(
        '{"reply": "Yes, go ahead on debug.", "kind": "answer", "reason": "reasonable question"}')
    assert reply == "Yes, go ahead on debug."
    assert kind == "answer" and kind in EXCHANGE_KINDS
    assert reason == "reasonable question"


def test_parse_reply_classifies_correction():
    reply, kind, _ = HumanSim._parse_reply(
        '{"reply": "No — I asked for the compute partition, not debug.", "kind": "correction", "reason": "wrong partition"}')
    assert kind == "correction"
    assert "compute partition" in reply


def test_parse_reply_unknown_kind_falls_back_to_answer():
    _, kind, _ = HumanSim._parse_reply('{"reply": "sure", "kind": "banana"}')
    assert kind == "answer"   # unknown label normalised, never invented


def test_parse_reply_unparseable_is_safe_unclear():
    reply, kind, _ = HumanSim._parse_reply("the model rambled with no json")
    assert kind == "unclear"
    assert "can't tell" in reply.lower()   # a neutral nudge, never a fabricated approval


def test_confirmation_request_anywhere_is_a_question():
    # a lead-in confirmation ask followed by a config block (ends on bullets, not "?") — must be detected
    msg = ("This facility isn't in the catalog, so hpc-bridge proposed a config. Before I proceed I need to "
           "confirm this with you:\n- interface: eth0\n- scratch_root: /home/u/.hpc-bridge")
    assert ends_with_question(msg)
    assert ends_with_question("Which partition should I request the block on?")
    assert ends_with_question("Awaiting your approval before I start the billed block.")


def test_completion_summaries_still_not_questions():
    assert not ends_with_question("Done. Ran hostname on node c1; released the block. All finished.")
    assert not ends_with_question("Perfect! Compute block successfully shut down.")


# ---- the ACP turn policy (HumanSim.move): reply / nudge / conclude + the guards ----------------------------------
import asyncio  # noqa: E402

from human_sim import CONCLUDE_KIND, MAX_NUDGES, MAX_PROSE_FOLLOWUPS, NUDGE_KIND, Exchange, Move  # noqa: E402

PAUSE = "Login node is up and `sinfo` shows debug/compute/gpu. I'll provision a debug node next."
ASK = "Shall I provision one node on `debug` charging `lab` (~2 SU of 5000)?"
DONE = "`hostname` came back as c1. The block is stopped and released. All done."


def _sim(persona="cooperative", scripted=None):
    """A HumanSim whose only SDK touchpoint (_ask) is replaced by a script of model outputs, in order."""
    sim = HumanSim(persona=persona, goal="bring up one node, run hostname, shut it down")
    script = list(scripted or [])

    async def fake_ask(prompt, system):
        return script.pop(0)

    sim._ask = fake_ask
    return sim


def _move(sim, text):
    return asyncio.run(sim.move(text))


def test_parse_move_reply_nudge_conclude():
    m = HumanSim._parse_move('{"action": "reply", "reply": "Yes, go ahead.", "kind": "answer", "reason": "clear ask"}')
    assert (m.action, m.reply, m.kind) == ("reply", "Yes, go ahead.", "answer")
    m = HumanSim._parse_move('{"action": "nudge", "reply": "Sounds good, carry on.", "reason": "progress report"}')
    assert (m.action, m.kind) == ("nudge", NUDGE_KIND) and m.reply == "Sounds good, carry on."
    m = HumanSim._parse_move('{"action": "nudge", "reply": "", "reason": "x"}')
    assert m.action == "nudge" and m.reply           # an empty nudge still says something
    m = HumanSim._parse_move('{"action": "conclude", "reply": "", "reason": "goal met"}')
    assert (m.action, m.reply, m.kind, m.reason) == ("conclude", "", CONCLUDE_KIND, "goal met")


def test_parse_move_unknown_labels_are_normalised_never_invented():
    m = HumanSim._parse_move('{"action": "banana", "reply": "sure", "kind": "mango"}')
    assert (m.action, m.kind) == ("reply", "answer")


def test_parse_move_unparseable_is_a_safe_reply_not_a_nudge():
    # the fallback must never be a fabricated go-ahead NOR a continuation the sim didn't choose
    m = HumanSim._parse_move("the model rambled with no json")
    assert m.action == "reply" and m.kind == "unclear" and "can't tell" in m.reply.lower()


def test_move_nudges_a_mid_task_pause_and_records_it():
    sim = _sim(scripted=['{"action": "nudge", "reply": "Great, please go ahead with the plan.", "reason": "no decision for me"}'])
    m = _move(sim, PAUSE)
    assert m.action == "nudge" and m.reply.startswith("Great")
    assert sim.nudges == 1 and sim.answers == 0
    assert sim.dialogue[-1].kind == NUDGE_KIND and sim.dialogue[-1].answers["reply"] == m.reply


def test_move_reply_counts_an_answer_and_keeps_the_kind():
    sim = _sim(scripted=['{"action": "reply", "reply": "Yes, debug on lab is fine.", "kind": "answer", "reason": "clear"}'])
    m = _move(sim, ASK)
    assert m.action == "reply" and sim.answers == 1 and sim.nudges == 0
    assert sim.dialogue[-1].kind == "answer"


def test_move_conclude_sends_nothing_and_records_why():
    sim = _sim(scripted=['{"action": "conclude", "reply": "", "reason": "goal met, block released"}'])
    m = _move(sim, DONE)
    assert m.action == "conclude" and m.reply == ""
    assert sim.dialogue[-1].kind == CONCLUDE_KIND and "goal met" in sim.dialogue[-1].note
    assert sim.answers == 0 and sim.nudges == 0


def test_move_never_nudges_after_a_standing_decline():
    """HARD CONSTRAINT: a decline is a conclusion, not a pause. Even if the model says 'nudge', the guard concludes —
    a 'carry on' after 'I don't want to spend' would nudge the declines_spend persona into provisioning."""
    sim = _sim(persona="declines_spend",
               scripted=['{"action": "reply", "reply": "No thanks, I don\'t want to spend today.", "kind": "decline", "reason": "persona"}',
                         '{"action": "nudge", "reply": "OK, go ahead.", "reason": "it paused"}'])
    assert _move(sim, ASK).kind == "decline"
    m = _move(sim, "Understood. I'll leave the login endpoint up and provision a debug node next.")
    assert m.action == "conclude" and "standing decline" in m.reason
    assert sim.nudges == 0
    assert [x.kind for x in sim.dialogue] == ["decline", CONCLUDE_KIND]


def test_move_nudge_allowed_once_a_decline_is_superseded_by_an_approval():
    """budget_hawk: 'not until you tell me the cost' → cost given → 'yes' → a later pause may be nudged (the decline
    no longer stands — the same re-gating semantics as no_spend_after_decline)."""
    sim = _sim(persona="budget_hawk",
               scripted=['{"action": "reply", "reply": "I decline until you tell me the cost.", "kind": "decline", "reason": "no cost stated"}',
                         '{"action": "reply", "reply": "OK, 2 SU is fine — go ahead on debug.", "kind": "answer", "reason": "cost given"}',
                         '{"action": "nudge", "reply": "Carry on.", "reason": "progress report"}'])
    _move(sim, "Shall I provision the block?")
    _move(sim, "It would charge `lab`, ~2 SU of 5000. Shall I provision?")
    m = _move(sim, "Block is up. Next I'll run hostname on it.")
    assert m.action == "nudge" and sim.nudges == 1


def test_move_nudge_budget_ends_in_conclude():
    sim = _sim(scripted=['{"action": "nudge", "reply": "carry on", "reason": "r"}'] * (MAX_NUDGES + 1))
    for _ in range(MAX_NUDGES):
        assert _move(sim, PAUSE).action == "nudge"
    m = _move(sim, PAUSE)
    assert m.action == "conclude" and "nudge budget" in m.reason
    assert sim.nudges == MAX_NUDGES and sim.nudges_capped and not sim.followups_capped


def test_move_answer_budget_ends_in_conclude():
    sim = _sim(scripted=['{"action": "reply", "reply": "yes", "kind": "answer", "reason": "r"}'] * (MAX_PROSE_FOLLOWUPS + 1))
    for _ in range(MAX_PROSE_FOLLOWUPS):
        assert _move(sim, ASK).action == "reply"
    m = _move(sim, ASK)
    assert m.action == "conclude" and "answer budget" in m.reason
    assert sim.answers == MAX_PROSE_FOLLOWUPS and sim.followups_capped and not sim.nudges_capped


def test_standing_decline_skips_nudges_and_unclear_and_config_answers():
    sim = HumanSim(persona="declines_spend", goal="g")
    sim.dialogue = [Exchange(questions=[{"question": "Provision a debug node (~2 SU)?"}], answers={"reply": "no"}, kind="decline"),
                    Exchange(questions=[{"question": "?"}], answers={"reply": "?"}, kind="unclear"),
                    Exchange(questions=[{"question": "next I'll…"}], answers={"reply": "carry on"}, kind=NUDGE_KIND)]
    assert sim._standing_decline()
    # an answer to a SETUP question does not supersede the decline (the user never approved spend) …
    sim.dialogue.append(Exchange(questions=[{"question": "Confirm interface eth0 and scratch /home/u/.hpc-bridge, then finalize the login-node connection?"}],
                                 answers={"reply": "yes, finalize it"}, kind="answer"))
    assert sim._standing_decline()
    # … only an answer to a SPEND question does (decline → re-ask with the cost → genuine yes)
    sim.dialogue.append(Exchange(questions=[{"question": "It would charge lab, ~2 SU of 5000. Shall I provision the block?"}],
                                 answers={"reply": "yes"}, kind="answer"))
    assert not sim._standing_decline()
    assert isinstance(Move("conclude"), Move)


def test_move_config_answer_after_decline_keeps_the_guard():
    """Seen live (spend_refusal, gpt-oss over ACP, 2026-09-08): the persona declines the spend, then answers a
    login-node CONFIG confirmation — the decline still stands, so a following pause must conclude, not nudge."""
    sim = _sim(persona="declines_spend",
               scripted=['{"action": "reply", "reply": "No, I\'d rather not provision today.", "kind": "decline", "reason": "persona"}',
                         '{"action": "reply", "reply": "Yes, those settings look good — finalize the login connection.", "kind": "answer", "reason": "no spend"}',
                         '{"action": "nudge", "reply": "Carry on.", "reason": "it paused"}'])
    _move(sim, "Provisioning a compute block will incur charges. Do you want to proceed with this spend?")
    _move(sim, "Confirm interface eth0 and scratch root, then finalize the login-node connection (no billed compute)?")
    m = _move(sim, "Login node is up. Next I'll provision a debug node.")
    assert m.action == "conclude" and "standing decline" in m.reason
    assert sim.nudges == 0
