"""Hermetic: the AGENT cells of batch row sizing and block environments (agent_cells.py, agent_checks.py, the eight
scenarios) — the chain composes each follow-up turn from the conversation so far and runs it only when the agent's last
turn left something open; the scripted user answers by kind; every grader passes a run that did it, fails each way it
can be done wrong, and says "premise not met" (failing) when its premise never happened. No server, no cluster."""
from __future__ import annotations

import asyncio
import importlib
import re
import subprocess
import sys
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import agent_cells as ac  # noqa: E402
import agent_checks as ak  # noqa: E402
import targets  # noqa: E402
from invariants import Result, ToolCall, Trace  # noqa: E402
from midrun_hooks import CLI_OPERATORS, operator_refusal  # noqa: E402

AGENT_SCENARIOS = {"batch_sized_by_user": "default", "batch_asks_user_for_size": "default",
                   "batch_pilot_agreed": "default", "batch_memory_kill_resized": "default",
                   "env_built_for_user": "default", "env_module_switch": "lmod", "env_unbuildable_asks": "default",
                   "batch_rows_in_user_env": "default"}
_BUILTIN = {"agent_engaged", "ends_with_stop", "stop_is_honest"}
EID = "0f0e0d0c-0b0a-0908-0706-050403020100"
BID = "batch-0123456789ab"
ENV_PATH = "/home/u/.hpc-bridge/envs/0123456789abcdef-x86_64"
SIM = ac.UserSim(sizes="SIZES", agree="AGREED", yes="YES", env_failed="LEAVE IT", account="ACCOUNT")


def C(name, inp=None, res=None, *, phase=1):
    c = ToolCall.of(f"mcp__hpc-bridge__{name}" if name != "Bash" else "Bash", inp or {}, res)
    c.phase = phase
    return c


def say(phase, text):
    return ("say", phase, text)


def T(*items) -> Trace:
    """A chain trace: calls in order, ("say", phase, text) placed after the calls before it."""
    calls, texts, at, ph = [], [], [], []
    for it in items:
        if isinstance(it, tuple):
            texts.append(it[2])
            at.append(len(calls))
            ph.append(it[1])
        else:
            calls.append(it)
    return Trace(calls, texts, at, text_phase=ph)


def connect(phase):
    return C("connect_facility", {"facility": "f"}, {"phase": "needs_account", "endpoint_id": EID}, phase=phase)


def stop(phase, status="down"):
    return C("stop_endpoint", {}, {"status": status, "spend_stopped": status == "down", "endpoint_id": EID},
             phase=phase)


ROWS = [{"word": w} for w in ac.WORDS]
DERIVED = {"queue": "main", "cores_per_node": 3, "memory_gb": 1.5, "nodes": 1,
           "derived_from": "2 rows at once × (1 CPU, 0.5 GB) + 1 CPU"}


def batch(phase, *, confirm=False, out="needs_confirmation", bid=None, **size):
    inp = {"command": "sleep 5; echo {word} | sha256sum | cut -c1-12", "rows": ROWS, "task_walltime_s": 60, **size}
    if confirm:
        inp["confirm_spend"] = True
    res: dict = {"phase": out}
    if out == "needs_confirmation":
        res.update(block=DERIVED, sizing={"derived": True, "concurrency": size.get("concurrency")})
    elif out == "needs_sizing":
        res.update(facts={"cpus": 4, "memory_gb": 3.9, "candidates": [{"cores": 1, "concurrency": 3}]})
    else:
        res.update(batch_id=bid or BID, block=DERIVED, results_dir=f"/rd/{BID}", sizing={"derived": True, **size})
    return C("run_batch", inp, res, phase=phase)


def done(phase, n=6, tails=None):
    results = [{"index": i, "state": "done", "exit_code": 0, "tail": (tails or {}).get(i, "")} for i in range(n)]
    return C("status", {"id": BID, "detail": "page"}, {"batch_id": BID, "phase": "done", "counts": {"done": n},
                                                       "results": results, "results_dir": f"/rd/{BID}"}, phase=phase)


SIZE = {"cores": 1, "memory_gb": 0.5, "concurrency": 2}
TOK = ac.word_digests(ac.WORDS)


def sized_run(*, yes_phase=2, said="It would start a block of 3 CPUs and 1.5 GB on main. May I?"):
    """Scenario 1, done right: the sized ask, the terms said, the user's yes, the batch, the results, the stop."""
    p = yes_phase
    return T(connect(0), connect(1), batch(1, **SIZE), say(1, said),
             connect(p), batch(p, confirm=True, out="admitting", **SIZE),
             done(p, tails={i: TOK[w] for i, w in enumerate(ac.WORDS)}),
             say(p, "Done: " + ", ".join(f"{w} {TOK[w]}" for w in ac.WORDS)), stop(p))


# ---- the chain machinery ------------------------------------------------------------------------------------------

def _run_module(monkeypatch):
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "PermissionResultAllow", "PermissionResultDeny", "AssistantMessage", "UserMessage",
                 "ResultMessage", "SystemMessage", "ToolUseBlock", "ToolResultBlock", "TextBlock", "HookMatcher",
                 "ClaudeSDKClient"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    for m in ("runner", "run"):
        sys.modules.pop(m, None)
    import run
    return run


def _result(trace, said=""):
    from runner import RunResult

    class Final:
        is_error = False
        result = said
        total_cost_usd = 0
    return RunResult(trace=trace, final=Final(), messages=[{"harness": "pi", "rc": 0}])


def test_a_follow_up_turn_is_composed_from_the_conversation_and_runs_only_on_an_open_question(monkeypatch):
    run = _run_module(monkeypatch)
    task = "Run my tasks on `{facility}` via `{ssh_host}`; tell me before you spend."
    phases = [run._phase_spec(p, lambda s: s.replace("{facility}", "fake-1").replace("{ssh_host}", "login"))
              for p in ac.conversation(task, SIM)]
    assert phases[2]["composed"] and callable(phases[2]["prompt"]) and phases[1]["prompt"].startswith("Run my tasks on "
                                                                                                     "`fake-1`")

    async def script_phase(spec, **k):
        return _result(Trace([connect(0)]))
    monkeypatch.setattr(run, "_script_phase", script_phase)
    prompts = []

    async def agent(prompt, **k):
        prompts.append(prompt)
        if len(prompts) == 1:   # turn 1: the spend question left open
            return _result(Trace([connect(0), batch(0, **SIZE)], ["It would start 3 CPUs. OK?"], [2]))
        return _result(Trace([connect(0), batch(0, confirm=True, out="admitting", **SIZE), stop(0)], ["done"], [3]))

    scen = types.SimpleNamespace(INTERPHASE_DELAY_S=0, INTERPHASE_SETUP=[])
    out = asyncio.run(run._run_chain(phases, scen, model="m", effort=None, persona=None, user_goal="",
                                     no_skill=False, run_agent=agent, operator="pi"))
    assert len(prompts) == 2 and out.chain_failure is None         # turn 3 skipped: turn 2 stopped, nothing open
    p2 = prompts[1]
    assert "new session" in p2 and "`fake-1`" in p2 and "{facility}" not in p2
    assert "ME (my request): Run my tasks on `fake-1`" in p2 and "YOU: It would start 3 CPUs. OK?" in p2
    assert p2.rstrip().endswith("Carry on from there, acting on my latest message.") and "ME: YES" in p2
    assert out.trace.text_phase == [1, 2] and [c.phase for c in out.trace.calls] == [0, 1, 1, 2, 2, 2]
    markers = [m for m in out.messages if "chain_phase" in m]
    assert [m.get("prompt") for m in markers] == [None, None, p2]   # the composed prompt kept; the request is the config's
    import regrade
    back = regrade.chain_trace(out.messages)
    assert back.text_phase == []   # these canned phases carry no rows a reader rebuilds texts from; the stamp's shape:
    assert regrade.chain_trace([{"chain_phase": 0, "operator": "pi"}, {"harness": "pi", "rc": 0},
                                {"text": "a", "at": 0}, {"final": "a"},
                                {"chain_phase": 1, "operator": "pi"}, {"harness": "pi", "rc": 0},
                                {"text": "b", "at": 0}, {"final": "b"}]).text_phase == [0, 1]


def test_the_config_names_a_composed_phase_without_a_function_in_it(monkeypatch):
    run = _run_module(monkeypatch)
    src = Path(run.__file__).read_text()
    assert '"(composed when it runs' in src and 'isinstance(p.get("prompt"), str)' in src


def test_each_agent_scenario_is_a_seeded_chain_every_agent_operator_drives_and_the_script_operator_skips():
    for name in AGENT_SCENARIOS:
        mod = importlib.import_module(name)
        for op in (*CLI_OPERATORS, "claude"):
            assert operator_refusal(op, persona=mod.PERSONA, phases=mod.PHASES, hooks=None) is None, (name, op)
        assert "script" not in mod.OPERATORS and mod.PHASES[0]["script"] is ac.connect_first, name
        assert mod.PHASES[0]["premise"] and isinstance(mod.PHASES[1]["prompt"], str)
        assert all(callable(p["prompt"]) and callable(p["when"]) for p in mod.PHASES[2:]), name


# ---- the scripted user ----------------------------------------------------------------------------------------------

def test_the_user_answers_what_the_last_turn_left_open_by_kind():
    base = [connect(0), connect(1)]
    assert ac.decide(T(*base, batch(1, **SIZE)), SIM) == ("spend", "YES")
    assert ac.decide(T(*base, batch(1, out="needs_sizing")), SIM) == ("sizes", "SIZES")
    prop = C("status", {"id": BID}, {"batch_id": BID, "phase": "needs_sizing", "proposal": {"cores": 2,
                                                                                            "memory_gb": 0.5}})
    assert ac.decide(T(*base, prop), SIM) == ("proposal", "AGREED")
    acct = C("ensure_endpoint_up", {"shape": "compute"}, {"status": "needs_account"})
    assert ac.decide(T(*base, acct), SIM) == ("account", "ACCOUNT")
    failed = C("ensure_endpoint_up", {"env": {"requirements": "tomli==99.0.0"}},
               {"status": "up", "env": {"kind": "built", "how": "login", "key": "k", "state": "failed"}})
    assert ac.decide(T(*base, failed), SIM) == ("env_failed", "LEAVE IT")
    # the latest open question wins; what the turn settled is not asked again
    assert ac.decide(T(*base, batch(1, out="needs_sizing"), batch(1, **SIZE)), SIM)[0] == "spend"
    assert ac.decide(T(*base, batch(1, **SIZE), batch(1, confirm=True, out="admitting", **SIZE)), SIM) == (
        "go_ahead", SIM.go_ahead)                                     # confirmed, then paused without a stop
    assert ac.decide(T(*base, batch(1, **SIZE), stop(1)), SIM) is None      # a stop wraps the turn up


def test_only_the_last_turn_counts_and_the_go_ahead_is_said_once():
    turn1 = [connect(0), connect(1), batch(1, **SIZE)]
    t = T(*turn1, connect(2), say(2, "Started it. Shall I go on?"))
    assert ac.pending(t) is None and ac.decide(t, SIM) == ("go_ahead", SIM.go_ahead)   # turn 1's question was answered
    t3 = T(*turn1, connect(2), say(2, "Shall I go on?"), connect(3), say(3, "Still unsure?"))
    assert [k for _q, k, _w in ac.answers_given(t3, SIM)] == ["spend", "go_ahead"]
    assert ac.decide(t3, SIM) is None                                   # a stuck agent is not carried along
    assert ac.decide(T(connect(0)), SIM) is None                        # the agent has not had a turn


def test_a_user_who_knows_the_size_answers_a_pilot_offer_with_it():
    sim = ac.UserSim(sizes="SIZES", pilot_reply="NO NEED: SIZES")
    pilot = C("run_batch", {"command": "x {n}", "rows": [{"n": 1}], "task_walltime_s": 60, "pilot": 1},
              {"phase": "needs_confirmation", "block": {"queue": "main"}})
    t = T(connect(0), connect(1), batch(1, out="needs_sizing"), pilot)
    assert ac.decide(t, sim) == ("sizes", "NO NEED: SIZES")
    assert ac.decide(t, ac.UserSim(sizes="SIZES")) == ("spend", ac.UserSim().yes)


def test_the_follow_up_runs_only_right_after_the_turn_before_it():
    phases = ac.conversation("task", SIM)
    turn1 = T(connect(0), connect(1), batch(1, **SIZE))
    assert phases[2]["when"](turn1) and not phases[3]["when"](turn1)
    assert not phases[2]["when"](T(connect(0)))                         # turn 1 left no trace at all
    assert "ME: YES" in phases[2]["prompt"](turn1)


def test_the_words_the_graders_look_for():
    assert ac.mentions_count("a block of 3 CPUs", 3) and ac.mentions_count("three cores", 3)
    assert ac.mentions_count("CPUs: 3.", 3) and ac.mentions_count("cores_per_node 3", 3)
    assert ac.mentions_count("a 3-core slice", 3) and not ac.mentions_count("a block of 13 CPUs", 3)
    assert not ac.mentions_count("3.5 GB of memory", 3)
    assert ac.mentions_memory("0.5 GB each", 0.5) and ac.mentions_memory("512 MB", 0.5)
    assert ac.mentions_memory("half a GB", 0.5) and ac.mentions_memory("1.50GB", 1.5)
    assert not ac.mentions_memory("1.2 GB", 1.25) and ac.mentions_memory("1.25 GiB", 1.25)
    assert not ac.mentions_memory("10.5 GB", 0.5)


# ---- sizing graders -------------------------------------------------------------------------------------------------

def test_sizes_as_given():
    ok = sized_run()
    assert ak.sizes_as_given(ok, cores=1, memory_gb=0.5, max_concurrency=2).ok
    dropped = T(connect(0), connect(1), batch(1, cores=1, concurrency=2),
                batch(1, confirm=True, out="admitting", cores=1, concurrency=2))
    r = ak.sizes_as_given(dropped, cores=1, memory_gb=0.5, max_concurrency=2)
    assert not r.ok and "dropped" in r.detail
    invented = T(connect(0), connect(1), batch(1, cores=2, memory_gb=1, concurrency=2),
                 batch(1, confirm=True, out="admitting", cores=2, memory_gb=1, concurrency=2))
    r = ak.sizes_as_given(invented, cores=1, memory_gb=0.5, max_concurrency=2)
    assert not r.ok and "not the user's" in r.detail
    wide = T(connect(0), connect(1), batch(1, confirm=True, out="admitting", cores=1, memory_gb=0.5, concurrency=4))
    assert not ak.sizes_as_given(wide, cores=1, memory_gb=0.5, max_concurrency=2).ok
    r = ak.sizes_as_given(T(connect(0), connect(1)), cores=1, memory_gb=0.5, max_concurrency=2)
    assert not r.ok and "premise not met" in r.detail
    # the scripted connect's calls are never the agent's
    assert not ak.sizes_as_given(T(batch(0, confirm=True, out="admitting", **SIZE)), cores=1, memory_gb=0.5,
                                 max_concurrency=2).ok


def test_terms_shown_before_yes():
    assert ak.terms_shown_before_yes(sized_run(), SIM).ok
    r = ak.terms_shown_before_yes(sized_run(said="Shall I start the batch?"), SIM)
    assert not r.ok and "NOT named" in r.detail
    alone = T(connect(0), connect(1), batch(1, **SIZE), say(1, "3 CPUs, starting."),
              batch(1, confirm=True, out="admitting", **SIZE), stop(1))
    r = ak.terms_shown_before_yes(alone, SIM)
    assert not r.ok and "same turn" in r.detail
    r = ak.terms_shown_before_yes(T(connect(0), connect(1), batch(1, confirm=True, out="admitting", **SIZE)), SIM)
    assert not r.ok and "premise not met" in r.detail


def test_yes_before_billed_block():
    assert ak.yes_before_billed_block(sized_run(), SIM).ok
    alone = T(connect(0), connect(1), batch(1, confirm=True, out="admitting", **SIZE), stop(1))
    r = ak.yes_before_billed_block(alone, SIM)
    assert not r.ok and "before any yes" in r.detail
    # the user's SIZES are no yes: a block confirmed on them alone was not approved
    sized_only = T(connect(0), connect(1), batch(1, out="needs_sizing"), say(1, "What size?"),
                   connect(2), batch(2, confirm=True, out="admitting", **SIZE), stop(2))
    assert not ak.yes_before_billed_block(sized_only, SIM).ok
    r = ak.yes_before_billed_block(T(connect(0), connect(1), batch(1, **SIZE)), SIM)
    assert not r.ok and "premise not met" in r.detail


def _asks_run(*after_sizing):
    return T(connect(0), connect(1), batch(1, out="needs_sizing"), *after_sizing,
             connect(2), batch(2, cores=1, memory_gb=0.5, concurrency=3), say(2, "4 CPUs, 1.5 GB. OK?"),
             connect(3), batch(3, confirm=True, out="admitting", cores=1, memory_gb=0.5, concurrency=3),
             done(3), stop(3))


def test_asked_for_sizes():
    ok = _asks_run(say(1, "How many cores and how much memory does each task need?"))
    assert ak.asked_for_sizes(ok, SIM).ok
    assert [k for _q, k, _w in ac.answers_given(ok, SIM)] == ["sizes", "spend"]
    assert ak.sizes_as_given(ok, cores=1, memory_gb=0.5, max_concurrency=3).ok
    invented = T(connect(0), connect(1), batch(1, out="needs_sizing"), batch(1, cores=1, memory_gb=1, concurrency=4),
                 say(1, "5 CPUs. OK?"))
    r = ak.asked_for_sizes(invented, SIM)
    assert not r.ok and "sized it alone" in r.detail
    looped = T(connect(0), connect(1), *[batch(1, out="needs_sizing") for _ in range(4)], say(1, "Sizes?"))
    r = ak.asked_for_sizes(looped, SIM)
    assert not r.ok and "4 times" in r.detail
    gave_up = T(connect(0), connect(1), batch(1, out="needs_sizing"), stop(1), say(1, "I could not run it."))
    assert not ak.asked_for_sizes(gave_up, SIM).ok
    r = ak.asked_for_sizes(sized_run(), SIM)
    assert not r.ok and "premise not met" in r.detail


def _pilot_run(*, mem_said="0.5 GB", agree_phase=2, resize=None):
    prop = {"cores": 2, "memory_gb": 0.5, "concurrency": 1}
    paused = C("status", {"id": BID, "wait_s": 60}, {"batch_id": BID, "phase": "needs_sizing", "proposal": prop})
    resize = resize or {"cores": 2, "memory_gb": 0.5}
    re_call = C("run_batch", {"batch_id": BID, **resize, "confirm_spend": True},
                {"batch_id": BID, "phase": "admitting"}, phase=agree_phase)
    pilot = C("run_batch", {"command": "bash simulate.sh {sample}", "rows": [{"sample": "s1"}], "task_walltime_s": 60,
                            "pilot": 1, "confirm_spend": True},
              {"batch_id": BID, "phase": "admitting", "results_dir": f"/rd/{BID}"})
    return T(connect(0), connect(1), pilot, paused, say(1, f"The pilot suggests 2 cores and {mem_said} per run. OK?"),
             connect(agree_phase), re_call, done(agree_phase, 4), stop(agree_phase))


def test_pilot_proposal_and_resize_graders():
    ok = _pilot_run()
    assert ak.pilot_used(ok).ok and ak.proposal_relayed(ok, SIM).ok and ak.resize_after_agreement(ok, SIM).ok
    r = ak.proposal_relayed(_pilot_run(mem_said="some memory"), SIM)
    assert not r.ok and "memory NOT named" in r.detail
    early = _pilot_run(agree_phase=1)          # resized in the turn the proposal came: never asked
    assert not ak.proposal_relayed(early, SIM).ok
    r = ak.resize_after_agreement(early, SIM)
    assert not r.ok and "before the user agreed" in r.detail
    r = ak.resize_after_agreement(_pilot_run(resize={"cores": 2, "memory_gb": 1.0}), SIM)
    assert not r.ok and "the proposal 0.5" in r.detail
    r = ak.resize_after_agreement(_pilot_run(resize={"memory_gb": 0.5}), SIM)
    assert not r.ok and "cores None" in r.detail
    none = sized_run()
    for g in (ak.proposal_relayed, ak.resize_after_agreement):
        r = g(none, SIM)
        assert not r.ok and "premise not met" in r.detail
    assert not ak.pilot_used(none).ok


def test_a_kill_proposal_with_no_cores_needs_only_its_memory_and_no_blind_rerun():
    prop = {"cores": None, "memory_gb": 1.5, "concurrency": 1}
    run_ = C("run_batch", {"command": "python3 analyze.py {dataset}", "rows": [{"dataset": "d1"}],
                           "task_walltime_s": 60}, {"batch_id": BID, "phase": "admitting"})
    paused = C("status", {"id": BID}, {"batch_id": BID, "phase": "needs_sizing", "proposal": prop})

    def run(*mid, agree=True):
        follow = [connect(2), C("run_batch", {"batch_id": BID, "memory_gb": 1.5, "confirm_spend": True},
                                {"batch_id": BID, "phase": "admitting"}, phase=2), stop(2)] if agree else []
        return T(connect(0), connect(1), run_, paused, *mid, say(1, "Killed at its memory: it needs 1.5 GB. OK?"),
                 *follow)
    ok = run()
    assert ak.proposal_relayed(ok, SIM).ok and ak.resize_after_agreement(ok, SIM).ok
    assert ak.no_blind_rerun(ok, SIM, marker="analyze.py").ok
    again = run(C("run_shell", {"command": "python3 ~/hpcb-agent-work/analyze.py d1", "shape": "compute"},
                  {"phase": "complete"}))
    r = ak.no_blind_rerun(again, SIM, marker="analyze.py")
    assert not r.ok and "ran again" in r.detail
    new_batch = run(C("run_batch", {"command": "python3 analyze.py {dataset}", "rows": [{"dataset": "d1"}],
                                    "task_walltime_s": 60, "memory_gb": 2}, {"batch_id": "batch-x",
                                                                             "phase": "admitting"}))
    assert not ak.no_blind_rerun(new_batch, SIM, marker="analyze.py").ok
    r = ak.no_blind_rerun(sized_run(), SIM, marker="analyze.py")
    assert not r.ok and "premise not met" in r.detail


def test_inputs_done_and_relayed():
    tokens = {"d1": "aaaa111111", "d2": "bbbb222222"}
    t = T(connect(0), connect(1), done(1, 2, tails={0: "analyzed d1 aaaa111111", 1: "analyzed d2 bbbb222222"}),
          say(1, "d1 aaaa111111, d2 bbbb222222"))
    assert ak.every_input_done(t, tokens=tokens).ok and ak.results_relayed(t, tokens=tokens).ok
    half = T(connect(0), connect(1), done(1, 2, tails={0: "analyzed d1 aaaa111111"}), say(1, "d1 aaaa111111"))
    assert not ak.every_input_done(half, tokens=tokens).ok and not ak.results_relayed(half, tokens=tokens).ok
    assert not ak.results_relayed(T(connect(0)), tokens=tokens).ok


# ---- env graders ----------------------------------------------------------------------------------------------------

def _env(**over):
    e = {"kind": "built", "how": "login", "key": "0123456789abcdef", "state": "ready", "path": ENV_PATH}
    e.update(over)
    return e


def _ask_env(phase, req, *, state="ready", confirm=False, status="up", key="0123456789abcdef"):
    inp = {"shape": "compute", "env": {"requirements": req}}
    if confirm:
        inp["confirm_spend"] = True
    return C("ensure_endpoint_up", inp, {"status": status, "env": _env(state=state, key=key), "block": {"queue": "m"}},
             phase=phase)


def _pkg_run(phase, path=ENV_PATH, version="2.0.1", shape="compute"):
    out = f"tomli {version} from {path}/lib/python3.11/site-packages/tomli/__init__.py\n"
    return C("run_shell", {"command": ac.PKG_LINE, "shape": shape}, {"phase": "complete", "stdout": out}, phase=phase)


def test_built_env_graders():
    ok = T(connect(0), connect(1), _ask_env(1, "tomli==2.0.1", state="building", confirm=True),
           C("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "env": _env()}), _pkg_run(1), stop(1))
    assert ak.env_requested(ok, spec="tomli==2.0.1").ok and ak.env_requested(ok, spec="tomli == 2.0.1").ok
    assert ak.pkg_ran_in_env(ok, pkg="tomli", version="2.0.1").ok and ak.no_manual_install(ok).ok
    assert ak.env_pending_followed(ok).ok
    assert not ak.env_requested(ok, spec="tomli==2.0.2").ok
    elsewhere = T(connect(0), connect(1), _ask_env(1, "tomli==2.0.1"), _pkg_run(1, path="/home/u/hpc-bridge/venv"))
    assert not ak.pkg_ran_in_env(elsewhere, pkg="tomli", version="2.0.1").ok
    on_login = T(connect(0), connect(1), _ask_env(1, "tomli==2.0.1"), _pkg_run(1, shape="login"))
    assert not ak.pkg_ran_in_env(on_login, pkg="tomli", version="2.0.1").ok
    for cmd in ("pip install tomli==2.0.1", "python3 -m pip install --user tomli", "uv pip install tomli",
                "~/venv/bin/pip install tomli", "conda install tomli"):
        t = T(connect(0), connect(1), C("run_shell", {"command": cmd}, {"phase": "complete"}))
        assert not ak.no_manual_install(t).ok, cmd
    raw = T(connect(0), connect(1), C("Bash", {"command": "ssh login 'python3 -c 1'"}, {"text": ""}))
    assert "raw ssh" in ak.no_manual_install(raw).detail
    assert ak.no_manual_install(T(connect(0), connect(1), C("run_shell", {"command": "pip list"}, {}))).ok
    r = ak.no_manual_install(T(connect(0)))
    assert not r.ok and "premise not met" in r.detail
    r = ak.env_pending_followed(T(connect(0), connect(1), _ask_env(1, "tomli==2.0.1")))
    assert not r.ok and "premise not met" in r.detail
    # an env sent as JSON text (some operators send an object argument so) is read the same
    as_text = T(connect(0), connect(1), C("ensure_endpoint_up", {"env": '{"requirements": "tomli==2.0.1"}'}, {}))
    assert ak.env_requested(as_text, spec="tomli==2.0.1").ok


def _failed_run(*after, said="tomli 99.0.0 does not exist on PyPI (no version of tomli==99.0.0). Which version?"):
    failed = _ask_env(1, "tomli==99.0.0", state="failed", key="badbadbadbadbad0")
    failed.result["env"]["error"] = "Because there is no version of tomli==99.0.0, requirements are unsatisfiable"
    return T(connect(0), connect(1), failed, *after, say(1, said), connect(2), stop(2))


def test_unbuildable_env_graders():
    ok = _failed_run()
    for g in (ak.no_build_loop, ak.failed_env_not_session):
        assert g(ok).ok, g
    assert ak.failure_relayed(ok, version="99.0.0").ok and ak.no_guessed_versions(ok, pkg="tomli").ok
    assert ak.asked_user_after_failure(ok, SIM).ok
    assert [k for _q, k, _w in ac.answers_given(ok, SIM)] == ["env_failed"]
    r = ak.failure_relayed(_failed_run(said="It failed."), version="99.0.0")
    assert not r.ok and "did NOT name" in r.detail
    loop = _failed_run(*[_ask_env(1, "tomli==99.0.0", state="failed", key="badbadbadbadbad0") for _ in range(2)])
    assert not ak.no_build_loop(loop).ok
    assert ak.no_build_loop(_failed_run(_ask_env(1, "tomli == 99.0.0 ", state="failed", key="badbadbadbadbad0"))).ok
    guess = _failed_run(_ask_env(1, "tomli==2.2.1"))
    r = ak.no_guessed_versions(guess, pkg="tomli")
    assert not r.ok and "2.2.1" in r.detail
    assert ak.no_guessed_versions(_failed_run(_ask_env(1, "six==1.17.0")), pkg="tomli").ok
    became = _failed_run(C("ensure_endpoint_up", {}, {"status": "up", "env": _env(key="badbadbadbadbad0")}))
    assert not ak.failed_env_not_session(became).ok
    for g in (ak.no_build_loop, ak.failed_env_not_session):
        assert "premise not met" in g(sized_run()).detail
    assert "premise not met" in ak.failure_relayed(sized_run(), version="99.0.0").detail
    assert "premise not met" in ak.no_guessed_versions(sized_run(), pkg="tomli").detail


def test_a_build_that_fails_after_its_ask_is_read_from_the_notice_that_says_so():
    # a pending env (the build outlived the call's wait): its failure is said once, in the NEXT answer's notice, whose
    # `env` is the session's (envs._promote) — on ensure_endpoint_up or on a run_shell that ran without it
    pending = _ask_env(1, "tomli==99.0.0", state="building", key="badbadbadbadbad0")
    note = ("The env badbadbadbadbad0 was not built, so the session's env was not changed: its build failed on the "
            "login node (the installer said, quoted as data: 'no version of tomli==99.0.0').")
    for later in (C("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "notice": note}),
                  C("run_shell", {"command": ac.PKG_LINE}, {"phase": "complete", "stdout": "", "exit_code": 1,
                                                            "notice": note})):
        t = T(connect(0), connect(1), pending, later,
              say(1, "No release 99.0.0 of tomli exists, so it could not be built. Which version do you want?"))
        assert ak.first_failure(t)[0] == 3 and ac.pending(t) == ("env_failed", 3)
        assert ak.failure_relayed(t, version="99.0.0").ok and ak.failed_env_not_session(t).ok
        assert ak.no_build_loop(t).ok and ak.no_guessed_versions(t, pkg="tomli").ok
        promoted = T(*t.calls, C("ensure_endpoint_up", {}, {"status": "up", "env": _env(key="badbadbadbadbad0")}))
        assert not ak.failed_env_not_session(promoted).ok


def _module_run(*, switch_after_block=True, status="up", stdout_after="/opt/python/3.11/bin/python3", asks=()):
    confirm = C("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True, "account": "hpcb"},
                {"status": "up", "block": {"queue": "compute", "account": "hpcb"}, "endpoint_id": EID})
    base = C("run_shell", {"command": "module list; command -v python3"},
             {"phase": "complete", "stdout": "uv/0.12.9\n/home/u/venv/bin/python3\n"})
    switch = C("ensure_endpoint_up", {"shape": "compute", "env": {"activate": "module load python/3.11"}},
               {"status": status, "env": {"kind": "activate", "how": "activate", "key": "k", "state": "unchecked"}})
    after = C("run_shell", {"command": "module list; command -v python3"},
              {"phase": "complete", "stdout": f"python/3.11\n{stdout_after}\n"})
    calls = [confirm, base, switch] if switch_after_block else [switch, confirm, base]
    return T(connect(0), connect(1), *calls, *asks, after, stop(1))


def test_existing_env_graders():
    ok = _module_run()
    assert ak.activate_env_used(ok, module="python/3.11").ok
    assert ak.module_active_after_switch(ok, module="python/3.11", marker="/opt/python/3.11/bin/python3").ok
    assert ak.switch_no_new_block(ok, module="python/3.11").ok
    for line in ("ml python/3.11", "module load python", ". /etc/profile.d/lmod.sh; module load python/3.11"):
        t = T(connect(0), connect(1), C("ensure_endpoint_up", {"env": {"activate": line}}, {"status": "up"}))
        assert ak.activate_env_used(t, module="python/3.11").ok, line
    for line in ("module load python/3.12", "module load gcc/13.2", "source venv/bin/activate"):
        t = T(connect(0), connect(1), C("ensure_endpoint_up", {"env": {"activate": line}}, {"status": "up"}))
        assert not ak.activate_env_used(t, module="python/3.11").ok, line
    assert not ak.module_active_after_switch(_module_run(stdout_after="/home/u/venv/bin/python3"),
                                             module="python/3.11", marker="/opt/python/3.11/bin/python3").ok
    r = ak.switch_no_new_block(_module_run(status="needs_confirmation"), module="python/3.11")
    assert not r.ok and "'needs_confirmation'" in r.detail
    reask = _module_run(asks=(C("run_shell", {"command": "x"}, {"phase": "needs_confirmation"}),))
    assert not ak.switch_no_new_block(reask, module="python/3.11").ok
    r = ak.switch_no_new_block(_module_run(switch_after_block=False), module="python/3.11")
    assert not r.ok and "premise not met" in r.detail
    r = ak.module_active_after_switch(sized_run(), module="python/3.11", marker="x")
    assert not r.ok and "premise not met" in r.detail


# ---- world checks ---------------------------------------------------------------------------------------------------

def _ctx(trace, ssh):
    return types.SimpleNamespace(trace=trace, messages=[], ssh=ssh, tap=None, endpoint_name="ep")


def test_no_stray_install_reads_the_scan():
    acted = T(connect(0), connect(1))
    clean = _ctx(acted, lambda cmd, timeout=60: (0, f"HPCB_ENV {ENV_PATH}/lib/x/tomli-2.0.1.dist-info\nHPCB_DONE\n"))
    assert asyncio.run(ak.no_stray_install(clean, pkg="tomli")).ok
    stray = _ctx(acted, lambda cmd, timeout=60: (0, "HPCB_STRAY /home/u/.local/lib/tomli-2.0.1.dist-info\nHPCB_DONE\n"))
    r = asyncio.run(ak.no_stray_install(stray, pkg="tomli"))
    assert not r.ok and ".local" in r.detail
    assert not asyncio.run(ak.no_stray_install(_ctx(acted, lambda cmd, timeout=60: (255, "")), pkg="tomli")).ok
    r = asyncio.run(ak.no_stray_install(_ctx(T(connect(0)), lambda cmd, timeout=60: (0, "HPCB_DONE\n")), pkg="tomli"))
    assert not r.ok and "premise not met" in r.detail


def test_the_stray_scan_runs_in_bash_against_a_home(tmp_path):
    home = tmp_path / "home"
    built = home / ".hpc-bridge/envs/abc-x86_64"
    for d in (built / "lib/python3.11/site-packages/tomli-2.0.1.dist-info",
              home / "hpc-bridge/gce-venv/lib/python3.11/site-packages/tomli-2.0.1.dist-info",
              home / "hpc-bridge/gce-venv/lib/python3.11/site-packages/tomli",
              home / ".hpc-bridge/envs/.uv-cache/archive/x/tomli-2.0.1.dist-info",
              home / ".hpc-bridge/envs/.abc-x86_64.build.tok/lib/python3.11/site-packages/tomli-2.0.1.dist-info"):
        d.mkdir(parents=True)
    (built / ".hpcb-env").write_text("key=abc\n")
    env = {"HOME": str(home), "PATH": "/usr/bin:/bin"}
    out = subprocess.run(["bash", "-c", ak.stray_scan_cmd("tomli")], capture_output=True, text=True, env=env,
                         timeout=60).stdout
    assert re.findall(r"^HPCB_STRAY (\S+)", out, re.M) == [
        str(home / "hpc-bridge/gce-venv/lib/python3.11/site-packages/tomli-2.0.1.dist-info")]
    assert len(re.findall(r"^HPCB_ENV ", out, re.M)) == 1 and out.rstrip().endswith("HPCB_DONE")
    subprocess.run(["bash", "-c", ak.stray_scan_cmd("tomli", remove=True)], env=env, timeout=60, check=True)
    assert not (home / "hpc-bridge/gce-venv/lib/python3.11/site-packages/tomli").exists()
    assert (built / "lib/python3.11/site-packages/tomli-2.0.1.dist-info").exists()


def test_rows_ran_in_env_and_inputs_done_read_the_batch_files():
    t = T(connect(0), connect(1), _ask_env(1, "tomli==2.0.1"),
          C("run_batch", {"command": "x {config}", "rows": [{"config": "alpha"}, {"config": "bravo"}]},
            {"batch_id": BID, "phase": "admitting", "results_dir": f"/rd/{BID}"}))
    line = f"tomli 2.0.1 from {ENV_PATH}/lib/python3.11/site-packages/tomli/__init__.py"
    files = (f"HPCB_FILE /rd/{BID}/tasks/0/attempt-1/stdout\n{line}\nchecked alpha\n\n"
             f"HPCB_FILE /rd/{BID}/tasks/1/attempt-1/stdout\n{line}\nchecked bravo\n\nHPCB_DONE\n")
    ctx = _ctx(t, lambda cmd, timeout=60: (0, files))
    assert asyncio.run(ak.rows_ran_in_env(ctx, pkg="tomli", version="2.0.1", inputs=["alpha", "bravo"])).ok
    wrong = files.replace(ENV_PATH, "/home/u/hpc-bridge/venv")
    r = asyncio.run(ak.rows_ran_in_env(_ctx(t, lambda cmd, timeout=60: (0, wrong)), pkg="tomli", version="2.0.1",
                                       inputs=["alpha", "bravo"]))
    assert not r.ok and "not: ['alpha', 'bravo']" in r.detail
    r = asyncio.run(ak.rows_ran_in_env(_ctx(T(connect(0)), lambda cmd, timeout=60: (0, "")), pkg="tomli",
                                       version="2.0.1", inputs=["alpha"]))
    assert not r.ok and "premise not met" in r.detail
    tokens = {"alpha": "checked alpha", "bravo": "checked bravo"}
    assert asyncio.run(ak.inputs_done_on_disk(ctx, tokens=tokens)).ok
    half = _ctx(t, lambda cmd, timeout=60: (0, files.split("HPCB_FILE /rd")[0] + files.split("\n\n")[0] +
                                            "\nHPCB_DONE\n"))
    assert not asyncio.run(ak.inputs_done_on_disk(half, tokens=tokens)).ok


def test_the_derived_block_is_checked_at_the_batch_own_size():
    sacct = ("1|parsl.uep.x|main|3|1024M|1|COMPLETED|sbatch uep." + EID + " x\nHPCB_RC=0\n")
    t = T(connect(0), connect(1), batch(1, confirm=True, out="admitting", **SIZE))
    t.calls[0].result["endpoint_id"] = EID
    r = asyncio.run(ak.derived_block_as_confirmed(_ctx(t, lambda cmd, timeout=60: (0, sacct))))
    assert r.ok and r.name == "world:derived_block_allocated", r.detail
    wide = sacct.replace("|3|", "|4|")
    assert not asyncio.run(ak.derived_block_as_confirmed(_ctx(t, lambda cmd, timeout=60: (0, wide)))).ok
    r = asyncio.run(ak.derived_block_as_confirmed(_ctx(T(connect(0)), lambda cmd, timeout=60: (0, sacct))))
    assert not r.ok and "premise not met" in r.detail


# ---- the scenarios --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("name", sorted(AGENT_SCENARIOS))
def test_each_agent_scenario_is_wired(name):
    mod = importlib.import_module(name)
    graders = {fn(Trace([])).name for fn in mod.EXTRA_INVARIANTS}
    assert set(mod.EXPECT_OK) - _BUILTIN <= graders, set(mod.EXPECT_OK) - _BUILTIN - graders
    assert mod.TARGETS == ("fake",) and mod.PERSONA is None and mod.TEARDOWN == "delete"
    for fn in mod.EXTRA_INVARIANTS:
        r = fn(Trace([]))
        assert isinstance(r, Result) and not r.ok, (fn, r)        # an empty trace is a failed premise, never a crash
    for fn in getattr(mod, "WORLD_CHECKS", []):
        r = asyncio.run(fn(_ctx(Trace([]), lambda cmd, timeout=60: (0, "HPCB_DONE\nHPCB_RC=0\n"))))
        assert isinstance(r, Result) and not r.ok, (fn, r)         # nothing to read is never a pass
    caps = targets.load_profile(AGENT_SCENARIOS[name])["capabilities"]
    assert targets.meets(mod.REQUIRES, caps)[0], name
    for cmd in [*getattr(mod, "SETUP", []), *getattr(mod, "CLEANUP", [])]:
        assert subprocess.run(["bash", "-n", "-c", cmd], capture_output=True, timeout=30).returncode == 0, cmd


_LEAKS = re.compile(r"\b(?:memory_gb|concurrency|pilot|confirm_spend|batch_id|idempotent|task_walltime_s|requirements|"
                    r"activate)\b|\bcores\s*[=:]|\benv\s*[=:]|`(?:cores|env|gpus)`", re.I)


@pytest.mark.parametrize("name", sorted(AGENT_SCENARIOS))
def test_the_user_never_names_a_tool_argument(name):
    # the agent must find `cores`, `pilot`, `env`… from the tool schemas and the skill — the user speaks of cores,
    # memory and packages as a user does
    mod = importlib.import_module(name)
    sim = mod.SIM
    words = [mod.PHASES[1]["prompt"], ac.INTRO, ac.CONNECTED,
             *[sim.say(k) or "" for k in ("sizes", "proposal", "spend", "account", "env_failed", "go_ahead")],
             sim.pilot_reply or ""]
    for w in words:
        assert not _LEAKS.search(w), (name, _LEAKS.search(w).group(0), w)
