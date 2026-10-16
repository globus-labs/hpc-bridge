"""Hermetic: mid-run hooks (midrun_hooks.py) — when a hook is due (after a result, or DURING a call), what it needs, which
operator can serve it, and the journal feed that serves the free CLI operators."""
from __future__ import annotations

import asyncio
import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import midrun_hooks as hooks  # noqa: E402
from invariants import ToolCall  # noqa: E402

SCENARIOS = sorted(p.stem for p in (HERE.parent / "scenarios").glob("*.py") if not p.stem.startswith("_"))


@dataclass
class ToolUseBlock:
    id: str
    name: str
    input: dict


@dataclass
class ToolResultBlock:
    tool_use_id: str
    content: str


@dataclass
class Msg:
    content: list


def _start(w, cid, name, inp):
    w.observe(Msg([ToolUseBlock(cid, f"mcp__endpoint__{name}", inp)]))


def _end(w, cid, result):
    return w.observe(Msg([ToolResultBlock(cid, json.dumps(result))]))


# -- the watcher -------------------------------------------------------------------------------------------------------

def test_an_after_hook_names_the_call_that_returned_even_when_calls_overlap():
    w = hooks.HookWatcher([{"name": "h", "after_tool": "run_shell", "when_input": {"shape": "compute"}, "cmd": "x"}])
    _start(w, "a", "run_shell", {"shape": "compute", "command": "sleep 5"})
    _start(w, "b", "poll_task", {"task_id": "t"})          # a second call starts before the first returns
    assert _end(w, "b", {"phase": "running"}) == []
    due = _end(w, "a", {"phase": "complete", "endpoint_id": "e1"})
    assert [(d["name"], d["call_index"]) for d in due] == [("h", 0)]   # call 0 returned, not the latest call (1)
    assert due[0]["endpoint_ids"] == ["e1"] and w.unfired == []


def test_when_input_is_a_dict_of_equal_arguments_or_a_regex_over_the_values():
    m = hooks._input_matches
    assert m({"shape": "compute"}, {"shape": "compute", "command": "x"}) and not m({"shape": "compute"}, {"command": "x"})
    assert m({"confirm_spend": True}, {"confirm_spend": "true"})        # True and "true" alike (Pi, Hermes coerce)
    assert m("HPCB_ESC_DONE", {"command": "sleep 60 && echo HPCB_ESC_DONE | tee marker"})
    assert m(r"sleep \d+", {"command": "sleep 60"}) and not m("HPCB_ESC_DONE", {"command": "cat marker"})
    assert m("compute", {"shape": "compute"}) and m(None, {}) and m({}, {"x": 1})
    assert m('"a": 1', {"args": {"a": 1}})                                # a nested value is searched as JSON


def test_a_during_hook_fires_only_while_its_call_is_in_flight_and_once():
    w = hooks.HookWatcher([{"name": "esc", "on_tool": "run_shell", "when_input": "MARK", "after_s": 10, "interject": "oops"}])
    _start(w, "u0", "run_shell", {"command": "hostname"})       # not the marker: never armed
    assert w.take_armed() == []
    _end(w, "u0", {"phase": "complete"})
    _start(w, "u1", "run_shell", {"command": "echo MARK"})      # armed at its start …
    (a1,) = w.take_armed()
    assert a1["call_index"] == 1 and a1["call_id"] == "u1"
    assert _end(w, "u1", {"phase": "needs_confirmation"}) == []  # … but it returned before after_s (a cold block)
    assert w.due_during(a1) is None                             # → not due: nothing was in flight to interrupt
    _start(w, "u2", "run_shell", {"command": "sleep 60; echo MARK"})
    (a2,) = w.take_armed()
    hook = w.due_during(a2)
    assert hook is not None and hook["name"] == "esc" and hook["call_index"] == 2
    assert w.due_during(a2) is None and w.unfired == []          # fires once
    _start(w, "u3", "run_shell", {"command": "echo MARK"})
    assert w.take_armed() == []                                  # a fired hook arms no more


def test_a_during_hooks_nth_counts_the_calls_still_in_flight_at_after_s():
    w = hooks.HookWatcher([{"name": "esc2", "on_tool": "poll_task", "after_s": 1, "nth": 2, "action": "interrupt"}])
    armed = []
    for cid in ("p1", "p2", "p3"):
        _start(w, cid, "poll_task", {"task_id": "t"})
        armed += w.take_armed()
    _end(w, "p2", {"phase": "running"})                          # p2 returned quickly: it does not count
    assert w.due_during(armed[0]) is None                        # p1: the 1st in flight
    assert w.due_during(armed[1]) is None                        # p2: ended
    assert w.due_during(armed[2])["call_index"] == 2             # p3: the 2nd in flight → fires


def test_the_journal_feed_fires_after_hooks_and_never_a_during_hook():
    w = hooks.HookWatcher([{"name": "cancel", "after_tool": "run_shell", "when_input": "MARK", "when_phase": "complete", "cmd": "x"},
                           {"name": "esc", "on_tool": "run_shell", "after_s": 0, "interject": "oops"}])
    rows = [ToolCall.of("mcp__hpc-bridge__connect_facility", {"facility": "f"}, {"phase": "connected", "endpoint_id": "e9"}),
            ToolCall.of("mcp__hpc-bridge__run_shell", {"command": "echo MARK"}, {"phase": "cold_start"}),
            ToolCall.of("mcp__hpc-bridge__run_shell", {"command": "echo MARK"}, {"phase": "complete"})]
    due = [d for r in rows for d in w.observe_call(r)]
    assert [(d["name"], d["call_index"], d["endpoint_ids"]) for d in due] == [("cancel", 2, ["e9"])]
    assert w.take_armed() == [] and [h["name"] for h in w.unfired] == ["esc"]   # a row is a call that has ENDED


def test_records_say_what_fired_and_what_never_did():
    w = hooks.HookWatcher([{"name": "a", "after_tool": "poll_task", "cmd": "x"}, {"on_tool": "run_shell", "after_s": 2.5, "action": "interrupt"}])
    rec = hooks.fired_record({"name": "a", "after_tool": "poll_task", "nth": 2, "call_index": 7}, {"rc": 0, "out": "ok"})
    assert rec == {"name": "a", "trigger": "after poll_task #2", "nth": 2, "call_index": 7, "after_tool": "poll_task",
                   "rc": 0, "out": "ok"}
    never = w.unfired_records()
    assert [(r["name"], r["call_index"], r["out"]) for r in never] == [("a", None, "NEVER FIRED"), ("run_shell", None, "NEVER FIRED")]
    assert never[1]["trigger"] == "during run_shell +2.5s" and never[1]["on_tool"] == "run_shell"


# -- what a hook needs, and who can serve it ----------------------------------------------------------------------------

def test_a_hooks_actions_and_needs():
    assert hooks.hook_actions({"after_tool": "x", "cmd": "c"}) == {"cmd"}
    assert hooks.hook_actions({"after_tool": "x", "interject": "t"}) == {"interrupt", "interject"}   # an interject interrupts
    assert hooks.hook_actions({"on_tool": "x", "action": "interrupt"}) == {"interrupt"}
    assert hooks.hook_actions({"on_tool": "x", "action": ["interrupt", "cmd"], "cmd": "c"}) == {"interrupt", "cmd"}
    assert hooks.hook_needs([{"after_tool": "x", "cmd": "c"}, {"on_tool": "y", "interject": "t"}]) == {
        "cmd", "interrupt", "interject", hooks.DURING_TOOL}
    assert hooks.hook_needs(None) == set()
    assert hooks.hook_actions({"after_tool": "poll_task", "world": "stop_manager"}) == {"cmd"}   # the harness builds the cmd
    assert hooks.hook_problems([{"after_tool": "poll_task", "world": "stop_manager"}]) == []
    assert hooks.operator_refusal("pi", persona=None, phases=False,
                                  hooks=[{"after_tool": "poll_task", "world": "stop_manager"}]) is None


@pytest.mark.parametrize(("hook", "problem"), [
    ({"cmd": "c"}, "exactly one trigger"),
    ({"after_tool": "a", "on_tool": "b", "cmd": "c"}, "exactly one trigger"),
    ({"after_tool": "a"}, "does nothing"),
    ({"after_tool": "a", "action": "explode"}, "unknown action"),
    ({"after_tool": "a", "interject": "  "}, "needs the user's text"),
    ({"on_tool": "a", "action": "interrupt"}, "after_s >= 0"),
    ({"on_tool": "a", "action": "interrupt", "after_s": "soon"}, "after_s >= 0"),
    ({"after_tool": "a", "cmd": "c", "after_s": 3}, "during hook (on_tool) only"),
    ({"after_tool": "a", "cmd": "c", "when_input": "(unclosed"}, "not a regex"),
    ({"after_tool": "a", "cmd": "c", "when_input": ["shape"]}, "dict or a regex"),
    ({"after_tool": "a", "world": "reboot_cluster"}, "unknown world action"),
    ({"after_tool": "a", "world": "stop_manager", "cmd": "c"}, "both a cmd and a world action"),
    ({"after_tool": "a", "cmd": "c", "expect_any_host": ""}, "expect_any_host is a non-empty substring"),
    ({"after_tool": "a", "world": "stop_manager", "expect_any_host": "x"}, "for a cmd hook"),
    ({"after_tool": "a", "cmd": "c", "when_endpoint": "yes"}, "when_endpoint is True or absent"),
    ({"name": "h", "after_tool": "a", "cmd": "c", "after_hook": "h"}, "after_hook names no other hook"),
    ({"after_tool": "a", "cmd": "c", "after_hook": "missing"}, "after_hook names no other hook"),
    ({"on_tool": "a", "after_s": 1, "action": "interrupt", "when_phase": "running"}, "an after_tool hook only"),
    ({"on_tool": "a", "after_s": 1, "action": "interrupt", "when_endpoint": True}, "an after_tool hook only"),
])
def test_a_malformed_hook_is_named(hook, problem):
    probs = hooks.hook_problems([hook])
    assert len(probs) >= 1 and any(problem in p for p in probs), probs


@pytest.mark.parametrize("name", SCENARIOS)
def test_every_scenarios_hooks_are_well_formed(name):
    assert hooks.hook_problems(getattr(importlib.import_module(name), "MIDRUN_HOOKS", None)) == []


def test_which_operator_can_drive_what():
    ref = hooks.operator_refusal
    cmd = [{"after_tool": "run_shell", "cmd": "scancel"}]
    esc = [{"on_tool": "run_shell", "after_s": 10, "interject": "oops"}]
    talk = [{"after_tool": "run_shell", "interject": "I'm back"}]
    for op in ("codex", "pi", "hermes21"):
        assert ref(op, persona=None, phases=False, hooks=cmd) is None          # a cmd after a result: from the journal
        assert ref(op, persona=None, phases=False, hooks=None) is None
        assert "during_tool" in ref(op, persona=None, phases=False, hooks=esc)
        assert "interject" in ref(op, persona=None, phases=False, hooks=talk)
        assert ref(op, persona="cooperative", phases=False, hooks=None) == "an interactive persona"
        assert ref(op, persona=None, phases=True, hooks=None) == "cross-restart chains (PHASES)"
    for op in ("hermes", "claude-acp"):
        assert ref(op, persona="cooperative", phases=False, hooks=None) is None
        assert ref(op, persona=None, phases=False, hooks=cmd) == "mid-run hooks that need cmd"
        assert ref(op, persona=None, phases=True, hooks=None) == "cross-restart chains (PHASES)"
    assert all(ref("claude", persona="p", phases=True, hooks=h) is None for h in (cmd, esc, talk))
    assert ref("someone-new", persona=None, phases=False, hooks=cmd) == "mid-run hooks that need cmd"


def test_the_chaos_cells_that_only_run_a_cmd_are_free_now():
    for name in ("orphaned_task", "draining_restop", "reap_replaced_block"):
        mod = importlib.import_module(name)
        assert hooks.operator_refusal("pi", persona=getattr(mod, "PERSONA", None), phases=False,
                                      hooks=mod.MIDRUN_HOOKS) is None, name
    esc = importlib.import_module("esc_mid_call")
    assert hooks.hook_needs(esc.MIDRUN_HOOKS) == {"interrupt", "interject", hooks.DURING_TOOL}


# -- the journal feed --------------------------------------------------------------------------------------------------

def test_a_tail_returns_each_record_once_and_waits_for_a_partial_lines_end(tmp_path):
    p = tmp_path / "j.jsonl"
    tail = hooks.JsonlTail(p)
    assert tail.read_new() == []                                  # no file yet
    p.write_text('{"tool": "a"}\n{"tool": "b", "x": "half')
    assert [r["tool"] for r in tail.read_new()] == ["a"]
    with p.open("a") as fh:
        fh.write('-written"}\nnot json\n{"tool": "c", "u": " "}\n')
    assert [r["tool"] for r in tail.read_new()] == ["b", "c"]     # U+2028 inside a record does not split it
    assert tail.read_new() == []


def _row(tool, args, result, ts):
    return json.dumps({"tool": tool, "args": args, "result": result, "ts": ts, "ms": 100}) + "\n"


def test_follow_journal_acts_in_order_while_the_run_goes_and_on_the_last_rows(tmp_path):
    from cli_runner import _journal_call

    j = tmp_path / "journal.jsonl"
    w = hooks.HookWatcher([{"name": "first", "after_tool": "run_shell", "when_input": "MARK", "cmd": "a"},
                           {"name": "second", "after_tool": "run_shell", "when_input": "MARK", "cmd": "b"},
                           {"name": "late", "after_tool": "stop_endpoint", "cmd": "c"}])
    acted: list[tuple[str, int]] = []

    async def act(hook):
        await asyncio.sleep(0.05)              # the second waits for the first, as in the SDK path
        acted.append((hook["name"], hook["call_index"]))

    async def main():
        stop = asyncio.Event()
        follower = asyncio.create_task(hooks.follow_journal(j, w, act, stop=stop, to_call=_journal_call, poll_s=0.02))
        j.write_text(_row("connect_facility", {}, {"phase": "connected"}, 1.0))
        await asyncio.sleep(0.1)
        with j.open("a") as fh:
            fh.write(_row("run_shell", {"command": "echo MARK"}, {"phase": "complete"}, 2.0))
        for _ in range(100):                   # the hooks act DURING the run, as soon as the row lands
            if len(acted) == 2:
                break
            await asyncio.sleep(0.02)
        assert acted == [("first", 1), ("second", 1)]
        with j.open("a") as fh:                # the agent's last call returns …
            fh.write(_row("stop_endpoint", {}, {"status": "down"}, 3.0))
        stop.set()                             # … and the turn ends inside the same poll window (review P2)
        await follower

    asyncio.run(main())
    assert acted == [("first", 1), ("second", 1), ("late", 2)] and w.unfired == []   # the final read caught it


def test_a_hook_whose_action_raises_is_recorded_and_the_next_one_still_acts(tmp_path):
    # review P1: the follower died with the hook, which then sat in no record at all
    from cli_runner import _journal_call

    j = tmp_path / "journal.jsonl"
    j.write_text(_row("run_shell", {"command": "echo MARK"}, {"phase": "complete"}, 1.0)
                 + _row("stop_endpoint", {}, {"status": "down"}, 2.0))
    w = hooks.HookWatcher([{"name": "boom", "after_tool": "run_shell", "cmd": "x"},
                           {"name": "next", "after_tool": "stop_endpoint", "cmd": "y"}])
    record: list[dict] = []

    async def act(hook):
        if hook["name"] == "boom":
            raise RuntimeError("ssh exploded")
        record.append(hooks.fired_record(hook, {"rc": 0}))

    async def main():
        stop = asyncio.Event()
        stop.set()   # the turn already ended: the final read still acts
        await hooks.follow_journal(j, w, act, stop=stop, to_call=_journal_call, record=record)

    asyncio.run(main())
    assert [(r["name"], r["rc"]) for r in record] == [("boom", 1), ("next", 0)] and "ssh exploded" in record[0]["out"]


# -- a blind opening batch (hermes21 on gpt-oss, live 2026-10-15) ---------------------------------------------------------

# What hpc-bridge answers each call of a parallel batch sent before connect_facility bound a facility (the orphaned_task
# and teardown_slow_fs bundles of 2026-10-15: no endpoint_id anywhere, nothing touched).
_NO_FACILITY = "No facility connected yet: connect_facility('fake-1') only PROPOSED a config (proposed_facility_details)"
_BATCH = [
    ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "down", "endpoint_id": None, "notice": _NO_FACILITY}),
    ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
     {"status": "down", "endpoint_id": None, "notice": _NO_FACILITY}),
    ("run_shell", {"shape": "compute", "command": "python3 -c 'print(1)'"}, {"phase": "failed", "notice": _NO_FACILITY}),
    ("poll_task", {"task_id": "PLACEHOLDER"},
     {"phase": "failed", "notice": "no task 'PLACEHOLDER' — already retrieved, or its block ended (stop / partition / switch)."}),
    ("reset_session", {"shape": "compute"}, {"phase": "failed", "notice": _NO_FACILITY}),
    ("stop_endpoint", {}, {"status": "down", "endpoint_id": None, "block_state": "cold", "notice": "no endpoint was up"}),
    ("teardown_endpoint", {}, {"status": "down", "endpoint_id": None, "block_state": "cold", "notice": "no endpoint was up"}),
]


def _batch_for(hook_list):
    """The batch, plus — for each hook's own `when_input` — the same no-op call carrying exactly that input (a marker in
    the command, the confirm), so a hook keyed on its input meets the batch too."""
    rows = [ToolCall.of(f"mcp__hpc-bridge__{n}", i, r) for n, i, r in _BATCH]
    by_tool = {n: r for n, _, r in _BATCH}
    for h in hook_list:
        tool, want = hooks.trigger_tool(h), h.get("when_input")
        if tool in by_tool and want:
            inp = dict(want) if isinstance(want, dict) else {"command": f"echo {want}"}
            rows.append(ToolCall.of(f"mcp__hpc-bridge__{tool}", inp, by_tool[tool]))
    return rows


HOOKED = [n for n in SCENARIOS if getattr(importlib.import_module(n), "MIDRUN_HOOKS", None)]


@pytest.mark.parametrize("name", HOOKED)
def test_no_scenario_hook_fires_on_a_blind_opening_batch(name):
    # orphaned_task's kill_manager fired on poll_task("PLACEHOLDER"): the audit of every MIDRUN_HOOKS against the batch
    mod = importlib.import_module(name)
    w = hooks.HookWatcher(mod.MIDRUN_HOOKS)
    fired = [d["name"] for c in _batch_for(mod.MIDRUN_HOOKS) for d in w.observe_call(c)]
    assert fired == [], f"{name}: {fired} fired on a call that acted on nothing"
    assert w.take_armed() == []


def _feed(w, seq):
    return [(d["name"], d["call_index"]) for n, i, r in seq for d in w.observe_call(ToolCall.of(f"mcp__hpc-bridge__{n}", i, r))]


def test_the_orphaned_task_hook_waits_for_a_poll_that_finds_the_task_running():
    import orphaned_task as sc
    seq = [_BATCH[0], _BATCH[2], _BATCH[3], _BATCH[5],      # the hermes21 batch: ensure, run_shell, the placeholder, stop
           ("connect_facility", {"facility": "f", "details": {}}, {"phase": "provisioning"}),
           ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "up", "endpoint_id": "e1"}),
           ("run_shell", {"shape": "compute", "command": "python3 -c …"}, {"phase": "running", "task_id": "compute-1"}),
           ("poll_task", {"task_id": "compute-1"}, {"phase": "running", "task_id": "compute-1"}),
           ("poll_task", {"task_id": "compute-1"}, {"phase": "failed", "notice": "task compute-1 is ORPHANED"})]
    assert _feed(hooks.HookWatcher(sc.MIDRUN_HOOKS), seq) == [("kill_manager", 7)]   # the RUNNING poll, not the placeholder
    # hermes21's own run (2026-10-15): its one real poll waited 300 s and came back complete — too late to orphan
    # anything; the hook stays unfired, so harness:midrun_hooks says NEVER FIRED rather than a misleading orphan failure
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    assert _feed(w, [*seq[:7], ("poll_task", {"task_id": "compute-1", "wait": 300}, {"phase": "complete"})]) == []
    assert [r["out"] for r in w.unfired_records()] == ["NEVER FIRED"]


def test_when_endpoint_counts_only_a_call_that_acted_on_an_endpoint():
    w = hooks.HookWatcher([{"name": "at_stop", "after_tool": "stop_endpoint", "when_endpoint": True, "nth": 1, "cmd": "x"}])
    noop = ToolCall.of("mcp__hpc-bridge__stop_endpoint", {}, {"status": "down", "endpoint_id": None,
                                                               "notice": "no endpoint was up"})
    real = ToolCall.of("mcp__hpc-bridge__stop_endpoint", {}, {"status": "down", "endpoint_id": "e1",   # a real down
                                                               "notice": "compute block released over AMQP"})
    assert w.observe_call(noop) == []
    (d,) = w.observe_call(real)
    assert d["name"] == "at_stop" and d["call_index"] == 1 and d["endpoint_ids"] == ["e1"]
    # an EARLIER result's endpoint is not this call's: the condition is read on the call's own answer
    w = hooks.HookWatcher([{"name": "at_stop", "after_tool": "stop_endpoint", "when_endpoint": True, "cmd": "x"}])
    assert w.observe_call(ToolCall.of("mcp__hpc-bridge__ensure_endpoint_up", {}, {"status": "up", "endpoint_id": "e1"})) == []
    assert w.observe_call(noop) == [] and w.endpoint_ids == ["e1"]


def test_after_hook_counts_only_calls_that_end_after_the_named_hook_fired():
    import draining_restop as sc
    run = ("run_shell", {"shape": "compute", "command": "hostname"})
    # hermes21, live 2026-10-15 (1792092595): a run, a stop BEFORE the runs were done, a refused run, the second run
    # that ran, then a stop — the freeze after the second run that RAN, the thaw at the first stop after the freeze
    seq = [(*run, {"phase": "complete"}),
           ("stop_endpoint", {}, {"status": "down", "endpoint_id": "e1"}),
           (*run, {"phase": "needs_confirmation"}),
           (*run, {"phase": "complete"}),
           ("stop_endpoint", {}, {"status": "draining", "endpoint_id": "e1"})]
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    assert _feed(w, seq) == [("freeze_login_worker", 3), ("thaw_login_worker", 4)] and w.unfired == []
    # ...and a stop after the freeze that answered `down` anyway still thaws (CLEANUP then has nothing left to do)
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    assert _feed(w, [(*run, {"phase": "complete"}), (*run, {"phase": "complete"}),
                     ("stop_endpoint", {}, {"status": "down", "endpoint_id": "e1"})])[-1] == ("thaw_login_worker", 2)
    # a hook due on the very call the named hook fired on is not "after" it
    w2 = hooks.HookWatcher([{"name": "a", "after_tool": "run_shell", "cmd": "x"},
                            {"name": "b", "after_tool": "run_shell", "after_hook": "a", "cmd": "y"}])
    one = ("run_shell", {}, {"phase": "complete"})
    assert _feed(w2, [one]) == [("a", 0)] and _feed(w2, [one]) == [("b", 1)]
