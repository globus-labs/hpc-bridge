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


# -- an argument the call left out is the tool's own default (hermes21, draining_restop, live 2026-10-16) ----------------

def test_a_dict_when_input_reads_the_calls_effective_arguments():
    m = hooks._input_matches
    assert m({"shape": "compute"}, {"command": "hostname"}, "run_shell")            # run_shell's shape defaults to compute
    assert not m({"shape": "compute"}, {"command": "hostname", "shape": "login"}, "run_shell")
    assert m({"shape": "compute"}, {}, "reset_session") and m({"shape": "compute"}, {}, "ensure_endpoint_up")
    assert not m({"confirm_spend": True}, {}, "ensure_endpoint_up")                   # its default is False
    assert m({"confirm_spend": False}, {"shape": "compute"}, "ensure_endpoint_up")
    assert not m({"shape": "compute"}, {"command": "x"}, "login_shell")               # no such argument: no default
    assert not m("compute", {"command": "hostname"}, "run_shell")   # a regex reads what was SENT, never a default
    assert hooks.effective_input("run_shell", {"command": "c"}) == {"command": "c", "session_id": "default",
                                                                     "shape": "compute"}


def test_the_tool_defaults_are_the_servers_mcp_schema():
    # read from server.py's source (the jail's harness interpreter cannot import the server): pinned to what an MCP
    # client is told — every tool, every default, nothing else
    import asyncio

    server = pytest.importorskip("hpc_bridge.server")
    schema = {t.name: {k: p["default"] for k, p in (t.inputSchema.get("properties") or {}).items() if "default" in p}
              for t in asyncio.run(server.mcp.list_tools())}
    assert hooks.tool_defaults() == schema
    assert schema["run_shell"]["shape"] == "compute"   # the case the freeze missed


def test_tool_defaults_follow_a_constant_to_the_module_it_is_imported_from(tmp_path, capsys):
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "context.py").write_text('DEFAULT_SHAPE = "compute"\nWAIT = 2 * 60\nALIASED = DEFAULT_SHAPE\n')
    (pkg / "broken.py").write_text("def (:\n")
    (pkg / "server.py").write_text(
        "from .context import DEFAULT_SHAPE, WAIT as POLL_WAIT\nfrom pkg.context import ALIASED\n"
        "from .broken import GONE\nfrom elsewhere import OUTSIDE\nLIMIT: int = 5\nmcp = None\n\n"
        "@mcp.tool()\nasync def run(command: str, ctx, shape: str = DEFAULT_SHAPE, *, n: int = LIMIT, k=object()):\n"
        "    pass\n\n"
        '@mcp.tool(name="poll")\ndef _poll(task_id: str, wait: float = POLL_WAIT, neg: int = -LIMIT):\n    pass\n\n'
        '@mcp.tool("named")\ndef _named(a=ALIASED, b=GONE, c=OUTSIDE, d=1):\n    pass\n\n'
        "def helper(x=1):\n    pass\n")
    got = hooks.tool_defaults(pkg / "server.py")
    assert got == {"run": {"shape": "compute", "n": 5}, "poll": {"wait": 120, "neg": -5},
                   "named": {"a": "compute", "d": 1}}
    err = capsys.readouterr().err   # each default left out is SAID, never silently dropped
    assert "run(k=…): default not read (object())" in err and "named(c=…): default not read (OUTSIDE)" in err
    assert "named(b=…): default not read (SyntaxError" in err   # a followed module that does not parse: that one only


def test_tool_defaults_never_raise_and_say_when_they_read_nothing(tmp_path, capsys):
    # a hook watcher that raised would end the Claude SDK feed's run, or stop the journal follower (NEVER FIRED)
    (tmp_path / "broken.py").write_text("def (:\n")
    (tmp_path / "plain.py").write_text("def f(x=1):\n    pass\n")
    assert hooks.tool_defaults(tmp_path / "missing.py") == {}   # unreadable: when_input matches the args as sent
    assert hooks.tool_defaults(tmp_path / "broken.py") == {}
    assert hooks.tool_defaults(tmp_path / "plain.py") == {}
    err = capsys.readouterr().err
    assert err.count("could not read the tools' defaults") == 2 and "no @mcp.tool function in" in err
    # the real server, read afresh (past the cache): every default read, no warning
    assert hooks.tool_defaults.__wrapped__(hooks._SERVER_SRC) == hooks.tool_defaults()
    assert capsys.readouterr().err == ""


def test_the_freeze_fires_on_compute_runs_that_sent_no_shape():
    # hermes21's hpc-bridge calls in 1792179366-84683-56970 (the journal, verbatim args): both compute runs sent only
    # `{"command": "hostname"}`, and harness:midrun_hooks read NEVER FIRED for the freeze and the thaw
    import draining_restop as sc
    e1 = {"endpoint_id": "e1"}
    stored = [("connect_facility", {"facility": "f", "ssh_host": "login"}, {"phase": "proposed_facility_details"}),
              ("connect_facility", {"facility": "f", "details": {}}, {"phase": "provisioning"}),
              ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "provisioning", **e1}),
              ("ensure_endpoint_up", {}, {"status": "up", **e1}),
              ("run_shell", {"command": "hostname"}, {"phase": "complete"}),
              ("stop_endpoint", {}, {"status": "down", **e1}),
              ("run_shell", {"command": "hostname"}, {"phase": "needs_confirmation"}),
              ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "provisioning", **e1}),
              ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "up", **e1}),
              ("run_shell", {"command": "hostname"}, {"phase": "complete"}),
              ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "up", **e1}),
              ("stop_endpoint", {}, {"status": "down", **e1})]
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    # the freeze: the FIRST compute run that ran (nth 1 since the 2026-10-22 review: after the second, a CLI operator's
    # journal-fed hook raced the stop that came next); the thaw: the first stop after it
    assert _feed(w, stored) == [("freeze_login_worker", 4), ("thaw_login_worker", 5)] and w.unfired == []
    # the login shape, named, is not the compute shape
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    login = ("run_shell", {"command": "hostname", "shape": "login"}, {"phase": "complete"})
    assert _feed(w, [login, login, ("stop_endpoint", {}, {"status": "down", **e1})]) == []


def test_the_reap_fires_on_the_sdk_stream_when_the_call_sent_no_shape():
    # the during/after feed of the Claude SDK reads the ToolUseBlock's input as the model sent it
    import block_reaped_resume as sc
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    _start(w, "u0", "run_shell", {"command": "mkdir -p ~/hpcb_reap && cd ~/hpcb_reap && echo HPCB_REAP_7c1d > marker"})
    assert [d["name"] for d in _end(w, "u0", {"phase": "complete"})] == ["reap_block", "user_returns"]


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
    # conditions that could never fire (review of e126f41)
    *[({"after_tool": t, "cmd": "c", "when_endpoint": True}, "names no endpoint_id")
      for t in ("run_shell", "poll_task", "reset_session", "connect_facility")],
    *[({"after_tool": t, "cmd": "c", "when_phase": "down"}, "carries a status, not a phase")
      for t in ("ensure_endpoint_up", "stop_endpoint", "teardown_endpoint")],
    ({"after_tool": "poll_task", "cmd": "c", "when_phase": "runing"}, "is no phase poll_task answers"),
    ({"after_tool": "run_shell", "cmd": "c", "when_phase": "draining"}, "is no phase run_shell answers"),
    ({"after_tool": "list_facilities", "cmd": "c", "when_phase": "complete"}, "whose answer has no phase"),
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
    ("run_shell", {"command": "hostname"}, {"phase": "failed", "notice": _NO_FACILITY}),   # no shape sent: compute
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


def test_the_orphaned_task_hook_fires_on_the_run_that_answered_running_whatever_the_polling():
    import orphaned_task as sc
    task = "python3 -c \"import time; print('HPCB_CHAOS start', flush=True); time.sleep(300)\""
    seq = [_BATCH[0], _BATCH[2], _BATCH[3], _BATCH[5],      # the hermes21 batch: ensure, run_shell, the placeholder, stop
           ("connect_facility", {"facility": "f", "details": {}}, {"phase": "provisioning"}),
           ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "up", "endpoint_id": "e1"}),
           ("run_shell", {"shape": "login", "command": "sinfo"}, {"phase": "complete"}),
           ("run_shell", {"shape": "compute", "command": task}, {"phase": "running", "task_id": "compute-1"}),
           ("poll_task", {"task_id": "compute-1"}, {"phase": "running", "task_id": "compute-1"}),
           ("poll_task", {"task_id": "compute-1"}, {"phase": "failed", "notice": "task compute-1 is ORPHANED"})]
    # the task's own RUNNING answer — never the batch's failed run_shell, a placeholder poll, or a login command
    assert _feed(hooks.HookWatcher(sc.MIDRUN_HOOKS), seq) == [("kill_manager", 7)]
    # qwen under pi and codex (2026-10-22): one poll_task(wait=200) that waited the task out. The kill is already done
    # by then, so that poll meets the orphan instead of a `complete` (the old poll-keyed hook never fired)
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    assert _feed(w, [*seq[:8], ("poll_task", {"task_id": "compute-1", "wait": 200}, {"phase": "complete"})]) \
        == [("kill_manager", 7)]
    # a task that never answered running (it finished inline) leaves nothing to orphan: NEVER FIRED, not a product fail
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    assert _feed(w, [*seq[:7], ("run_shell", {"shape": "compute", "command": task}, {"phase": "complete"})]) == []
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
    e1 = {"endpoint_id": "e1"}
    # hermes21, live 2026-10-15 (1792092595), its hpc-bridge calls as the journal holds them: a run, a stop BEFORE the
    # runs were done, a refused run, a re-provision, the second run that RAN — and the trace ends there (no stop after)
    stored = [("list_facilities", {}, {}), ("connect_facility", {}, {"phase": "proposed_facility_details"}),
              ("connect_facility", {"details": {}}, {"phase": "provisioning"}),
              ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "provisioning", **e1}),
              ("ensure_endpoint_up", {}, {"status": "up", **e1}),
              (*run, {"phase": "complete"}),
              ("stop_endpoint", {}, {"status": "down", **e1}),
              (*run, {"phase": "needs_confirmation"}),
              *[("ensure_endpoint_up", {"confirm_spend": True}, {"status": st, **e1})
                for st in ("provisioning", "provisioning", "up", "up")],
              (*run, {"phase": "complete"})]
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    # nth 1 (the 2026-10-22 review): the first run that RAN freezes (5), the stop after it thaws (6) — never the
    # refused `needs_confirmation` run (7), which the old hooks froze on
    assert _feed(w, stored) == [("freeze_login_worker", 5), ("thaw_login_worker", 6)] and w.unfired == []
    # a stop BEFORE the freeze thaws nothing: the thaw is the first stop after it
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    early = [("stop_endpoint", {}, {"status": "down", **e1}), (*run, {"phase": "needs_confirmation"}),
             (*run, {"phase": "complete"})]
    assert _feed(w, early) == [("freeze_login_worker", 2)] and [h["name"] for h in w.unfired] == ["thaw_login_worker"]
    assert _feed(w, [("stop_endpoint", {}, {"status": "draining", **e1})]) == [("thaw_login_worker", 3)]
    # ...and a stop after the freeze that answered `down` anyway still thaws (CLEANUP then has nothing left to do)
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    assert _feed(w, [(*run, {"phase": "complete"}), (*run, {"phase": "complete"}),
                     ("stop_endpoint", {}, {"status": "down", "endpoint_id": "e1"})])[-1] == ("thaw_login_worker", 2)
    # a hook due on the very call the named hook fired on is not "after" it
    w2 = hooks.HookWatcher([{"name": "a", "after_tool": "run_shell", "cmd": "x"},
                            {"name": "b", "after_tool": "run_shell", "after_hook": "a", "cmd": "y"}])
    one = ("run_shell", {}, {"phase": "complete"})
    assert _feed(w2, [one]) == [("a", 0)] and _feed(w2, [one]) == [("b", 1)]


# -- the answer tables against the product, and every hook fires on a good run ---------------------------------------------

def test_the_answer_tables_are_the_products():
    # hook_problems refuses a condition that could never fire from these tables: they must be what the tools answer
    import typing

    from hpc_bridge import models, server
    phases = {m: typing.get_args(m.model_fields["phase"].annotation)
              for m in (models.ShellOutcome, models.ConnectFacilityResult, models.LoginStatus, models.PreauthStatus,
                        models.BatchStatus)}
    for tool, values in hooks.ANSWER_PHASES.items():
        ret = typing.get_type_hints(getattr(server, {"status": "status_tool"}.get(tool, tool)))["return"]
        assert "phase" in ret.model_fields and "endpoint_id" not in ret.model_fields, tool
        assert set(values) == set(phases[ret]), tool
    for tool in hooks.ENDPOINT_ANSWERS:
        ret = typing.get_type_hints(getattr(server, tool))["return"]
        assert ret is models.EndpointStatus and "phase" not in ret.model_fields and "endpoint_id" in ret.model_fields


# A good run of each hooked scenario, abridged from its newest OK bundle's hpc-bridge calls (repeated connects / polls
# and login runs no hook reads dropped; only the fields a hook reads; endpoint ids shortened). Every after-hook must
# fire, on the same call it fired on live (its index here is the abridged one).
_E = {"endpoint_id": "e1"}
_GOOD = {
    "block_reaped_resume": ("1791901834 (claude)", [
        ("connect_facility", {}, {"phase": "proposed_facility_details"}), ("connect_facility", {}, {"phase": "needs_account"}),
        ("run_shell", {"shape": "login", "command": "sinfo"}, {"phase": "complete"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", **_E}),
        ("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", **_E}),
        ("run_shell", {"shape": "compute", "command": "mkdir -p ~/hpcb_reap && cd ~/hpcb_reap && echo HPCB_REAP_7c1d > marker"},
         {"phase": "complete"}),
        ("ensure_endpoint_up", {"shape": "compute"}, {"status": "needs_confirmation", **_E}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "up", **_E}),
        ("run_shell", {"shape": "compute", "command": "cat marker && hostname && pwd"}, {"phase": "complete"}),
        ("stop_endpoint", {}, {"status": "down", **_E})], {"reap_block": 5, "user_returns": 5}),
    "draining_restop": ("1792089638 (pi)", [
        ("connect_facility", {}, {"phase": "proposed_facility_details"}), ("connect_facility", {}, {"phase": "needs_account"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", **_E}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "up", **_E}),
        ("run_shell", {"shape": "compute", "command": "hostname"}, {"phase": "complete"}),
        ("run_shell", {"shape": "compute", "command": "hostname"}, {"phase": "complete"}),
        ("stop_endpoint", {}, {"status": "draining", **_E}), ("stop_endpoint", {}, {"status": "down", **_E})],
        {"freeze_login_worker": 4, "thaw_login_worker": 6}),   # the freeze after the FIRST run since nth 1 (2026-10-22)
    "fake_mep_stop_names_block": ("1792101787 (hermes21)", [
        ("connect_facility", {}, {"phase": "needs_account"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", **_E}),
        ("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", **_E}),
        ("run_shell", {"shape": "compute", "command": "echo JOB=$SLURM_JOB_ID; hostname; whoami"}, {"phase": "complete"}),
        ("stop_endpoint", {}, {"status": "draining", **_E})], {"block_listed_at_stop": 4}),
    "fake_mep_stop_while_provisioning": ("1792103060 (hermes21)", [
        ("connect_facility", {}, {"phase": "needs_account"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", **_E}),
        ("stop_endpoint", {}, {"status": "draining", **_E})], {"queue_at_stop": 2}),
    "fake_pbs_mep_compute": ("1792104316 (hermes21)", [
        ("connect_facility", {}, {"phase": "needs_account"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", **_E}),
        ("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", **_E}),
        ("run_shell", {"shape": "compute", "command": "echo JOB=$PBS_JOBID QUEUE=$PBS_QUEUE; whoami"}, {"phase": "complete"}),
        ("stop_endpoint", {}, {"status": "draining", **_E})], {"block_listed_at_stop": 4}),
    "orphaned_task": ("1792104913 (codex)", [
        ("connect_facility", {}, {"phase": "proposed_facility_details"}), ("connect_facility", {}, {"phase": "provisioning"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", **_E}),
        ("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", **_E}),
        ("run_shell", {"shape": "compute", "command": "python3 -c \"print('HPCB_CHAOS start')…\""},
         {"phase": "running", "task_id": "compute-1"}),
        ("poll_task", {"task_id": "compute-1", "wait": 30}, {"phase": "running", "task_id": "compute-1"}),
        ("poll_task", {"task_id": "compute-1", "wait": 30}, {"phase": "failed"}),
        ("stop_endpoint", {}, {"status": "down", **_E})], {"kill_manager": 4}),
    "reap_replaced_block": ("1792093389 (hermes21)", [
        ("connect_facility", {}, {"phase": "proposed_facility_details"}), ("connect_facility", {}, {"phase": "needs_account"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "up", **_E}),
        ("run_shell", {"shape": "login", "command": "hostname"}, {"phase": "complete"}),
        ("run_shell", {"shape": "compute", "command": "mkdir -p ~/hpcb_rr && cd ~/hpcb_rr && echo HPCB_RR_9f3e > marker"},
         {"phase": "complete"}),
        ("run_shell", {"shape": "compute", "command": "echo JOB=$SLURM_JOB_ID"}, {"phase": "failed"}),
        ("stop_endpoint", {}, {"status": "down", **_E})], {"cancel_block": 4}),
    "spend_revoked": ("1788654391 (claude)", [
        ("connect_facility", {}, {"phase": "proposed_facility_details"}), ("connect_facility", {}, {"phase": "needs_account"}),
        ("run_shell", {"shape": "login", "command": "sinfo"}, {"phase": "complete"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning", **_E}),
        ("stop_endpoint", {}, {"status": "down", **_E})], {"user_revokes_spend": 3}),
    # synthetic until its first live OK run (batch phase 1): the polling hook is armed by the run_batch answer
    "batch_block_reaped_midrun": ("synthetic (batch phase 1, no live bundle yet)", [
        ("connect_facility", {}, {"phase": "proposed_facility_details"}), ("connect_facility", {}, {"phase": "needs_account"}),
        ("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "up", **_E}),
        ("run_batch", {"command": "sleep 15; echo row {n}", "rows": [{"n": 0}], "task_walltime_s": 60},
         {"phase": "admitting", "batch_id": "batch-000000000001"}),
        ("status", {"id": "batch-000000000001"}, {"phase": "needs_confirmation"})], {"reap_after_5_results": 3}),
}


def test_every_hooked_scenario_has_a_good_run_that_fires_its_hooks():
    during = {n for n in HOOKED if any(hooks.is_during(h) for h in importlib.import_module(n).MIDRUN_HOOKS)}
    assert set(_GOOD) | during == set(HOOKED), "a hooked scenario with no good run here"
    assert during == {"esc_mid_call"}


@pytest.mark.parametrize("name", sorted(_GOOD))
def test_a_good_run_fires_every_hook_on_the_call_it_fired_on_live(name):
    mod = importlib.import_module(name)
    _bundle, seq, want = _GOOD[name]
    w = hooks.HookWatcher(mod.MIDRUN_HOOKS)
    assert dict(_feed(w, seq)) == want and w.unfired == [], name


def test_esc_mid_call_presses_esc_during_its_marker_run():
    # a during hook: the stored run (1791918619, claude) pressed Esc 10 s into the marker command, still in flight
    import esc_mid_call as sc
    w = hooks.HookWatcher(sc.MIDRUN_HOOKS)
    _start(w, "u0", "run_shell", {"shape": "compute", "command": "hostname"})
    _end(w, "u0", {"phase": "complete"})
    assert w.take_armed() == []
    _start(w, "u1", "run_shell", {"shape": "compute", "command": sc._CMD})
    (armed,) = w.take_armed()
    hook = w.due_during(armed)
    assert hook is not None and hook["name"] == "esc" and hook["call_index"] == 1 and w.unfired == []
