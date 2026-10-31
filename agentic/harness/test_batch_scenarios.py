"""Hermetic: batch phase 1's harness work (design §10) — run_batch as a billed start, the journal's event rows, the
polling hook, the script operator's server restart / second server / journal hooks — and the seven scenarios' graders
and world checks (agentic/harness/batch_checks.py), on synthetic traces. No server, no cluster."""
from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import sqlite3
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import batch_checks as bc  # noqa: E402
import cli_runner  # noqa: E402
import midrun_hooks as hooks  # noqa: E402
import script_runner  # noqa: E402
from invariants import Result, ToolCall, Trace, _billed_start_idxs  # noqa: E402

BATCH_SCENARIOS = ["batch_fanout_collect", "batch_crosses_walltime", "batch_restart_resume",
                   "batch_two_servers_one_ledger", "batch_block_reaped_midrun", "mep_batch_fanout", "batch_big_output",
                   "batch_abort_midrun"]
BID = "batch-0123456789ab"


def _c(name, inp=None, res=None, *, t=None, server=""):
    c = ToolCall.of(f"mcp__hpc-bridge__{name}", inp or {}, res)
    if t is not None:
        c.t_call, c.t_result = t
    c.server = server
    return c


def _status(phase="admitting", results=(), counts=None, **extra):
    return {"batch_id": BID, "phase": phase, "tasks": 10, "counts": counts or {}, "results": list(results), **extra}


# ---- run_batch as a billed start, events, t0 ------------------------------------------------------------------------

def test_run_batch_with_confirm_spend_is_a_billed_start_and_a_refusal_is_not():
    t = Trace([
        _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "up", "block": {"queue": "main"}}),
        _c("run_batch", {"command": "x", "rows": [1]}, _status()),                                    # no confirm: rides it
        _c("run_batch", {"batch_id": BID, "confirm_spend": True}, _status(block={"queue": "main"})),   # the resume: a start
        _c("run_batch", {"batch_id": BID, "confirm_spend": "true"}, _status("refused")),
        _c("run_batch", {"batch_id": BID, "confirm_spend": True}, _status("needs_account")),
        _c("run_batch", {"batch_id": BID, "confirm_spend": True}, {"text": "Unknown tool: run_batch", "is_error": True}),
        _c("run_batch", {"shape": "login", "confirm_spend": True}, _status()),
    ])
    assert _billed_start_idxs(t) == [0, 2]
    assert bc.one_confirmation(t).ok is False                    # two receipts: the first block and the resume's


def test_build_trace_puts_the_servers_event_rows_in_events_not_calls():
    rows = [{"ts": 100.0, "ms": 50, "seq": 1, "pid": 7, "tool": "run_batch", "args": {"command": "x"},
             "result": _status()},
            {"ts": 100.2, "pid": 7, "event": "batch.admit", "batch_id": BID, "index": 0, "attempt": 1, "capacity": 1,
             "in_flight": 1, "remaining_s": 900, "need_s": 90},
            {"ts": 101.0, "ms": 20, "seq": 2, "pid": 7, "tool": "status", "args": {"id": BID}, "result": _status()}]
    events = [cli_runner.Native("run_batch", {"command": "x"}, None, True),
              cli_runner.Native("status", {"id": BID}, None, True)]
    trace, stats = cli_runner.build_trace(rows, events, [])
    assert [c.name for c in trace.calls] == ["run_batch", "status"]
    assert stats["journal_unplaced"] == 0 and stats["events"] == 1
    assert trace.events[0]["event"] == "batch.admit" and trace.t0 == 100.0
    assert bc.epoch(trace, trace.calls[1].t_call) == 101.0


async def _follow(rows):
    """follow_journal over rows: what fired (an event row is no call and arms nothing)."""
    import tempfile
    path = Path(tempfile.mkdtemp()) / "journal.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    w = hooks.HookWatcher([{"name": "h", "after_tool": "run_batch", "cmd": "true"}])
    fired: list[dict] = []
    stop = asyncio.Event()
    stop.set()

    async def act(h):
        fired.append(h)

    await hooks.follow_journal(path, w, act, stop=stop, to_call=cli_runner._journal_call)
    return fired


def test_an_event_row_arms_no_hook():
    assert asyncio.run(_follow([{"event": "batch.admit", "ts": 1}])) == []
    assert len(asyncio.run(_follow([{"tool": "run_batch", "args": {}, "result": _status(), "ts": 1}]))) == 1


# ---- the polling hook -----------------------------------------------------------------------------------------------

def test_a_polling_hook_is_checked():
    ok = {"name": "p", "after_tool": "run_batch", "poll": "ls | wc -l", "until": r"^\s*5", "cmd": "scancel 1"}
    assert hooks.hook_problems([ok]) == []
    assert any("needs `until`" in p for p in hooks.hook_problems([{**ok, "until": ""}]))
    assert any("not a regex" in p for p in hooks.hook_problems([{**ok, "until": "("}]))
    assert any("acts with a cmd" in p for p in hooks.hook_problems([{k: v for k, v in ok.items() if k != "cmd"}]))
    assert any("every_s" in p for p in hooks.hook_problems([{**ok, "every_s": 0}]))
    assert any("belong to a polling hook" in p for p in hooks.hook_problems([{"after_tool": "x", "cmd": "y", "until": "z"}]))
    assert hooks.operator_refusal("script", persona=None, phases=False, hooks=[ok]) is None
    assert hooks.operator_refusal("pi", persona=None, phases=False, hooks=[ok]) is None


def test_hook_text_fills_the_trigger_results_fields():
    h = {"endpoint_ids": ["e-1", "e-2"], "result": {"results_dir": "/s/b", "n": 3, "none": None}}
    got = hooks.hook_text(h, "ls {result:results_dir} {eid} {endpoint_name} {result:n} [{result:none}] [{result:x}]",
                          endpoint_name="ep")
    assert got == "ls /s/b e-2 ep 3 [] []"


def test_run_hook_polls_until_the_world_matches_then_acts(harness_run, monkeypatch):
    import run
    calls: list[str] = []
    counts = iter(["1", "3", "5", "7"])

    def fake_ssh(cmd, *, timeout=60, host=None):
        calls.append(cmd)
        if cmd.startswith("ls"):
            return 0, next(counts) + "\n"
        return 0, "reaped 42\n"

    monkeypatch.setattr(run, "_ssh_run", fake_ssh)
    hook = {"name": "reap", "after_tool": "run_batch", "poll": "ls {result:results_dir} | wc -l", "until": r"^\s*[5-9]",
            "every_s": 0.5, "cmd": "scancel by {eid}", "expect_any_host": "reaped", "endpoint_ids": ["e1"],
            "result": {"results_dir": "/r"}}
    out = asyncio.run(run._run_hook(hook))
    assert out["rc"] == 0 and calls == ["ls /r | wc -l"] * 3 + ["scancel by e1"]
    # never matching within its timeout: the cmd does not run, the hook fails
    calls.clear()
    monkeypatch.setattr(run, "_ssh_run", lambda cmd, *, timeout=60, host=None: (calls.append(cmd), (0, "1\n"))[1])
    out = asyncio.run(run._run_hook({**hook, "poll_timeout_s": 1.0}))
    assert out["rc"] == 1 and "never matched" in out["out"] and all(c.startswith("ls") for c in calls)


# ---- the script operator: hooks, restart, a second server -------------------------------------------------------------

class _Server:
    """Stand-in MCP sessions: each `factory()` is a new 'process' (its own pid label in the journal rows); `answers`
    is tool -> callable(args) -> payload."""

    def __init__(self, answers, pid_of=None):
        self.answers = answers
        self.opened = 0
        self.closed = 0
        self.pid_of = pid_of or (lambda n: 50_000 + n)
        self.calls: list[tuple[int, str, dict]] = []

    def factory(self, cmd, env, cwd):
        journal = Path(env["HPC_BRIDGE_JOURNAL"])
        self.opened += 1
        n, server = self.opened, self

        class Session:
            async def call_tool(self, name, args, read_timeout_seconds=None):
                started = time.time()   # the real journal's `ts`: when the call STARTED (it writes the row at its end)
                server.calls.append((n, name, dict(args)))
                if isinstance(args.get("delay"), int | float):
                    await asyncio.sleep(args["delay"])
                fn = server.answers.get(name)
                got = fn(dict(args)) if fn else {}
                row = {"ts": started, "ms": 5, "seq": len(server.calls), "pid": server.pid_of(n), "tool": name,
                       "args": args, "result": got}
                with journal.open("a") as fh:
                    fh.write(json.dumps(row) + "\n")
                if isinstance(got, dict) and got.get("is_error"):
                    return types.SimpleNamespace(isError=True, structuredContent=None,
                                                 content=[types.SimpleNamespace(text=got["text"])])
                return types.SimpleNamespace(isError=False, structuredContent=got,
                                             content=[types.SimpleNamespace(text=json.dumps(got))])

        @contextlib.asynccontextmanager
        async def cm():
            try:
                yield Session()
            finally:
                server.closed += 1

        return cm()


def _stub_sdk(monkeypatch, tmp_path):
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "ClaudeSDKClient", "PermissionResultAllow"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    sys.modules.pop("runner", None)
    monkeypatch.setenv("HOME", str(tmp_path))


def _drive(script, server, **kw):
    return asyncio.run(script_runner.run_scenario("p", repo_root=HERE.parents[1], script=script, facility="fake-1",
                                                  ssh_host="login", session_factory=server.factory, **kw))


def test_the_script_operator_serves_a_cmd_hook_from_the_journal(monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    ran: list[dict] = []

    async def runner(hook):
        ran.append(hook)
        return {"rc": 0, "out": "ok"}

    async def script(ops):
        await ops.call("run_shell", command="hostname")
        await ops.call("run_batch", command="x", rows=[1], task_walltime_s=5)
        await ops.sleep(1.2)    # the follower polls the journal every 0.5 s
        return "done"

    res = _drive(script, _Server({"run_batch": lambda a: _status()}),
                 midrun_hooks=[{"name": "h", "after_tool": "run_batch", "cmd": "true"}], hook_runner=runner)
    assert [h["name"] for h in ran] == ["h"] and res.hooks_fired[0]["rc"] == 0


def test_restart_server_kills_the_process_and_the_steps_go_on_against_a_new_one(monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    victim = subprocess.Popen(["sleep", "60"])
    server = _Server({"status": lambda a: _status()}, pid_of=lambda n: victim.pid if n == 1 else 60_000 + n)

    async def script(ops):
        await ops.call("status", id=BID)
        rec = await ops.restart_server()
        await ops.call("status", id=BID)
        return json.dumps(rec)

    res = _drive(script, server)
    assert victim.wait(timeout=10) == -9                            # SIGKILL: a crash, not a shutdown
    rec = json.loads(res.final.result)
    assert rec["killed_pid"] == victim.pid and server.opened == 2 and server.closed == 2
    assert [n for n, _tool, _a in server.calls] == [1, 2]
    head = next(m for m in res.messages if "harness" in m)
    assert head["restarts"][0]["killed_pid"] == victim.pid
    assert bc.restart_index(res.trace) == 1


def test_a_second_server_labels_its_calls_and_the_bundle_keeps_the_labels(monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    server = _Server({"status": lambda a: _status(owner="pid 1 on h"),
                      "run_batch": lambda a: _status("refused", reason="owned by pid 1")})

    async def script(ops):
        await ops.call("status", id=BID)
        b = await ops.open_server("B")
        await b.call("status", id=BID)
        await b.call("run_batch", batch_id=BID, confirm_spend=True)
        await ops.call("status", id=BID)
        return "done"

    res = _drive(script, server)
    assert [c.server for c in res.trace.calls] == ["A", "B", "B", "A"] and server.opened == 2
    again = cli_runner.trace_from_bundle_messages(res.messages)
    assert [c.server for c in again.calls] == ["A", "B", "B", "A"]
    assert bc.second_server_reads_ledger(res.trace).ok


# ---- the graders ------------------------------------------------------------------------------------------------------

def _ev(kind, ts, index, *, attempt=1, pid=7, **kw):
    return {"event": kind, "ts": ts, "pid": pid, "batch_id": BID, "index": index, "attempt": attempt, **kw}


def test_the_window_counts_admissions_against_settlements_and_main_run_shell_calls():
    evs = [_ev("batch.admit", 1, 0, capacity=1), _ev("batch.settle", 2, 0), _ev("batch.admit", 3, 1, capacity=1)]
    t = Trace([_c("run_batch", {}, _status())], events=evs)
    assert bc.window(t).peak == 1 and bc.window_within_capacity(t, workers=2).ok
    t2 = Trace([_c("run_batch", {}, _status())], events=evs + [_ev("batch.admit", 3.5, 2, capacity=1)])
    assert not bc.window_within_capacity(t2, workers=2).ok
    # main: 3 concurrent run_shell calls, two handed back as handles and polled later
    shells = [_c("run_shell", {"shape": "compute"}, {"phase": "running", "task_id": f"compute-{i}"}, t=(0.0, 1.0))
              for i in range(2)] + [_c("run_shell", {"shape": "compute"}, {"phase": "complete"}, t=(0.0, 5.0))]
    polls = [_c("poll_task", {"task_id": "compute-0"}, {"phase": "complete"}, t=(9.0, 10.0))]
    w = bc.window(Trace(shells + polls))
    assert w.peak == 3 and not bc.window_within_capacity(Trace(shells + polls), workers=2).ok


def test_batch_used_and_failures_have_rc():
    rows = [{"index": i, "state": "failed" if i == 7 else "done", "exit_code": 3 if i == 7 else 0} for i in range(10)]
    t = Trace([_c("run_batch", {}, _status()), _c("status", {"id": BID}, _status("done", rows))], ["n=7 exit code 3"])
    assert bc.batch_used(t, rows=10).ok and bc.failures_have_rc(t, failing={7: 3}).ok
    assert bc.agent_reports_failures(t, failing={7: 3}).ok
    assert not bc.failures_have_rc(t, failing={7: 3, 8: 3}).ok
    loop = Trace([_c("run_batch", {}, {"text": "Unknown tool: run_batch", "is_error": True})]
                 + [_c("run_shell", {"shape": "compute"}, {"phase": "complete"}) for _ in range(40)])
    r = bc.batch_used(loop, rows=40)
    assert not r.ok and "not a tool" in r.detail


def test_no_admission_under_margin_reads_the_events_or_mains_start_lines():
    ok = Trace([], events=[_ev("batch.admit", 1, 0, remaining_s=200, need_s=90)])
    bad = Trace([], events=[_ev("batch.admit", 1, 0, remaining_s=200, need_s=90),
                            _ev("batch.admit", 2, 1, remaining_s=80, need_s=90)])
    assert bc.no_admission_under_margin(ok).ok and not bc.no_admission_under_margin(bad).ok
    main = Trace([_c("run_batch", {"task_walltime_s": 60}, {"text": "Unknown tool", "is_error": True}),
                  _c("poll_task", {}, {"phase": "complete", "stdout": f"{bc.STARTED} 3 1000 77 41\nrow 3\n"})])
    r = bc.no_admission_under_margin(main)
    assert not r.ok and "[3]" in r.detail


def test_reask_named_and_nothing_admitted_before_the_yes():
    ask = _c("status", {"id": BID}, _status("needs_confirmation", reason="the block's walltime is nearly over"),
             t=(10.0, 11.0))
    resume = _c("run_batch", {"batch_id": BID, "confirm_spend": True}, _status(), t=(20.0, 21.0))
    t = Trace([ask, resume], events=[_ev("batch.admit", 125.0, 4)], t0=100.0)
    assert bc.reask_named(t, words=("walltime",), name="r").ok
    early = Trace([ask, resume], events=[_ev("batch.admit", 115.0, 4)], t0=100.0)
    assert not bc.reask_named(early, words=("walltime",), name="r").ok
    assert not bc.reask_named(Trace([resume]), words=("walltime",), name="r").ok


def test_every_row_returned_after_the_restart_and_nothing_admitted_before_the_resume():
    rows = [{"index": i, "state": "done"} for i in range(4)]
    calls = [_c("status", {"id": BID}, _status("admitting", rows[:2]), t=(0, 1)),
             _c("status", {"id": BID}, _status("needs_confirmation", rows, reason="the server restarted"), t=(5, 6)),
             _c("run_batch", {"batch_id": BID, "confirm_spend": True}, _status(), t=(8, 9))]
    t = Trace(calls, ["server A killed (SIGKILL, pid 9) and started again"], [1], events=[_ev("batch.admit", 109, 3)],
              t0=100.0)
    assert bc.every_row_returned(t, rows=4, after=bc.restart_index(t)).ok
    assert bc.nothing_admitted_before_resume(t).ok
    t.events = [_ev("batch.admit", 106.5, 3)]
    assert not bc.nothing_admitted_before_resume(t).ok
    assert not bc.every_row_returned(Trace(calls[:1]), rows=4).ok


def test_only_holder_admits():
    b_status = _c("status", {"id": BID}, _status(owner="pid 7"), server="B")
    b_try = _c("run_batch", {"batch_id": BID, "confirm_spend": True}, _status("refused"), server="B")
    t = Trace([_c("run_batch", {}, _status(), server="A"), b_status, b_try],
              events=[_ev("batch.admit", 1, 0, pid=7), _ev("batch.admit", 2, 1, pid=7)])
    assert bc.only_holder_admits(t).ok
    t.events.append(_ev("batch.admit", 3, 2, pid=8))
    assert not bc.only_holder_admits(t).ok
    main = Trace([_c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "up"}, server="B"),
                  _c("run_shell", {"shape": "compute"}, {"phase": "complete"}, server="B")])
    assert not bc.only_holder_admits(main).ok


def test_lost_on_evidence_backlog_kept_and_the_mep_stop():
    rows = [{"index": 5, "state": "lost", "settled_by": "evidence: sacct says job 12 CANCELLED", "job_id": "12"},
            {"index": 6, "state": "done", "job_id": "12"}]
    ask = _status("needs_confirmation", rows, counts={"backlog": 3, "done": 6, "lost": 1}, reason="block 12 was reaped")
    t = Trace([_c("status", {"id": BID}, ask)])
    assert bc.inflight_lost_with_evidence(t).ok and bc.backlog_survives(t, rows=10).ok
    assert not bc.inflight_lost_with_evidence(Trace([_c("status", {}, _status(results=[{**rows[0], "settled_by": "presumption"}]))])).ok
    stop = _c("stop_endpoint", {}, {"status": "draining", "spend_stopped": False, "notice": "block job 12 idles out"})
    assert bc.mep_stop_names_jobs(Trace([_c("status", {}, _status(results=rows)), stop])).ok
    assert not bc.mep_stop_names_jobs(Trace([_c("status", {}, _status(results=rows)),
                                             _c("stop_endpoint", {}, {"status": "down"})])).ok
    summary = _c("status", {"id": BID}, {**_status(), "blocks_seen": ["12"]})   # a summary lists no rows
    assert bc.mep_stop_names_jobs(Trace([summary, stop])).ok
    assert not bc.mep_stop_names_jobs(Trace([_c("status", {"id": BID}, _status()), stop])).ok


def test_nothing_admitted_after_the_stop():
    stop = _c("stop_endpoint", {}, {"status": "up"}, t=(10.0, 11.0))
    rows = [{"index": 0, "state": "done", "started_at": 105.0}, {"index": 1, "state": "done", "started_at": 109.0}]
    t = Trace([_c("status", {}, _status(results=rows)), stop],
              events=[_ev("batch.admit", 104.0, 0), _ev("batch.admit", 108.0, 1)], t0=100.0)
    assert bc.nothing_admitted_after_stop(t).ok
    t.events.append(_ev("batch.admit", 112.0, 2))
    assert not bc.nothing_admitted_after_stop(t).ok
    main = Trace([stop, _c("poll_task", {}, {"phase": "complete", "stdout": f"{bc.STARTED} 4 140 12 null\n"})], t0=100.0)
    assert not bc.nothing_admitted_after_stop(main).ok


def test_big_output_truncated():
    row = {"index": 1, "state": "done", "attempt": 1, "truncated": True, "stdout_bytes": 1_000_000,
           "stdout_path": "/h/r/tasks/1/attempt-1/stdout", "tail": "last line\n"}
    assert bc.big_output_truncated(Trace([_c("status", {}, _status(results=[row]))]), index=1, full=12_000_011).ok
    main = Trace([_c("poll_task", {}, {"phase": "failed", "notice": "… over Globus Compute's result limit …"})])
    assert not bc.big_output_truncated(main, index=1, full=12_000_011).ok


# ---- the world checks -------------------------------------------------------------------------------------------------

def _sacct_out(rows, now=2_000_000_000):
    lines = ["|".join(r) for r in rows]
    return "\n".join(lines) + f"\nHPCB_RC=0\nHPCB_NOW {now} 2033-05-18T03:33:20\n"


def _ctx(trace, ssh, messages=None):
    return types.SimpleNamespace(trace=trace, messages=messages or [], ssh=ssh, tap=None, endpoint_name="ep")


def _ledger(tmp_path, tasks, blocks_seen=("12",)):
    p = tmp_path / "hpc-bridge-state"
    p.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(p / "ledger.sqlite3")
    con.execute("create table batch (id text primary key, blocks_seen text)")
    con.execute("create table task (batch_id text, idx integer, state text, job_id text, admitted_at real, "
                "settled_at real, started_at real)")
    con.execute("insert into batch values (?, ?)", (BID, json.dumps(list(blocks_seen))))
    con.executemany("insert into task values (?,?,?,?,?,?,?)", [(BID, *r) for r in tasks])
    con.commit()
    con.close()


EID = "11111111-2222-3333-4444-555555555555"


def _pilot(job, state, submit, start, end):
    return [job, "parsl.slurm.block", state, submit, start, end, f"sbatch /h/.globus_compute/uep.{EID}.1/s.sh"]


def test_ledger_matches_sacct(tmp_path):
    t = Trace([_c("ensure_endpoint_up", {}, {"endpoint_id": EID}), _c("run_batch", {}, _status())])
    sacct = _sacct_out([_pilot("12", "RUNNING", "2033-05-18T03:20:00", "2033-05-18T03:20:05", "Unknown")])
    _ledger(tmp_path, [(0, "done", "12", 1_999_999_300.0, 1_999_999_400.0, 1_999_999_310.0)])
    got = asyncio.run(bc.ledger_matches_sacct(_ctx(t, lambda cmd, timeout=60, host=None: (0, sacct))))
    assert got.ok, got.detail
    # a second pilot ran during the batch and the ledger never heard of it
    sacct2 = _sacct_out([_pilot("12", "CANCELLED", "2033-05-18T03:20:00", "2033-05-18T03:20:05", "2033-05-18T03:22:20"),
                         _pilot("13", "RUNNING", "2033-05-18T03:22:25", "2033-05-18T03:22:30", "Unknown")])
    got = asyncio.run(bc.ledger_matches_sacct(_ctx(t, lambda cmd, timeout=60, host=None: (0, sacct2))))
    assert not got.ok and "13" in got.detail


def test_ledger_matches_sacct_fails_with_no_ledger(tmp_path):
    t = Trace([_c("run_batch", {}, _status())])
    got = asyncio.run(bc.ledger_matches_sacct(_ctx(t, lambda *a, **k: (0, ""))))
    assert not got.ok and "no ledger" in got.detail


def test_no_job_before_reask():
    confirm = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
                 {"status": "up", "endpoint_id": EID, "block": {"queue": "main"}}, t=(0.0, 1.0))
    t = Trace([confirm], t0=1_999_999_000.0)
    one = _sacct_out([_pilot("12", "COMPLETED", "2033-05-18T03:16:41", "2033-05-18T03:16:42", "2033-05-18T03:20:42")])
    assert asyncio.run(bc.no_job_before_reask(_ctx(t, lambda *a, **k: (0, one)))).ok
    two = _sacct_out([_pilot("12", "TIMEOUT", "2033-05-18T03:16:41", "2033-05-18T03:16:42", "2033-05-18T03:20:42"),
                      _pilot("13", "RUNNING", "2033-05-18T03:20:50", "2033-05-18T03:20:51", "Unknown")])
    got = asyncio.run(bc.no_job_before_reask(_ctx(t, lambda *a, **k: (0, two))))
    assert not got.ok and "13" in got.detail


def test_the_start_files_grade_the_margin_and_a_row_run_twice():
    t = Trace([_c("run_batch", {"task_walltime_s": 60}, _status(results_dir="/h/r"))], ["HPCB_RESULTS_DIR /h/r"])
    out = ("HPCB_META /h/r/tasks/0/attempt-1/meta.json " + json.dumps({"index": 0, "remaining_s": 200}) + "\n"
           "HPCB_META /h/r/tasks/1/attempt-1/meta.json " + json.dumps({"index": 1, "remaining_s": 40}) + "\n"
           "HPCB_META /h/r/tasks/1/attempt-2/meta.json " + json.dumps({"index": 1, "remaining_s": 230}) + "\nHPCB_DONE\n")
    ctx = _ctx(t, lambda *a, **k: (0, out))
    margin = asyncio.run(bc.tasks_started_with_margin(ctx))
    twice = asyncio.run(bc.no_row_ran_twice(ctx))
    assert not margin.ok and "row 1 (40s)" in margin.detail
    assert not twice.ok and "[1]" in twice.detail


# ---- the scenarios ----------------------------------------------------------------------------------------------------

_BUILTIN = {"agent_engaged", "ends_with_stop", "stop_is_honest"}


@pytest.mark.parametrize("name", BATCH_SCENARIOS)
def test_each_batch_scenario_is_wired(name):
    mod = importlib.import_module(name)
    graders = {fn(Trace([])).name for fn in mod.EXTRA_INVARIANTS}
    assert set(mod.EXPECT_OK) - _BUILTIN <= graders, set(mod.EXPECT_OK) - _BUILTIN - graders
    assert callable(mod.SCRIPT) and mod.TARGETS == ("fake",)
    assert hooks.hook_problems(getattr(mod, "MIDRUN_HOOKS", None)) == []
    for fn in mod.EXTRA_INVARIANTS:
        assert isinstance(fn(Trace([])), Result)        # an empty trace is a failed premise, never a crash
    assert all(fn.__name__ for fn in getattr(mod, "WORLD_CHECKS", []))


def _connect_answers():
    seq = iter([{"phase": "proposed_facility_details", "proposed_details": {"ssh_host": "login", "env_setup": "x"}},
                {"phase": "needs_account"}])
    return lambda a: next(seq, {"phase": "needs_account"})


def test_the_fanout_script_takes_mains_fallback_when_run_batch_is_no_tool(monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    mod = importlib.import_module("batch_fanout_collect")
    server = _Server({
        "connect_facility": _connect_answers(),
        "ensure_endpoint_up": lambda a: {"status": "up", "endpoint_id": EID, "block": {"queue": "main"}},
        "run_batch": lambda a: {"text": "Unknown tool: run_batch"},   # FastMCP's answer: plain text, no error
        "run_shell": lambda a: {"phase": "complete", "exit_code": 3 if "case 7 in" in a["command"] else 0,
                                "stdout": "row\n"},
        "stop_endpoint": lambda a: {"status": "down", "endpoint_id": EID},
    })
    res = _drive(mod.SCRIPT, server)
    assert not res.final.is_error, res.final.result
    assert sum(1 for c in res.trace.calls if c.name == "run_shell") == mod.N_ROWS
    assert not mod.batch_used(res.trace).ok and not mod.window_within_capacity(res.trace).ok


def test_the_fanout_script_drives_run_batch_status_and_stop(monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    mod = importlib.import_module("batch_fanout_collect")
    rows = [{"index": i, "state": "failed" if i in mod.FAILING else "done",
             "exit_code": mod.FAILING.get(i, 0)} for i in range(mod.N_ROWS)]
    server = _Server({
        "connect_facility": _connect_answers(),
        "ensure_endpoint_up": lambda a: {"status": "up", "endpoint_id": EID, "block": {"queue": "main"}},
        "run_batch": lambda a: _status(capacity=1, admitted=1, results_dir="/h/r"),
        "status": lambda a: {**_status("done", rows if a.get("detail") == "page" else [],
                                       counts={"done": 37, "failed": 3}), "next_offset": None},
        "stop_endpoint": lambda a: {"status": "down", "endpoint_id": EID},
    })
    res = _drive(mod.SCRIPT, server)
    assert not res.final.is_error, res.final.result
    assert mod.failures_have_rc(res.trace).ok and mod.agent_reports_failures(res.trace).ok
    assert mod.batch_used(res.trace).ok and bc.stop_down(res.trace).ok


def test_concurrent_calls_that_end_out_of_order_all_align_with_the_journal(monkeypatch, tmp_path):
    # live 2026-10-31 (batch_fanout_collect's main fallback): 40 concurrent run_shell calls ended out of order, the steps
    # were kept in END order and the journal in START order, and 5 calls fell out of the alignment (trace_complete)
    _stub_sdk(monkeypatch, tmp_path)
    server = _Server({"run_shell": lambda a: {"phase": "complete", "exit_code": 0}})

    async def script(ops):
        await asyncio.gather(*(ops.call("run_shell", command=f"r{i}", delay=0.05 * (8 - i)) for i in range(8)))
        await ops.call("stop_endpoint")
        return "done"

    res = _drive(script, server)
    stats = next(m for m in res.messages if "harness" in m)["trace_stats"]
    assert stats["journal_unplaced"] == 0 and stats["hpcb_not_reaching_server"] == 0, stats
    assert [c.name for c in res.trace.calls][-1] == "stop_endpoint"


def test_journal_order_follows_the_server_not_the_client():
    # live 2026-10-31: 2 of 40 concurrent calls reached the server in another order than the client sent them
    steps = [script_runner.Step(n, "run_shell", {"command": f"r{i}"}, {}, 100.0 + 0.001 * i, 120.0 - i)
             for n, i in enumerate(range(4))]
    rows = [{"tool": "run_shell", "args": {"command": f"r{i}"}, "ts": 100.0 + 0.01 * k, "seq": k}
            for k, i in enumerate([1, 0, 3, 2])]
    got = script_runner.journal_order(steps, rows)
    assert [s.args["command"] for s in got] == ["r1", "r0", "r3", "r2"]
    events = [cli_runner.Native(s.tool, s.args, {}, True) for s in got]
    _trace, stats = cli_runner.build_trace(rows, events, [])
    assert stats["journal_unplaced"] == 0 and stats["hpcb_not_reaching_server"] == 0


def test_a_server_that_dies_under_the_script_never_cancels_the_script(monkeypatch, tmp_path):
    # live 2026-10-31 (batch_restart_resume on main): the SIGKILL made the stdio client's task group cancel the scope it
    # was entered in — the script's — and run.py read the cell as INTERRUPTED; each server now lives in a host task
    _stub_sdk(monkeypatch, tmp_path)
    import anyio

    opened: list[int] = []

    def factory(cmd, env, cwd):
        @contextlib.asynccontextmanager
        async def cm():
            opened.append(1)
            async with anyio.create_task_group() as tg:
                dead = anyio.Event()

                async def reader():
                    await dead.wait()
                    raise ConnectionError("the server closed its stdout")

                tg.start_soon(reader)

                class Session:
                    async def call_tool(self, name, args, read_timeout_seconds=None):
                        if name == "die":
                            dead.set()
                            await anyio.sleep(0.2)
                            raise ConnectionError("closed")
                        return types.SimpleNamespace(isError=False, structuredContent={"ok": len(opened)}, content=[])

                yield Session()
                tg.cancel_scope.cancel()

        return cm()

    async def script(ops):
        await ops.call("die")
        await ops.sleep(0.3)
        await ops.restart_server(kill=False)
        return json.dumps(await ops.call("status"))

    res = asyncio.run(script_runner.run_scenario("p", repo_root=HERE.parents[1], script=script, facility="f",
                                                  ssh_host="h", session_factory=factory))
    assert not res.final.is_error, res.final.result
    assert json.loads(res.final.result) == {"ok": 2} and len(opened) == 2
