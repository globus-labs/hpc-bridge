"""Hermetic: batch row sizing's scenarios (agentic/scenarios/batch_*: sizing_checks.py) — each scenario is wired, its
commands pass run_batch's template reader as they stand, and the graders and world checks read synthetic traces and
synthetic row lines the way they will read a live cell's. No server, no cluster."""
from __future__ import annotations

import asyncio
import importlib
import sys
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import sizing_checks as sc  # noqa: E402
from invariants import Result, ToolCall, Trace  # noqa: E402

SIZING_SCENARIOS = ["batch_rows_overlap", "batch_cores_fit", "batch_shared_pool", "batch_memory_kill",
                    "batch_needs_sizing_then_sized", "batch_size_first_terms", "batch_gpu_rows", "batch_pilot_proposal",
                    "batch_row_srun"]
BID = "batch-0123456789ab"
_BUILTIN = {"agent_engaged", "ends_with_stop", "stop_is_honest"}


def _c(name, inp=None, res=None, *, t=None):
    c = ToolCall.of(f"mcp__hpc-bridge__{name}", inp or {}, res)
    if t is not None:
        c.t_call, c.t_result = t
    return c


def _ctx(trace, out_by_dir):
    def ssh(cmd, _timeout=60):
        for d, out in out_by_dir.items():
            if d in cmd:
                return 0, out
        return 0, "HPCB_DONE\n"
    return types.SimpleNamespace(trace=trace, messages=[], ssh=ssh, tap=None, endpoint_name="ep")


def _row(i, k, start, end, *, aff, nproc=None, gpu="unset", cpus="none"):
    lines = f"HPCB_ROW {i} k={k} start={start} nproc={nproc or k} aff={aff} cpus={cpus} gpu={gpu}\n"
    if end is not None:
        lines += f"HPCB_ROW_END {i} end={end}\n"
    return f"HPCB_FILE /rd/{BID}/tasks/{i}/attempt-1/stdout\n" + lines


def _rows_out(*rows):
    return "".join(rows) + "HPCB_DONE\n"


def _accepted(**sizing):
    return _c("run_batch", {"command": "x", "rows": [1]},
              {"batch_id": BID, "phase": "admitting", "results_dir": f"/rd/{BID}", "capacity": sizing.get("slots", 3),
               "sizing": sizing})


# ---- wiring ----------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("name", SIZING_SCENARIOS)
def test_each_sizing_scenario_is_wired(name):
    mod = importlib.import_module(name)
    graders = {fn(Trace([])).name for fn in mod.EXTRA_INVARIANTS}
    assert set(mod.EXPECT_OK) - _BUILTIN <= graders, set(mod.EXPECT_OK) - _BUILTIN - graders
    assert callable(mod.SCRIPT) and mod.TARGETS == ("fake",) and mod.OPERATORS == ("script",)
    for fn in mod.EXTRA_INVARIANTS:
        assert isinstance(fn(Trace([])), Result)          # an empty trace is a failed premise, never a crash
    for fn in getattr(mod, "WORLD_CHECKS", []):
        r = asyncio.run(fn(_ctx(Trace([]), {})))
        assert isinstance(r, Result) and not r.ok         # nothing to read is never a pass


@pytest.mark.parametrize("name", SIZING_SCENARIOS)
def test_each_sizing_scenarios_commands_pass_the_template_reader(name):
    """A scenario whose command run_batch refuses would fail live for a reason it does not test."""
    from hpc_bridge import batch

    mod = importlib.import_module(name)
    pairs = [(getattr(mod, "COMMAND", None), getattr(mod, "ROWS", None))]
    pairs += [(spec["command"], spec["rows"]) for spec in (getattr(mod, "A", None), getattr(mod, "B", None)) if spec]
    for command, rows in pairs:
        if command is not None:
            batch.validate(command, rows, 60)


def test_the_probe_puts_the_row_number_bare_and_reads_back():
    p = sc.probe(2, "sleep 1")
    assert p.startswith("echo HPCB_ROW {n} k=2 ") and p.endswith("echo HPCB_ROW_END {n} end=$(date +%s.%N)")
    runs = sc.parse_rows("/rd", _rows_out(_row(0, 2, 10.5, 20.25, aff="0-1", cpus="0,1"), _row(1, 2, 11.0, None,
                                                                                                aff="2,3")))
    assert [(r.index, r.cores, r.start, r.end, sorted(r.aff_set())) for r in runs] == [
        (0, 2, 10.5, 20.25, [0, 1]), (1, 2, 11.0, None, [2, 3])]
    assert sc.parse_cpu_list("0-3,8,10-11") == {0, 1, 2, 3, 8, 10, 11} and sc.parse_cpu_list("x") == set()


# ---- overlap: the lower bound from each row's own start and end ------------------------------------------------------

def test_peak_overlap_counts_rows_that_ran_together_and_not_rows_that_only_touch():
    runs = sc.parse_rows("/rd", _rows_out(_row(0, 1, 0, 10, aff="0"), _row(1, 1, 1, 10, aff="1"),
                                          _row(2, 1, 2, 11, aff="2"), _row(3, 1, 10, 20, aff="0")))
    assert sc.peak_overlap(runs)[0] == 3                                   # row 3 starts as 0 and 1 end: not counted
    assert sc.peak_overlap(runs, weight=lambda r: r.cores)[0] == 3


def test_rows_overlap_needs_the_lower_bound_and_respects_the_announced_slots():
    t = Trace([_accepted(slots=3, cores=1)])
    together = _rows_out(*(_row(i, 1, i * 0.1, 20, aff=str(i)) for i in range(3)))
    serial = _rows_out(*(_row(i, 1, i * 20, i * 20 + 19, aff="0") for i in range(3)))
    assert asyncio.run(sc.rows_overlap(_ctx(t, {BID: together}), at_least=3)).ok
    r = asyncio.run(sc.rows_overlap(_ctx(t, {BID: serial}), at_least=3))
    assert not r.ok and "at most 1 ran at once" in r.detail
    four = _rows_out(*(_row(i, 1, 0, 20, aff=str(i)) for i in range(4)))
    assert not asyncio.run(sc.rows_overlap(_ctx(t, {BID: four}), at_least=3)).ok        # past the 3 announced


def test_cpus_never_oversubscribed_sums_cores_and_wants_disjoint_masks():
    t = Trace([_accepted(slots=2, cores=2)])
    ok = _rows_out(_row(0, 2, 0, 10, aff="0-1"), _row(1, 2, 0, 10, aff="2-3"), _row(2, 2, 10, 20, aff="0-1"))
    assert asyncio.run(sc.cpus_never_oversubscribed(_ctx(t, {BID: ok}), block_cpus=4)).ok
    shared = _rows_out(_row(0, 2, 0, 10, aff="0-1"), _row(1, 2, 0, 10, aff="1-2"))
    assert not asyncio.run(sc.cpus_never_oversubscribed(_ctx(t, {BID: shared}), block_cpus=4)).ok
    over = _rows_out(*(_row(i, 2, 0, 10, aff=f"{2 * i}-{2 * i + 1}") for i in range(3)))
    assert not asyncio.run(sc.cpus_never_oversubscribed(_ctx(t, {BID: over}), block_cpus=4)).ok


def test_nproc_matches_cores_reads_nproc_and_the_mask():
    t = Trace([_accepted()])
    assert asyncio.run(sc.nproc_matches_cores(_ctx(t, {BID: _rows_out(_row(0, 2, 0, 1, aff="4-5"))}))).ok
    assert not asyncio.run(sc.nproc_matches_cores(_ctx(t, {BID: _rows_out(_row(0, 2, 0, 1, aff="0-11"))}))).ok
    assert not asyncio.run(sc.nproc_matches_cores(_ctx(t, {BID: _rows_out(_row(0, 2, 0, 1, aff="0-1",
                                                                              nproc=12))}))).ok


def test_gpus_one_per_row_wants_one_of_the_blocks_and_different_ones_together():
    t = Trace([_accepted()])
    ok = _rows_out(_row(0, 1, 0, 10, aff="0", gpu="0"), _row(1, 1, 0, 10, aff="1", gpu="1"),
                   _row(2, 1, 10, 20, aff="0", gpu="0"))
    assert asyncio.run(sc.gpus_one_per_row(_ctx(t, {BID: ok}), gpus=("0", "1"))).ok
    every = _rows_out(_row(0, 1, 0, 10, aff="0", gpu="0,1"))
    clash = _rows_out(_row(0, 1, 0, 10, aff="0", gpu="1"), _row(1, 1, 5, 10, aff="1", gpu="1"))
    assert not asyncio.run(sc.gpus_one_per_row(_ctx(t, {BID: every}), gpus=("0", "1"))).ok
    assert not asyncio.run(sc.gpus_one_per_row(_ctx(t, {BID: clash}), gpus=("0", "1"))).ok


def test_the_neighbour_must_have_run_beside_the_kill():
    t = Trace([_accepted()])
    beside = _rows_out(_row(0, 1, 100, None, aff="0"), _row(1, 1, 100.2, 108.3, aff="1"))
    after = _rows_out(_row(0, 1, 100, None, aff="0"), _row(1, 1, 130, 138, aff="1"))
    assert asyncio.run(sc.neighbour_ran_beside_the_kill(_ctx(t, {BID: beside}), killed=0, neighbour=1)).ok
    assert not asyncio.run(sc.neighbour_ran_beside_the_kill(_ctx(t, {BID: after}), killed=0, neighbour=1)).ok


# ---- trace graders ---------------------------------------------------------------------------------------------------

_FACTS = {"source": "partition", "cpus": 4, "memory_gb": 3.9, "candidates": [{"cores": 1, "concurrency": 3}]}


def test_needs_sizing_first_wants_the_facts_before_any_spend():
    good = Trace([_c("run_batch", {"command": "x"}, {"phase": "needs_sizing", "facts": _FACTS})])
    assert sc.needs_sizing_first(good).ok
    spend_first = Trace([_c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
                            {"status": "up", "block": {"queue": "main"}}),
                         _c("run_batch", {"command": "x"}, {"phase": "needs_sizing", "facts": _FACTS})])
    assert not sc.needs_sizing_first(spend_first).ok
    asked_spend = Trace([_c("run_batch", {"command": "x"}, {"phase": "needs_confirmation", "block": {}})])
    assert not sc.needs_sizing_first(asked_spend).ok                   # the base: the spend question, never the size


def _terms(cores_per_node, *, memory_gb=1.0, gpus=0, concurrency=2, receipt_cores=None):
    asked = {"phase": "needs_confirmation", "block": {"cores_per_node": cores_per_node, "memory_gb": memory_gb,
                                                       "gpus_per_node": gpus, "nodes": 1},
             "sizing": {"concurrency": concurrency, "derived": True}}
    confirm = {"batch_id": BID, "phase": "admitting",
               "block": {"cores_per_node": receipt_cores or cores_per_node, "memory_gb": memory_gb}}
    return Trace([_c("run_batch", {"command": "x", "cores": 1}, asked),
                  _c("run_batch", {"command": "x", "cores": 1, "confirm_spend": True}, confirm)])


def test_derived_terms_wants_n_k_plus_one_cpus_the_memory_and_a_matching_receipt():
    assert sc.derived_terms(_terms(3), cores=1, memory_gb=0.25, concurrency=2).ok
    assert not sc.derived_terms(_terms(2), cores=1, memory_gb=0.25, concurrency=2).ok          # no interactive CPU
    assert not sc.derived_terms(_terms(3, memory_gb=0.25), cores=1, memory_gb=0.25, concurrency=2).ok   # < N × m
    assert not sc.derived_terms(_terms(3, receipt_cores=4), cores=1, memory_gb=0.25, concurrency=2).ok
    assert sc.derived_terms(_terms(3, gpus=2), cores=1, memory_gb=0.25, gpus=1, concurrency=2).ok
    assert not sc.derived_terms(_terms(3, gpus=3), cores=1, memory_gb=0.25, gpus=1, concurrency=2).ok   # no reserve GPU


def _status_rows(*rows, phase="done", **extra):
    return _c("status", {"id": BID}, {"batch_id": BID, "phase": phase, "results": list(rows),
                                      "counts": {"done": sum(1 for r in rows if r.get("state") == "done")}, **extra})


def test_memory_graders_read_why_rc_and_the_neighbours_output():
    t = Trace([_status_rows({"index": 0, "state": "failed", "why": "memory", "exit_code": sc.MEMORY_RC},
                            {"index": 1, "state": "done", "exit_code": 0, "tail": "row 1\nfine\n"})])
    assert sc.memory_killed_with_why(t, index=0).ok and sc.neighbour_survived(t, index=1, word="fine").ok
    oom = Trace([_status_rows({"index": 0, "state": "failed", "exit_code": 137},
                              {"index": 1, "state": "failed", "exit_code": 137})])
    assert not sc.memory_killed_with_why(oom, index=0).ok and not sc.neighbour_survived(oom, index=1, word="fine").ok


def test_pilot_proposed_and_batch_done():
    t = Trace([_status_rows(phase="needs_sizing", proposal={"cores": 2, "memory_gb": 0.5, "concurrency": 2})])
    assert sc.pilot_proposed(t, cores=2, memory_gb=(0.25, 0.75)).ok
    assert not sc.pilot_proposed(t, cores=3, memory_gb=(0.25, 0.75)).ok
    done = Trace([_status_rows({"index": 0, "state": "done"}, {"index": 1, "state": "done"})])
    assert sc.batch_done(done, rows=2).ok and not sc.batch_done(done, rows=3).ok


def test_sizing_announced_compares_every_accepted_batch():
    t = Trace([_accepted(cores=2, slots=2, bound_by="cpus")])
    assert sc.sizing_announced(t, cores=2, slots=2, bound_by="cpus").ok
    assert not sc.sizing_announced(t, slots=3).ok
    assert not sc.sizing_announced(Trace([]), slots=2).ok


def test_block_allocation_reads_this_runs_pilot_from_sacct():
    eid = "0123abcd-0000-4000-8000-000000000000"
    out = (f"77|parsl.HTEX.block-0|shared|3|1G|1|COMPLETED|sbatch --job-name=x uep.{eid}.1/submit\n"
           "78|other|shared|4|4G|1|COMPLETED|sbatch y\nHPCB_RC=0\n")
    a = sc.block_allocation(out, [eid])
    assert a is not None and (a["job"], a["cpus"], a["mem_mb"], a["nodes"]) == ("77", "3", 1024.0, "1")


def test_srun_graders_want_each_step_run_and_said():
    """batch_row_srun (the Anvil live test, F3): each row's step ran (its line, srun's rc 0), each result says it ran
    srun steps, and an answer says where such a step runs."""
    note = ("Row(s) 0, 1 ran srun steps: a step runs on CPUs Slurm picks within the block, not the row's slot (it may "
            "share another row's), and outside the row's memory watch.")
    ran = [{"index": i, "state": "done", "exit_code": 0, "srun_steps": True,
            "tail": f"HPCB_ROW {i} k=1\nHPCB_STEP_OK c1\nHPCB_SRUN_RC=0\nHPCB_ROW_END {i} end=2\n"} for i in range(2)]
    good = Trace([_status_rows(*ran, notice=note)])
    assert sc.srun_ran(good, rows=2).ok and sc.srun_steps_said(good, rows=2).ok
    died = [{**r, "tail": f"HPCB_ROW {r['index']} k=1\nHPCB_SRUN_RC=1\n", "srun_steps": False,
             "stderr_tail": "srun: fatal: cpus-per-task set by two different environment variables"} for r in ran]
    base = Trace([_status_rows(*died)])                                    # the base: the step died, nothing said
    assert not sc.srun_ran(base, rows=2).ok and not sc.srun_steps_said(base, rows=2).ok
    assert not sc.srun_steps_said(Trace([_status_rows(*ran)]), rows=2).ok   # flagged, but where it runs never said
