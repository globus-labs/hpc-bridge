"""Hermetic: the restart_cold_stop chain's graders and world check (a block an earlier server left running is released
by the next server's stop, called before any connect), on synthetic traces. No server, no cluster."""
from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import script_runner  # noqa: E402
from invariants import ToolCall, Trace, ends_with_stop, stop_is_honest  # noqa: E402

rcs = importlib.import_module("restart_cold_stop")

_DOWN = ("No endpoint was bound in this session; hpc-bridge's record names block job 40, which an earlier hpc-bridge "
         "session started on 'fake-1' (endpoint e), so this stop reached that endpoint over the login endpoint (AMQP, "
         "no SSH): compute block released over AMQP (released 40). The login endpoint stays online for reuse.")


def _c(name, inp=None, res=None, *, phase=0):
    c = ToolCall.of(f"mcp__hpc-bridge__{name}", inp or {}, res)
    c.phase = phase
    return c


def _phase0(job="40"):
    return [
        _c("connect_facility", {"facility": "fake-1"}, {"phase": "proposed_facility_details"}),
        _c("connect_facility", {"facility": "fake-1", "details": {}}, {"phase": "needs_account"}),
        _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "provisioning"}),
        _c("ensure_endpoint_up", {"shape": "compute"}, {"status": "up", "block_job_id": job, "endpoint_id": "e"}),
    ]


def _stop(status, notice, spend_stopped=None):
    return _c("stop_endpoint", {}, {"status": status, "notice": notice, "spend_stopped": spend_stopped}, phase=1)


def _grade(t):
    return {r.name: r.ok for r in (rcs.cold_stop_released(t), rcs.cold_stop_honest(t), stop_is_honest(t),
                                   ends_with_stop(t))}


def test_the_chain_is_two_scripted_phases_the_first_its_premise():
    assert script_runner.script_refusal(rcs) is None
    assert [callable(p["script"]) for p in rcs.PHASES] == [True, True] and rcs.PHASES[0]["premise"] is True
    assert rcs.OPERATORS == ("script",) and rcs.TARGETS == ("fake",)
    assert {"cold_stop_released", "cold_stop_honest"} <= set(rcs.EXPECT_OK)
    assert rcs.cold_stop_block_gone in rcs.WORLD_CHECKS


def test_a_cold_stop_that_released_the_earlier_block_passes():
    t = Trace([*_phase0(), _stop("down", _DOWN, True)])
    assert rcs.phase0_job(t) == "40" and rcs.released_ids(t.calls[-1].result) == ["40"]
    assert _grade(t) == {"cold_stop_released": True, "cold_stop_honest": True, "stop_is_honest": True,
                         "ends_with_stop": True}


def test_draining_then_down_passes():
    cold = ("No endpoint was bound in this session; hpc-bridge's record names block job 40 …: cancel not confirmed "
            "(allocating nodes…). Spend is NOT confirmed stopped — the login release channel was cold.")
    t = Trace([*_phase0(), _stop("draining", cold), _stop("down", _DOWN, True)])
    assert all(_grade(t).values())


def test_mains_cold_stop_fails_it():
    # before the fix: nothing bound, so "no endpoint was up" — and job 40 billed on
    t = Trace([*_phase0(), _stop("down", "no endpoint was up")])
    g = _grade(t)
    assert g["cold_stop_released"] is False and g["cold_stop_honest"] is False
    assert "no endpoint was up" in rcs.cold_stop_released(t).detail


def test_a_down_without_spend_stopped_or_before_the_release_is_not_honest():
    t = Trace([*_phase0(), _stop("down", "the login endpoint is OFFLINE … job 40", None)])
    assert rcs.cold_stop_honest(t).ok is False and rcs.cold_stop_released(t).ok is False
    other = Trace([*_phase0(), _stop("down", "compute block released over AMQP (released 41).", True)])
    assert rcs.cold_stop_honest(other).ok is False          # down, but phase 0's block was never said released
    assert rcs.cold_stop_released(other).ok is False


def test_a_draining_end_or_a_stop_that_is_not_first_fails_it():
    t = Trace([*_phase0(), _stop("draining", "Spend is NOT confirmed stopped: … job 40")])
    assert rcs.cold_stop_released(t).ok is False and rcs.cold_stop_honest(t).ok is True
    first = Trace([*_phase0(), _c("connect_facility", {"facility": "fake-1"}, {"phase": "needs_account"}, phase=1),
                   _stop("down", _DOWN, True)])
    assert rcs.cold_stop_released(first).ok is False and "not stop_endpoint" in rcs.cold_stop_released(first).detail
    assert rcs.cold_stop_released(Trace(_phase0()[:3] + [_stop("down", _DOWN, True)])).ok is False   # no job named


def test_the_world_check_reads_the_job_state():
    assert rcs.job_state_cmd("40") == "squeue -h -j 40 -o %t 2>/dev/null || true"
    assert rcs.gone_verdict("40", "") == (True, "job 40 is gone from the queue")
    assert rcs.gone_verdict("40", "CG\n")[0] is None
    assert rcs.gone_verdict("40", "R\n")[0] is False and rcs.gone_verdict("40", "PD\n")[0] is False


def _ctx(outs, monkeypatch):
    monkeypatch.setattr(rcs, "GONE_WAIT_S", 30.0)

    async def no_wait(_s):
        return None

    monkeypatch.setattr(rcs, "_pause", no_wait)
    seen = []

    def ssh(cmd, timeout=120, host=None):
        seen.append(cmd)
        return outs.pop(0) if outs else (0, "")

    return SimpleNamespace(trace=Trace([*_phase0(), _stop("down", _DOWN, True)]), ssh=ssh), seen


def test_the_world_check_waits_out_an_ending_job_and_fails_one_still_running(monkeypatch):
    ctx, seen = _ctx([(0, "CG\n"), (0, "")], monkeypatch)
    r = asyncio.run(rcs.cold_stop_block_gone(ctx))
    assert r.name == "world:cold_stop_block_gone" and r.ok and len(seen) == 2 and "squeue -h -j 40" in seen[0]
    ctx, _ = _ctx([(0, "R\n")], monkeypatch)
    assert asyncio.run(rcs.cold_stop_block_gone(ctx)).ok is False
    ctx, _ = _ctx([(1, "slurm_load_jobs error")], monkeypatch)
    bad = asyncio.run(rcs.cold_stop_block_gone(ctx))
    assert bad.ok is False and bad.detail.startswith("UNVERIFIABLE")
