"""Batch phase 2a, fix round 2 (2026-11-01): the live cells at 3d0b272 and the probe outputs (scratchpad
probe_r1_deadline.out, probe_r1_safe.out). Written failing first (red log: scratchpad red_p2a_r2.txt).

1. site_backstop_grptresmins: Slurm held the job PENDING with AssocGrpCPUMinutesLimit (the `safe` probe), but every
   status said only "PENDING; charged 0" — sacct's Reason is `None` for it. The reason is squeue's `%r` (PBS: the job's
   comment), and a site limit is said plainly: it will not start until that limit changes.
2. job_promote_track_cancel (script): the proposed cap was exactly the job's worst case; by the yes the meter held 0.01
   (a leftover job of the user's), so the yes refused itself. The proposal is made after a meter reading, covers what is
   already held, names it; a yes after what is held grew RE-PROPOSES (the choice made here) — it never refuses itself
   for an amount the user could not see, and never opens at a cap the user did not see.
3. Probe A: a stepped array reads back `ArrayTaskId=0-98:2%4`; sbatch REFUSES a walltime past `--deadline` (so a gated
   hand submit the envelope's deadline cuts short fails, and the answer must say why and how to proceed); scancel of an
   ended job is rc 0 and silent — "cancelled" is decided by the job's state afterwards.
"""
from __future__ import annotations

import re

import pytest

from tests.test_batch_phase2a import (
    _BASH,
    _confirmed,
    _hand_script,
    _submit,
    pbs,
    world,
)

_FIXTURES = (world, pbs)
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]


# ---- 1. a job held by a site limit says so ----------------------------------------------------------------------------

async def test_r2_1_a_job_pending_under_a_site_limit_carries_the_schedulers_reason_and_says_so(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    world.sched.set(out.job.native_id, state="PENDING", reason="AssocGrpCPUMinutesLimit")
    st = await server._status(world.app, out.job.job_id)
    assert st.job.reason == "AssocGrpCPUMinutesLimit" and st.job.state == "PENDING" and st.job.charged == 0, st.job
    words = (st.notice or "").lower()
    assert "assocgrpcpuminuteslimit" in words and "site limit" in words and "will not start" in words, st.notice


async def test_r2_1_an_ordinary_pending_reason_is_not_called_a_site_limit(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    world.sched.set(out.job.native_id, state="PENDING", reason="Priority")
    st = await server._status(world.app, out.job.job_id)
    assert st.job.reason == "Priority" and "site limit" not in (st.notice or "").lower(), (st.job, st.notice)


def test_r2_1_the_pending_reason_is_read_from_squeue_not_sacct():
    """sacct's Reason is `None` for a pending job (live r1): the tick's squeue carries `%r`."""
    from hpc_bridge import meter

    assert "%r" in meter.slurm_cmd("2026-11-01T00:00:00")
    raw = ("HPCB_SACCT_BEGIN\n7|hpcb-job-x|PENDING|0|60||billing=4,cpu=4,node=1|main|hpcb|0:0|None\nHPCB_SACCT_RC=0\n"
           "HPCB_SQUEUE_BEGIN\n7|PENDING|0:00|1:00:00|4|AssocGrpCPUMinutesLimit|hpcb-job-x\nHPCB_SQUEUE_RC=0\n"
           "HPCB_METER_END\n")
    view = meter.parse_slurm(raw)
    r = meter.job_reading({"native_id": "7", "rate": 4, "walltime_s": 3600, "worst_case": 4, "tasks": 1}, view)
    assert r.state == "PENDING" and r.reason == "AssocGrpCPUMinutesLimit", r


async def test_r2_1_a_pbs_job_held_by_a_server_limit_says_so(pbs):
    from hpc_bridge import server

    out = await _confirmed(pbs.app, command="sleep 60", partition="workq", envelope={"cap": 20})
    native = out.job.native_id.split(".")[0]
    pbs.sched.set(native, state="PENDING", reason="Not Running: User has reached queue workq running job limit.")
    st = await server._status(pbs.app, out.job.job_id)
    assert "running job limit" in (st.job.reason or ""), st.job
    assert "site limit" in (st.notice or "").lower(), st.notice


# ---- 2. the proposal covers what is already held; a yes never refuses itself -------------------------------------------

async def test_r2_2_the_proposal_is_made_after_a_meter_reading_and_covers_what_is_already_held(world):
    left = world.sched.add_job(name="leftover", state="RUNNING", elapsed=0, timelimit_min=60, billing=4)
    before = len(world.sched.calls("sacct"))
    ask = await _submit(world.app, command="sleep 60")
    assert ask.phase == "needs_confirmation" and ask.reason == "envelope_needed", ask
    assert len(world.sched.calls("sacct")) > before                       # read before it proposed
    assert ask.envelope.cap >= 4.0 + 4.0 - 1e-9, ask.envelope              # what is held (4) + this job's worst (4)
    assert [u.native_id for u in ask.envelope.unledgered] == [left], ask.envelope.unledgered
    assert left in (ask.notice or "") and "already" in (ask.notice or "").lower(), ask.notice
    assert "exclude_jobs" in (ask.notice or ""), ask.notice
    yes = await _submit(world.app, command="sleep 60", confirm_spend=True)
    assert yes.phase == "queued", yes                                      # the yes admits what it was asked about
    assert yes.envelope.cap == pytest.approx(ask.envelope.cap), (yes.envelope.cap, ask.envelope.cap)


async def test_r2_2_a_yes_after_what_is_held_grew_re_proposes_and_never_refuses_itself(world):
    ask = await _submit(world.app, command="sleep 60")
    assert ask.phase == "needs_confirmation" and ask.envelope.cap == pytest.approx(4.0), ask.envelope
    grew = world.sched.add_job(name="leftover", state="RUNNING", elapsed=0, timelimit_min=60, billing=2)
    yes = await _submit(world.app, command="sleep 60", confirm_spend=True)
    assert yes.phase == "needs_confirmation" and yes.reason == "envelope_needed", yes   # re-proposed, not exhausted
    assert yes.envelope.cap >= 6.0 - 1e-9 and grew in (yes.notice or ""), (yes.envelope, yes.notice)
    assert world.sched.calls("sbatch") == []
    again = await _submit(world.app, command="sleep 60", confirm_spend=True)
    assert again.phase == "queued" and again.envelope.cap == pytest.approx(yes.envelope.cap), again


async def test_r2_2_a_yes_the_server_never_proposed_is_asked_first(world):
    """A confirming call with no cap of its own, that no proposal of this server preceded (a restart, an agent that
    skipped the question), proposes — it never opens at a figure the user did not see."""
    yes = await _submit(world.app, command="sleep 60", confirm_spend=True)
    assert yes.phase == "needs_confirmation" and yes.reason == "envelope_needed", yes
    assert world.sched.calls("sbatch") == []


async def test_r2_2_a_yes_with_its_own_cap_that_cannot_hold_what_is_held_asks_naming_it(world):
    left = world.sched.add_job(name="leftover", state="RUNNING", elapsed=0, timelimit_min=60, billing=4)
    out = await _submit(world.app, command="sleep 60", envelope={"cap": 5}, confirm_spend=True)
    assert out.phase == "needs_confirmation" and out.reason == "envelope_exhausted", out
    assert left in (out.notice or "") and "exclude_jobs" in (out.notice or ""), out.notice
    figures = [float(x) for x in re.findall(r"at least (\d+(?:\.\d+)?)", out.notice or "")]
    assert figures and min(figures) >= 8.0 - 1e-9, out.notice
    assert world.sched.calls("sbatch") == []


# ---- 3. probe A ---------------------------------------------------------------------------------------------------------

def test_r2_3_a_stepped_throttled_array_reads_back_as_every_task():
    from hpc_bridge import jobs

    line = ("JobId=3 ArrayJobId=3 ArrayTaskId=0-98:2%4 JobName=x JobState=PENDING Reason=JobHeldUser "
            "TimeLimit=00:05:00 NumNodes=1-1 NumCPUs=1 ReqTRES=cpu=1,mem=4000M,node=1,billing=1 AllocTRES=(null)")
    r = jobs._slurm_request("3", line)
    assert r is not None and r.tasks == 50, r


async def test_r2_3_a_hand_submit_the_envelopes_deadline_cut_short_is_explained(world):
    """sbatch refuses a walltime past the wrapper's `--deadline` (probe r1): the answer says the envelope's deadline
    caused it, and how to proceed."""
    from hpc_bridge import server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 50, "deadline_hours": 2})
    script = _hand_script(world, "#!/bin/bash\n#SBATCH -t 05:00:00\n#SBATCH -p main\nsleep 300\n")
    out = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
    assert [j for j in world.sched.submitted() if j["id"] != first.job.native_id] == []   # sbatch refused it
    words = (out.notice or "").lower()
    assert "deadline" in words and "walltime" in words and "deadline_hours" in (out.notice or ""), out.notice
    m = re.search(r"walltime of at most (\d{2}):(\d{2}):\d{2}", out.notice or "")   # what does fit
    assert m and int(m.group(1)) * 60 + int(m.group(2)) in range(115, 121), out.notice
    assert world.sched.calls("scancel") == []


async def test_r2_3_cancelled_is_decided_by_the_jobs_state_not_scancels_rc(world):
    """scancel of an ended job is rc 0 and silent (probe r1): a job that had already ended is never "cancelled"."""
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60")
    world.sched.set(out.job.native_id, state="COMPLETED", elapsed=30)     # ended; the ledger has not read it yet
    c = await server._cancel(world.app, out.job.job_id, "abort")
    assert f"cancelled: {out.job.native_id}" not in (c.notice or ""), c.notice
    assert "ended" in (c.notice or "").lower() and c.status == "down", c
