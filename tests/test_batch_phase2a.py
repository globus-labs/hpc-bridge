"""Batch phase 2a (vault `Planned/Batch and swarm execution.md` §2, §3, §4, §10 "Phase 2", §12 D2): the budget envelope,
the billing meter, Jobs mode (`submit_job` with arrays, `--deadline` and promotion; SSH facilities only) and D2's gate
and adopt. Written failing first: strict xfails on main at cc9f856 (the surface does not exist there); the mark comes
off with the build.

One test (at least) per rule the maintainer set for this phase, plus the §3 meter cases. Everything runs through the
server's own seams (`server._submit_job`, `_status`, `_cancel`, `_run_shell`, `_stop_endpoint`) with the free login
shape as REAL bash on this machine and a fake Slurm / PBS on its PATH (tests/fake_sched.py): what hpc-bridge sends —
the frozen job.sh, its header, the meter's sacct / qstat, the hold and release of a gated hand submit — is what is
tested, never a stand-in for its own parsing.

The surface these tests are written against (the stage-A checkpoint fixes it):

- `submit_job(command=None, script=None, session_id="default", account=None, partition=None, nodes=None, cores=None,
  memory_gb=None, gpus=None, walltime=None, array=None, depends_on=None, adopt=None, envelope=None,
  confirm_spend=False)` → JobStatus {phase, reason, job: JobInfo, terms: JobTerms, envelope: EnvelopeInfo, notice}.
  `phase`: needs_confirmation | needs_account | refused | queued | running | done | failed | cancelled (a job) and
  open | exhausted | draining | closed (an envelope, from status). `reason` on needs_confirmation: envelope_needed |
  envelope_exhausted | envelope_new_session; on refused: envelope_ceiling, mep, invalid, …
- `envelope` (submit_job's argument): {cap, deadline_hours, max_jobs, max_nodes_per_block, accounts, partitions,
  at_deadline: drain|abort, site_backstop}. Opened by the confirming call; read by status("env-…"); closed by
  cancel("env-…", drain|abort), by its deadline, or by stop_endpoint.
- `status("job-…")` / `status("env-…")` → the one BatchStatus model (`kind` job | envelope, `job`, `envelope`: the
  coordinator's ruling, so structured output survives); `cancel("job-…"|"env-…", mode)` → CancelResult.
- `run_shell(…, confirm_spend=False)`: a recognised hand sbatch/qsub while this session's envelope is open answers
  needs_confirmation (nothing runs); with confirm_spend=True it runs HELD, is adopted (ledgered, reserved, given the
  envelope's deadline), then released — or cancelled while still held when it does not fit (D2 (c)).
- `stop_endpoint(jobs=None|"cancel"|"keep")` (the user's decision 1): with Jobs-mode jobs of ours that can still bill,
  a stop with no choice answers needs_confirmation naming them and changes nothing; cancel ends them (down only once
  none is listed); keep releases only the blocks (draining, never down).
- `submit_job(…, exclude_jobs=[native ids])` with confirm_spend=True: jobs the user says are not theirs to count.
- `Ledger.add_envelope(…)`, `Ledger.reserve_job(…)` (THE admission point: BEGIN IMMEDIATE), `EnvelopeExhausted`.
- `PartitionShape.min_charge_h`: a queue's minimum charge, in hours of the job's billing (a catalog fact).
- `HPC_BRIDGE_ENVELOPE_MAX`: the operator ceiling on the SUM of open envelopes' caps, per facility and account.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_bridge.models import PartitionShape
from hpc_bridge.profile import Profile
from hpc_bridge.server import AppCtx
from tests.fake_sched import FakeSched
from tests.fakes import FakeFacility
from tests.test_server import _DoneFuture, _FakeRunner, _Res

_BASH = shutil.which("bash")
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]

WHOLE = PartitionShape(name="main", kind="whole_node", cores=4, billing={"cpu": 1.0})     # 4 billing units / hour
SLICED = PartitionShape(name="shared", kind="per_core", cores=4, billing={"cpu": 1.0})
PREEMPT = PartitionShape(name="preempt", kind="whole_node", cores=4, billing={"cpu": 1.0}, min_charge_h=2.0)
WORKQ = PartitionShape(name="workq", kind="whole_node", cores=4, billing={"cpu": 1.0})


# ---- the world: a facility whose login shape is this machine's bash, a fake scheduler on its PATH -------------------

class _ShellRunner(_FakeRunner):
    """Every shape's runner: runs what is submitted under bash, as ShellFunction would (brace-escaped, then format()),
    with the fake scheduler first on PATH and no TZ of the test's own."""

    def __init__(self, sched: FakeSched, home: Path, endpoint_id: str = "fake-eid") -> None:
        super().__init__(endpoint_id, _Res(0, "", ""))
        self.sched, self.home = sched, home

    def submit(self, command):
        from hpc_bridge.runner import _escape_for_shellfunction

        self.commands.append(command)
        env = {**self.sched.env(), "HOME": str(self.home), "TMPDIR": str(self.home)}
        p = subprocess.run([_BASH, "-c", _escape_for_shellfunction(command).format()], capture_output=True,
                           text=True, env=env, timeout=60)
        fut = _DoneFuture(_Res(p.returncode, p.stdout, p.stderr))
        self.futures.append(fut)
        return fut


def _app(tmp: Path, sched: FakeSched, *, scheduler: str = "slurm", mep: bool = False, partitions=None,
         account_required: bool = False) -> AppCtx:
    f = FakeFacility()
    f.workers = 1
    if mep:
        f.supported_shapes = ("compute",)
    f.profile = SimpleNamespace(scheduler=scheduler)
    f.partitions = list(partitions or [WHOLE, SLICED, PREEMPT])
    f.account_required = account_required
    app = AppCtx(facility=f, profile=Profile())
    app.state.endpoint_id = "fake-eid"
    app.machine = "fakehpc"
    app.scratch_root = str(tmp / "scratch")
    app.batch_autoloop = False
    home = tmp / "home"
    home.mkdir(exist_ok=True)
    runner = _ShellRunner(sched, home)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    app.test_runner = runner
    return app


@pytest.fixture
def world(tmp_path):
    sched = FakeSched(tmp_path / "sched", user="u")
    return SimpleNamespace(tmp=tmp_path, sched=sched, app=_app(tmp_path, sched))


@pytest.fixture
def pbs(tmp_path):
    sched = FakeSched(tmp_path / "sched", scheduler="pbs", user="u")
    return SimpleNamespace(tmp=tmp_path, sched=sched, app=_app(tmp_path, sched, scheduler="pbs", partitions=[WORKQ]))


async def _submit(app, **kw):
    from hpc_bridge import server

    kw.setdefault("command", "echo hello")
    kw.setdefault("partition", "main")
    kw.setdefault("walltime", "01:00:00")
    return await server._submit_job(app, **kw)


async def _confirmed(app, **kw):
    """A job admitted: the call as an agent makes it; when it asks (no envelope yet, or one that must be widened), the
    user's yes — the same call with confirm_spend=True. Inside an envelope this session holds, a job that fits is
    admitted on the first call (the envelope is the approval)."""
    out = await _submit(app, **kw)
    if out.phase == "needs_confirmation":
        out = await _submit(app, confirm_spend=True, **kw)
    assert out.phase in ("queued", "running"), out
    return out


def _envelope_of(out):
    assert out.envelope is not None and out.envelope.envelope_id.startswith("env-"), out
    return out.envelope.envelope_id


def _new_session(world, *, owner_gone: bool = True):
    """Another server (a later chat, or this one restarted) on the same state dir: its own session; the earlier one's
    process is gone (`owner_gone`) or still alive."""
    from hpc_bridge.ledger import Ledger, Owner

    if world.app.ledger is not None:
        world.app.ledger.close()
    app2 = _app(world.tmp, world.sched)
    app2.batch_owner = Owner(host=Owner.here().host, pid=os.getpid() + 100_000)
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = (lambda pid: pid != os.getpid()) if owner_gone else (lambda pid: True)
    return app2


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%S")


def _epoch(iso: str) -> float:
    return datetime.strptime(iso.rstrip("Z")[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC).timestamp()


# ---- rule 1: nothing billed starts that the user did not confirm ------------------------------------------------------

async def test_a_first_submit_job_asks_and_submits_nothing(world):
    """The first submit_job proposes the envelope and the job's terms; NOTHING is written to the facility or submitted
    until the user's yes comes back as confirm_spend=True."""
    out = await _submit(world.app, command="sleep 60")
    assert out.phase == "needs_confirmation" and out.reason == "envelope_needed", out
    assert out.terms is not None and out.terms.worst_case == pytest.approx(4.0) and out.terms.unit, out.terms
    assert out.envelope is not None and out.envelope.state == "proposed" and out.envelope.cap >= 4.0, out.envelope
    assert world.sched.calls("sbatch") == [] and world.sched.submitted() == []
    assert not list((world.tmp / "scratch").glob("jobs/*/job.sh"))


async def test_the_yes_submits_once_and_the_job_is_ledgered(world):
    out = await _confirmed(world.app, command="sleep 60")
    assert out.job is not None and out.job.job_id.startswith("job-") and out.job.source == "submit_job", out.job
    assert [j["id"] for j in world.sched.submitted()] == [out.job.native_id]
    assert len(world.sched.calls("sbatch")) == 1


async def test_a_restart_never_submits_again(world):
    """A new server reads the ledger: its status and cancel calls look and cancel, and never submit — not a job the old
    server submitted, nor one whose submission was refused."""
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    world.sched.knob(refuse=True)
    refused = await _submit(world.app, command="sleep 61", confirm_spend=True)
    assert refused.phase == "refused" and "Invalid account" in (refused.notice or ""), refused
    app2 = _new_session(world)
    for _ in range(3):
        await server._status(app2, out.job.job_id)
        await server._status(app2, out.envelope.envelope_id)
    assert len(world.sched.calls("sbatch")) == 2                       # the first job's and the refused one: no others
    assert "compute" not in app2.shapes and "compute" not in world.app.shapes   # nothing was ever sent to a block


async def test_a_refused_submission_holds_nothing_against_the_envelope(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 10})
    world.sched.knob(refuse=True)
    refused = await _submit(world.app, command="sleep 61", confirm_spend=True)
    assert refused.phase == "refused"
    env = await server._status(world.app, out.envelope.envelope_id)
    assert env.envelope.spent + env.envelope.committed == pytest.approx(4.0), env.envelope   # only the first job


# ---- D2 (c): a recognised hand sbatch / qsub while an envelope is open — gate, then adopt ----------------------------

def _hand_script(world, text: str = "#!/bin/bash\n#SBATCH -t 01:00:00\n#SBATCH -p main\nsleep 300\n") -> Path:
    p = world.tmp / "home" / "hand.sh"
    p.write_text(text)
    return p


async def test_a_hand_sbatch_while_the_envelope_is_open_asks_first_and_runs_nothing(world):
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    script = _hand_script(world)
    before = len(world.sched.calls("sbatch"))
    out = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True)
    assert out.phase == "needs_confirmation", out
    assert "confirm_spend" in (out.notice or "") and out.envelope is not None, out
    assert len(world.sched.calls("sbatch")) == before                  # the command never ran


async def test_a_confirmed_hand_sbatch_runs_unmodified_and_its_job_is_adopted(world):
    """D2 as the design says (round 3, the user's decision): the yes runs the command as it is; the job that appeared
    while it ran is adopted after the fact — ledgered, reserved at its full request — with no envelope deadline."""
    from hpc_bridge import server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    script = _hand_script(world)
    out = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
    assert out.phase == "complete" and out.jobs and out.jobs[0].source == "adopt", out
    native = out.jobs[0].native_id
    job = world.sched.job(native)
    assert job["held_at_submit"] is False and job["deadline_epoch"] is None, job     # submitted as the command said
    assert out.jobs[0].worst_case == pytest.approx(4.0) and out.jobs[0].unheld, out.jobs   # 4 × 1 h, from scontrol
    env = await server._status(world.app, first.envelope.envelope_id)
    assert out.jobs[0].job_id in env.envelope.job_ids


async def test_a_hand_sbatch_that_does_not_fit_is_adopted_over_the_cap_and_never_cancelled(world):
    from hpc_bridge import server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 6})   # 4 of 6 held by the first job
    script = _hand_script(world)
    out = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
    assert "over its cap" in (out.notice or "").lower() and out.jobs, out
    hand = [j for j in world.sched.submitted() if j["id"] != first.job.native_id]
    assert len(hand) == 1 and hand[0]["state"] != "CANCELLED" and world.sched.calls("scancel") == [], hand


async def test_a_hand_sbatch_with_no_envelope_open_keeps_the_advisory(world):
    from hpc_bridge import server

    script = _hand_script(world)
    out = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True)
    assert out.phase == "complete" and "Submitted batch job" in out.stdout, out
    assert "bills the user's allocation" in (out.notice or "")         # #176's advisory, as on main
    assert not out.jobs


async def test_a_hand_qsub_is_gated_and_adopted_on_pbs(pbs):
    from hpc_bridge import server

    await _confirmed(pbs.app, command="sleep 60", partition="workq", envelope={"cap": 20})
    script = pbs.tmp / "home" / "hand.pbs"
    script.write_text("#!/bin/bash\n#PBS -l select=1:ncpus=4\n#PBS -l walltime=01:00:00\nsleep 300\n")
    asked = await server._run_shell(pbs.app, f"qsub {script}", shape="login", agent_call=True)
    assert asked.phase == "needs_confirmation" and len(pbs.sched.calls("qsub")) == 1, asked
    out = await server._run_shell(pbs.app, f"qsub {script}", shape="login", agent_call=True, confirm_spend=True)
    assert out.jobs and out.jobs[0].source == "adopt", out
    job = pbs.sched.job(out.jobs[0].native_id.split(".")[0])
    assert job["held_at_submit"] is False and job["state"] != "CANCELLED", job     # run unmodified, adopted after


# ---- rule 2: the meter never understates -------------------------------------------------------------------------------

async def test_the_meter_charges_billing_tres_times_elapsed(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600")
    world.sched.set(out.job.native_id, state="RUNNING", elapsed=1800, billing=4)
    st = await server._status(world.app, out.job.job_id)
    assert st.job.meter_source == "sacct" and st.job.charged == pytest.approx(2.0), st.job      # 4 × 0.5 h
    assert st.job.committed == pytest.approx(2.0)                                               # its 0.5 h left


async def test_the_meter_never_charges_below_the_queues_minimum(world):
    """Perlmutter's `preempt` charges at least 2 h: a job that ran 10 minutes is charged 2 h of its billing."""
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 30", partition="preempt", walltime="03:00:00",
                           envelope={"cap": 50})
    assert out.terms.min_charge_h == pytest.approx(2.0) and out.terms.worst_case == pytest.approx(12.0), out.terms
    world.sched.set(out.job.native_id, state="RUNNING", elapsed=600, billing=4)
    st = await server._status(world.app, out.job.job_id)
    assert st.job.charged == pytest.approx(8.0) and st.job.committed == pytest.approx(4.0), st.job
    world.sched.set(out.job.native_id, state="COMPLETED", elapsed=600)
    st = await server._status(world.app, out.job.job_id)
    assert st.job.charged == pytest.approx(8.0) and st.job.committed == pytest.approx(0.0), st.job
    # a minimum is a catalog fact: a queue with none charges what ran
    plain = await _confirmed(world.app, command="sleep 30")
    world.sched.set(plain.job.native_id, state="COMPLETED", elapsed=600, billing=4)
    st = await server._status(world.app, plain.job.job_id)
    assert st.job.charged == pytest.approx(4 * 600 / 3600, abs=0.01)


async def test_committed_is_a_running_jobs_remaining_walltime_and_a_queued_jobs_full_one(world):
    from hpc_bridge import server

    a = await _confirmed(world.app, command="sleep 3600", envelope={"cap": 20})
    b = await _confirmed(world.app, command="sleep 3600")
    world.sched.set(a.job.native_id, state="RUNNING", elapsed=900, billing=4)
    st_a = await server._status(world.app, a.job.job_id)
    st_b = await server._status(world.app, b.job.job_id)
    assert st_a.job.committed == pytest.approx(3.0) and st_b.job.committed == pytest.approx(4.0), (st_a.job, st_b.job)
    env = await server._status(world.app, a.envelope.envelope_id)
    assert env.envelope.spent == pytest.approx(1.0) and env.envelope.committed == pytest.approx(7.0), env.envelope
    assert env.envelope.remaining == pytest.approx(12.0)


@pytest.mark.parametrize("world_says", ["squeue_only", "neither", "sacct_down"])
async def test_a_ledger_job_missing_from_sacct_counts_at_its_full_walltime(world, world_says):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600")
    jid = out.job.native_id
    if world_says == "squeue_only":
        world.sched.set(jid, state="RUNNING", elapsed=600)
        world.sched.knob(hide_sacct=[jid])
    elif world_says == "neither":
        world.sched.set(jid, state="COMPLETED", elapsed=600)
        world.sched.knob(hide_sacct=[jid])
    else:
        world.sched.set(jid, state="COMPLETED", elapsed=600)
        world.sched.knob(sacct_down=True)
    st = await server._status(world.app, out.job.job_id)
    assert st.job.charged + st.job.committed == pytest.approx(4.0), st.job       # its full hour, never its 10 minutes
    assert st.job.meter_source == ("squeue" if world_says == "squeue_only" else "missing"), st.job
    assert "full walltime" in (st.notice or ""), st.notice


async def test_with_no_billing_tres_a_slice_is_metered_at_its_own_rate_not_a_whole_nodes(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600", partition="shared", cores=2)
    assert out.terms.rate == pytest.approx(2.0) and out.terms.whole_node is False, out.terms
    world.sched.set(out.job.native_id, state="RUNNING", elapsed=1800, no_billing=True, cpus=2)
    st = await server._status(world.app, out.job.job_id)
    assert st.job.charged == pytest.approx(1.0), st.job                 # 2 cores × 0.5 h, never 4 × 0.5 h


async def test_the_meter_counts_every_array_task(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600", array="0-19%4")
    jid = out.job.native_id
    job = world.sched.job(jid)
    for i in range(4):
        job["tasks"]["state"][str(i)] = "RUNNING"
        job["tasks"]["elapsed"][str(i)] = 600
    world.sched.set(jid, tasks=job["tasks"], billing=4)
    st = await server._status(world.app, out.job.job_id)
    assert st.job.charged == pytest.approx(4 * 4 * 600 / 3600, abs=0.01), st.job
    assert st.job.charged + st.job.committed == pytest.approx(80.0, abs=0.01), st.job          # 20 tasks × 4 × 1 h
    assert st.job.array_counts.get("RUNNING") == 4 and st.job.array_counts.get("PENDING") == 16, st.job


def test_reserve_before_admit_is_atomic(tmp_path):
    """Room for one job: eight admitters at once, exactly one is reserved (the check and the reservation are one BEGIN
    IMMEDIATE transaction)."""
    from hpc_bridge.ledger import EnvelopeExhausted, Ledger, Owner

    led = Ledger.open(tmp_path / "state")
    me = Owner.here()
    led.add_envelope("env-000000000001", facility="fakehpc", account=None, unit="billing units", cap=5.0,
                     deadline=time.time() + 3600, approved_by="session-a", owner=me)
    got: list[str] = []
    lock = threading.Lock()

    def take(k: int) -> None:
        mine = Ledger.open(tmp_path / "state")
        try:
            mine.reserve_job("env-000000000001", f"job-{k:012x}", session="session-a", worst_case=4.0, tasks=1)
            with lock:
                got.append("ok")
        except EnvelopeExhausted:
            with lock:
                got.append("full")
        finally:
            mine.close()

    threads = [threading.Thread(target=take, args=(k,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert got.count("ok") == 1 and got.count("full") == 7, got


async def test_unledgered_jobs_count_and_are_named(world):
    """The sweep (`sacct -u $USER -S <opened>`) finds a job hpc-bridge did not start (a script's own sbatch, a
    subprocess): it counts against the envelope, and the answer names it so the user can say it is not theirs."""
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    stray = world.sched.add_job(name="bypass", state="RUNNING", elapsed=600, timelimit_min=60, billing=4)
    env = await server._status(world.app, out.envelope.envelope_id)
    assert [u.native_id for u in env.envelope.unledgered] == [stray], env.envelope
    assert env.envelope.spent + env.envelope.committed == pytest.approx(4.0 + 4.0), env.envelope
    assert stray in (env.notice or "")


async def test_jobs_the_user_says_are_not_theirs_are_never_counted_again(world):
    """One unrelated job of the user's must not exhaust an envelope: `exclude_jobs` on a confirming call records them
    as the user's own, and the meter never counts them again (the notice names what it does count)."""
    from hpc_bridge import batch, server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 10})
    stray = world.sched.add_job(name="mine", state="RUNNING", elapsed=0, timelimit_min=60, billing=4)
    asked = await _submit(world.app, command="sleep 61", confirm_spend=True)
    assert asked.phase == "needs_confirmation" and asked.reason == "envelope_exhausted", asked
    assert stray in (asked.notice or "") and "exclude_jobs" in (asked.notice or ""), asked.notice
    yes = await _submit(world.app, command="sleep 61", confirm_spend=True, exclude_jobs=[stray])
    assert yes.phase in ("queued", "running"), yes
    env = await server._status(world.app, out.envelope.envelope_id)
    assert env.envelope.unledgered == [] and env.envelope.spent + env.envelope.committed == pytest.approx(8.0), env
    assert stray in batch.ledger_of(world.app).excluded_jobs(out.envelope.envelope_id)


async def test_the_meter_is_one_sacct_call_per_tick(world):
    from hpc_bridge import server

    a = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    await _confirmed(world.app, command="sleep 61")
    before = len(world.sched.calls("sacct"))
    await server._status(world.app, a.envelope.envelope_id)
    assert len(world.sched.calls("sacct")) - before == 1
    before = len(world.sched.calls("sacct"))
    await server._status(world.app, a.job.job_id)
    assert len(world.sched.calls("sacct")) - before == 1


async def test_the_pbs_meter_reads_qstat_x_f_json(pbs):
    from hpc_bridge import server

    out = await _confirmed(pbs.app, command="sleep 3600", partition="workq")
    native = out.job.native_id.split(".")[0]
    pbs.sched.set(native, state="RUNNING", elapsed=1800)
    st = await server._status(pbs.app, out.job.job_id)
    assert st.job.meter_source == "qstat" and st.job.charged == pytest.approx(2.0), st.job
    assert st.job.committed == pytest.approx(2.0)
    assert any("-x" in c and "-f" in c and "json" in c for c in pbs.sched.calls("qstat")), pbs.sched.calls("qstat")


# ---- rule 3: the envelope belongs to its session; a deadline; the operator ceiling ------------------------------------

async def test_a_new_session_is_asked_again_naming_what_remains(world):
    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 10})
    app2 = _new_session(world)
    asked = await _submit(app2, command="sleep 61")
    assert asked.phase == "needs_confirmation" and asked.reason == "envelope_new_session", asked
    assert asked.envelope.envelope_id == out.envelope.envelope_id and asked.envelope.remaining == pytest.approx(6.0)
    assert "6" in (asked.notice or "") and len(world.sched.calls("sbatch")) == 1, asked.notice
    yes = await _submit(app2, command="sleep 61", confirm_spend=True)
    assert yes.phase in ("queued", "running") and yes.envelope.owned is True, yes
    assert len(world.sched.calls("sbatch")) == 2


async def test_the_deadline_defaults_within_24_hours_and_every_job_carries_it(world):
    now = time.time()
    out = await _confirmed(world.app, command="sleep 60")
    dl = _epoch(out.envelope.deadline)
    assert now < dl <= now + 24 * 3600 + 5, out.envelope.deadline
    job = world.sched.job(out.job.native_id)
    header = [ln for ln in job["script"].splitlines() if ln.startswith("#SBATCH")]
    assert any(ln.startswith("#SBATCH --deadline=") for ln in header), header
    # the header's deadline is in the cluster's own local time — no TZ is forced on sbatch (it would reach the job)
    assert abs(job["deadline_epoch"] - dl) < 2 and job["tz"] is None, job
    for hours in (0, -1, 24 * 8):   # none, or past the operator's longest: refused, nothing submitted
        bad = await _submit(world.app, command="sleep 60", envelope={"cap": 50, "deadline_hours": hours},
                            confirm_spend=True)
        assert bad.phase == "refused" and "deadline" in (bad.notice or ""), (hours, bad)
    assert len(world.sched.calls("sbatch")) == 1


async def test_the_operator_ceiling_bounds_the_sum_of_open_envelopes(world, monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_ENVELOPE_MAX", "10")
    await _confirmed(world.app, command="sleep 60", account="hpcb", envelope={"cap": 6})
    app2 = _new_session(world, owner_gone=False)                       # another live chat on the same facility
    over = await _submit(app2, command="sleep 61", account="hpcb", envelope={"cap": 6}, confirm_spend=True)
    assert over.phase == "refused" and over.reason == "envelope_ceiling", over
    assert "HPC_BRIDGE_ENVELOPE_MAX" in (over.notice or "") and "4" in (over.notice or ""), over.notice
    fits = await _submit(app2, command="sleep 61", account="hpcb", envelope={"cap": 4}, confirm_spend=True)
    assert fits.phase in ("queued", "running"), fits
    other = await _submit(app2, command="sleep 62", account="other", envelope={"cap": 6}, confirm_spend=True)
    assert other.phase in ("queued", "running"), other                  # per facility AND account
    assert len(world.sched.calls("sbatch")) == 3


# ---- rule 4: a stop or cancel is never `down` while something of ours can still bill; exhausted is not down -----------

async def test_cancel_of_a_job_is_draining_until_the_scheduler_has_ended_it(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600")
    world.sched.set(out.job.native_id, state="RUNNING", elapsed=60)
    world.sched.knob(cancel_lag=3)
    first = await server._cancel(world.app, out.job.job_id, "abort")
    assert any(out.job.native_id in c for c in world.sched.calls("scancel")), world.sched.calls("scancel")
    assert first.status == "draining" and out.job.native_id in (first.notice or ""), first
    final = first
    for _ in range(6):
        final = await server._cancel(world.app, out.job.job_id, "abort")
        if final.status == "down":
            break
    assert final.status == "down", final


async def _two_billing_jobs(world):
    """A submit_job job and an adopted hand submit, both RUNNING."""
    from hpc_bridge import server

    job = await _confirmed(world.app, command="sleep 3600", envelope={"cap": 20})
    script = _hand_script(world)
    hand = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
    ids = [job.job.native_id, hand.jobs[0].native_id]
    for jid in ids:
        world.sched.set(jid, state="RUNNING", elapsed=60)
    return job, ids


async def test_a_stop_with_no_job_of_ours_billing_works_as_today(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 30")
    world.sched.set(out.job.native_id, state="COMPLETED", elapsed=30)
    st = await server._stop_endpoint(world.app)
    assert st.status == "down" and st.spend_stopped is True, st
    assert world.sched.calls("scancel") == []


async def test_a_stop_while_our_jobs_can_bill_asks_first_and_changes_nothing(world):
    from hpc_bridge import server

    _job, ids = await _two_billing_jobs(world)
    st = await server._stop_endpoint(world.app)
    assert st.status == "needs_confirmation", st
    notice = st.notice or ""
    assert all(j in notice for j in ids) and "RUNNING" in notice and 'jobs="cancel"' in notice, notice
    assert {j.native_id for j in st.jobs} == set(ids) and all(j.committed > 0 for j in st.jobs), st.jobs
    assert world.sched.calls("scancel") == []                                  # nothing cancelled …
    assert not any("-O" in c for c in world.sched.calls("squeue"))             # … nor the blocks released


async def test_stop_jobs_cancel_ends_them_closes_the_envelope_and_is_down_only_once_none_is_listed(world):
    from hpc_bridge import server

    job, ids = await _two_billing_jobs(world)
    world.sched.knob(cancel_lag=2)
    first = await server._stop_endpoint(world.app, jobs="cancel")
    assert all(any(j in c for c in world.sched.calls("scancel")) for j in ids), world.sched.calls("scancel")
    assert first.status == "draining" and all(j in (first.notice or "") for j in ids), first
    final = first
    for _ in range(4):
        final = await server._stop_endpoint(world.app, jobs="cancel")
        if final.status == "down":
            break
    assert final.status == "down" and final.spend_stopped is True, final
    env = await server._status(world.app, job.envelope.envelope_id)
    assert env.phase == "closed", env
    again = await _submit(world.app, command="sleep 60")
    assert again.phase == "needs_confirmation" and again.envelope.envelope_id != job.envelope.envelope_id, again


async def test_stop_jobs_keep_releases_the_blocks_leaves_the_jobs_and_is_never_down(world):
    from hpc_bridge import server

    job, ids = await _two_billing_jobs(world)
    st = await server._stop_endpoint(world.app, jobs="keep")
    assert st.status == "draining" and st.spend_stopped is not True, st
    notice = st.notice or ""
    assert all(j in notice for j in ids) and job.job.job_id in notice and job.envelope.envelope_id in notice, notice
    assert "cancel(" in notice and world.sched.calls("scancel") == []          # the jobs run on
    assert any("-O" in c for c in world.sched.calls("squeue"))                 # the endpoint's blocks: released as today
    env = await server._status(world.app, job.envelope.envelope_id)
    assert env.phase == "draining" and set(env.envelope.job_ids) >= {job.job.job_id}, env
    late = await _submit(world.app, command="sleep 60", confirm_spend=False)
    assert late.phase == "needs_confirmation", late                          # nothing new is admitted after a stop
    assert len(world.sched.calls("sbatch")) == 2
    ended = await server._cancel(world.app, job.envelope.envelope_id, "abort")
    assert all(any(j in c for c in world.sched.calls("scancel")) for j in ids), ended


async def test_a_later_sessions_stop_asks_about_an_earlier_servers_jobs(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600")
    world.sched.set(out.job.native_id, state="RUNNING", elapsed=60)
    app2 = _new_session(world)
    st = await server._stop_endpoint(app2)
    assert st.status == "needs_confirmation" and out.job.native_id in (st.notice or ""), st
    unbound = _new_session(world)
    unbound.state.endpoint_id = None                                   # a chat that has not connected yet
    blind = await server._stop_endpoint(unbound)
    assert blind.status == "needs_confirmation" and out.job.native_id in (blind.notice or ""), blind
    kept = await server._stop_endpoint(unbound, jobs="cancel")         # no channel: nothing can be cancelled from here
    assert kept.status == "draining" and "connect_facility" in (kept.notice or ""), kept
    done = await server._stop_endpoint(app2, jobs="cancel")
    assert any(out.job.native_id in c for c in world.sched.calls("scancel")) and done.status in ("draining", "down")


async def test_a_held_job_a_dead_server_never_released_is_ours_listed_and_cancelled(world):
    """D2: the server died between the hold and the release — the held job is ledgered (ours): a later session's stop
    lists it, and a cancel of it ends it."""
    from hpc_bridge import batch, server

    first = await _confirmed(world.app, command="sleep 3600", envelope={"cap": 20})
    held = world.sched.add_job(name="hand", state="PENDING", held=True, reason="JobHeldUser", timelimit_min=60)
    batch.ledger_of(world.app).reserve_job(first.envelope.envelope_id, "job-00000000beef", session="dead-session",
                                           worst_case=4.0, source="adopt", native_id=held, state="held",
                                           ignore_owner=True)
    app2 = _new_session(world)
    st = await server._stop_endpoint(app2)
    assert st.status == "needs_confirmation" and held in (st.notice or "") and "held" in (st.notice or "").lower(), st
    env = await server._status(app2, first.envelope.envelope_id)
    assert "job-00000000beef" in env.envelope.job_ids
    c = await server._cancel(app2, "job-00000000beef", "abort")
    assert any(held in x for x in world.sched.calls("scancel")) and c.status in ("draining", "down"), c


async def test_exhausted_is_not_down(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600", envelope={"cap": 5})
    world.sched.set(out.job.native_id, state="RUNNING", elapsed=60)
    again = await _submit(world.app, command="sleep 3600", confirm_spend=True)
    assert again.phase == "needs_confirmation" and again.reason == "envelope_exhausted", again
    env = await server._status(world.app, out.envelope.envelope_id)
    assert env.phase == "exhausted" and out.job.job_id in (env.notice or ""), env     # it names what still bills
    drained = await server._cancel(world.app, out.envelope.envelope_id, "drain")
    assert drained.status == "draining" and drained.in_flight == 1, drained


# ---- rule 5: the terms state the honest limit --------------------------------------------------------------------------

async def test_the_terms_state_the_honest_limit_and_no_site_backstop_unless_named(world):
    out = await _submit(world.app, command="sleep 60")
    binds = (out.terms.binds or "").lower()
    assert "what hpc-bridge starts" in binds and "sbatch" in binds and "token" in binds, out.terms.binds
    assert out.terms.site_backstop == "none" and out.envelope.site_backstop == "none"
    named = await _submit(world.app, command="sleep 60", envelope={"site_backstop": "GrpTRESMins cpu=600 on hpcb"})
    assert named.terms.site_backstop == "GrpTRESMins cpu=600 on hpcb", named.terms
    assert "GrpTRESMins" in (named.notice or "")


# ---- rule 6: Jobs mode is SSH-only ------------------------------------------------------------------------------------

async def test_submit_job_on_a_facility_mep_is_refused_with_the_reason(tmp_path):
    sched = FakeSched(tmp_path / "sched")
    app = _app(tmp_path, sched, mep=True)
    out = await _submit(app, command="sleep 60", confirm_spend=True)
    assert out.phase == "refused" and out.reason == "mep", out
    words = (out.notice or "").lower()
    assert "status" in words and "cancel" in words and "campaign" in words, out.notice
    assert app.test_runner.commands == [] and sched.calls() == []


# ---- rule 7: no value becomes shell code; a frozen job.sh, its hash on the record --------------------------------------

@pytest.mark.parametrize("arg,value", [
    ("partition", "main\n#SBATCH --account=evil"),
    ("partition", "main; touch /tmp/hpcb-pwned"),
    ("account", "hpcb$(touch /tmp/hpcb-pwned)"),
    ("walltime", "01:00:00 --wrap=id"),
    ("array", "0-3; id"),
    ("array", "0-3%2\n#SBATCH -A evil"),
    ("depends_on", ["job-000000000001; id"]),
    ("adopt", "123; scancel -u $USER"),
    ("session_id", "../../etc"),
])
async def test_no_argument_value_becomes_shell_code(world, arg, value):
    out = await _submit(world.app, command="sleep 1", confirm_spend=True, **{arg: value})
    assert out.phase == "refused", (arg, value, out)
    assert world.sched.calls("sbatch") == [] and world.sched.calls("scancel") == []
    assert not Path("/tmp/hpcb-pwned").exists()


async def test_the_index_placeholder_follows_the_batch_template_rules(world):
    refused = await _submit(world.app, command='python3 run.py --seed "{index}"', array="0-3", confirm_spend=True)
    assert refused.phase == "refused" and "inside quotes" in (refused.notice or ""), refused
    out = await _confirmed(world.app, command="echo seed {index}", array="0-3")
    env = {"PATH": os.environ["PATH"], "HOME": str(world.tmp / "home"), "SLURM_JOB_ID": out.job.native_id,
           "SLURM_ARRAY_JOB_ID": out.job.native_id, "SLURM_ARRAY_TASK_ID": "2"}
    p = subprocess.run([_BASH, out.job.script_path], capture_output=True, text=True, env=env, timeout=30)
    assert "seed 2" in p.stdout.splitlines(), p.stdout + p.stderr     # the task's own index, as data
    assert (Path(out.job.script_path).parent / "exit_code.2").read_text().strip() == "0"


async def test_job_sh_is_written_once_and_its_hash_is_on_the_record(world):
    from hpc_bridge import batch

    out = await _confirmed(world.app, command="echo frozen")
    path = Path(out.job.script_path)
    assert path.is_file() and path.parent.parent == Path(world.app.scratch_root) / "jobs", path
    assert hashlib.sha256(path.read_bytes()).hexdigest() == out.job.script_hash
    assert world.sched.job(out.job.native_id)["script_path"] == str(path)
    row = batch.ledger_of(world.app).job(out.job.job_id)
    assert row["script_hash"] == out.job.script_hash
    from hpc_bridge import jobs

    with pytest.raises(jobs.JobRefusal, match="exists"):
        await jobs.write_job_script(world.app, out.job.job_id, "#!/bin/bash\necho tampered\n")
    assert hashlib.sha256(path.read_bytes()).hexdigest() == out.job.script_hash   # written once: never replaced


async def test_the_command_cannot_add_directives_to_the_header(world):
    out = await _confirmed(world.app, command="#SBATCH --account=evil\n#SBATCH -p other\necho hi")
    script = world.sched.job(out.job.native_id)["script"]
    from tests.fake_sched import _directives

    header = _directives(script, "#SBATCH")
    assert "--account=evil" not in header and "other" not in header, header
    assert world.sched.job(out.job.native_id)["account"] != "evil"


# ---- promotion, arrays, exhaustion -------------------------------------------------------------------------------------

async def test_promotion_carries_the_sessions_cwd_and_env(world):
    """submit_job(command, session_id=s): the job cd's to the session's cwd, sources its env and writes its exit code
    (PSI/J's rule) — run here under bash as the scheduler would."""
    work = world.tmp / "home" / "work"
    work.mkdir(parents=True)
    sd = Path(world.app.scratch_root) / "sessions" / "dev"
    sd.mkdir(parents=True)
    (sd / ".cwd").write_text(str(work) + "\n")
    (sd / ".env").write_text("export HPCB_PROMO=carried\n")
    out = await _confirmed(world.app, command="pwd; echo $HPCB_PROMO; exit 3", session_id="dev")
    env = {"PATH": os.environ["PATH"], "HOME": str(world.tmp / "home"), "SLURM_JOB_ID": out.job.native_id}
    p = subprocess.run([_BASH, out.job.script_path], capture_output=True, text=True, env=env, timeout=30)
    assert p.stdout.splitlines()[:2] == [str(work), "carried"], p.stdout + p.stderr
    rc = list(Path(out.job.script_path).parent.glob("exit_code*"))
    assert rc and rc[0].read_text().strip() == "3", rc


async def test_an_arrays_worst_case_is_every_task_not_the_throttle(world):
    out = await _submit(world.app, command="sleep 60", array="0-19%4")
    assert out.terms.tasks == 20 and out.terms.throttle == 4, out.terms
    assert out.terms.worst_case == pytest.approx(80.0), out.terms        # 20 × 4 units × 1 h, not 4 × …
    assert "20" in (out.terms.basis or "") and out.envelope.cap >= 80.0
    capped = await _submit(world.app, command="sleep 60", array="0-19%4", envelope={"cap": 100, "max_jobs": 10},
                           confirm_spend=True)
    assert capped.phase in ("needs_confirmation", "refused") and "max_jobs" in (capped.notice or ""), capped
    assert world.sched.calls("sbatch") == []


async def test_an_exhausted_envelope_asks_and_submits_nothing(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600", envelope={"cap": 5})
    again = await _submit(world.app, command="sleep 3600", confirm_spend=True)
    assert again.phase == "needs_confirmation" and again.reason == "envelope_exhausted", again
    assert again.envelope.remaining == pytest.approx(1.0) and "4" in (again.notice or ""), again
    assert len(world.sched.calls("sbatch")) == 1
    raised = await _submit(world.app, command="sleep 3600", envelope={"cap": 9}, confirm_spend=True)
    assert raised.phase in ("queued", "running") and raised.envelope.cap == pytest.approx(9.0), raised
    st = await server._status(world.app, out.envelope.envelope_id)
    assert st.envelope.envelope_id == out.envelope.envelope_id      # the user's yes raised THIS envelope's cap


async def test_a_block_confirmed_under_an_open_envelope_is_reserved_against_it(world):
    """The envelope binds everything hpc-bridge starts: a compute block confirmed while it is open is admitted against
    it (its worst case reserved) — refused when it does not fit, asked as for a job."""
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 3600", envelope={"cap": 4.5})      # 4 of 4.5 held by the job
    st = await server._ensure_endpoint_up(world.app, "compute", confirm_spend=True, partition="main")
    assert st.status == "needs_confirmation" and "envelope" in (st.notice or "").lower(), st
    assert "compute" not in world.app.shapes or not world.app.shapes["compute"].spend_confirmed


# ---- the tool surface ----------------------------------------------------------------------------------------------

async def test_status_and_cancel_read_job_and_envelope_ids_and_refuse_others(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60")
    job = await server._status(world.app, out.job.job_id)
    env = await server._status(world.app, out.envelope.envelope_id)
    assert job.job.job_id == out.job.job_id and env.envelope.envelope_id == out.envelope.envelope_id
    assert (job.kind, env.kind) == ("job", "envelope") and job.batch_id is None, (job, env)
    from hpc_bridge.models import BatchStatus

    assert isinstance(job, BatchStatus) and isinstance(env, BatchStatus)    # one model: structured output survives
    from hpc_bridge import server as srv

    tool = next(t for t in await srv.mcp.list_tools() if t.name == "status")
    assert tool.outputSchema and "kind" in tool.outputSchema.get("properties", {}), tool.outputSchema
    bad = await server._status(world.app, "job-zzz")
    assert bad.phase == "refused", bad
    unknown = await server._cancel(world.app, "env-000000000000", "abort")
    assert unknown.status == "refused", unknown
    names = {t.name for t in await server.mcp.list_tools()}
    assert "submit_job" in names and len(names) == 16, sorted(names)


def test_minimum_charges_are_a_catalog_fact():
    from hpc_bridge.catalog.entry import partitions_raw_problems

    shape = PartitionShape.model_validate({"name": "preempt", "kind": "whole_node", "min_charge_h": 2})
    assert shape.min_charge_h == 2.0
    assert PartitionShape.model_validate({"name": "x", "min_charge_h": "soon"}).min_charge_h is None   # lenient
    assert partitions_raw_problems({"defaults": {"partitions": [{"name": "p", "min_charge_h": 2}]}}) == []


def test_the_journal_records_one_meter_tick_per_status(tmp_path, monkeypatch):
    """The harness grades 'one sacct call per tick' from the journal's `meter.tick` rows."""
    from hpc_bridge import meter

    path = tmp_path / "journal.jsonl"
    monkeypatch.setenv("HPC_BRIDGE_JOURNAL", str(path))
    meter.journal_tick(envelope_id="env-000000000001", jobs=2, sacct_calls=1, source="sacct")
    rows = [json.loads(x) for x in path.read_text().splitlines()]
    assert rows[-1]["event"] == "meter.tick" and rows[-1]["sacct_calls"] == 1, rows


_ = (asyncio, re)
