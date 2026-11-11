"""A per-core partition's memory share (the billed Anvil live test of 2026-11-10; Purdue Anvil `shared`: Slurm 23.11,
per-core billing, DefMemPerCPU = MaxMemPerCPU = 1896 MB). Written failing first, each before its fix.

R1 (blocker). `ensure_endpoint_up(cores=8, memory_gb=15.5)` sent `--cpus-per-task=8` and `--mem=15872M`. 15872 MB is
more than 8 × 1896 MB, so Slurm raised the job to 9 CPUs; parsl's launcher `srun` then died at once ("cpus-per-task set
by two different environment variables SLURM_CPUS_PER_TASK=9 != SLURM_TRES_PER_TASK=cpu=8"), the job FAILED in 5 s and
the executor went to BadStateException — while the terms said 8 cores. hpc-bridge's own default proposal said "8
cores, 15 GB": 15 GB said back is over the share too. The rule now (`resources.cores_for_memory`): where a partition's
ENFORCED per-core memory share is known (MaxMemPerCPU — the partition's own, else the cluster's; DefMemPerCPU alone
never raises: round 2) and a block's memory is above its cores' share, the block ASKS for ceil(memory / share) cores — the count Slurm would raise it to — so the request and Slurm's CPUs agree,
the terms say it in hpc-bridge's words and the bill counts it. A default proposal never exceeds its cores' share.
Every path: ensure_endpoint_up, a batch's derived block, a Jobs-mode job, N nodes, a facility endpoint's template.

R2. A reused block's "a task runs up to ~Ns" said the whole walltime; it is what is left of the block's walltime from
its known start."""
from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

import pytest

from hpc_bridge import notices, resources, server, sizing
from hpc_bridge.models import PartitionShape
from hpc_bridge.server import _ensure_endpoint_up
from tests.test_resource_requests import SHARED, _app

_SHARE = 1896


# =====================================================================================================================
# R1 — the exact Anvil case, on hpc-bridge's own SSH template
# =====================================================================================================================

async def test_r1_anvils_8_cores_and_15_5_gb_ask_for_and_bill_9_cores():
    app = _app(walltime="00:15:00")
    res = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15.5)
    b = res.block
    assert res.status == "needs_confirmation" and app.built == []
    assert (b.cores_per_node, b.memory_gb, b.whole_node) == (9, 15.5, False), b
    assert getattr(b, "cores_asked", None) == 8, b
    assert b.max_charge == 2.25 and b.charge_basis.startswith("9 cores × 0.25 h"), (b.max_charge, b.charge_basis)
    assert "1 node × 9 cores, 15.5 GB" in res.notice, res.notice
    assert "15.5 GB needs 9 cores' memory share on partition 'shared' (1896 MB each)" in res.notice, res.notice
    assert "9 cores are billed" in res.notice, res.notice
    up = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert up.status == "up", up.notice
    sent = app.built[-1]
    assert sent["cores_per_node"] == 9 and sent["exclusive"] is False, sent       # --cpus-per-task=9: Slurm agrees
    assert "#SBATCH --mem=15872M" in (sent.get("scheduler_options") or ""), sent
    assert up.block.cores_per_node == 9                                            # the receipt says what started


async def test_r1_the_default_proposal_never_exceeds_its_cores_share():
    app = _app()
    res = await _ensure_endpoint_up(app, shape="compute")
    b = res.block
    assert (b.cores_per_node, b.memory_gb, b.memory_default, b.size_source) == (8, 14.8, True, "proposed")
    assert "1 node × 8 cores, 14.8 GB (the partition's default per core)" in res.notice, res.notice
    assert " 15 GB" not in res.notice, res.notice                     # 8 × 1896 MB said rounded UP was over the share
    # never rounded up anywhere: 3 cores of 1896 MB are 5.55 GB, said 5.5 (5.6 GB said back is over their share)
    three = resources.effective({"cores_per_node": 3, "nodes_per_block": 1}, SHARED, channel="ssh")
    assert three.memory_gb == 5.5 and three.memory_default, three
    # the terms said back exactly: the same 8 cores, nothing raised
    echo = await _ensure_endpoint_up(app, shape="compute", cores=b.cores_per_node, memory_gb=b.memory_gb)
    assert echo.block.cores_per_node == 8 and getattr(echo.block, "cores_asked", None) is None, echo.block
    # the old words said back ("15 GB": --mem=15g is 15360 MB, over 8 × 1896): 9 cores, asked and billed
    r15 = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15)
    assert r15.block.cores_per_node == 9 and r15.block.max_charge == 4.5, r15.block
    await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15, confirm_spend=True)
    assert app.built[-1]["cores_per_node"] == 9 and app.built[-1]["mem_per_node"] == 15, app.built[-1]


async def test_r1_a_proposal_that_sends_the_nodes_memory_per_core_is_held_to_the_share():
    """No DefMemPerCPU, a MaxMemPerCPU below the node's memory per core: the proposal sends memory, never above the
    cores' share (it sent 16 GB for 8 cores of 1900 MB: Slurm would raise it to 9)."""
    capped = PartitionShape(name="mx", kind="per_core", why="OverSubscribe=YES", cores=128, memory_gb=256.0,
                            max_mem_per_core_mb=1900, billing={"cpu": 1.0})
    app = _app(partitions=(capped,), partition="mx")
    res = await _ensure_endpoint_up(app, shape="compute")
    b = res.block
    assert b.cores_per_node == 8 and b.memory_gb is not None and b.memory_gb * 1024 <= 8 * 1900, b
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    sent = app.built[-1]
    assert sent["cores_per_node"] == 8, sent
    mem = sent.get("mem_per_node")
    mb = mem * 1024 if mem else int((sent.get("scheduler_options") or "").split("--mem=")[1].rstrip("M\n"))
    assert mb <= 8 * 1900, sent


async def test_r1_memory_alone_raises_the_proposed_slice():
    app = _app()
    res = await _ensure_endpoint_up(app, shape="compute", memory_gb=64)
    assert res.block.cores_per_node == 35, res.block                         # ceil(65536 / 1896)
    assert "64 GB needs 35 cores' memory share" in res.notice, res.notice


async def test_r1_n_nodes_raise_each_nodes_cores():
    app = _app()
    res = await _ensure_endpoint_up(app, shape="compute", nodes=2, cores=8, memory_gb=15.5)
    b = res.block
    assert (b.nodes, b.cores_per_node, b.max_charge) == (2, 9, 9.0), b       # 2 nodes × 9 cores × 0.5 h
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert app.built[-1]["cores_per_node"] == 9 and app.built[-1]["nodes_per_block"] == 2


async def test_r1_a_raised_count_over_a_per_job_cap_is_refused_before_anything_starts():
    capped = SHARED.model_copy(update={"max_cores_per_job": 8})
    app = _app(partitions=(capped,))
    res = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15.5, confirm_spend=True)
    assert app.built == [] and "9 cores" in res.notice and "cap" in res.notice, res.notice


async def test_r1_where_the_share_is_unknown_nothing_is_guessed_and_the_terms_say_so():
    noshare = SHARED.model_copy(update={"mem_per_core_mb": None, "max_mem_per_core_mb": None,
                                          "why": "OverSubscribe=YES", "billing": None})
    app = _app(partitions=(noshare,))
    res = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15.5)
    assert res.block.cores_per_node == 8, res.block
    assert "may give — and bill — more cores than asked" in res.notice, res.notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert app.built[-1]["cores_per_node"] == 8


# =====================================================================================================================
# R1 — a batch's derived block
# =====================================================================================================================

def test_r1_derive_block_counts_the_raised_cores_against_a_cap():
    shape = SHARED.model_copy(update={"max_cores_per_job": 9})
    # 4 rows × (1 CPU, 4 GB) + 1 = 5 CPUs, 17 GB: 17 GB needs 10 cores' share — over the cap of 9
    with pytest.raises(sizing.SizingRefusal, match="10"):
        sizing.derive_block(sizing.Sizing(cores=1, memory_mb=4096, concurrency=4), shape=shape)
    b = sizing.derive_block(sizing.Sizing(cores=1, memory_mb=4096, concurrency=4), shape=SHARED)
    assert (b.cores, b.memory_gb, b.workers) == (5, 17, 5), b


async def test_r1_a_batchs_derived_block_over_the_share_asks_for_and_bills_the_raised_cores():
    from tests.test_batch_sizing import _run, _sized_app

    app = _sized_app()
    out = await _run(app, n=8, cores=1, memory_gb=4.0, concurrency=4)
    assert out.phase == "needs_confirmation", (out.phase, out.notice)
    b = out.block
    assert b.cores_per_node == 10 and b.derived_from and b.max_charge == 5.0, b      # 10 cores × 0.5 h
    assert "17 GB needs 10 cores' memory share" in out.notice, out.notice
    yes = await _run(app, n=8, cores=1, memory_gb=4.0, concurrency=4, confirm_spend=True)
    assert app.built, (yes.phase, yes.notice)
    sent = app.built[-1]
    assert (sent["cores_per_node"], sent["mem_per_node"], sent["max_workers_per_node"]) == (10, 17, 5), sent


async def test_r1_a_derived_block_of_default_memory_rows_crossing_the_share_by_its_rounding():
    from tests.test_batch_sizing import _run, _sized_app

    app = _sized_app()
    # 16 rows × 1 CPU at the partition's 1896 MB: 17 CPUs, ⌈30336 + the pool's 1956 MB⌉ = 32 GB > 17 × 1896 MB — a
    # DEFAULTED memory is held to the cores' share (review round of #253: it had raised the block to 18 cores the user
    # never asked for), so 31 GB and 17 CPUs
    out = await _run(app, n=16, cores=1, concurrency=16)
    assert out.phase == "needs_confirmation", (out.phase, out.notice)
    assert (out.block.cores_per_node, out.block.memory_gb, out.block.cores_asked) == (17, 31, None), out.block
    b32 = sizing.derive_block(sizing.Sizing(cores=1, concurrency=32), shape=SHARED)
    assert (b32.cores, b32.memory_gb) == (33, 61), b32                  # it had been 63 GB: 35 cores
    assert resources.cores_for_memory(b32.cores, b32.memory_gb, SHARED) == 33
    # a memory the rows ASKED for is never held: 4 rows × 4 GB still raise to 10 (the test above)
    asked = sizing.derive_block(sizing.Sizing(cores=1, memory_mb=4096, concurrency=4), shape=SHARED)
    assert asked.memory_gb == 17 and resources.cores_for_memory(asked.cores, asked.memory_gb, SHARED) == 10


# =====================================================================================================================
# R1 — a Jobs-mode job
# =====================================================================================================================

def test_r1_a_jobs_request_asks_for_the_raised_cores_and_its_rate_counts_them():
    from hpc_bridge import jobs

    app = _app(walltime="00:15:00")
    spec = jobs.spec_of(app, command="true", session_id="default", account=None, partition="shared", nodes=None,
                        cores=8, memory_gb=15.5, gpus=None, whole_node=None, walltime="00:15:00", array=None,
                        depends_on=None)
    assert spec.cores == 9 and spec.rate == 9.0, spec
    assert "15.5 GB needs 9 cores' memory share" in spec.basis, spec.basis
    script = jobs.job_script(app, job_id="job-0123456789ab", envelope_id="env-0123456789ab", spec=spec, deadline=0.0)
    assert "#SBATCH --cpus-per-task=9" in script and "#SBATCH --mem=15872M" in script, script[:600]
    terms = jobs.terms_of(app, spec, deadline=time.time() + 600, backstop=None)
    assert terms.cores_per_node == 9


# =====================================================================================================================
# R1 — a facility endpoint's template (Anvil's own multi-user endpoint)
# =====================================================================================================================

async def test_r1_anvils_facility_endpoint_sends_the_raised_cores_beside_its_whole_gb_memory(monkeypatch):
    from tests.test_resource_review_r2 import _anvil

    app, _res = await _anvil(monkeypatch)
    # 15.5 GB goes to the template's whole-GB key as 16 GB: 16384 MB needs 9 cores' share of 1896 MB
    r = await _ensure_endpoint_up(app, shape="compute", account="cis250223", cores=8, memory_gb=15.5)
    assert (r.block.whole_node, r.block.cores_per_node, r.block.memory_gb) == (False, 9, 16.0), r.block
    assert "16 GB needs 9 cores' memory share on partition 'shared' (1896 MB each)" in r.notice, r.notice
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    sent = app.built[-1]
    assert (sent["cores_per_node"], sent["mem_per_node"]) == (9, 16), sent
    # the registry's own 1 core with 1.9 GB asked: 2 GB sent is over one core's 1896 MB — 2 cores
    one = await _ensure_endpoint_up(app, shape="compute", cores=1, memory_gb=1.9)
    assert one.block.cores_per_node == 2, one.block


async def test_r1_a_facility_endpoint_whose_share_is_unknown_says_more_cores_may_bill(monkeypatch):
    from tests.test_resource_review_r2 import _anvil

    noshare = SHARED.model_copy(update={"mem_per_core_mb": None, "max_mem_per_core_mb": None,
                                          "why": "OverSubscribe=YES", "billing": None})
    app, _res = await _anvil(monkeypatch)
    app.facility.partitions = (noshare,)
    r = await _ensure_endpoint_up(app, shape="compute", account="cis250223", memory_gb=4)
    assert r.block.cores_per_node == 1 and r.block.memory_gb == 4, r.block     # the registry's 1 core: not guessed
    assert "may give — and bill — more cores than asked" in r.notice, r.notice


# =====================================================================================================================
# R1 — an exported handoff's config (it goes out as it is, its canary too)
# =====================================================================================================================

async def test_r1_a_handoff_config_whose_memory_is_above_its_cores_share_is_refused_before_any_canary(tmp_path):
    from tests.test_handoff_phase2b import _export, _sent, _ssh_app

    shared = SHARED.model_copy(update={"name": "main"})
    app = _ssh_app(tmp_path, partitions=(shared,))
    out = await _export(app, {"partition": "main", "walltime": "00:30:00", "max_blocks": 1, "exclusive": False,
                              "cores_per_node": 8, "mem_per_node": 16})
    assert out.phase != "needs_confirmation" and _sent(app) == [], out
    assert "16 GB needs 9 cores' memory share" in out.notice and "`cores_per_node`: 9" in out.notice, out.notice
    ok = await _export(app, {"partition": "main", "walltime": "00:30:00", "max_blocks": 1, "exclusive": False,
                             "cores_per_node": 9, "mem_per_node": 16})
    assert ok.phase == "needs_confirmation" and ok.terms.cores_per_node == 9, ok


# =====================================================================================================================
# R1 — Slurm's own check, hermetic: the block script hpc-bridge's wire renders, run under a fake Slurm 23.11 that
# raises the job's CPUs to cover its memory (as Anvil did) and a fake srun that refuses as parsl's launcher met it
# =====================================================================================================================

_BASH = shutil.which("bash")

# Slurm, for one job: --cpus-per-task C, --mem M; NumCPUs = max(C, ceil(M / share)) — SLURM_CPUS_PER_TASK is the
# job's (raised) count, SLURM_TRES_PER_TASK the request's.
_FAKE_SBATCH = r"""
share=$1; script=$2
c=$(sed -n 's/^#SBATCH --cpus-per-task=\([0-9][0-9]*\).*/\1/p' "$script" | tail -n 1)
m=$(sed -n 's/^#SBATCH --mem=\([0-9][0-9]*[gGmM]\{0,1\}\).*/\1/p' "$script" | tail -n 1)
case "$m" in
  *[gG]) mb=$(( ${m%?} * 1024 ));;
  *[mM]) mb=${m%?};;
  '') mb='';;
  *) mb=$m;;
esac
cpus=${c:-1}
if [ -n "$mb" ]; then
  need=$(( (mb + share - 1) / share ))
  if [ "$need" -gt "$cpus" ]; then cpus=$need; fi
fi
SLURM_CPUS_PER_TASK=$cpus; export SLURM_CPUS_PER_TASK
if [ -n "$c" ]; then SLURM_TRES_PER_TASK="cpu=$c"; export SLURM_TRES_PER_TASK; fi
SLURM_JOB_NAME=parsl.test; export SLURM_JOB_NAME
cd "$(dirname "$script")" && exec bash "$script"
"""

_FAKE_SRUN = r"""#!/bin/sh
if [ -n "${SLURM_TRES_PER_TASK:-}" ] && [ -n "${SLURM_CPUS_PER_TASK:-}" ]; then
  c=$(printf '%s\n' "$SLURM_TRES_PER_TASK" | tr ',' '\n' | sed -n 's/^cpu[=:]//p' | head -n 1)
  if [ -n "$c" ] && [ "$c" != "$SLURM_CPUS_PER_TASK" ]; then
    echo "srun: fatal: cpus-per-task set by two different environment variables SLURM_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK != SLURM_TRES_PER_TASK=$SLURM_TRES_PER_TASK" >&2
    exit 1
  fi
fi
echo HPCB_POOL_STARTED
"""


def _block_script(wire: dict, tmp: Path) -> Path:
    """The sbatch script for the rendered provider block `wire`: parsl's own when it is importable (the locked
    2026.8.10: `uv run --with parsl==2026.8.10 …`), else its documented rendering — SlurmProvider writes cores_per_node as
    `--cpus-per-task` (one task per node), mem_per_node as `--mem=<GB>g`, then the scheduler_options, and SrunLauncher
    starts the pool with `srun --ntasks <nodes>` (tests/test_resource_templates.py reads both)."""
    try:
        from parsl.launchers import SrunLauncher
        from parsl.providers import SlurmProvider
    except ImportError:
        lines = ["#!/bin/bash", "#SBATCH --job-name=parsl.test", f"#SBATCH --nodes={wire.get('nodes_per_block', 1)}",
                 "#SBATCH --ntasks-per-node=1"]
        if wire.get("cores_per_node"):
            lines.append(f"#SBATCH --cpus-per-task={wire['cores_per_node']}")
        if wire.get("mem_per_node"):
            lines.append(f"#SBATCH --mem={wire['mem_per_node']}g")
        if wire.get("exclusive", True):
            lines.append("#SBATCH --exclusive")
        lines += [ln for ln in (wire.get("scheduler_options") or "").splitlines() if ln.strip()]
        lines.append("srun --ntasks 1 -l bash -c 'process_worker_pool.py --block_id=0'")
        p = tmp / "parsl.test.submit"
        p.write_text("\n".join(lines) + "\n")
        return p
    calls: list[str] = []

    class _P(SlurmProvider):
        def execute_wait(self, cmd, timeout=None):
            calls.append(cmd)
            return 0, "Submitted batch job 7\n", ""

    kw = {k: wire[k] for k in ("partition", "account", "walltime", "scheduler_options", "exclusive", "cores_per_node",
                               "mem_per_node", "nodes_per_block") if wire.get(k) is not None}
    p = _P(launcher=SrunLauncher(), **kw)
    p.script_dir = str(tmp)
    p.submit("process_worker_pool.py --block_id=0", 1, "parsl.test")
    return Path(calls[-1].split()[-1])


def _run_block(wire: dict, tmp: Path) -> subprocess.CompletedProcess:
    (tmp / "bin").mkdir(exist_ok=True)
    srun = tmp / "bin" / "srun"
    srun.write_text(_FAKE_SRUN)
    srun.chmod(0o755)
    script = _block_script(wire, tmp)
    env = {"PATH": f"{tmp / 'bin'}:/usr/bin:/bin", "HOME": str(tmp)}
    return subprocess.run([_BASH, "-c", _FAKE_SBATCH, "sbatch", str(_SHARE), str(script)], capture_output=True,
                          text=True, env=env, timeout=60)


@pytest.mark.skipif(_BASH is None, reason="no bash")
async def test_r1_the_block_hpc_bridge_sends_passes_sruns_check_under_slurms_raised_cpus(tmp_path):
    app = _app(walltime="00:15:00")
    await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15.5, confirm_spend=True)
    sent = app.built[-1]
    done = _run_block(sent, tmp_path)
    assert "srun: fatal" not in done.stderr and "HPCB_POOL_STARTED" in done.stdout, (done.stdout, done.stderr)
    # and the old request, as sent on 2026-11-10, fails it — the fake is the check
    old = {**sent, "cores_per_node": 8}
    bad = _run_block(old, tmp_path)
    assert "cpus-per-task set by two different environment variables" in bad.stderr, (bad.stdout, bad.stderr)


# =====================================================================================================================
# R2 — a block's age bounds a task
# =====================================================================================================================

def test_r2_the_bounds_note_counts_the_blocks_age():
    app = _app(walltime="00:15:00")
    rt = server._shape_runtime(app, "compute")
    fresh = notices._billed_bounds_note(app, rt)
    assert "a task runs up to ~880s" in fresh, fresh                        # no age known: the walltime less margin
    rt.block_since = time.monotonic() - 300                                  # up 5 min of its 15
    aged = notices._billed_bounds_note(app, rt)
    assert "~880s" not in aged, aged
    n = int(aged.split("a task runs up to ~")[1].split("s")[0])
    assert 590 <= n <= 600, aged
    assert "left of the block's 00:15:00 walltime" in aged, aged
    # counted from its first answer, not Slurm's StartTime: about, never exact (review round of #253)
    assert "about what is left" in aged and "first answer" in aged, aged


def test_r2_a_running_tasks_words_never_outlast_the_block():
    app = _app(walltime="00:15:00")
    out = notices._running_outcome(app, "t-1", 880.0, block_left_s=600.0)
    assert "~880s" not in out.notice and "~600s" in out.notice, out.notice
    assert "about ~600s" in out.notice and "first answer" in out.notice, out.notice


async def test_r2_a_reused_earlier_block_states_what_is_left_of_its_walltime(monkeypatch):
    from hpc_bridge.ledger import Ledger
    from tests.test_restart_block_record import EID, _connect, _listing, _server, _session_1, _Slurm

    class Slurm15(_Slurm):
        def config_template(self, profile):
            return "", {"walltime": "00:15:00"}

    fac = Slurm15()
    _s1, n = await _session_1(monkeypatch, fac)
    led = Ledger.open()
    led._con.execute("update block set up_at = up_at - 300 where endpoint_id = ?", (EID,))   # up since 5 min ago
    s2 = _server(monkeypatch, fac)
    await _connect(s2, _listing(n=n))
    st = await _ensure_endpoint_up(s2, confirm_spend=True)
    assert st.status == "up" and "Reused block job 40" in st.notice, st.notice
    assert "~880s" not in st.notice, st.notice
    n_s = int(st.notice.split("a task runs up to ~")[1].split("s")[0])
    assert n_s <= 600, st.notice


# =====================================================================================================================
# Round 2 (2026-11-10, Anvil's live `scontrol show partition`): only an ENFORCED share raises cores. `shared` reads
# DefMemPerCPU=1896 MaxMemPerCPU=1896 — Slurm's own MaxMemPerCPU enforcement is what added the CPU; `gpu` reads
# DefMemPerCPU=3750 with no MaxMemPerCPU (MaxMemPerNode=UNLIMITED), where Slurm adds no CPUs for memory. Three cases:
# (a) MaxMemPerCPU known: raise; (b) known to have none: no raise, no line; (c) not known: no raise, one honest line.
# DefMemPerCPU alone only caps the default proposal's memory.
# =====================================================================================================================

_SCONTROL = [
    "PartitionName=shared AllowGroups=ALL Default=YES QoS=part-shared DefaultTime=00:30:00 MaxNodes=1 "
    "OverSubscribe=NO State=UP TotalNodes=250 DefMemPerCPU=1896 MaxMemPerCPU=1896 TRESBillingWeights=CPU=1.0",
    "PartitionName=wholenode QoS=part-standard OverSubscribe=EXCLUSIVE State=UP TotalNodes=750 "
    "DefMemPerNode=UNLIMITED MaxMemPerNode=UNLIMITED TRESBillingWeights=CPU=1.0",
    "PartitionName=gpu QoS=part-gpu OverSubscribe=NO State=UP TotalNodes=16 DefMemPerCPU=3750 "
    "MaxMemPerNode=UNLIMITED TRESBillingWeights=GRES/gpu=1.0",
    # the 2026-10-24 capture asked for a few fields only: the per-core cap is not known from it
    "PartitionName=debug QoS=part-debug OverSubscribe=NO State=UP DefMemPerCPU=1896 TRESBillingWeights=CPU=1.0",
]


def _discovered() -> dict[str, PartitionShape]:
    parts = resources.classify_slurm(select_type="select/cons_tres", select_params="CR_CORE_MEMORY",
                                     shapes=["shared|128|257400|(null)", "wholenode|128|257400|(null)",
                                             "gpu|128|515400|gpu:4", "debug|128|257400|(null)"],
                                     details=_SCONTROL, qos=[],
                                     # Anvil's cluster sets no per-CPU cap of its own (`gpu` falls back to none):
                                     # `scontrol show config` reads MaxMemPerNode = UNLIMITED (review round of #253)
                                     cluster_max_mem_per_node="UNLIMITED")
    return {p.name: p for p in parts}


def test_round2_discovery_reads_the_enforced_share_and_whether_there_is_one():
    p = _discovered()
    assert (p["shared"].mem_per_core_mb, p["shared"].max_mem_per_core_mb) == (1896, 1896)
    assert p["gpu"].mem_per_core_mb == 3750 and p["gpu"].max_mem_per_core_mb is None
    assert p["gpu"].max_mem_per_core_known is True                       # (b): read, and there is none
    assert p["wholenode"].max_mem_per_core_known is True
    assert p["debug"].max_mem_per_core_known is False                    # (c): not in what was read
    assert resources.enforced_share_mb(p["shared"]) == 1896 and resources.enforced_share_mb(p["gpu"]) is None
    assert not resources.share_unknown(p["shared"]) and not resources.share_unknown(p["gpu"])
    assert resources.share_unknown(p["debug"]) and resources.share_unknown(None)


async def test_round2_a_max_equal_to_def_raises_as_on_anvils_shared():
    shared = _discovered()["shared"].model_copy(update={"cores": 128, "memory_gb": 251.4})
    app = _app(partitions=(shared,), walltime="00:15:00")
    r = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15.5)
    assert (r.block.cores_per_node, r.block.cores_asked, r.block.max_charge) == (9, 8, 2.25), r.block
    assert "15.5 GB needs 9 cores' memory share on partition 'shared' (1896 MB each)" in r.notice, r.notice


async def test_round2_a_def_only_partition_never_raises_cores_nor_bills_them():
    gpu = _discovered()["gpu"].model_copy(update={"billing": {"cpu": 1.0}})
    app = _app(partitions=(gpu,), partition="gpu", walltime="00:15:00")
    # 60 GB is over 8 × 3750 MB, but Slurm adds no CPUs for memory without a MaxMemPerCPU
    r = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=60)
    assert (r.block.cores_per_node, r.block.cores_asked, r.block.max_charge) == (8, None, 2.0), r.block
    assert "memory share" not in r.notice and "more cores than asked" not in r.notice, r.notice   # (b): no line
    assert "may bill too" not in r.notice, r.notice
    assert resources.cores_for_memory(8, 60, gpu) == 8
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert app.built[-1]["cores_per_node"] == 8, app.built[-1]


async def test_round2_the_default_on_a_def_only_partition_is_capped_by_def_and_raises_nothing():
    gpu = _discovered()["gpu"].model_copy(update={"billing": {"cpu": 1.0}})
    app = _app(partitions=(gpu,), partition="gpu")
    d = await _ensure_endpoint_up(app, shape="compute")
    b = d.block
    assert (b.cores_per_node, b.memory_gb, b.memory_default, b.cores_asked) == (8, 29.2, True, None), b
    assert b.memory_gb * 1024 <= 8 * 3750                                # never above cores × DefMemPerCPU
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert "mem_per_node" not in app.built[-1] or app.built[-1]["mem_per_node"] is None, app.built[-1]
    # the default's size said back rounded up (30 GB: over 8 × 3750 MB) adds no CPU there
    up = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=30)
    assert up.block.cores_per_node == 8, up.block


async def test_round2_an_unknown_share_raises_nothing_and_says_so_once():
    debug = _discovered()["debug"].model_copy(update={"cores": 128, "memory_gb": 251.4})
    app = _app(partitions=(debug,), partition="debug")
    r = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15.5)
    assert (r.block.cores_per_node, r.block.cores_asked) == (8, None), r.block
    said = r.notice.count("more cores than asked") + r.notice.count("may bill too")
    assert said == 1, r.notice
    assert "8 cores × 0.5 h" in (r.block.charge_basis or ""), r.block.charge_basis


async def test_round2_anvils_registry_entry_states_shareds_cap_and_leaves_debug_unknown(monkeypatch):
    from tests.test_resource_review_r2 import _anvil, _seed

    entry = _seed("anvil.yaml", "anvil")
    parts = {p.name: p for p in entry.defaults.partitions or []}
    assert parts["shared"].max_mem_per_core_mb == 1896                  # read live 2026-11-10
    assert parts["debug"].max_mem_per_core_mb is None and not parts["debug"].max_mem_per_core_known
    app, _res = await _anvil(monkeypatch)
    r = await _ensure_endpoint_up(app, shape="compute", account="cis250223", partition="debug", cores=1,
                                  memory_gb=4)
    assert r.block.cores_per_node == 1, r.block                          # (c): not raised …
    said = r.notice.count("more cores than asked") + r.notice.count("may bill too")
    assert said == 1, r.notice                                           # … and said once


def test_round2_pbs_raises_only_on_a_share_a_registry_entry_states():
    plain = PartitionShape(name="workq", kind="per_core", cores=64, memory_gb=256.0, mem_per_core_mb=4000)
    stated = plain.model_copy(update={"max_mem_per_core_mb": 4000})
    uec = {"compute": True, "partition": "workq", "nodes_per_block": 1, "cores_per_node": 2, "mem_per_node": 16}
    assert resources.effective(uec, plain, channel="ssh", scheduler="pbs", profile_cpus=64).cores == 2
    assert resources.effective(uec, stated, channel="ssh", scheduler="pbs", profile_cpus=64).cores == 5   # 16384 / 4000


# =====================================================================================================================
# Review round of #253 (2026-11-10)
# 1. `MaxMemPerNode=UNLIMITED` on a partition is no "known: no share": Slurm 23.11 (`job_mgr.c` `_valid_pn_min_mem`)
#    falls back to the cluster's own MaxMemPerCPU (slurm.conf) for a partition with none of its own, which
#    `partition_info.c` prints as `MaxMemPerNode=UNLIMITED`. The probe reads the cluster's from `scontrol show config`.
# 2. A raise past MaxCPUsPerNode (or a QOS per-job cap) is refused on every path, naming the cap and what to change.
# 4. export_handoff never advises more cores than a node has.
# =====================================================================================================================

_GLOBAL_LINE = ("PartitionName=shared QoS=part-shared OverSubscribe=NO State=UP TotalNodes=250 DefMemPerCPU=1896 "
                "MaxMemPerNode=UNLIMITED TRESBillingWeights=CPU=1.0")


def _one(detail: str, **conf) -> PartitionShape:
    return resources.classify_slurm(select_type="select/cons_tres", select_params="CR_CORE_MEMORY",
                                    shapes=["shared|128|257400|(null)"], details=[detail], qos=[], **conf)[0]


def test_review1_a_cluster_wide_maxmempercpu_applies_where_the_partition_sets_none():
    p = _one(_GLOBAL_LINE, cluster_max_mem_per_cpu="1896")
    assert (p.max_mem_per_core_mb, p.max_mem_per_core_known) == (1896, True), p
    assert resources.cores_for_memory(8, 15.5, p) == 9
    # the cluster read, with no per-CPU cap of its own (MaxMemPerNode = UNLIMITED): known — there is none
    none = _one(_GLOBAL_LINE, cluster_max_mem_per_node="UNLIMITED")
    assert (none.max_mem_per_core_mb, none.max_mem_per_core_known) == (None, True), none


def test_review1_a_partitions_own_setting_overrides_the_cluster():
    own = _one(_GLOBAL_LINE.replace("MaxMemPerNode=UNLIMITED", "MaxMemPerCPU=4000"), cluster_max_mem_per_cpu="1896")
    assert (own.max_mem_per_core_mb, own.max_mem_per_core_known) == (4000, True), own
    unl = _one(_GLOBAL_LINE.replace("MaxMemPerNode=UNLIMITED", "MaxMemPerCPU=UNLIMITED"),
               cluster_max_mem_per_cpu="1896")
    assert (unl.max_mem_per_core_mb, unl.max_mem_per_core_known) == (None, True), unl
    per_node = _one(_GLOBAL_LINE.replace("MaxMemPerNode=UNLIMITED", "MaxMemPerNode=200000"),
                    cluster_max_mem_per_cpu="1896")
    assert (per_node.max_mem_per_core_mb, per_node.max_mem_per_core_known) == (None, True), per_node


def test_review1_an_unlimited_partition_with_the_cluster_not_read_is_unknown():
    p = _one(_GLOBAL_LINE)
    assert (p.max_mem_per_core_mb, p.max_mem_per_core_known) == (None, False), p      # (c), never (b)
    assert resources.share_unknown(p)


@pytest.mark.skipif(_BASH is None, reason="no bash")
def test_review1_the_probe_reads_the_clusters_cap_from_scontrol_show_config(tmp_path, monkeypatch):
    from hpc_bridge import discovery

    monkeypatch.delenv("HPC_BRIDGE_REMOTE_VENV", raising=False)
    d = tmp_path / "bin"
    d.mkdir()
    tools = {
        "sinfo": "#!/bin/sh\ncase $3 in\n  '%P|%a') echo 'shared*|up' ;;\n"
                 "  'PSHAPE=%P|%c|%m|%G') echo 'PSHAPE=shared*|128|257400|(null)' ;;\nesac\n",
        "scontrol": "#!/bin/sh\ncase $2 in\n"
                    "  config) printf '%s\\n' 'SelectType              = select/cons_tres' "
                    "'SelectTypeParameters    = CR_CORE_MEMORY' 'MaxMemPerCPU            = 1896' ;;\n"
                    f"  partition) echo '{_GLOBAL_LINE}' ;;\nesac\n",
        "sacctmgr": "#!/bin/sh\n", "id": "#!/bin/sh\necho alice users\n", "whoami": "#!/bin/sh\necho alice\n",
    }
    for name, body in tools.items():
        (d / name).write_text(body)
        (d / name).chmod(0o755)
    env = {"PATH": f"{d}:/usr/bin:/bin", "HOME": str(tmp_path), "USER": "alice", "LC_ALL": "C"}
    r = subprocess.run([_BASH, "--noprofile", "--norc", "-c", discovery._probe_script()], env=env,
                       capture_output=True, text=True, timeout=60, check=False)
    assert "HPCB_PROBE_END" in r.stdout, r.stderr
    draft, _notes = discovery.parse_probe(r.stdout, ssh_host="cluster.example.org")
    shared = {p.name: p for p in draft.partitions or []}["shared"]
    assert (shared.max_mem_per_core_mb, shared.max_mem_per_core_known) == (1896, True), shared


async def test_review2_a_raise_past_maxcpuspernode_is_refused_naming_the_cap():
    capped = SHARED.model_copy(update={"max_cores_per_node": 8})
    app = _app(partitions=(capped,))
    res = await _ensure_endpoint_up(app, shape="compute", cores=8, memory_gb=15.5, confirm_spend=True)
    assert app.built == [], app.built
    assert "MaxCPUsPerNode=8" in res.notice and "less memory" in res.notice, res.notice
    assert "14.8 GB" in res.notice, res.notice                    # the most 8 cores' share holds


def test_review2_derive_block_checks_maxcpuspernode_beside_a_qos_cap():
    shape = SHARED.model_copy(update={"max_cores_per_job": 200, "max_cores_per_node": 9})
    with pytest.raises(sizing.SizingRefusal, match="MaxCPUsPerNode"):     # 10 CPUs (the memory's) on a 9-CPU cap
        sizing.derive_block(sizing.Sizing(cores=1, memory_mb=4096, concurrency=4), shape=shape)


@pytest.mark.parametrize("cap", [{"max_cores_per_node": 8}, {"max_cores_per_job": 8}])
def test_review2_a_jobs_raised_cores_meet_every_cap(cap):
    from hpc_bridge import jobs

    app = _app(partitions=(SHARED.model_copy(update=cap),))
    with pytest.raises(jobs.JobRefusal) as exc:
        jobs.spec_of(app, command="true", session_id="default", account=None, partition="shared", nodes=None,
                     cores=8, memory_gb=15.5, gpus=None, whole_node=None, walltime="00:15:00", array=None,
                     depends_on=None)
    msg = str(exc.value)
    assert "9" in msg and ("MaxCPUsPerNode" in msg or "per job" in msg) and "less memory" in msg, msg
    # and the unraised count meets them too (it had checked a node's cores only)
    with pytest.raises(jobs.JobRefusal):
        jobs.spec_of(app, command="true", session_id="default", account=None, partition="shared", nodes=None,
                     cores=9, memory_gb=None, gpus=None, whole_node=None, walltime="00:15:00", array=None,
                     depends_on=None)


async def test_review4_a_handoff_memory_no_node_holds_is_refused_as_such(tmp_path):
    from tests.test_handoff_phase2b import _export, _sent, _ssh_app

    app = _ssh_app(tmp_path, partitions=(SHARED.model_copy(update={"name": "main"}),))
    out = await _export(app, {"partition": "main", "walltime": "00:30:00", "max_blocks": 1, "exclusive": False,
                              "cores_per_node": 120, "mem_per_node": 240})
    assert out.phase != "needs_confirmation" and _sent(app) == [], out
    assert "`cores_per_node`: 130" not in out.notice, out.notice           # never advise more cores than a node has
    assert "128 cores" in out.notice and "less memory" in out.notice, out.notice
