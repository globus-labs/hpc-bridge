"""Round 5 of the review of the scheduler-directive fix (on ad664ce): this request's user endpoint, started but with no
pilot listed, unread — so an earlier request's pilot whose log names no request was taken for this block's (a refused
submission read as queued); a squeue that refuses the field suffix read as a failed query on every poll; the Slurm
GPU advice that lost the line's ngpus; a foreign directive on PBS refused for a term it does not set; and a HOME with a
space that lost a dead pilot's own reason.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from hpc_bridge import binding, scheduler_ops
from hpc_bridge.catalog.entry import defaults_extra_raw_problems
from hpc_bridge.models import FacilityDetails
from tests.fakes import fake_entry
from tests.test_scheduler_directives import (
    _PBS,
    _SLURM,
    EID,
    UEP_A,
    UEP_B,
    _refusal,
    _runner,
    _stub,
    _uep_app,
    _uep_log,
)

PAST_GRACE = scheduler_ops.PROVISION_GRACE_S + 60

# --- R1. this request's own user endpoint is known past the grace, pilot or none ----------------------------------------


def _earlier_pilot_only(tmp_path: Path, variant: str) -> dict:
    """rv-pbs-r5/unread_own.py: this request's user endpoint A has logged block request 12 but has no pilot (its
    submission was refused); an earlier user endpoint B has a live PENDING pilot 57, its log naming no block request."""
    env = _stub(tmp_path, f"PENDING 57 Priority /h/.globus_compute/uep.{EID}.{UEP_B}/submit_scripts/b.stdout\n")
    _uep_log(tmp_path, UEP_A, 12)
    bdir = tmp_path / ".globus_compute" / f"uep.{EID}.{UEP_B}"
    bdir.mkdir(parents=True, exist_ok=True)
    if variant == "past50":       # its start past the lookup's first 50 lines (or rotated beyond .log.1)
        (bdir / "endpoint.log").write_text("x\n" * 60 + "running endpoint_setup command: true hpcb_block=11\n")
    elif variant == "named":      # control: its log names block request 11
        (bdir / "endpoint.log").write_text("\nrunning endpoint_setup command: true hpcb_block=11\n")
    return env                    # "nolog": B's directory holds no log


@pytest.mark.parametrize("variant", ["nolog", "past50", "named"])
async def test_a_refused_request_is_never_read_as_an_earlier_requests_pilot(tmp_path, variant):
    env = _earlier_pilot_only(tmp_path, variant)
    p = await scheduler_ops._probe_pilots_over_login(_uep_app(), EID, PAST_GRACE, _runner(env), block=12)
    assert p is not None and p.category == "rejected" and p.subject is None
    assert "NO pilot job of this block request" in p.suffix and "likely REJECTED" in p.suffix
    assert "pilot 57 (PENDING, reason 'Priority')" in p.suffix          # named after the verdict, as another's


def test_the_refusal_reader_names_this_requests_user_endpoint_once_after_the_rows(tmp_path):
    env = _earlier_pilot_only(tmp_path, "nolog")
    gc = tmp_path / ".globus_compute"
    for i in range(400):          # 1000..1399: "hpcb_block=12…" in 100 of them passes grep -F; awk confirms the number
        d = gc / f"uep.{EID}.00000000-0000-0000-0000-{i:012d}"
        d.mkdir(parents=True)
        (d / "endpoint.log").write_text(f"\nrunning endpoint_setup command: true hpcb_block={1000 + i}\n")
    cmd = scheduler_ops._pilot_status_cmd("slurm", EID, 12, request=12)
    lines = subprocess.run(["bash", "-c", cmd], env=env, capture_output=True, text=True, check=False).stdout.split("\n")
    assert [ln for ln in lines if ln.startswith("HPCB_BREQ ")] == [f"HPCB_BREQ {UEP_A} 12"]
    assert lines.index(f"HPCB_BREQ {UEP_A} 12") > lines.index("PENDING 57 - Priority")
    assert "HPCB_BREQ" not in subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("slurm", EID, request=12)],
                                             env=env, capture_output=True, text=True, check=False).stdout


async def test_a_template_not_known_to_name_requests_never_uses_the_refusal_readers_line(tmp_path):
    # guard: where the probe was not asked by request (logs_block_requests unknown or false), the line is not compared
    # with anything: this block's own pilot is never "another request's" for it
    env = _stub(tmp_path, f"PENDING 57 Priority /h/.globus_compute/uep.{EID}.{UEP_A}/submit_scripts/a.stdout\n")
    _uep_log(tmp_path, UEP_A, 12)
    p = await scheduler_ops._probe_pilots_over_login(_uep_app(logs=False), EID, PAST_GRACE, _runner(env), block=12)
    assert p is not None and p.category == "queued" and p.subject == "57"


# --- N1. a squeue that refuses the field suffix --------------------------------------------------------------------------


def _plain(st: str, j: str, r: str, path: str) -> str:
    return f"{st:<20}{j:<24}{r[:60]:<60}{path:<1024}\n"


async def test_a_squeue_that_refuses_the_suffix_is_asked_again_without_it(tmp_path):
    # rv-pbs-r5/slurm_suffix.py "old-rejects": an older squeue's "Invalid job format specification", exit 1
    body = _plain("PENDING", "57", "Priority", f"/h/.globus_compute/uep.{EID}.{UEP_A}/a.stdout")
    env = _stub(tmp_path, body)
    sq = Path(env["PATH"].split(":")[0]) / "squeue"
    sq.write_text("#!/bin/sh\ncase \"$*\" in *'|'*) echo 'squeue: error: Invalid job format specification' >&2; "
                  "exit 1;; esac\ncat <<'EOF'\n" + body + "EOF\n")
    out = subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("slurm", EID)], env=env, capture_output=True,
                         text=True, check=False).stdout
    assert "HPCB_FAIL" not in out and scheduler_ops._pilot_rows(out) == [["PENDING", "57", "-", "Priority"]]
    _uep_log(tmp_path, UEP_A, 12)
    p = await scheduler_ops._probe_pilots_over_login(_uep_app(), EID, 30.0, _runner(env), block=12)
    assert p is not None and p.category == "queued" and p.subject == "57"         # never "unknown" on every poll


def test_a_squeue_that_fails_both_ways_is_still_a_failed_query(tmp_path):
    env = _stub(tmp_path, "")
    (Path(env["PATH"].split(":")[0]) / "squeue").write_text("#!/bin/sh\nexit 1\n")
    out = subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("slurm", EID)], env=env, capture_output=True,
                         text=True, check=False).stdout
    assert "HPCB_FAIL squeue" in out


# --- N4. the Slurm GPU advice reads the line's own ngpus -----------------------------------------------------------------


@pytest.mark.parametrize("opts", ["#PBS -l select=2:ngpus=4", "#PBS -l select=1:ngpus=4,walltime=01:00:00",
                                  "#PBS -l select=1:ngpus=4+1:ngpus=4", "#PBS -l select=1:ncpus=8:ngpus=4"])
def test_the_slurm_gpu_advice_keeps_the_lines_ngpus(opts):
    msg = _refusal(opts, _SLURM)
    assert "ensure_endpoint_up(gpus=4)" in msg and "gpus=<n>" not in msg


# --- 6a. a foreign directive on PBS is refused as what it is -----------------------------------------------------------


@pytest.mark.parametrize("opts", ["#SBATCH --output=/x", "#SBATCH -a 0-3", "#SBATCH -A proj", "#BSUB -q gpu"])
def test_a_foreign_directive_on_pbs_is_not_a_directive_there(opts):
    # qsub reads #PBS alone: an #SBATCH line sets nothing there — it is refused as not this scheduler's directive
    msg = _refusal(opts, _PBS)
    assert "is not a #PBS directive or a plain comment" in msg and "scheduler_options sets" not in msg


@pytest.mark.parametrize(("opts", "kind"), [("#PBS -J 0-3", "a job array"), ("#SLURM --account=x", "the account"),
                                            ("#PBS -A x", "the account")])
def test_a_foreign_directive_sbatch_reads_is_still_refused_for_its_term(opts, kind):
    assert f"scheduler_options sets {kind}" in _refusal(opts, _SLURM)              # sbatch translates or reads these
    assert "scheduler_options sets the account" in _refusal("#PBS-Afoo", _PBS)  # qsub reads its own word unspaced


def test_the_ingest_names_a_foreign_directive_on_pbs_as_such():
    raw = fake_entry(id="pbsx", facility_key="lab").model_dump(mode="json")
    raw["compute"]["scheduler"] = "pbs"
    raw["compute"]["scheduler_options"] = "#SBATCH -A proj"
    problems = defaults_extra_raw_problems(raw)
    assert any("is not a #PBS directive or a plain comment" in p for p in problems)
    assert not any("sets the account" in p for p in problems)
    raw["compute"]["scheduler"] = "slurm"
    raw["compute"]["scheduler_options"] = "#PBS -A proj"                               # sbatch translates it
    assert any("sets the account or the partition/queue" in p for p in defaults_extra_raw_problems(raw))


def test_a_cached_foreign_directive_on_pbs_is_dropped_as_not_this_schedulers():
    d, dropped = binding._accepted_details(FacilityDetails(**_PBS, scheduler_options="#SBATCH --output=/x"))
    assert d.scheduler_options is None and dropped == ["'#SBATCH --output=/x' (not a directive of this scheduler)"]


# --- 6b. a HOME with a space keeps a dead pilot's own reason -------------------------------------------------------------


_SAID = "hpc-bridge: endpoint venv /x (built on x86_64) does not run on c1 (aarch64)"


def test_a_dead_slurm_pilot_under_a_home_with_a_space_says_why(tmp_path):
    scripts = tmp_path / "John Doe" / ".globus_compute" / f"uep.{EID}.{UEP_A}" / "submit_scripts"
    scripts.mkdir(parents=True)
    (scripts / "b.submit.stderr").write_text(f"some noise\n{_SAID}\n")
    env = _stub(tmp_path, "", f"FAILED|60|1:0|sbatch {scripts}/b.submit|1792538300\n")
    out = subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("slurm", EID)], env=env, capture_output=True,
                         text=True, check=False).stdout
    assert f"HPCB_SAID 60 {_SAID}\n" in out and f"HPCB_DIR 60 {UEP_A}\n" in out


def test_a_dead_pbs_pilot_under_a_home_with_a_space_says_why(tmp_path):
    home = tmp_path / "John Doe"
    home.mkdir()
    (home / "err.log").write_text(f"{_SAID}\n")
    rec = (f"Job Id: 5.pbsserver\n    job_state = F\n    Exit_status = 1\n    Error_Path = login01:{home}/err.log\n"
           f"    Output_Path = login01:/h/.globus_compute/uep.{EID}.{UEP_A}/submit_scripts/y.stdout\n")
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "qstat").write_text("#!/bin/sh\ncat <<'EOF'\n" + rec + "EOF\n")
    (bin_ / "qstat").chmod(0o755)
    out = subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("pbs", EID)],
                         env={"HOME": str(tmp_path), "USER": "u", "PATH": f"{bin_}:/usr/bin:/bin"},
                         capture_output=True, text=True, check=False).stdout
    assert "F 5.pbsserver 1" in out and f"HPCB_SAID 5.pbsserver {_SAID}\n" in out
