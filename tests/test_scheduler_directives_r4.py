"""Round 4 of the review of the scheduler-directive fix (on 117afe2): Slurm pilots named in PBS words by a reconnect, a
block request lookup whose output grew with the user endpoint directories until the worker's cut lost the pilot rows
(a false "likely REJECTED"), a select line's other resources lost by the advice and the cached conversion, PBS-only
advice on Slurm, the error file's refusal giving the output file's reason, the Slurm reason read by words, and nits.
New names are imported inside the tests, so each fails on its own against the round-3 code.
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path

import pytest

from hpc_bridge import binding, connect, notices, scheduler_ops
from hpc_bridge.context import AppCtx, SeenPilots, SshLeft
from hpc_bridge.models import FacilityDetails, ShellOutcome
from hpc_bridge.profile import Profile
from tests.fakes import fake_entry, fake_mep_entry
from tests.test_scheduler_directives import (
    _PBS,
    _SLURM,
    EID,
    UEP_A,
    UEP_B,
    _no_login,
    _refusal,
    _stub,
    _uep_app,
    _uep_log,
)
from tests.test_scheduler_directives_r3 import _parsl_select_line, _rendered_provider

# --- 1. a Slurm pilot left by a reconnect is named in Slurm's words ----------------------------------------------------


def _slurm_seen() -> SeenPilots:
    rows = [["PENDING", "57", "-", "Priority"], ["CONFIGURING", "58", "-", "None"], ["SUSPENDED", "59", "-", "None"]]
    return SeenPilots(jobs=scheduler_ops._live_pilots(rows), at=time.monotonic() - 5)


def test_recorded_pilots_keep_the_schedulers_own_state_word():
    seen = _slurm_seen()
    assert seen.jobs == (("57", "P", "Priority", "PENDING"), ("58", "C", "None", "CONFIGURING"),
                         ("59", "S", "None", "SUSPENDED"))
    assert seen.waiting == ("57", "58", "59") and seen.held == () and seen.reason("57") == "Priority"


def test_a_slurm_reconnect_note_names_its_pilots_in_slurm_words():
    # review r4 #1 (seen_words.py): "pilots 57 (PBS state P: see qstat, …)" on a Slurm site
    words = " ".join(notices._pilots_left_words(_slurm_seen()))
    assert "57 (PENDING, reason 'Priority')" in words and "58 (CONFIGURING)" in words and "59 (SUSPENDED)" in words
    assert "PBS state" not in words and "qstat" not in words
    app = AppCtx(facility=_uep_app().facility, profile=Profile())
    note = notices._ssh_left_note(app, EID, same=True, queued=0, left=SshLeft(up=False, pilots=_slurm_seen()))
    assert note.startswith("This connect cancels nothing on this endpoint") and "57 (PENDING, reason 'Priority')" in note
    assert "PBS state" not in note and "qstat" not in note


def test_a_pbs_reconnect_note_still_names_the_pbs_letter():
    seen = SeenPilots(jobs=scheduler_ops._live_pilots([["Q", "7.pbs", "-", ""], ["W", "8.pbs", "-", ""]]), at=0.0)
    words = " ".join(notices._pilots_left_words(seen))
    assert "7.pbs (PBS state Q: queued)" in words and "8.pbs (PBS state W: waiting for its start time)" in words


async def test_the_probe_records_the_word_squeue_listed():
    async def run_login(cmd):
        out = f"HPCB_NOW 1\nCONFIGURING 58 - None\nHPCB_DIR 58 {UEP_A}\nHPCB_BREQ {UEP_A} 12\n"
        return ShellOutcome(phase="complete", exit_code=0, stdout=out, block_state="warm")
    p = await scheduler_ops._probe_pilots_over_login(_uep_app(), EID, 30.0, run_login, block=12)
    assert p is not None and p.live == (("58", "C", "None", "CONFIGURING"),) and p.subject == "58"


# --- 2. the block request lookup never pushes the pilot rows out of the worker's cut ------------------------------------


def _cut_runner(env):
    """The login channel as the worker returns a task's stdout: its LAST runner.SNIPPET_LINES lines only."""
    from hpc_bridge.runner import SNIPPET_LINES

    async def run_login(cmd):
        r = subprocess.run(["bash", "-c", cmd], env=env, capture_output=True, text=True, check=False)
        lines = r.stdout.splitlines()
        return ShellOutcome(phase="complete", exit_code=r.returncode, stdout="\n".join(lines[-SNIPPET_LINES:]) + "\n",
                            block_state="warm")
    return run_login


def _many_ueps(tmp_path: Path, n: int) -> None:
    gc = tmp_path / ".globus_compute"
    for i in range(n):  # one directory per block request, until a teardown: none of them has a pilot listed
        d = gc / f"uep.{EID}.00000000-0000-0000-0000-{i:012d}"
        d.mkdir(parents=True)
        (d / "endpoint.log").write_text(f"\nrunning endpoint_setup command: true hpcb_block={1000 + i}\n")


@pytest.mark.parametrize("n", [300, 2100])
async def test_thousands_of_user_endpoint_directories_never_read_as_a_rejection(tmp_path, n):
    # review r4 #2 (many_ueps.py): 2,100 directories → "rejected | None" through the real command and the cut
    env = _stub(tmp_path, f"PENDING 57 Priority /h/.globus_compute/uep.{EID}.{UEP_A}/submit_scripts/a.stdout\n")
    _uep_log(tmp_path, UEP_A, 12)
    _many_ueps(tmp_path, n)
    p = await scheduler_ops._probe_pilots_over_login(_uep_app(), EID, 120.0, _cut_runner(env), block=12)
    assert p is not None and p.category == "queued" and p.subject == "57" and "REJECTED" not in p.suffix
    cmd = scheduler_ops._pilot_status_cmd("slurm", EID, request=12)
    out = subprocess.run(["bash", "-c", cmd], env=env, capture_output=True, text=True, check=False).stdout
    assert out.count("HPCB_BREQ ") == 1 and len(out.splitlines()) < 10   # one line per listed pilot's user endpoint


async def test_a_cut_drops_the_lookup_before_any_pilot_row(tmp_path):
    # round 5, N2: the lookup is printed FIRST, so a cut that keeps exactly as many lines as the rows hold keeps every
    # row, and the probe falls back to "cannot tell" (every live pilot may be this block's), never to "no pilot". With
    # the lookup after the rows (117afe2), that cut took HPCB_NOW and pilot 57's row, and read "likely REJECTED". (The
    # order itself is also pinned by test_the_lookup_reads_the_rotated_copy_and_only_each_logs_first_lines and
    # test_the_pbs_lookup_reads_the_pilots_own_user_endpoint.)
    died = f"FAILED|60|1:0|sbatch /h/.globus_compute/uep.{EID}.{UEP_B}/b.sh|1792538300\n"
    env = _stub(tmp_path, f"PENDING 57 Priority /h/.globus_compute/uep.{EID}.{UEP_A}/submit_scripts/a.stdout\n", died)
    _uep_log(tmp_path, UEP_A, 12)
    _uep_log(tmp_path, UEP_B, 11)

    async def run_login(cmd):
        r = subprocess.run(["bash", "-c", cmd], env=env, capture_output=True, text=True, check=False)
        lines = r.stdout.splitlines()
        rows = [ln for ln in lines if not ln.startswith("HPCB_BREQ ")]
        assert len(lines) > len(rows) >= 4                 # the cut below takes something, and there are rows to keep
        return ShellOutcome(phase="complete", exit_code=r.returncode, stdout="\n".join(lines[-len(rows):]) + "\n",
                            block_state="warm")
    p = await scheduler_ops._probe_pilots_over_login(_uep_app(), EID, 120.0, run_login, block=12)
    assert p is not None and p.category == "queued" and p.subject == "57" and "REJECTED" not in p.suffix


def test_the_lookup_runs_in_a_subshell_and_never_without_a_block_request():
    cmd = scheduler_ops._pilot_status_cmd("pbs", EID, request=3)
    assert cmd.startswith("( o=$(") and "HPCB_GC=" in cmd and "|| true" in cmd    # no variable left in the session
    assert "HPCB_BREQ" not in scheduler_ops._pilot_status_cmd("pbs", EID)
    assert "HPCB_REQ <" not in (scheduler_ops._pilot_status_cmd.__doc__ or "")
    assert not hasattr(scheduler_ops, "_NEVER_PENDING")                             # r4 #9: dead code gone


def test_the_pbs_lookup_reads_the_pilots_own_user_endpoint(tmp_path):
    rec = (f"Job Id: 7.pbsserver\n    job_state = Q\n"
           f"    Output_Path = login:/home/u/.globus_compute/uep.{EID}.{UEP_A}/submit_scripts/y.stdout\n")
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "qstat").write_text(f"#!/bin/sh\nprintf '%s' '{rec}'\n")
    (bin_ / "qstat").chmod(0o755)
    _uep_log(tmp_path, UEP_A, 4)
    _many_ueps(tmp_path, 50)
    out = subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("pbs", EID, request=4)],
                         env={"HOME": str(tmp_path), "USER": "u", "PATH": f"{bin_}:/usr/bin:/bin"},
                         capture_output=True, text=True, check=False).stdout
    assert out.startswith(f"HPCB_BREQ {UEP_A} 4\nHPCB_NOW ") and out.count("HPCB_BREQ") == 1
    assert "Q 7.pbsserver -" in out and f"HPCB_DIR 7.pbsserver {UEP_A}" in out


# --- 3. a select line's other resources are kept, and said so -----------------------------------------------------------


@pytest.mark.parametrize(("line", "options", "rest"), [
    ("#PBS -l select=1:ngpus=4,filesystems=home:eagle", "ngpus=4", "#PBS -l filesystems=home:eagle"),
    ("-l select=1:system=polaris -l filesystems=home:eagle", "system=polaris", "#PBS -l filesystems=home:eagle"),
    ("#PBS -l select=1:ngpus=4 -l filesystems=home", "ngpus=4", "#PBS -l filesystems=home"),
    ("#PBS -l filesystems=home,select=1:ngpus=4", "ngpus=4", "#PBS -l filesystems=home"),
    ("#PBS -lselect=1:ngpus=4,filesystems=home -m abe", "ngpus=4", "#PBS -l filesystems=home -m abe"),
])
def test_the_refusal_keeps_the_lines_other_resources(line, options, rest):
    # review r4 #3 (sel_probe.py): "write it as select_options='ngpus=4'" lost filesystems= (a Polaris hook holds it)
    msg = _refusal(line)
    assert f"write it as select_options={options!r}" in msg
    assert f"keep the line's other resources as {rest!r} in scheduler_options" in msg
    # …and what it says is accepted, and renders the same chunk with the rest beside it
    d = FacilityDetails(**_PBS, scheduler_options=rest, select_options=options)
    assert binding._entry_from_details("mine", d).compute.scheduler_options == rest
    assert _parsl_select_line(_rendered_provider(d)) == f"#PBS -l select=1:ncpus=1:{options}"


@pytest.mark.parametrize("line", ["#PBS -l select=1:ncpus=64:ngpus=4,walltime=01:00:00",
                                  "#PBS -l select=1:ngpus=4 -l select=1:ngpus=2"])
def test_a_rest_that_sets_a_term_gets_no_conversion(line):
    from hpc_bridge.catalog.entry import select_advice

    assert select_advice(line) is None
    assert "write the resources after the chunk as select_options='ngpus=4' (for instance" in _refusal(line)
    d, dropped = binding._accepted_details(FacilityDetails(**_PBS, scheduler_options=line))
    assert d.scheduler_options is None and d.select_options is None and len(dropped) == 1


@pytest.mark.parametrize(("line", "rest"), [
    ("#PBS -l select=1:ngpus=4,filesystems=home:eagle", "#PBS -l filesystems=home:eagle"),
    ("-l select=1:system=polaris -l filesystems=home:eagle", "#PBS -l filesystems=home:eagle"),
])
def test_the_cached_conversion_keeps_the_lines_other_resources(line, rest):
    d, dropped = binding._accepted_details(FacilityDetails(**_PBS, scheduler_options=line))
    assert d.scheduler_options == rest and d.select_options in ("ngpus=4", "system=polaris")
    assert any(f"the line's other resources are kept as {rest!r}" in why for why in dropped)
    assert binding._entry_from_details("mine", d)          # the proposal is a configuration the connect accepts


async def test_a_cached_select_line_with_filesystems_is_proposed_with_both(monkeypatch):
    class _Empty:
        async def get(self, machine_id):
            return None

    from tests.test_scheduler_directives import _connect_app

    monkeypatch.setattr(binding, "make_catalog", lambda: _Empty())
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    binding._facility_store().put("login.example.edu",
                                  {**_PBS, "scheduler_options": "#PBS -l select=1:ngpus=4,filesystems=home:eagle"})
    app = _connect_app(monkeypatch)
    res = await connect._connect_facility(app, "mine", ssh_host="login.example.edu", run_login=_no_login)
    assert res.phase == "proposed_facility_details" and res.proposed_details is not None
    d = res.proposed_details
    assert d.scheduler_options == "#PBS -l filesystems=home:eagle" and d.select_options == "ngpus=4"
    assert "its resources are offered as select_options='ngpus=4'" in res.notice
    assert "the line's other resources are kept as '#PBS -l filesystems=home:eagle'" in res.notice


# --- 4. on Slurm a select gets Slurm's advice ---------------------------------------------------------------------------


@pytest.mark.parametrize(("opts", "gpus"), [("#PBS -l select=1:ngpus=4", "4"),
                                            ("#PBS -l select=1:system=x", "<n>")])
def test_a_slurm_facility_is_pointed_at_the_gpus_argument_not_select_options(opts, gpus):
    # review r4 #4 (slurm_select.py): "write it as select_options='ngpus=4'", which is then refused as PBS only; since
    # resource requests (review round 1) a Slurm block's GPUs are ensure_endpoint_up(gpus=…), never a directive
    msg = _refusal(opts, _SLURM)
    assert f"ensure_endpoint_up(gpus={gpus})" in msg and "select_options is PBS only" in msg
    assert "write it as select_options" not in msg and "parsl writes the block's own select" not in msg
    with pytest.raises(binding.TermsDirectiveError, match="the block's size"):
        binding._entry_from_details("mine", FacilityDetails(**_SLURM, scheduler_options="#SBATCH --gres=gpu:4"))


# --- 5. the error file's refusal and the cached record's reason ---------------------------------------------------------


def test_a_cached_error_file_line_names_its_own_reason():
    d, dropped = binding._accepted_details(FacilityDetails(**_SLURM, scheduler_options="#SBATCH -e /scratch/x.err"))
    assert d.scheduler_options is None and len(dropped) == 1
    assert "it sets the pilot's error file: hpc-bridge reads why a pilot died from its own error file" in dropped[0]
    d, dropped = binding._accepted_details(FacilityDetails(**_SLURM, scheduler_options="#SBATCH -o /scratch/x.out"))
    assert "it sets the pilot's output file: hpc-bridge finds and cancels its pilots by their own output file" in dropped[0]


# --- 6. the Slurm reason read whole, by squeue's field suffix -----------------------------------------------------------


def _suffixed(rows: list[tuple[str, str, str, str]]) -> str:
    """squeue -h -O "State:20|,JobID:24|,Reason:60|,StdOut:1024": each field padded (and cut) to its size, then `|`."""
    return "".join(f"{st[:20]:<20}|{j[:24]:<24}|{r[:60]:<60}|{path[:1024]:<1024}\n" for st, j, r, path in rows)


def _rows_of(tmp_path: Path, squeue: str) -> str:
    env = _stub(tmp_path, squeue)
    return subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("slurm", EID)], env=env, capture_output=True,
                          text=True, check=False).stdout


def test_squeue_is_asked_for_a_delimiter_after_each_field():
    assert '-O "State:20|,JobID:24|,Reason:60|,StdOut:1024"' in scheduler_ops._pilot_status_cmd("slurm", EID)


@pytest.mark.parametrize(("reason", "path", "want"), [
    # review r4 #6: a StdOut path with a space left its head in the reason ('None   /home/John')
    ("None", f"/home/John Doe/.globus_compute/uep.{EID}.{UEP_A}/submit_scripts/a.stdout", "None"),
    # a reason of 60 characters ran into the path and lost its last word
    ("ReqNodeNotAvail, UnavailableNodes:c[001-099],d[001-099],e[01", f"/h/uep.{EID}.{UEP_A}/a.stdout",
     "ReqNodeNotAvail, UnavailableNodes:c[001-099],d[001-099],e[01"),
    ("X" * 60, f"/h/uep.{EID}.{UEP_A}/a.stdout", "X" * 60),
    ("launch failed requeued held", f"/h/uep.{EID}.{UEP_A}/a.stdout", "launch failed requeued held"),
])
def test_the_whole_reason_is_read_by_the_delimiter(tmp_path, reason, path, want):
    out = _rows_of(tmp_path, _suffixed([("PENDING", "1001", reason, path)]))
    assert scheduler_ops._pilot_rows(out) == [["PENDING", "1001", "-", want]]
    assert f"HPCB_DIR 1001 {UEP_A}\n" in out                       # the job id, not "|1001"


def test_a_squeue_without_the_suffix_is_still_read_by_words(tmp_path):
    from tests.test_scheduler_directives_r3 import _fixed_width

    out = _rows_of(tmp_path, _fixed_width([("PENDING", "1001", "ReqNodeNotAvail, UnavailableNodes:c[1-3]")]))
    assert scheduler_ops._pilot_rows(out) == [["PENDING", "1001", "-", "ReqNodeNotAvail, UnavailableNodes:c[1-3]"]]


# --- 7-9. nits ----------------------------------------------------------------------------------------------------------


async def test_metadata_without_a_template_claims_nothing_about_select_options():
    # review r4 #7: "configured before hpc-bridge wrote select_options" with no template read at all
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility
    from tests.test_env_setup import _Client

    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup="x", interface="ib0", partition="p",
                          account="", worker_init="x", scheduler="pbs", select_options="ngpus=4")
    for md in ({}, {"user_config_template": None}, {"user_config_template": "  "}):
        client = _Client(md=md)
        fac = SlurmFacility(prof, cli=None, client_factory=lambda c=client: c)
        await fac.adopt_endpoint("eid-1")
        assert fac.select_note == "", md


@pytest.mark.parametrize("which", ["slurm", "mep"])
def test_the_ingest_refuses_select_options_where_nothing_renders_them(which):
    # review r4 #8: a Slurm or facility-endpoint entry's select_options would be published only to be ignored
    from hpc_bridge.catalog.entry import defaults_extra_raw_problems

    raw = (fake_entry(id="x", facility_key="lab") if which == "slurm" else fake_mep_entry()).model_dump(mode="json")
    if which == "mep":
        raw["compute"]["scheduler"] = "pbs"
    raw["defaults"]["select_options"] = "ngpus=4"
    assert any("defaults.select_options is for an SSH entry on PBS" in p for p in defaults_extra_raw_problems(raw))
    pbs = fake_entry(id="y", facility_key="lab").model_dump(mode="json")
    pbs["compute"]["scheduler"] = "pbs"
    pbs["defaults"]["select_options"] = "ngpus=4"
    assert defaults_extra_raw_problems(pbs) == []


def test_advice_for_a_chunk_without_ncpus_names_cpus_per_node():
    # review r4 #9: without it parsl's chunk is select=1:ncpus=1:ngpus=4 (one core, on a shared-node site)
    msg = _refusal("-l select=1:ngpus=4")
    assert "write it as select_options='ngpus=4', with cpus_per_node=<the cores per node> if it is not set" in msg
    assert "ncpus=1" in msg
    assert "cpus_per_node=<the cores" not in _refusal("#PBS -l select=1:ncpus=64:ngpus=4")
