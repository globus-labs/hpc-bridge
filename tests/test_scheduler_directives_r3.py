"""Round 3 of the review of the scheduler-directive fix (on d22f2a2): what the `-l select=` ban took from a PBS site (a
chunk's resources now go in `select_options`, appended to parsl's own select so the node count stays hpc-bridge's), a
Slurm reason read only to its first word and state words read by their first letter, pilot files and arrays that slip
past the terms, a pilot of another block request taken for this one past the grace, an unconfirmed stop that forgot a
held pilot, truncated user endpoint ids, the lookup's cost, and some reasons worded wrong.
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_bridge import binding, connect, scheduler_ops
from hpc_bridge.catalog.entry import CatalogEntry, defaults_extra_raw_problems
from hpc_bridge.context import AppCtx, SeenPilots, SshLeft
from hpc_bridge.facility.remote import MachineProfile, SlurmFacility, profile_from_catalog_entry
from hpc_bridge.models import FacilityDetails
from hpc_bridge.profile import Profile
from hpc_bridge.shapes import shape_config
from tests.fakes import fake_entry
from tests.test_scheduler_directives import _PBS, _SLURM, EID, UEP_A, UEP_B, _no_login, _probe_of, _refusal, _uep_app
from tests.test_server import _none, _queued_app, _scripted_login

# parsl 2026.10.19's PBSProProvider, as it writes a block's select (parsl/providers/pbspro/{pbspro,template}.py):
#   template:  #PBS -l select=${nodes_per_block}:ncpus=${ncpus}${select_options}
#   submit():  job_config["ncpus"] = self.cpus_per_node
#              if self.select_options and not self.select_options.startswith(":"): self.select_options = ":" + …


def _parsl_select_line(provider: dict) -> str:
    sel = provider.get("select_options") or ""
    if sel and not sel.startswith(":"):
        sel = ":" + sel
    return f"#PBS -l select={provider.get('nodes_per_block', 1)}:ncpus={provider.get('cpus_per_node', 1)}{sel}"


def _rendered_provider(details: FacilityDetails) -> dict:
    """details= → the session entry → the MachineProfile → hpc-bridge's PBS template, rendered as the endpoint manager
    renders it (sanitized config, sandboxed Jinja, safe_load): the provider parsl builds."""
    from tests.test_env_setup import _render

    entry = binding._entry_from_details("mine", details)
    fac = SlurmFacility(profile_from_catalog_entry(entry, user="u", account=""), cli=None)
    tmpl, defaults = fac.config_template(Profile())
    return _render(tmpl, {**defaults, **shape_config("compute")})


# --- 1. a chunk's resources go in select_options -----------------------------------------------------------------------


@pytest.mark.parametrize(("sel", "cpus", "line"), [
    ("ngpus=4", None, "#PBS -l select=1:ncpus=1:ngpus=4"),
    ("ngpus=4", 64, "#PBS -l select=1:ncpus=64:ngpus=4"),
    ("system=polaris", 32, "#PBS -l select=1:ncpus=32:system=polaris"),
    (":system=polaris:ngpus=4", 32, "#PBS -l select=1:ncpus=32:system=polaris:ngpus=4"),
])
def test_select_options_reach_parsls_own_select_line(sel, cpus, line):
    provider = _rendered_provider(FacilityDetails(**_PBS, select_options=sel, cpus_per_node=cpus))
    assert provider["type"] == "PBSProProvider" and provider["nodes_per_block"] == 1
    assert _parsl_select_line(provider) == line                # the node count is the block's, hpc-bridge's


def test_no_select_options_render_no_key():
    provider = _rendered_provider(FacilityDetails(**_PBS))
    assert "select_options" not in provider and _parsl_select_line(provider) == "#PBS -l select=1:ncpus=1"


@pytest.mark.parametrize(("opts", "advice"), [
    ("-l select=1:ngpus=4", "write it as select_options='ngpus=4'"),
    ("#PBS -l select=1:ncpus=64:ngpus=4", "write it as select_options='ngpus=4' and cpus_per_node=64"),
    ("#PBS -l select=1:system=polaris", "write it as select_options='system=polaris'"),
    ("-l select=2:ngpus=4", "write the resources after the chunk as select_options='ngpus=4' (for instance"),
    ("-l select=1:ngpus=2+1:ngpus=4", "write the resources after the chunk as select_options='ngpus=4' (for instance"),
])
def test_a_select_line_is_refused_and_pointed_at_select_options(opts, advice):
    msg = _refusal(opts)
    assert "sets the node count (a select)" in msg and advice in msg
    assert "hpc-bridge writes select=<the block's node count>:ncpus=<cpus_per_node>:<select_options>" in msg


@pytest.mark.parametrize("sel", ["ngpus=4", ":ngpus=4", "system=polaris:ngpus=4", "mem=64gb:host=x0001"])
def test_a_chunk_resource_list_passes(sel):
    from hpc_bridge.catalog.entry import select_options_problem
    assert select_options_problem(sel) is None
    assert binding._entry_from_details("mine", FacilityDetails(**_PBS, select_options=sel)).defaults.select_options == sel


@pytest.mark.parametrize(("sel", "why"), [
    ("select=1:ngpus=4", "a select of its own"), ("2:ngpus=4", "chunk count"), ("ngpus=4+1:ngpus=2", "second chunk"),
    ("ncpus=64", "cpus_per_node"), ("walltime=04:00:00", "the walltime"), ("nodes=2", "the node count"),
    ("place=scatter", "a placement"), ("account=x", "the account"), ("ngpus=4 -A x", "not a chunk resource"),
    ("ngpus=4\n#PBS -A x", "not a chunk resource"), ("", "empty"),
])
def test_select_options_carry_chunk_resources_only(sel, why):
    from hpc_bridge.catalog.entry import select_options_problem
    assert why in (select_options_problem(sel) or "")
    with pytest.raises(ValueError, match="select_options"):
        binding._entry_from_details("mine", FacilityDetails(**_PBS, select_options=sel))


def test_select_options_are_pbs_only():
    with pytest.raises(ValueError, match="select_options is PBS only"):
        binding._entry_from_details("mine", FacilityDetails(**_SLURM, select_options="ngpus=4"))


def test_a_registry_pbs_entry_may_carry_select_options_and_the_ingest_checks_them():
    raw = fake_entry(id="pbsx", facility_key="lab").model_dump(mode="json")
    raw["compute"]["scheduler"] = "pbs"
    raw["defaults"]["select_options"] = "ngpus=4"
    e = CatalogEntry.model_validate(raw)
    assert e.defaults.select_options == "ngpus=4" and e.profile_kwargs()["select_options"] == "ngpus=4"
    assert defaults_extra_raw_problems(raw) == []
    raw["defaults"]["select_options"] = "2:ngpus=4"
    assert CatalogEntry.model_validate(raw).defaults.select_options is None      # read leniently: dropped
    assert any("defaults.select_options starts a chunk count" in p for p in defaults_extra_raw_problems(raw))


async def test_a_cached_select_line_is_offered_back_as_select_options(monkeypatch):
    class _Empty:
        async def get(self, machine_id):
            return None

    from tests.test_scheduler_directives import _connect_app

    monkeypatch.setattr(binding, "make_catalog", lambda: _Empty())
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    binding._facility_store().put("login.example.edu",
                                  {**_PBS, "scheduler_options": "#PBS -l filesystems=home\n-l select=1:ncpus=64:ngpus=4"})
    app = _connect_app(monkeypatch)
    res = await connect._connect_facility(app, "mine", ssh_host="login.example.edu", run_login=_no_login)
    assert res.phase == "proposed_facility_details" and res.proposed_details is not None
    d = res.proposed_details
    assert d.scheduler_options == "#PBS -l filesystems=home" and d.select_options == "ngpus=4" and d.cpus_per_node == 64
    assert "its resources are offered as select_options='ngpus=4' and cpus_per_node=64" in res.notice


async def test_a_reused_endpoint_whose_template_predates_select_options_says_so():
    from tests.test_env_setup import _Client

    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup="x", interface="ib0", partition="p",
                          account="", worker_init="x", scheduler="pbs", select_options="ngpus=4")
    tmpl = SlurmFacility(prof, cli=None).config_template(Profile())[0]
    old = "\n".join(ln for ln in tmpl.splitlines() if "select_options" not in ln)
    for template, said in ((old, True), (tmpl, False)):
        client = _Client(template)
        fac = SlurmFacility(prof, cli=None, client_factory=lambda c=client: c)
        await fac.adopt_endpoint("eid-1")
        assert ("configured before hpc-bridge wrote select_options" in fac.select_note) is said, fac.select_note


# --- 2. a Slurm reason read whole, a state word read whole -------------------------------------------------------------


def _fixed_width(rows: list[tuple[str, str, str]], uep: str = UEP_A) -> str:
    """squeue -h -O "State:20,JobID:24,Reason:60,StdOut:1024", as squeue pads each field to its width."""
    out = []
    for st, j, r in rows:
        path = f"/home/u/.globus_compute/uep.{EID}.{uep}/submit_scripts/parsl.slurm.{j}.submit.stdout"
        out.append(f"{st:<20}{j:<24}{r[:60]:<60}{path:<1024}")
    return "\n".join(out) + "\n"


def _slurm_rows(tmp_path: Path, squeue: str) -> list[list[str]]:
    bin_ = tmp_path / "bin"
    bin_.mkdir(exist_ok=True)
    (bin_ / "squeue").write_text("#!/bin/sh\ncat <<'EOF'\n" + squeue + "EOF\n")
    (bin_ / "sacct").write_text("#!/bin/sh\nexit 0\n")
    for f in bin_.iterdir():
        f.chmod(0o755)
    out = subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("slurm", EID)],
                         env={"HOME": str(tmp_path), "USER": "u", "PATH": f"{bin_}:/usr/bin:/bin"},
                         capture_output=True, text=True, check=False).stdout
    return scheduler_ops._pilot_rows(out)


@pytest.mark.parametrize(("state", "reason", "want"), [
    ("PENDING", "launch failed requeued held", "H"), ("PENDING", "job requeued in held state", "H"),
    ("PENDING", "JobHoldMaxRequeue", "H"), ("REQUEUE_HOLD", "JobHeldUser", "H"), ("RESV_DEL_HOLD", "ReservationDeleted", "H"),
    ("SPECIAL_EXIT", "None", "H"), ("PENDING", "ReqNodeNotAvail, UnavailableNodes:c[1-3]", "P"),
    ("PENDING", "AssocGrpBillingMinutes", "P"), ("CONFIGURING", "None", "C"), ("RUNNING", "None", "R"),
    ("REQUEUED", "None", "P"), ("PENDING", "QOSNotAllowed", "N"),
])
def test_a_slurm_row_is_read_whole_through_the_real_rows_command(tmp_path, state, reason, want):
    rows = _slurm_rows(tmp_path, _fixed_width([(state, "1001", reason)]))
    assert rows == [[state, "1001", "-", reason]]                             # the whole reason, not its first word
    assert scheduler_ops._pilot_state(rows[0]) == want


def test_a_requeued_hold_is_never_said_to_be_running():
    cat, why = scheduler_ops._summarize_pilot("REQUEUE_HOLD 1004 - JobHeldUser\n", 30, scheduler="slurm")
    assert cat == "held" and why.startswith("— pilot 1004 is HELD (reason 'JobHeldUser')") and "RUNNING" not in why
    cat, why = scheduler_ops._summarize_pilot("PENDING 1001 - launch failed requeued held\n", 30, scheduler="slurm")
    assert cat == "held" and "'launch failed requeued held'" in why and "scontrol show job 1001" in why
    cat, why = scheduler_ops._summarize_pilot("SPECIAL_EXIT 1009 - None\n", 30, scheduler="slurm")
    assert cat == "held" and "suspended" not in why


def test_an_exhausted_allocation_is_named_and_not_promised_soon():
    for reason in ("AssocGrpBillingMinutes", "QOSGrpBillingMinutes", "AssocGrpGRESMinutes"):
        cat, why = scheduler_ops._summarize_pilot(f"PENDING 7 - {reason}\n", 30, scheduler="slurm")
        assert cat == "held" and "the account's allocation" in why and "is used up" in why
        assert "which may not be soon" in why and "an account with time left" in why
        fates = " ".join(scheduler_ops._pilot_fates([["PENDING", "8", "-", reason]]))
        assert "waiting on an allocation that is used up" in fates and "may not be soon" in fates
    cat, why = scheduler_ops._summarize_pilot("PENDING 7 - QOSMaxJobsPerAccount\n", 30, scheduler="slurm")
    assert cat == "held" and "it may start later on its own" in why


def test_pbs_words_for_pbs_rows():
    cat, why = scheduler_ops._summarize_pilot("Q 7.pbs - \n", 30, scheduler="pbs")
    assert cat == "queued" and "(PBS state Q: queued)" in why and "PENDING" not in why
    cat, why = scheduler_ops._summarize_pilot("Q 7.pbs - Can Never Run: Insufficient amount of resource: ngpus\n", 30,
                                              scheduler="pbs")
    assert cat == "held" and "will not start it as submitted" in why
    assert scheduler_ops._pilot_state(["Q", "7.pbs", "-", "Can Never Run: …"]) == "N"
    assert scheduler_ops._summarize_pilot("PENDING 13 - None\n", 30) == (
        "queued", "— pilot 13 is queued (PENDING); waiting on the scheduler.")


# --- 3. the pilot's files and arrays -----------------------------------------------------------------------------------


@pytest.mark.parametrize(("base", "opts", "kind"), [
    (_SLURM, "--output=/scratch/u/%x-%j.out", "the pilot's output file"),
    (_SLURM, "-o /scratch/u/x.out", "the pilot's output file"),
    (_SLURM, "--error=/scratch/u/x.err", "the pilot's error file"),
    (_SLURM, "-e x.err", "the pilot's error file"), (_SLURM, "--out=x", "the pilot's output file"),
    (_SLURM, "--err=x", "the pilot's error file"), (_SLURM, "-vo x", "the pilot's output file"),
    (_SLURM, "--array=0-9", "a job array"), (_SLURM, "-a 0-9", "a job array"), (_PBS, "-J 0-9", "a job array"),
])
def test_the_pilots_files_and_an_array_are_refused(base, opts, kind):
    msg = _refusal(opts, base)
    assert f"sets {kind}" in msg
    if "output" in kind:   # the release and the probe find the pilot by its StdOut (squeue), the output file only
        assert "stop_endpoint could not cancel the pilot" in msg
    elif "error" in kind:  # round 4 #5: the error file is where a dead pilot's reason is read, nothing more
        assert "stop_endpoint could not cancel" not in msg and "reported without that reason" in msg
        assert "no setting that moves that directory" in msg and "GLOBUS_COMPUTE_USER_DIR" not in msg
    else:
        assert "several blocks" in msg


@pytest.mark.parametrize("opts", ["#PBS -o /scratch/u/out.log", "#PBS -e /scratch/u/err.log", "#SBATCH --open-mode=append",
                                  "#SBATCH --export=ALL"])
def test_options_beside_them_pass(opts):
    base = _PBS if opts.startswith("#PBS") else _SLURM
    assert binding._entry_from_details("mine", FacilityDetails(**base, scheduler_options=opts))


def test_a_pbs_pilot_with_its_own_output_file_is_still_found_and_cancelled(tmp_path):
    # PBS -o/-e stay allowed: the record keeps the marker in Submit_arguments (the script's path) and PBS_O_WORKDIR.
    # (Unwrapped here: the probe joins qstat's 80-column continuations with GNU sed, as on a login node; the review's
    # wrapped fixtures checked that part.)
    script = f"/home/u/.globus_compute/uep.{EID}.{UEP_A}/submit_scripts/parsl.htex.block-0.1792540000.submit"
    rec = "\n".join([
        "Job Id: 7.pbsserver", "    job_state = H", "    comment = held by the site",
        "    Output_Path = login01:/scratch/u/out.log", "    Error_Path = login01:/scratch/u/err.log",
        f"    Submit_arguments = -q debug {script}",
        f"    Variable_List = PBS_O_HOME=/home/u,PBS_O_WORKDIR=/home/u/.globus_compute/uep.{EID}.{UEP_A},X=1",
    ]) + "\n\n"
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "qstat").write_text("#!/bin/sh\ncat <<'EOF'\n" + rec + "EOF\n")
    (bin_ / "qdel").write_text("#!/bin/sh\necho \"$1\" >> \"$HOME/qdel.log\"\n")
    for f in bin_.iterdir():
        f.chmod(0o755)
    env = {"HOME": str(tmp_path), "USER": "u", "PATH": f"{bin_}:/usr/bin:/bin"}
    out = subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd("pbs", EID)], env=env, capture_output=True,
                         text=True, check=False).stdout
    assert "H 7.pbsserver - held by the site" in out and f"HPCB_DIR 7.pbsserver {UEP_A}" in out
    rel = subprocess.run(["bash", "-c", scheduler_ops._release_cmd("pbs", EID)], env=env, capture_output=True,
                         text=True, check=False).stdout
    assert rel.strip() == "released 7.pbsserver" and (tmp_path / "qdel.log").read_text() == "7.pbsserver\n"


# --- 4. a pilot of another block request is never this one's, in every branch -------------------------------------------


async def test_past_the_grace_another_requests_held_pilot_is_not_this_blocks():
    # no user endpoint of this request found (not started, or its log unread), but the held pilot's own user endpoint
    # names ANOTHER block request: never "pilot 2 is HELD" for this block (round 1's misattribution)
    out = (f"HPCB_NOW 1\nH 2.pbsserver - held by the site\nHPCB_DIR 2.pbsserver {UEP_B}\nHPCB_BREQ {UEP_B} 123\n")
    p = await _probe_of(out, app=_uep_app(), elapsed=60.0, block=12)
    assert p is not None and p.subject is None and "is HELD" not in p.suffix
    assert p.category == "rejected" and "NO pilot job of this block request" in p.suffix and "2.pbsserver, held" in p.suffix
    # one whose user endpoint names no block request at all stays a candidate (it may be this one's)
    out = f"HPCB_NOW 1\nH 2.pbsserver - held by the site\nHPCB_DIR 2.pbsserver {UEP_B}\n"
    p = await _probe_of(out, app=_uep_app(), elapsed=60.0, block=12)
    assert p is not None and p.subject == "2.pbsserver" and p.category == "held"


# --- 6. a user endpoint id that is not whole is unknown, never another's ------------------------------------------------


async def test_a_cut_user_endpoint_id_never_makes_this_requests_pilot_another_ones():
    cut = UEP_A[:20]                                      # a path cut at 1024, a wrapped record not joined
    out = (f"HPCB_NOW 1\nPENDING 57 - Priority\nHPCB_DIR 57 {cut}\nHPCB_BREQ {UEP_A} 12\n")
    p = await _probe_of(out, app=_uep_app(), elapsed=60.0, block=12)
    assert p is not None and p.subject == "57" and p.category == "queued" and "REJECTED" not in p.suffix


# --- 5. only a confirmed release forgets what a re-bind left -------------------------------------------------------------


def _left() -> SshLeft:
    return SshLeft(up=False, pilots=SeenPilots(jobs=(("0.pbsserver", "H", "x", "H"),), at=time.monotonic(), subject=None))


async def test_an_unconfirmed_stop_keeps_naming_the_held_pilot_and_a_confirmed_one_forgets_it(monkeypatch):
    from hpc_bridge.server import _ensure_endpoint_up, _stop_endpoint

    monkeypatch.setenv("HPC_BRIDGE_RELEASE_ATTEMPTS", "2")
    app, _runner = _queued_app(monkeypatch, pbs=True)
    _scripted_login(monkeypatch, probes=[""])
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    eid = app.state.endpoint_id
    app.ssh_left[eid] = _left()
    _scripted_login(monkeypatch, releases=[None] * 10)                    # a cold channel: nothing confirmed
    res = await _stop_endpoint(app)
    assert res.status != "down" and eid in app.ssh_left
    _scripted_login(monkeypatch, releases=["released 0.pbsserver\n"] * 10)
    await _stop_endpoint(app)
    assert eid not in app.ssh_left


async def test_a_teardown_forgets_what_a_re_bind_left(monkeypatch):
    from hpc_bridge import server
    from hpc_bridge.server import _ensure_endpoint_up, _teardown_endpoint

    app, _runner = _queued_app(monkeypatch)
    _scripted_login(monkeypatch, probes=[""])
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    eid = app.state.endpoint_id
    app.ssh_left[eid] = _left()
    monkeypatch.setattr(server, "_teardown_preauth_gate", lambda *a, **k: _none())
    _scripted_login(monkeypatch)
    assert (await _teardown_endpoint(app)).status == "down" and eid not in app.ssh_left


# --- 9. what an endpoint holds is the endpoint's, not this session's ------------------------------------------------------


def test_the_reconnect_note_says_this_endpoints():
    from hpc_bridge.notices import _ssh_left_note

    app = AppCtx(facility=SimpleNamespace(supported_shapes=("login", "compute")), profile=Profile())
    note = _ssh_left_note(app, "eid", same=True, queued=0, left=_left())
    assert "this endpoint still has work in its scheduler" in note and "this session's earlier work" not in note


def test_the_template_renders_select_options_only_for_pbs():
    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup="x", interface="ib0", partition="p",
                          account="", worker_init="x", scheduler="slurm", select_options="ngpus=4")
    tmpl, defaults = SlurmFacility(prof, cli=None).config_template(Profile())
    assert "select_options" not in defaults and "select_options" not in tmpl     # the Slurm template has no such key
