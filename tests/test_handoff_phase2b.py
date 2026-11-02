"""Batch phase 2b (vault `Planned/Batch and swarm execution.md` §8, §7, §10, §12 D6): `export_handoff` v1, the loam
handoff. Written failing first: on main (a00952b) the surface does not exist, so every test here is red for that one
reason (`server._export_handoff` / `hpc_bridge.handoff` missing); the build makes them green.

The surface these tests are written against (the stage-A checkpoint fixes it):

- `export_handoff(user_endpoint_config, expires_hours=24, confirm_spend=False)` (MCP tool 17) →
  HandoffResult {phase, reason, terms: HandoffTerms, user_endpoint_config, record: HandoffRecord, path, task_id,
  notice}. phase: needs_confirmation (nothing sent: the terms) | validating (the one billed canary is out; call again
  with the same config to collect it — never a second canary) | exported (the record, written to `path`) |
  needs_account | refused (`reason`: mep_min_blocks — D6 —, invalid, not_rendered, schema, unbounded, expires,
  canary_failed, no_facility, template_unknown).
- `hpc_bridge.handoff`: SCHEMA = "hpc-bridge.handoff", VERSION = 1, `load_record(text)` → HandoffRecord (unknown fields
  ignored; another schema or version raises HandoffFormatError), `CANARY_CMD`.

The world: a facility endpoint whose template and schema are the fake `pbs_mep` profile's own files (Polaris-shaped,
strict schema), or one in the shape of ALCF's published Polaris template (open schema), or hpc-bridge's own SSH
template as `SlurmFacility.config_template` writes it. The billed canary lands on a "block" that is this machine's
bash, run as ShellFunction runs it, with the environment a scheduler gives a worker (its job id, a GPU pin, the user
endpoint's directory as the working directory): what the record says is what the canary command really reports.
"""
from __future__ import annotations

import asyncio
import json
import os
import platform
import shlex
import shutil
import socket
import stat
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from globus_compute_sdk.errors import TaskExecutionFailed

from hpc_bridge import binding, server
from hpc_bridge.catalog.entry import CatalogEntry, facility_fingerprint
from hpc_bridge.facility import remote
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.models import PartitionShape
from hpc_bridge.profile import Profile
from hpc_bridge.server import AppCtx
from tests.fakes import MEP_UUID, FakeCatalog
from tests.test_server import _DoneFuture, _FakeRunner, _PendingFuture

_BASH = shutil.which("bash")
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]

REPO = Path(__file__).resolve().parents[1]
PBS_MEP = REPO / "agentic" / "fakecluster" / "profiles" / "pbs_mep" / "mep"
FAKE_TEMPLATE = (PBS_MEP / "pbs_user_config_template.yaml.j2").read_text()
FAKE_SCHEMA = json.loads((PBS_MEP / "pbs_user_config_schema.json").read_text())
UEP_ID = "4b1c0a52-8d3e-4f6a-9b7c-2e5d8f0a1c33"
SSH_EID = "7e0f6c1a-2b3d-4e5f-8a9b-0c1d2e3f4a5b"

# loam's Polaris-shaped user endpoint config, in the facility's own key names (as loam's HPC_ENDPOINT_CONFIG_JSON is)
LOAM_UEC = {"queue": "debug", "account": "hpcb", "walltime": "00:15:00", "nodes_per_block": 1, "init_blocks": 0,
            "min_blocks": 0, "max_blocks": 2, "max_workers_per_node": 2, "available_accelerators": 2,
            "scheduler_options": "#PBS -l filesystems=home"}

# A template in the shape of ALCF's published Polaris one (the registry's `polaris` entry; additionalProperties: true)
POLARIS_TEMPLATE = """engine:
  type: GlobusComputeEngine
  available_accelerators: {{ available_accelerators|default(4) }}
  max_workers_per_node: {{ max_workers_per_node|default(4) }}
  job_status_kwargs:
    max_idletime: {{ max_idletime|default(240) }}
  provider:
    type: PBSProProvider
    account: {{ account }}
    queue: {{ queue }}
    walltime: {{ walltime|default("01:00:00") }}
    nodes_per_block: {{ nodes_per_block|default(1) }}
    scheduler_options: {{ scheduler_options|default("#PBS -l filesystems=home:eagle") }}
    worker_init: {{ worker_init|default("") }}
    init_blocks: {{ init_blocks|default(0) }}
    min_blocks: {{ min_blocks|default(0) }}
    max_blocks: {{ max_blocks|default(1) }}
"""
POLARIS_SCHEMA = {"type": "object", "additionalProperties": True, "required": ["account", "queue"],
                  "properties": {"account": {"type": "string"}, "queue": {"type": "string"}}}


# ---- the world --------------------------------------------------------------------------------------------------------

class _Meta:
    """The web client the facility binds with: endpoint status and its published metadata."""

    def __init__(self, template: str, schema: dict | None = None, version: str = "4.18.0", python: str = "3.13.11"):
        self.template, self.schema, self.version, self.python = template, schema, version, python
        self.metadata_reads = 0

    def get_endpoint_status(self, eid):
        return {"status": "online"}

    def get_endpoint_metadata(self, eid):
        self.metadata_reads += 1
        return {"user_config_template": self.template, "user_config_schema": self.schema,
                "endpoint_version": self.version, "python_version": self.python, "display_name": "fake facility"}

    def get_task(self, tid):
        """The web service's view of a task by id (SDK 4.18's `Client.get_task`), for every canary any runner sent."""
        fut = TASKS.get(tid)
        if fut is None or not fut.done() or fut.cancelled():
            return {"pending": True, "status": "waiting-for-nodes"}
        exc = fut.exception()
        if isinstance(exc, TaskExecutionFailed):
            raise exc
        if exc is not None:   # failed on the client's side alone: the service still has it
            return {"pending": True, "status": "running"}
        return {"pending": False, "status": "success", "task_id": tid, "result": fut.result()}


# Every canary sent in this test process, by task id: what the service knows (the `_Meta.get_task` stand-in).
TASKS: dict[str, object] = {}


class _Block:
    """What the scheduler gives the canary's worker: its job id, a GPU pin, the worker count, and the user endpoint's
    own directory (the worker runs in a sandbox under it)."""

    def __init__(self, tmp: Path, manager: str, *, scheduler: str = "pbs", job: str = "12.fakepbs",
                 pin: str | None = "0", workers: int = 2, uep: str | None = UEP_ID, sched=None):
        self.scheduler, self.job, self.pin, self.workers, self.sched = scheduler, job, pin, workers, sched
        name = f"uep.{manager}.{uep}" if uep else "endpoint"
        self.cwd = tmp / "home" / ".globus_compute" / name / "tasks_working_dir" / "task-1"
        self.cwd.mkdir(parents=True, exist_ok=True)
        self.home = tmp / "home"
        # the login node's scheduler commands, for a stop's release: an empty queue (nothing of ours listed)
        self.bin = tmp / "bin"
        self.bin.mkdir(exist_ok=True)
        for cmd in ("squeue", "scancel", "qstat", "qdel", "qselect"):
            (self.bin / cmd).write_text("#!/bin/sh\nexit 0\n")
            (self.bin / cmd).chmod(0o755)

    def env(self) -> dict[str, str]:
        env = {"PATH": f"{self.bin}:{Path(sys.executable).parent}:{os.environ.get('PATH', '/usr/bin:/bin')}",
               "HOME": str(self.home), "TMPDIR": str(self.home), "PARSL_WORKER_COUNT": str(self.workers)}
        if self.scheduler == "pbs":
            env["PBS_JOBID"] = self.job
        else:
            env["SLURM_JOB_ID"] = self.job
            env["SLURM_JOB_END_TIME"] = str(int(datetime.now(UTC).timestamp()) + 900)
        if self.pin is not None:
            env["CUDA_VISIBLE_DEVICES"] = self.pin
        if self.sched is not None:   # the login node's scheduler: the fake Slurm (tests/fake_sched.py) first on PATH
            senv = self.sched.env()
            env.update({k: v for k, v in senv.items() if k != "PATH"})
            env["PATH"] = f"{self.sched.bin}:{env['PATH']}"
        return env


class _Runner(_FakeRunner):
    """A runner as the server builds them: records the endpoint and config it was built for and what it was sent. A
    submitted command runs on the block (bash, brace-escaped then format()ted, as ShellFunction does); the handoff's
    canary (the one command carrying the canary's sentinel) may instead stay queued (`pending`) or fail as the
    endpoint's verdict (`fail`). A warmth check (`canary()`) answers, as a live worker does."""

    def __init__(self, world: _Runners, eid: str, uec: dict):
        super().__init__(eid, _Out(0, "", ""))
        self.world, self.user_endpoint_config = world, uec

    def submit(self, command):
        from hpc_bridge.runner import _CANARY_SENTINEL, _escape_for_shellfunction

        self.commands.append(command)
        w = self.world
        canary = _CANARY_SENTINEL in command
        if canary and w.fail is not None:
            fut = _DoneFuture(exc=w.fail)
            if w.fail_tid:
                fut.task_id = f"canary-{len(w.futures) + 1}-{id(w)}"
                w.futures.append((fut, command))
                TASKS[fut.task_id] = _PendingFuture()   # the service still has it queued
            return fut
        if canary and w.pending:
            fut = _PendingFuture()
            fut.task_id = f"canary-{len(w.futures) + 1}"
            w.futures.append((fut, command))
            self.futures.append(fut)
            TASKS[fut.task_id] = fut
            return fut
        p = subprocess.run([_BASH, "-c", _escape_for_shellfunction(command).format()], capture_output=True, text=True,
                           env=w.block.env(), cwd=w.block.cwd, timeout=60)
        fut = _DoneFuture(_Out(p.returncode, p.stdout, p.stderr))
        if canary:
            fut.task_id = f"canary-{len(w.futures) + 1}"
            w.futures.append((fut, command))
            self.futures.append(fut)
            TASKS[fut.task_id] = fut
        return fut


class _Out:
    def __init__(self, rc, out, err):
        self.returncode, self.stdout, self.stderr = rc, out, err


class _Runners:
    """app.runner_factory: every runner the server asks for, with the endpoint and the config it was built for."""

    def __init__(self, block: _Block):
        self.block = block
        self.built: list[_Runner] = []
        self.futures: list[tuple[object, str]] = []
        self.pending = False
        self.fail: BaseException | None = None
        self.fail_tid = False   # the failure is the client's alone: the task was registered (it has an id)

    def __call__(self, eid, user_endpoint_config=None, **_kw):
        r = _Runner(self, eid, dict(user_endpoint_config or {}))
        self.built.append(r)
        return r

    def run_pending(self) -> None:
        """The queued canary's block starts: run it now, resolving its future."""
        from hpc_bridge.runner import _escape_for_shellfunction

        for fut, command in self.futures:
            if isinstance(fut, _PendingFuture) and not fut.done():
                p = subprocess.run([_BASH, "-c", _escape_for_shellfunction(command).format()], capture_output=True,
                                   text=True, env=self.block.env(), cwd=self.block.cwd, timeout=60)
                fut.finish(_Out(p.returncode, p.stdout, p.stderr))


def _pbs_entry(**over) -> CatalogEntry:
    """The fake `pbs_mep` profile's catalog entry (hpcb-pbs-mep-catalog), Polaris's shape."""
    raw = {
        "id": "fake-pbs-mep", "facility_key": "fake-pbs", "facility": "hpc-bridge fake PBS cluster",
        "description": "the fake PBS cluster (Polaris-shaped) via its facility multi-user endpoint",
        "display_name": "fake PBS cluster MEP (Polaris-shaped)", "compute_mep_uuid": MEP_UUID, "ssh_host": None,
        "auth_method": "ssh-key", "allocation": None, "account_required": True,
        "account_hint": "your project on the fake PBS cluster",
        "compute": {"scheduler": "pbs", "interface": "eth0", "env_setup": "", "key_map": {"partition": "queue"},
                    "scratch_root": "$HOME/.hpc-bridge", "endpoint_name": "hpcb-mep-pbs"},
        "defaults": {"partition": "debug", "walltime": "00:15:00", "max_workers_per_node": 2, "nodes_per_block": 1,
                     "init_blocks": 1, "max_blocks": 1},
        "provenance": "session", "last_validated": "2026-11-02",
    }
    raw.update(over)
    return CatalogEntry.model_validate(raw)


async def _mep_app(tmp: Path, monkeypatch, *, template: str = FAKE_TEMPLATE, schema: dict | None = None,
                   entry: CatalogEntry | None = None, block: _Block | None = None) -> AppCtx:
    """A facility endpoint bound through the real connect path (zero SSH)."""
    entry = entry or _pbs_entry()
    meta = _Meta(template, FAKE_SCHEMA if schema is None else schema)
    fac = MEPFacility.from_entry(entry, client_factory=lambda: meta)
    app = AppCtx(facility=fac, profile=Profile())
    app.meta = meta
    app.runners = _Runners(block or _Block(tmp, MEP_UUID, scheduler=entry.compute.scheduler))
    app.runner_factory = app.runners
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: fac)
    res = await server._connect_facility(app, entry.id)
    assert app.state.endpoint_id == MEP_UUID, res
    return app


def _ssh_app(tmp: Path, *, scheduler: str = "slurm", partitions=(), block: _Block | None = None) -> AppCtx:
    """An endpoint hpc-bridge stood up over SSH (its own template), bound and up."""
    prof = remote.MachineProfile(name="fakehpc", endpoint_name="hpc-bridge-fakehpc", env_setup="true",
                                 interface="eth0", partition="main", account="", worker_init="true",
                                 scheduler=scheduler, partitions=tuple(partitions))
    tmpl, _defaults = remote.SlurmFacility(prof, cli=None).config_template(Profile())
    meta = _Meta(tmpl)
    fac = remote.SlurmFacility(prof, cli=None, client_factory=lambda: meta)
    app = AppCtx(facility=fac, profile=Profile())
    app.meta = meta
    app.state.endpoint_id = SSH_EID
    app.machine = "fakehpc"
    app.runners = _Runners(block or _Block(tmp, SSH_EID, scheduler=scheduler, job="4242", pin=None, workers=2))
    app.runner_factory = app.runners
    return app


async def _export(app: AppCtx, uec: dict, **kw):
    return await server._export_handoff(app, user_endpoint_config=uec, **kw)


async def _yes(app: AppCtx, uec: dict, **kw):
    """The handoff as an agent drives it: the call asks; the user's yes is the same call with confirm_spend=True."""
    ask = await _export(app, uec, **kw)
    assert ask.phase == "needs_confirmation", ask
    return ask, await _export(app, uec, confirm_spend=True, **kw)


def _sent(app: AppCtx) -> list[str]:
    """Every handoff canary sent, on any runner."""
    from hpc_bridge.runner import _CANARY_SENTINEL

    return [c for r in app.runners.built for c in r.commands if _CANARY_SENTINEL in c]


# ---- 1. the record: schema, round trip, unknown fields ignored --------------------------------------------------------

async def test_the_exported_record_is_written_0600_and_round_trips(tmp_path, monkeypatch):
    from hpc_bridge import handoff

    app = await _mep_app(tmp_path, monkeypatch)
    _ask, out = await _yes(app, LOAM_UEC)
    assert out.phase == "exported", out
    path = Path(out.path)
    state = Path(os.environ["HPC_BRIDGE_STATE_DIR"])
    assert path.parent == state / "handoffs" and path.name.startswith("hoff-") and path.suffix == ".json", path
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    raw = json.loads(path.read_text())
    assert raw["schema"] == handoff.SCHEMA == "hpc-bridge.handoff" and raw["version"] == handoff.VERSION == 1, raw
    assert raw == out.record.model_dump(mode="json")
    back = handoff.load_record(path.read_text())
    assert back == out.record
    assert handoff.load_record(back.model_dump_json()) == back
    for key in ("schema", "version", "created_at", "expires_at", "hpc_bridge_version", "facility", "endpoint",
                "user_endpoint_config", "template", "worker", "client_requirements", "terms", "validation",
                "release"):
        assert key in raw, key


async def test_unknown_fields_are_ignored_at_every_level_and_another_version_is_refused(tmp_path, monkeypatch):
    from hpc_bridge import handoff

    app = await _mep_app(tmp_path, monkeypatch)
    _ask, out = await _yes(app, LOAM_UEC)
    raw = json.loads(Path(out.path).read_text())
    raw["from_the_future"] = {"x": 1}
    raw["terms"]["a_new_term"] = "ignored"
    raw["validation"]["another_check"] = [1, 2]
    raw["release"]["more"] = True
    assert handoff.load_record(json.dumps(raw)) == out.record
    for bad in ({**raw, "version": 2}, {**raw, "schema": "something.else"}):
        with pytest.raises(handoff.HandoffFormatError):
            handoff.load_record(json.dumps(bad))


# ---- 2. terms before the canary -------------------------------------------------------------------------------------

async def test_the_first_call_states_the_terms_and_sends_nothing(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    ask = await _export(app, LOAM_UEC)
    assert ask.phase == "needs_confirmation" and ask.record is None and ask.path is None, ask
    assert app.runners.built == [] and _sent(app) == []                     # no runner, no canary, no block
    assert not (Path(os.environ["HPC_BRIDGE_STATE_DIR"]) / "handoffs").exists()
    t = ask.terms
    assert t is not None and t.enforced_by == "uec-bounds-only", t
    assert (t.account, t.queue, t.walltime, t.nodes_per_block, t.min_blocks, t.max_blocks) == (
        "hpcb", "debug", "00:15:00", 1, 0, 2), t
    # the facility's unit: no billing weights are known here, so node-hours — never a bare number
    assert t.unit == "node-hours" and t.approved_at is None, t
    # the canary: one block for its walltime; loam's pool: max_blocks × nodes, until the record expires, plus the
    # walltime of a block started just before it does
    assert t.canary_worst_case == pytest.approx(0.25), t
    assert t.worst_case >= 2 * 1 * (24 + 0.25) - 1e-9, t
    assert t.worst_case >= t.canary_worst_case
    # the honest limit is said in the terms themselves, and the user's question names the number and its unit
    assert "loam" in t.binds and "hpc-bridge" in t.binds and "not" in t.binds, t.binds
    assert f"{t.worst_case:g}" in (ask.notice or "") and "node-hours" in (ask.notice or ""), ask.notice
    assert ask.user_endpoint_config == {**LOAM_UEC}, ask.user_endpoint_config   # what the canary would send


async def test_the_yes_sends_one_canary_with_exactly_the_record_config(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    ask, out = await _yes(app, LOAM_UEC)
    assert out.phase == "exported", out
    assert len(app.runners.built) == 1 and len(_sent(app)) == 1, (app.runners.built, _sent(app))
    r = app.runners.built[0]
    assert r.endpoint_id == MEP_UUID
    assert r.user_endpoint_config == out.record.user_endpoint_config == ask.user_endpoint_config
    # the record's terms are the ones the user said yes to, now dated
    a, b = ask.terms.model_dump(), out.record.terms.model_dump()
    assert b.pop("approved_at") is not None and a.pop("approved_at") is None
    assert b.pop("expires_at") is not None
    a.pop("expires_at")
    assert a == b


async def test_a_queued_canary_is_waited_on_never_sent_again(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    app.runners.pending = True
    _ask, out = await _yes(app, LOAM_UEC)
    assert out.phase == "validating" and out.task_id == "canary-1" and out.record is None, out
    again = await _export(app, LOAM_UEC, confirm_spend=True)        # a second yes: the same canary
    assert again.phase == "validating" and again.task_id == "canary-1", again
    plain = await _export(app, LOAM_UEC)                              # no yes: it reads the same canary, asks nothing
    assert plain.phase == "validating" and plain.task_id == "canary-1", plain
    assert len(_sent(app)) == 1 and len(app.runners.built) == 1
    app.runners.run_pending()                                         # the block came up and ran it
    done = await _export(app, LOAM_UEC)
    assert done.phase == "exported" and done.record.validation.ok is True, done
    assert len(_sent(app)) == 1


async def test_a_failed_canary_writes_no_record_and_asks_again_before_another(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    app.runners.fail = TaskExecutionFailed("qsub: Unknown queue MSG=requested queue not found", "0")
    _ask, out = await _yes(app, LOAM_UEC)
    assert out.phase == "refused" and out.reason == "canary_failed" and out.record is None, out
    assert "Unknown queue" in (out.notice or "")
    store = Path(os.environ["HPC_BRIDGE_STATE_DIR"]) / "handoffs"
    assert list(store.glob("hoff-*")) == [] and list(store.glob("pending-*")) == [], list(store.iterdir())
    app.runners.fail = None
    again = await _export(app, LOAM_UEC)                              # the failure spent the yes: asked again
    assert again.phase == "needs_confirmation", again
    assert len(_sent(app)) == 1


async def test_terms_never_understate_a_queue_minimum_charge(tmp_path):
    # A queue that charges at least 2 h a block (Perlmutter `preempt`), on hpc-bridge's own SSH template (its blocks
    # idle-release after max_idletime). The worst a slot can bill over the horizon T: blocks back to back, each living
    # only the idle window and each charged the minimum — plus a block started just before the record expires.
    preempt = PartitionShape(name="preempt", kind="whole_node", cores=4, billing={"cpu": 1.0}, min_charge_h=2.0)
    app = _ssh_app(tmp_path, partitions=(preempt,))
    uec = {"partition": "preempt", "walltime": "00:30:00", "nodes_per_block": 1, "max_blocks": 2}
    ask = await _export(app, uec, expires_hours=6)
    assert ask.phase == "needs_confirmation", ask
    t = ask.terms
    idle_h = Profile().max_idletime_s / 3600
    per_block_h = 4.0                                     # 4 cores × 1 billing unit / core-hour, a whole node
    starts_per_slot = 6 / idle_h                          # the most blocks one slot can start in 6 h
    pathological = 2 * per_block_h * (starts_per_slot * 2.0 + 2.0)
    assert t.min_charge_h == 2.0 and t.unit != "node-hours", t
    assert t.worst_case >= pathological - 1e-6, (t.worst_case, pathological)
    assert t.canary_worst_case >= per_block_h * 2.0 - 1e-9, t        # one block, charged at least 2 h


async def test_a_minimum_charge_with_no_known_idle_window_is_refused_as_unbounded(tmp_path, monkeypatch):
    template = FAKE_TEMPLATE.replace("max_idletime: {{ max_idletime | default(240) }}", "strategy: simple")
    entry = _pbs_entry(defaults={"partition": "debug", "walltime": "00:15:00", "max_blocks": 1,
                                 "partitions": [{"name": "debug", "kind": "whole_node", "cores": 4,
                                                 "billing": {"node": 1.0}, "min_charge_h": 1.0}]})
    app = await _mep_app(tmp_path, monkeypatch, template=template, entry=entry)
    out = await _export(app, {k: v for k, v in LOAM_UEC.items() if k != "max_idletime"}, confirm_spend=True)
    assert out.phase == "refused" and out.reason == "unbounded", out
    assert _sent(app) == []


async def test_a_stop_while_the_canary_is_queued_asks_first(tmp_path):
    # (cancel and keep while it is queued: test_handoff_phase2b_r1.py, F5)
    app = _ssh_app(tmp_path)
    app.runners.pending = True
    _ask, out = await _yes(app, {"partition": "main", "walltime": "00:10:00", "max_blocks": 1})
    assert out.phase == "validating", out
    st = await server._stop_endpoint(app)
    assert st.status != "down" and out.task_id in (st.notice or ""), st


# ---- 3. D6: a MEP handoff with min_blocks >= 1 is refused -----------------------------------------------------------

@pytest.mark.parametrize("confirm", [False, True], ids=["ask", "yes"])
async def test_d6_min_blocks_on_a_facility_endpoint_is_refused_before_any_terms(tmp_path, monkeypatch, confirm):
    app = await _mep_app(tmp_path, monkeypatch)
    out = await _export(app, {**LOAM_UEC, "min_blocks": 1}, confirm_spend=confirm)
    assert out.phase == "refused" and out.reason == "mep_min_blocks", out
    assert out.terms is None and out.record is None and app.runners.built == [], out
    assert "min_blocks" in (out.notice or "") and "0" in (out.notice or "")
    assert not (Path(os.environ["HPC_BRIDGE_STATE_DIR"]) / "handoffs").exists()


@pytest.mark.parametrize("line", ["min_blocks: 1", "min_blocks: {{ min_blocks|default(1) }}"],
                         ids=["literal", "default"])
async def test_d6_reads_the_templates_own_min_blocks(tmp_path, monkeypatch, line):
    template = POLARIS_TEMPLATE.replace("min_blocks: {{ min_blocks|default(0) }}", line)
    app = await _mep_app(tmp_path, monkeypatch, template=template, schema=POLARIS_SCHEMA)
    uec = {k: v for k, v in LOAM_UEC.items() if k != "min_blocks"}
    out = await _export(app, uec)
    if line.startswith("min_blocks: 1"):          # fixed by the facility: nothing loam sends can make it 0
        assert out.phase == "refused" and out.reason == "mep_min_blocks", out
        assert app.runners.built == []
    else:                                         # rendered, with a default of 1: hpc-bridge writes 0 itself
        assert out.phase == "needs_confirmation", out
        assert out.user_endpoint_config["min_blocks"] == 0 and out.terms.min_blocks == 0, out


async def test_hpc_bridges_own_template_fixes_min_blocks_0_so_asking_for_1_is_refused_by_name(tmp_path):
    app = _ssh_app(tmp_path)
    out = await _export(app, {"partition": "main", "walltime": "00:10:00", "min_blocks": 1})
    assert out.phase == "refused" and out.reason == "not_rendered" and "min_blocks" in (out.notice or ""), out
    assert app.runners.built == []


# ---- 4. a wire-ready config: after key_map and schema fitting --------------------------------------------------------

def _within(schema: dict, uec: dict) -> list[str]:
    """What of `uec` the facility's JSON schema would refuse (the subset the fake's schema uses)."""
    bad, props = [], schema.get("properties") or {}
    if schema.get("additionalProperties") is False:
        bad += [f"{k}: not in the schema" for k in uec if k not in props]
    bad += [f"{k}: required" for k in schema.get("required") or [] if k not in uec]
    kinds = {"string": str, "integer": int, "number": (int, float), "boolean": bool}
    for k, v in uec.items():
        p = props.get(k) or {}
        if "type" in p and not isinstance(v, kinds[p["type"]]):
            bad.append(f"{k}: not {p['type']}")
        if ("minimum" in p and v < p["minimum"]) or ("maximum" in p and v > p["maximum"]):
            bad.append(f"{k}={v}: outside {p.get('minimum')}..{p.get('maximum')}")
    return bad


async def test_the_record_config_is_what_the_strict_fake_polaris_schema_takes(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    # hpc-bridge's own name for the queue (partition) is renamed by the facility's key_map, as at every submit
    uec = {**{k: v for k, v in LOAM_UEC.items() if k != "queue"}, "partition": "debug"}
    _ask, out = await _yes(app, uec)
    wire = out.record.user_endpoint_config
    assert wire["queue"] == "debug" and "partition" not in wire, wire
    assert not {"compute", "interface", "hpcb_block", "endpoint_setup"} & set(wire), wire
    assert _within(FAKE_SCHEMA, wire) == [], wire
    assert app.runners.built[0].user_endpoint_config == wire
    renders = out.record.template.renders
    assert set(renders) == set(wire) and all(renders.values()), renders
    assert out.record.template.sha256 == facility_fingerprint(app.meta.get_endpoint_metadata(MEP_UUID))[
        "template_sha256"]


async def test_a_key_the_strict_schema_lacks_or_out_of_its_bounds_is_refused_by_name(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    for uec, word in (({**LOAM_UEC, "filesystems": "home"}, "filesystems"),
                      ({**LOAM_UEC, "max_blocks": 3}, "max_blocks"),
                      ({**LOAM_UEC, "walltime": "1 hour"}, "walltime")):
        out = await _export(app, uec, confirm_spend=True)
        assert out.phase == "refused" and out.reason in ("schema", "invalid"), (uec, out)
        assert word in (out.notice or ""), out.notice
    both = await _export(app, {**LOAM_UEC, "partition": "debug"}, confirm_spend=True)
    assert both.phase == "refused" and "queue" in (both.notice or "") and "partition" in (both.notice or ""), both
    assert app.runners.built == []


async def test_a_key_the_template_does_not_render_is_refused_never_dropped(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch, template=POLARIS_TEMPLATE, schema=POLARIS_SCHEMA)
    out = await _export(app, {**LOAM_UEC, "mem_per_node": 64}, confirm_spend=True)
    assert out.phase == "refused" and out.reason == "not_rendered" and "mem_per_node" in (out.notice or ""), out
    assert app.runners.built == []
    # the entry's own defaults the template does not render (interface) are simply not sent
    ok = await _export(app, LOAM_UEC)
    assert ok.phase == "needs_confirmation" and "interface" not in ok.user_endpoint_config, ok


async def test_no_account_on_an_account_floor_facility_asks_for_one(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    out = await _export(app, {k: v for k, v in LOAM_UEC.items() if k != "account"}, confirm_spend=True)
    assert out.phase == "needs_account" and app.runners.built == [], out


@pytest.mark.parametrize("key,value", [
    ("account", "hpcb; rm -rf ~"),
    ("queue", "debug$(id)"),
    ("scheduler_options", "#PBS -l filesystems=home\nrm -rf ~"),
    ("scheduler_options", "#PBS -l select=999:ncpus=64"),          # a directive, but one that moves the terms' bounds
    ("scheduler_options", "#PBS -o /scratch/elsewhere"),           # ... or the path a stop finds loam's blocks by
    ("worker_init", "source env.sh\x00; curl x"),
    ("endpoint_setup", "true"),
    ("walltime", "00:15:00\n#PBS -l select=999"),
    ("available_accelerators", [{"x": 1}]),
    ("nodes_per_block", True),
    ("max_blocks", 10 ** 9),
], ids=lambda v: str(v)[:24])
async def test_no_value_becomes_shell_code(tmp_path, monkeypatch, key, value):
    app = await _mep_app(tmp_path, monkeypatch, template=POLARIS_TEMPLATE, schema=POLARIS_SCHEMA)
    out = await _export(app, {**LOAM_UEC, key: value}, confirm_spend=True)
    assert out.phase == "refused" and out.reason in ("invalid", "schema", "not_rendered"), (key, out)
    assert key in (out.notice or ""), out.notice
    assert app.runners.built == []


async def test_loams_environment_is_escaped_data(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    uec = {**LOAM_UEC, "worker_init": "source /lus/proj/env/bin/activate && export X='a b' Y=\"$HOME\""}
    _ask, out = await _yes(app, uec)
    rec = out.record
    env = rec.loam.env
    assert env["HPC_GLOBUS_ENDPOINT_ID"] == MEP_UUID and env["HPC_NNODES"] == "1", env
    assert env["HPC_SYSTEM"] == rec.facility.system == "fake-pbs-mep", env
    assert json.loads(env["HPC_ENDPOINT_CONFIG_JSON"]) == rec.user_endpoint_config
    assert all(isinstance(v, str) for v in env.values()), env
    # the record carries data, never a script: loam writes set_env.sh itself, quoting each value
    raw = json.loads(Path(out.path).read_text())
    assert set(raw["loam"]) == {"env"}, raw["loam"]
    for how in ("shlex", "printf"):
        script = tmp_path / f"set_env_{how}.sh"
        if how == "shlex":   # Python's way, as the vault says
            script.write_text("".join(f"export {k}={shlex.quote(v)}\n" for k, v in env.items()))
        else:                # the shell's: printf 'export %s=%q\n'
            gen = subprocess.run([_BASH, "-c", 'for k in "$@"; do printf "export %s=%q\\n" "$k" "${!k}"; done', "_",
                                  *env], capture_output=True, text=True, timeout=30,
                                 env={"PATH": "/usr/bin:/bin", **env})
            assert gen.returncode == 0, gen
            script.write_text(gen.stdout)
        p = subprocess.run([_BASH, "-c", f'. {shlex.quote(str(script))} && printf "%s" "$HPC_ENDPOINT_CONFIG_JSON"'],
                           capture_output=True, text=True, timeout=30,
                           env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)})
        assert p.returncode == 0 and json.loads(p.stdout) == rec.user_endpoint_config, (how, p)


# ---- what the canary saw ---------------------------------------------------------------------------------------------

async def test_the_validation_is_what_the_canary_reported_from_inside_the_block(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    _ask, out = await _yes(app, LOAM_UEC)
    rec = out.record
    v = rec.validation
    assert v.ok is True and v.job_id == "12.fakepbs" and v.host == socket.gethostname(), v
    assert v.device_pin == {"CUDA_VISIBLE_DEVICES": "0"}, v
    assert v.identity and v.task_id == "canary-1", v
    assert rec.endpoint.manager_id == MEP_UUID and rec.endpoint.uep_id_seen == UEP_ID, rec.endpoint
    assert rec.endpoint.kind == "facility_mep" and rec.facility.access == "mep", rec
    assert rec.facility.id == "fake-pbs-mep" and rec.facility.scheduler == "pbs", rec.facility
    assert rec.worker.python == platform.python_version(), rec.worker
    major_minor = ".".join(platform.python_version().split(".")[:2])
    assert rec.client_requirements.python == major_minor, rec.client_requirements
    assert any(c.ok and "debug" in c.check for c in v.queue_checks), v.queue_checks


# ---- 5. release names the end that really works, per facility type ---------------------------------------------------

async def test_release_on_a_pbs_facility_endpoint_is_stop_submitting_then_idle_never_qdel_alone(tmp_path,
                                                                                               monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    _ask, out = await _yes(app, LOAM_UEC)
    rel = out.record.release
    assert rel.min_blocks == 0 and rel.ends_by_itself is True and rel.block_idle_release_s == 240, rel
    assert rel.cancel_block == "qdel 12.fakepbs", rel
    assert "stop" in rel.steps[0].lower() and "submit" in rel.steps[0].lower(), rel.steps
    qdel = [i for i, s in enumerate(rel.steps) if "qdel" in s]
    assert qdel and qdel[0] > 0, rel.steps
    assert all("min_blocks" in rel.steps[i] for i in qdel), rel.steps          # why the qdel ends it: nothing replaces it
    assert "daemon.pid" not in " ".join(rel.steps)


async def test_release_on_a_slurm_facility_endpoint_names_scancel(tmp_path, monkeypatch):
    entry = _pbs_entry(id="fake-mep", compute={"scheduler": "slurm", "interface": "eth0", "env_setup": "",
                                               "scratch_root": "$HOME/.hpc-bridge"},
                       defaults={"partition": "debug", "walltime": "00:15:00", "max_blocks": 1})
    template = POLARIS_TEMPLATE.replace("PBSProProvider", "SlurmProvider").replace("queue: {{ queue }}",
                                                                                 "partition: {{ partition }}")
    block = _Block(tmp_path, MEP_UUID, scheduler="slurm", job="5150")
    app = await _mep_app(tmp_path, monkeypatch, template=template, entry=entry, block=block,
                         schema={"type": "object", "properties": {}})
    uec = {"partition": "debug", "account": "hpcb", "walltime": "00:15:00", "max_blocks": 1, "min_blocks": 0}
    _ask, out = await _yes(app, uec)
    assert out.phase == "exported", out
    assert out.record.release.cancel_block == "scancel 5150", out.record.release


async def test_release_on_hpc_bridges_own_ssh_endpoint(tmp_path):
    app = _ssh_app(tmp_path)
    _ask, out = await _yes(app, {"partition": "main", "walltime": "00:10:00", "max_blocks": 1})
    assert out.phase == "exported", out
    rec = out.record
    assert rec.facility.access == "ssh" and rec.endpoint.kind == "hpc_bridge_ssh", rec
    rel = rec.release
    assert rel.ends_by_itself is True and rel.cancel_block == "scancel 4242", rel
    assert rel.block_idle_release_s == Profile().max_idletime_s, rel
    assert rel.uep_idle_exit_s == Profile().uep_idle_heartbeats * 30, rel
    assert "stop_endpoint" in rel.hpc_bridge_scope and "teardown_endpoint" in rel.hpc_bridge_scope, rel
    # its own user endpoint (a block-request number of its own), never the session's
    wire = rec.user_endpoint_config
    assert isinstance(wire.get("hpcb_block"), int) and wire["hpcb_block"] > 0, wire
    assert "compute" not in wire and wire["partition"] == "main", wire


async def test_a_hostile_job_id_from_the_block_never_reaches_a_command(tmp_path, monkeypatch):
    block = _Block(tmp_path, MEP_UUID, job="12;touch${IFS}/tmp/x")
    app = await _mep_app(tmp_path, monkeypatch, block=block)
    _ask, out = await _yes(app, LOAM_UEC)
    rel = out.record.release
    assert ";" not in (rel.cancel_block or "") and "touch" not in json.dumps(rel.model_dump()), rel
    assert out.record.validation.job_id is None, out.record.validation


# ---- 6. expires_at -----------------------------------------------------------------------------------------------------

async def test_expires_at_is_counted_from_the_yes_and_bounded(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    _ask, out = await _yes(app, LOAM_UEC, expires_hours=12)
    rec = out.record

    def at(s: str) -> datetime:
        assert s.endswith("Z"), s
        return datetime.fromisoformat(s[:-1] + "+00:00")

    approved, created, expires = at(rec.terms.approved_at), at(rec.created_at), at(rec.expires_at)
    assert expires - approved == timedelta(hours=12) and approved <= created < expires, rec
    assert rec.terms.expires_at == rec.expires_at
    for bad in (0, -1, 169, "soon", float("nan")):
        refused = await _export(app, LOAM_UEC, expires_hours=bad)
        assert refused.phase == "refused" and refused.reason == "expires", (bad, refused)


async def test_a_longer_horizon_is_a_larger_worst_case(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    short = await _export(app, LOAM_UEC, expires_hours=1)
    long = await _export(app, LOAM_UEC, expires_hours=48)
    assert long.terms.worst_case > short.terms.worst_case > short.terms.canary_worst_case, (short.terms, long.terms)


# ---- 7. a stop or teardown while a handoff of the endpoint is unexpired (the coordinator's addition, stage B) ----------

SESSION_UEP = "9d8c7b6a-5f4e-4d3c-8b2a-1f0e9d8c7b6a"


async def _ssh_with_handoff(tmp: Path, *, lag: int = 0):
    """hpc-bridge's own endpoint with a fake Slurm on its login node: the session's block (job 2001, its own user
    endpoint) and loam's, exported through a handoff whose canary saw loam's user endpoint (job 2002)."""
    from tests.fake_sched import FakeSched

    sched = FakeSched(tmp / "sched", user="u")
    if lag:
        sched.knob(cancel_lag=lag)
    home = tmp / "home" / ".globus_compute"
    mine = sched.add_job(state="RUNNING", name="parsl.mine",
                         stdout=f"{home}/uep.{SSH_EID}.{SESSION_UEP}/submit_scripts/parsl.mine.stdout")
    loam = sched.add_job(state="RUNNING", name="parsl.loam",
                         stdout=f"{home}/uep.{SSH_EID}.{UEP_ID}/submit_scripts/parsl.loam.stdout")
    block = _Block(tmp, SSH_EID, scheduler="slurm", job=loam, pin=None, workers=2, sched=sched)
    app = _ssh_app(tmp, block=block)
    _ask, out = await _yes(app, {"partition": "main", "walltime": "00:10:00", "max_blocks": 1})
    assert out.phase == "exported" and out.record.endpoint.uep_id_seen == UEP_ID, out
    return app, sched, mine, loam, out


async def test_a_stop_with_an_unexpired_handoff_asks_first_and_changes_nothing(tmp_path):
    app, sched, mine, loam, out = await _ssh_with_handoff(tmp_path)
    st = await server._stop_endpoint(app)
    assert st.status == "needs_confirmation", st
    assert [h.handoff_id for h in st.handoffs] == [out.record.handoff_id], st.handoffs
    assert st.handoffs[0].expires_at == out.record.expires_at and out.record.expires_at in (st.notice or "")
    assert "cancels loam's blocks" in (st.notice or "") and "user endpoint" in (st.notice or ""), st.notice
    assert sched.calls("scancel") == [] and sched.job(mine)["state"] == sched.job(loam)["state"] == "RUNNING"
    again = await server._stop_endpoint(app)                      # nothing changed: it asks again
    assert again.status == "needs_confirmation"


async def test_keep_releases_only_the_sessions_block_and_names_the_running_handoff(tmp_path):
    app, sched, mine, loam, out = await _ssh_with_handoff(tmp_path)
    st = await server._stop_endpoint(app, handoffs="keep")
    assert sched.job(mine)["state"] == "CANCELLED" and sched.job(loam)["state"] == "RUNNING", sched.jobs()
    assert st.status == "draining" and st.spend_stopped is not True, st
    assert out.record.handoff_id in (st.notice or "") and [h.handoff_id for h in st.handoffs] == [
        out.record.handoff_id], st
    from hpc_bridge import scheduler_ops

    assert scheduler_ops.RELEASE_KEEP.get() is None                 # the scope lasts the one stop, in its own context


async def test_cancel_ends_the_handoff_and_is_down_only_once_nothing_of_it_is_listed(tmp_path):
    app, sched, mine, loam, _out = await _ssh_with_handoff(tmp_path)
    st = await server._stop_endpoint(app, handoffs="cancel")
    assert sched.job(mine)["state"] == sched.job(loam)["state"] == "CANCELLED", sched.jobs()
    assert st.status == "down" and "nothing of it" in (st.notice or ""), st
    nxt = await server._stop_endpoint(app)                          # ended: no later stop asks about it
    assert nxt.status == "down" and nxt.handoffs == [], nxt


async def test_cancel_while_loams_job_is_still_listed_is_draining(tmp_path):
    app, sched, _mine, loam, _out = await _ssh_with_handoff(tmp_path, lag=5)   # a cancelled job listed COMPLETING
    st = await server._stop_endpoint(app, handoffs="cancel")
    assert st.status == "draining" and loam in (st.notice or ""), st
    assert sched.job(loam)["state"] == "CANCELLED"


async def test_an_expired_handoff_is_not_asked_about(tmp_path):
    app, _sched, _mine, _loam, out = await _ssh_with_handoff(tmp_path)
    raw = json.loads(Path(out.path).read_text())
    raw["expires_at"] = (datetime.now(UTC) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    Path(out.path).write_text(json.dumps(raw))
    st = await server._stop_endpoint(app)
    assert st.status != "needs_confirmation" and st.handoffs == [], st


async def test_teardown_asks_the_same_way_and_keep_is_answered_with_the_stop(tmp_path, monkeypatch):
    app, sched, _mine, _loam, _out = await _ssh_with_handoff(tmp_path)
    torn: list[bool] = []

    async def teardown(a):
        torn.append(True)
        return server.EndpointStatus(status="down", block_state="cold", notice="torn down")

    monkeypatch.setattr(server, "_teardown_endpoint_blocks", teardown)
    ask = await server._teardown_endpoint(app)
    assert ask.status == "needs_confirmation" and ask.handoffs and "teardown" in (ask.notice or ""), ask
    keep = await server._teardown_endpoint(app, handoffs="keep")
    assert keep.status == "needs_confirmation" and 'stop_endpoint(handoffs="keep")' in (keep.notice or ""), keep
    assert torn == [] and sched.calls("scancel") == []
    done = await server._teardown_endpoint(app, handoffs="cancel")
    assert done.status == "down" and torn == [True], done
    from hpc_bridge import handoff

    assert handoff.live(app) == []


async def test_on_a_facility_endpoint_the_stop_names_the_handoff_and_its_release_only(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    _ask, out = await _yes(app, LOAM_UEC)
    st = await server._stop_endpoint(app)
    assert st.status == "draining" and [h.handoff_id for h in st.handoffs] == [out.record.handoff_id], st
    assert out.record.handoff_id in (st.notice or "") and "qdel 12.fakepbs" in (st.notice or ""), st.notice
    assert "hpc-bridge does not stop it" in (st.notice or "")


# ---- the surface -----------------------------------------------------------------------------------------------------

def test_export_handoff_is_the_seventeenth_tool_and_says_what_it_does_not_enforce():
    tools = {t.name: t for t in asyncio.run(server.mcp.list_tools())}
    assert "export_handoff" in tools and len(tools) == 17, sorted(tools)
    doc = tools["export_handoff"].description or ""
    assert "confirm_spend" in doc and "uec-bounds-only" in doc and "min_blocks" in doc, doc
