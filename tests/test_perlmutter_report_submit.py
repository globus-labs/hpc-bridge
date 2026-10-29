"""Perlmutter (NERSC) user report, 2026-10-29, items 6 and 7: Slurm submission on a self-hosted SSH endpoint.

The reporter ran plugin 0.1.20 against globus-compute-endpoint 4.16.0. Every block submission was refused AT SUBMIT —
sbatch exited 1 with one line, "sbatch: error: Job request does not match any supported policy." (NERSC's submit
filter), and no job id — while ensure_endpoint_up / run_shell kept answering `provisioning` / `cold_start` ("allocating
nodes…"); the words were only in the user endpoint's own endpoint.log.

What these tests pin, on main:

- #6, the check carried the refusal: a terminal `down` + REJECTED quoting the scheduler's reply (0.1.17's REJECTED, the
  reply read from the whole failure since #213). Passes.
- #6, the check's failure never reached the service (globus-compute-endpoint's bad-state watcher stops the user
  endpoint as its result publisher is still sending: the lost-result race, #208 — the check stays queued): past the
  grace, ensure_endpoint_up's pilot probe reads the refusal from the block's own user endpoint log and quotes it as
  "likely REJECTED" (#213). Passes. At 0.1.20 the same state answered `provisioning` with a wordless "likely REJECTED
  … Check … and the endpoint log" — where the reporter found the line.
- #6, residual (xfail): run_shell(shape="compute") on that same state still answers cold_start "allocating nodes…"
  (est_wait_s=60), forever: only ensure_endpoint_up runs the probe, and the skill tells the agent to read a
  `provisioning` / `cold_start` alike as "a worker is still warming; retry shortly".
- #7(c)/(d): a bring-your-own facility whose partition is "" reaches parsl with no partition (parsl writes
  `--partition` only for a non-empty value), so a site that routes by QOS and constraint alone is reachable through
  connect_facility(details=…); hpc-bridge itself never writes a QOS; `--constraint=cpu` / `-C cpu` pass the terms and
  size refusals (#234). Pass.

The facility text here is SYNTHETIC, shaped like the report (no real account, user or path of the reporter's).
"""
from __future__ import annotations

import json
import time

import jinja2
import pytest
import yaml

from hpc_bridge import binding, resources, server
from hpc_bridge.catalog.entry import scheduler_terms_kind
from hpc_bridge.facility.remote import SlurmFacility, profile_from_catalog_entry
from hpc_bridge.models import FacilityDetails, ShellOutcome
from hpc_bridge.profile import Profile
from hpc_bridge.server import _ensure_endpoint_up, _run_shell
from hpc_bridge.shapes import shape_config
from tests.test_rejected_words import (
    _ASK,
    _LIKELY,
    _QUOTED,
    _SLURM,
    _bad_state_log,
    _probe,
    _refusing,
    _ssh_app,
    _task_failure,
    _write_log,
    needs_posix_shell,
)
from tests.test_uep_evidence import _lost_check_app

# NERSC's submit filter's reply, as the report quotes parsl's log of it (one stderr line; sbatch exited 1, no job id)
PM = "sbatch: error: Job request does not match any supported policy."
# the endpoint's Python on a NERSC-like home (synthetic; `alice` is no real user)
_PY = "/global/homes/a/alice/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/lib/python3.11"
REFUSED = _task_failure(f"{_SLURM}; recode=1, stdout=, stderr={PM}\n", py=_PY)


# --- #6: the check carried the refusal ---------------------------------------------------------------------------------

async def test_6_a_refusal_the_check_carries_is_down_and_quotes_the_policy_line(monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app()
    _refusing(app, REFUSED)
    res = await _ensure_endpoint_up(app, partition="debug", account="m0000", confirm_spend=True)
    assert res.status == "down" and res.block_state == "cold"
    assert res.notice.startswith("the scheduler REJECTED the block submission (submitted on partition 'debug', "
                                 f"account 'm0000'). Its reply, {_QUOTED}: {PM!r}. The new block was not created")
    out = await _run_shell(app, "hostname", shape="compute")      # the spend was voided: asked again, the reply named
    assert out.phase == "needs_confirmation" and "Job request does not match any supported policy." in out.notice


# --- #6: the check's failure never reached the service (the lost-result race) ------------------------------------------

async def _lost_refusal(monkeypatch, tmp_path):
    """The reporter's state: spend confirmed on this plugin's SSH endpoint (a user endpoint per block request, which
    names it in its log), the check still queued for good — its user endpoint failed it in bad state after sbatch
    refused the block, and stopped before its report left — the log on the login node holding the refusal, nothing of
    this endpoint's in squeue or sacct, and the block cold well past the grace. The login channel RUNS the pilot probe
    here, against that log. Returns the app and ensure_endpoint_up's answer on it."""
    app, _runner, block = await _lost_check_app(monkeypatch)
    root = tmp_path / "gc"
    _write_log(root, _bad_state_log(block, [PM], recode="1"))

    async def login(a, command, session_id="default", shape="compute"):
        out = _probe(command, root, tmp_path) if "HPCB_ERR" in command else ""
        return ShellOutcome(phase="complete", exit_code=0, stdout=out, block_state="warm")

    monkeypatch.setattr(server, "_run_shell", login)              # the login channel the scheduler ops ride
    server._shape_runtime(app, "compute").provisioning_since = time.monotonic() - 600
    return app, await _ensure_endpoint_up(app, shape="compute")


@needs_posix_shell
async def test_6_a_lost_refusal_is_quoted_as_likely_by_ensure_endpoint_up(monkeypatch, tmp_path):
    _app, res = await _lost_refusal(monkeypatch, tmp_path)
    assert res.status == "provisioning"                           # a log read is not conclusive: nothing is voided
    assert (f"this block's user endpoint logged the scheduler's reply to its submission, {_QUOTED}: {PM!r}. {_LIKELY} "
            f"Tell the user what the reply says and ask which terms to use: {_ASK}.") in res.notice


@needs_posix_shell
@pytest.mark.xfail(strict=True, raises=AssertionError, reason=(
    "perlmutter report #6: run_shell(shape='compute') on a block whose submission the scheduler refused, and whose "
    "failure never reached the service, answers cold_start 'allocating nodes…' (est_wait_s=60) on every call — only "
    "ensure_endpoint_up runs the pilot probe that reads the refusal (notices._cold_outcome)"))
async def test_6_run_shell_on_a_lost_refusal_says_more_than_allocating_nodes(monkeypatch, tmp_path):
    app, res = await _lost_refusal(monkeypatch, tmp_path)
    # the precondition (test_6_a_lost_refusal_is_quoted_as_likely_by_ensure_endpoint_up pins it): the server has just
    # read the refusal on this very state
    out = await _run_shell(app, "hostname", shape="compute")
    assert "REJECTED" in (out.notice or "") or PM in (out.notice or ""), (out.phase, out.notice, res.notice[:80])


# --- #7(c)/(d): routing by QOS and constraint alone; the constraint passes the terms and size refusals -----------------

_BYO = dict(ssh_host="perlmutter.example.org", interface="hsn0", env_setup="true",
            scratch_root="/pscratch/sd/a/{user}/.hpc-bridge", scheduler="slurm")


def _rendered_provider(details: FacilityDetails) -> dict:
    """The provider block the endpoint's manager renders for a compute task of a bring-your-own facility bound from
    `details` (its sanitizer, then Jinja, as tests/test_resource_templates does)."""
    entry = binding._entry_from_details("perlmutter", details)
    facility = SlurmFacility(profile_from_catalog_entry(entry, user="alice", account="m0000"), cli=None)
    tmpl, defaults = facility.config_template(Profile(mode="batch"))
    wire = facility.dispatch_uec({**defaults, **shape_config("compute")})
    sanitized = {k: (json.dumps(v) if isinstance(v, str) else v) for k, v in wire.items()}
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    return yaml.safe_load(env.from_string(tmpl).render(**sanitized))["engine"]["provider"]


def test_7c_an_empty_partition_reaches_parsl_as_none_so_qos_and_constraint_alone_route_the_block():
    # parsl's SlurmProvider writes `#SBATCH --partition=…` only `if partition:` — an empty one sends none
    opts = "#SBATCH --constraint=cpu\n#SBATCH --qos=debug"
    p = _rendered_provider(FacilityDetails(**_BYO, partition="", scheduler_options=opts))
    assert p["partition"] == "" and p["scheduler_options"] == opts
    # #7(d): hpc-bridge writes no QOS of its own — `-q` reaches sbatch only from the user's own scheduler_options
    plain = _rendered_provider(FacilityDetails(**_BYO, partition="debug", scheduler_options="#SBATCH --constraint=cpu"))
    assert "qos" not in plain and plain["scheduler_options"] == "#SBATCH --constraint=cpu"


@pytest.mark.parametrize("given", ["#SBATCH --constraint=cpu", "#SBATCH -C cpu", "--constraint=cpu", "#SBATCH -C gpu"])
def test_7_a_constraint_passes_the_terms_and_size_refusals(given):
    details, _rewrites = binding._directive_details(FacilityDetails(**_BYO, partition="debug", scheduler_options=given))
    written = details.scheduler_options
    assert written.startswith("#SBATCH ") and scheduler_terms_kind(written) is None
    assert resources.options_size(written) == resources.options_size(None)   # no size read into the terms
