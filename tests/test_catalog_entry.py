# tests/test_catalog_entry.py
import datetime

import pytest
from pydantic import ValidationError

from hpc_bridge.catalog.entry import CatalogEntry, CatalogSummary

VALID_UUID = "11111111-2222-3333-4444-555555555555"


def _entry(**over):
    base = {
        "id": "anvil",
        "facility_key": "purdue",
        "facility": "Purdue / ACCESS",
        "description": "Anvil CPU cluster",
        "display_name": "HPC-Bridge Anvil",
        "transfer_endpoint_uuid": VALID_UUID,
        "ssh_host": "anvil.rcac.purdue.edu",
        "allocation": {"command": "mybalance", "parser": "mybalance"},
        "compute": {
            "scheduler": "slurm",
            "interface": "ib0",
            "env_setup": "module load x && source {venv}/bin/activate",
            "scratch_root": "/anvil/scratch/{user}/.hpc-bridge",
        },
        "defaults": {"partition": "debug"},
        "last_validated": "2026-06-03",
    }
    base.update(over)
    return base


def test_valid_entry_parses_and_applies_defaults():
    e = CatalogEntry.model_validate(_entry())
    assert e.id == "anvil"
    assert e.compute.amqp_port == 443             # defaulted
    assert e.compute.endpoint_name is None  # no bare default; derived as hpc-bridge-<id> at profile build
    assert e.defaults.walltime == "00:30:00"      # defaulted
    assert e.auth_method == "ssh-key"             # defaulted
    assert e.provenance == "curated"              # defaulted
    assert e.compute_mep_uuid is None             # optional
    assert e.last_validated == datetime.date(2026, 6, 3)


def test_subject_is_facility_key_colon_id():
    assert CatalogEntry.model_validate(_entry()).subject == "purdue:anvil"


def test_summary_is_agent_safe_subset():
    s = CatalogEntry.model_validate(_entry()).summary()
    assert s.subject == "purdue:anvil"
    assert s.display_name == "HPC-Bridge Anvil"
    # summary must NOT leak executable config
    assert not hasattr(s, "env_setup")
    assert set(CatalogSummary.model_fields) == {
        "subject", "id", "facility", "description", "display_name",
        "provenance", "last_validated", "access", "access_note", "scheduler",
    }
    assert s.access == "ssh" and "anvil.rcac.purdue.edu" in s.access_note and s.scheduler == "slurm"


def test_summary_says_how_a_mep_facility_is_reached():
    from tests.fakes import fake_mep_entry
    s = fake_mep_entry().summary()
    assert s.access == "mep" and "identity mapped" in s.access_note and "NO ACCOUNT" in s.access_note
    assert "only attaches" in s.access_note  # walk finding: the agent promised a login node + allocation list
    assert "da3df250" not in s.model_dump_json()  # still no raw UUIDs


_MEP_COMPUTE = {
    "scheduler": "slurm",
    "interface": "enP7s7",
    "env_setup": "uv pip install -q globus-compute-endpoint==4.15.0",
    "scratch_root": "$HOME/.hpc-bridge",  # worker-side form: the mapped user's shell expands it
}


def test_mep_entry_omits_ssh_host():
    # a facility-MEP entry dispatches over AMQP only — no SSH host, and `compute_mep_uuid` is its reach
    e = CatalogEntry.model_validate(_entry(ssh_host=None, compute_mep_uuid=VALID_UUID, compute=_MEP_COMPUTE))
    assert e.ssh_host is None
    assert e.compute_mep_uuid == VALID_UUID


def test_mep_only_entry_rejects_client_side_templating():
    # no SSH login name / client venv exists to resolve {user}/{venv} on a MEP — they'd reach the
    # worker literally, so the seed must be caught at validation (ingest), not at first run_shell
    for field, bad in (("scratch_root", "/scratch/{user}/.hpc-bridge"), ("env_setup", "source {venv}/bin/activate")):
        with pytest.raises(ValidationError, match="client-side templating"):
            CatalogEntry.model_validate(_entry(
                ssh_host=None, compute_mep_uuid=VALID_UUID, compute={**_MEP_COMPUTE, field: bad},
            ))
    # an entry that ALSO has ssh_host is still CONSUMED as a MEP (MEP wins in _facility_from_entry), so
    # client-side templating is rejected there too (review 2026-09-03: it shipped literally to the worker)
    with pytest.raises(ValidationError, match="client-side templating"):
        CatalogEntry.model_validate(_entry(compute_mep_uuid=VALID_UUID))  # default fixture uses {user}/{venv}
    CatalogEntry.model_validate(_entry(compute_mep_uuid=VALID_UUID, compute=_MEP_COMPUTE))  # worker-side forms: fine


def test_entry_without_reach_rejected():
    # neither a MEP to dispatch to nor an SSH host to bootstrap ⇒ unreachable, must fail at validation
    with pytest.raises(ValidationError, match="needs a reach"):
        CatalogEntry.model_validate(_entry(ssh_host=None))


def test_init_blocks_defaults_zero_and_overridable():
    assert CatalogEntry.model_validate(_entry()).defaults.init_blocks == 0
    e = CatalogEntry.model_validate(_entry(defaults={"partition": "main", "init_blocks": 1}))
    assert e.defaults.init_blocks == 1
    # init_blocks is a MEP-UEC knob, not a MachineProfile field — stays out of the binding seam
    assert "init_blocks" not in e.profile_kwargs()


def test_bad_uuid_rejected():
    with pytest.raises(ValidationError):
        CatalogEntry.model_validate(_entry(transfer_endpoint_uuid="not-a-uuid"))


def test_unknown_parser_rejected():
    with pytest.raises(ValidationError):
        CatalogEntry.model_validate(
            _entry(allocation={"command": "x", "parser": "bogus"})
        )


def test_uuid_is_normalized_to_canonical_form():
    e = CatalogEntry.model_validate(_entry(
        transfer_endpoint_uuid="11111111222233334444555555555555"  # no dashes
    ))
    assert e.transfer_endpoint_uuid == "11111111-2222-3333-4444-555555555555"


def test_profile_kwargs_maps_every_machineprofile_field():
    kw = CatalogEntry.model_validate(_entry()).profile_kwargs()
    # superset-of-MachineProfile contract; account/worker_init are intentionally absent
    expected = {
        "name", "endpoint_name", "display_name", "env_setup", "interface",
        "partition", "walltime", "max_workers_per_node", "nodes_per_block",
        "max_blocks", "available_accelerators", "amqp_port", "scheduler_options",
        "scratch_root", "scheduler", "cpus_per_node", "exclusive",
    }
    assert set(kw) == expected
    assert "account" not in kw
    assert "worker_init" not in kw
    assert kw["interface"] == "ib0"
    assert kw["name"] == "anvil"


def test_mep_only_scratch_root_must_expand_on_the_worker():
    # only a LEADING $HOME (or ${HOME}/, ~/) expands in the session wrapper; $USER or any other $VAR
    # would be quoted literal (a directory literally named '$USER') — reject at validation
    for sr in ("$HOME/.hpc-bridge", "${HOME}/x/.hpc-bridge", "~/.hpc-bridge", "/scratch/shared/.hpc-bridge"):
        CatalogEntry.model_validate(_entry(ssh_host=None, compute_mep_uuid=VALID_UUID,
                                           compute={**_MEP_COMPUTE, "scratch_root": sr}))
    for sr in ("/scratch/$USER/.hpc-bridge", "$HOME/scratch/$USER/.hpc-bridge", "$SCRATCH/.hpc-bridge", "rel/.hpc-bridge"):
        with pytest.raises(ValidationError, match="scratch_root"):
            CatalogEntry.model_validate(_entry(ssh_host=None, compute_mep_uuid=VALID_UUID,
                                               compute={**_MEP_COMPUTE, "scratch_root": sr}))
    # an SSH entry keeps {user} templating (resolved client-side from the login name) — unchanged
    CatalogEntry.model_validate(_entry(compute={**_MEP_COMPUTE, "scratch_root": "/scratch/{user}/.hpc-bridge"}))
