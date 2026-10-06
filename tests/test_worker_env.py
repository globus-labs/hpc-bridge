"""Facility-MEP worker environments (2026-10-06): the registry entry records how the worker pool's packages are kept
in step with the facility's user endpoint (`compute.worker_env`), and an attach to a facility whose endpoint version
moved since the entry was proven says so. parsl's interchange<->worker protocol changes between releases, and a
skew lets a block run and bill while every result is dropped (Anvil floated ahead of our worker; Delta's fixed
install sat behind a fresh worker venv)."""
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from hpc_bridge.catalog.entry import CatalogEntry, Compute
from hpc_bridge.facility.mep import MEPFacility

SEEDS = Path(__file__).resolve().parents[1] / "src" / "hpc_bridge" / "catalog" / "seed"
_INSTALL = 'uv pip install -q "globus-compute-endpoint=={gce_version}"'


def _compute(env_setup, **over):
    return Compute(scheduler="slurm", interface="ib0", env_setup=env_setup, scratch_root="$HOME/.hpc-bridge", **over)


def _seed(name, entry_id):
    rows = yaml.safe_load((SEEDS / name).read_text())
    return CatalogEntry.model_validate(next(r for r in rows if r["id"] == entry_id))


class _Client:
    def __init__(self, version):
        self.version = version

    def get_endpoint_metadata(self, endpoint_id):
        return {"endpoint_version": self.version, "display_name": "a facility", "user_config_schema": None}

    def get_endpoint_status(self, endpoint_id):
        return {"status": "online"}


def test_the_strategy_must_match_the_install_line():
    _compute(_INSTALL + ' "parsl==2026.8.10"', worker_env={"strategy": "pin", "verified_with": "4.16.0"})
    _compute(_INSTALL.replace("-q ", "-q --upgrade "), worker_env={"strategy": "float"})
    with pytest.raises(ValidationError, match="float"):
        _compute(_INSTALL, worker_env={"strategy": "float"})
    with pytest.raises(ValidationError, match="pin"):
        _compute(_INSTALL.replace("-q ", "-q --upgrade "), worker_env={"strategy": "pin"})
    with pytest.raises(ValidationError, match="version string"):
        _compute(_INSTALL, worker_env={"strategy": "pin", "verified_with": "4.16; rm -rf ~"})
    assert _compute(_INSTALL).worker_env is None  # optional: SSH entries share one environment


def test_the_registry_entries_carry_their_worker_rules():
    delta, anvil, lab = (_seed("ncsa-delta.yaml", "delta"), _seed("anvil.yaml", "anvil"),
                         _seed("globus-cluster.yaml", "globus-labs"))
    assert delta.compute.worker_env.strategy == "pin" and '"parsl==2026.8.10"' in delta.compute.env_setup
    assert anvil.compute.worker_env.strategy == "float" and "--upgrade" in anvil.compute.env_setup
    assert lab.compute.worker_env.strategy == "pin" and lab.compute.worker_env.verified_with == "4.15.0"
    # the install stays self-contained: no placeholder an older plugin would pass through unresolved
    for e in (delta, anvil, lab):
        leftover = e.compute.env_setup.replace("{gce_version}", "").replace("{python_version}", "")
        assert "{" not in leftover.replace("${", "")


def test_the_worker_init_sent_to_delta_pins_its_parsl():
    fac = MEPFacility.from_entry(_seed("ncsa-delta.yaml", "delta"), account="bgta-delta-gpu")
    fac.endpoint_version = "4.16.0"
    uec = fac.sanitize_uec(fac.config_template(None)[1])
    assert '"globus-compute-endpoint==4.16.0" "parsl==2026.8.10"' in uec["worker_init"]
    assert "--upgrade" not in uec["worker_init"]


async def test_an_upgraded_facility_is_flagged_at_attach():
    entry = _seed("ncsa-delta.yaml", "delta")
    fac = MEPFacility.from_entry(entry, client_factory=lambda: _Client("4.17.0"))
    await fac.load_template()
    note = fac.stale_worker_note()
    assert note and "v4.17.0" in note and "v4.16.0" in note and note in fac.template_notes

    same = MEPFacility.from_entry(entry, client_factory=lambda: _Client("4.16.0"))
    await same.load_template()
    assert same.stale_worker_note() is None and not any("STALE" in n for n in same.template_notes)


async def test_no_verified_version_means_no_staleness_claim():
    entry = _seed("ncsa-delta.yaml", "delta")
    entry.compute.worker_env = None
    fac = MEPFacility.from_entry(entry, client_factory=lambda: _Client("9.9.9"))
    await fac.load_template()
    assert fac.stale_worker_note() is None
