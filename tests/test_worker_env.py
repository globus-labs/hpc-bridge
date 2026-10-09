"""Facility-MEP worker environments (2026-10-06): the registry entry records how the worker pool's packages are kept
in step with the facility's user endpoint (`compute.worker_env`), and an attach to a facility whose endpoint version
moved since the entry was proven says so. parsl's interchange<->worker protocol changes between releases, and a
skew lets a block run and bill while every result is dropped (Anvil floated ahead of our worker; Delta's fixed
install sat behind a fresh worker venv)."""
from pathlib import Path

import pytest
import yaml

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


def test_the_curator_check_ties_the_strategy_to_the_install_line():
    from hpc_bridge.catalog.entry import worker_env_problems

    def probs(env_setup, **we):
        return worker_env_problems(_compute(env_setup, worker_env={"verified_with": "4.16.0", **we}))

    pinned = _INSTALL + ' "parsl==2026.8.10"'
    assert probs(pinned, strategy="pin") == []
    for upgrade in ("-q --upgrade ", "-qU ", "-U -q "):  # every spelling that floats every package
        floated = _INSTALL.replace("-q ", upgrade)
        assert probs(floated, strategy="float") == [], upgrade
        assert any("must not upgrade" in p for p in probs(floated + ' "parsl==2026.8.10"', strategy="pin")), upgrade
    # -P / --upgrade-package upgrades ONE package: not a float
    assert any("needs `--upgrade`" in p for p in probs(_INSTALL.replace("-q ", "-q -P globus-compute-endpoint "),
                                                        strategy="float"))
    # an upgrade elsewhere in the line (bootstrapping uv) is not the worker install
    assert any("needs `--upgrade`" in p for p in probs("pip install --upgrade uv; " + _INSTALL, strategy="float"))
    assert any("explicit parsl" in p for p in probs(_INSTALL, strategy="pin"))  # gce alone sets only a floor
    assert any("not one of" in p for p in probs(pinned, strategy="constrain"))


def test_clients_read_worker_env_leniently():
    # every installed plugin parses registry entries: a strategy a later curator adds, or a garbled block, must
    # cost the staleness check at most — never the facility
    assert _compute(_INSTALL, worker_env={"strategy": "constrain", "verified_with": "4.17.0"}).worker_env.strategy == "constrain"
    assert _compute(_INSTALL, worker_env=["not", "a", "mapping"]).worker_env is None
    assert _compute(_INSTALL, worker_env={"strategy": "pin", "verified_with": "4.16; rm -rf ~"}).worker_env.verified_with is None
    assert _compute(_INSTALL).worker_env is None  # optional: SSH entries share one environment


def test_ingest_refuses_an_entry_whose_worker_env_does_not_check(tmp_path):
    from hpc_bridge.catalog.ingest import ingest

    rows = yaml.safe_load((SEEDS / "ncsa-delta.yaml").read_text())
    rows[0]["compute"]["worker_env"]["strategy"] = "float"  # but the install pins
    bad = tmp_path / "seed.yaml"
    bad.write_text(yaml.safe_dump(rows))

    class _Search:
        def ingest(self, index, doc):
            raise AssertionError("nothing must reach the index")

    with pytest.raises(ValueError, match="worker_env problems"):
        ingest("idx", bad, _Search())


def test_the_registry_entries_carry_their_worker_rules():
    delta, anvil, lab = (_seed("ncsa-delta.yaml", "delta"), _seed("anvil.yaml", "anvil"),
                         _seed("globus-cluster.yaml", "globus-labs"))
    assert delta.compute.worker_env.strategy == "pin" and '"parsl==2026.8.10"' in delta.compute.env_setup
    assert anvil.compute.worker_env.strategy == "float" and "--upgrade" in anvil.compute.env_setup
    assert lab.compute.worker_env.strategy == "pin" and lab.compute.worker_env.verified_with == "4.15.0"
    assert "parsl==2026.4.20" in lab.compute.env_setup  # gce 4.15.0's own exact pin, stated
    from hpc_bridge.catalog.entry import worker_env_problems
    assert [worker_env_problems(e.compute) for e in (delta, anvil, lab)] == [[], [], []]
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


def test_a_long_allocation_on_a_facility_endpoint_names_the_silent_block_and_the_stale_entry():
    from hpc_bridge.notices import _allocating_notice

    late = _allocating_notice("gpuA40x4", 480, facility_mep=True)
    assert "RUNNING and billing" in late and "stop_endpoint" in late and "squeue" in late
    assert "scheduler_options" in late  # the rejection case is still named
    stale = "STALE ENTRY: this facility's endpoint now runs v4.17.0"
    assert stale in _allocating_notice("gpuA40x4", 480, facility_mep=True, stale=stale)
    assert "RUNNING" not in _allocating_notice("gpuA40x4", 120, facility_mep=True)  # not before five minutes
    assert "RUNNING" not in _allocating_notice("main", 480, facility_mep=False)    # our own endpoints can stop


def test_the_worker_init_sent_to_anvil_floats_at_the_client_version():
    from importlib.metadata import version

    fac = MEPFacility.from_entry(_seed("anvil.yaml", "anvil"), account="cis250223")
    fac.endpoint_version = "4.16.0"
    wi = fac.sanitize_uec(fac.config_template(None)[1])["worker_init"]
    assert f'--upgrade "globus-compute-endpoint=={version("globus-compute-sdk")}"' in wi


async def test_a_failed_status_call_still_reads_the_template_and_keeps_the_pin():
    # the status API failing used to skip load_template: no endpoint version, so the worker_init was dropped and
    # the facility's own default ran — Delta's has no parsl pin, i.e. exactly the 10-06 skew
    class _NoStatus(_Client):
        def get_endpoint_status(self, endpoint_id):
            raise RuntimeError("status API unavailable")

    fac = MEPFacility.from_entry(_seed("ncsa-delta.yaml", "delta"), client_factory=lambda: _NoStatus("4.16.0"))
    assert await fac.manager_online(fac.endpoint_id) is True
    assert fac.endpoint_version == "4.16.0"
    assert '"parsl==2026.8.10"' in fac.sanitize_uec(fac.config_template(None)[1])["worker_init"]


async def test_an_unreadable_facility_version_falls_back_to_the_verified_one():
    class _NoMetadata(_Client):
        def get_endpoint_metadata(self, endpoint_id):
            raise RuntimeError("metadata unavailable")

    fac = MEPFacility.from_entry(_seed("ncsa-delta.yaml", "delta"), client_factory=lambda: _NoMetadata("x"))
    await fac.load_template()
    wi = fac.sanitize_uec(fac.config_template(None)[1])["worker_init"]
    assert '"globus-compute-endpoint==4.16.0" "parsl==2026.8.10"' in wi


async def test_versions_compare_by_value_not_spelling():
    entry = _seed("ncsa-delta.yaml", "delta")
    for live in ("4.16", "v4.16.0", " 4.16.0 "):
        fac = MEPFacility.from_entry(entry, client_factory=lambda live=live: _Client(live))
        await fac.load_template()
        assert fac.stale_worker_note() is None, live


async def test_the_stale_note_reaches_the_connect_notice(monkeypatch):
    from hpc_bridge import binding, server
    from hpc_bridge.profile import Profile
    from hpc_bridge.server import AppCtx
    from tests.fakes import FakeCatalog

    entry = _seed("ncsa-delta.yaml", "delta")
    fac = MEPFacility.from_entry(entry, client_factory=lambda: _Client("4.17.0"))
    app = AppCtx(facility=fac, profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: fac)
    res = await server._connect_facility(app, "delta")
    assert "STALE ENTRY" in res.notice and "v4.17.0" in res.notice


def test_ingest_refuses_a_worker_env_the_client_would_quietly_drop(tmp_path):
    # the lenient client parse turns these into worker_env=None; ingest must refuse them, not publish null (which
    # silently removes the entry's verified-version fallback)
    from hpc_bridge.catalog.ingest import ingest

    class _Search:
        def ingest(self, index, doc):
            raise AssertionError("nothing must reach the index")

    for garble in ({"strategy": "pin", "verified_with": 4.16}, {"verified_with": "4.16.0"}, ["pin"]):
        rows = yaml.safe_load((SEEDS / "ncsa-delta.yaml").read_text())
        rows[0]["compute"]["worker_env"] = garble
        seed = tmp_path / "seed.yaml"
        seed.write_text(yaml.safe_dump(rows))
        with pytest.raises(ValueError, match="worker_env"):
            ingest("idx", seed, _Search())


def test_a_v_prefixed_verified_version_is_stored_bare():
    assert _compute(_INSTALL, worker_env={"strategy": "pin", "verified_with": "v4.16.0"}).worker_env.verified_with == "4.16.0"


def test_the_pin_note_says_when_the_version_came_from_the_entry():
    fac = MEPFacility.from_entry(_seed("ncsa-delta.yaml", "delta"))
    fac.sanitize_uec(fac.config_template(None)[1])  # live version never read (the startup-pinned path)
    assert any("verified with" in n and "not read" in n for n in fac.template_notes)


def test_ingest_takes_a_seed_directory(tmp_path):
    # `hpc-bridge-catalog <index> <dir>` ingests every *.yaml in it; the raw worker_env check must read the same set
    from hpc_bridge.catalog.ingest import ingest

    sent = []

    class _Search:
        def ingest(self, index, doc):
            sent.append(doc)

    for name in ("ncsa-delta.yaml", "anvil.yaml", "globus-cluster.yaml"):
        (tmp_path / name).write_text((SEEDS / name).read_text())
    assert ingest("idx", tmp_path, _Search()) == 3 and len(sent) == 1


async def test_the_stale_note_names_a_python_or_template_change_too():
    from hpc_bridge.catalog.entry import CatalogEntry, facility_fingerprint

    md = {"endpoint_version": "4.16.0", "python_version": "3.13.13", "user_config_template": "engine: {}\n",
          "user_config_schema": {"type": "object"}, "display_name": "Delta"}

    class _MD:
        def __init__(self, md):
            self.md = md

        def get_endpoint_metadata(self, endpoint_id):
            return self.md

    base = _seed("ncsa-delta.yaml", "delta").model_dump(mode="json")
    entry = CatalogEntry.model_validate({**base, "verification": {"verified_on": "2026-10-09", **facility_fingerprint(md)}})
    for live, says in ((md, None), (dict(md, python_version="3.13.14"), None),  # an OS patch: not every agent's problem
                       (dict(md, python_version="3.14.0"), "Python 3.14.0"),
                       (dict(md, user_config_template="engine: {x: 1}\n"), "template changed")):
        fac = MEPFacility.from_entry(entry, client_factory=lambda live=live: _MD(live))
        await fac.load_template()
        note = fac.stale_worker_note()
        assert (note is None) if says is None else (says in note and note.startswith("STALE ENTRY")), live
