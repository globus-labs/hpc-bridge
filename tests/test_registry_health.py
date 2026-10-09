"""The registry health check: index ↔ seeds, facility drift, SSH reachability, releases, fresh-install resolution.
Hermetic — every network seam is injected."""
from pathlib import Path

import pytest
import yaml

from hpc_bridge.catalog import health
from hpc_bridge.catalog.bundled import BundledCatalog
from hpc_bridge.catalog.entry import CatalogEntry, facility_fingerprint

SEEDS = Path(health.__file__).parent / "seed"


def _seeds():
    return list(BundledCatalog(SEEDS).entries())


def _entry(eid):
    return next(e for e in _seeds() if e.id == eid)


_MD = {"endpoint_version": "4.16.0", "python_version": "3.13.13", "user_config_template": "engine: {}\n",
       "user_config_schema": {"type": "object"}, "endpoint_config": "display_name: x\n"}


def _verified(entry, md=_MD, **over):
    fp = facility_fingerprint(md)
    data = entry.model_dump(mode="json")
    data["verification"] = {"verified_on": "2026-10-06", **fp, **over}
    return CatalogEntry.model_validate(data)


class _Search:
    def __init__(self, entries, listed=None):
        self.entries = {e.subject: e for e in entries}
        self.listed = listed

    def get_subject(self, index, subject):
        e = self.entries.get(subject)
        return {"entries": [{"content": e.model_dump(mode="json")}]} if e else {"entries": []}

    def post_search(self, index, q):
        subjects = self.listed if self.listed is not None else list(self.entries)
        return {"gmeta": [{"subject": s} for s in subjects]}


class _Compute:
    def __init__(self, md=_MD, status="online"):
        self.md, self.status = md, status

    def get_endpoint_status(self, eid):
        return {"status": self.status}

    def get_endpoint_metadata(self, eid):
        return self.md


def test_the_fingerprint_is_stable_and_ignores_what_does_not_matter():
    a = facility_fingerprint(_MD)
    assert a == facility_fingerprint(dict(_MD, hostname="elsewhere"))
    assert a["template_sha256"] != facility_fingerprint(dict(_MD, user_config_template="engine: {x: 1}\n"))["template_sha256"]
    assert a["template_sha256"] != facility_fingerprint(dict(_MD, user_config_schema={"type": "array"}))["template_sha256"]
    assert facility_fingerprint({})["template_sha256"] is None


def test_the_index_must_serve_the_seeds_and_nothing_else():
    seeds = _seeds()
    assert all(f.level == "ok" for f in health.check_index(seeds, _Search(seeds), "idx"))
    stale = [s if s.id != "delta" else CatalogEntry.model_validate({**s.model_dump(mode="json"),
                                                                   "description": "old text"}) for s in seeds]
    findings = health.check_index(seeds, _Search(stale), "idx")
    bad = [f for f in findings if f.level == "fail"]
    assert len(bad) == 1 and bad[0].entry == "delta" and "description" in bad[0].detail
    missing = health.check_index(seeds, _Search([s for s in seeds if s.id != "anvil"]), "idx")
    assert any(f.entry == "anvil" and "missing" in f.detail for f in missing)
    extra = health.check_index(seeds, _Search(seeds, listed=[s.subject for s in seeds] + ["purdue:old"]), "idx")
    assert any(f.level == "warn" and "purdue:old" in f.detail for f in extra)


def test_a_facility_matching_its_verification_is_ok_and_drift_fails():
    delta = _verified(_entry("delta"))
    assert [f.level for f in health.check_facility(delta, _Compute())] == ["ok"]
    for change, word in ((dict(endpoint_version="4.17.0"), "version"), (dict(python_version="3.13.14"), "python"),
                         (dict(user_config_template="engine: {y: 2}\n"), "template")):
        findings = health.check_facility(delta, _Compute(dict(_MD, **change)))
        assert any(f.level == "fail" and f.detail.startswith(word) for f in findings), change
    cfg = health.check_facility(delta, _Compute(dict(_MD, endpoint_config="other\n")))
    assert [f.level for f in cfg] == ["warn"]
    assert health.check_facility(delta, _Compute(status="offline"))[0].detail.startswith("offline")


def _unbaselined(eid):
    return CatalogEntry.model_validate({**_entry(eid).model_dump(mode="json"), "verification": None})


def test_an_unbaselined_facility_warns_and_an_ssh_entry_has_no_facility_check():
    findings = health.check_facility(_unbaselined("anvil"), _Compute())
    assert [f.level for f in findings] == ["warn"] and "unbaselined" in findings[0].detail
    assert health.check_facility(_entry("expanse"), _Compute()) == []


def test_ssh_reachability():
    class _Sock:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    ok = health.check_ssh(_entry("expanse"), connect=lambda addr, timeout: _Sock())
    assert ok[0].level == "ok"

    def refuse(addr, timeout):
        raise ConnectionRefusedError("refused")

    assert health.check_ssh(_entry("expanse"), connect=refuse)[0].level == "fail"
    assert health.check_ssh(_entry("delta")) == []


def test_a_parsl_release_after_a_float_entry_was_proven_warns():
    pypi = {"releases": {"2026.10.5": [{"upload_time_iso_8601": "2026-10-05T22:45:00Z"}],
                         "2026.10.12": [{"upload_time_iso_8601": "2026-10-12T22:45:00Z"}]}}
    anvil = CatalogEntry.model_validate({**_unbaselined("anvil").model_dump(mode="json"), "last_validated": "2026-10-06"})
    findings = health.check_releases([anvil, _entry("delta")], pypi=lambda pkg: pypi)
    assert [f.entry for f in findings] == ["anvil"]  # delta pins parsl: a release does not move its worker
    assert findings[0].level == "warn" and "2026.10.12" in findings[0].detail
    later = CatalogEntry.model_validate({**anvil.model_dump(mode="json"), "last_validated": "2026-10-13"})
    assert health.check_releases([later], pypi=lambda pkg: pypi)[0].level == "ok"


def test_a_fresh_install_that_leaves_the_lock_fails_on_the_sdk():
    lock = ('version = 1\n[[package]]\nname = "globus-compute-sdk"\nversion = "4.16.0"\n'
            '[[package]]\nname = "dill"\nversion = "0.3.9"\n[[package]]\nname = "mcp"\nversion = "1.29.1"\n')

    def raw(ref, path):
        return lock if path == "uv.lock" else "[project]\nname='x'\n"

    same = health.check_install(raw=raw, pythons=("3.13",),
                                fresh=lambda pp, py, names: {"globus-compute-sdk": "4.16.0", "dill": "0.3.9"})
    assert [f.level for f in same] == ["ok"]
    moved = health.check_install(raw=raw, pythons=("3.13",),
                                 fresh=lambda pp, py, names: {"globus-compute-sdk": "4.18.0", "dill": "0.3.9",
                                                              "mcp": "1.30.0"})
    levels = {f.detail.split(":")[0]: f.level for f in moved}
    assert levels == {"globus-compute-sdk": "fail", "mcp": "warn"}


def test_alerts_fire_only_for_new_findings(tmp_path):
    state = tmp_path / "state.json"
    f1 = [health.Finding("delta", "facility", "fail", "version: x"), health.Finding("anvil", "index", "ok", "fine")]
    assert [f.entry for f in health.changes(f1, state)] == ["delta"]
    assert health.changes(f1, state) == []  # unchanged: no repeat alert
    f2 = f1 + [health.Finding("*", "install", "fail", "globus-compute-sdk: moved")]
    assert [f.check for f in health.changes(f2, state)] == ["install"]


def test_a_garbled_verification_block_never_drops_the_entry():
    delta = _entry("delta").model_dump(mode="json")
    for garble in ({"endpoint_version": "4.16; rm -rf ~", "template_sha256": "zz"}, ["x"], "nonsense"):
        e = CatalogEntry.model_validate({**delta, "verification": garble})
        assert e.id == "delta"
        assert e.verification is None or (e.verification.endpoint_version is None
                                          and e.verification.template_sha256 is None)


def test_the_seeds_parse_and_the_cli_reports(monkeypatch, capsys):
    monkeypatch.setattr(health, "run_checks", lambda seeds, **kw: [health.Finding("x", "ssh", "warn", "w: y")])
    assert health.main(["--skip", "index,facility"]) == 1
    assert "WARN" in capsys.readouterr().out
    assert yaml.safe_load((SEEDS / "ncsa-delta.yaml").read_text())  # sanity: seeds readable


@pytest.mark.parametrize("level,code", [("ok", 0), ("warn", 1), ("fail", 2)])
def test_exit_codes_follow_the_worst_finding(monkeypatch, level, code):
    monkeypatch.setattr(health, "run_checks", lambda seeds, **kw: [health.Finding("x", "ssh", level, "d: e")])
    assert health.main(["--skip", "index,facility"]) == code


def _reprove_module():
    import importlib.util

    path = Path(health.__file__).resolve().parents[3] / "agentic" / "registry_reprove.py"
    spec = importlib.util.spec_from_file_location("registry_reprove", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("seed,eid", [("ncsa-delta.yaml", "delta"), ("anvil.yaml", "anvil"),
                                      ("globus-cluster.yaml", "globus-labs")])
def test_recording_a_verification_edits_only_that_entry_and_keeps_comments(seed, eid):
    import datetime

    rp = _reprove_module()
    text = (SEEDS / seed).read_text()
    block = {**facility_fingerprint(dict(_MD, endpoint_version="4.18.0")), "worker": "py3.13.12 dill0.3.9 parsl2026.8.10 on a000"}
    on = datetime.date(2026, 10, 9)
    once = rp.record_verification(text, eid, block, on)
    twice = rp.record_verification(once, eid, dict(block, worker="py3.13.12 dill0.3.9 parsl2026.8.10 on a001"), on)
    for out in (once, twice):
        entry = next(e for e in BundledCatalog(_write(out, seed)).entries() if e.id == eid)
        assert entry.last_validated == on and entry.verification.verified_on == on
        assert entry.verification.endpoint_version == "4.18.0" and entry.verification.template_sha256
        assert entry.compute.worker_env.verified_with == "4.18.0"
    assert twice.count("verification:") == 1 and "on a001" in twice and "on a000" not in twice
    # every comment line of the original survives
    assert [ln for ln in text.splitlines() if ln.lstrip().startswith("#")] == \
           [ln for ln in twice.splitlines() if ln.lstrip().startswith("#")]


def _write(text, name):
    import tempfile

    d = Path(tempfile.mkdtemp())
    (d / name).write_text(text)
    return d / name


def test_ingest_refuses_a_verification_block_the_client_would_drop(tmp_path):
    from hpc_bridge.catalog.entry import verification_raw_problems
    from hpc_bridge.catalog.ingest import ingest

    assert verification_raw_problems({"verified_on": "2026-10-09", "endpoint_version": "4.18.0"}) == []
    assert verification_raw_problems({"endpoint_version": 4.18})  # an unquoted YAML float
    assert verification_raw_problems({"template_sha256": "abc"})  # not a digest
    assert verification_raw_problems({True: "2026-10-09"})  # a bare `on:` key, read by YAML 1.1 as true
    assert verification_raw_problems(["x"])

    class _Search:
        def ingest(self, index, doc):
            raise AssertionError("nothing must reach the index")

    rows = yaml.safe_load((SEEDS / "ncsa-delta.yaml").read_text())
    rows[0]["verification"] = {"verified_on": "2026-10-09", "endpoint_version": 4.16}
    seed = tmp_path / "seed.yaml"
    seed.write_text(yaml.safe_dump(rows))
    with pytest.raises(ValueError, match="verification"):
        ingest("idx", seed, _Search())
