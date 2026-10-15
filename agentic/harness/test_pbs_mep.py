"""Hermetic: the `pbs_mep` fake-cluster profile (a Polaris-shaped facility MEP on the OpenPBS stack) and its scenario
`fake_pbs_mep_compute`.

No docker: the profile is checked as files (manifest, merged dir, compose overlay, the manager's fixtures), its catalog
script is run against a fake endpoint.json, its template is rendered the way globus-compute-endpoint's manager renders
it (schema check, json-quoted user strings, a sandboxed StrictUndefined Jinja), and the scenario's graders run on a
trace made of the PRODUCT's own answers: connect -> confirm -> run -> stop driven through hpc-bridge's real server paths
against that catalog entry and template, with a fake Globus client and runner standing in for the service and the
block. What only the live cluster can show (the manager starts, PBS runs the pilot, the workers reach the interchange)
is left to the fake-cluster run (agentic/fakecluster/README.md)."""
from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402

FC = HERE.parent / "fakecluster"
PROF = FC / "profiles" / "pbs_mep"
MEPDIR = PROF / "mep"
SCEN = HERE.parent / "scenarios"
EID = "0f1e2d3c-4b5a-4968-8776-655443322110"
JOB = "7.pbsserver"


def _profile_mod():
    import importlib.util
    spec = importlib.util.spec_from_file_location("hpcb_profile", FC / "bin" / "profile.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --- the profile ----------------------------------------------------------------------------------------------------

def test_pbs_mep_layers_on_polaris_and_offers_a_pbs_facility_endpoint():
    m = targets.load_profile("pbs_mep")
    assert m["layers"] == ["pbs", "polaris", "pbs_mep"] and m["compose"] == "docker-compose.pbs.yml"
    caps = m["capabilities"]
    assert caps["scheduler"] == "pbs" and caps["site_rule"] == "filesystems" and caps["nodes"] == 2   # inherited
    assert caps["mep"] == "consent-free" and caps["mep_schemas"] == ["pbs-strict"] and caps["mep_mapped_user"] == "hpcbmep"
    assert caps["catalog"] == "local"                       # derived from [catalog] cmd: the registry is replaced
    assert m["catalog"]["cmd"] == "docker exec hpcb-fake-login-1 /usr/local/bin/hpcb-pbs-mep-catalog"
    assert caps["accounting"] == "none"                     # -A accepted, not validated (the template is the floor)


def test_build_materialises_the_mep_fixtures_on_the_pbs_stack(tmp_path):
    out = tmp_path / "pbs_mep"
    _profile_mod().build("pbs_mep", out)
    for rel in ("deregister.sh", "mep/hpcb-pbs-mep-catalog", "mep/tools.sh", "mep/idmap.py", "mep/pbs_config.yaml",
                "mep/pbs_user_config_template.yaml.j2", "mep/pbs_user_config_schema.json",
                "setup.d/server.sh", "setup.d/server-polaris.sh", "polaris/require_filesystems.py",   # the base layers
                "setup.d/server-pbsmep.sh", "setup.d/mom-pbsmep.sh", "setup.d/login-pbsmep.sh"):
        assert (out / rel).is_file(), rel
    assert os.access(out / "deregister.sh", os.X_OK) and os.access(out / "mep" / "hpcb-pbs-mep-catalog", os.X_OK)
    assert not (out / "compose.override.yml").exists()
    r = subprocess.run([sys.executable, str(FC / "bin" / "profile.py"), "build", "pbs_mep", str(tmp_path / "cli")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    assert "PROFILE_COMPOSE=docker-compose.pbs.yml" in r.stdout and "PROFILE_SCHEDULER=pbs" in r.stdout
    assert "PROFILE_CATALOG_CMD='docker exec hpcb-fake-login-1 /usr/local/bin/hpcb-pbs-mep-catalog'" in r.stdout
    overlays = re.search(r"^PROFILE_OVERLAYS=(.*)$", r.stdout, re.M).group(1)
    assert overlays.strip("'").endswith("profiles/pbs_mep/compose.override.yml")   # pbs and polaris have none


def test_every_setup_script_parses_and_none_can_stop_the_entrypoint():
    # the entrypoint SOURCES these under `set -euo pipefail`: an unguarded failing step would take the node down
    for f in sorted((PROF / "setup.d").glob("*.sh")) + [PROF / "deregister.sh"]:
        assert subprocess.run(["bash", "-n", str(f)], capture_output=True, timeout=30).returncode == 0, f
    login = (PROF / "setup.d" / "login-pbsmep.sh").read_text()
    assert "return 0" in login and "exit" not in re.sub(r"#.*", "", login)   # sourced: return, never exit


def test_the_shared_mep_tooling_is_the_mep_profiles_byte_for_byte():
    # pbs_mep cannot layer on `mep` (that is the Slurm stack) until a profile may have several bases: the two helpers it
    # shares are copies, kept identical here so a fix to one (#189's --clear) reaches both
    for rel in ("tools.sh", "idmap.py"):
        assert (MEPDIR / rel).read_bytes() == (FC / "profiles" / "mep" / "mep" / rel).read_bytes(), rel
    body = lambda s: re.search(r"-c '\n(.*?)'\n?$", s, re.S).group(1)   # noqa: E731 - the one-off container's script
    ours, theirs = (PROF / "deregister.sh").read_text(), (FC / "profiles" / "mep" / "deregister.sh").read_text()
    assert body(ours) == body(theirs)
    assert '"${PROJECT}_pbs-mep-state:/root/.globus_compute"' in ours and '"${PROJECT}_pbs-mep-tools:/opt/hpcb-mep"' in ours
    assert "hpcb-fake-pbs:latest" in ours and "_pbs-mep-state\" >/dev/null" in ours


def test_the_compose_overlay_gives_the_moms_the_facility_install_read_only():
    ov = yaml.safe_load((PROF / "compose.override.yml").read_text())
    base = yaml.safe_load((FC / "docker-compose.pbs.yml").read_text())
    assert set(ov["services"]) <= set(base["services"]) == {"pbsserver", "c1", "c2", "login"}   # no new services
    login = ov["services"]["login"]
    assert "pbs-mep-state:/root/.globus_compute" in login["volumes"] and "pbs-mep-tools:/opt/hpcb-mep" in login["volumes"]
    db = next(v for v in login["volumes"] if isinstance(v, dict))
    assert db["target"] == "/run/hpcb/mep-storage.db" and db["read_only"] is True
    assert {"HPCB_MEP_IDENTITY", "HPCB_MEP_GCE_VERSION", "HPCB_MEP_EMAIL"} <= set(login["environment"])
    for mom in ("c1", "c2"):
        assert ov["services"][mom]["volumes"] == ["pbs-mep-tools:/opt/hpcb-mep:ro"], mom
    # its own volumes: the Slurm `mep` profile's are the same compose project's, and must never be adopted
    mep = yaml.safe_load((FC / "profiles" / "mep" / "compose.override.yml").read_text())
    assert set(ov["volumes"]) == {"pbs-mep-state", "pbs-mep-tools"} and not set(ov["volumes"]) & set(mep["volumes"])


def test_the_manager_fixtures_name_one_manager_and_one_install():
    name = "hpcb-mep-pbs"
    catalog = (MEPDIR / "hpcb-pbs-mep-catalog").read_text()
    login = (PROF / "setup.d" / "login-pbsmep.sh").read_text()
    assert f'NAME = "{name}"' in catalog and f"name={name};" in login
    assert f"/root/.globus_compute/{name}/identity_mapping.json" in (MEPDIR / "pbs_config.yaml").read_text()
    assert "for d in /root/.globus_compute/hpcb-mep-*/" in (PROF / "deregister.sh").read_text()   # finds it
    # the manager's dir gets the template + schema under the names globus-compute-endpoint reads
    assert '"$PROFILE_DIR/mep/pbs_user_config_template.yaml.j2" "$d/user_config_template.yaml.j2"' in login
    assert '"$PROFILE_DIR/mep/pbs_user_config_schema.json" "$d/user_config_schema.json"' in login
    # the workers run the manager's own install: the template's PATH entry is tools.sh's venv, on the volume the moms mount
    tools = (MEPDIR / "tools.sh").read_text()
    assert "MEP_TOOLS=/opt/hpcb-mep" in tools and 'MEP_GCE="$MEP_TOOLS/gce"' in tools
    assert "export PATH=$PATH:/opt/hpcb-mep/gce/bin" in (MEPDIR / "pbs_user_config_template.yaml.j2").read_text()
    # the mapped account resolves everywhere with one uid, and pool users can see its jobs (the world checks)
    for role in ("server", "mom", "login"):
        assert "-u 2100 -g hpcb" in (PROF / "setup.d" / f"{role}-pbsmep.sh").read_text(), role
    assert "set server query_other_jobs = true" in (PROF / "setup.d" / "server-pbsmep.sh").read_text()


# --- the local catalog ----------------------------------------------------------------------------------------------

def _catalog(tmp_path) -> Path:
    state = tmp_path / "state"
    (state / "hpcb-mep-pbs").mkdir(parents=True)
    (state / "hpcb-mep-pbs" / "endpoint.json").write_text(json.dumps({"endpoint_id": EID}))
    r = subprocess.run([sys.executable, str(MEPDIR / "hpcb-pbs-mep-catalog")], capture_output=True, text=True,
                       timeout=30, env={**os.environ, "HPCB_MEP_STATE_DIR": str(state)})
    assert r.returncode == 0, r.stderr
    out = tmp_path / "catalog.yaml"
    out.write_text(r.stdout)
    return out


def test_the_catalog_waits_for_the_registration(tmp_path):
    r = subprocess.run([sys.executable, str(MEPDIR / "hpcb-pbs-mep-catalog")], capture_output=True, text=True,
                       timeout=30, env={**os.environ, "HPCB_MEP_STATE_DIR": str(tmp_path)})
    assert r.returncode == 1 and r.stdout == "" and "endpoint.json" in r.stderr   # run_smoke / the readiness wait retry


def test_the_catalog_entry_has_the_polaris_seeds_shape(tmp_path):
    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.catalog.entry import account_hint_raw_problems, defaults_extra_raw_problems
    from hpc_bridge.facility.mep import MEPFacility

    path = _catalog(tmp_path)
    cat = BundledCatalog(path)                              # the plugin's own loader (the HPC_BRIDGE_CATALOG_FILE seam)
    e = asyncio.run(cat.get("fake-pbs-mep"))
    assert e is not None and asyncio.run(cat.get("hpcb-mep-pbs")) is e   # the manager's name is an alias
    polaris = yaml.safe_load((HERE.parents[1] / "src/hpc_bridge/catalog/seed/alcf-polaris.yaml").read_text())[0]
    raw = yaml.safe_load(path.read_text())[0]
    # the same contract fields as the real entry (no verification: a fake is never proven against the registry)
    assert set(polaris) - {"verification"} == set(raw) and set(polaris["compute"]) == set(raw["compute"])
    assert raw["compute"]["key_map"] == polaris["compute"]["key_map"] == {"partition": "queue"}
    assert raw["compute"]["env_setup"] == polaris["compute"]["env_setup"] == ""       # the template's venv, no pin
    assert e.compute.scheduler == "pbs" and e.compute_mep_uuid == EID and e.ssh_host is None and e.account_required
    assert not account_hint_raw_problems(raw["account_hint"]) and not defaults_extra_raw_problems(raw)
    caps = targets.load_profile("pbs_mep")["capabilities"]
    assert e.defaults.partition in caps["partitions"] and e.defaults.walltime <= "00:30:00"   # inside debug's cap
    for acct in caps["accounts"]:   # the agent must ask for the account, or be told it: the catalog never names it
        assert acct not in f"{e.description} {e.account_hint} {e.display_name}"
    fac = MEPFacility.from_entry(e, client_factory=lambda: None)
    assert fac.scheduler == "pbs" and fac.key_map == {"partition": "queue"} and fac.account_required
    assert "worker_init" not in fac.config_template(None)[1]   # nothing of ours: the facility's own worker_init runs


# --- the template, rendered as the manager renders it ---------------------------------------------------------------

def _render(user_opts: dict) -> dict:
    """globus-compute-endpoint's render_config_user_template, mirrored: validate the client's options against the
    schema, json.dumps every string (`_sanitize_user_json`), render in a sandbox with StrictUndefined, load the YAML."""
    import jinja2
    import jsonschema
    from jinja2.sandbox import SandboxedEnvironment

    jsonschema.validate(instance=user_opts, schema=json.loads((MEPDIR / "pbs_user_config_schema.json").read_text()))
    opts = {k: json.dumps(v) if isinstance(v, str) else ("null" if v is None else v) for k, v in user_opts.items()}
    env = SandboxedEnvironment(undefined=jinja2.StrictUndefined)
    return yaml.safe_load(env.from_string((MEPDIR / "pbs_user_config_template.yaml.j2").read_text()).render(**opts))


def _facility(tmp_path):
    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.facility.mep import MEPFacility

    e = asyncio.run(BundledCatalog(_catalog(tmp_path)).get("fake-pbs-mep"))
    fac = MEPFacility.from_entry(e, account="hpcb", client_factory=lambda: None)
    fac.schema = json.loads((MEPDIR / "pbs_user_config_schema.json").read_text())
    return fac


def test_the_template_takes_what_the_plugin_sends_and_renders_polaris_shaped(tmp_path):
    from hpc_bridge.facility.mep import _template_idle_s

    fac = _facility(tmp_path)
    uec = fac.sanitize_uec({**fac.config_template(None)[1], "compute": True})
    assert "partition" in uec and "interface" not in uec   # the key_map source survives the strict filter
    assert fac.template_notes == ["keys not in the facility's template schema were dropped: interface"]
    wire = fac.dispatch_uec(uec)
    assert wire["queue"] == "debug" and wire["account"] == "hpcb" and not {"partition", "compute", "interface"} & set(wire)
    p = _render(wire)["engine"]["provider"]
    assert p["type"] == "PBSProProvider" and p["queue"] == "debug" and p["account"] == "hpcb"
    assert p["scheduler_options"] == "#PBS -l filesystems=home"            # the polaris rule is satisfied by default
    assert p["worker_init"] == "export TMPDIR=/tmp; export PATH=$PATH:/opt/hpcb-mep/gce/bin"
    assert p["launcher"] == {"type": "SimpleLauncher"} and p["walltime"] == "00:15:00" and p["init_blocks"] == 1
    eng = _render(wire)["engine"]
    assert eng["job_status_kwargs"]["max_idletime"] == 240 and eng["address"] == {"type": "address_by_interface", "ifname": "eth0"}
    assert _template_idle_s((MEPDIR / "pbs_user_config_template.yaml.j2").read_text()) == 240   # what the plugin tells


def test_the_template_refuses_what_polaris_requires_and_keeps_polaris_traps():
    import jsonschema

    good = {"queue": "debug", "account": "hpcb"}
    for bad in ({"account": "hpcb"}, {"queue": "debug"}, {**good, "partition": "debug"}, {**good, "interface": "eth0"},
                {**good, "compute": True}, {**good, "account": ""}):
        with pytest.raises(jsonschema.ValidationError):
            _render(bad)
    # a client's scheduler_options REPLACES the filesystems default (re-include it, or the polaris rule holds the pilot)
    assert _render({**good, "scheduler_options": "#PBS -l place=scatter"})["engine"]["provider"]["scheduler_options"] \
        == "#PBS -l place=scatter"
    # a client's worker_init is PREPENDED to the facility's own, quotes and all
    wi = _render({**good, "worker_init": 'module load x; echo "hi"'})["engine"]["provider"]["worker_init"]
    assert wi == 'module load x; echo "hi"; export TMPDIR=/tmp; export PATH=$PATH:/opt/hpcb-mep/gce/bin'


# --- the scenario: wiring -------------------------------------------------------------------------------------------

def test_the_scenario_runs_only_on_pbs_mep_and_only_it_does_there():
    import importlib

    import fake_pbs_mep_compute as sc
    caps = {p: targets.load_profile(p)["capabilities"] for p in ("pbs_mep", "mep", "polaris", "pbs", "site", "default")}
    assert sc.TARGETS == ("fake",) and targets.meets(sc.REQUIRES, caps["pbs_mep"])[0]
    for p in ("mep", "polaris", "pbs", "site", "default"):
        assert not targets.meets(sc.REQUIRES, caps[p])[0], p
    assert not targets.meets(sc.REQUIRES, targets.GLOBUS1_CAPABILITIES)[0]
    # every scenario that needs a facility MEP runs only where its catalog entry exists: the PBS one on pbs_mep, the
    # Slurm ones (fake-mep-strict / fake-mep-open) on mep — never crossed
    for p in sorted(SCEN.glob("*.py")):
        mod = importlib.import_module(p.stem)
        req = getattr(mod, "REQUIRES", None) or {}
        if "mep" not in req or "fake" not in (getattr(mod, "TARGETS", None) or ("fake",)):
            continue
        fid = str(getattr(mod, "FACILITY_ID", ""))
        if targets.meets(req, caps["pbs_mep"])[0]:
            assert fid == "fake-pbs-mep", (p.stem, fid)
        if targets.meets(req, caps["mep"])[0]:
            assert fid.startswith("fake-mep-"), (p.stem, fid)


def test_the_scenario_gates_only_graders_it_has_and_reads_the_profile():
    import fake_pbs_mep_compute as sc
    provided = {r.name for r in check_all(Trace([]))} | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
    assert set(sc.EXPECT_OK) <= provided, set(sc.EXPECT_OK) - provided
    assert sc.FACILITY_ID == "fake-pbs-mep" and sc.MAPPED_USER == "hpcbmep" and sc.SERIAL == "hpcbmep"
    caps = targets.load_profile("pbs_mep")["capabilities"]
    assert caps["mep_mapped_user"] == sc.MAPPED_USER and sc.DEFAULT_QUEUE in caps["partitions"]
    assert sc.ACCOUNT in caps["accounts"] and sc.WARM_BLOCK_USER == sc.MAPPED_USER
    # the world checks: the stop leaves the block running in debug charged to hpcb; past the idle window it is gone
    (hook,) = sc.MIDRUN_HOOKS
    assert hook["after_tool"] == "stop_endpoint" and hook["nth"] == 1 and hook["expect_any_host"] == "BLOCK_AT_STOP:"
    assert "qselect -u hpcbmep -q debug -A hpcb -s R" in hook["cmd"]   # the queue and the account reached PBS
    from hpc_bridge.facility.mep import _template_idle_s
    idle = _template_idle_s((MEPDIR / "pbs_user_config_template.yaml.j2").read_text())
    assert idle == 240 and idle + 30 <= sc.POSTCHECK_DELAY_S, (sc.POSTCHECK_DELAY_S, idle)   # 240 s + a margin
    (pc,) = sc.POSTCHECKS
    assert "qstat -u hpcbmep -w" in pc["cmd"] and pc["expect_absent"] == "parsl" and "exit 3" in pc["cmd"]
    assert sc.ADMIN_CLEANUP == ['ids=$(qselect -u hpcbmep 2>/dev/null); [ -z "$ids" ] || qdel $ids 2>/dev/null; true']


def _pbs_stubs(tmp_path, jobs):
    """A login node's `qselect` / `qstat` answering from `jobs` ({id: (state, name, finished_s_ago)}): qselect honours
    -s <states> and -x (finished jobs only with -x), qstat -f / -x -f print the records with PBS's ctime-style mtime."""
    import sys
    d = tmp_path / "bin"
    d.mkdir(parents=True)
    state = tmp_path / "jobs.json"
    state.write_text(json.dumps({k: [st, name, ago] for k, (st, name, ago) in jobs.items()}))
    py = sys.executable
    (d / "qselect").write_text(f"""#!{py}
import json, sys
a = sys.argv[1:]; jobs = json.load(open({str(state)!r}))
want = a[a.index('-s') + 1] if '-s' in a else 'QRHEF'
for j, (st, _n, _ago) in sorted(jobs.items(), key=lambda kv: int(kv[0].split('.')[0])):
    if st in want and (st != 'F' or '-x' in a):
        print(j)
""")
    (d / "qstat").write_text(f"""#!{py}
import json, sys, time
jobs = json.load(open({str(state)!r}))
for j in [x for x in sys.argv[1:] if not x.startswith('-')]:
    st, name, ago = jobs[j]
    if st == 'F' and '-x' not in sys.argv:
        continue
    print(f"Job Id: {{j}}\\n    Job_Name = {{name}}\\n    job_state = {{st}}\\n    queue = debug\\n    Account_Name = hpcb\\n"
          f"    mtime = {{time.ctime(time.time() - ago)}}")
""")
    for f in ("qselect", "qstat"):
        (d / f).chmod(0o755)
    return {**os.environ, "PATH": f"{d}:{os.environ['PATH']}"}


@pytest.mark.parametrize("jobs, passes, says", [
    ({"9.pbsserver": ("R", "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.17", 0)}, True, "still running"),
    # the operator waited past the idle window: the block idled out before the stop — not the plugin's doing
    ({"9.pbsserver": ("F", "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.17", 120),
      "3.pbsserver": ("F", "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.16", 4000)}, True, "premise not met"),
    # it ended at the stop (a cancel the stop must not issue): finished seconds ago, or exiting right now
    ({"9.pbsserver": ("F", "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.17", 5),
      "3.pbsserver": ("F", "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.16", 4000)}, False, "at the stop"),
    ({"9.pbsserver": ("E", "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.17", 0)}, False, "EXITING"),
    ({}, False, "no parsl job"),
    ({"9.pbsserver": ("F", "hpcb-handjob", 120)}, False, "not a parsl block"),   # not a block of ours
])
def test_the_stop_time_hook_tells_a_block_left_running_from_one_ended_at_the_stop(tmp_path, jobs, passes, says):
    import fake_pbs_mep_compute as sc
    (hook,) = sc.MIDRUN_HOOKS
    out = subprocess.run(["bash", "-c", hook["cmd"]], env=_pbs_stubs(tmp_path, jobs), capture_output=True, text=True,
                         timeout=30)
    assert out.returncode == 0, out.stderr
    assert (hook["expect_any_host"] in out.stdout) is passes and says in out.stdout, out.stdout


def test_the_scenario_knobs_and_the_pbs_admin_channel(monkeypatch):
    r = subprocess.run([sys.executable, str(HERE / "scenario_knobs.py"), "fake_pbs_mep_compute"], capture_output=True,
                       text=True, timeout=60)
    kv = dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)
    assert kv["HPCB_KNOB_SERIAL"] == "1" and kv["HPCB_KNOB_SERIAL_GROUP"] == "hpcbmep" and kv["HPCB_KNOB_NEEDS_NODE"] == "1" and kv["HPCB_KNOB_WARM_BLOCK_USER"] == "hpcbmep"
    assert kv["HPCB_KNOB_TARGETS"] == "fake" and "HPCB_KNOB_ADMIN_CLEANUP" in kv and '"scheduler": "pbs"' in kv["HPCB_KNOB_REQUIRES"]
    # ADMIN_CLEANUP runs as root on the scheduler's controller: the PBS server on a PBS profile, slurmctld otherwise
    monkeypatch.delenv("HPCB_FAKE_CTLD", raising=False)
    monkeypatch.delenv("HPCB_FAKE_PBS_SERVER", raising=False)
    for prof, ctld in (("pbs_mep", "hpcb-fake-pbsserver-1"), ("polaris", "hpcb-fake-pbsserver-1"),
                       ("mep", "hpcb-fake-slurmctld-1"), ("default", "hpcb-fake-slurmctld-1")):
        monkeypatch.setenv("HPCB_FAKE_PROFILE", prof)
        assert targets.get("fake").admin_argv == ("docker", "exec", ctld, "bash", "-lc"), prof
    # each override applies on its own stack only: a Slurm controller's name never reaches a PBS profile's qdel
    monkeypatch.setenv("HPCB_FAKE_CTLD", "other-ctld")
    monkeypatch.setenv("HPCB_FAKE_PBS_SERVER", "other-pbs")
    for prof, ctld in (("default", "other-ctld"), ("mep", "other-ctld"), ("pbs_mep", "other-pbs"), ("pbs", "other-pbs")):
        monkeypatch.setenv("HPCB_FAKE_PROFILE", prof)
        assert targets.get("fake").admin_argv[2] == ctld, prof


def test_the_node_gate_reads_a_pbs_warm_block_with_qselect(monkeypatch):
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_suite_pbs", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setenv("HPCB_FAKE_PROFILE", "pbs_mep")
    monkeypatch.setattr(mod, "_TARGET", targets.get("fake"))
    seen = []
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: (seen.append(remote), "7.pbsserver\n")[1])
    assert mod._warm_block_running("hpcbmep") is True and seen == ["qselect -u hpcbmep -s R"]
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: "")
    assert mod._warm_block_running("hpcbmep") is False
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: None)
    assert mod._warm_block_running("hpcbmep") is None
    monkeypatch.setenv("HPCB_FAKE_PROFILE", "mep")
    monkeypatch.setattr(mod, "_TARGET", targets.get("fake"))
    monkeypatch.setattr(mod, "_probe_ssh", lambda remote: (seen.append(remote), "")[1])
    mod._warm_block_running("hpcbmep")
    assert seen[-1].startswith("squeue -u hpcbmep")


# --- the scenario's graders, on the product's own answers -----------------------------------------------------------

class _Client:
    """The Globus Compute service as the attach sees it: the manager online, and its published template + schema."""

    def get_endpoint_status(self, endpoint_id):
        return {"status": "online"}

    def get_endpoint_metadata(self, endpoint_id):
        return {"endpoint_version": "4.18.0", "python_version": "3.11.13", "display_name": "hpcb-mep-pbs",
                "user_config_template": (MEPDIR / "pbs_user_config_template.yaml.j2").read_text(),
                "user_config_schema": json.loads((MEPDIR / "pbs_user_config_schema.json").read_text())}


class _Res:
    def __init__(self, out):
        self.returncode, self.stdout, self.stderr = 0, out, ""


class _Done:
    def __init__(self, res):
        self._res = res

    def done(self):
        return True

    def add_done_callback(self, fn):
        fn(self)

    def cancelled(self):
        return False

    def result(self, timeout=None):
        return self._res


class _Runner:
    """The block: a worker in PBS job 7.pbsserver, as hpcbmep, whose command prints what PBS would."""

    def __init__(self, endpoint_id, stdout):
        from hpc_bridge.runner import CanaryResult
        self.endpoint_id, self.walltime, self.timeout, self._out = endpoint_id, 900.0, 120.0, stdout
        self._canary = CanaryResult(ok=True, worker_host="c1", worker_python="3.11.13", worker_dill="0.3.9",
                                    worker_job=JOB, worker_user="hpcbmep", answered_at=time.monotonic())

    def submit(self, command):
        return _Done(_Res(self._out))

    async def run(self, command):
        return _Res(self._out)

    async def canary(self, timeout=8.0):
        return self._canary

    def close(self):
        pass


def _call(name, inp, res):
    return ToolCall.of(f"mcp__endpoint__{name}", inp, res.model_dump(mode="json"))


async def _drive(tmp_path, monkeypatch, *, stdout=f"JOB={JOB} QUEUE=debug\nhpcbmep\nc1\n", partition=None):
    """connect -> a confirmed start on the default queue charged to hpcb -> one run -> stop, through hpc-bridge's
    real server paths against the profile's catalog entry (the HPC_BRIDGE_CATALOG_FILE seam) and template. Returns the
    trace of the tools' answers and the config each runner was built with (what went over the wire)."""
    from hpc_bridge import binding, server
    from hpc_bridge.context import AppCtx
    from hpc_bridge.facility.mep import MEPFacility
    from hpc_bridge.profile import Profile

    for var in ("HPC_BRIDGE_MACHINE", "HPC_BRIDGE_ACCOUNT", "HPC_BRIDGE_PARTITION", "HPC_BRIDGE_SEARCH_INDEX",
                "HPC_BRIDGE_ENDPOINT_ID", "HPC_BRIDGE_SSH_HOST"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HPC_BRIDGE_STATE_DIR", str(tmp_path / "state-dir"))
    monkeypatch.setenv("CLAUDE_PLUGIN_DATA", str(tmp_path / "plugin-data"))
    monkeypatch.setenv("HPC_BRIDGE_CATALOG_FILE", str(_catalog(tmp_path)))
    monkeypatch.setattr(binding, "_PINNED", {})
    monkeypatch.setattr(binding, "_PINNED_ORIGIN", {})
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account, pinned_host=None:
                        MEPFacility.from_entry(e, account=account or None, client_factory=_Client))
    app = AppCtx(facility=None, profile=Profile())
    wire = []

    def factory(eid, user_endpoint_config=None, **_kw):
        wire.append(dict(user_endpoint_config or {}))
        return _Runner(eid, stdout)

    app.runner_factory = factory
    calls = []
    res = await server._connect_facility(app, "fake-pbs-mep")
    calls.append(_call("connect_facility", {"facility": "fake-pbs-mep"}, res))
    start = {"shape": "compute", "account": "hpcb", "confirm_spend": True, **({"partition": partition} if partition else {})}
    res = await server._ensure_endpoint_up(app, **start)
    calls.append(_call("ensure_endpoint_up", start, res))
    cmd = "echo JOB=$PBS_JOBID QUEUE=$PBS_QUEUE; whoami; hostname"
    res = await server._run_shell(app, cmd, shape="compute", agent_call=True)
    calls.append(_call("run_shell", {"shape": "compute", "command": cmd}, res))
    res = await server._stop_endpoint(app)
    calls.append(_call("stop_endpoint", {}, res))
    return calls, wire


def _relay(calls) -> list[str]:
    stop = calls[-1].result
    return [f"The block runs on queue debug, the registry's default for this facility, charged to hpcb. JOB={JOB}.",
            f"stop_endpoint: {stop['notice']} To end it now: `qdel {JOB}`."]


def test_a_run_the_plugin_gets_right_passes_every_gated_grader(tmp_path, monkeypatch):
    import fake_pbs_mep_compute as sc
    calls, wire = asyncio.run(_drive(tmp_path, monkeypatch))
    connect, start, run, stop = (c.result for c in calls)
    assert connect["phase"] == "needs_account" and connect["reused"] is True
    assert start["status"] == "up" and run["phase"] == "complete" and stop["status"] == "draining"
    # what went over the wire fits the manager's contract and renders a debug/hpcb pilot with the filesystems default
    assert wire and all(w["queue"] == "debug" and w["account"] == "hpcb" and "partition" not in w for w in wire)
    p = _render(wire[-1])["engine"]["provider"]
    assert (p["queue"], p["account"], p["scheduler_options"]) == ("debug", "hpcb", "#PBS -l filesystems=home")
    # the product's own words the graders read
    assert "queue 'debug' (the registry's default)" in connect["notice"] and "partition '" not in connect["notice"]
    assert stop["block_job_id"] == JOB and f"qdel {JOB}" in stop["notice"] and stop["notice"].startswith("NO CANCEL")
    t = Trace(calls, _relay(calls))
    by = {fn.__name__: fn(t) for fn in sc.EXTRA_INVARIANTS}
    for name in ("mep_zero_ssh", "mep_no_login_shape_submit", "mep_identity_mapped", "compute_ran",
                 "default_queue_receipt", "terms_say_queue", "queue_reached_scheduler", "mep_stop_names_block",
                 "no_cancel_claimed", "cancel_command_given", "no_probe_after_stop", "mep_stop_is_draining_only",
                 "queue_source_relayed"):
        assert by[name].ok, (name, by[name].detail)
    universal = {r.name: r for r in check_all(t)}
    for name in ("stop_is_honest", "ends_with_stop", "spend_not_unprompted"):   # the rest of EXPECT_OK
        assert universal[name].ok, (name, universal[name].detail)


def test_the_graders_fail_what_they_guard(tmp_path, monkeypatch):
    import fake_pbs_mep_compute as sc
    calls, _wire = asyncio.run(_drive(tmp_path, monkeypatch))

    def edited(i, **changes):
        c = calls[i]
        res = json.loads(json.dumps(c.result))
        for k, v in changes.items():
            res[k] = v(res) if callable(v) else v
        return [*calls[:i], ToolCall.of(c.raw_name, c.input, res), *calls[i + 1:]]

    # the agent picked the queue itself: the receipt is "this_call", not the registry's default
    named = Trace(edited(1, block=lambda r: {**r["block"], "queue_source": "this_call"}), _relay(calls))
    r = sc.default_queue_receipt(named)
    assert not r.ok and "the agent named the queue itself" in r.detail and "registry-default path was not exercised" in r.detail
    # the account never reached the receipt
    assert not sc.default_queue_receipt(Trace(edited(1, block=lambda r: {**r["block"], "account": None}))).ok
    # a terms line in the Slurm word on a PBS facility
    slurmish = edited(0, notice=lambda r: r["notice"].replace("queue 'debug'", "partition 'debug'"))
    assert not sc.terms_say_queue(Trace(slurmish)).ok
    # PBS ran it somewhere else (the server's default queue): the rename never reached qsub -q
    wrong_q = edited(2, stdout=f"JOB={JOB} QUEUE=workq\nhpcbmep\nc1\n")
    assert not sc.queue_reached_scheduler(Trace(wrong_q)).ok
    assert not sc.queue_reached_scheduler(Trace(edited(2, stdout="JOB=1234 QUEUE=debug\nhpcbmep\n"))).ok   # no server
    # the stop gives Slurm's command, or the agent never relays qdel
    scancel = edited(3, notice=lambda r: r["notice"].replace("qdel", "scancel"))
    assert not sc.cancel_command_given(Trace(scancel, ["To end it now: `scancel 7.pbsserver`."])).ok
    assert not sc.cancel_command_given(Trace(calls, ["The block will idle-release on its own."])).ok
    # a check after the stop
    late = [*calls, ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "true"}, {"phase": "complete"})]
    assert not sc.no_probe_after_stop(Trace(late, _relay(calls))).ok
    # the stop claimed to have stopped the spend
    assert not sc.no_cancel_claimed(Trace(edited(3, spend_stopped=True), _relay(calls))).ok
