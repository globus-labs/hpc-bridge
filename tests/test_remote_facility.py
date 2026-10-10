import asyncio
import base64

import jinja2
import pytest
import yaml as _yaml

from hpc_bridge.facility import remote
from hpc_bridge.facility.base import EndpointHandle, Facility
from hpc_bridge.facility.remote import (
    NeedsPreauth,
    RemoteEndpointCLI,
    SlurmFacility,
    SshTarget,
    is_interactive_auth_failure,
)
from hpc_bridge.profile import Profile
from hpc_bridge.shapes import shape_config


def _profile():
    return remote.MachineProfile(
        name="testmachine",
        endpoint_name="hpc-bridge",
        display_name="HPC-Bridge Test",
        env_setup="module load x && source /home/x-u/gce-venv/bin/activate",
        interface="ib0",
        partition="debug",
        account="ACC",
        worker_init="module load x && source /home/x-u/gce-venv/bin/activate",
        scratch_root="/scratch/x-u/.hpc-bridge",
    )


def _pbs_profile():
    return remote.MachineProfile(
        name="polaris", endpoint_name="hpc-bridge-polaris",
        display_name="Polaris",
        env_setup="module load conda && source /home/u/gce/bin/activate",
        interface="hsn0", partition="debug", account="ACC",
        worker_init="module load conda && source /home/u/gce/bin/activate",
        scratch_root="/home/u/.hpc-bridge",
        scheduler="pbs", cpus_per_node=32,
        scheduler_options="#PBS -l filesystems=home:eagle",
    )


async def test_ssh_exec_kills_child_on_timeout(monkeypatch):
    # A connected ssh session that wedges mid-command must not leak: on timeout the child is
    # killed and reaped (else process + 3 pipe FDs leak per stuck control-plane call).
    state = {"killed": False, "waited": False}

    class _HangingProc:
        returncode = None

        async def communicate(self, payload=None):
            await asyncio.sleep(30)  # never returns within the test timeout

        def kill(self):
            state["killed"] = True
            self.returncode = -9

        async def wait(self):
            state["waited"] = True
            return -9

    async def _fake_create(*_a, **_k):
        return _HangingProc()

    monkeypatch.setattr(remote.asyncio, "create_subprocess_exec", _fake_create)
    with pytest.raises(TimeoutError):
        await remote.ssh_exec(SshTarget("h", "u", "/k"), "hang", timeout=0.05)
    assert state["killed"] and state["waited"]  # child killed + reaped, not abandoned


def _sanitize_user_json(opts):
    """Mirror the endpoint manager's `_sanitize_user_json`: json.dumps every string (its
    YAML-injection guard), which QUOTES strings ("x" -> '"x"'). Tests MUST apply this or
    they miss the whole class of bug where a quoted string breaks an in-template compare or
    gets double-encoded by `| tojson` (both shipped to Anvil before being caught)."""
    import json

    def inner(v):
        if isinstance(v, dict):
            return {k: inner(x) for k, x in v.items()}
        if isinstance(v, list):
            return [inner(x) for x in v]
        if isinstance(v, str):
            return json.dumps(v)
        if isinstance(v, (int, float)):  # bool is an int -> passes through unchanged
            return v
        if v is None:
            return "null"
        return v

    return inner(opts)


def _render(template_str, user_opts):
    # Faithfully mirror render_config_user_template: sanitize user_opts (json.dumps strings)
    # THEN render with StrictUndefined, exactly as the endpoint manager does.
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    return _yaml.safe_load(env.from_string(template_str).render(**_sanitize_user_json(user_opts)))


def test_template_renders_localprovider_for_login_shape():
    f = SlurmFacility(_profile(), cli=None)
    tmpl, _defaults = f.config_template(Profile(mode="interactive"))
    cfg = _render(tmpl, shape_config("login"))
    assert cfg["engine"]["provider"]["type"] == "LocalProvider"


def test_template_renders_slurmprovider_for_slurm_shape():
    f = SlurmFacility(_profile(), cli=None)
    tmpl, _defaults = f.config_template(Profile(mode="interactive"))
    cfg = _render(tmpl, shape_config("compute", partition="debug", account="ACC"))
    prov = cfg["engine"]["provider"]
    assert prov["type"] == "SlurmProvider"
    assert prov["partition"] == "debug" and prov["account"] == "ACC"
    assert prov["min_blocks"] == 0 and prov["max_blocks"] == 1


def test_template_defaults_to_slurm_account_from_profile():
    f = SlurmFacility(_profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    cfg = _render(tmpl, {**defaults, **shape_config("compute")})
    assert cfg["engine"]["provider"]["account"] == "ACC"


def test_slurm_provider_params_survive_the_manager_sanitizer():
    # Regression: the manager json.dumps's string user_opts, so a template `{% if
    # provider_type == 'SlurmProvider' %}` saw '"SlurmProvider"' and fell through to the
    # else branch, dropping partition/account/walltime entirely (the job then ran on the
    # facility default partition with no account). The bool `compute` guard fixes it.
    f = SlurmFacility(_profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    prov = _render(tmpl, {**defaults, **shape_config("compute")})["engine"]["provider"]
    assert prov["partition"] == "debug" and prov["account"] == "ACC"
    assert prov["walltime"] and "launcher" in prov  # full slurm block, not the else branch


def test_pbs_template_renders_pbsproprovider_compute_shape():
    f = SlurmFacility(_pbs_profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    prov = _render(tmpl, {**defaults, **shape_config("compute")})["engine"]["provider"]
    assert prov["type"] == "PBSProProvider"
    assert prov["queue"] == "debug"           # partition slot renders under the queue key
    assert prov["account"] == "ACC"
    assert prov["cpus_per_node"] == 32
    assert prov["min_blocks"] == 0
    # SingleNodeLauncher, not MpiExecLauncher: live Polaris validation showed MpiExecLauncher's
    # generated mpiexec command is incompatible with Polaris's PALS mpiexec under the pinned Parsl
    # (it printed usage and the worker pool never launched). A single-node block (nodes_per_block=1,
    # hpc-bridge's model) doesn't need mpiexec; SingleNodeLauncher runs the pool directly.
    assert prov["launcher"]["type"] == "SingleNodeLauncher"
    assert "partition" not in prov            # PBS uses queue, not partition
    assert prov["scheduler_options"] == "#PBS -l filesystems=home:eagle"


def test_pbs_template_login_shape_is_localprovider():
    f = SlurmFacility(_pbs_profile(), cli=None)
    tmpl, _defaults = f.config_template(Profile(mode="interactive"))
    cfg = _render(tmpl, shape_config("login"))
    assert cfg["engine"]["provider"]["type"] == "LocalProvider"


def test_pbs_provider_params_survive_the_manager_sanitizer():
    f = SlurmFacility(_pbs_profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    prov = _render(tmpl, {**defaults, **shape_config("compute")})["engine"]["provider"]
    assert prov["queue"] == "debug" and prov["account"] == "ACC"  # full block, not else branch


def test_slurm_template_still_selected_for_slurm_profile():
    f = SlurmFacility(_profile(), cli=None)  # _profile() defaults scheduler="slurm"
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    prov = _render(tmpl, {**defaults, **shape_config("compute")})["engine"]["provider"]
    assert prov["type"] == "SlurmProvider" and prov["partition"] == "debug"


def test_worker_init_not_double_encoded_through_sanitizer():
    # Regression: the sanitizer already quotes strings, so an extra `| tojson` on worker_init
    # double-encoded it ('"module load ..."' with literal quotes), and the worker tried to run
    # the whole quoted string as one command. worker_init must be the bare command.
    f = SlurmFacility(_profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    wi = _render(tmpl, {**defaults, **shape_config("compute")})["engine"]["provider"]["worker_init"]
    assert wi == _profile().worker_init  # exact command, no wrapping quotes
    assert not wi.startswith('"')


def test_template_max_blocks_and_nodes_per_block_default_and_override():
    from dataclasses import replace as _dc_replace

    # profile defaults flow into the rendered slurm provider
    prof = _dc_replace(_profile(), max_blocks=4, nodes_per_block=3)
    f = SlurmFacility(prof, cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    prov = _render(tmpl, {**defaults, **shape_config("compute")})["engine"]["provider"]
    assert prov["max_blocks"] == 4 and prov["nodes_per_block"] == 3
    assert prov["min_blocks"] == 0  # idle-release cost net unchanged

    # a per-task user_endpoint_config overrides the profile default
    over = _render(tmpl, shape_config("compute", max_blocks=9, nodes_per_block=2))
    assert over["engine"]["provider"]["max_blocks"] == 9
    assert over["engine"]["provider"]["nodes_per_block"] == 2


def test_template_max_blocks_configurable_for_local_shape():
    from dataclasses import replace as _dc_replace

    f = SlurmFacility(_dc_replace(_profile(), max_blocks=5), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    prov = _render(tmpl, {**defaults, **shape_config("login")})["engine"]["provider"]
    assert prov["type"] == "LocalProvider" and prov["max_blocks"] == 5


def test_template_emits_available_accelerators_only_when_set():
    from dataclasses import replace as _dc_replace

    # unset -> the key is omitted entirely
    f0 = SlurmFacility(_profile(), cli=None)
    t0, d0 = f0.config_template(Profile(mode="interactive"))
    assert "available_accelerators" not in _render(t0, {**d0, **shape_config("compute")})["engine"]

    # int count
    f1 = SlurmFacility(_dc_replace(_profile(), available_accelerators=4), cli=None)
    t1, d1 = f1.config_template(Profile(mode="interactive"))
    assert _render(t1, {**d1, **shape_config("compute")})["engine"]["available_accelerators"] == 4

    # explicit device list
    f2 = SlurmFacility(
        _dc_replace(_profile(), available_accelerators=["gpu0", "gpu1"]), cli=None
    )
    t2, d2 = f2.config_template(Profile(mode="interactive"))
    eng = _render(t2, {**d2, **shape_config("compute")})["engine"]
    assert eng["available_accelerators"] == ["gpu0", "gpu1"]


def test_slurm_facility_satisfies_protocol():
    assert isinstance(SlurmFacility(_profile(), cli=None), Facility)


class _FakeRemoteCLI:
    def __init__(self, status=None):
        self.calls = []
        self.written = {}
        self._status = status

    async def status(self, name):
        self.calls.append(("status", name))
        return self._status

    async def configure(self, name, multi_user=False):
        self.calls.append(("configure", name, multi_user))

    async def write_config(self, name, manager_yaml, uep_yaml):
        self.written = {"name": name, "manager": manager_yaml, "uep": uep_yaml}

    async def start(self, name):
        self.calls.append(("start", name))
        return ("fake-eid", "login03.anvil.rcac.purdue.edu", None)

    async def stop(self, name):
        self.calls.append(("stop", name))
        return (0, "")

    async def clean_uep_pidfiles(self, endpoint_id):
        self.calls.append(("clean_uep_pidfiles", endpoint_id))

    async def endpoint_id(self, name):
        return "running-eid"

    async def login_exec(self, command):
        self.calls.append(("login_exec", command))
        return (0, "OUT", "")

    async def wipe_storage_db(self):
        self.calls.append(("wipe", "hpc-bridge"))
        return True

    async def delete(self, name):
        self.calls.append(("delete", name))
        return True

    async def remove_uep_dirs(self, endpoint_id):
        self.calls.append(("remove_uep_dirs", endpoint_id))

    async def cancel_blocks(self, endpoint_id, scheduler="slurm"):
        self.calls.append(("cancel_blocks", endpoint_id, scheduler))
        return []

    def rebind(self, host):
        self.calls.append(("rebind", host))

    async def close(self):
        self.calls.append(("close",))
        return True


def _kinds(cli):
    return [c[0] for c in cli.calls]


class _FakeGCClient:
    """Stand-in for the Globus Compute web Client (get_endpoints / get_endpoint_status)."""

    def __init__(self, endpoints=(), statuses=None, raises=False):
        self._endpoints = list(endpoints)
        self._statuses = statuses or {}
        self._raises = raises

    def get_endpoints(self, role=None):
        if self._raises:
            raise RuntimeError("web boom")
        return self._endpoints

    def get_endpoint_status(self, eid):
        return {"status": self._statuses.get(eid, "offline")}


def _no_endpoints():
    """client_factory that reports no reusable endpoint -> bootstrap takes the SSH path."""
    return _FakeGCClient()


class _BootstrapCLI(_FakeRemoteCLI):
    def __init__(self, status=None, remote_db_present=False):
        super().__init__(status=status)
        self.remote_db_present = remote_db_present
        self.seeded = None
        self.fqdn = "login03.anvil.rcac.purdue.edu"
        self.target = SshTarget("anvil.rcac.purdue.edu", "x-u", "/tmp/k")

    async def whoami(self):
        return self.remote_db_present

    async def seed_storage_db(self, local_db, *, replace_ours=False):
        self.seeded = local_db
        self.replaced_ours = replace_ours
        self.remote_db_present = True

    async def hostname_fqdn(self):
        return self.fqdn


async def test_bootstrap_seeds_when_remote_db_absent(monkeypatch, tmp_path):
    cli = _BootstrapCLI(status=None, remote_db_present=False)
    fac = SlurmFacility(_profile(), cli=cli, client_factory=_no_endpoints)
    made = tmp_path / "trimmed.db"
    made.write_bytes(b"db")
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: made)
    handle = await fac.bootstrap(Profile(mode="interactive"))
    assert cli.seeded == made  # creds shipped because remote db was absent
    assert handle.login_host == "login03.anvil.rcac.purdue.edu"
    assert handle.reused is False  # #20: fresh SSH bootstrap, not a reuse
    assert ("start", "hpc-bridge") in cli.calls


async def test_bootstrap_skips_seed_when_remote_db_present(monkeypatch, tmp_path):
    cli = _BootstrapCLI(status="running", remote_db_present=True)
    fac = SlurmFacility(_profile(), cli=cli, client_factory=_no_endpoints)
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: tmp_path / "x.db")
    handle = await fac.bootstrap(Profile(mode="interactive"))
    assert cli.seeded is None  # already had creds -> no reseed
    assert handle.login_host is None  # reused (already-running) endpoint isn't re-probed for its node
    assert handle.reused is True  # #20: status=running is a reuse


async def test_bootstrap_records_login_node_in_store(monkeypatch, tmp_path):
    from hpc_bridge.state import LoginNodeStore

    cli = _BootstrapCLI(status=None, remote_db_present=True)  # present -> no seed needed
    store = LoginNodeStore(tmp_path / "endpoints.json")
    fac = SlurmFacility(
        _profile(), cli=cli, store=store, alias="anvil.rcac.purdue.edu", client_factory=_no_endpoints
    )
    await fac.bootstrap(Profile(mode="interactive"))
    rec = store.get(alias="anvil.rcac.purdue.edu", name="hpc-bridge")
    assert rec is not None and rec.login_host == "login03.anvil.rcac.purdue.edu"
    assert rec.user == "x-u" and rec.key_path == "/tmp/k"


# --- SSH-once: reuse an already-online endpoint over the web, no SSH -------------------------


async def test_find_online_endpoint_returns_uuid_when_owned_and_online():
    # Reuse is gated on manager_online ALONE — no liveness probe. A probe can't tell a dead ghost
    # from a just-started cold worker, so probe-gating false-rejects a fresh endpoint (#37); a dead
    # 'online' ghost is handled downstream by the robust canary (-> provisioning -> teardown).
    client = _FakeGCClient(
        endpoints=[{"name": "other", "uuid": "x"}, {"name": "hpc-bridge", "uuid": "ours"}],
        statuses={"ours": "online"},
    )
    f = SlurmFacility(_profile(), cli=None, client_factory=lambda: client)
    assert await f.find_online_endpoint("hpc-bridge") == "ours"


async def test_find_online_endpoint_none_when_offline_or_name_mismatch():
    offline = SlurmFacility(
        _profile(), cli=None,
        client_factory=lambda: _FakeGCClient([{"name": "hpc-bridge", "uuid": "ep"}], {"ep": "offline"}),
    )
    assert await offline.find_online_endpoint("hpc-bridge") is None  # registered but not online
    mismatch = SlurmFacility(
        _profile(), cli=None,
        client_factory=lambda: _FakeGCClient([{"name": "other", "uuid": "ep"}], {"ep": "online"}),
    )
    assert await mismatch.find_online_endpoint("hpc-bridge") is None  # not ours by name


async def test_find_online_endpoint_swallows_web_error():
    # A web/auth failure must not crash provisioning — return None and let the SSH path run.
    f = SlurmFacility(_profile(), cli=None, client_factory=lambda: _FakeGCClient(raises=True))
    assert await f.find_online_endpoint("hpc-bridge") is None


async def test_bootstrap_reuses_online_endpoint_without_any_ssh():
    # The SSH-once keystone: a web-online endpoint we own is reused over AMQP and NOT a single SSH
    # op (status/configure/start/whoami/seed) runs.
    cli = _BootstrapCLI(status=None, remote_db_present=False)
    client = _FakeGCClient([{"name": "hpc-bridge", "uuid": "reused-eid"}], {"reused-eid": "online"})
    fac = SlurmFacility(_profile(), cli=cli, client_factory=lambda: client)
    handle = await fac.bootstrap(Profile(mode="interactive"))
    assert handle.endpoint_id == "reused-eid"
    assert handle.reused is True  # #20: surfaced as a reuse, not a fresh bootstrap
    assert cli.calls == []  # zero SSH: no status/configure/start/login_exec
    assert cli.seeded is None  # and no credential seeding


async def test_provision_fresh_configures_writes_and_starts():
    cli = _FakeRemoteCLI(status=None)  # not configured yet
    handle = await SlurmFacility(_profile(), cli=cli).provision(Profile(mode="interactive"))
    assert isinstance(handle, EndpointHandle) and handle.endpoint_id == "fake-eid"
    assert handle.reused is False  # #20: fresh provision
    assert ("configure", "hpc-bridge", False) in cli.calls  # forced single-user
    assert ("start", "hpc-bridge") in cli.calls
    assert "amqp_port: 443" in cli.written["manager"]  # firewall-friendly AMQP in manager cfg
    assert "HPC-Bridge Test" in cli.written["manager"]  # human display_name, not the dir name
    assert "provider_type" in cli.written["uep"]  # template references the var, not a literal
    assert "run_in_sandbox: true" in cli.written["uep"]  # ShellFunctions expect a sandbox (per-task dir)


async def test_provision_skips_configure_when_already_configured():
    cli = _FakeRemoteCLI(status="configured")  # Initialized/Stopped from a prior run
    handle = await SlurmFacility(_profile(), cli=cli).provision(Profile(mode="interactive"))
    assert handle.endpoint_id == "fake-eid"
    assert "configure" not in _kinds(cli)  # re-configuring raises ConfigExists — must skip
    assert cli.written and ("start", "hpc-bridge") in cli.calls  # config rewritten, then started


async def test_provision_reuses_running_endpoint():
    # A locally-"Running" endpoint is re-adopted WITHOUT a liveness probe: cold-start and a genuine
    # hang are indistinguishable by a probe (and a slow facility must be allowed to warm), so provision
    # does not declare "wedged" here — the canary flow reports 'provisioning' until a worker answers (#37).
    cli = _FakeRemoteCLI(status="running")
    handle = await SlurmFacility(_profile(), cli=cli).provision(Profile(mode="interactive"))
    assert handle.endpoint_id == "running-eid"
    assert handle.reused is True
    assert "configure" not in _kinds(cli) and "start" not in _kinds(cli)  # reuse, don't restart
    assert "stop" not in _kinds(cli) and "rebind" not in _kinds(cli)  # untouched -> stay on the current host


async def test_provision_clears_stale_uep_pidfiles_before_restart():
    # #37 (live 9c056131): a stopped/configured endpoint being re-started may carry stale per-UEP
    # daemon.pid files from force-killed workers; provision clears them (scoped to the UUID) BEFORE
    # start so the fresh worker doesn't exit 73 ("Another instance is running"). The real "won't warm" fix.
    cli = _FakeRemoteCLI(status="configured")
    await SlurmFacility(_profile(), cli=cli).provision(Profile(mode="interactive"))
    kinds = _kinds(cli)
    assert ("clean_uep_pidfiles", "running-eid") in cli.calls  # cleared for THIS endpoint's UUID
    assert kinds.index("clean_uep_pidfiles") < kinds.index("start")  # BEFORE the fresh start


# --- interactive auth (password / MFA): detect, hand off, never handle the secret --------------


def test_is_interactive_auth_failure_detects_interactive_offer():
    assert is_interactive_auth_failure(255, "Permission denied (publickey,password,keyboard-interactive).")
    assert is_interactive_auth_failure(255, "Permission denied (keyboard-interactive).")
    assert not is_interactive_auth_failure(255, "Permission denied (publickey).")  # no interactive fallback
    assert not is_interactive_auth_failure(255, "ssh: connect to host h port 22: Connection refused")
    assert not is_interactive_auth_failure(0, "")  # success


def test_preauth_command_opens_a_shareable_master_without_batchmode():
    cmd = SshTarget(host="midway", user="gus", control_dir="/tmp/cm").preauth_command()
    assert cmd.startswith("ssh -fN ")
    assert "ControlMaster=yes" in cmd
    assert "ControlPath=/tmp/cm/%C" in cmd  # the SAME %C path argv() uses -> ControlMaster=auto joins it
    assert "BatchMode" not in cmd            # must prompt interactively (the USER types the secret)
    assert cmd.endswith("gus@midway")


async def test_discover_raises_needs_preauth_on_interactive_auth_failure(monkeypatch):
    from hpc_bridge import discovery

    async def fake_ssh_exec(target, cmd, **kw):
        return 255, "", "Permission denied (publickey,password,keyboard-interactive)."

    monkeypatch.setattr(discovery, "ssh_exec", fake_ssh_exec)
    with pytest.raises(NeedsPreauth):
        await discovery.discover_facility_details(SshTarget(host="midway", user="g", control_dir="/tmp/cm"))


async def test_discover_raises_runtimeerror_on_unreachable_host(monkeypatch):
    from hpc_bridge import discovery

    async def fake_ssh_exec(target, cmd, **kw):
        return 255, "", "ssh: connect to host midway port 22: Connection refused"

    monkeypatch.setattr(discovery, "ssh_exec", fake_ssh_exec)
    with pytest.raises(RuntimeError):
        await discovery.discover_facility_details(SshTarget(host="midway", control_dir="/tmp/cm"))


def test_routable_pin_drops_internal_hostnames():
    from hpc_bridge.facility.remote import _routable_pin
    yes, no = (lambda h: True), (lambda h: False)
    assert _routable_pin("login03.anvil.rcac.purdue.edu", resolves=yes) == "login03.anvil.rcac.purdue.edu"  # external FQDN kept
    assert _routable_pin("beagle3-tbd1.rcc.local", resolves=yes) is None  # internal .local dropped (Midway, seen live)
    assert _routable_pin("login01", resolves=yes) is None                 # single-label dropped
    assert _routable_pin("node.internal", resolves=yes) is None
    # a plain-looking name the client's DNS does not know is just as useless (fake `internal` profile, 2026-09-06)
    assert _routable_pin("login01.int.hpcb.test", resolves=no) is None
    # …unless the bootstrap connection's own server address is known: pin THAT (still key-checked against the alias)
    assert _routable_pin("login01.int.hpcb.test", "172.20.0.8", resolves=no) == "172.20.0.8"
    assert _routable_pin("beagle3-tbd1.rcc.local", "10.50.1.7", resolves=yes) == "10.50.1.7"
    assert _routable_pin("login03.anvil.rcac.purdue.edu", "1.2.3.4", resolves=yes) == "login03.anvil.rcac.purdue.edu"  # name first
    assert _routable_pin(None, "127.0.0.1") is None and _routable_pin(None, "garbage") is None and _routable_pin(None, "") is None
    assert _routable_pin(None) is None and _routable_pin("") is None


def test_routable_pin_drops_management_plane_hostnames():
    # ALCF Aurora's `hostname -f` is a management-plane name (seen live) whose `hostmgmt.cm` infix isn't
    # routable through the bastion — pinning it would break teardown/reconnect. Drop it -> fall back to
    # the alias (which the user pins to a specific UAN in ~/.ssh/config).
    from hpc_bridge.facility.remote import _routable_pin
    assert _routable_pin("aurora-uan-0009.hostmgmt.cm.aurora.alcf.anl.gov") is None  # Aurora, seen live
    assert _routable_pin("x.cm.polaris.alcf.anl.gov") is None                        # ALCF cluster-mgmt plane
    assert _routable_pin("n01.mgmt.example.gov") is None
    # whole-label match only — legit external names that merely CONTAIN those substrings are kept
    assert _routable_pin("cms.web.cern.ch") == "cms.web.cern.ch"                     # 'cms' != 'cm' label
    assert _routable_pin("mgmtnode.hpc.example.edu") == "mgmtnode.hpc.example.edu"   # 'mgmtnode' != 'mgmt'


async def test_provision_rebinds_cli_to_the_node_the_daemon_landed_on():
    # After `start` captures the manager's real login node, the live CLI must repoint
    # there so later control-plane ops (esp. teardown) reach THIS node instead of the
    # round-robin alias — otherwise stop hits the wrong node and orphans the daemon.
    cli = _FakeRemoteCLI(status=None)
    handle = await SlurmFacility(_profile(), cli=cli).provision(Profile(mode="interactive"))
    assert ("rebind", "login03.anvil.rcac.purdue.edu") in cli.calls
    assert _kinds(cli).index("rebind") > _kinds(cli).index("start")  # rebind AFTER start
    assert handle.login_host == "login03.anvil.rcac.purdue.edu"


async def test_teardown_stops_endpoint():
    cli = _FakeRemoteCLI()
    await SlurmFacility(_profile(), cli=cli).teardown("fake-eid")
    assert ("stop", "hpc-bridge") in cli.calls


async def test_teardown_cancels_blocks_as_backstop():
    # an ungraceful stop can orphan the running block until walltime -> teardown must scancel
    cli = _FakeRemoteCLI()
    await SlurmFacility(_profile(), cli=cli).teardown("eid-123")
    assert ("stop", "hpc-bridge") in cli.calls
    assert ("cancel_blocks", "eid-123", "slurm") in cli.calls  # cancel this endpoint's blocks
    assert _kinds(cli).index("cancel_blocks") > _kinds(cli).index("stop")  # after stop


async def test_teardown_cancels_blocks_with_pbs_scheduler():
    # the facility's scheduler must ride along to cancel_blocks so a PBS profile's teardown
    # qdels instead of scancels.
    cli = _FakeRemoteCLI()
    await SlurmFacility(_pbs_profile(), cli=cli).teardown("eid-123")
    assert ("cancel_blocks", "eid-123", "pbs") in cli.calls


async def test_clean_uep_pidfiles_scopes_to_uuid_and_refuses_non_uuid(monkeypatch):
    # The cleanup globs a remote path built from endpoint_id, so only run it for a real UUID — never
    # build a shell command from arbitrary text.
    calls = []

    async def _fake_ssh_exec(target, cmd, **kw):
        calls.append(cmd)
        return (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", _fake_ssh_exec)
    cli = remote.RemoteEndpointCLI(SshTarget("h", "u", "k"), "env")
    await cli.clean_uep_pidfiles("a42e6169-55f3-4dc4-be72-8ae2cfda721d")
    assert len(calls) == 1 and "uep.a42e6169-55f3-4dc4-be72-8ae2cfda721d.*/daemon.pid" in calls[0]
    calls.clear()
    await cli.clean_uep_pidfiles("; rm -rf ~ #")  # not a UUID -> refused, no SSH built from it
    assert calls == []


async def test_cancel_blocks_targets_only_this_endpoints_jobs(monkeypatch):
    # precision: match blocks by StdOut path under uep.<endpoint_id>, never another
    # GlobusComputeEngine endpoint's jobs sharing the parsl job-name prefix.
    eid = "8791269d-47f4-47f7-91a6-3485b4289269"
    other = "c5fd7ad7-0b92-4b50-83e1-56f7b9c1f91d"
    seen = []

    async def fake_ssh(target, cmd, **kw):
        seen.append(cmd)
        if "squeue" in cmd:
            return (
                0,
                f"111   /home/u/.globus_compute/uep.{eid}.aaa/submit_scripts/x.stdout\n"
                f"222   /home/u/.globus_compute/uep.{other}.bbb/submit_scripts/y.stdout\n"
                f"333   /home/u/.globus_compute/uep.{eid}.ccc/submit_scripts/z.stdout\n",
                "",
            )
        return (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh)
    cli = remote.RemoteEndpointCLI(SshTarget("h", "u", "k"), "env")
    cancelled = await cli.cancel_blocks(eid)
    assert cancelled == ["111", "333"]  # ours only; 222 (other endpoint) untouched
    scancel = [c for c in seen if "scancel" in c]
    assert len(scancel) == 1
    assert "111" in scancel[0] and "333" in scancel[0] and "222" not in scancel[0]


async def test_cancel_blocks_no_jobs_is_noop(monkeypatch):
    async def fake_ssh(target, cmd, **kw):
        return (0, "", "") if "squeue" in cmd else (0, "", "")  # noqa: RUF034

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh)
    cli = remote.RemoteEndpointCLI(SshTarget("h", "u", "k"), "env")
    assert await cli.cancel_blocks("any-eid") == []  # nothing matched -> no scancel


def test_pbs_jobid_regex_matches_dotted_ids():
    from hpc_bridge.facility.remote import _JOBID_PBS

    assert _JOBID_PBS.match("1234567.polaris-pbs-01")
    assert _JOBID_PBS.match("1234567")
    assert not _JOBID_PBS.match("garbage")


async def test_cancel_blocks_pbs_targets_only_this_endpoints_jobs(monkeypatch):
    # PBS variant: qstat -f (continuation-unwrapped) -> match the uep.<eid> marker -> qdel.
    eid = "8791269d-47f4-47f7-91a6-3485b4289269"
    other = "c5fd7ad7-0b92-4b50-83e1-56f7b9c1f91d"
    seen = []

    async def fake_ssh(target, cmd, **kw):
        seen.append(cmd)
        if "qstat -f" in cmd:
            return (
                0,
                "Job Id: 111.polaris-pbs-01\n"
                "    Job_Name = uep\n"
                f"    Output_Path = polaris-pbs-01:/home/u/.globus_compute/uep.{eid}.aaa/submit_scripts/x.stdout\n"
                "Job Id: 222.polaris-pbs-01\n"
                f"    Output_Path = polaris-pbs-01:/home/u/.globus_compute/uep.{other}.bbb/submit_scripts/y.stdout\n"
                "Job Id: 333.polaris-pbs-01\n"
                f"    Output_Path = polaris-pbs-01:/home/u/.globus_compute/uep.{eid}.ccc/submit_scripts/z.stdout\n",
                "",
            )
        return (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh)
    cli = remote.RemoteEndpointCLI(SshTarget("h", "u", "k"), "env")
    cancelled = await cli.cancel_blocks(eid, "pbs")
    assert cancelled == ["111.polaris-pbs-01", "333.polaris-pbs-01"]  # ours only
    qdel = [c for c in seen if "qdel" in c]
    assert len(qdel) == 1
    # qstat must NOT be -u filtered: PBS Pro emits empty full-format output with -u (live bug).
    assert "-u" not in next(c for c in seen if "qstat -f" in c)
    assert "111.polaris-pbs-01" in qdel[0] and "333.polaris-pbs-01" in qdel[0]
    assert "222.polaris-pbs-01" not in qdel[0]
    assert "squeue" not in seen[0] and "scancel" not in " ".join(seen)


async def test_cancel_blocks_pbs_no_jobs_is_noop(monkeypatch):
    async def fake_ssh(target, cmd, **kw):
        return (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh)
    cli = remote.RemoteEndpointCLI(SshTarget("h", "u", "k"), "env")
    assert await cli.cancel_blocks("any-eid", "pbs") == []


async def test_manager_online_uses_injected_client():
    class _Client:
        def get_endpoint_status(self, eid):
            return {"status": "online"}

    f = SlurmFacility(_profile(), cli=None, client_factory=lambda: _Client())
    assert await f.manager_online("fake-eid") is True


async def test_endpoint_id_parses_list_and_picks_named_row(monkeypatch):
    sample = (
        "| Endpoint ID | Status | Endpoint Name |\n"
        "| 358c89fb-2774-4a5c-8dc5-da2406ccdc9c | Running | hpc-bridge |\n"
        "| c5fd7ad7-0b92-4b50-83e1-56f7b9c1f91d | Disconnected | default |\n"
    )

    async def fake_ssh_exec(target, cmd, **kw):
        return (0, sample, "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    assert await cli.endpoint_id("hpc-bridge") == "358c89fb-2774-4a5c-8dc5-da2406ccdc9c"
    with pytest.raises(RuntimeError, match="could not find"):
        await cli.endpoint_id("nope")


async def test_list_parse_fails_loud_on_unrecognized_format(monkeypatch):
    # A gce version/format change away from the pipe table must NOT read as "no endpoints"
    # (silent mis-provision); it must raise a clear, actionable error (issue #8).
    borderless = (
        "Endpoint ID                           Status   Endpoint Name\n"
        "358c89fb-2774-4a5c-8dc5-da2406ccdc9c  Running  hpc-bridge\n"
    )

    async def fake_ssh_exec(target, cmd, **kw):
        return (0, borderless, "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    with pytest.raises(RuntimeError, match="could not parse"):
        await cli.status("hpc-bridge")
    with pytest.raises(RuntimeError, match="could not parse"):
        await cli.endpoint_id("hpc-bridge")


async def test_list_no_endpoints_is_not_a_parse_error(monkeypatch):
    # The legitimate "nothing configured" message must stay quiet: status -> None,
    # endpoint_id -> the normal "could not find", NOT the parse-error path.
    empty = "No endpoints configured!\n\n (Hint: globus-compute-endpoint configure)\n"

    async def fake_ssh_exec(target, cmd, **kw):
        return (0, empty, "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    assert await cli.status("hpc-bridge") is None
    with pytest.raises(RuntimeError, match="could not find"):
        await cli.endpoint_id("hpc-bridge")


async def test_write_file_lets_remote_expand_home(monkeypatch):
    # Regression: shlex.quote single-quoted the path, so `~`/$HOME never expanded
    # remotely and the write hit a literal path → "No such file or directory".
    captured = {}

    async def fake_ssh_exec(target, cmd, *, stdin=None, **kw):
        captured["cmd"] = cmd
        captured["stdin"] = stdin
        return (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    await cli._write_file("$HOME/.globus_compute/hpc-bridge/config.yaml", "display_name: x")
    assert captured["stdin"] == "display_name: x"
    assert "$HOME" in captured["cmd"] and "'$HOME" not in captured["cmd"]  # not single-quoted


async def test_configure_tolerates_already_configured(monkeypatch):
    async def fake_ssh_exec(target, cmd, **kw):
        return (1, "", "Endpoint hpc-bridge is already configured")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    await cli.configure("hpc-bridge")  # must not raise — re-provision overwrites the config


async def test_configure_raises_on_real_error(monkeypatch):
    async def fake_ssh_exec(target, cmd, **kw):
        return (1, "", "globus-compute-endpoint: command not found")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    with pytest.raises(RuntimeError, match="configure failed"):
        await cli.configure("hpc-bridge")


async def test_status_parses_running_configured_absent(monkeypatch):
    out = {"v": ""}

    async def fake_ssh_exec(target, cmd, **kw):
        return (0, out["v"], "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    out["v"] = "| 358c89fb-... | Running | hpc-bridge |\n"
    assert await cli.status("hpc-bridge") == "running"
    out["v"] = "| None | Initialized | hpc-bridge |\n"  # the exact state we hit on Anvil
    assert await cli.status("hpc-bridge") == "configured"
    out["v"] = "| 358c89fb-... | Running | anvil-dev |\n"  # different endpoint only
    assert await cli.status("hpc-bridge") is None


async def test_status_and_endpoint_id_match_name_exactly_not_substring(monkeypatch):
    # Regression (caught live on Anvil): a pre-existing 'hpc-bridge-login' row contains
    # the substring 'hpc-bridge', so a naive `name in line` mis-identified it as ours and
    # skipped configure. status()/endpoint_id() must match the Endpoint Name column exactly.
    listing = (
        "+--------------------------------------+---------+------------------+\n"
        "|             Endpoint ID              | Status  |  Endpoint Name   |\n"
        "+======================================+=========+==================+\n"
        "| c76765f0-fcf7-4fce-afa9-2dda47677fcc | Stopped | hpc-bridge-login |\n"
        "+--------------------------------------+---------+------------------+\n"
    )

    async def fake_ssh_exec(target, cmd, **kw):
        return (0, listing, "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    assert await cli.status("hpc-bridge") is None  # must NOT match hpc-bridge-login
    assert await cli.status("hpc-bridge-login") == "configured"  # exact match works
    with pytest.raises(RuntimeError, match="could not find"):
        await cli.endpoint_id("hpc-bridge")  # no exact 'hpc-bridge' row


async def test_remote_cli_login_exec_runs_bash_lc_over_ssh(monkeypatch):
    captured = {}

    async def fake_ssh_exec(target, cmd, **kw):
        captured["cmd"] = cmd
        return (0, "shared*|up|infinite|250|128|257400|226/12/12/250\n", "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    rc, out, _ = await cli.login_exec("sinfo -h")
    assert rc == 0 and "shared" in out
    assert captured["cmd"].startswith("bash -lc ") and "sinfo" in captured["cmd"]  # login shell, no venv


async def test_slurm_facility_login_exec_delegates_to_cli():
    cli = _FakeRemoteCLI()
    rc, out, _ = await SlurmFacility(_profile(), cli=cli).login_exec("sinfo -h")
    assert rc == 0 and out == "OUT"
    assert ("login_exec", "sinfo -h") in cli.calls  # discovery routed through the SSH CLI, not _gce


async def test_seed_storage_db_streams_b64_and_locks_permissions(monkeypatch, tmp_path):
    db = tmp_path / "storage.db"
    db.write_bytes(b"\x00sqlite-bytes\xff")
    cmds = []

    async def fake_ssh_exec(target, cmd, *, stdin=None, **kw):
        cmds.append((cmd, stdin))
        return (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    await cli.seed_storage_db(db)

    mkdir_i = next(i for i, (c, _) in enumerate(cmds) if "mkdir -p" in c)
    write_i = next(i for i, (c, _) in enumerate(cmds) if "base64 -d" in c)
    chmod_file_i = next(i for i, (c, _) in enumerate(cmds) if "chmod 600" in c)
    # dir created, then file written, then file locked
    assert mkdir_i < write_i < chmod_file_i
    assert any("chmod 700" in c for c, _ in cmds)  # dir locked to user-only
    # the base64 payload round-trips to the original bytes
    payload = next(s for c, s in cmds if "base64 -d" in c)
    assert base64.b64decode(payload) == b"\x00sqlite-bytes\xff"


async def test_seed_storage_db_raises_on_remote_failure(monkeypatch, tmp_path):
    db = tmp_path / "storage.db"
    db.write_bytes(b"x")

    async def fake_ssh_exec(target, cmd, *, stdin=None, **kw):
        return (1, "", "Permission denied")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    with pytest.raises(RuntimeError, match="seed storage.db"):  # noqa: RUF043
        await cli.seed_storage_db(db)




def test_rebind_points_cli_at_a_specific_host():
    cli = RemoteEndpointCLI(SshTarget("anvil.rcac.purdue.edu", "u", "k"), "true")
    cli.rebind("login03.anvil.rcac.purdue.edu")
    assert cli.target.host == "login03.anvil.rcac.purdue.edu"
    assert cli.target.user == "u"
    assert cli.target.key_path == "k"  # other fields preserved


def test_template_max_idletime_is_float():
    f = SlurmFacility(_profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    cfg = _render(tmpl, {**defaults, **shape_config("compute")})
    jsk = cfg["engine"]["job_status_kwargs"]
    assert isinstance(jsk["max_idletime"], float) and jsk["max_idletime"] == 600.0


@pytest.mark.parametrize("profile", [_profile, _pbs_profile], ids=["slurm", "pbs"])
@pytest.mark.parametrize("shape", ["login", "compute"])
def test_template_scaling_pass_is_5s_for_first_result_latency(profile, shape):
    # the first scale-out waits one pass, paid twice per first SSH result (30 s → ~31 s each, measured 2026-10)
    f = SlurmFacility(profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="batch"))
    cfg = _render(tmpl, {**defaults, **shape_config(shape)})
    assert cfg["engine"]["job_status_kwargs"]["strategy_period"] == 5


def test_template_idle_timeout_follows_profile():
    f = SlurmFacility(_profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive", max_idletime_s=300))
    cfg = _render(tmpl, {**defaults, **shape_config("compute")})
    assert cfg["engine"]["job_status_kwargs"]["max_idletime"] == 300.0


def test_template_batch_mode_init_blocks_zero_interactive_one():
    f = SlurmFacility(_profile(), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="batch"))
    cfg = _render(tmpl, {**defaults, **shape_config("compute")})
    assert cfg["engine"]["provider"]["init_blocks"] == 0
    tmpl2, defaults2 = f.config_template(Profile(mode="interactive"))
    cfg2 = _render(tmpl2, {**defaults2, **shape_config("compute")})
    assert cfg2["engine"]["provider"]["init_blocks"] == 1


def test_template_emits_scheduler_options_when_profile_sets_it():
    from dataclasses import replace as _dc_replace
    base = _profile()
    prof = _dc_replace(base, scheduler_options="#SBATCH --constraint=A100")
    f = SlurmFacility(prof, cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    cfg = _render(tmpl, {**defaults, **shape_config("compute")})
    assert cfg["engine"]["provider"]["scheduler_options"] == "#SBATCH --constraint=A100"


def test_template_survives_special_chars_in_profile():
    from dataclasses import replace as _dc_replace
    base = _profile()
    # apostrophe, percent, and braces in worker_init must NOT break Jinja compile
    prof = _dc_replace(base, worker_init="source it's/venv && export P=50% {ok}")
    f = SlurmFacility(prof, cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    cfg = _render(tmpl, {**defaults, **shape_config("compute")})
    expected = "source it's/venv && export P=50% {ok}"
    assert cfg["engine"]["provider"]["worker_init"] == expected


async def test_wipe_storage_db_removes_remote_credential(monkeypatch):
    wiped = {}

    async def fake_ssh_exec(target, cmd, **kw):
        if "rm -f" in cmd and "storage.db" in cmd:
            wiped["yes"] = True
        return (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    await cli.wipe_storage_db()
    assert wiped.get("yes") is True


async def test_teardown_keeps_credentials_by_default():
    cli = _FakeRemoteCLI()
    await SlurmFacility(_profile(), cli=cli).teardown("fake-eid")
    assert ("stop", "hpc-bridge") in cli.calls
    assert ("wipe", ) not in [(c[0],) for c in cli.calls]  # default keeps creds for reconnect


async def test_teardown_wipes_credentials_only_when_hpc_bridge_seeded_them():
    # the store hpc-bridge placed goes with the endpoint…
    cli = _FakeRemoteCLI()
    fac = SlurmFacility(_profile(), cli=cli)
    fac._seeded_credentials = True
    await fac.teardown("fake-eid", wipe_credentials=True)
    assert ("wipe", "hpc-bridge") in cli.calls
    # …a store that was already there (a shared account, say) is never ours to delete (2026-09-04)
    cli2 = _FakeRemoteCLI()
    await SlurmFacility(_profile(), cli=cli2).teardown("fake-eid", wipe_credentials=True)
    assert ("wipe", "hpc-bridge") not in cli2.calls


# --- Persistent SSH (ControlMaster multiplexing) --------------------------------------------


def test_argv_unchanged_without_control_dir():
    # No multiplexing configured -> argv is byte-identical to the pre-ControlMaster baseline.
    assert SshTarget("h", "u", "/k").argv("CMD") == [
        "ssh", "-i", "/k",
        "-o", "IdentitiesOnly=yes",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=20",
        "--", "u@h", "CMD",  # no StrictHostKeyChecking override: your ssh's trust decides (review 2026-09-04)
    ]


def test_argv_adds_controlmaster_when_control_dir_set():
    argv = SshTarget("h", "u", "/k", control_dir="/cm", control_persist=45).argv("CMD")
    assert argv[:9] == [  # base options unchanged...
        "ssh", "-i", "/k",
        "-o", "IdentitiesOnly=yes",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=20",
    ]
    assert argv[-9:] == [  # ...then the three multiplexing options (%C-keyed), then the target
        "-o", "ControlMaster=auto",
        "-o", "ControlPath=/cm/%C",
        "-o", "ControlPersist=45",
        "--", "u@h", "CMD",
    ]


def test_control_argv_builds_exit_request():
    argv = SshTarget("h", "u", "/k", control_dir="/cm").control_argv("exit")
    assert argv == ["ssh", "-O", "exit", "-o", "ControlPath=/cm/%C", "--", "u@h"]


async def test_close_sends_ssh_o_exit_when_multiplexed(monkeypatch):
    captured = {}

    class _FakeProc:
        returncode = 0

        async def communicate(self, payload=None):
            return (b"", b"")

    async def fake_create(*argv, **kw):
        captured["argv"] = argv
        return _FakeProc()

    monkeypatch.setattr(remote.asyncio, "create_subprocess_exec", fake_create)
    await RemoteEndpointCLI(SshTarget("h", "u", "k", control_dir="/cm"), "true").close()
    assert captured["argv"][:3] == ("ssh", "-O", "exit")
    assert "ControlPath=/cm/%C" in captured["argv"] and "u@h" in captured["argv"]


async def test_close_is_noop_without_control_dir(monkeypatch):
    called = {"n": 0}

    async def fake_create(*a, **k):
        called["n"] += 1

    monkeypatch.setattr(remote.asyncio, "create_subprocess_exec", fake_create)
    await RemoteEndpointCLI(SshTarget("h", "u", "k"), "true").close()
    assert called["n"] == 0  # no master configured -> nothing to close


async def test_teardown_closes_the_ssh_master():
    cli = _FakeRemoteCLI()
    await SlurmFacility(_profile(), cli=cli).teardown("fake-eid")
    assert ("close",) in cli.calls  # shared SSH master dropped on full teardown


def test_rebind_preserves_control_settings():
    cli = RemoteEndpointCLI(SshTarget("alias", "u", "k", control_dir="/cm", control_persist=30), "true")
    cli.rebind("login03")
    assert cli.target.host == "login03"  # %C re-derives a fresh socket for the pinned node
    assert cli.target.control_dir == "/cm" and cli.target.control_persist == 30  # preserved


def test_argv_defers_to_ssh_config_when_user_and_key_absent():
    # No explicit user/key -> bare host, no -i / IdentitiesOnly, so OpenSSH resolves User +
    # IdentityFile from ~/.ssh/config (read live — no boot-env var the running server can't see).
    assert SshTarget("globus1").argv("CMD") == [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=20",
        "--", "globus1", "CMD",
    ]


def test_argv_user_without_key_defers_identity():
    argv = SshTarget("h", user="u").argv("CMD")  # explicit user, key deferred to config
    assert "-i" not in argv and "IdentitiesOnly=yes" not in argv  # no key pinned
    assert argv[-2:] == ["u@h", "CMD"]


def test_control_argv_bare_host_when_user_absent():
    assert SshTarget("globus1", control_dir="/cm").control_argv("exit") == [
        "ssh", "-O", "exit", "-o", "ControlPath=/cm/%C", "--", "globus1",
    ]


# ---- issue #39: the "registration lag" was a WRAPPED `list` table (+ a real ~3.6 s lag) ------------------------
LONG_NAME = "hpc-bridge-globus1-1788535474-99999"  # 34 chars: the harness's per-run shape; hpc-bridge-<fqdn> too
WRAPPED_LIST = (  # captured verbatim on globus1 (gce 4.13, no TTY, COLUMNS unset)
    "+--------------------------------------+---------+-----------------------------+\n"
    "|             Endpoint ID              | Status  |        Endpoint Name        |\n"
    "+======================================+=========+=============================+\n"
    "| ec844b37-fba8-4053-bbbf-e8f1848964d3 | Running | hpc-bridge-                 |\n"
    "|                                      |         | globus1-1788535474-99999    |\n"
    "+--------------------------------------+---------+-----------------------------+\n"
)
WIDE_LIST = (  # the same listing with COLUMNS=300
    "| ec844b37-fba8-4053-bbbf-e8f1848964d3 | Running | hpc-bridge-globus1-1788535474-99999    |\n"
)


def _gce_fake(list_for_call):
    """ssh_exec fake: `list` output chosen per (call index, cmd); everything else succeeds silently."""
    calls: list[str] = []

    async def fake(target, cmd, **kw):
        calls.append(cmd)
        if "endpoint list" in cmd:
            return (0, list_for_call(sum("endpoint list" in c for c in calls) - 1, cmd), "")
        return (0, "HPCB_HOST=login1.example\nHPCB_ADDR=10.0.0.5\n", "")

    return fake, calls


async def test_39_list_runs_with_a_wide_columns_so_long_names_do_not_wrap(monkeypatch):
    fake, calls = _gce_fake(lambda n, cmd: WIDE_LIST if "COLUMNS=" in cmd else WRAPPED_LIST)
    monkeypatch.setattr(remote, "ssh_exec", fake)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    assert await cli.status(LONG_NAME) == "running"
    assert await cli.endpoint_id(LONG_NAME) == "ec844b37-fba8-4053-bbbf-e8f1848964d3"
    assert all("export COLUMNS=" in c for c in calls)


async def test_39_without_the_wide_render_the_wrapped_row_never_matches(monkeypatch):
    # the pre-fix behaviour, kept as the explanation: per-line parsing sees the name cell as "hpc-bridge-"
    rows = RemoteEndpointCLI._list_rows(WRAPPED_LIST)
    data = [r for r in rows if r[0].startswith("ec844b37")]
    assert data and data[0][-1] == "hpc-bridge-"  # the continuation row has 1 cell and is dropped


async def test_39_start_waits_for_the_uuid_to_register(monkeypatch):
    fake, calls = _gce_fake(lambda n, cmd: "No endpoints configured!\n" if n == 0 else WIDE_LIST)
    monkeypatch.setattr(remote, "ssh_exec", fake)
    slept: list[float] = []

    async def no_sleep(s):
        slept.append(s)

    monkeypatch.setattr(remote, "_sleep", no_sleep)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    eid, host, _addr = await cli.start(LONG_NAME)
    assert eid == "ec844b37-fba8-4053-bbbf-e8f1848964d3" and host == "login1.example"
    assert sum("endpoint list" in c for c in calls) == 2 and slept == [remote._START_REGISTER_POLL_S]


async def test_39_start_gives_up_after_the_bounded_wait(monkeypatch):
    fake, calls = _gce_fake(lambda n, cmd: "No endpoints configured!\n")
    monkeypatch.setattr(remote, "ssh_exec", fake)
    clock = {"t": 0.0}

    async def no_sleep(s):
        clock["t"] += s

    monkeypatch.setattr(remote, "_sleep", no_sleep)
    monkeypatch.setattr(remote.time, "monotonic", lambda: clock["t"])
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    with pytest.raises(RuntimeError, match="could not find endpoint"):
        await cli.start(LONG_NAME)
    n = sum("endpoint list" in c for c in calls)
    assert 2 <= n <= int(remote._START_REGISTER_WAIT_S / remote._START_REGISTER_POLL_S) + 2


@pytest.mark.parametrize("refusal", [
    (73, "", ""),  # gce 4.13 live: exit 73, nothing on either stream
    (1, "", "Another instance of this endpoint is running. Refusing to start."),  # the documented text
])
async def test_39_start_adopts_an_already_running_manager(monkeypatch, refusal):
    calls: list[str] = []

    async def fake(target, cmd, **kw):
        calls.append(cmd)
        if "endpoint list" in cmd:
            return (0, WIDE_LIST, "")  # the manager IS running
        return refusal

    monkeypatch.setattr(remote, "ssh_exec", fake)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    eid, host, _addr = await cli.start(LONG_NAME)  # no raise: the running manager is the endpoint we want
    assert eid == "ec844b37-fba8-4053-bbbf-e8f1848964d3" and host is None


async def test_39_start_still_fails_on_a_real_start_error(monkeypatch):
    async def fake(target, cmd, **kw):
        return (1, "", "ModuleNotFoundError: No module named 'parsl'")

    monkeypatch.setattr(remote, "ssh_exec", fake)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    with pytest.raises(RuntimeError, match=r"remote start failed \(rc=1\): ModuleNotFoundError"):
        await cli.start(LONG_NAME)


async def test_39_refused_start_with_no_running_manager_is_a_real_failure(monkeypatch):
    async def fake(target, cmd, **kw):
        if "endpoint list" in cmd:
            return (0, "No endpoints configured!\n", "")  # nothing running -> not an adopt
        return (73, "", "")

    monkeypatch.setattr(remote, "ssh_exec", fake)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    with pytest.raises(RuntimeError, match=r"rc=73.*no output"):
        await cli.start(LONG_NAME)


async def test_gce_and_start_tolerate_an_empty_env_setup(monkeypatch):
    # An entry with no env_setup used to produce `export COLUMNS=…;  && globus-compute-endpoint …` — a shell
    # syntax error (Expanse, live 2026-09-04).
    cmds: list[str] = []

    async def fake(target, cmd, **kw):
        cmds.append(cmd)
        return (0, WIDE_LIST if "endpoint list" in cmd else "HPCB_HOST=h\n", "")

    monkeypatch.setattr(remote, "ssh_exec", fake)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "")
    await cli.status(LONG_NAME)
    await cli.start(LONG_NAME)
    assert cmds and all("&& globus-compute-endpoint" not in c and " && " not in c.split("globus-compute-endpoint")[0] for c in cmds)
    assert any("export COLUMNS=400; globus-compute-endpoint list" in c for c in cmds)
    cli2 = RemoteEndpointCLI(SshTarget("h", "u", "k"), "module load python")
    cmds.clear()
    await cli2.status(LONG_NAME)
    assert any("module load python && globus-compute-endpoint list" in c for c in cmds)


# ---- teardown reports what was MEASURED (review 2026-09-05, Fix-now #1) ------------------------------------------

async def test_teardown_aborts_when_ssh_itself_fails_and_keeps_the_record(tmp_path):
    # A one-time-code facility whose shared connection expired (or any host that refuses our key): `stop`
    # never ran on the login node. NOTHING else may be attempted, the report must say so, and the record
    # (pin + seeded-flag) must survive so a later teardown can still wipe the token copy.
    from hpc_bridge.state import EndpointRecord, LoginNodeStore

    class _Denied(_FakeRemoteCLI):
        async def stop(self, name):
            self.calls.append(("stop", name))
            return (255, "u@login02: Permission denied (gssapi-with-mic,keyboard-interactive,hostbased).")

    cli = _Denied()
    store = LoginNodeStore(tmp_path / "e.json")
    store.put(EndpointRecord(endpoint_id="eid-1", login_host="login02", alias="a", user="u", key_path=None,
                             name="hpc-bridge", provisioned_at="t", seeded_credentials=True))
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias="a")
    report = await fac.teardown("eid-1", wipe_credentials=True)
    assert report["ssh_failed"] and not report["stopped"] and not report["deleted"] and not report["credentials_wiped"]
    assert "keyboard-interactive" in report["error"]
    assert _kinds(cli) == ["stop"]  # no delete, no wipe, no close: nothing pretended
    assert store.get(alias="a", name="hpc-bridge").seeded_credentials is True  # the record stays


async def test_teardown_treats_a_gce_stop_error_as_stopped_only_if_the_daemon_is_gone():
    class _Traceback(_FakeRemoteCLI):
        async def stop(self, name):
            self.calls.append(("stop", name))
            return (1, "Traceback … psutil.NoSuchProcess")

    cli = _Traceback(status="configured")  # the daemon did die on the way out
    assert (await SlurmFacility(_profile(), cli=cli).teardown("eid-1"))["stopped"] is True
    cli2 = _Traceback(status="running")  # it did not
    assert (await SlurmFacility(_profile(), cli=cli2).teardown("eid-1"))["stopped"] is False


async def test_teardown_keeps_the_record_when_delete_failed(tmp_path):
    from hpc_bridge.state import EndpointRecord, LoginNodeStore

    class _NoDelete(_FakeRemoteCLI):
        async def delete(self, name):
            self.calls.append(("delete", name))
            return False

    store = LoginNodeStore(tmp_path / "e.json")
    store.put(EndpointRecord(endpoint_id="eid-1", login_host="login02", alias="a", user="u", key_path=None,
                             name="hpc-bridge", provisioned_at="t", seeded_credentials=True))
    report = await SlurmFacility(_profile(), cli=_NoDelete(), store=store, alias="a").teardown("eid-1")
    assert report["stopped"] and not report["deleted"]
    assert store.get(alias="a", name="hpc-bridge") is not None  # the directory is still there: so is our record


async def test_teardown_credentials_wiped_is_measured():
    class _WipeFails(_FakeRemoteCLI):
        async def wipe_storage_db(self):
            self.calls.append(("wipe", "hpc-bridge"))
            return False

    fac = SlurmFacility(_profile(), cli=_WipeFails())
    fac._seeded_credentials = True
    report = await fac.teardown("eid-1", wipe_credentials=True)
    assert report["credentials_wiped"] is False  # rm failed: never claim the token copy is gone


def _render_pbs(account_default: str, **uec):
    """The PBS template as the endpoint's manager renders it (Jinja over the client's user_endpoint_config), with the
    plugin's json.dumps'd defaults substituted."""
    import jinja2
    import yaml

    from hpc_bridge.facility import remote as rm
    tpl = rm._PBS_TEMPLATE
    for k, v in {"@@MAXW@@": "1", "@@IFACE@@": '"eth0"', "@@IDLE@@": "60.0", "@@PARTITION@@": '"debug"',
                 "@@ACCOUNT@@": account_default, "@@WALLTIME@@": '"00:30:00"', "@@WORKER_INIT@@": '"true"',
                 "@@EAGER@@": "0", "@@NODES@@": "1", "@@MAXBLK@@": "1"}.items():
        tpl = tpl.replace(k, v)
    return yaml.safe_load(jinja2.Template(tpl).render(uec))["engine"]["provider"]


def test_pbs_template_omits_an_empty_account():
    """parsl's PBSProProvider emits `-A <account>` for any non-None account; an empty string made qsub swallow the
    script path as the account name (an empty STDIN job, exit 127 — fake OpenPBS 2026-09-06). No account ⇒ no key."""
    assert "account" not in _render_pbs('""')                                   # no facility account
    assert "account" not in _render_pbs('""', account='""')                     # the manager's sanitizer quotes "" as '""'
    assert _render_pbs('"cis250223"')["account"] == "cis250223"                # the facility default
    assert _render_pbs('""', account='"proj"')["account"] == "proj"             # the agent's choice
    assert _render_pbs('"cis250223"')["queue"] == "debug"


def test_a_shared_partition_entry_can_ask_for_a_non_exclusive_block():
    # Parsl's SlurmProvider defaults exclusive=True (`#SBATCH --exclusive`, a whole node): on Expanse `shared` (QOS
    # max 127 CPUs/job, 128-core nodes) the block pended forever as QOSMaxCpuPerJobLimit (2026-10-06)
    import dataclasses

    f = SlurmFacility(dataclasses.replace(_profile(), exclusive=False), cli=None)
    tmpl, defaults = f.config_template(Profile(mode="interactive"))
    prov = _render(tmpl, {**defaults, **shape_config("compute")})["engine"]["provider"]
    assert prov["exclusive"] is False
    # unset: no key, so Parsl's own default stands (no change for every other facility)
    tmpl, defaults = SlurmFacility(_profile(), cli=None).config_template(Profile(mode="interactive"))
    assert "exclusive" not in _render(tmpl, {**defaults, **shape_config("compute")})["engine"]["provider"]
    # a PBS facility has no such knob: it is never passed
    pbs = dataclasses.replace(_profile(), exclusive=False, scheduler="pbs")
    assert "exclusive" not in SlurmFacility(pbs, cli=None).config_template(Profile(mode="interactive"))[1]


def test_the_expanse_entry_asks_for_a_non_exclusive_block():
    from pathlib import Path

    from hpc_bridge.catalog.bundled import BundledCatalog
    from hpc_bridge.facility.remote import profile_from_catalog_entry

    seeds = Path(remote.__file__).resolve().parents[1] / "catalog" / "seed"
    entry = next(e for e in BundledCatalog(seeds / "sdsc-expanse.yaml").entries() if e.id == "expanse")
    assert entry.defaults.exclusive is False
    assert profile_from_catalog_entry(entry, user="u", account="chi152").exclusive is False
