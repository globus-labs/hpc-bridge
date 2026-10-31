"""Hermetic: the Perlmutter report's fake-cluster RED proofs (2026-10-29) — the `nersc` profile, the harness support they
need (node actions, the script's world channel, seeded login pins), and each scripted scenario's SCRIPT and graders on
what main answered (live runs, quoted) and on what a fixed build would answer. No agent, no cluster."""
from __future__ import annotations

import asyncio
import json
import sys
import threading
import time
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import byo_script  # noqa: E402
import cached_password_otp as cpo  # noqa: E402
import home_outside_home as hoh  # noqa: E402
import kept_entry_after_failure as kea  # noqa: E402
import node_actions  # noqa: E402
import noisy_failure_tail as nft  # noqa: E402
import partition_proposal_policy as ppp  # noqa: E402
import script_runner  # noqa: E402
import stale_login_pin as slp  # noqa: E402
import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all, no_password_handling, staged_connects  # noqa: E402

FC = HERE.parent / "fakecluster"
NERSC = (cpo, hoh, nft, ppp)
SITE = (slp, kea)


def _run_module(monkeypatch):
    """run.py, importable without the Claude Agent SDK (only the jail has it): a stub, as test_server_stderr does."""
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "PermissionResultAllow", "PermissionResultDeny", "AssistantMessage", "UserMessage",
                 "ResultMessage", "SystemMessage", "ToolUseBlock", "ToolResultBlock", "TextBlock", "HookMatcher",
                 "ClaudeSDKClient"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    for m in ("runner", "run"):
        sys.modules.pop(m, None)
    import run
    return run


# ---- the profile ------------------------------------------------------------------------------------------------------

def test_the_nersc_profile_layers_on_site_with_perlmutters_shape():
    m = targets.load_profile("nersc")
    caps = m["capabilities"]
    assert m["layers"] == ["site", "nersc"] and caps["login_nodes"] == 2 and caps["harness_ssh_port"] == 2200
    assert caps["mfa"] == "password_otp" and caps["home_root"] == "/global/homes" and caps["default_partition"] == "system"
    assert caps["partitions"] == ["system", "regular_milan_ss11", "shared_milan_ss11", "realtime_ss11", "gpu_ss11"]
    conf = (FC / "profiles" / "nersc" / "slurm.conf").read_text()
    assert "NodeName=c[1-2] CPUs=4 RealMemory=4000 Features=cpu" in conf and "Features=gpu" in conf
    assert "PartitionName=system             Nodes=c[1-3] Default=YES" in conf
    assert "AllowQos=realtime" in conf and "JobSubmitPlugins=lua" in conf
    lua = (FC / "profiles" / "nersc" / "job_submit.lua").read_text()
    assert 'slurm.log_user("Job request does not match any supported policy.")' in lua
    assert "job_desc.partition ~= nil and job_desc.qos ~= nil" in lua and '"regular_milan_ss11"' in lua
    early = (FC / "profiles" / "nersc" / "setup.d" / "early-nersc.sh").read_text()
    assert "/global/homes/${u:0:1}/$u" in early and "ln -sfn /home/.global /global" in early
    assert early.index("site login environment") < early.index("cat /etc/skel/.bashrc")   # the noise ABOVE the guard
    login = (FC / "profiles" / "nersc" / "setup.d" / "login-nersc.sh").read_text()
    assert "PasswordAuthentication yes" in login and "pam_google_authenticator" in login
    assert "AuthorizedKeysFile=/etc/ssh/hpcb-harness-keys/%u" in login and "-p 2200" in login
    ep = (FC / "entrypoint.sh").read_text()
    assert "run_setup early" in ep and 'h="$(getent passwd "$u" | cut -d: -f6)"' in ep   # homes from passwd


def test_each_proof_runs_only_where_its_quirk_is():
    caps = {p: targets.load_profile(p)["capabilities"] for p in ("default", "site", "nersc", "totp", "slices")}
    for sc in NERSC:
        assert sc.OPERATORS == ("script",) and sc.TARGETS == ("fake",)
        assert targets.meets(sc.REQUIRES, caps["nersc"])[0], sc.__name__
        assert not any(targets.meets(sc.REQUIRES, caps[p])[0] for p in ("default", "site", "totp")), sc.__name__
    for sc in SITE:
        assert targets.meets(sc.REQUIRES, caps["site"])[0], sc.__name__
        assert not targets.meets(sc.REQUIRES, caps["default"])[0] and not targets.meets(sc.REQUIRES, caps["nersc"])[0]
    assert slp.NODE_ACTIONS is True


# ---- node actions -----------------------------------------------------------------------------------------------------

def _server(tmp_path, ran=None):
    ran = [] if ran is None else ran

    def execute(container, cmd):
        ran.append((container, cmd))
        return 0, f"HPCB_NODE {container} ok"

    srv = node_actions.Server(tmp_path / "req", user="hpcbridge-test-03", hosts=["login01.hpcb.test", "login02.hpcb.test"],
                              containers=lambda: {"login02.hpcb.test": "hpcb-fake-login02-1"}, execute=execute)
    return srv, ran


def test_a_node_action_runs_only_a_known_action_on_a_pool_node_and_is_undone(tmp_path):
    srv, ran = _server(tmp_path)
    assert srv.act("rm_rf", "login02.hpcb.test")[0] == 2
    assert srv.act("sshd_off", "evil.example.org")[0] == 2
    assert "no running container" in srv.act("sshd_off", "login01.hpcb.test")[1]
    assert ran == []
    assert srv.act("sshd_off", "login02.hpcb.test") == (0, "HPCB_NODE hpcb-fake-login02-1 ok")
    container, cmd = ran[-1]
    assert container == "hpcb-fake-login02-1" and "ListenAddress 127.0.0.1" in cmd and "kill -HUP" in cmd
    assert "pkill -KILL -u hpcbridge-test-03" in cmd and "{user}" not in cmd
    assert srv.pending_undo == {"login02.hpcb.test": "sshd_on"}
    srv.undo_all()                                              # the cell ended with the node away: given back
    assert "rm -f /etc/ssh/sshd_config.d/zz-hpcb-node-down.conf" in ran[-1][1] and srv.pending_undo == {}
    srv.act("sshd_off", "login02.hpcb.test")
    srv.act("sshd_on", "login02.hpcb.test")                     # the script gave it back itself: nothing left to undo
    assert srv.pending_undo == {}


def test_the_jail_asks_and_the_host_answers_through_the_runs_volume(tmp_path):
    srv, _ran = _server(tmp_path)
    jail = node_actions.NodeActions(tmp_path / "req")
    stop = threading.Event()

    def host():
        while not stop.is_set():
            srv.answer_new()
            time.sleep(0.05)

    t = threading.Thread(target=host, daemon=True)
    t.start()
    try:
        assert jail.run("sshd_off", "login02.hpcb.test", timeout_s=5, sleep=lambda s: time.sleep(0.05)) == (
            0, "HPCB_NODE hpcb-fake-login02-1 ok")
        assert jail.run("bogus", "login02.hpcb.test", timeout_s=5, sleep=lambda s: time.sleep(0.05))[0] == 2
    finally:
        stop.set()
        t.join(2)
    assert [json.loads(x)["id"] for x in node_actions.done_path(tmp_path / "req").read_text().splitlines()] == [1, 2]
    assert node_actions.NodeActions(None).run("sshd_off", "login02.hpcb.test")[0] == 2   # no channel: said, not hung
    unanswered = node_actions.NodeActions(tmp_path / "other")
    assert unanswered.run("sshd_on", "login02.hpcb.test", timeout_s=0.2, sleep=lambda s: time.sleep(0.05))[0] == 124


def test_a_script_reaches_the_world_channel_and_a_seed_writes_the_login_pin(monkeypatch):
    seen = []

    def world(cmd, *, timeout, host):
        seen.append((cmd, timeout, host))
        return 0, "ok"

    ops = script_runner.ScriptOps(None, facility="f", ssh_host="login", world=world)
    assert asyncio.run(ops.world("true", host="login02.hpcb.test", timeout=30)) == (0, "ok")
    assert seen == [("true", 30, "login02.hpcb.test")]
    assert asyncio.run(script_runner.ScriptOps(None, facility="f", ssh_host="login").world("true"))[0] == 255
    run = _run_module(monkeypatch)
    pin = {"endpoint_id": "e", "login_host": "login02.hpcb.test", "alias": "hpcb-cluster", "user": None,
           "key_path": None, "name": "hpc-bridge-fake-1", "provisioned_at": "2026-10-13T00:00:00+00:00"}
    run._seed_facility_cache(SimpleNamespace(SEED_FACILITY_CACHE={"login": {"ssh_host": "login"}}, SEED_LOGIN_PINS=[pin]))
    state = byo_script.state_dir()
    assert json.loads((state / "endpoints.json").read_text()) == {"hpcb-cluster::hpc-bridge-fake-1": pin}
    assert byo_script.login_pin("hpcb-cluster") == "login02.hpcb.test" and byo_script.cached_hosts() == ["login"]
    assert byo_script.forget_cached("login") and byo_script.cached_hosts() == [] and not byo_script.forget_cached("login")


# ---- a stand-in for the script operator -------------------------------------------------------------------------------

class _Ops:
    """script_runner.ScriptOps' surface, answering from `answers` (tool -> list, consumed in order)."""

    def __init__(self, answers, world=None):
        self.answers = {k: list(v) for k, v in answers.items()}
        self.calls, self.texts, self.worlds = [], [], []
        self.facility, self.ssh_host = "fake-1", "login"
        self._world = world or (lambda cmd, host: (0, ""))

    async def call(self, tool, **args):
        res = self.answers[tool].pop(0)
        self.calls.append((tool, args, res))
        return res

    async def world(self, cmd, *, host=None, timeout=60):
        self.worlds.append((cmd, host))
        return self._world(cmd, host)

    def say(self, text):
        self.texts.append(text)

    def abort(self, reason):
        raise script_runner.ScriptAbort(reason)

    async def sleep(self, s):
        return None

    def trace(self) -> Trace:
        return Trace([ToolCall.of(f"mcp__hpc_bridge__{t}", a, r) for t, a, r in self.calls], self.texts)


class _Ctx:
    def __init__(self, trace, out, rc=0):
        self.trace, self._out, self._rc, self.cmds = trace, out, rc, []

    def ssh(self, cmd, timeout=120):
        self.cmds.append(cmd)
        return self._rc, self._out


def _gated(sc, t: Trace, operator="script") -> dict[str, str]:
    from invariants import regrade_staged
    results = regrade_staged(check_all(t), t, staged_connects(t, sc, operator)) + [fn(t) for fn in sc.EXTRA_INVARIANTS]
    by = {r.name: r for r in results}
    return {n: by[n].detail for n in sc.EXPECT_OK if not by[n].ok}


def _proposal(**d) -> dict:
    return {"phase": "proposed_facility_details", "notice": "probed the login node and proposed this config",
            "proposed_details": {"ssh_host": "login", "interface": "eth0", "env_setup": "<hpc-bridge's line>",
                                 "scratch_root": "/global/homes/h/{user}/.hpc-bridge", "partition": "system",
                                 "scheduler": "slurm", "partitions": [{"name": "system"}], **d}}


_UP = {"phase": "needs_account", "notice": "login node is up; your allocations are listed"}


# ---- partition_proposal_policy ----------------------------------------------------------------------------------------

_REFUSED = ("HPCB_SB sbatch: error: Job request does not match any supported policy.\n"
            "HPCB_SB allocation failure: Unspecified error\nHPCB_RC=1\n")
_ACCEPTED = ("Lmod is automatically replacing \"PrgEnv-gnu/8.5.0\" with \"PrgEnv-nvidia/8.5.0\".\n"
             "HPCB_SB sbatch: Job 1 to start at 2026-10-29T22:37:45 using 1 processors on nodes c1 in partition "
             "regular_milan_ss11\nHPCB_RC=0\n")


def test_the_proposed_terms_are_offered_to_sbatch_test_only_as_a_batch_script():
    ops = _Ops({"connect_facility": [_proposal()]})
    asyncio.run(ppp.SCRIPT(ops))
    t = ops.trace()
    assert ppp.proposed_partition(t).detail == "partition 'system', scheduler_options None"
    main = asyncio.run(ppp.proposed_terms_accepted(_Ctx(t, _REFUSED)))     # main, live 2026-10-29
    assert not main.ok and "refuses the proposed partition 'system'" in main.detail
    assert "does not match any supported policy" in main.detail
    fixed = Trace([ToolCall.of("mcp__hpc_bridge__connect_facility", {}, _proposal(
        partition="debug", scheduler_options="#SBATCH --constraint=cpu"))])
    ctx = _Ctx(fixed, _ACCEPTED)
    ok = asyncio.run(ppp.proposed_terms_accepted(ctx))
    assert ok.ok and "regular_milan_ss11" in ok.detail
    assert "#SBATCH --partition=debug" in ctx.cmds[0] and "#SBATCH --constraint=cpu" in ctx.cmds[0]
    assert "sbatch --test-only" in ctx.cmds[0]
    down = asyncio.run(ppp.proposed_terms_accepted(_Ctx(t, "ssh: connect to host login port 22: refused", rc=255)))
    assert not down.ok and down.detail.startswith("UNVERIFIABLE")
    assert "premise not met" in asyncio.run(ppp.proposed_terms_accepted(_Ctx(Trace([]), _ACCEPTED))).detail


# ---- home_outside_home ------------------------------------------------------------------------------------------------

_VENV_DENIED = {"phase": "failed", "notice": (
    "could not build hpc-bridge's endpoint venv for /home/hpcbridge-test/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11 on "
    "login (globus-compute-endpoint 4.18.0 on Python 3.11): mkdir: cannot create directory '/home/hpcbridge-test': "
    "Permission denied. Fix that, then connect_facility again.")}


def test_the_home_script_confirms_the_proposal_and_waits_for_the_login_shape():
    ops = _Ops({"connect_facility": [_proposal(), {"phase": "provisioning"}, _UP]})
    asyncio.run(hoh.SCRIPT(ops))
    confirm = ops.calls[1][1]["details"]
    assert "env_setup" not in confirm and "partitions" not in confirm and confirm["ssh_host"] == "login"
    assert _gated(hoh, ops.trace()) == {}
    main = _Ops({"connect_facility": [_proposal(), _VENV_DENIED]})       # main, live 2026-10-29: the run goes on
    asyncio.run(hoh.SCRIPT(main))
    failed = _gated(hoh, main.trace())
    assert set(failed) == {"login_shape_up"} and "Permission denied" in failed["login_shape_up"]
    assert hoh.venv_failure_names_home(main.trace()).detail.startswith("the failure names '/home/hpcbridge-test/")
    with pytest.raises(script_runner.ScriptAbort, match="did not propose"):
        asyncio.run(hoh.SCRIPT(_Ops({"connect_facility": [{"phase": "failed", "notice": "NO SSH ACCESS"}]})))


def test_the_world_check_reads_where_the_venv_landed():
    home = "/global/homes/h/hpcbridge-test"
    t = Trace([])
    none = asyncio.run(hoh.venv_under_home(_Ctx(t, f"HPCB_HOME={home}\nHPCB_DONE\n")))
    assert not none.ok and f"$HOME={home}/hpc-bridge/" in none.detail
    venv = f"{home}/hpc-bridge/gce-venv-endpoint-4.18.0-py3.11"
    ok = asyncio.run(hoh.venv_under_home(_Ctx(t, f"noise\nHPCB_HOME={home}\nHPCB_VENV={venv}\nHPCB_DONE\n")))
    assert ok.ok and venv in ok.detail
    assert asyncio.run(hoh.venv_under_home(_Ctx(t, "Permission denied", rc=255))).detail.startswith("UNVERIFIABLE")


# ---- noisy_failure_tail -----------------------------------------------------------------------------------------------

_NOISE = ('Lmod is automatically replacing "PrgEnv-gnu/8.5.0" with "PrgEnv-nvidia/8.5.0".\n'
          "Due to MODULEPATH changes, the following have been reloaded:\n"
          "error: could not lock config file /global/homes/h/hpcbridge-test/.gitconfig: File exists\n")


def test_a_failure_behind_startup_noise_must_quote_its_own_last_line():
    head_cut = {"phase": "failed", "notice": "hpc-bridge error: ConfigStepFailed: remote configure failed: " + _NOISE}
    ops = _Ops({"connect_facility": [_proposal(), head_cut]})            # main, live 2026-10-29
    asyncio.run(nft.SCRIPT(ops))
    assert ops.calls[1][1]["details"]["env_setup"] == nft.FAILING_ENV_SETUP
    t = ops.trace()
    assert staged_connects(t, nft, "script") == frozenset({1})            # the premise is no #39 failure
    assert staged_connects(t, nft, "codex") == frozenset()
    failed = _gated(nft, t)
    assert set(failed) == {"failure_quotes_last_line"} and "Lmod" in failed["failure_quotes_last_line"]
    assert not nft.failure_not_startup_noise(t).ok
    tail = {"phase": "failed", "notice": f"remote configure failed: bash: line 1: {nft.SITE_SCRIPT}: No such file or "
                                         "directory (after 9 lines of login-shell output)"}
    good = _Ops({"connect_facility": [_proposal(), tail]})
    asyncio.run(nft.SCRIPT(good))
    assert _gated(nft, good.trace()) == {} and nft.failure_not_startup_noise(good.trace()).ok
    with pytest.raises(script_runner.ScriptAbort, match="premise is unmet"):
        asyncio.run(nft.SCRIPT(_Ops({"connect_facility": [_proposal(), _UP]})))


# ---- cached_password_otp ----------------------------------------------------------------------------------------------

_NO_ACCESS = {"phase": "failed", "notice": (
    "NO SSH ACCESS to login: the login-node SSH as 'hpcbridge-test' was refused (hpcbridge-test@login: Permission denied "
    "(publickey,password,keyboard-interactive).). hpc-bridge needs an account on this facility and key-based SSH to its "
    "login node.")}
_HANDOFF = {"phase": "needs_preauth", "preauth_code_ok": False,
            "preauth_command": "ssh -fN -o ControlMaster=yes -o ControlPath=/home/agent/.hpc-bridge/cm/%C login",
            "notice": "login needs a one-time interactive login (a password and/or MFA/Duo). Ask the USER to run this"}


def test_the_cached_facility_must_hand_off_like_a_first_contact_does():
    ops = _Ops({"connect_facility": [_NO_ACCESS, _HANDOFF]})              # main, live 2026-10-29
    asyncio.run(cpo.SCRIPT(ops))
    assert [a["ssh_host"] for _t, a, _r in ops.calls] == [cpo.CACHED_HOST, cpo.FRESH_HOST]
    assert ops.calls[1][1]["facility"] != ops.calls[0][1]["facility"]
    t = ops.trace()
    failed = _gated(cpo, t)
    assert set(failed) == {"cached_facility_handoff"} and "NO SSH ACCESS" in failed["cached_facility_handoff"]
    assert no_password_handling(t).ok, "the script's own words never talk of a password"
    fixed = _Ops({"connect_facility": [_HANDOFF, _HANDOFF]})
    asyncio.run(cpo.SCRIPT(fixed))
    assert _gated(cpo, fixed.trace()) == {} and no_password_handling(fixed.trace()).ok
    code = dict(_HANDOFF, preauth_code_ok=True)                           # a code prompt is the key + code shape
    assert not cpo.cached_facility_handoff(Trace([ToolCall.of("connect_facility", {"ssh_host": "login"}, code)])).ok
    assert cpo.SEED_FACILITY_CACHE[cpo.CACHED_HOST]["ssh_host"] == cpo.CACHED_HOST
    assert "authorized_keys" in cpo.SETUP[0] and "authorized_keys" in cpo.CLEANUP[0]


# ---- kept_entry_after_failure -----------------------------------------------------------------------------------------

_KEPT = {"phase": "failed", "notice": ("hpc-bridge error: ConfigStepFailed: remote configure failed: bash: line 1: "
                                       f"/home/hpcbridge-test/hpc-bridge/{kea.MARKER}/bin/activate: No such file or directory")}


def _seeded(monkeypatch):
    _run_module(monkeypatch)._seed_facility_cache(kea)
    assert byo_script.cached_hosts() == [kea.HOST_A, kea.HOST_B]


def test_a_failed_cached_config_keeps_winning_on_main_and_must_not(monkeypatch):
    _seeded(monkeypatch)
    ops = _Ops({"connect_facility": [_KEPT, _KEPT, _KEPT, _KEPT]})       # main, live 2026-10-29
    asyncio.run(kea.SCRIPT(ops))
    assert [(a["facility"][-2:], a["ssh_host"]) for _t, a, _r in ops.calls] == [
        ("-a", kea.HOST_A), ("-a", kea.HOST_A_NEW), ("-b", kea.HOST_B), ("-b", kea.HOST_B)]
    assert byo_script.cached_hosts() == [kea.HOST_A]                      # (b)'s record deleted, by hand, mid-run
    assert "token store" in ops.worlds[0][0] or "storage.db" in ops.worlds[0][0]
    failed = _gated(kea, ops.trace())
    assert set(failed) == {"explicit_host_not_ignored", "removed_record_resolved_afresh"}
    assert "was ignored" in failed["explicit_host_not_ignored"]
    assert "brought up again" in failed["removed_record_resolved_afresh"]


def test_a_fixed_build_probes_the_named_host_and_the_host_whose_record_is_gone(monkeypatch):
    _seeded(monkeypatch)
    a = _proposal(ssh_host=kea.HOST_A_NEW)
    b = _proposal(ssh_host=kea.HOST_B)
    ops = _Ops({"connect_facility": [_KEPT, a, _KEPT, b]})
    asyncio.run(kea.SCRIPT(ops))
    assert _gated(kea, ops.trace()) == {}
    refusal = {"phase": "failed", "notice": "this session keeps a configuration for it; for 'login', connect another id"}
    _seeded(monkeypatch)
    ops = _Ops({"connect_facility": [_KEPT, refusal, _KEPT, b]})
    asyncio.run(kea.SCRIPT(ops))
    assert kea.explicit_host_not_ignored(ops.trace()).ok                 # a refusal that names the host, without SSH
    _seeded(monkeypatch)
    with pytest.raises(script_runner.ScriptAbort, match="did not fail as seeded"):
        asyncio.run(kea.SCRIPT(_Ops({"connect_facility": [a]})))


# ---- stale_login_pin --------------------------------------------------------------------------------------------------

_DEAD = {"phase": "failed", "notice": (
    "CANNOT REACH login01.hpcb.test: ssh: connect to host login01.hpcb.test port 22: Connection refused. Check the "
    "login host name and your network/VPN, then call connect_facility again.")}
_DROPPED = dict(_DEAD, notice=_DEAD["notice"] + " (The remembered login-node pin was dropped: the next connect "
                "resolves the facility's host afresh.)")


class _Nodes:
    def __init__(self):
        self.ran = []

    def run(self, action, host, **_kw):
        self.ran.append((action, host))
        return 0, f"HPCB_NODE {host} {action}"


def _stale_ops(monkeypatch, after, *, pin="login01.hpcb.test", stop_rc="rc=0", reach_rc=255):
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-1")
    nodes = _Nodes()
    monkeypatch.setattr(slp.NodeActions, "from_env", classmethod(lambda cls: nodes))
    state = byo_script.state_dir()
    state.mkdir(parents=True, exist_ok=True)
    (state / "endpoints.json").write_text(json.dumps({f"{slp.ALIAS}::hpc-bridge-fake-1": {
        "alias": slp.ALIAS, "login_host": pin, "name": "hpc-bridge-fake-1"}}))

    def world(cmd, host):
        if "HPCB_STOP" in cmd:
            return 0, f"HPCB_STOP {host} {stop_rc} stopped"
        return reach_rc, "ssh: connect to host login01.hpcb.test port 22: Connection refused"

    ops = _Ops({"connect_facility": [_proposal(ssh_host=slp.ALIAS), {"phase": "provisioning"}, _UP, *after]}, world)
    return ops, nodes


def test_the_dead_pin_is_kept_on_main_and_the_node_is_given_back(monkeypatch):
    ops, nodes = _stale_ops(monkeypatch, [_DEAD, _DEAD])                 # main, live 2026-10-29
    asyncio.run(slp.SCRIPT(ops))
    assert nodes.ran == [("sshd_off", "login01.hpcb.test"), ("sshd_on", "login01.hpcb.test")]
    assert [h for _c, h in ops.worlds] == ["login01.hpcb.test", "login01.hpcb.test"]   # stop the manager, then probe
    assert [a.get("ssh_host") for _t, a, _r in ops.calls] == [slp.ALIAS] * 3 + [None, None]
    failed = _gated(slp, ops.trace())
    assert set(failed) == {"dead_pin_dropped", "bringup_reached_live_node"}
    assert failed["dead_pin_dropped"].startswith("the dead pin was kept: 'failed' 'CANNOT REACH login01.hpcb.test")


def test_a_dropped_pin_and_a_bring_up_on_the_live_node_pass(monkeypatch):
    ops, _nodes = _stale_ops(monkeypatch, [_DROPPED, {"phase": "provisioning"}, _UP])
    asyncio.run(slp.SCRIPT(ops))
    assert _gated(slp, ops.trace()) == {}
    at_once, _nodes = _stale_ops(monkeypatch, [{"phase": "provisioning"}, _UP])   # a fix that goes around at once
    asyncio.run(slp.SCRIPT(at_once))
    assert _gated(slp, at_once.trace()) == {}


@pytest.mark.parametrize("over, why", [
    ({"pin": None}, "not pinned to a login node"),
    ({"stop_rc": "none"}, "could not stop the manager"),
    ({"reach_rc": 0}, "still answers ssh"),
])
def test_the_stale_pin_script_aborts_on_an_unmet_premise_and_still_gives_the_node_back(monkeypatch, over, why):
    ops, nodes = _stale_ops(monkeypatch, [_DEAD, _DEAD], **over)
    with pytest.raises(script_runner.ScriptAbort, match=why):
        asyncio.run(slp.SCRIPT(ops))
    if nodes.ran:
        assert nodes.ran[-1][0] == "sshd_on"


def test_a_stopped_endpoint_still_read_online_is_a_premise_unmet(monkeypatch):
    zero = {"phase": "provisioning", "notice": "reconnected to the endpoint this session started (zero-SSH)."}
    ops, nodes = _stale_ops(monkeypatch, [zero])
    with pytest.raises(script_runner.ScriptAbort, match="reused as online"):
        asyncio.run(slp.SCRIPT(ops))
    assert nodes.ran[-1] == ("sshd_on", "login01.hpcb.test")
