"""The login-node pin, seen from the CLIENT (2026-11-03).

A user drove the fake cluster from the host: an ssh alias `hpcb-fake` -> `localhost:2222`, a Docker port-forward. After
the bootstrap, `provision` pinned `$SSH_CONNECTION`'s server field, 172.20.0.7: the node's address INSIDE the Docker
network, which the Mac cannot reach. A pin also replaced the ssh destination, so the alias's Port, ProxyJump and
IdentityFile were lost; and the dead-pin check probed port 22, not the alias's 2222, read a client outage, and kept the
bad pin for good. The harness never saw it: it runs inside the Docker network, where 172.20.0.7:22 answers.

The rules these tests hold (and the vantage rule behind them): anything learned on the FACILITY side — `hostname -f`,
`$SSH_CONNECTION` — reaches an ssh command only through `_routable_pin`, which pins a name or address only if the CLIENT
reaches it on the alias's own path (`ssh -G`: its port; never by address through a ProxyJump/ProxyCommand). A pin
overrides HostName only: ssh still connects to the alias. Every probe uses the alias's port.

Hermetic, faked only at the boundary: `ssh -G` (a fake OpenSSH client on PATH that answers from a fake ~/.ssh/config),
the client's TCP connects (`socket.create_connection`) and its resolver. Nothing connects anywhere.
"""
from __future__ import annotations

import ast
import contextlib
import inspect
import os
import socket
import stat
import sys
from pathlib import Path

import pytest

import hpc_bridge
from hpc_bridge import binding, config, connect
from hpc_bridge.facility import remote
from hpc_bridge.facility.remote import MachineProfile, RemoteEndpointCLI, SlurmFacility, SshTarget
from hpc_bridge.profile import Profile
from hpc_bridge.state import EndpointRecord, LoginNodeStore

pytestmark = [pytest.mark.skipif(sys.platform == "win32", reason="the fake OpenSSH client is a sh script"),
              pytest.mark.client_view]   # this module models the client's ssh config and network itself

NAME = "hpc-bridge-hpcb-fake"
NAT_ADDR = "172.20.0.7"        # $SSH_CONNECTION's server field on the fake cluster: Docker-internal

# The user's ~/.ssh/config, as `ssh -G` prints it (only the keys hpc-bridge reads).
SSH_CONFIG = {
    "hpcb-fake": "hostname localhost\nport 2222\nuser u\n",                                  # a port-forward
    "anvil": "hostname anvil.rcac.purdue.edu\nport 22\nuser u\n",                            # a standard alias
    "cluster": "hostname login.internal.site\nport 22\nuser u\nproxyjump bastion.site.edu\n",  # behind a bastion
    "jumpy": "hostname login.internal.site\nport 2222\nuser u\nproxyjump jump.site.edu\n",   # bastion AND port 2222
    "pc-h": "hostname login.internal.site\nport 22\nuser u\nproxycommand ssh gw -W %h:%p\n",
    "pc-fixed": "hostname login.internal.site\nport 22\nuser u\nproxycommand ssh gw nc login.internal.site 22\n",
    "gw": "hostname gw.example.edu\nport 2222\nuser u\n",                                    # a port-forward, public
}

FAKE_SSH = r'''#!/bin/sh
# A fake OpenSSH client that only answers `ssh -G <host>`: from $FAKE_SSH_CONFIG/<host>, else as ssh does for a host
# no Host block names. Anything else is a connection, which these tests never make.
[ "$1" = "-G" ] || { echo "fake ssh: a connection was attempted: $*" >&2; exit 255; }
shift; [ "$1" = "--" ] && shift
if [ -f "$FAKE_SSH_CONFIG/$1" ]; then cat "$FAKE_SSH_CONFIG/$1"; else printf 'hostname %s\nport 22\nuser u\n' "$1"; fi
'''


class Client:
    """The user's machine: its ~/.ssh/config (via the fake `ssh -G`), what its resolver knows, and which (host, port)
    take a TCP connection from it. Every TCP connect hpc-bridge makes is logged in `probes`."""

    def __init__(self, monkeypatch, tmp_path, *, answers=(), resolves=()) -> None:
        bin_dir, conf_dir = tmp_path / "bin", tmp_path / "ssh-config"
        bin_dir.mkdir()
        conf_dir.mkdir()
        for host, text in SSH_CONFIG.items():
            (conf_dir / host).write_text(text)
        ssh = bin_dir / "ssh"
        ssh.write_text(FAKE_SSH)
        ssh.chmod(ssh.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
        monkeypatch.setenv("FAKE_SSH_CONFIG", str(conf_dir))
        monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
        monkeypatch.delenv("HPC_BRIDGE_SSH_KEY", raising=False)
        monkeypatch.setattr(config, "_control_settings", lambda: (str(tmp_path / "cm"), 60))
        self.answers = set(answers)
        self.probes: list[tuple[str, int]] = []
        resolvable = set(resolves)
        monkeypatch.setattr(remote, "_resolves_from_here", lambda host: host in resolvable)

        def create_connection(address, timeout=None, *a, **kw):
            host, port = address[0], int(address[1])
            self.probes.append((host, port))
            if (host, port) not in self.answers:
                raise TimeoutError("timed out")
            return contextlib.nullcontext()

        monkeypatch.setattr(socket, "create_connection", create_connection)


class _Cli(RemoteEndpointCLI):
    """hpc-bridge's CLI with the login node's answers scripted: `start` says what `hostname -f` and `$SSH_CONNECTION`
    said there. Its target, `rebind` and argv are the real ones."""

    def __init__(self, alias: str, landed: tuple[str | None, str | None]) -> None:
        super().__init__(SshTarget(alias, "u", control_dir="/tmp/cm"), "true")
        self.landed = landed

    async def ensure_venv(self):
        pass

    async def status(self, name):
        return None

    async def configure(self, name, multi_user=False):
        pass

    async def write_config(self, name, manager, uep):
        pass

    async def start(self, name):
        return ("eid-1", *self.landed)

    async def endpoint_id(self, name):
        return "eid-1"

    async def clean_uep_pidfiles(self, eid):
        pass


def _profile() -> MachineProfile:
    return MachineProfile(name="m", endpoint_name=NAME, env_setup="true", interface="eth0", partition="debug",
                          account="", worker_init="true")


async def _provision(alias: str, host: str | None, addr: str | None, tmp_path) -> SlurmFacility:
    store = LoginNodeStore(tmp_path / "endpoints.json")
    fac = SlurmFacility(_profile(), _Cli(alias, (host, addr)), store=store, alias=alias)
    fac.config_template = lambda hpc: ("tmpl", {})
    fac.handle = await fac.provision(Profile())
    return fac


def _dest(argv: list[str]) -> str:
    """The ssh destination: the word after `--`."""
    return argv[argv.index("--") + 1]


def _opts(argv: list[str]) -> list[str]:
    """The `-o` values before `--`."""
    head = argv[: argv.index("--")]
    return [head[i + 1] for i, a in enumerate(head) if a == "-o"]


def _hostname_override(argv: list[str]) -> str | None:
    return next((o.split("=", 1)[1] for o in _opts(argv) if o.lower().startswith("hostname=")), None)


def _all_argvs(target) -> list[list[str]]:
    return [target.argv("true"), target.control_argv("check"), target.preauth_argv()]


def _record(store: LoginNodeStore, alias: str, pin: str) -> None:
    store.put(EndpointRecord(endpoint_id="eid-1", login_host=pin, alias=alias, user="u", key_path=None, name=NAME,
                             provisioned_at="2026-11-02T00:00:00+00:00", seeded_credentials=True))


# ---- 1. pin only what the client reaches on the alias's own path -----------------------------------------------------

async def test_a_port_forwarded_alias_never_pins_the_nodes_internal_address(monkeypatch, tmp_path):
    # the user's case: hpcb-fake -> localhost:2222; the node's own name is not known here, its address is Docker's
    client = Client(monkeypatch, tmp_path, answers={("localhost", 2222)})
    fac = await _provision("hpcb-fake", "login01.int.hpcb.test", NAT_ADDR, tmp_path)
    assert fac.handle.login_host is None                      # no pin: the alias it is
    target = fac.cli.target
    for argv in _all_argvs(target):
        assert _dest(argv) == "u@hpcb-fake" and _hostname_override(argv) is None and NAT_ADDR not in argv
    assert all(port == 2222 for _host, port in client.probes), client.probes


async def test_a_reachable_fqdn_on_a_standard_alias_is_pinned_as_today(monkeypatch, tmp_path):
    client = Client(monkeypatch, tmp_path, answers={("login03.anvil.rcac.purdue.edu", 22)},
                    resolves={"login03.anvil.rcac.purdue.edu"})
    fac = await _provision("anvil", "login03.anvil.rcac.purdue.edu", "128.211.2.3", tmp_path)
    assert fac.handle.login_host == "login03.anvil.rcac.purdue.edu"
    assert client.probes == [("login03.anvil.rcac.purdue.edu", 22)]   # checked from here, on the alias's port


async def test_an_unreachable_fqdn_falls_back_to_a_reachable_address_and_else_to_the_alias(monkeypatch, tmp_path):
    client = Client(monkeypatch, tmp_path, answers={("128.211.2.3", 22)}, resolves={"login03.anvil.rcac.purdue.edu"})
    fac = await _provision("anvil", "login03.anvil.rcac.purdue.edu", "128.211.2.3", tmp_path)
    assert fac.handle.login_host == "128.211.2.3"
    client.answers.clear()
    fac = await _provision("anvil", "login03.anvil.rcac.purdue.edu", "128.211.2.3", tmp_path)
    assert fac.handle.login_host is None


@pytest.mark.parametrize("alias", ["cluster", "jumpy", "pc-h"])
async def test_a_proxied_alias_never_pins_an_address(monkeypatch, tmp_path, alias):
    # behind a bastion the node's address is the bastion's business, and no TCP probe can tell
    client = Client(monkeypatch, tmp_path, answers={("10.1.2.3", 22), ("10.1.2.3", 2222)})
    fac = await _provision(alias, "login3.rcc.local", "10.1.2.3", tmp_path)
    assert fac.handle.login_host is None and client.probes == []
    for argv in _all_argvs(fac.cli.target):
        assert _dest(argv) == f"u@{alias}" and "10.1.2.3" not in " ".join(argv)


@pytest.mark.parametrize("alias", ["cluster", "jumpy", "pc-h"])
async def test_a_proxied_alias_pins_a_name_only_as_a_hostname_override_through_the_same_proxy(
        monkeypatch, tmp_path, alias):
    # the node's FQDN is no name the client resolves, but the bastion (or the ProxyCommand's %h) reaches it
    client = Client(monkeypatch, tmp_path)
    fac = await _provision(alias, "login3.site.edu", "10.1.2.3", tmp_path)
    assert fac.handle.login_host == "login3.site.edu" and client.probes == []
    for argv in _all_argvs(fac.cli.target):
        assert _dest(argv) == f"u@{alias}" and _hostname_override(argv) == "login3.site.edu"


async def test_a_proxycommand_that_never_reads_the_hostname_gets_no_pin(monkeypatch, tmp_path):
    # `ssh gw nc login.internal.site 22` goes where it says whatever HostName is: a pin would be a pin in name only
    Client(monkeypatch, tmp_path)
    fac = await _provision("pc-fixed", "login3.site.edu", "10.1.2.3", tmp_path)
    assert fac.handle.login_host is None


# ---- 2. a pin overrides HostName only ----------------------------------------------------------------------------------

def test_a_pinned_target_keeps_the_aliass_port_user_and_proxyjump(monkeypatch, tmp_path):
    Client(monkeypatch, tmp_path)
    for alias, trusted in (("gw", "[gw.example.edu]:2222"), ("cluster", "login.internal.site")):
        cli = RemoteEndpointCLI(SshTarget(alias, "u", control_dir="/tmp/cm"), "true")
        cli.rebind("login3.example.edu")
        assert cli.target.host == "login3.example.edu"         # the node the endpoint runs on, as every notice says
        for argv in _all_argvs(cli.target):
            # ssh connects to the ALIAS: its Port, User, ProxyJump and IdentityFile apply; only HostName is the pin's
            assert _dest(argv) == f"u@{alias}" and _hostname_override(argv) == "login3.example.edu", argv
            assert "-p" not in argv and not any(o.lower().startswith(("port=", "proxyjump=")) for o in _opts(argv))
        # the key is checked under the name known_hosts keeps for the alias — `[host]:port` off port 22
        assert f"HostKeyAlias={trusted}" in _opts(cli.target.argv("true"))
        assert f"HostKeyAlias={trusted}" in _opts(cli.target.preauth_argv())
        # the master socket is keyed by what ssh connects to: the alias's own and the pinned node's never collide,
        # and `ssh -O check` finds the pinned one (same ControlPath, same destination, same HostName)
        assert cli.target.control_argv("check")[-1] == f"u@{alias}"


def test_a_second_pin_keeps_the_alias_and_its_trusted_name(monkeypatch, tmp_path):
    Client(monkeypatch, tmp_path)
    cli = RemoteEndpointCLI(SshTarget("gw", "u", control_dir="/tmp/cm"), "true")
    cli.rebind("login3.example.edu")
    cli.rebind("login4.example.edu")
    argv = cli.target.argv("true")
    assert _dest(argv) == "u@gw" and _hostname_override(argv) == "login4.example.edu"
    assert "HostKeyAlias=[gw.example.edu]:2222" in _opts(argv)


# ---- 3. the dead-pin check: the alias's real port; an old bad pin clears itself -------------------------------------

def _pinned_facility(tmp_path, alias: str, pin: str) -> tuple[SlurmFacility, LoginNodeStore]:
    """A session pinned (before this fix, perhaps) to `pin`, its record saying so."""
    store = LoginNodeStore()
    _record(store, alias, pin)
    cli = RemoteEndpointCLI(SshTarget(alias, "u", control_dir=str(tmp_path / "cm")), "true")
    cli.rebind(pin)
    return SlurmFacility(_profile(), cli, store=store, alias=alias), store


def test_the_dead_pin_check_probes_the_aliass_own_port(monkeypatch, tmp_path):
    # the user's state: a connect through the pin met CANNOT REACH; localhost:22 is closed on a Mac, :2222 forwards
    client = Client(monkeypatch, tmp_path, answers={("localhost", 2222)})
    fac, store = _pinned_facility(tmp_path, "hpcb-fake", NAT_ADDR)
    client.probes.clear()
    assert connect._drop_dead_pin(fac) is True
    rec = store.get(alias="hpcb-fake", name=NAME)
    assert rec is None or rec.login_host is None
    assert client.probes and all(port == 2222 for _host, port in client.probes), client.probes


def test_the_dead_pin_check_keeps_the_pin_through_a_client_outage(monkeypatch, tmp_path):
    Client(monkeypatch, tmp_path)   # nothing answers: the client's own network is down
    fac, store = _pinned_facility(tmp_path, "hpcb-fake", NAT_ADDR)
    assert connect._drop_dead_pin(fac) is False
    assert store.get(alias="hpcb-fake", name=NAME).login_host == NAT_ADDR


def test_the_dead_pin_check_never_strands_a_pin_behind_a_proxy(monkeypatch, tmp_path):
    # no TCP probe reaches past a bastion; the failure the caller saw is not read as the client's outage
    client = Client(monkeypatch, tmp_path)
    fac, store = _pinned_facility(tmp_path, "cluster", "login3.site.edu")
    assert connect._drop_dead_pin(fac) is True and client.probes == []
    rec = store.get(alias="cluster", name=NAME)
    assert rec is None or rec.login_host is None


def test_an_old_bad_pin_clears_itself_on_the_next_connect(monkeypatch, tmp_path):
    # endpoints.json from before this fix pins 172.20.0.7; the next connect builds the facility: the pin does not
    # answer on the alias's port, the alias does — the pin is forgotten, the record (who placed the token store) kept
    client = Client(monkeypatch, tmp_path, answers={("localhost", 2222)})
    _record(LoginNodeStore(), "hpcb-fake", NAT_ADDR)
    fac = binding._slurm_facility(_profile(), alias="hpcb-fake", user="u")
    for argv in _all_argvs(fac.cli.target):
        assert _dest(argv) == "u@hpcb-fake" and _hostname_override(argv) is None and NAT_ADDR not in argv
    rec = LoginNodeStore().get(alias="hpcb-fake", name=NAME)
    assert rec is not None and rec.login_host is None and rec.seeded_credentials
    assert all(port == 2222 for _host, port in client.probes), client.probes


def test_an_old_bad_pin_is_kept_when_nothing_answers(monkeypatch, tmp_path):
    Client(monkeypatch, tmp_path)   # a client outage: neither the pin nor the alias answers
    _record(LoginNodeStore(), "hpcb-fake", NAT_ADDR)
    binding._slurm_facility(_profile(), alias="hpcb-fake", user="u")
    assert LoginNodeStore().get(alias="hpcb-fake", name=NAME).login_host == NAT_ADDR


def test_a_pin_stays_in_use_through_a_client_outage(monkeypatch, tmp_path):
    # review 2's rule, kept: with the VPN off nothing answers, yet a reuse over the web needs no SSH — the session must
    # still name the node the manager runs on when a teardown comes once the network is back
    Client(monkeypatch, tmp_path)
    _record(LoginNodeStore(), "anvil", "login03.anvil.rcac.purdue.edu")
    fac = binding._slurm_facility(_profile(), alias="anvil", user="u")
    argv = fac.cli.target.argv("true")
    assert _dest(argv) == "u@anvil" and _hostname_override(argv) == "login03.anvil.rcac.purdue.edu"
    assert LoginNodeStore().get(alias="anvil", name=NAME).login_host == "login03.anvil.rcac.purdue.edu"


def test_a_good_pin_is_still_used_on_the_next_connect(monkeypatch, tmp_path):
    Client(monkeypatch, tmp_path, answers={("login03.anvil.rcac.purdue.edu", 22)},
           resolves={"login03.anvil.rcac.purdue.edu"})
    _record(LoginNodeStore(), "anvil", "login03.anvil.rcac.purdue.edu")
    fac = binding._slurm_facility(_profile(), alias="anvil", user="u")
    assert fac.cli.target.host == "login03.anvil.rcac.purdue.edu"
    argv = fac.cli.target.argv("true")
    assert _dest(argv) == "u@anvil" and _hostname_override(argv) == "login03.anvil.rcac.purdue.edu"
    assert LoginNodeStore().get(alias="anvil", name=NAME).login_host == "login03.anvil.rcac.purdue.edu"


# ---- the client-view convention: every probe and every argv reads the alias's own config -----------------------------

def test_tcp_answers_takes_no_default_port():
    for mod in (remote, connect):
        fn = getattr(mod, "_tcp_answers", None)
        if fn is not None:
            assert inspect.signature(fn).parameters["port"].default is inspect.Parameter.empty, mod.__name__


@pytest.mark.parametrize(("alias", "proxied"), [("hpcb-fake", False), ("jumpy", True)])
async def test_client_view_convention_every_probe_and_argv_follows_the_alias(monkeypatch, tmp_path, alias, proxied):
    # an alias on port 2222, direct and behind a ProxyJump: every TCP probe hpc-bridge makes uses 2222 (or none is
    # made through the bastion), and every ssh argv connects to the alias
    client = Client(monkeypatch, tmp_path, answers={("localhost", 2222), ("login3.site.edu", 2222)},
                    resolves={"login3.site.edu"})
    fac = await _provision(alias, "login3.site.edu", NAT_ADDR, tmp_path)           # the bootstrap's pin
    assert fac.handle.login_host == "login3.site.edu"
    _record(LoginNodeStore(), alias, NAT_ADDR)
    rebuilt = binding._slurm_facility(_profile(), alias=alias, user="u")             # the next connect's
    pinned, _store = _pinned_facility(tmp_path, alias, "login3.site.edu")
    connect._drop_dead_pin(pinned)                                                   # the dead-pin check
    if proxied:
        assert client.probes == []
    else:
        assert client.probes and all(port == 2222 for _host, port in client.probes), client.probes
    for target in (fac.cli.target, rebuilt.cli.target, pinned.cli.target):
        for argv in _all_argvs(target):
            assert _dest(argv) == f"u@{alias}", argv
            override = _hostname_override(argv)
            assert override == (None if target.host == alias else target.host), argv
            assert NAT_ADDR not in " ".join(argv)


# ---- the pre-auth line the user runs in their own terminal ------------------------------------------------------------

async def test_the_preauth_command_under_a_port_forward_is_the_alias(monkeypatch, tmp_path):
    Client(monkeypatch, tmp_path, answers={("localhost", 2222)})
    fac = await _provision("hpcb-fake", "login01.int.hpcb.test", NAT_ADDR, tmp_path)
    cmd = fac.cli.target.preauth_command()
    assert cmd.endswith("-- u@hpcb-fake") and NAT_ADDR not in cmd and "HostName" not in cmd, cmd


async def test_the_preauth_command_under_a_reachable_pin_overrides_hostname_only(monkeypatch, tmp_path):
    Client(monkeypatch, tmp_path, answers={("login3.example.edu", 2222)}, resolves={"login3.example.edu"})
    fac = await _provision("gw", "login3.example.edu", "10.9.8.7", tmp_path)
    cmd = fac.cli.target.preauth_command()
    assert cmd.endswith("-- u@gw") and "HostName=login3.example.edu" in cmd, cmd
    assert "'HostKeyAlias=[gw.example.edu]:2222'" in cmd and "10.9.8.7" not in cmd


async def test_the_preauth_command_under_a_proxyjump_rides_the_same_bastion(monkeypatch, tmp_path):
    Client(monkeypatch, tmp_path)
    fac = await _provision("cluster", "login3.site.edu", "10.1.2.3", tmp_path)
    cmd = fac.cli.target.preauth_command()
    assert cmd.endswith("-- u@cluster") and "HostName=login3.site.edu" in cmd and "10.1.2.3" not in cmd, cmd
    fac = await _provision("cluster", "login3.rcc.local", "10.1.2.3", tmp_path)   # nothing to pin: the bare alias
    cmd = fac.cli.target.preauth_command()
    assert cmd.endswith("-- u@cluster") and "HostName" not in cmd and "10.1.2.3" not in cmd, cmd


# ---- the vantage rule, over the source -------------------------------------------------------------------------------

SRC = Path(hpc_bridge.__file__).parent
_SANCTIONED = "_routable_pin"
_PRODUCERS = {"RemoteEndpointCLI.start", "RemoteEndpointCLI._start_once"}   # print and parse HPCB_HOST / HPCB_ADDR


def _calls_pin(node: ast.AST) -> bool:
    """`node` holds a call of the sanctioned function: `_routable_pin(...)` or `asyncio.to_thread(_routable_pin, ...)`."""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            if isinstance(sub.func, ast.Name) and sub.func.id == _SANCTIONED:
                return True
            if any(isinstance(a, ast.Name) and a.id == _SANCTIONED for a in sub.args):
                return True
    return False


def _pin_call_names(fn: ast.AST) -> set[int]:
    """ids of the Name nodes that sit inside a call of the sanctioned function (as its arguments)."""
    inside: set[int] = set()
    for sub in ast.walk(fn):
        if isinstance(sub, ast.Call) and _calls_pin(sub) and not any(
                isinstance(c, ast.Call) and c is not sub and _calls_pin(c) for c in ast.walk(sub)):
            inside |= {id(n) for n in ast.walk(sub) if isinstance(n, ast.Name)}
    return inside


class _Vantage(ast.NodeVisitor):
    def __init__(self, rel: str) -> None:
        self.rel, self.stack, self.found = rel, [], []

    def _scope(self, node) -> None:
        self.stack.append(node.name)
        if not isinstance(node, ast.ClassDef):
            self._function(node)
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = visit_AsyncFunctionDef = visit_ClassDef = _scope

    def _say(self, node, what: str) -> None:
        self.found.append(f"{self.rel}:{node.lineno} {'.'.join(self.stack) or '<module>'}: {what}")

    def visit_Constant(self, node) -> None:
        # the markers the login node prints its own name / address under: read only where they are printed and parsed
        if isinstance(node.value, str) and ("HPCB_HOST" in node.value or "HPCB_ADDR" in node.value) \
                and ".".join(self.stack) not in _PRODUCERS:
            self._say(node, "reads HPCB_HOST/HPCB_ADDR outside the start that prints them")

    def _function(self, fn) -> None:
        where = ".".join(self.stack)
        assigns: list[tuple[int, str, ast.AST]] = []
        for sub in ast.walk(fn):
            if isinstance(sub, ast.Assign):
                for t in sub.targets:
                    if isinstance(t, ast.Name):
                        assigns.append((sub.lineno, t.id, sub.value))
        inside = _pin_call_names(fn)
        for sub in ast.walk(fn):
            if not isinstance(sub, ast.Call):
                continue
            func = sub.func
            # every call of the sanctioned function names the alias's route: reachability is checked on ITS path
            pin_call = (isinstance(func, ast.Name) and func.id == _SANCTIONED) or any(
                isinstance(a, ast.Name) and a.id == _SANCTIONED for a in sub.args)
            if pin_call and not any(k.arg == "route" for k in sub.keywords):
                self._say(sub, f"{_SANCTIONED} called without the alias's route=")
            # a rebind takes only what the sanctioned function returned
            if isinstance(func, ast.Attribute) and func.attr == "rebind" and where != "RemoteEndpointCLI.rebind":
                arg = sub.args[0] if sub.args else None
                src = None
                if isinstance(arg, ast.Name):
                    prior = [v for ln, n, v in assigns if n == arg.id and ln <= sub.lineno]
                    src = prior[-1] if prior else None
                if src is None or not _calls_pin(src):
                    self._say(sub, f"rebind({ast.unparse(arg) if arg else ''}) not from {_SANCTIONED}")
            # a recorded pin never reaches a target but through the sanctioned function
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else ""
            fed = ([*sub.args, *sub.keywords] if name in ("SshTarget", "rebind")
                   else [k for k in sub.keywords if k.arg in ("host", "ssh_alias")] if name == "replace" else [])
            if any(isinstance(n, ast.Attribute) and n.attr == "login_host" for a in fed for n in ast.walk(a)):
                self._say(sub, f"{name}(...) takes a recorded login_host directly")
            # outside rebind, a target is only ever re-pointed at the user's own alias (or un-pinned)
            if name == "replace" and where != "RemoteEndpointCLI.rebind":
                for k in sub.keywords:
                    if k.arg in ("host", "ssh_alias") and not (
                            (isinstance(k.value, ast.Attribute) and k.value.attr == "alias")
                            or (isinstance(k.value, ast.Name) and k.value.id == "alias")
                            or (isinstance(k.value, ast.Constant) and k.value.value is None)):
                        self._say(sub, f"replace({k.arg}={ast.unparse(k.value)}) re-points a target outside rebind")
        # what `start` reports from the login node reaches nothing but the sanctioned function
        for sub in ast.walk(fn):
            if isinstance(sub, ast.Assign) and isinstance(sub.value, ast.Await) and isinstance(sub.value.value, ast.Call) \
                    and isinstance(sub.value.value.func, ast.Attribute) and sub.value.value.func.attr == "start" \
                    and isinstance(sub.targets[0], ast.Tuple) and len(sub.targets[0].elts) == 3:
                learned = {e.id for e in sub.targets[0].elts[1:] if isinstance(e, ast.Name)}
                cut = {n: min([ln for ln, nm, v in assigns if nm == n and ln > sub.lineno and _calls_pin(v)] or [10**9])
                       for n in learned}
                for n in ast.walk(fn):
                    if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id in learned \
                            and sub.lineno < n.lineno <= cut[n.id] and id(n) not in inside:
                        self._say(n, f"`{n.id}` from the login node's own report used outside {_SANCTIONED}")


def test_vantage_rule_facility_side_hosts_reach_ssh_only_through_routable_pin():
    found: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        v = _Vantage(str(path.relative_to(SRC)))
        v.visit(ast.parse(path.read_text(encoding="utf-8")))
        found += v.found
    assert not found, "\n".join(found)


def test_the_vantage_scan_catches_a_bypass():
    # the scan's own control: each bypass it exists to catch is caught
    bad = '''
class RemoteEndpointCLI:
    async def start(self, name):
        return "HPCB_HOST="
class SlurmFacility:
    async def provision(self, hpc):
        eid, host, addr = await self.cli.start(name)
        self.cli.rebind(addr)
    def other(self, rec):
        self.cli.rebind(rec.login_host)
        pin = _routable_pin(rec.login_host)
        self.cli.rebind(pin)
        self.cli.target = replace(self.cli.target, host=rec.login_host)
        marker = "HPCB_ADDR="
'''
    v = _Vantage("bad.py")
    v.visit(ast.parse(bad))
    said = "\n".join(v.found)
    assert "rebind(addr)" in said and "`addr` from the login node" in said            # start's report, used raw
    assert "rebind(rec.login_host)" in said                                           # a record, used raw
    assert "without the alias's route=" in said                                       # unchecked on the alias's path
    assert "replace(host=rec.login_host)" in said or "takes a recorded login_host" in said
    assert "<module>" not in said and "SlurmFacility.other" in said and "reads HPCB_HOST/HPCB_ADDR" in said
