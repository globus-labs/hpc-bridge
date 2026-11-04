#!/usr/bin/env python3
"""The fake cluster's NETWORK TOPOLOGY: where the agentic jail (the MCP server's vantage point) sits relative to the
cluster. It is orthogonal to the profile (the cluster's shape) and selected by HPCB_FAKE_TOPOLOGY (default `in_network`):

  in_network    the jail joins the cluster's compose network; HPC_BRIDGE_SSH_HOST=login, port 22. Every cell ran this
                way before 2026-11-03: every internal address is reachable from the jail, and SSH is always port 22.
  host_forward  the jail joins NO cluster network. It reaches the login node only through the port the HOST publishes
                (host.docker.internal:<port>), behind an ssh-config alias, as a user on a laptop does (`hpcb-fake` ->
                localhost:2222). This is the NAT / port-forward case.
  bastion       a `bastion` container (sshd only, on an `edge` network and the cluster's) is the way in. The jail joins
                only `edge` and reaches the login node through `ProxyJump bastion`; the pool key works on both hops.
  vip           a TCP proxy (haproxy) on `edge` fronts the login node(s), so the address the jail connects to is no
                node's own. The jail joins only `edge`.

Joining another network is not enough to put the cluster out of reach: Docker Desktop (29.x, 2026-11-03) ROUTES between
the bridge networks of its VM, so a container on `edge` or on Docker's default bridge opened the login node's internal
172.x:22 directly, and the pin "worked" from a vantage a laptop never has. So every topology but in_network ISOLATES the
client (`Plan.isolate`): run_smoke.sh starts a small holder container on the jail's network with an `unreachable`
route for each of the stack's subnets the jail is not on (`internal_subnets`), and the jail joins that holder's network
namespace (`--network container:<holder>`). What the client can reach is then what a client behind NAT, a bastion or a
VIP can: its own network, the host's published ports, the topology's way in, and the internet. `unreachable`, not
`blackhole`: a connect fails with "No route to host", an error ssh and hpc-bridge read as a network failure
(`_UNREACHABLE`), where a blackhole's EINVAL ("Invalid argument") is one no real client sees.

Each topology fixes the jail's docker network, the ssh host the server is given, and two ssh configs written in the
jail before the cell:
- the USER's ~/.ssh/config: the alias as a user would write it (HostName, Port, ProxyJump, User, IdentityFile). The MCP
  server's ssh reads it and nothing else. It is all the product is told.
- the HARNESS' own config (`ssh -F`, run.py `_ssh_run`): the alias, plus a way to each login node by the name the
  harness uses for it (the world channel's teardown and per-node checks). The product never sees it, so a path that
  only the harness has can never rescue the product.

Why: hpc-bridge pins the login node by the address `$SSH_CONNECTION` reached (facility/remote.py `_routable_pin`). Behind
NAT, a bastion or a VIP that is the node's INTERNAL address, which the client cannot reach. The in-network jail reached
every internal address, so no cell could see it (a user on the host could: 2026-11-03).

Pure apart from `internal_subnets` (docker). Importable on the host (targets.py) and in the jail (run.py), and a CLI:

    python3 agentic/harness/topology.py internal <project> <jail-network>   # the cluster's subnets the jail is NOT on
"""
from __future__ import annotations

import ipaddress
import json
import os
import subprocess
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

TOPOLOGIES = ("in_network", "host_forward", "bastion", "vip")
DEFAULT = "in_network"
ALIAS = "hpcb-fake"                 # the user's ssh-config Host in every topology but in_network (as ssh_config.example)
HOST_GATEWAY = "host.docker.internal"
BASTION = "bastion"                 # the bastion's compose service and hostname (on `edge` and the cluster network)
VIP = "vip"                         # the VIP's compose service and hostname
VIP_NODE_PORT0 = 2201               # the VIP's per-node ports, in login_hosts order: the HARNESS' path to each node only
EDGE = "edge"                       # the compose network the bastion / VIP share with the jail
# The compose overlay (relative to agentic/fakecluster) a topology adds to its profile's stack, and its own service.
OVERLAYS = {"bastion": "topologies/bastion.yml", "vip": "topologies/vip.yml"}
SERVICES = {"bastion": BASTION, "vip": VIP}
HARNESS_CONFIG = ".ssh/hpcb-harness.conf"   # under the jail's $HOME
PLAN_ENV = "HPCB_TOPOLOGY_PLAN"             # the plan, as JSON, into the jail (run_smoke.sh)
INTERNAL_ENV = "HPCB_TOPOLOGY_INTERNAL"     # the cluster's subnets the jail is not on (space-separated CIDRs)
# Options every stanza of the HARNESS config carries. ProxyJump's hop runs `ssh -F <this file> -W … bastion` and gets
# none of the command line's -o options, so the hop's own settings must be in the file.
_HARNESS_OPTS = (("BatchMode", "yes"), ("StrictHostKeyChecking", "accept-new"), ("LogLevel", "ERROR"),
                 ("ConnectTimeout", "20"))


def selected(env: dict[str, str] | None = None) -> str:
    """The topology this run uses (HPCB_FAKE_TOPOLOGY, default in_network). An unknown name is refused."""
    name = ((env if env is not None else os.environ).get("HPCB_FAKE_TOPOLOGY") or DEFAULT).strip().lower()
    if name not in TOPOLOGIES:
        raise SystemExit(f"topology: unknown HPCB_FAKE_TOPOLOGY {name!r} ({' | '.join(TOPOLOGIES)})")
    return name


@dataclass(frozen=True)
class Plan:
    """One topology made concrete for one stack. `user_hosts` / `harness_hosts`: ssh-config stanzas
    {Host: {Option: value}}, in order; `network`: the docker network the jail joins (None = Docker's default bridge,
    i.e. no cluster network); `service`: the topology's own container, which the running stack must have; `isolate`:
    the jail runs in a network namespace where the stack's other subnets are unreachable (the module doc)."""
    name: str
    ssh_host: str
    network: str | None
    docker_args: tuple[str, ...] = ()
    user_hosts: dict[str, dict[str, str]] = field(default_factory=dict)
    harness_hosts: dict[str, dict[str, str]] = field(default_factory=dict)
    service: str | None = None
    overlay: str | None = None
    isolate: bool = False

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


def plan(name: str, *, project: str, login_hosts: list[str], ports: list[int | str], harness_ssh_port: int | str = 22,
         network: str | None = None) -> Plan:
    """The plan for topology `name` on the stack `project`. `login_hosts`: the profile's login nodes as the cluster
    names them (compose aliases); `ports`: the host ports published for them, in the same order (HPCB_FAKE_SSH_PORT,
    HPCB_FAKE_SSH_PORT2); `network`: in_network's network (HPCB_FAKE_NETWORK), default `<project>_default`."""
    name = selected({"HPCB_FAKE_TOPOLOGY": name})
    hosts = [str(h) for h in login_hosts] or ["login"]
    if name == "in_network":
        return Plan(name, ssh_host="login", network=network or f"{project}_default")
    if str(harness_ssh_port) != "22":
        # an MFA profile publishes its key-only HARNESS sshd, not port 22: a forwarded alias would skip the second factor
        raise SystemExit(f"topology: {name} needs a profile whose login sshd is published as is (harness_ssh_port 22, "
                         f"not {harness_ssh_port})")
    edge = f"{project}_{EDGE}"
    if name == "host_forward":
        if len(ports) < len(hosts):
            raise SystemExit(f"topology: host_forward needs a published port per login node ({hosts}, ports {ports})")
        fwd = {h: {"HostName": HOST_GATEWAY, "Port": str(p)} for h, p in zip(hosts, ports, strict=False)}
        alias = {ALIAS: dict(fwd[hosts[0]])}
        # --add-host: Docker Desktop resolves host.docker.internal already; Linux Docker needs the mapping
        return Plan(name, ssh_host=ALIAS, network=None, docker_args=(f"--add-host={HOST_GATEWAY}:host-gateway",),
                    user_hosts=alias, harness_hosts={**alias, **fwd}, isolate=True)
    if name == "bastion":
        hop = {BASTION: {"HostName": BASTION}}
        alias = {ALIAS: {"HostName": "login", "ProxyJump": BASTION}}
        nodes = {h: {"HostName": h, "ProxyJump": BASTION} for h in hosts}
        return Plan(name, ssh_host=ALIAS, network=edge, user_hosts={**hop, **alias},
                    harness_hosts={**hop, **alias, **nodes}, service=SERVICES[name], overlay=OVERLAYS[name],
                    isolate=True)
    # vip
    alias = {ALIAS: {"HostName": VIP, "Port": "22"}}
    nodes = {h: {"HostName": VIP, "Port": str(VIP_NODE_PORT0 + i)} for i, h in enumerate(hosts)}
    return Plan(name, ssh_host=ALIAS, network=edge, user_hosts=alias, harness_hosts={**alias, **nodes},
                service=SERVICES[name], overlay=OVERLAYS[name], isolate=True)


def ssh_config(hosts: dict[str, dict[str, str]], *, user: str, key: str, harness: bool = False) -> str:
    """ssh-config stanzas for `hosts`, each with the pool user and key (a ProxyJump hop gets neither from the command
    line). The harness' config also never prompts and accepts a first host key — the trust the harness gives the jail's
    known_hosts once, as a user's first `ssh` did."""
    out: list[str] = []
    for host, opts in hosts.items():
        out.append(f"Host {host}")
        out += [f"    {k} {v}" for k, v in opts.items()]
        out += [f"    User {user}", f"    IdentityFile {key}", "    IdentitiesOnly yes"]
        if harness:
            out += [f"    {k} {v}" for k, v in _HARNESS_OPTS]
    return "\n".join(out) + "\n" if out else ""


def install(spec: dict, *, home: Path, user: str, key: str) -> Path | None:
    """In the jail, before anything connects: append the user's stanzas to ~/.ssh/config and write the harness' own
    config. Returns the harness config's path (run.py's `ssh -F`), or None when the topology needs none (in_network)."""
    if not spec.get("user_hosts") and not spec.get("harness_hosts"):
        return None
    ssh = home / ".ssh"
    ssh.mkdir(parents=True, exist_ok=True, mode=0o700)
    if spec.get("user_hosts"):
        cfg = ssh / "config"
        with cfg.open("a") as fh:
            fh.write(f"# hpc-bridge harness: topology {spec.get('name')} (what the user's own ssh config says)\n")
            fh.write(ssh_config(spec["user_hosts"], user=user, key=key))
        cfg.chmod(0o600)
    harness = home / HARNESS_CONFIG
    harness.write_text(f"# hpc-bridge harness' world channel: topology {spec.get('name')} (never the product's)\n"
                       + ssh_config(spec.get("harness_hosts") or {}, user=user, key=key, harness=True))
    harness.chmod(0o600)
    return harness


def internal_subnets(project: str, jail_network: str | None,
                     run: Callable[[list[str]], str] | None = None) -> list[str]:
    """The subnets of the stack's networks the jail is NOT on: an address there is internal-only from the jail's
    vantage point (host-side; docker)."""
    def _run(argv: list[str]) -> str:
        return subprocess.run(argv, capture_output=True, text=True, timeout=30, check=False).stdout

    run = run or _run
    names = run(["docker", "network", "ls", "--filter", f"label=com.docker.compose.project={project}",
                 "--format", "{{.Name}}"]).split()
    out: list[str] = []
    for n in names:
        if n == jail_network:
            continue
        out += run(["docker", "network", "inspect", "-f", "{{range .IPAM.Config}}{{.Subnet}} {{end}}", n]).split()
    return out


def internal_only(pin: str | None, subnets: list[str]) -> bool:
    """Is `pin` an address on a network the jail is not on? A NAME is judged by what the jail's resolver gives it."""
    if not pin:
        return False
    try:
        addrs = [ipaddress.ip_address(pin)]
    except ValueError:
        import socket
        try:
            addrs = [ipaddress.ip_address(a[4][0]) for a in socket.getaddrinfo(pin, 22, proto=socket.IPPROTO_TCP)]
        except (OSError, ValueError):
            return False   # unresolvable here: unreachable, but not an address on a cluster network (pin_reachable sees it)
    nets = []
    for s in subnets:
        try:
            nets.append(ipaddress.ip_network(s, strict=False))
        except ValueError:
            continue
    return any(a in n for a in addrs for n in nets if a.version == n.version)


def preauth_check_argv(command: str, control_path: str) -> list[str]:
    """A `preauth_command` (`ssh -fN -o ControlMaster=yes -o ControlPath=… … -- user@host`, the line a user pastes into
    their own terminal) as the harness runs it to see that it WORKS from here: the same destination, port, jump, host-key
    alias and identity, plus BatchMode (a prompt fails instead of waiting) and a ConnectTimeout (both first, so they win),
    with the master's socket at `control_path` instead of the product's (so the check never touches its masters)."""
    import shlex

    argv = shlex.split(command)
    if not argv or Path(argv[0]).name != "ssh":
        raise ValueError(f"not an ssh command: {command[:80]!r}")
    out = [argv[0], "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
    i = 1
    while i < len(argv):
        if argv[i] == "-o" and i + 1 < len(argv) and argv[i + 1].lower().startswith("controlpath="):
            out += ["-o", f"ControlPath={control_path}"]
            i += 2
            continue
        out.append(argv[i])
        i += 1
    return out


def run_preauth(command: str, *, timeout: float = 45.0) -> tuple[int, str]:
    """Run `preauth_check_argv(command)` and close the master it opened. (rc, output). Output goes through a FILE:
    `ssh -f` backgrounds a master that keeps any pipe open, and a captured run would wait for it."""
    import tempfile

    with tempfile.TemporaryDirectory(prefix="hpcb-pa-") as d:
        cp = f"{d}/m"
        try:
            argv = preauth_check_argv(command, cp)
        except ValueError as exc:
            return 2, str(exc)
        log = Path(d) / "out"
        try:
            with log.open("w") as fh:
                rc = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=fh, stderr=subprocess.STDOUT,
                                    timeout=timeout, check=False).returncode
        except subprocess.TimeoutExpired:
            rc = 124
        finally:
            subprocess.run(["ssh", "-O", "exit", "-o", f"ControlPath={cp}", "--", argv[-1]], stdin=subprocess.DEVNULL,
                           capture_output=True, timeout=15, check=False)
        return rc, log.read_text(errors="replace") if log.exists() else ""


def main(argv: list[str]) -> int:
    if argv[:1] == ["internal"] and len(argv) >= 2:
        print(" ".join(internal_subnets(argv[1], argv[2] if len(argv) > 2 and argv[2] else None)))
        return 0
    print(__doc__.split("\n\n")[-1].strip(), file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
