#!/usr/bin/env python3
"""The cluster a run targets — `globus1` (the lab cluster) or `fake` (agentic/fakecluster, a compose Slurm cluster).

One preset carries every target-specific fact together (ssh host as the JAIL reaches it, node count, the endpoint
name prefix, the pool key, the docker network the jail must join, how the HOST reaches the cluster for probes and
cleanup), so the two targets can never be half-mixed. Selected by HPCB_TARGET (default globus1); run_suite's
`--target` sets it for every cell. Importable on the host (no SDK) and in the jail; also a CLI that prints shell
assignments for run_smoke.sh:

    eval "$(python3 agentic/harness/targets.py fake)"   # HPCB_T_SSH_HOST=login HPCB_T_NETWORK=hpcb-fake_default …

Prompts name the login host as `{ssh_host}`; `fill_prompt` substitutes it (and `{facility}`) literally — never
str.format, a prompt may embed braces of its own.
"""
from __future__ import annotations

import json
import os
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_TARGET = "globus1"
REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILES_DIR = REPO_ROOT / "agentic" / "fakecluster" / "profiles"

# What the lab cluster offers, in the same vocabulary as a fake-cluster profile's [capabilities]. Four compute nodes
# since the 2026-10 Spark rebuild (spark1-4 on main/backfill; spark5-6 sit in a `service` partition no cell may use).
# `catalog`: where `list_facilities` / a catalogued connect read the facilities from — the public registry here.
# `pilot_end_wait_s`: how long the universal pilot check (run._no_pilot_left) waits for a cancelled or exiting pilot to
# leave the queue — a real cluster's epilogue can hold a job in CG / E for minutes; the fake cluster's default is
# FAKE_PILOT_END_WAIT_S (a profile may set its own).
GLOBUS1_CAPABILITIES = {
    "scheduler": "slurm", "nodes": 4, "login_nodes": 1, "login_hosts": ["globus1.cs.uchicago.edu"],
    "partitions": ["main", "backfill"], "default_partition": "main", "accounting": "none", "accounts": ["lab"],
    "balance_tool": "none", "nics": 1, "qos": [], "gres": "none", "module_system": "none", "mfa": "none",
    "catalog": "registry", "pilot_end_wait_s": 300,
}
# The fake cluster: KillWait=30 (Slurm, no epilog) and a qdel'd PBS job listed R/E for ~19 s (live 2026-10-16).
FAKE_PILOT_END_WAIT_S = 60


@dataclass(frozen=True)
class Target:
    name: str
    ssh_host: str            # the login host as the JAIL (and the product inside it) reaches it
    nodes: int               # compute nodes in the `main` partition (saturation's sleeper count / node need)
    endpoint_prefix: str     # this target's endpoints in the shared Globus identity: <prefix>-<runid>
    default_key: str         # host path of the pool users' private key
    docker_network: str | None   # the jail joins this network (fake: reach `login:22` with no port mapping)
    probe_argv: tuple[str, ...]  # host-side: one shell command on the cluster (node probes) — argv prefix
    cleanup_host: str        # host-side ssh destination host for per-pool-user cleanup (sweep, abandoned cells)
    cleanup_ssh_opts: tuple[str, ...]
    profile: str | None = None                        # fake: the active cluster profile (profiles/<name>/)
    capabilities: dict = field(default_factory=dict)  # what this cluster offers (a scenario's REQUIRES is matched against it)
    # Host-side ADMIN channel: argv prefix that runs one shell command AS THE CLUSTER ADMIN (root on the controller).
    # A scenario's ADMIN_SETUP/ADMIN_CLEANUP run through it (run_smoke.sh, `{user}` = the cell's pool user) — the
    # cluster-side world changes a pool user cannot make (an association submit limit, a drained node). Only the
    # fake cluster has one (docker exec into the scheduler's controller: slurmctld, or pbsserver on a PBS profile); on a
    # real facility we are not the admin → None, and run_suite skips the cell.
    admin_argv: tuple[str, ...] | None = None
    # Host-side command printing this cluster's LOCAL CATALOG (seed-format YAML) — a profile with facility MEPs minted
    # per cluster declares it ([catalog] cmd); run_smoke.sh mounts the output into the jail as HPC_BRIDGE_CATALOG_FILE.
    catalog_cmd: str | None = None
    # MFA profiles: the fixture TOTP secret every pool user is enrolled with ([totp] secret) — handed to the human-sim.
    totp_secret: str | None = None

    def cleanup_argv(self, user: str, key: str) -> list[str]:
        """Host-side ssh as the pool `user` with `key` (the run-scoped cleanup channel)."""
        return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", "-i", key, "-o", "IdentitiesOnly=yes",
                *self.cleanup_ssh_opts, f"{user}@{self.cleanup_host}"]


def load_profile(name: str) -> dict:
    """A fake-cluster profile's MERGED manifest (profiles/<name>/profile.toml layered on its `base` chain — see
    fakecluster/bin/profile.py) — its [capabilities] are the vocabulary a scenario's REQUIRES is matched against."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("hpcb_profile", PROFILES_DIR.parent / "bin" / "profile.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    try:
        man = mod.manifest(name)
    except SystemExit as exc:
        raise SystemExit(f"targets: {exc}") from None
    # DERIVED capability `catalog`: a profile with a [catalog] cmd mounts its own catalog into the jail
    # (HPC_BRIDGE_CATALOG_FILE), which REPLACES the public registry for that process — a scenario that needs the real
    # registry's entries (globus-labs, anvil, …) declares REQUIRES = {"catalog": "registry"} and is skipped there.
    caps = man.setdefault("capabilities", {})
    caps.setdefault("catalog", "local" if (man.get("catalog") or {}).get("cmd") else "registry")
    return man


def meets(requires: dict | None, caps: dict) -> tuple[bool, str]:
    """Does a cluster with `caps` satisfy a scenario's REQUIRES? Keys: scheduler (==), min_nodes (<= nodes),
    login_nodes (<= login_nodes), accounting (==), balance_tool (==), gres (==), module_system (==), mfa (==),
    catalog (==), min_partitions (<= len(partitions)), min_nics (<= nics). A LIST-valued capability (partitions,
    accounts, qos, modules, …) is a membership test: `{"partitions": ["debug"], "accounts": "hpcb"}` needs each named
    one to be on offer. Returns (ok, the first unmet requirement)."""
    for k, want in (requires or {}).items():
        if k == "min_nodes":
            ok = int(caps.get("nodes", 0)) >= int(want)
        elif k == "login_nodes":
            ok = int(caps.get("login_nodes", 1)) >= int(want)
        elif k == "min_partitions":
            ok = len(caps.get("partitions", [])) >= int(want)
        elif k == "min_nics":
            ok = int(caps.get("nics", 1)) >= int(want)
        elif isinstance(caps.get(k), list):
            ok = all(str(w) in [str(x) for x in caps[k]] for w in ([want] if isinstance(want, str) else want))
        else:
            ok = str(caps.get(k)) == str(want)
        if not ok:
            return False, f"{k}={want!r} (cluster has {caps.get(k.removeprefix('min_'), caps.get(k))!r})"
    return True, ""


def _admin_container(caps: dict) -> str:
    """The fake cluster's ADMIN container — root on the scheduler's controller: `slurmctld` on the Slurm stack, the PBS
    server (`pbsserver`: qmgr, and qdel of any user's job) on the PBS stack. Each has its own override, read only on its
    own stack: HPCB_FAKE_CTLD (Slurm) and HPCB_FAKE_PBS_SERVER (PBS) — a Slurm controller's name would never run a qdel."""
    if str(caps.get("scheduler", "slurm")) == "pbs":
        return os.environ.get("HPCB_FAKE_PBS_SERVER") or "hpcb-fake-pbsserver-1"
    return os.environ.get("HPCB_FAKE_CTLD") or "hpcb-fake-slurmctld-1"


def get(name: str | None = None) -> Target:
    name = (name or os.environ.get("HPCB_TARGET") or DEFAULT_TARGET).strip().lower()
    home = str(Path.home())
    if name == "globus1":
        return Target(
            name="globus1", ssh_host="globus1.cs.uchicago.edu", nodes=4, endpoint_prefix="hpc-bridge-globus1",
            default_key=f"{home}/.ssh/hpcbridge-test", docker_network=None,
            # the operator's own ssh alias (admin identity) — sinfo/squeue for the node gate
            probe_argv=("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", os.environ.get("HPCB_NODE_PROBE_SSH", "globus1")),
            cleanup_host="globus1.cs.uchicago.edu", cleanup_ssh_opts=(), capabilities=dict(GLOBUS1_CAPABILITIES),
        )
    if name == "fake":
        profile = os.environ.get("HPCB_FAKE_PROFILE", "default")
        man = load_profile(profile)
        caps = dict(man.get("capabilities", {}))
        caps.setdefault("pilot_end_wait_s", FAKE_PILOT_END_WAIT_S)
        port = os.environ.get("HPCB_FAKE_SSH_PORT", "2222")
        key = os.environ.get("HPCB_FAKE_KEY", f"{home}/.ssh/hpcb-fake")
        nohostkey = ("-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR")
        return Target(
            name="fake", ssh_host="login", nodes=int(caps.get("nodes", 2)), endpoint_prefix="hpc-bridge-fake",
            default_key=key, docker_network=os.environ.get("HPCB_FAKE_NETWORK", "hpcb-fake_default"),
            # the (first) login container's sshd on the host port, as a pool user
            probe_argv=("ssh", "-p", port, "-i", key, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
                        "-o", "ConnectTimeout=10", *nohostkey, "hpcbridge-test-00@localhost"),
            cleanup_host="localhost", cleanup_ssh_opts=("-p", port, *nohostkey),
            profile=profile, capabilities=caps,
            admin_argv=("docker", "exec", _admin_container(caps), "bash", "-lc"),
            catalog_cmd=(man.get("catalog") or {}).get("cmd") or None,
            totp_secret=(man.get("totp") or {}).get("secret") or None,
        )
    raise SystemExit(f"targets: unknown target {name!r} (globus1 | fake)")


def fill_prompt(text: str, *, facility: str, ssh_host: str) -> str:
    """Literal substitution of the two per-run tokens a scenario may use in PROMPT / PHASES / USER_GOAL."""
    return text.replace("{facility}", facility).replace("{ssh_host}", ssh_host)


def main(argv: list[str]) -> int:
    t = get(argv[0] if argv else None)
    for k, v in (("NAME", t.name), ("SSH_HOST", t.ssh_host), ("NODES", t.nodes), ("EP_PREFIX", t.endpoint_prefix),
                 ("KEY_DEFAULT", t.default_key), ("NETWORK", t.docker_network or ""), ("PROFILE", t.profile or "")):
        print(f"HPCB_T_{k}={v}")
    print(f"HPCB_T_CAPS_JSON={json.dumps(t.capabilities, separators=(',', ':'))!r}")
    print(f"HPCB_T_ADMIN_ARGV={shlex.quote(shlex.join(t.admin_argv)) if t.admin_argv else ''}")  # '' = no admin channel
    print(f"HPCB_T_CATALOG_CMD={shlex.quote(t.catalog_cmd) if t.catalog_cmd else ''}")  # empty = the registry is the catalog
    print(f"HPCB_T_HARNESS_SSH_PORT={t.capabilities.get('harness_ssh_port', 22)}")  # the world channel's sshd port
    print(f"HPCB_T_TOTP_SECRET={shlex.quote(t.totp_secret) if t.totp_secret else ''}")  # empty = no MFA fixture
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
