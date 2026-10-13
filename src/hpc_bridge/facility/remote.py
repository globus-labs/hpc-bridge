"""Provision a Globus Compute endpoint on a remote Slurm login node over SSH.

The facility-agnostic runtime (dispatch / session shell / cost) is unchanged;
everything machine-specific lives in a `MachineProfile` (data), so new HPC
systems are added as profiles rather than code. Anvil (Purdue/ACCESS) is the
first profile — its key-based SSH (no per-login MFA) makes the bootstrap the
easy case; OTP facilities will later route SSH through the credential broker.
"""
from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import re
import shlex
import sys
import tempfile
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import yaml

# PRIVATE SDK path (globus-compute-sdk 4.x): a rename here breaks at IMPORT — i.e. the 'no hpc-bridge tools'
# boot failure. Re-check on every SDK bump (dependency audit 2026-09-03).
from globus_compute_sdk.sdk.auth.token_storage import (
    _get_storage_filepath,
    _resolve_namespace,
)

from ..credentials import build_minimal_storage_db
from ..profile import Profile
from ..state import EndpointRecord, LoginNodeStore

if TYPE_CHECKING:
    from ..catalog.entry import CatalogEntry
from .base import EndpointHandle

# A Globus endpoint UUID — scopes the per-UEP pidfile-cleanup glob to one endpoint, and refuses to
# build a remote glob from anything that isn't a plain UUID.
_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")

# ---------------------------------------------------------------- SSH transport


_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")  # endpoint names spliced into remote shell paths


class RemoteTokenStoreExists(RuntimeError):
    """The login node already holds a Globus token store WITH tokens in it, yet `globus-compute-endpoint whoami` failed
    there. hpc-bridge will not replace (and later delete) a credential it did not create (review 2026-09-05 #5)."""


# Exit 0 when the SQLite token store at argv[1] holds NO tokens — the empty store `globus-compute-endpoint whoami`
# itself creates when nobody has logged in; 1 when it holds tokens or cannot be read (a missing table, a lock, not
# SQLite); 3 when this interpreter has no sqlite3, so the caller tries the next one. Opened read-only, the path
# URI-escaped. No `!`, `$` or double quotes: it rides inside a double-quoted `sh -c` argument through any login shell
# (tcsh history-expands `!` even in single quotes).
_EMPTY_STORE_PY = ("import sys,importlib.util,urllib.parse; "
                   "importlib.util.find_spec('_sqlite3') is None and sys.exit(3); import sqlite3; "
                   "c=sqlite3.connect('file:'+urllib.parse.quote(sys.argv[1])+'?mode=ro',uri=True); "
                   "sys.exit(0 if c.execute('select count(*) from token_storage').fetchone()[0]==0 else 1)")
_PY_SENTINEL = "HPCB_PY="


@dataclass(frozen=True)
class SshTarget:
    host: str
    # user/key_path optional: when None, defer to the user's ~/.ssh/config (User/IdentityFile), which
    # OpenSSH reads live — so access needs no boot-env var the already-running server can't see.
    user: str | None = None
    key_path: str | None = None
    connect_timeout: int = 20
    # When set, multiplex every connection over one ControlMaster socket under this (0700) dir, so
    # the whole bootstrap+discovery authenticates ONCE instead of per-call — the MFA-once win, and
    # what makes a multi-command discovery sweep cheap. None ⇒ no multiplexing (argv unchanged).
    control_dir: str | None = None
    control_persist: int = 60
    # After a login-node PIN (rebind to the FQDN the manager landed on) the host key is verified against
    # the name the USER trusted — the alias they connected to — via OpenSSH's HostKeyAlias. A pin can then
    # never redirect the bearer credential to a machine whose key differs from the alias's (review A10).
    host_key_alias: str | None = None

    def __post_init__(self) -> None:
        # Defence in depth behind the model validators: never let a "-…" or whitespace host reach argv.
        h = self.host or ""
        if h.startswith("-") or any(ch.isspace() for ch in h) or not h:
            raise ValueError(f"invalid ssh host {self.host!r}")

    def _control_path(self) -> str:
        # %C hashes localhost/user/host/port: short (fits macOS' ~104-char socket-path limit) and
        # auto-distinct after `rebind` swaps host, so the alias master and the pinned-node master
        # land on separate sockets with no bookkeeping.
        return f"{self.control_dir}/%C"

    def argv(self, remote_cmd: str) -> list[str]:
        # Never-prompt (BatchMode fails fast instead of hanging on a password/MFA prompt). With an
        # explicit key we pin it (IdentitiesOnly); with none we DEFER to ~/.ssh/config's IdentityFile.
        # A pre-opened master (e.g. one Duo on an MFA facility) is reused regardless of BatchMode;
        # ControlMaster=auto opens one non-interactively on a key host.
        opts = ["ssh"]
        if self.key_path:
            opts += ["-i", self.key_path, "-o", "IdentitiesOnly=yes"]
        # Host keys: hpc-bridge trusts exactly what YOUR ssh trusts. No StrictHostKeyChecking override —
        # OpenSSH's default (ask) refuses an unknown host under BatchMode, and any relaxation you made in
        # ~/.ssh/config applies. `accept-new` used to accept an attacker-chosen registry/details= host and
        # ship the seeded Globus tokens to it (security review 2026-09-04, A1/B-06/C-1).
        opts += [
            "-o", "BatchMode=yes",
            "-o", f"ConnectTimeout={self.connect_timeout}",
        ]
        if self.host_key_alias:
            opts += ["-o", f"HostKeyAlias={self.host_key_alias}"]
        if self.control_dir:
            opts += [
                "-o", "ControlMaster=auto",
                "-o", f"ControlPath={self._control_path()}",
                "-o", f"ControlPersist={self.control_persist}",
            ]
        opts += ["--", self._destination(), remote_cmd]  # `--`: the destination can never be read as an option
        return opts

    def _destination(self) -> str:
        return f"{self.user}@{self.host}" if self.user else self.host

    def control_argv(self, request: str) -> list[str]:
        """argv for an `ssh -O <request>` control command (e.g. 'exit', 'check') against this
        target's master socket. Only meaningful when control_dir is set."""
        return [
            "ssh", "-O", request,
            "-o", f"ControlPath={self._control_path()}",
            "--", self._destination(),
        ]

    def preauth_argv(self) -> list[str]:
        """argv that opens a ControlMaster INTERACTIVELY (no BatchMode, so ssh prompts) on the same `%C`
        ControlPath `argv()` uses, so `ControlMaster=auto` joins it. Requires `control_dir`."""
        opts = ["ssh", "-fN", "-o", "ControlMaster=yes",
                "-o", f"ControlPath={self._control_path()}", "-o", "ControlPersist=1h"]
        if self.host_key_alias:  # a pinned login node: verify against the alias the user trusted (live 2026-09-04:
            opts += ["-o", f"HostKeyAlias={self.host_key_alias}"]  # without it the pin's name is an UNKNOWN host)
        if self.key_path:
            opts += ["-i", self.key_path]
        opts += ["--", self._destination()]
        return opts

    def preauth_command(self) -> str:
        """The command the USER runs in THEIR OWN terminal to open a ControlMaster interactively —
        entering any password + approving MFA/Duo **once**. hpc-bridge's BatchMode calls then
        multiplex over it, so the secret never reaches hpc-bridge or the agent. Same argv as
        `preauth_argv`, quoted for a shell (A4)."""
        return " ".join(shlex.quote(o) for o in self.preauth_argv())  # pasted into the user's shell: every token quoted


# Teardown SSH ops (gce stop, squeue, scancel) hit a possibly-loaded login node with *fresh*
# connections; bound them tighter than the 120s default so stop_endpoint releases the allocation
# promptly instead of dragging on a slow sshd.
_TEARDOWN_SSH_S = 30.0
# Bootstrap steps run `env_setup` first, and on a FIRST connect that is the toolchain install (uv + the
# endpoint stack into the user's home — several minutes on a slow home filesystem: Expanse took >120 s and
# the default ssh timeout killed the ssh while the install kept going on the login node, live 2026-09-04).
_BOOTSTRAP_SSH_S = 900.0


def _resolved_hostname(host: str) -> str:
    """The HostName OpenSSH would connect to for `host` (an alias resolves through ~/.ssh/config) — the
    name known_hosts keys the host key by. Falls back to `host` itself when `ssh -G` is unavailable."""
    import subprocess

    try:
        out = subprocess.run(["ssh", "-G", "--", host], capture_output=True, text=True, timeout=10, check=False).stdout
    except Exception:  # noqa: BLE001 - no ssh binary / odd host -> the name as given
        return host
    for line in out.splitlines():
        k, _, v = line.partition(" ")
        if k.lower() == "hostname" and v.strip():
            return v.strip()
    return host


async def ssh_exec(
    target: SshTarget, remote_cmd: str, *, stdin: str | None = None, timeout: float = 120.0
) -> tuple[int, str, str]:
    """Run one command on the remote host over SSH; returns (rc, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *target.argv(remote_cmd),
        stdin=asyncio.subprocess.PIPE if stdin is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    payload = stdin.encode() if stdin is not None else None
    try:
        out, err = await asyncio.wait_for(proc.communicate(payload), timeout)
    except BaseException:
        # ssh child still running; kill + reap it so we don't leak the process and its 3 pipe
        # FDs (bites when a *connected* session wedges mid-command past `timeout`). Then re-raise.
        if proc.returncode is None:
            proc.kill()
            try:
                await proc.wait()
            except Exception:  # noqa: BLE001 - best-effort reap
                pass
        raise
    return proc.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")


_INTERACTIVE_AUTH_METHODS = ("password", "keyboard-interactive")


def is_interactive_auth_failure(rc: int, stderr: str) -> bool:
    """True when a BatchMode SSH was DENIED but the server offers an interactive method
    (password / keyboard-interactive) — i.e. a one-time interactive pre-auth in the user's own
    terminal would succeed. Distinguishes "needs a password/MFA" from "host down" or "bad key with
    no interactive fallback" (`Permission denied (publickey)` alone → False)."""
    s = (stderr or "").lower()
    denied = "permission denied" in s or "authentication failed" in s
    offers_interactive = any(m in s for m in _INTERACTIVE_AUTH_METHODS)
    return rc != 0 and denied and offers_interactive


def _denied_methods(stderr: str) -> set[str]:
    """The method list in sshd's `Permission denied (a,b,c)` — the methods STILL required. OpenSSH drops a method
    from this list once it has succeeded, so a key that was accepted no longer appears."""
    import re as _re

    m = _re.search(r"permission denied \(([^)]*)\)", (stderr or "").lower())
    return {x.strip() for x in m.group(1).split(",")} if m else set()


def key_accepted_second_factor_pending(stderr: str) -> bool:
    """The first factor PASSED and only an interactive second factor remains: the denial still lists
    `keyboard-interactive` but no longer lists `publickey` (Expanse: `(gssapi-with-mic,keyboard-interactive,
    hostbased)`). A stranger whose key was refused sees `publickey` still listed (Anvil: `(publickey,…,
    keyboard-interactive,…)`) — that is NO SSH ACCESS, not a handoff, even though the site offers 2FA."""
    methods = _denied_methods(stderr)
    return bool(methods) and "keyboard-interactive" in methods and "publickey" not in methods


def offers_one_time_code(stderr: str) -> bool:
    """A one-time code (TOTP / Duo passcode) would complete this login — the key was accepted and the remaining
    method is keyboard-interactive, the channel such prompts arrive on. Passwords never enter the session."""
    return key_accepted_second_factor_pending(stderr)


class NeedsPreauth(Exception):
    """An SSH op needs a one-time interactive pre-auth (password/MFA). Carries the target so a caller can
    surface the user's pre-open command (`preauth_command`), and `otp_ok`: whether a one-time CODE would
    do (keyboard-interactive offered) — the in-session `complete_preauth(code)` path (2026-09-04)."""

    def __init__(self, target: SshTarget, otp_ok: bool = False) -> None:
        self.target = target
        self.otp_ok = otp_ok
        super().__init__(f"{target.host} needs interactive pre-authentication (password/MFA)")


_INTERNAL_HOST_SUFFIXES = (".local", ".internal", ".localdomain")
# Management-/out-of-band-plane LABELS that mark a `hostname -f` as an internal name not reachable via
# the alias's external path — matched as whole dot-labels (not substrings), so a legit host like
# `cms.example.edu` or `mgmtnode` isn't caught. ALCF Aurora's `hostname -f` is
# `aurora-uan-0009.hostmgmt.cm.aurora.alcf.anl.gov` (seen live) — the routable per-UAN name drops the
# `hostmgmt.cm` infix, so pinning the raw FQDN breaks teardown/reconnect. Dropping is always SAFE (we
# fall back to the alias, which the user pins to a specific node in ~/.ssh/config), so we err toward it.
_INTERNAL_HOST_LABELS = frozenset({"hostmgmt", "cm", "mgmt", "ipmi", "bmc"})


def _resolves_from_here(host: str) -> bool:
    """Does the client's own resolver know `host`? (An internal login-node name that passes the suffix heuristics —
    a `.int.` domain, a bare site FQDN not published outside — is still useless as a pin.)"""
    import socket

    try:
        socket.getaddrinfo(host, 22, proto=socket.IPPROTO_TCP)
        return True
    except OSError:
        return False


def _routable_pin(host: str | None, addr: str | None = None, *, resolves=None) -> str | None:
    """The reconnect pin for the login node the manager landed on — the node's FQDN when it is reachable from the
    client via the SAME external path as the alias, else the ADDRESS the bootstrap connection actually reached
    (`$SSH_CONNECTION`'s server field), else None (keep the alias).

    Some facilities' `hostname -f` reports an INTERNAL name — Midway's `beagle3-tbd1.rcc.local`, Aurora's
    `aurora-uan-0009.hostmgmt.cm.aurora.alcf.anl.gov` (both seen live), or a plain-looking name their DNS never
    publishes — and pinning it breaks login_shell and teardown. Internal-suffix (`.local`/`.internal`/
    `.localdomain`), single-label and management-plane (`hostmgmt`/`cm`/`mgmt`/…) names are dropped outright; any
    other name must RESOLVE from here (`resolves`, injectable for tests). The address is used only when it is a
    real unicast one — a NAT/VIP front makes it as round-robin as the alias, but no worse. The host KEY is still
    checked against the alias the user trusted (HostKeyAlias), so neither pin can redirect the credential."""
    check = resolves or _resolves_from_here
    if host:
        h = host.strip().rstrip(".")
        labels = {label.lower() for label in h.split(".")}
        if "." in h and not h.lower().endswith(_INTERNAL_HOST_SUFFIXES) and not (labels & _INTERNAL_HOST_LABELS) \
                and check(h):
            return h
    if addr:
        try:
            ip = ipaddress.ip_address(addr.strip())
        except ValueError:
            return None
        if not (ip.is_loopback or ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved):
            return str(ip)
    return None


# ------------------------------------------------------------- machine profiles


@dataclass(frozen=True)
class MachineProfile:
    """Per-facility data: how to reach the endpoint binary and what Slurm to request."""

    name: str
    endpoint_name: str      # registration / on-disk dir name (e.g. "hpc-bridge")
    env_setup: str          # bash that puts globus-compute-endpoint on PATH (module + venv)
    interface: str          # address_by_interface ifname for the worker (e.g. ib0)
    partition: str
    account: str
    worker_init: str        # replays env_setup on the compute worker (parsl writes it into sbatch)
    # Human-readable label shown in the Globus web UI / `gce list` (distinct from the
    # endpoint_name used for registration). Defaults to endpoint_name when unset.
    display_name: str | None = None
    walltime: str = "00:30:00"
    max_workers_per_node: int = 2     # parsl workers per node (the engine's slots)
    nodes_per_block: int = 1          # nodes requested per scheduler block
    max_blocks: int = 1               # ceiling on concurrent scheduler blocks Parsl may hold
    available_accelerators: int | list[str] | None = None  # GPU count or device IDs
    amqp_port: int = 443    # facilities firewall the default AMQPS 5671; 443 is allowed
    scheduler_options: str | None = None
    scratch_root: str | None = None  # session-shell root on the shared filesystem
    scheduler: Literal["slurm", "pbs"] = "slurm"  # "slurm" | "pbs" — selects the config_template branch
    cpus_per_node: int | None = None  # PBS: emitted as PBSProProvider.cpus_per_node; Slurm: unused
    exclusive: bool | None = None  # Slurm: SlurmProvider.exclusive (None = Parsl's default, True — a whole node)


def profile_from_catalog_entry(
    entry: CatalogEntry,
    *,
    user: str,
    account: str,
    partition: str | None = None,
    venv: str | None = None,
) -> MachineProfile:
    """Build a `MachineProfile` from a catalog entry plus per-user runtime values.

    The catalog stores user-agnostic templates; this resolves them at provision time:
    ``{user}`` is the SSH login user and ``{venv}`` is the remote globus-compute-endpoint venv
    (defaults to the ``/home/{user}/hpc-bridge/gce-venv`` convention). ``account`` and the derived
    ``worker_init`` (= the resolved ``env_setup``, replayed on the compute worker) are supplied
    here, never stored in the catalog. ``partition`` overrides the entry's default when given.
    Substitution uses ``str.replace`` (not ``str.format``) so literal shell braces in ``env_setup``
    are left untouched.
    """
    venv = venv or f"/home/{user}/hpc-bridge/gce-venv"

    def _resolve(template: str) -> str:
        # Accept the plugin template ({user}/{venv}) AND the natural shell idiom ($USER/${USER}) —
        # a BYO facility's scratch_root is embedded in the session shim *single-quoted*, so the
        # remote shell never expands $USER; we must resolve it here or `mkdir` hits a literal $USER.
        for tok in ("{user}", "${USER}", "$USER"):
            template = template.replace(tok, user)
        return template.replace("{venv}", venv)

    kw = entry.profile_kwargs()
    # Naming convention: never the bare "hpc-bridge" — endpoints are keyed by identity+name, so a
    # shared name collides (stale-reuse → stuck "provisioning"). A seed that omits endpoint_name
    # derives hpc-bridge-<id> here; session entries already set it explicitly.
    kw["endpoint_name"] = kw["endpoint_name"] or f"hpc-bridge-{entry.id}"
    kw["env_setup"] = _resolve(kw["env_setup"])
    kw["scratch_root"] = _resolve(kw["scratch_root"])
    if partition:
        kw["partition"] = partition
    return MachineProfile(**kw, account=account, worker_init=kw["env_setup"])


# ------------------------------------------------- globus-compute-endpoint / SSH


_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

# `globus-compute-endpoint list` renders an 80-column table when there is no TTY (how we run it over
# SSH) and WRAPS an Endpoint Name longer than ~27 chars onto a second row — so `hpc-bridge-<fqdn>`
# names were never matched (issue #39: the "registration lag" that wasn't; measured on globus1, gce 4.13).
# A wide COLUMNS is honoured without a TTY and keeps the name on one row.
_GCE_COLUMNS = 400
# After `start --detach` returns, the daemon still has to register: the UUID (endpoint.json) landed
# ~3.6 s later on globus1. Poll the listing for it, bounded.
_START_REGISTER_WAIT_S = 30.0
_START_REGISTER_POLL_S = 1.5
_ANOTHER_INSTANCE = "another instance"
_sleep = asyncio.sleep  # indirection so tests can run the poll loop without waiting
_JOBID = re.compile(r"^\d+(_\d+)?$")  # plain or array slurm job id (guards what we scancel)
_JOBID_PBS = re.compile(r"^\d+(\.\S+)?$")  # PBS: bare or host-qualified (1234567.polaris-pbs-01)


class RemoteEndpointCLI:
    """Drive `globus-compute-endpoint` on a remote login node over SSH.

    Mirrors the local `EndpointCLI` (configure / start / stop) but runs every
    command inside the remote venv, writes config files over SSH, and reads the
    UUID from `list` (v4 MEP mode does not reliably write endpoint.json)."""

    def __init__(self, target: SshTarget, env_setup: str, *, remote_dir: str = "$HOME/.globus_compute") -> None:
        self.target = target
        self.last_whoami_error: str | None = None  # the remote's words when `whoami` last failed
        self.gce_python: str | None = None  # python3 inside the gce environment, seen by whoami (it has sqlite3)
        self.env_setup = env_setup
        self.remote_dir = remote_dir

    async def _gce(self, *args: str, timeout: float = 120.0) -> tuple[int, str, str]:
        inner = (f"export COLUMNS={_GCE_COLUMNS}; {self._env_prefix()}globus-compute-endpoint "
                 + " ".join(shlex.quote(a) for a in args))
        return await ssh_exec(self.target, f"bash -lc {shlex.quote(inner)}", timeout=timeout)

    def _env_prefix(self) -> str:
        """`<env_setup> && ` — or nothing when the entry has no env_setup (a bare `&& gce …` is a shell
        syntax error; an Expanse probe with neither uv nor gce proposed "" — live 2026-09-04)."""
        es = (self.env_setup or "").strip()
        return f"{es} && " if es else ""

    async def login_exec(self, command: str) -> tuple[int, str, str]:
        """Run a read-only command on the login node over SSH for facility discovery
        (sinfo/sacctmgr/module). Unlike `_gce` it does NOT source the endpoint venv — base
        login-node tools are already on PATH — and it provisions nothing."""
        return await ssh_exec(self.target, f"bash -lc {shlex.quote(command)}")

    async def configure(self, name: str, multi_user: bool = False) -> None:
        # Force --multi-user false (personal endpoint): the default auto-selects from
        # POSIX caps and can silently create an identity-mapping MEP — see endpoint.py.
        rc, out, err = await self._gce("configure", "--multi-user", "true" if multi_user else "false", name,
                                       timeout=_BOOTSTRAP_SSH_S)
        if rc != 0:
            msg = (err or out).strip()
            if "already" in msg.lower() or "configexists" in msg.lower():  # dir exists -> fine
                return
            raise RuntimeError(f"remote configure failed: {msg}")

    @staticmethod
    def _list_rows(out: str) -> list[list[str]]:
        """Parse `gce list`'s table into [uuid, status, name] cells per data row.

        Matching the Endpoint Name column EXACTLY (not as a substring of the line) is
        load-bearing: a `hpc-bridge-login` row contains the substring `hpc-bridge`, so a
        naive `name in line` would mis-identify a *different* endpoint as ours and skip
        configure — caught live on Anvil."""
        rows = []
        for line in out.splitlines():
            if "|" not in line:
                continue
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if len(cells) >= 3:
                rows.append(cells)
        return rows

    @classmethod
    def _parsed_rows(cls, out: str) -> list[list[str]]:
        """`_list_rows`, but fail LOUD when `list` clearly emitted an endpoint table we could
        NOT parse (a gce version/format/locale change away from the pipe table) — otherwise an
        unparsed listing reads as "no endpoints" and the caller silently mis-provisions or
        can't find a live endpoint. The legitimate empty case ("No endpoints configured")
        still returns []. See issue #8 (the robust fix is the SDK's get_endpoints())."""
        rows = cls._list_rows(out)
        low = out.lower()
        if not rows and "endpoint" in low and "no endpoint" not in low:
            raise RuntimeError(
                "could not parse `globus-compute-endpoint list` output "
                f"(gce version/format change?); raw output:\n{out.strip()[:500]}"
            )
        return rows

    async def status(self, name: str) -> str | None:
        """'running' | 'configured' | None — drives idempotent (re)provisioning."""
        rc, out, _err = await self._gce("list")
        if rc != 0:
            return None
        for cells in self._parsed_rows(out):
            if cells[-1] == name:  # exact Endpoint Name match, not a line substring
                return "running" if "Running" in cells[1] else "configured"
        return None

    async def write_config(self, name: str, manager_yaml: str, uep_yaml: str) -> None:
        if not _SAFE_NAME.match(name):  # the path is double-quoted for $HOME, so `$(…)` would EXECUTE remotely
            raise ValueError(f"unsafe endpoint name {name!r}: must match [A-Za-z0-9][A-Za-z0-9_.-]{{0,63}}")
        base = f"{self.remote_dir}/{name}"
        await self._write_file(f"{base}/config.yaml", manager_yaml)
        await self._write_file(f"{base}/user_config_template.yaml.j2", uep_yaml)

    async def _write_file(self, path: str, content: str) -> None:
        # Double-quote (NOT shlex.quote) so the remote shell expands $HOME; content
        # rides stdin so it needs no escaping. `path` is a controlled config path.
        rc, out, err = await ssh_exec(self.target, f'cat > "{path}"', stdin=content)
        if rc != 0:
            raise RuntimeError(f"remote write {path} failed: {(err or out).strip()}")

    async def seed_storage_db(self, local_db: Path, *, replace_ours: bool = False) -> None:
        """Ship a (trimmed) storage.db to the remote ~/.globus_compute/storage.db.

        The db is binary SQLite, so it rides stdin base64-encoded and is decoded
        remotely. The directory is created 0700 and the file chmod'd 0600 — this is a
        bearer credential. Raises RuntimeError on any remote step failure.

        A store already on the node is replaced only with `replace_ours` — when OUR record says hpc-bridge placed
        it (a copy of ours whose tokens went stale) — or when it holds no tokens at all; otherwise
        RemoteTokenStoreExists (review 2026-09-05 #5)."""
        payload = base64.b64encode(Path(local_db).read_bytes()).decode("ascii")
        db_path = f"{self.remote_dir}/storage.db"
        rc, out, err = await ssh_exec(
            self.target,
            f'mkdir -p "{self.remote_dir}" && chmod 700 "{self.remote_dir}"',
        )
        if rc != 0:
            raise RuntimeError(
                f"seed storage.db (mkdir) failed: {(err or out).strip()}"
            )
        # No-clobber, decided ON the login node in the same command as the write: `whoami` failing does not mean
        # there is no store (env_setup broke, gce is not on PATH, the tokens lack a scope, auth.globus.org is
        # unreachable from the node) — overwriting would destroy the user's credential, and teardown would then
        # delete our replacement (review 2026-09-05 #5).
        # Run the check-and-write in `sh`, not the user's login shell: under tcsh the `if [ … ]` line is a syntax error
        # that still runs the write (a silent overwrite), and fish cannot parse it at all. `set -C` (noclobber) closes
        # the check-then-write race too. stdin passes through `sh -c` to `base64 -d`.
        # A store with NO tokens holds no credential to protect: it is what `whoami` itself leaves on a node where
        # nobody has logged in (live 2026-10-06 — refusing it broke every first SSH bring-up), so it is replaced. The
        # check tries the gce environment's python3 first (the plain PATH may have none with sqlite3), then python3.
        # Not atomic with the rm: a store being CREATED in that instant (a `globus-compute-endpoint login` run by hand
        # at the same moment, which writes an empty store before it asks for the code) can be replaced — accepted, as
        # it holds no tokens yet; `set -C` still closes the race between the rm and our write.
        pys = " ".join(shlex.quote(c) for c in dict.fromkeys(x for x in (self.gce_python, "python3") if x))
        inner = (f'db="{db_path}"; ' + ('' if replace_ours else
                 f'if [ -e "$db" ]; then r=9; for py in {pys}; do if command -v "$py" >/dev/null 2>&1; then '
                 f'"$py" -c "{_EMPTY_STORE_PY}" "$db" 2>/dev/null; r=$?; [ "$r" -le 1 ] && break; fi; done; '
                 'if [ "$r" -eq 0 ]; then rm -f "$db" "$db-wal" "$db-shm" "$db-journal"; '
                 'elif [ "$r" -eq 1 ]; then echo HPCB_EXISTS; exit 3; else echo HPCB_UNCHECKED; exit 4; fi; fi; '
                 'set -C; ') + 'base64 -d > "$db"')
        rc, out, err = await ssh_exec(self.target, f"sh -c {shlex.quote(inner)}", stdin=payload)
        if "HPCB_UNCHECKED" in (out or ""):
            why = (self.last_whoami_error or "no output").strip()
            raise RemoteTokenStoreExists(
                f"a Globus token store already exists on {self.target.host} (~/.globus_compute/storage.db) and "
                "hpc-bridge could not check whether it holds tokens (no python3 with sqlite3 on the login node), so "
                f"it will not replace it. `globus-compute-endpoint whoami` failed there: {why[-300:]}. If nobody has "
                "logged in there, it is the empty store whoami leaves: remove it, or run `globus-compute-endpoint "
                "login` there yourself, then call connect_facility again. Nothing was started or billed."
            )
        if "HPCB_EXISTS" in (out or ""):
            why = (self.last_whoami_error or "no output").strip()
            raise RemoteTokenStoreExists(
                f"a Globus token store already exists on {self.target.host} (~/.globus_compute/storage.db) and "
                "is not empty (or could not be read), but "
                f"`globus-compute-endpoint whoami` failed there: {why[-300:]}. hpc-bridge will not replace a "
                "credential it did not create. Fix what whoami reports (the facility's env_setup, gce on PATH, the "
                "login node's network to auth.globus.org), or run `globus-compute-endpoint login` there yourself, "
                "then call connect_facility again. Nothing was started or billed."
            )
        if rc != 0:
            raise RuntimeError(
                f"seed storage.db (write) failed: {(err or out).strip()}"
            )
        rc, out, err = await ssh_exec(self.target, f'chmod 600 "{db_path}"')
        if rc != 0:
            raise RuntimeError(
                f"seed storage.db (chmod) failed: {(err or out).strip()}"
            )

    async def start(self, name: str) -> tuple[str, str | None, str | None]:
        # Start the daemon AND capture the login node it landed on in the SAME ssh
        # connection: the alias round-robins, so a separate hostname probe could resolve
        # a different node than the one now hosting the manager daemon. The sentinel
        # isolates the FQDN from gce's own stdout.
        # …and the ADDRESS this connection reached ($SSH_CONNECTION's server field): routable by construction, it is
        # the pin when the node's own name is not (internal names — Midway's *.rcc.local, a `.int.` domain).
        inner = (
            f"{self._env_prefix()}globus-compute-endpoint start {shlex.quote(name)} "
            "--detach && echo HPCB_HOST=$(hostname -f) "
            "&& echo \"HPCB_ADDR=$(echo \"$SSH_CONNECTION\" | awk '{print $3}')\""
        )
        rc, out, err = await ssh_exec(self.target, f"bash -lc {shlex.quote(inner)}", timeout=_BOOTSTRAP_SSH_S)
        if rc != 0:
            # A refused start whose manager is ALREADY running (its pidfile blocks a second start; gce 4.13
            # exits 73 and prints nothing at all — the "Another instance is running" text is not reliable)
            # is the endpoint we want: adopt it instead of failing (#39's second symptom).
            if _ANOTHER_INSTANCE in (err + out).lower() or await self.status(name) == "running":
                out = ""
            else:
                raise RuntimeError(f"remote start failed (rc={rc}): {(err or out).strip() or 'no output'}")
        host = addr = None
        for line in out.splitlines():
            if line.startswith("HPCB_HOST="):
                host = line[len("HPCB_HOST=") :].strip() or None
            elif line.startswith("HPCB_ADDR="):
                addr = line[len("HPCB_ADDR=") :].strip() or None
        return await self._await_endpoint_id(name), host, addr

    async def _await_endpoint_id(self, name: str) -> str:
        """`endpoint_id`, polled for up to `_START_REGISTER_WAIT_S` after a fresh start: the detached
        daemon registers (and the UUID appears in `list`) a few seconds after `start` returns."""
        deadline = time.monotonic() + _START_REGISTER_WAIT_S
        while True:
            try:
                return await self.endpoint_id(name)
            except RuntimeError as exc:
                if "could not find endpoint" not in str(exc) or time.monotonic() >= deadline:
                    raise
            await _sleep(_START_REGISTER_POLL_S)

    async def stop(self, name: str) -> tuple[int, str]:
        """`globus-compute-endpoint stop <name>` — returns (rc, stderr). Bounded: a fresh SSH to a loaded
        login node is slow, don't let it hold teardown hostage. rc 255 is ssh itself failing (no
        connection / denied) — the caller must treat that as "nothing happened", not "stopped". A
        non-zero gce rc can still mean the daemon died (a psutil traceback on the way out): the caller
        re-checks `status`."""
        rc, out, err = await self._gce("stop", name, timeout=_TEARDOWN_SSH_S)
        return rc, (err or out).strip()

    async def delete(self, name: str) -> bool:
        """`globus-compute-endpoint delete --yes <name>`: remove the endpoint's directory and its web
        registration. True when the CLI reported success. (Teardown used to stop only: the endpoint dir
        and registration stayed behind and the next connect silently re-adopted it — live 2026-09-04.)"""
        rc, out, err = await self._gce("delete", "--yes", name, timeout=_TEARDOWN_SSH_S)
        if rc == 0:
            return True
        if "currently running" not in (out + err).lower():
            return False
        # `stop` returns before the daemon has exited on a slow filesystem (Expanse, live 2026-09-04) and
        # `delete` refuses a running endpoint: wait for it to land, then force — the manager has been told to
        # stop and its blocks are cancelled by marker regardless.
        for _ in range(8):
            await _sleep(2.5)
            if await self.status(name) != "running":
                break
        rc, _out, _err = await self._gce("delete", "--yes", name, timeout=_TEARDOWN_SSH_S)
        if rc == 0:
            return True
        rc, _out, _err = await self._gce("delete", "--yes", "--force", name, timeout=_TEARDOWN_SSH_S)
        return rc == 0

    async def remove_uep_dirs(self, endpoint_id: str) -> None:
        """Remove this endpoint's per-worker `uep.<uuid>.*` dirs, which `gce delete` leaves behind.
        UUID-gated like `clean_uep_pidfiles`; best-effort."""
        if not _UUID_RE.match(endpoint_id):
            return
        try:
            await ssh_exec(self.target, f'rm -rf "{self.remote_dir}"/uep.{endpoint_id}.*', timeout=_TEARDOWN_SSH_S)
        except Exception:  # noqa: BLE001 - cleanup must never break teardown
            pass

    async def clean_uep_pidfiles(self, endpoint_id: str) -> None:
        """Remove stale per-UEP daemon.pid files left by force-killed workers, so a later rebuild's
        fresh user-endpoints don't hit "Another instance is running. Refusing to start." -> exit 73
        (#37). Scoped to this endpoint's UEP dirs by its UUID; best-effort (never raises)."""
        if not _UUID_RE.match(endpoint_id):
            return  # not a plain UUID -> don't build a remote glob from it
        # endpoint_id is a validated UUID (no shell metacharacters); the glob expands remotely, and
        # `rm -f` is silent when it matches nothing (a clean endpoint).
        try:
            await ssh_exec(
                self.target,
                f'rm -f "$HOME"/.globus_compute/uep.{endpoint_id}.*/daemon.pid',
                timeout=_TEARDOWN_SSH_S,
            )
        except Exception:  # noqa: BLE001 - cleanup must never break teardown
            pass

    async def wipe_storage_db(self) -> bool:
        """Remove the seeded credential from the remote host. True only when the remote `rm` succeeded —
        teardown's "token copy removed" is spoken from this, never assumed."""
        rc, _out, _err = await ssh_exec(self.target, f'rm -f "{self.remote_dir}/storage.db"', timeout=_TEARDOWN_SSH_S)
        return rc == 0

    async def cancel_blocks(self, endpoint_id: str, scheduler: str = "slurm") -> list[str]:
        """Best-effort cancel of THIS endpoint's scheduler blocks; returns the cancelled IDs.

        An ungraceful `stop` (it can die on a psutil traceback) won't scale Parsl's block in,
        so the compute keeps its allocation until walltime. We find our blocks precisely by
        their StdOut path, which Parsl writes under the endpoint's UEP dir
        (`uep.<endpoint_id>.*`) — so we never touch another GlobusComputeEngine endpoint's
        jobs. Never raises: teardown must not crash on a flaky scheduler query."""
        marker = f"uep.{endpoint_id}"
        if scheduler == "pbs":
            return await self._cancel_blocks_pbs(marker)
        try:
            squeue = 'squeue -u "$USER" -h -O "JobID:30,StdOut:1024" 2>/dev/null'
            rc, out, _err = await ssh_exec(
                self.target, f"bash -lc {shlex.quote(squeue)}", timeout=_TEARDOWN_SSH_S
            )
        except Exception:  # noqa: BLE001 - scheduler unreachable -> nothing to cancel
            return []
        if rc != 0:
            return []
        ids = [
            line.split()[0]
            for line in out.splitlines()
            if marker in line and line.split() and _JOBID.match(line.split()[0])
        ]
        if ids:
            try:
                await ssh_exec(
                    self.target,
                    f"bash -lc {shlex.quote('scancel ' + ' '.join(ids))}",
                    timeout=_TEARDOWN_SSH_S,
                )
            except Exception:  # noqa: BLE001 - best-effort
                pass
        return ids

    async def _cancel_blocks_pbs(self, marker: str) -> list[str]:
        """PBS variant of cancel_blocks: qstat -f -> unwrap continuations -> match marker -> qdel.

        Bare `qstat -f` (NOT `-u $USER`): on PBS Pro the -u filter suppresses full-format output
        entirely, so `qstat -f -u $USER` returns nothing and the cancel silently no-ops (caught in
        live Polaris validation). The endpoint-unique `uep.<eid>` marker scopes the match to us."""
        try:
            q = "qstat -f 2>/dev/null | sed ':a;N;$!ba;s/\\n\\t//g'"
            rc, out, _err = await ssh_exec(
                self.target, f"bash -lc {shlex.quote(q)}", timeout=_TEARDOWN_SSH_S
            )
        except Exception:  # noqa: BLE001 - scheduler unreachable -> nothing to cancel
            return []
        if rc != 0:
            return []
        ids: list[str] = []
        for record in out.split("Job Id: ")[1:]:
            jid = record.split(None, 1)[0] if record.split() else ""
            if marker in record and _JOBID_PBS.match(jid):
                ids.append(jid)
        if ids:
            try:
                await ssh_exec(
                    self.target,
                    f"bash -lc {shlex.quote('qdel ' + ' '.join(ids))}",
                    timeout=_TEARDOWN_SSH_S,
                )
            except Exception:  # noqa: BLE001 - best-effort
                pass
        return ids

    async def endpoint_id(self, name: str) -> str:
        rc, out, err = await self._gce("list")
        if rc != 0:
            raise RuntimeError(f"remote list failed: {(err or out).strip()}")
        for cells in self._parsed_rows(out):
            if cells[-1] == name:  # exact Endpoint Name match, not a line substring
                m = _UUID.search(cells[0])
                if m:
                    return m.group(0)
        raise RuntimeError(f"could not find endpoint {name!r} in `list` output")

    async def whoami(self) -> bool:
        """True if the remote endpoint can authenticate (storage.db usable). On failure the remote's own words are
        kept in `last_whoami_error` — seed_storage_db quotes them if it then finds a store it must not replace. Also
        notes the gce environment's python3 (`gce_python`): the interpreter seed_storage_db tries first to check
        whether a store holds tokens, since the login node's plain PATH may have none with sqlite3."""
        inner = (f"export COLUMNS={_GCE_COLUMNS}; {self._env_prefix()}{{ command -v python3 >/dev/null 2>&1 && "
                 f'echo "{_PY_SENTINEL}$(command -v python3)"; globus-compute-endpoint whoami; }}')
        rc, out, err = await ssh_exec(self.target, f"bash -lc {shlex.quote(inner)}", timeout=_BOOTSTRAP_SSH_S)
        kept = []
        for line in (out or "").splitlines():
            if line.startswith(_PY_SENTINEL):
                self.gce_python = line[len(_PY_SENTINEL):].strip() or None
            else:
                kept.append(line)
        out = "\n".join(kept)
        self.last_whoami_error = None if rc == 0 else ((err or out or "").strip() or f"rc={rc}")
        return rc == 0

    def rebind(self, host: str) -> None:
        """Re-point this CLI at a specific host (the pinned FQDN) for reconnect. The host KEY is still
        checked against the name the user trusted (HostKeyAlias), so a pin cannot redirect to a
        different machine. known_hosts is keyed by the RESOLVED HostName (an ssh-config alias such as
        `globus1` stores its key under `globus1.cs.uchicago.edu`), hence `ssh -G` (found live 2026-09-04)."""
        alias = self.target.host_key_alias or _resolved_hostname(self.target.host)
        self.target = replace(self.target, host=host, host_key_alias=alias)

    async def close(self) -> bool:
        """Close the SSH ControlMaster for this target (best-effort, bounded). True when a master was
        there and took the exit request — the teardown notice's "connection closed" is spoken from this.

        `ControlPersist` self-reaps an idle master, so this is just a prompt, explicit teardown of
        the shared connection at endpoint end — not load-bearing. False when multiplexing is off."""
        if self.target.control_dir is None:
            return False
        try:
            proc = await asyncio.create_subprocess_exec(
                *self.target.control_argv("exit"),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.communicate(), _TEARDOWN_SSH_S)
            return proc.returncode == 0
        except Exception:  # noqa: BLE001 - no master / already gone / slow sshd: ControlPersist reaps it
            return False


# `strategy_period: 5` (both templates; 30 on endpoints configured before this change): a new user endpoint scales
# out only on its first scaling pass, one period after it starts, and a first SSH result pays that wait twice
# (login shape, then compute). It is not the scheduler query rate (parsl polls Slurm/PBS at most every 60 s).
# warmth._IDLE_GRACE_S is two of these passes plus 5 s; a test ties them.
_SLURM_TEMPLATE = """\
engine:
  type: GlobusComputeEngine
  run_in_sandbox: true
  max_workers_per_node: {{ max_workers_per_node | default(@@MAXW@@) }}
  address:
    type: address_by_interface
    ifname: {{ interface | default(@@IFACE@@) }}
{% if available_accelerators is defined and available_accelerators %}
{% if available_accelerators is iterable and available_accelerators is not string %}
  available_accelerators: [{{ available_accelerators | join(', ') }}]
{% else %}
  available_accelerators: {{ available_accelerators }}
{% endif %}
{% endif %}
  job_status_kwargs:
    max_idletime: @@IDLE@@
    strategy_period: 5
  provider:
    type: {{ provider_type | default('SlurmProvider') }}
{% if compute | default(true) %}
    partition: {{ partition | default(@@PARTITION@@) }}
    account: {{ account | default(@@ACCOUNT@@) }}
    walltime: {{ walltime | default(@@WALLTIME@@) }}
    nodes_per_block: {{ nodes_per_block | default(@@NODES@@) }}
    worker_init: {{ worker_init | default(@@WORKER_INIT@@) }}
    launcher:
      type: SrunLauncher
    init_blocks: {{ init_blocks | default(@@EAGER@@) }}
    min_blocks: 0
    max_blocks: {{ max_blocks | default(@@MAXBLK@@) }}
{% if exclusive is defined and exclusive is not none %}
    exclusive: {{ 'true' if exclusive else 'false' }}
{% endif %}
{% if scheduler_options is defined and scheduler_options %}
    scheduler_options: {{ scheduler_options }}
{% endif %}
{% else %}
    init_blocks: 1
    min_blocks: 0
    max_blocks: {{ max_blocks | default(@@MAXBLK@@) }}
{% endif %}
"""

# PBS: `account:` is emitted ONLY when non-empty. parsl's PBSProProvider adds `-A <account>` whenever the value is not
# None, so an empty string produced a bare `-A` and qsub took the SCRIPT PATH as the account name, then read the script
# from stdin — an empty "STDIN" job that exited 127, which the pilot probe then reported as "never submitted" (found on
# the fake OpenPBS cluster, 2026-09-06: every facility we had met before required an account, so it never showed).
_PBS_TEMPLATE = """\
engine:
  type: GlobusComputeEngine
  run_in_sandbox: true
  max_workers_per_node: {{ max_workers_per_node | default(@@MAXW@@) }}
  address:
    type: address_by_interface
    ifname: {{ interface | default(@@IFACE@@) }}
{% if available_accelerators is defined and available_accelerators %}
{% if available_accelerators is iterable and available_accelerators is not string %}
  available_accelerators: [{{ available_accelerators | join(', ') }}]
{% else %}
  available_accelerators: {{ available_accelerators }}
{% endif %}
{% endif %}
  job_status_kwargs:
    max_idletime: @@IDLE@@
    strategy_period: 5
  provider:
    type: {{ provider_type | default('PBSProProvider') }}
{% if compute | default(true) %}
{% if (account | default(@@ACCOUNT@@)) not in ("", '""', none) %}
    account: {{ account | default(@@ACCOUNT@@) }}
{% endif %}
    queue: {{ partition | default(@@PARTITION@@) }}
    walltime: {{ walltime | default(@@WALLTIME@@) }}
    nodes_per_block: {{ nodes_per_block | default(@@NODES@@) }}
{% if cpus_per_node is defined and cpus_per_node %}
    cpus_per_node: {{ cpus_per_node }}
{% endif %}
    worker_init: {{ worker_init | default(@@WORKER_INIT@@) }}
    launcher:
      type: SingleNodeLauncher
    init_blocks: {{ init_blocks | default(@@EAGER@@) }}
    min_blocks: 0
    max_blocks: {{ max_blocks | default(@@MAXBLK@@) }}
{% if scheduler_options is defined and scheduler_options %}
    scheduler_options: {{ scheduler_options }}
{% endif %}
{% else %}
    init_blocks: 1
    min_blocks: 0
    max_blocks: {{ max_blocks | default(@@MAXBLK@@) }}
{% endif %}
"""


# ---------------------------------------------------------------- the facility


class SlurmFacility:
    """A Globus Compute endpoint on a remote Slurm cluster, provisioned over SSH."""

    def __init__(
        self,
        profile: MachineProfile,
        cli: RemoteEndpointCLI,
        *,
        client_factory=None,
        store: LoginNodeStore | None = None,
        alias: str | None = None,
        auth_method: str = "ssh-key",
    ) -> None:
        self.profile = profile
        self.cli = cli
        self.name = profile.name
        self._client_factory = client_factory or self._default_client
        self.store = store
        self.alias = alias
        # The catalog entry's `auth_method` ("ssh-key" | "mfa-otp"): the ops that MUST SSH (bootstrap, teardown)
        # consult it to raise the one-time-code handoff BEFORE a failing attempt, instead of after one.
        self.auth_method = auth_method

    @property
    def scratch_root(self) -> str | None:
        return self.profile.scratch_root

    def config_template(self, hpc: Profile) -> tuple[str, dict]:
        """Return (jinja_template_str, default_user_opts) for the UEP template.

        ONE template serves every shape: provider.type and resources are Jinja
        variables rendered per task from user_endpoint_config (see shapes.py). Defaults
        come from the MachineProfile so a bare submit still resolves. min_blocks is
        always 0 (+ max_idletime) so an idle slurm block self-releases — the cost net,
        validated live on Anvil. LocalProvider ignores the slurm keys.

        Profile defaults are injected as json.dumps'd literals via sentinel
        substitution (not %-formatting) so values containing quotes/%/braces can't
        break Jinja compilation.

        Two render-time invariants imposed by the endpoint manager (`render_config_user_template`
        → `_sanitize_user_json`), both learned by live debugging:
        - It json.dumps's every *string* user_opt, so `"SlurmProvider"` arrives as
          `'"SlurmProvider"'`. Branch on the BOOLEAN `compute` (bools pass through the
          sanitizer unchanged), never a string equality — a string compare silently fails
          and drops the whole provider block.
        - Because the sanitizer already quotes strings, the template must NOT also `| tojson`
          a string value (worker_init, scheduler_options) — that double-encodes it (embedded
          quotes) and breaks the worker. The json.dumps'd defaults are pre-quoted to match."""
        p = self.profile
        eager = 1 if hpc.mode == "interactive" else 0
        template = _PBS_TEMPLATE if p.scheduler == "pbs" else _SLURM_TEMPLATE
        subs = {
            "@@MAXW@@": str(p.max_workers_per_node),
            "@@IFACE@@": json.dumps(p.interface),
            "@@IDLE@@": repr(float(hpc.max_idletime_s)),
            "@@PARTITION@@": json.dumps(p.partition),
            "@@ACCOUNT@@": json.dumps(p.account),
            "@@WALLTIME@@": json.dumps(p.walltime),
            "@@WORKER_INIT@@": json.dumps(p.worker_init),
            "@@EAGER@@": str(eager),
            "@@NODES@@": str(p.nodes_per_block),
            "@@MAXBLK@@": str(p.max_blocks),
        }
        for token, value in subs.items():
            template = template.replace(token, value)
        defaults: dict = {
            "interface": p.interface,
            "partition": p.partition,
            "account": p.account,
            "walltime": p.walltime,
            "worker_init": p.worker_init,
            "max_workers_per_node": p.max_workers_per_node,
            "nodes_per_block": p.nodes_per_block,
            "max_blocks": p.max_blocks,
        }
        if p.scheduler_options is not None:
            defaults["scheduler_options"] = p.scheduler_options
        if p.available_accelerators is not None:
            defaults["available_accelerators"] = p.available_accelerators
        if p.cpus_per_node is not None:
            defaults["cpus_per_node"] = p.cpus_per_node
        if p.exclusive is not None and p.scheduler == "slurm":
            defaults["exclusive"] = p.exclusive
        return template, defaults

    async def bootstrap(self, hpc: Profile) -> EndpointHandle:
        """Full first-run bootstrap: ensure remote creds, provision, and record the
        login-node FQDN so later sessions reconnect direct-to-node.

        The FQDN is captured in the same SSH connection that starts the daemon (the
        alias round-robins, so a separate probe could name the wrong node). A reused
        already-running endpoint keeps its prior recorded node; reconciling a stale or
        dead pin is deferred. Idempotent: seeds storage.db only if the remote can't
        already authenticate, and reuses a running endpoint.

        SSH-once: before any SSH, ask the Globus web service whether an endpoint we own is
        already online and reuse it over AMQP — zero SSH, so an MFA facility isn't re-auth'd.
        Only when none is online do we fall through to the one allowed SSH bootstrap below."""
        reused = await self.find_online_endpoint(self.profile.endpoint_name)
        if reused is not None:
            return EndpointHandle(endpoint_id=reused, name=self.profile.endpoint_name, reused=True)
        store, alias = self.store, self.alias
        # ONE read of what an earlier run recorded, before anything below rewrites it: who placed the remote token
        # store, and which node the endpoint was pinned to (review 2026-10-05).
        prior = (store.get(alias=alias, name=self.profile.endpoint_name)
                 if store is not None and alias is not None else None)
        # an earlier run of ours placed the store — for THIS login (a record is keyed by alias + endpoint name only)
        ours = bool(prior is not None and prior.seeded_credentials and prior.user == self.cli.target.user)
        seeded = False
        if not await self.cli.whoami():
            with tempfile.TemporaryDirectory() as tmp:
                trimmed = build_minimal_storage_db(
                    src_path=Path(_get_storage_filepath()),
                    dst_path=Path(tmp) / "storage.db",
                    namespace=_resolve_namespace(),
                )
                # our OWN stale copy is refreshed (a logout, a scope change); anyone else's store is refused
                await self.cli.seed_storage_db(trimmed, replace_ours=ours)
            seeded = True
            if store is not None and alias is not None:
                # Record the seed NOW: a bootstrap that dies before provision completes (a first-connect
                # install past the ssh timeout — Expanse, live 2026-09-04) must not orphan a store we placed.
                # Carry the prior endpoint id and pin: this write must not erase them before provision runs.
                store.put(EndpointRecord(
                    endpoint_id=prior.endpoint_id if prior is not None else "",
                    login_host=prior.login_host if prior is not None else None,
                    alias=alias, user=self.cli.target.user,
                    key_path=self.cli.target.key_path, name=self.profile.endpoint_name,
                    provisioned_at=datetime.now(UTC).isoformat(), seeded_credentials=True,
                ))
        else:
            seeded = ours
        self._seeded_credentials = seeded  # remembered for teardown in THIS process; the store keeps it across sessions
        handle = await self.provision(hpc)
        if self.store is not None and self.alias is not None:
            # written even with no routable pin (login_host None): the record also carries WHO seeded the
            # remote token store, which teardown in a later call/session needs (live 2026-09-04: the flag
            # lived on a facility object connect rebuilds every call, so teardown never wiped)
            # An ADOPTED endpoint (already running: provision's reuse branch, or start()'s "another instance") has no
            # fresh node to report (login_host None) — keep the node we pinned when we launched it, or the next
            # session's control-plane SSH goes to the round-robin alias and orphans it (review 2026-09-05 #4; the
            # guard #79 removed when it made this write unconditional).
            # ...but only for the SAME endpoint: a re-registered one must not inherit the old one's node.
            same = prior is not None and prior.endpoint_id == handle.endpoint_id
            self.store.put(
                EndpointRecord(
                    endpoint_id=handle.endpoint_id,
                    login_host=handle.login_host or (prior.login_host if prior is not None and same else None),
                    alias=self.alias,
                    user=self.cli.target.user,
                    key_path=self.cli.target.key_path,
                    name=handle.name,
                    provisioned_at=datetime.now(UTC).isoformat(),
                    seeded_credentials=seeded,
                )
            )
        return handle

    def _seeded_by_us(self) -> bool:
        """Did hpc-bridge place the remote token store? This process remembers its own bootstrap; across
        sessions the endpoint record does. Unknown ⇒ False: never delete a credential we did not create."""
        if getattr(self, "_seeded_credentials", False):
            return True
        if self.store is not None and self.alias is not None:
            rec = self.store.get(alias=self.alias, name=self.profile.endpoint_name)
            return bool(rec is not None and rec.seeded_credentials)
        return False

    async def provision(self, hpc: Profile) -> EndpointHandle:
        # Idempotent: reuse a running endpoint; configure only if it doesn't exist yet
        # (re-configuring an existing one raises ConfigExists).
        name = self.profile.endpoint_name
        st = await self.cli.status(name)
        if st == "running":
            # REUSE: we did NOT launch it, so its node is unknown from a fresh round-robin probe —
            # leave login_host None and keep any prior record. We do NOT probe-and-declare-"wedged"
            # here: a running-but-cold worker and a genuine hang are indistinguishable by a probe, and
            # a slow facility must be allowed to warm. The canary flow reports 'provisioning' until a
            # worker answers; a persistent hang is the agent's call to teardown+reconnect (#37).
            return EndpointHandle(endpoint_id=await self.cli.endpoint_id(name), name=name, reused=True)
        if st is None:
            await self.cli.configure(name)
        else:
            # A stopped/configured endpoint being re-started may carry stale per-UEP daemon.pid files
            # from force-killed workers of a prior run; clear them BEFORE the fresh manager forks its
            # workers, else the worker refuses to start ("Another instance is running.") -> exit 73
            # (#37). Scoped to this endpoint's UUID. This is the definite bug fix behind "won't warm".
            await self.cli.clean_uep_pidfiles(await self.cli.endpoint_id(name))
        # config.yaml is the engine-free MANAGER config (amqp_port lives here); the
        # engine goes in the UEP template (v4 manager+template model). Rewrite both so
        # a re-provision always applies the current profile.
        manager = yaml.safe_dump(
            {
                "display_name": self.profile.display_name or name,
                "amqp_port": self.profile.amqp_port,
            },
            sort_keys=False,
        )
        uep, _defaults = self.config_template(hpc)
        await self.cli.write_config(name, manager, uep)
        eid, host, addr = await self.cli.start(name)
        # the node's FQDN when routable from here, else the address this connection reached, else the alias
        host = await asyncio.to_thread(_routable_pin, host, addr)
        if host:
            # Pin the live session to the node the manager daemon actually landed on, so
            # later control-plane ops — above all teardown — reach THIS node instead of
            # the round-robin alias. Without this, `stop` hits a different login node,
            # the daemon survives, and the endpoint is orphaned (the very failure the
            # login-node pin exists to prevent).
            self.cli.rebind(host)
        return EndpointHandle(endpoint_id=eid, name=name, login_host=host)

    async def teardown(self, endpoint_id: str, *, wipe_credentials: bool = False) -> dict[str, Any]:
        """Stop the endpoint, cancel its scheduler block(s), DELETE it (directory + registration + its
        `uep.*` dirs) and drop its record — the explicit destroy. Every op runs over SSH, bounded by
        `_TEARDOWN_SSH_S`. `wipe_credentials=True` also removes the remote storage.db — but ONLY if
        hpc-bridge seeded it (`_seeded_by_us`): a token store that was already on the login node (a shared
        account the facility's own endpoint uses, say) is never ours to delete.

        Returns what was MEASURED, so the tool's notice can tell the truth (review 2026-09-05: every word
        used to come from an intention): {"stopped", "deleted", "credentials_wiped", "ssh_closed",
        "ssh_failed": bool, "error": str}. If the very first SSH op fails at the transport/auth layer
        (rc 255 — no connection, or a one-time-code facility whose shared connection expired), NOTHING
        else is attempted and `ssh_failed` is set: the manager is still running, the record (pin +
        seeded-flag) is kept so a later teardown can still do the job. The record is dropped only
        when the delete actually happened."""
        name = self.profile.endpoint_name
        try:
            rc, err = await self.cli.stop(name)
        except TimeoutError:  # a slow filesystem: the stop may be finishing there — the status re-check below decides
            rc, err = None, "the stop timed out on the login node"
        if rc == 255:  # ssh itself failed: nothing ran on the login node
            return {"stopped": False, "deleted": False, "credentials_wiped": False, "ssh_closed": False,
                    "ssh_failed": True, "error": err[:400]}
        # a non-zero gce rc can be a psutil traceback on the way out with the daemon in fact gone: re-check. A
        # re-check that TIMES OUT proves nothing either way — report "unconfirmed" (None), never "still running".
        stopped: bool | None
        try:
            stopped = rc == 0 or (await self.cli.status(name)) == "configured"
        except TimeoutError:
            stopped, err = None, (err or "").strip() + " (the status re-check timed out)"
        # `stop` kills the manager, but an ungraceful stop leaves Parsl's block holding the
        # allocation until walltime (no manager left to scale it in). Explicitly cancel this
        # endpoint's blocks so "teardown released the compute" actually holds.
        await self.cli.cancel_blocks(endpoint_id, self.profile.scheduler)
        # A slow filesystem (Expanse: minutes) can time an SSH op out AFTER the stop succeeded: report what is known
        # instead of letting the timeout escape as "nothing was removed, still running" (review 2026-10-05).
        delete_error = ""
        try:
            deleted = await self.cli.delete(name)  # the directory + registration — else the next connect re-adopts it
        except TimeoutError:
            deleted, delete_error = False, "the delete timed out on the login node (it may still complete there)"
        try:
            await self.cli.remove_uep_dirs(endpoint_id)
        except TimeoutError:
            pass  # leftover uep.* dirs are inert; the next teardown or a manual rm clears them
        wiped = False
        if wipe_credentials and self._seeded_by_us():
            try:
                wiped = await self.cli.wipe_storage_db()
            except TimeoutError:
                wiped = False
        if deleted and self.store is not None and self.alias is not None:
            self.store.remove(alias=self.alias, name=name)  # no endpoint, no pin, no seeded-flag to carry
        elif wiped and self.store is not None and self.alias is not None:
            # the record outlives a failed delete, but the store we placed is gone: never let a later bootstrap
            # believe a store found there is ours to replace (review 2026-10-05)
            rec = self.store.get(alias=self.alias, name=name)
            if rec is not None and rec.seeded_credentials:
                self.store.put(replace(rec, seeded_credentials=False))
            self._seeded_credentials = False
        # `ssh_closed`: the tool's notice says so, else the agent infers the connection is "still open" (it did,
        # live 2026-09-04) and tells the user something false about what is left on their machine.
        ssh_closed = bool(await self.cli.close())  # drop the shared SSH master; the endpoint is gone
        return {"stopped": stopped, "deleted": deleted, "credentials_wiped": wiped, "ssh_closed": ssh_closed,
                "ssh_failed": False, "error": "" if stopped else (err or "")[:400], "delete_error": delete_error}

    async def login_exec(self, command: str) -> tuple[int, str, str]:
        """Read-only login-node command for discovery — no block, no allocation (delegates
        to the SSH CLI). This is the channel that backs the `login_shell` tool."""
        return await self.cli.login_exec(command)

    async def manager_online(self, endpoint_id: str) -> bool:
        # Web-service query (runs in the MCP process, not over SSH) — works for any
        # endpoint UUID regardless of where it runs.
        client = self._client_factory()
        status = await asyncio.to_thread(client.get_endpoint_status, endpoint_id)
        return status.get("status") == "online"

    async def find_online_endpoint(self, name: str) -> str | None:
        """UUID of an *online* endpoint we own named `name`, else None — via the Globus web service,
        NO SSH. The SSH-once keystone: a fresh session reuses a still-running endpoint from a prior
        session over AMQP, instead of re-SSHing the login node (which on an MFA facility could force
        a re-auth). Bootstrap (the one allowed SSH) is the fallback.

        We gate reuse on `manager_online` alone — NOT a liveness probe. A probe cannot distinguish a
        dead ghost from a just-started endpoint whose worker is still cold-starting, so probe-gating
        false-rejects a healthy fresh endpoint and forces a re-bootstrap that then trips the manager's
        own pidfile ("Another instance is running") (#37, regression seen in endpoint_reuse_chain). A
        genuinely dead 'online' ghost is instead handled gracefully downstream: the robust canary maps
        its shut-down Executor to not-warm ('provisioning'), which the agent recovers by teardown.

        Web errors (e.g. the local Globus identity isn't logged in) are swallowed to None so we fall
        through to the SSH path, which surfaces the auth problem with a clearer error."""
        client = self._client_factory()
        try:
            endpoints = await asyncio.to_thread(client.get_endpoints, "owner")
        except Exception as exc:  # noqa: BLE001 - web/auth failure -> can't reuse, fall back to SSH
            print(f"hpc-bridge: endpoint reuse check failed ({type(exc).__name__}: {exc}); "
                  "falling back to SSH bootstrap", file=sys.stderr)
            return None
        for ep in endpoints or []:
            ep_name = ep.get("name") or ep.get("display_name")
            ep_id = ep.get("uuid") or ep.get("endpoint_uuid") or ep.get("id")
            if ep_name != name or not ep_id:
                continue
            if await self.manager_online(ep_id):  # our endpoint, reported online -> reuse over AMQP
                return ep_id
        return None

    def _default_client(self):
        from globus_compute_sdk import Client

        return Client()
