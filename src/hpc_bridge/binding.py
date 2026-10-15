"""Binding a machine: build the Facility for a catalog entry, the runtime catalog (the public registry),
the session/BYO helpers and the local stores (split step 5, 2026-09-03).

Everything here answers "how do we reach this facility": the SSH facility (login name from ~/.ssh/config,
key, ControlMaster, login-node pin) or the facility-run multi-user endpoint; the registry client
(anonymous unless a Search-scoped login exists); the session endpoint name; the facilities.json store.
The orchestration that USES a bound facility (connect, provision, run, stop) stays in `server`.

Tests patch names on THIS module (`binding.make_catalog`, `binding._facility_from_entry`,
`binding._make_search_client`, `binding._ssh_config_user`) and `config._control_settings`; callers
everywhere go through the owning module's attribute so a patch reaches them.
"""
from __future__ import annotations

import datetime
import os
import re

from . import config
from .catalog.entry import Allocation, CatalogEntry, Compute, Defaults, scheduler_terms_directive
from .config import _env_endpoint_id, _require_env
from .endpoint import EndpointCLI
from .facility.base import Facility
from .facility.local import LocalFacility
from .models import DefaultSource, FacilityDetails, TermsOrigin, validate_host
from .state import FacilityStore

# The facility_key of an entry built from a user's own details (`_entry_from_details`): a bring-your-own facility.
SESSION_KEY = "session"
# What HPC_BRIDGE_MACHINE resolved to at the startup pin (its value -> the entry's subject): the pin resolves aliases
# too, and the presets must follow the machine it named, however it was named (`_names_machine`).
_PINNED: dict[str, str] = {}
# Where the startup pin's default terms came from (its value -> TermsOrigin), for lifespan to bind with the facility.
_PINNED_ORIGIN: dict[str, TermsOrigin] = {}


def _names_machine(entry) -> bool:
    """Whether HPC_BRIDGE_MACHINE names `entry` — by id or subject, or by the alias the startup pin resolved to it: the
    one facility the env presets apply to."""
    if config.names_machine(entry.id, entry.subject):
        return True
    machine = config.machine()
    return machine is not None and _PINNED.get(machine) == entry.subject


def _ssh_config_user(host: str) -> str:
    """The login name OpenSSH would use for `host`, honoring ~/.ssh/config — via a local, no-connect
    `ssh -G`. Sources the user from the config the user already maintains, not a boot-env var the
    already-running server can't see. Falls back to the local username if `ssh -G` is unavailable."""
    import getpass
    import subprocess

    try:
        out = subprocess.run(["ssh", "-G", host], capture_output=True, text=True, timeout=10).stdout
        for line in out.splitlines():
            k, _, v = line.strip().partition(" ")
            if k.lower() == "user" and v.strip():
                return v.strip()
    except Exception:  # noqa: BLE001 - no ssh binary / odd host -> local username
        pass
    return getpass.getuser()

def _slurm_facility(profile, *, alias: str, user: str, auth_method: str = "ssh-key") -> Facility:
    """Wire a Slurm `MachineProfile` into a `SlurmFacility` over SSH — shared by the catalog
    and the hardcoded-Anvil paths."""
    from .facility.remote import RemoteEndpointCLI, SlurmFacility, SshTarget, _routable_pin
    from .state import LoginNodeStore

    control_dir, persist = config._control_settings()  # multiplex all SSH over one ControlMaster (MFA-once)
    key = (config.ssh_key() or "")  # else defer to ~/.ssh/config IdentityFile
    target = SshTarget(
        host=alias,
        user=user,
        key_path=os.path.expanduser(key) if key else None,
        control_dir=control_dir,
        control_persist=persist,
    )
    cli = RemoteEndpointCLI(target, profile.env_setup)
    store = LoginNodeStore()
    rec = store.get(alias=alias, name=profile.endpoint_name)
    pin = _routable_pin(rec.login_host) if rec is not None else None
    if pin is not None:  # reconnect direct-to-node (routable pins only) instead of the round-robin alias
        # An internal-only pin (e.g. Midway's beagle3-tbd1.rcc.local) is dropped -> stay on the alias.
        # A routable-but-dead pin fails fast (BatchMode) -> CANNOT REACH, and connect_facility then DROPS
        # the pin (_drop_dead_pin) so the next attempt resolves the canonical host again.
        cli.rebind(pin)
    return SlurmFacility(profile, cli, store=store, alias=alias, auth_method=auth_method)

def _unsupported_entry_reason(entry) -> str | None:
    """Why this catalog entry can't drive a stand-up, or None.

    A `compute_mep_uuid` entry is a facility-run multi-user endpoint: consumed with zero SSH, and
    with NO login shape — so an allocation LISTING (which runs over the free login node) has no
    channel on it. Such an entry must omit `allocation`; the account (if any) is supplied directly."""
    if entry.compute_mep_uuid:
        if entry.allocation is not None:
            return (
                "a multi-user-endpoint entry cannot list allocations (no login-node channel on a "
                "MEP) — drop `allocation` and let ensure_endpoint_up(account=…) take the account"
            )
        return None
    if entry.compute.scheduler not in ("slurm", "pbs"):
        return f"scheduler {entry.compute.scheduler!r} not supported yet (slurm/pbs only)"
    return None

def _partition_preset(entry) -> str | None:
    """HPC_BRIDGE_PARTITION, when it is this entry's default partition: an SSH facility from the registry that
    HPC_BRIDGE_MACHINE names. Never a facility endpoint (its defaults are the entry's) nor one you bring yourself
    (its details= give the partition)."""
    if entry.compute_mep_uuid or entry.facility_key == SESSION_KEY or not _names_machine(entry):
        return None
    return config.partition()

def _terms_origin(entry, *, discovered: bool = False) -> TermsOrigin:
    """Where the default terms of the facility built from `entry` come from (BlockTerms' sources): its partition from
    the registry, the HPC_BRIDGE_PARTITION preset, or — a facility you bring yourself — its details (`discovered`: the
    probe's proposal, kept); whether the registry says an account is needed (None for your own: nothing curated says);
    and the entry's account hint."""
    byo = entry.facility_key == SESSION_KEY
    partition: DefaultSource = (("discovered" if discovered else "byo_details") if byo
                                else "env" if _partition_preset(entry) else "registry")
    return TermsOrigin(partition=partition, account_needed=None if byo else bool(entry.account_required),
                       account_hint=getattr(entry, "account_hint", None))

def _facility_from_entry(entry, *, account: str, pinned_host: str | None = None) -> Facility:
    """Build the facility for a catalog entry + per-user runtime values — shared by the startup
    path (make_facility) and the runtime path (connect_facility). `account` may be empty for the
    agentic flow; ensure_endpoint_up(account=…) overrides it per scheduler block.

    HPC_BRIDGE_PARTITION is the default partition of ONE facility: the one HPC_BRIDGE_MACHINE names (`_names_machine`:
    the startup pin's, however it was named, or the same entry reached by connect_facility). Any other facility keeps
    its own default, and so does a bring-your-own one, whose details= give the partition explicitly. A facility MEP
    never takes it: its defaults are the entry's.

    A `compute_mep_uuid` entry builds a **MEPFacility** (zero SSH: no login name, no key, no host —
    the facility's identity mapping is the access) and MEP wins if an entry somehow carries both
    reaches. Otherwise a SlurmFacility over SSH.

    `pinned_host` overrides the entry's `ssh_host` and is passed ONLY on the env-pinned startup path
    (HPC_BRIDGE_MACHINE + HPC_BRIDGE_SSH_HOST). The agentic connect path leaves it None so the BOUND
    facility's own `ssh_host` is authoritative — a process-wide env must never silently redirect an
    agent-chosen facility to a different host (the "globus1 is Aurora" trap, [#35])."""
    if entry.compute_mep_uuid:
        from .facility.mep import MEPFacility

        return MEPFacility.from_entry(entry, account=account or None)

    from .facility.remote import profile_from_catalog_entry

    alias = validate_host(pinned_host) if pinned_host else entry.ssh_host  # an env pin reaches ssh argv too
    # Login name: optional env override, else read live from ~/.ssh/config (`ssh -G`) — never a
    # *required* boot-env var. The key is deferred to the config's IdentityFile in _slurm_facility.
    user = (config.ssh_user() or "") or _ssh_config_user(alias)
    profile = profile_from_catalog_entry(
        entry,
        user=user,
        account=account,
        partition=_partition_preset(entry),
        venv=config.remote_venv(),
    )
    return _slurm_facility(profile, alias=alias, user=user, auth_method=getattr(entry, "auth_method", "ssh-key"))

async def _catalog_facility(machine: str) -> Facility:
    """Build a facility from a catalog entry (HPC_BRIDGE_MACHINE), sourcing the machine config
    from `make_catalog()` (the live Globus Search index — HPC_BRIDGE_SEARCH_INDEX; no bundled
    fallback). v1 slice: SSH-bootstrap Slurm/PBS machines only."""
    entry = await make_catalog().get(machine)
    if entry is None:
        raise RuntimeError(f"HPC_BRIDGE_MACHINE={machine!r} not found in the catalog")
    reason = _unsupported_entry_reason(entry)
    if reason:
        raise RuntimeError(f"{machine}: {reason}")
    if entry.compute_mep_uuid:
        # A MEP entry IS the endpoint: a stray HPC_BRIDGE_ENDPOINT_ID (the BYO-UUID hatch this entry
        # supersedes) would otherwise seed app.state and silently win over the entry's UUID while
        # the entry's UEC defaults were applied to it. Refuse the ambiguity rather than guess.
        env_eid = _env_endpoint_id()
        if env_eid and env_eid != entry.compute_mep_uuid:
            raise RuntimeError(
                f"{machine}: HPC_BRIDGE_ENDPOINT_ID={env_eid} conflicts with the entry's "
                f"compute_mep_uuid={entry.compute_mep_uuid}; unset it (the entry is the endpoint)"
            )
        account = (config.account() or "")
        if entry.account_required and not account:
            account = _require_env("HPC_BRIDGE_ACCOUNT")  # raises with the standard message
        return _pinned_facility(machine, entry, account=account)
    # Startup pin only: HPC_BRIDGE_SSH_HOST may override the catalog's canonical ssh_host (your own
    # alias / a login node, or the FQDN the container needs). The agentic connect path does NOT (#35).
    return _pinned_facility(machine, entry, account=_require_env("HPC_BRIDGE_ACCOUNT"), pinned_host=config.ssh_host())


def _pinned_facility(machine: str, entry, **kw) -> Facility:
    """The startup pin's facility. The alias it resolved is recorded while the facility is built (its partition preset
    follows the machine named) and kept only if the build succeeds: a pin that fails — a bad HPC_BRIDGE_SSH_HOST, say
    — names no machine for the presets."""
    _PINNED[machine] = entry.subject
    try:
        fac = _facility_from_entry(entry, **kw)
    except Exception:
        _PINNED.pop(machine, None)
        raise
    _PINNED_ORIGIN[machine] = _terms_origin(entry)
    return fac

def _default_local_facility(*, unbound: bool = False) -> LocalFacility:
    """The facility the server holds when no machine is pinned (or the pin failed at boot) — make_facility's and
    lifespan's one builder. It is `implicit` (nothing chose it: the tools that would stand up or use an endpoint
    refuse on it with "No facility connected") unless HPC_BRIDGE_ENDPOINT_ID opts into local dev: an endpoint the
    user runs themself, which hpc-bridge dispatches to (it never stands one up on this boot path). `unbound=True` —
    lifespan's fallback after a FAILED pin — is implicit even with HPC_BRIDGE_ENDPOINT_ID set: that UUID may be the
    very one the pin refused (a MEP entry's conflict), and a failed pin starts unbound. Before, the unpinned server
    stood up a local `hpc-bridge-dev` endpoint on the MCP host for a run_shell made before any connect_facility
    (Hermes on the fake cluster, live 2026-10-10)."""
    return LocalFacility(EndpointCLI(user_dir=config.user_dir()), implicit=unbound or not _env_endpoint_id())

async def make_facility() -> Facility:
    """Select the facility: a catalog-described machine (HPC_BRIDGE_MACHINE — sourced from the
    Globus Search index), or — none pinned — the no-facility default (_default_local_facility: unbound,
    or local dev when opted into). Machines are catalog *data*, never hardcoded; the agent binds one
    at runtime via connect_facility. (lifespan boots resiliently if this raises.)"""
    machine = (config.machine() or "")
    if not machine and (config.env("HPC_BRIDGE_FACILITY") or ""):
        raise RuntimeError(
            "HPC_BRIDGE_FACILITY was removed — machines are catalog data now. Use "
            "HPC_BRIDGE_MACHINE=<id> (e.g. anvil), or let the agent pick via connect_facility."
        )
    if machine:
        return await _catalog_facility(machine)
    return _default_local_facility()

def _resolve_scratch_root(facility) -> str:
    """The session-shell root for `facility`: explicit env wins, else the facility's shared-FS
    scratch (e.g. Anvil $SCRATCH), else a home-relative default.

    `~` is expanded CLIENT-side only for a LocalFacility, whose worker IS this machine. A remote
    facility's root is kept verbatim: expanding `~/.hpc-bridge` here would bake the CLIENT's home
    (e.g. /Users/me/.hpc-bridge) into a command that runs as a different user on a different host —
    the exact bug a bound facility's scratch is meant to avoid. A `~/`/`$HOME/` root is instead
    expanded on the WORKER by `Session.quoted_state_dir()`, which is also what lets a multi-user
    endpoint (whose local username we can't know client-side) use a `$HOME`-relative scratch."""
    root = config.scratch() or getattr(facility, "scratch_root", None) or "~/.hpc-bridge"
    return os.path.expanduser(root) if isinstance(facility, LocalFacility) else root

def _make_search_client(_app_factory=None):
    """Build the Globus SearchClient for the facility registry.

    The registry's entries are `visible_to: public`, and Globus Search requires authentication
    ONLY for non-public entries (docs, verified 2026-09-03) — so the default is an **anonymous**
    client: a fresh install can `list_facilities` with zero setup and no consent. If the user's
    Compute identity already holds the Search scope (a curator who ran `hpc-bridge-catalog`, or a
    later registry with `visible_to`-restricted entries), we use it — the authenticated client also
    sees restricted entries. We never trigger a login here (a server runs non-interactively); a
    restricted registry that needs one is the job of the `needs_login` flow, lazily. Isolated so
    tests can substitute it.
    """
    from globus_sdk import SearchClient

    try:
        from globus_compute_sdk import Client

        # do_version_check=False: the check is an AUTHENTICATED call — on a fresh install it would
        # trigger the SDK's command-line login on the MCP transport (see the gate in _connect_facility).
        app = (_app_factory or (lambda: Client(do_version_check=False).app))()
        client = SearchClient(app=app)  # registers the search scope requirement on this app instance
        if not app.login_required():  # non-prompting: the scope is already granted -> authenticated
            return client
    except Exception:  # noqa: BLE001 - no Compute login at all, unreadable storage, SDK trouble -> anonymous
        pass
    return SearchClient()  # anonymous: public entries only (the registry's default)

def make_catalog():
    """The runtime catalog is the PUBLIC REGISTRY — a Globus Search index, read anonymously. The
    plugin ships its id (`PUBLIC_REGISTRY_INDEX`), so `list_facilities()` works out of the box with no
    login and no configuration; HPC_BRIDGE_SEARCH_INDEX overrides it (a private/staging registry).
    There is **no bundled fallback**: a machine the registry can't resolve is a hard failure (the
    soft agent-discovery fallback is a later slice). The bundled seed is the curator's ingest source
    (see `hpc-bridge-catalog`), never a runtime catalog. The one exception is the explicit dev/test
    seam HPC_BRIDGE_CATALOG_FILE (a seed-format YAML the caller supplies): the agentic fake cluster's
    facility MEPs are minted per cluster and cannot be registry entries; a curator can rehearse one.
    """
    from .catalog.search import SearchCatalog

    local = config.catalog_file()
    if local:  # dev/test seam (config.catalog_file): a seed-format YAML stands in for the registry
        from .catalog.bundled import BundledCatalog

        return BundledCatalog(local)
    index = config.search_index()
    client = _make_search_client()  # anonymous unless a Search-scoped login is already held
    cache_dir = (
        config.plugin_data_dir()
        / "catalog-cache"
    )
    return SearchCatalog(index_id=index, client=client, cache_dir=cache_dir)

def _session_endpoint_name(ssh_host: str) -> str:
    """A stable endpoint name for a session (BYO) facility, keyed on the **SSH host** — the canonical
    per-cluster identity — so it never SHARES a registration with another facility AND doesn't sprawl
    when the agent picks different facility ids for the same host (`midway` vs `midway3` both →
    `hpc-bridge-midway3`). Endpoints are keyed by (identity, name); a bare 'hpc-bridge' would collide
    with the curated Anvil endpoint and any stale 'online' registration, which find_online_endpoint
    would then wrongly reuse — leaving a canary that can never warm."""
    slug = re.sub(r"[^a-z0-9]+", "-", (ssh_host or "session").lower()).strip("-") or "session"
    return f"hpc-bridge-{slug}"

def _facility_store():
    """The persistent local-discovery cache of confirmed BYO facility configs (keyed by ssh_host).
    A thin indirection so tests can point it at a tmp path."""

    return FacilityStore()

class TermsDirectiveError(ValueError):
    """details= whose scheduler_options set the account or the partition/queue (refused: `_entry_from_details`)."""


def _entry_from_details(facility: str, details: FacilityDetails) -> CatalogEntry:
    """Build a SESSION-LOCAL CatalogEntry from user-supplied details — the Socratic fallback for a
    machine not in the catalog. provenance="session"; never written to the shared index. Identity is
    defaulted from the id; the transfer endpoint is omitted (compute-only); the allocation block is
    set only when a listing command + a parser were given (else the human supplies the account)."""
    if details.env_setup is None:
        raise ValueError("env_setup is missing and there is no proposal to take it from: call connect_facility with "
                         "ssh_host=… (no details) for a proposed config, or give env_setup")
    if line := scheduler_terms_directive(details.scheduler_options):
        # sent verbatim, it would charge an account or pick a queue that the block's terms (and the user) never see
        raise TermsDirectiveError(
            f"scheduler_options sets the account or the partition/queue ({line!r}; #SBATCH -A/--account/-p/"
            "--partition, #PBS -A/-q): take it out — give the partition as details' `partition` and the account to "
            "ensure_endpoint_up(account=…), so the spend question names what the block charges")
    alloc = None
    if details.allocation_command and details.allocation_parser:
        alloc = Allocation(command=details.allocation_command, parser=details.allocation_parser)
    # HPC_BRIDGE_ENDPOINT_NAME: opt-in override giving each agentic-harness RUN a DISTINCT endpoint
    # name — the shared ssh-host name + a shared test identity would otherwise collide one registration
    # across concurrent runs. Wins over an agent-supplied name too, so a flailing agent can't defeat run
    # isolation. Real users leave it unset and get the ssh-host key (_session_endpoint_name).
    ep_name = ((config.endpoint_name() or "")
               or details.endpoint_name or _session_endpoint_name(details.ssh_host or facility))
    return CatalogEntry(
        id=facility,
        facility_key=SESSION_KEY,
        facility=details.display_name or facility,
        description="session-local facility (user-supplied, not catalogued)",
        # The endpoint's UI title (manager config display_name) follows the same convention as its
        # registration name — `hpc-bridge-<ssh_host>` (ssh-host-keyed) — so the two never diverge.
        display_name=details.display_name or ep_name,
        transfer_endpoint_uuid=None,
        ssh_host=details.ssh_host,
        allocation=alloc,
        # Nothing curated says whether this scheduler enforces accounting, so the account floor stays off: the user
        # names an account when the facility needs one. The schema's default (True) is the registry's claim to make.
        account_required=False,
        compute=Compute(
            scheduler=details.scheduler,
            interface=details.interface,
            env_setup=details.env_setup,
            scratch_root=details.scratch_root,
            endpoint_name=ep_name,
            amqp_port=details.amqp_port,
            scheduler_options=details.scheduler_options,
        ),
        defaults=Defaults(
            partition=details.partition,
            walltime=details.walltime,
            cpus_per_node=details.cpus_per_node,
        ),
        provenance="session",
        last_validated=datetime.date.today(),
    )
