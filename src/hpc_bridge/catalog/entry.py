# src/hpc_bridge/catalog/entry.py
from __future__ import annotations

import datetime
import re
import uuid
from typing import Any, Literal

from pydantic import BaseModel, field_validator, model_validator

from ..models import validate_host


class Allocation(BaseModel):
    """How to LIST a user's allocations on this machine — not the allocations themselves.

    `parser` names a deterministic, plugin-side parser (Plan 2). The command's stdout is
    parsed in code, never handed to the model — inference is exactly what the catalog removes.
    """

    command: str
    parser: Literal["sbank", "iris", "mybalance"]


SAFE_ENDPOINT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class Compute(BaseModel):
    """Machine-invariant facts the plugin PINS — the user/agent cannot override these.

    Getting one wrong breaks the endpoint silently (e.g. the wrong `interface` means workers
    never phone home), so this is the same "look up, never infer" category as the UUIDs.
    """

    scheduler: Literal["slurm", "pbs", "lsf"]
    interface: str  # address_by_interface ifname (e.g. ib0)
    env_setup: str  # bash that puts globus-compute-endpoint on PATH (module + venv)
    scratch_root: str  # session-shell root on the shared filesystem; {user} templated
    endpoint_name: str | None = None  # None ⇒ derive hpc-bridge-<id> (never the bare collision name)

    @field_validator("endpoint_name")
    @classmethod
    def _safe_endpoint_name(cls, v: str | None) -> str | None:
        # The name is spliced into a remote shell path (`cat > "$HOME/.globus_compute/<name>/…"`) where
        # `$(…)` and backticks EXECUTE — and the endpoint's own validator lets them through (found in
        # review). Same allowlist the harness/session naming uses.
        if v is not None and not SAFE_ENDPOINT_NAME.match(v):
            raise ValueError("endpoint_name must match [A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
        return v
    amqp_port: int = 443  # facilities firewall AMQPS 5671; 443 is the near-universal allowed port
    # Facility-MEP entries: what `{gce_version}` in env_setup resolves to — the version the facility's USER endpoint
    # process runs, which its template decides and which the metadata does not expose. "manager": the manager's
    # `endpoint_version` from metadata (Delta: the UEP runs the manager's code). "client": this client's SDK
    # version (Anvil: the template builds the UEP's environment at the client's SDK version — pinning the
    # manager's 4.12 there launched a 4.12 worker pool under a 4.16 UEP: `unrecognized arguments: --logconf`).
    # Or an explicit version string. Set from the live check that validates the entry.
    worker_version: str = "manager"
    scheduler_options: str | None = None  # raw scheduler directives, verbatim (e.g. #SBATCH for Slurm, #PBS for PBS)


class Defaults(BaseModel):
    """Per-run tunables the agent/user MAY override at submit time via user_endpoint_config."""

    partition: str
    walltime: str = "00:30:00"
    max_workers_per_node: int = 2
    nodes_per_block: int = 1
    cpus_per_node: int | None = None  # PBSProProvider.cpus_per_node; Slurm ignores it
    init_blocks: int = 0  # blocks to pre-spawn; 0 = lazy (block on first task). A MEP entry sets 1 for a warm, low-latency block (its login-shape replacement)  # noqa: E501
    max_blocks: int = 1
    available_accelerators: int | list[str] | None = None
    # Facility-MEP entries only: extra `user_endpoint_config` keys the facility's own template consumes
    # (Anvil/Delta: qos, cores_per_node, exclusive, mem_per_node, scheduler_options, resource_specification…).
    # Passed through verbatim, then filtered against the facility's published schema at attach time.
    extra: dict[str, str | int | bool] | None = None


class CatalogSummary(BaseModel):
    """The agent-safe view of an entry — identity only, no executable config or raw UUIDs."""

    subject: str
    id: str
    facility: str
    description: str
    display_name: str
    provenance: Literal["curated", "community", "scraped", "plugin-validated", "session"]
    last_validated: datetime.date
    # How a user gets in — the one thing a stranger must know before choosing (stranger's walk, 2026-09-03).
    # Derived, never stored: mep = the facility runs the endpoint (zero SSH, but their identity mapping
    # must include you); ssh = you need an account + key-based SSH on the login host.
    access: Literal["mep", "ssh"]
    access_note: str
    scheduler: str


_SAFE_SLUG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")  # the derived endpoint name is hpc-bridge-<id>


class CatalogEntry(BaseModel):
    """One machine. A superset of MachineProfile; `profile_kwargs()` is the binding seam."""

    # identity
    id: str
    facility_key: str  # short slug for the subject, e.g. "purdue" (distinct from display `facility`)
    facility: str  # display, e.g. "Purdue / ACCESS"
    description: str
    display_name: str

    # identifiers (look up, never infer)
    compute_mep_uuid: str | None = None
    transfer_endpoint_uuid: str | None = None  # Globus Transfer (not wired yet) — compute-only entries omit it

    # access
    ssh_host: str | None = None  # None ⇒ a MEP entry (dispatch-only, no SSH); an SSH-bootstrap entry MUST set it
    auth_method: Literal["ssh-key", "mfa-otp", "sfapi"] = "ssh-key"  # only ssh-key wired in v1

    allocation: Allocation | None = None  # a facility may have no auto-listable allocation tool
    # Whether a scheduler block needs an --account to charge. False for an unmetered machine whose
    # scheduler doesn't enforce accounting (e.g. the lab cluster) — then the agent must NOT be sent
    # hunting for an allocation; it may confirm spend and proceed without one.
    account_required: bool = True
    compute: Compute
    defaults: Defaults

    # trust / provenance
    provenance: Literal["curated", "community", "scraped", "plugin-validated", "session"] = "curated"
    last_validated: datetime.date

    @field_validator("ssh_host")
    @classmethod
    def _safe_host(cls, v: str | None) -> str | None:
        return None if v is None else validate_host(v)  # a registry entry's host reaches ssh's argv (review 2026-09-04)

    @field_validator("id", "facility_key")
    @classmethod
    def _safe_slug(cls, v: str) -> str:
        if not _SAFE_SLUG.match(v or ""):
            raise ValueError(f"invalid id/facility_key {v!r}: letters, digits, '.', '_', '-' only "
                             "(it names the endpoint)")
        return v

    @field_validator("compute_mep_uuid", "transfer_endpoint_uuid")
    @classmethod
    def _valid_uuid(cls, v: str | None) -> str | None:
        if v is None:
            return v
        return str(uuid.UUID(v))  # normalize to canonical lowercase hyphenated form

    @model_validator(mode="after")
    def _reachable(self) -> CatalogEntry:
        """An entry must be reachable: a facility MEP to dispatch to, or an SSH host to bootstrap.

        `ssh_host` is optional now (MEP entries have none), so this guards the failure mode that
        optionality opens — a non-MEP entry that silently omits `ssh_host` and only fails, opaquely,
        at connect time. A future facility could carry both (MEP + SSH fallback); we require *at
        least* one, not exactly one.
        """
        if self.compute_mep_uuid is None and self.ssh_host is None:
            raise ValueError(
                "a catalog entry needs a reach: set compute_mep_uuid (facility MEP) "
                "or ssh_host (SSH bootstrap)"
            )
        if self.compute_mep_uuid is not None:
            # Any entry with a MEP is CONSUMED as a MEP (MEP wins over ssh_host in _facility_from_entry),
            # so this guard must not be gated on ssh_host being absent (found in review: a dual-reach
            # entry shipped literal templates to the worker).
            # A MEP entry is consumed with NO SSH, so there is no login name to resolve `{user}`
            # and no client-managed venv for `{venv}` — the facility maps our identity to a local
            # account we never learn. Those tokens would reach the worker LITERALLY. Such an entry
            # must use worker-side forms ($HOME, $USER) that the mapped user's shell expands.
            for field in ("env_setup", "scratch_root"):
                val = getattr(self.compute, field)
                if "{user}" in val or "{venv}" in val:
                    raise ValueError(
                        f"compute.{field} uses client-side templating ({{user}}/{{venv}}) but this is a "
                        "MEP-only entry (no ssh_host): nothing can resolve it — use worker-side forms "
                        "($HOME for scratch_root; $HOME/$USER in env_setup)"
                    )
            # scratch_root is spliced into the session-shell wrapper by `Session.quoted_state_dir`,
            # which expands ONLY a leading $HOME/ (or ${HOME}/, ~/); any other `$VAR` — `$USER`
            # included — is shell-quoted LITERAL, so `/scratch/$USER/…` would create a directory
            # literally named `$USER`. Catch it at validation, not at the first run_shell.
            sr = self.compute.scratch_root
            home_rel = any(sr.startswith(p) for p in ("$HOME/", "${HOME}/", "~/"))
            rest = sr.split("/", 1)[1] if home_rel and "/" in sr else sr
            if not ((home_rel and "$" not in rest) or (sr.startswith("/") and "$" not in sr)):
                raise ValueError(
                    f"compute.scratch_root {sr!r} on a MEP-only entry must be $HOME/… (or an absolute "
                    "path with no $VAR): only a leading $HOME expands on the worker — $USER or any "
                    "other variable would be quoted literal by the session shell"
                )
        return self

    @property
    def subject(self) -> str:
        return f"{self.facility_key}:{self.id}"

    def summary(self) -> CatalogSummary:
        access: Literal["mep", "ssh"]
        if self.compute_mep_uuid:
            access, note = "mep", (
                "zero SSH — the facility runs the endpoint; you need an account there with your Globus "
                "identity mapped to it (no account ⇒ a terminal NO ACCOUNT on first use, nothing billed). "
                "connect_facility only attaches: hpc-bridge has no login shape through this channel (the facility's "
                "login nodes are outside it), so no allocation list — the first billed block is where your access is "
                "actually tested"
            )
        else:
            access, note = "ssh", (
                f"SSH bootstrap on {self.ssh_host}: you need an account and key-based SSH there (login name and "
                "key from ~/.ssh/config, or HPC_BRIDGE_SSH_USER / HPC_BRIDGE_SSH_KEY); hpc-bridge stands up a "
                "personal endpoint once, then reuses it with no further SSH"
            )
        return CatalogSummary(
            subject=self.subject,
            id=self.id,
            facility=self.facility,
            description=self.description,
            display_name=self.display_name,
            provenance=self.provenance,
            last_validated=self.last_validated,
            access=access,
            access_note=note,
            scheduler=self.compute.scheduler,
        )

    def profile_kwargs(self) -> dict[str, Any]:
        """Constructor kwargs for MachineProfile (Plan 2 builds the profile from these).

        `account` is intentionally absent — it is per-user, from allocation selection.
        `worker_init` is absent — in the code it is derived as `= env_setup`.
        `ssh_host` is also absent — consumed by the transport layer (SshTarget) / facility
        selection, not MachineProfile. `init_blocks` is likewise absent — the SSH path derives
        eager start from the run mode (`@@EAGER@@`), and the MEP UEC builder reads
        `defaults.init_blocks` directly. `scheduler` IS passed through to the profile (the
        template dispatches on it). `auth_method` is reserved (only ssh-key is wired; nothing
        reads it yet).
        """
        return {
            "name": self.id,
            "endpoint_name": self.compute.endpoint_name,
            "display_name": self.display_name,
            "env_setup": self.compute.env_setup,
            "interface": self.compute.interface,
            "partition": self.defaults.partition,
            "walltime": self.defaults.walltime,
            "max_workers_per_node": self.defaults.max_workers_per_node,
            "nodes_per_block": self.defaults.nodes_per_block,
            "max_blocks": self.defaults.max_blocks,
            "available_accelerators": self.defaults.available_accelerators,
            "amqp_port": self.compute.amqp_port,
            "scheduler_options": self.compute.scheduler_options,
            "scratch_root": self.compute.scratch_root,
            "scheduler": self.compute.scheduler,
            "cpus_per_node": self.defaults.cpus_per_node,
        }
