from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator

NodeHours = float


class ShellOutcome(BaseModel):
    # needs_confirmation: a scheduler compute shape whose spend hasn't been acknowledged — the
    # command was NOT dispatched and no block was started (run ensure_endpoint_up(confirm_spend=True)).
    # running: the command was dispatched and is STILL executing past the client sync-wait — it was
    # NOT cut; `task_id` is a handle to poll for its result via poll_task(task_id). The task runs up
    # to the block walltime, and the block stays warm because a running task keeps it busy.
    phase: Literal["complete", "cold_start", "failed", "needs_confirmation", "needs_account", "running"]
    exit_code: int | None = None
    stdout: str = ""
    stderr_snippet: str = ""
    block_state: Literal["warm", "cold", "provisioning"]
    session_spend: NodeHours = 0.0
    est_wait_s: int | None = None
    # phase="running": poll for this task's result with poll_task(task_id). None for every other phase.
    task_id: str | None = None
    notice: str | None = None


class EndpointStatus(BaseModel):
    # needs_confirmation: a scheduler compute block was requested without an explicit spend
    # acknowledgement — nothing was provisioned; re-call with confirm_spend=True to proceed.
    # draining: stop_endpoint dispatched the block cancel but could NOT confirm it (the login
    # release channel was cold) — spend is NOT verifiably stopped; idle-release is the backstop
    # and re-calling stop_endpoint (channel now warming) confirms. Never claim "down" here — an
    # agent that reads "down" walks away while the block may still burn (issue #24).
    # tearing_down: teardown_endpoint started the login-node ops (gce stop + delete over SSH) and they outlived
    # the call's wait — they keep running in the server; call teardown_endpoint again to confirm 'down'.
    # (Expanse, live 2026-09-04: stop + delete take ~3 min on its filesystem, past the client's tool window.)
    # needs_account: the facility requires an allocation account for every block and none is set — nothing was
    # provisioned; re-call with account=… (a LOGIN NAME is not an account) and confirm_spend=True to proceed.
    status: Literal["up", "provisioning", "down", "needs_confirmation", "needs_account", "draining", "tearing_down"]
    block_state: Literal["warm", "cold", "provisioning"]
    endpoint_id: str | None = None
    session_spend: NodeHours = 0.0
    # Scheduler queue/partition this shape will provision onto (from the discovery selection gate,
    # or the facility default). None for the login shape.
    partition: str | None = None
    # Allocation/account this scheduler shape charges to (from connect_facility's selection, or the
    # facility default). None for the login shape.
    account: str | None = None
    notice: str | None = None


class LoginShellResult(BaseModel):
    """Result of a read-only login-node command (facility discovery) — a separate channel
    from the compute-block `ShellOutcome`: no block, no allocation, no session spend."""

    exit_code: int
    stdout: str = ""
    stderr_snippet: str = ""
    notice: str | None = None


class AllocationOption(BaseModel):
    """One allocation the user could charge to — the agent-facing choice (from connect_facility)."""

    account: str  # the value that becomes SlurmProvider.account
    balance: float
    units: str = "SU"
    type: str | None = None  # e.g. CPU / GPU, when the facility distinguishes them


# A login host as ssh should see it: a hostname/alias/IPv4 literal. Rejects a leading "-" (which ssh
# would parse as an OPTION — `-oProxyCommand=…` from a registry entry or a prompt-injected details= is
# local command execution), whitespace and shell metacharacters (security review 2026-09-04, A3/C-2).
SAFE_HOST = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$")


def validate_host(v: str) -> str:
    v = (v or "").strip()
    if not SAFE_HOST.match(v):
        raise ValueError(f"invalid login host {v!r}: a hostname or ssh alias only (letters, digits, '.', '-', '_')")
    return v


class FacilityDetails(BaseModel):
    """User-supplied config for a facility that isn't in the catalog — the elicitation template for
    `connect_facility`'s `needs_facility_details` phase. The field descriptions ARE the questions to
    ask the user; the agent fills this in from their answers. SSH login user + key come from your
    ~/.ssh/config (or optional HPC_BRIDGE_SSH_USER/KEY overrides), not here — facility config, not creds.
    Session-local: never written to the shared catalog."""

    ssh_host: str = Field(
        description="Login host to SSH to for the one-time bootstrap.",
        examples=["anvil.rcac.purdue.edu", "frontier.olcf.ornl.gov"],
    )

    @field_validator("ssh_host")
    @classmethod
    def _safe_host(cls, v: str) -> str:
        return validate_host(v)
    interface: str = Field(
        description="High-speed network interface the compute workers bind to so they can phone "
        "home (address_by_interface). Wrong value ⇒ workers never register.",
        examples=["ib0", "hsn0"],
    )
    env_setup: str = Field(
        description="Bash that puts `globus-compute-endpoint` on PATH on the login node — usually a "
        "`module load …` and/or `source <venv>/bin/activate`. '{user}'/'{venv}' are templated.",
        examples=["module load anaconda && source {venv}/bin/activate"],
    )
    scratch_root: str = Field(
        description="A writable path on the shared filesystem for session state ('{user}' is "
        "templated to the SSH login name).",
        examples=["/anvil/scratch/{user}/.hpc-bridge", "/lustre/orion/scratch/{user}/.hpc-bridge"],
    )
    partition: str = Field(
        description="Default scheduler partition/queue for compute blocks.",
        examples=["shared", "batch", "debug"],
    )
    scheduler: Literal["slurm", "pbs"] = Field(
        default="slurm",
        description="Batch scheduler: 'slurm' or 'pbs' (PBS Pro). Picks the provider/launcher "
        "and the queue vs partition wording; LSF is not supported yet.",
    )
    allocation_command: str | None = Field(
        default=None,
        description="Optional: a login-node command that lists the user's allocations/balances. "
        "Omit if the facility has none — you'll then pass account= directly.",
        examples=["mybalance", "xdusage -p <project>"],
    )
    allocation_parser: Literal["sbank", "iris", "mybalance"] | None = Field(
        default=None,
        description="Which built-in parser reads `allocation_command`'s output. Only 'mybalance' is "
        "implemented today; omit (with allocation_command) if unsure.",
    )
    walltime: str = Field(default="00:30:00", description="Default block walltime (HH:MM:SS).")
    cpus_per_node: int | None = Field(
        default=None,
        description="PBS only: cores per node to request (PBSProProvider.cpus_per_node). "
        "Omit for Slurm (ignored) or to accept the scheduler default. E.g. Polaris: 32.",
    )
    scheduler_options: str | None = Field(
        default=None,
        description="Raw scheduler directives injected verbatim into the submit script (Parsl "
        "scheduler_options) — e.g. Polaris REQUIRES '#PBS -l filesystems=home:eagle' or the job "
        "is held. Omit if none. Multiple directives: newline-separate them.",
        examples=["#PBS -l filesystems=home:eagle", "#SBATCH --constraint=gpu"],
    )
    amqp_port: int = Field(default=443, description="AMQPS port (443 is near-universally allowed).")
    endpoint_name: str | None = Field(
        default=None,
        description="On-disk/registration endpoint name. Leave unset for a session facility — it "
        "defaults to hpc-bridge-<facility> so it never collides with another facility's endpoint "
        "(Globus Compute keys endpoints by identity + name).",
    )
    display_name: str | None = Field(
        default=None, description="Optional human label for the facility (defaults to the id)."
    )


class ConnectFacilityResult(BaseModel):
    # needs_account: login node is up; `allocations` are the choices -> ensure_endpoint_up(account=…).
    # provisioning: login node still warming (call again). needs_facility_details: not in the catalog
    # and no SSH host to probe -> supply `details` (or an ssh_host to discover them).
    # proposed_facility_details: probed the login node -> `proposed_details` is a draft to confirm
    # with the user, then call again with details=. needs_preauth: the host needs a one-time
    # interactive login (password/MFA) -> relay `preauth_command` for the USER to run in their own
    # terminal, then call again. unsupported/failed: see notice.
    phase: Literal[
        "needs_account",
        "provisioning",
        "needs_facility_details",
        "proposed_facility_details",
        "needs_preauth",
        "needs_login",
        "unsupported",
        "failed",
    ]
    facility: str  # the id/subject that was connected (echoes connect_facility's arg)
    # True when we attached to an already-online endpoint (find_online_endpoint / status=running)
    # instead of SSH-bootstrapping a fresh one — a zero-SSH reconnect (#20).
    reused: bool = False
    allocations: list[AllocationOption] = []
    # phase="needs_preauth": the exact `ssh -fN …` command the USER runs in their OWN terminal to
    # open a reusable master (entering the password/MFA there). The agent relays it — never runs or
    # fills it, and never handles the secret.
    preauth_command: str | None = None
    # phase="needs_preauth": True when the facility accepts a ONE-TIME CODE (TOTP / Duo passcode) at the prompt —
    # the agent may ask the user for the CURRENT code and call complete_preauth(code). Never a password.
    preauth_code_ok: bool = False
    # phase="needs_login": Globus credentials are missing/under-scoped. `login_url` is the Globus
    # authorize link for the USER to open; `login_mode` says how it completes — "browser": a loopback
    # listener in this process receives the code (nothing to paste; call connect_facility again),
    # "paste": Globus shows a one-time code the user hands to complete_login(code). The agent relays
    # the link and never asks for a password or handles a token.
    login_url: str | None = None
    login_mode: Literal["browser", "paste"] | None = None
    # A draft discovered by probing the login node (phase="proposed_facility_details"): review/correct
    # it with the user, then connect_facility(details=…). None for every other phase.
    proposed_details: FacilityDetails | None = None
    notice: str | None = None


class LoginStatus(BaseModel):
    """Result of authenticate() / complete_login(): where the Globus login stands."""

    phase: Literal["logged_in", "needs_login", "failed"]
    login_url: str | None = None
    login_mode: Literal["browser", "paste"] | None = None
    notice: str | None = None


class PreauthStatus(BaseModel):
    """Result of complete_preauth(code): whether the shared SSH connection is open."""

    phase: Literal["opened", "failed", "needs_terminal"]
    preauth_command: str | None = None  # needs_terminal: the user opens it themselves (a password was asked)
    notice: str | None = None

