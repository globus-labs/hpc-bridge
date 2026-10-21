from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, Field, PrivateAttr, field_validator

NodeHours = float


@dataclass(frozen=True)
class Submission:
    """A scheduler job the agent's own command starts by hand (scheduler_ops._submission) — billed to the allocation,
    never seen by the spend floor."""

    command: str     # the first one: sbatch / qsub / salloc, or srun outside a block
    calls: int       # how many submit calls the command line makes
    own_exit: bool   # the line's exit code is that one submission's own answer (last, only, direct, not --wait)


# Where the next block's queue or account comes from (BlockTerms). A DEFAULT is the facility's configured one:
#   registry           the facility's registry entry (`defaults.partition`; a local catalog file stands in for it)
#   env                a preset: HPC_BRIDGE_PARTITION / HPC_BRIDGE_ACCOUNT, applied only to the facility
#                      HPC_BRIDGE_MACHINE names (an account a facility is configured with is only ever this preset)
#   byo_details        a facility you bring yourself: the partition its connect_facility(details=…) gave, other than
#                      the probe's proposal, or the config cached from an earlier session
#   discovered         a facility you bring yourself: the partition the login-node probe proposed, which details= kept
#   facility_template  hpc-bridge sends none: the facility's own template decides (queue is then None)
# A CHOICE is one ensure_endpoint_up(partition=… / account=…) made this binding (a stop keeps it, a new connect or a
# teardown forgets it):
#   this_call          this very call passed it (a change, or the same value again)
#   earlier_call       an earlier call passed it (also after a stop: the next block is seeded with it)
# For an account with none set:
#   none_needed        the registry entry says the facility charges no account (account_required: false)
#   none_set           none is set, and the facility requires one (the account floor) or nothing curated says
#                      (a facility you bring yourself: the scheduler's default account applies, if it has one)
DefaultSource = Literal["registry", "env", "byo_details", "discovered", "facility_template"]
QueueSource = Literal["registry", "env", "byo_details", "discovered", "facility_template", "this_call", "earlier_call"]
AccountSource = Literal["this_call", "earlier_call", "env", "none_set", "none_needed"]


@dataclass(frozen=True)
class TermsOrigin:
    """Where the bound facility's DEFAULT terms came from, recorded when it is bound (connect_facility, or the startup
    pin): read by notices._block_terms for every block of that binding."""

    partition: DefaultSource = "registry"   # the default partition's source (facility_template is decided per block)
    account_needed: bool | None = None      # the registry entry's account_required; None: a facility you bring yourself
    account_hint: str | None = None         # the registry entry's: where a user finds the account to charge


class BlockTerms(BaseModel):
    """The billed block a spend question is about — or, on the call that confirmed it, the block it started (a receipt).
    Set on needs_confirmation and needs_account (ensure_endpoint_up and run_shell), on the ensure_endpoint_up call that
    newly confirmed spend (its up/provisioning), and on connect_facility (the next block's defaults, before any spend).
    Only what the facility receives: a value a facility endpoint's schema drops at the wire reads as its own default."""

    facility: str | None = None
    queue: str | None = None                # the partition (Slurm) or queue (PBS); None: the facility's own default
    queue_source: QueueSource
    account: str | None = None              # None: no account is sent
    account_source: AccountSource
    account_required: bool = False          # the account floor: the facility starts no block without one
    account_hint: str | None = None         # the registry entry's: where a user finds the account to charge
    nodes: int | None = 1                   # None: not sent — the facility's own default size
    walltime: str | None = None             # HH:MM:SS (Slurm's D-HH:MM:SS too); None: the facility's own default
    max_node_hours: float | None = None     # nodes × walltime in hours: what this one block can bill at most
    compute_only: bool = False              # a facility endpoint: no free login shape, every command bills


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
    # needs_confirmation / needs_account: the block the spend question is about (its queue, account, size), so the
    # question to the user names it. None for every other phase.
    block: BlockTerms | None = None
    notice: str | None = None
    # Internal, never serialized: a `failed` outcome the worker itself answered (a result over Compute's size limit:
    # it ran the command) — a liveness proof, so warmth must not void the block's confirmation for it.
    _worker_answered: bool = PrivateAttr(default=False)


class EndpointStatus(BaseModel):
    # needs_confirmation: a scheduler compute block was requested without an explicit spend
    # acknowledgement — nothing was provisioned; re-call with confirm_spend=True to proceed.
    # draining: stop_endpoint did NOT confirm that spend stopped. Two cases, which the notice and
    # `spend_stopped` tell apart:
    #   - an endpoint hpc-bridge stood up over SSH: the block cancel was sent but not confirmed (the
    #     login release channel was cold or busy, the pilot had not landed yet, or a dispatch reached
    #     the block during the release), or it could NOT be sent (the release channel was busy with
    #     another scheduler command of ours). Idle-release is the backstop, and calling stop_endpoint
    #     again confirms (`spend_stopped` None: unknown).
    #   - a facility multi-user endpoint: NO cancel was issued — hpc-bridge has no cancel channel
    #     there. It stopped submitting; the block bills until the facility's idle-release or its
    #     walltime. FINAL: calling again never yields "down". The notice names the block's scheduler
    #     job (`block_job_id`), the latest it can bill until, and the user's own cancel command
    #     (`spend_stopped` False while a block is presumed up or a check of ours is queued).
    # Never claim "down" while spend may continue — an agent that reads "down" walks away while the
    # block may still burn (issues #24, #178).
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
    # The scheduler job id of the billed block this result is about, when hpc-bridge knows it: from the block's check
    # once it is up; while it provisions on an SSH endpoint, the newest pilot job the scheduler lists for the endpoint
    # (the one the notice's pilot verdict names — queued, HELD or starting); on a stop or a detach of a facility
    # endpoint, the block left running (the notice names the command that ends it). None: unknown, or no block.
    block_job_id: str | None = None
    # stop_endpoint / teardown_endpoint: is the spend stopped? True ONLY when an SSH stop's cancel was confirmed
    # (`down`). False: a block may still bill — a facility endpoint left one running or a check of ours queued there
    # (never True on a facility endpoint: nothing here can cancel its block), or the stop was refused. None: not known
    # either way (an unconfirmed SSH cancel, an offline manager, a facility block presumed released by now, no block
    # known), and on every other result.
    spend_stopped: bool | None = None
    # needs_confirmation / needs_account: the block the spend question is about. On the call that newly confirmed spend
    # (up / provisioning): the block it confirmed (a receipt — a one-call start still shows what it started); a poll
    # that passes confirm_spend=True again on a block already confirmed carries none. None for the login shape (free).
    block: BlockTerms | None = None
    notice: str | None = None


class LoginShellResult(BaseModel):
    """Result of a read-only login-node command (facility discovery) — a separate channel
    from the compute-block `ShellOutcome`: no block, no allocation, no session spend (unless the
    command itself submits a scheduler job, which bills the allocation; the notice says so)."""

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
    env_setup: str | None = Field(
        default=None,
        description="Bash that puts `globus-compute-endpoint` on PATH on the login node — usually a "
        "`module load …` and/or `source <venv>/bin/activate`. '{user}'/'{venv}' are templated. OMIT it after a "
        "proposal (phase='proposed_facility_details'): the server keeps the proposed env_setup; give it only when "
        "the user wants to change it.",
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
        "Omit if the facility has none — the account then comes from the user (ensure_endpoint_up(account=…)).",
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
    select_options: str | None = Field(
        default=None,
        description="PBS only: resources for each chunk of the block's select — hpc-bridge writes "
        "'select=<the block's node count>:ncpus=<cpus_per_node>' and appends ':<select_options>', so the node count "
        "and the spend terms stay hpc-bridge's. name=value pairs joined by ':', e.g. 'ngpus=4' or "
        "'system=polaris:ngpus=4'; never a chunk count, a second chunk ('+'), select=, ncpus (that is "
        "cpus_per_node), the walltime, the node count, the account or the queue. Omit if none. A '-l select=…' line "
        "in scheduler_options is refused: its resources go here (set cpus_per_node too: without it the chunk asks for "
        "ncpus=1).",
        examples=["ngpus=4", "system=polaris"],
    )
    scheduler_options: str | None = Field(
        default=None,
        description="Scheduler directives for the submit script's header (Parsl scheduler_options) — e.g. a PBS site "
        "that holds every job without a filesystems directive needs '#PBS -l filesystems=home:eagle'. Omit if none. "
        "Multiple directives: newline-separate them. Each line must be a directive of this scheduler at the line's "
        "start ('#PBS …' on PBS, '#SBATCH …' on Slurm) or a comment ('# …'); a bare option ('-l filesystems=…', as a "
        "scheduler's message quotes it) is written with the scheduler's prefix and the connect says so; any other "
        "line is refused (a shell command belongs in env_setup; another batch system's directive word, or one with "
        "no space after it, is not read as you mean). Never the account, partition/queue, walltime or node count "
        "(#SBATCH -A/-p/-t/-N, #PBS -A/-q/-l walltime=/select=, bare or prefixed): those are "
        "ensure_endpoint_up(account=…), `partition` and `walltime`, so the spend question names them — a PBS "
        "chunk's resources (ngpus=4) go in `select_options`, the line's other resources (filesystems=…) on a line of "
        "their own; on Slurm a GPU is '#SBATCH --gres=gpu:4'. Never the pilot's output file (#SBATCH -o: hpc-bridge "
        "finds and cancels its pilots by parsl's own) or error file (#SBATCH -e: where it reads why a pilot died), or "
        "an array (#SBATCH -a, #PBS -J).",
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
    # needs_account: the facility is bound (an SSH facility's login node is up); `block` holds the first block's terms
    # and `allocations` the choices where the facility lists them -> ask the user, then ensure_endpoint_up(account=…).
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
    # Once the facility is bound (needs_account, provisioning): the terms its next billed block would start on — the
    # default queue and its source, the account (or whether one is needed, and where to find it), size and walltime.
    # Nothing is started by connect_facility.
    block: BlockTerms | None = None
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

