"""Agent-facing wording: every pure notice/outcome builder (split step 3, 2026-09-03).

A notice is the product — the agent reads it and acts — so the words live in one module: the
first-contact explanations (NO SSH ACCESS / CANNOT REACH / REMOTE FILESYSTEM), the spend floor, the
warm-block bounds, the login instructions, the terminal refusals (NO ACCOUNT, NO LONGER transient),
and the ShellOutcome/ConnectFacilityResult/LoginStatus factories. No I/O, no state mutation: these
functions read the context and return text or a result model. `server` re-exports them.
"""
from __future__ import annotations

import asyncio
import re

from . import config
from .config import SYNC_WAIT_S, _task_ceiling_s
from .context import AppCtx, ShapeRuntime, _has_login_shape, _idle_release_s
from .cost import _with_spend
from .lifecycle import BlockState
from .login import LoginStart
from .models import ConnectFacilityResult, ShellOutcome
from .runner import CanaryResult

# sshd's own denial lines — the method list in parentheses is the tell (a bare "Permission denied" is
# usually the remote filesystem: `mkdir … : Permission denied` on an over-quota home, found in review).
_SSH_AUTH_DENIED = re.compile(
    r"permission denied \((?:publickey|password|keyboard-interactive|gssapi|hostbased)[^)]*\)"
    r"|permission denied, please try again|authentication failed",
    re.IGNORECASE,
)

# OpenSSH's host-key refusals (StrictHostKeyChecking default + BatchMode): the fix is the USER's own ssh.
_HOST_KEY_UNKNOWN = re.compile(
    r"host key verification failed|no \S+ host key is known|remote host identification has changed"
    r"|host key for \S+ has changed",
    re.IGNORECASE,
)


def _first_contact_note(fac) -> str:
    """What a fresh SSH bootstrap is about to trust, in the transcript: the login `user@host` and the shell
    line (`env_setup`) that will run there. A registry entry or a `details=` used to be executed unseen
    (security review 2026-09-04, A2/C-1)."""
    cli = getattr(fac, "cli", None)
    target = getattr(cli, "target", None)
    if target is None:
        return ""
    who = f"{target.user}@{target.host}" if getattr(target, "user", None) else str(target.host)
    env_setup = getattr(cli, "env_setup", None) or "(none)"
    pinned = " (host pinned by HPC_BRIDGE_SSH_HOST)" if (config.ssh_host() or "") else ""
    return f"first contact over SSH: {who}{pinned}; env_setup run there: {env_setup!r}. "


def _explain_provision_error(exc: BaseException, fac=None, *, host: str | None = None,
                             user: str | None = None, fallback: str | None = None) -> str:
    """Turn a bootstrap failure into what a newcomer can act on. The raw text names an internal step
    ('seed storage.db (mkdir) failed: u@host: Permission denied (publickey,…)') — a stranger with no
    account or key on the facility must instead hear WHICH host and login name were tried, where the
    name came from, and the two remedies (found on the stranger's walk, 2026-09-03)."""
    raw = str(exc)
    low = raw.lower()
    ssh_line = raw.rsplit("failed: ", 1)[-1].strip() if "failed: " in raw else raw
    # ssh prefixes the verdict with warnings ("Identity file … not accessible") — quote the verdict line
    verdict = [ln for ln in ssh_line.splitlines()
               if "permission denied" in ln.lower() or "denied" in ln.lower() or "host key" in ln.lower()]
    if verdict:
        ssh_line = verdict[-1].strip()
    cli = getattr(fac, "cli", None)
    target = getattr(cli, "target", None) or getattr(cli, "_target", None)
    host = host or getattr(target, "host", None) or getattr(fac, "alias", None) or "the login host"
    if user is None:
        user = getattr(target, "user", None)
    if _SSH_AUTH_DENIED.search(raw) or "too many authentication failures" in low:
        who = f"as {user!r}" if user else "as your local username (no login name is configured for this host)"
        src = ("HPC_BRIDGE_SSH_USER" if (config.ssh_user() or "")
               else "~/.ssh/config" if user else "nowhere")
        return (
            f"NO SSH ACCESS to {host}: the login-node SSH {who} was refused ({ssh_line[:200]}). hpc-bridge "
            "needs an account on this facility and key-based SSH to its login node. Put the host's User and "
            "IdentityFile in ~/.ssh/config (or set HPC_BRIDGE_SSH_USER / HPC_BRIDGE_SSH_KEY) and call "
            "connect_facility again; on a multi-factor facility, pre-open a session in your own terminal "
            f"first. The login name came from {src}. Nothing was started or billed."
        )
    if _HOST_KEY_UNKNOWN.search(raw):
        who = f"{user}@{host}" if user else host
        changed = "identification has changed" in low or ("host key for" in low and "changed" in low)
        why = ("its host key has CHANGED since you last connected — verify the new fingerprint with the facility "
               "before trusting it" if changed else
               "its host key is not in your ~/.ssh/known_hosts yet")
        line = ssh_line[:200].rstrip(".")  # OpenSSH's own line ends with a full stop already
        return (f"UNKNOWN HOST KEY for {host}: {line}. hpc-bridge connects only to hosts your own ssh "
                f"already trusts, and {why}. Verify the fingerprint and connect once from your own terminal "
                f"(`ssh {who}`), which saves the key; then call connect_facility again. Nothing was started or billed.")
    if "permission denied" in low:  # a denial from the remote FILESYSTEM (quota, read-only home), not from sshd
        return (f"REMOTE FILESYSTEM refused a write on {host}: {ssh_line[:200]}. The login worked; the home "
                "directory is over quota, read-only, or not writable. Free space or fix permissions there, then "
                "call connect_facility again. Nothing was started or billed.")
    if any(k in low for k in ("could not resolve hostname", "connection timed out", "connection refused",
                              "no route to host", "network is unreachable", "operation timed out",  # macOS strerror
                              "host is down", "network is down", "connection reset by peer",
                              "kex_exchange_identification", "ssh: connect to host")):
        return (f"CANNOT REACH {host}: {ssh_line[:200]}. Check the login host name and your network/VPN, "
                "then call connect_facility again. Nothing was started or billed.")
    if isinstance(exc, TimeoutError | asyncio.TimeoutError) or type(exc).__name__ == "TimeoutError":
        return (f"TIMED OUT waiting for the login node {host}: a bootstrap step did not finish in time. On a first "
                "connect that is usually the toolchain install (uv and globus-compute-endpoint into your home there), "
                "which keeps running on the login node after the timeout and is idempotent — call connect_facility "
                "again in a minute or two. Nothing was billed.")
    if "controlpath too long" in low:
        return (f"hpc-bridge error: the SSH ControlMaster socket path is too long ({ssh_line[:160]}); set "
                "HPC_BRIDGE_STATE_DIR to a short path (e.g. ~/.hpc-bridge). Nothing was started.")
    return (fallback or f"hpc-bridge error: {type(exc).__name__}: {raw}")[:500]

def _local_dill() -> str | None:
    try:
        import dill

        return dill.__version__
    except Exception:  # noqa: BLE001 - dill absent locally just means we can't compare
        return None

def _worker_notice(canary: CanaryResult | None) -> str | None:
    """A short warm descriptor for the agent: where the worker landed, its Python/Dill, and a
    serialization-skew warning when worker Dill differs from ours (the real failure mode)."""
    if canary is None:
        return None
    head = f"worker live on {canary.worker_host}" if canary.worker_host else "worker live"
    vers = [v for v in (
        f"py{canary.worker_python}" if canary.worker_python else None,
        f"dill{canary.worker_dill}" if canary.worker_dill else None,
    ) if v]
    note = head + (f" ({', '.join(vers)})" if vers else "")
    local = _local_dill()
    if canary.worker_dill and local and canary.worker_dill != local:
        note += f"; ⚠ dill skew: worker {canary.worker_dill} vs local {local} (serialization may fail)"
    return note

def _billed_bounds_note(app: AppCtx, rt: ShapeRuntime) -> str:
    """The bounds of a billed compute block ([#21]), surfaced so a caller runs long work AS A TASK
    rather than being surprised: a run_shell task runs up to the block walltime (then the worker kills
    it, exit 124) and, if it outlives the sync-wait, comes back as a poll handle (poll_task) — it is
    NOT cut at ~110s any more. The block idle-releases after `max_idletime` once nothing is running or
    queued, so keep long work in the FOREGROUND (a running task holds the block); a detached process
    is not a Compute task and would be idle-released out from under itself."""
    idle = _idle_release_s(app)
    ceiling = int(_task_ceiling_s(rt.user_endpoint_config))
    return (f"billed block bounds — a task runs up to ~{ceiling}s (the block walltime); one that "
            f"outlives the ~{int(SYNC_WAIT_S)}s sync-wait returns a poll handle (poll_task), it is NOT "
            f"cut. The block idle-releases after ~{idle}s once nothing runs or is queued, so run long "
            "work as a foreground task — don't detach it (a detached process isn't a Compute task).")

def _needs_confirmation_notice(app: AppCtx, where: str) -> str:
    """The spend-floor notice. Names the free login shape as the alternative ONLY where one exists —
    on a compute-only facility every shape is billed, so pointing at shape='login' is a dead-end."""
    head = (f"scheduler compute block{where} ({app.profile.nodes_per_block} node(s)): spend "
            "not yet confirmed. ")
    return head + _spend_floor_guidance(app)

def _spend_floor_guidance(app: AppCtx | None) -> str:
    """What to do about an unconfirmed spend — ONE text for ensure_endpoint_up and run_shell/reset
    (they used to drift). Names the free login shape only where one exists: on a compute-only
    facility every shape is billed, so pointing at shape='login' is a dead-end."""
    if app is not None and not _has_login_shape(app):
        return ("This facility is compute-only through this channel: hpc-bridge reaches it only via its compute "
                "endpoint, so there is no free login shape here (the facility's login nodes are outside this "
                "channel) and every command bills a block, which then stays warm between calls. Confirm with the "
                "user, then call ensure_endpoint_up(confirm_spend=True) before running work.")
    return ("Surface the allocation balance (e.g. run_shell('mybalance', shape='login')) and call "
            "ensure_endpoint_up(confirm_spend=True) to proceed — or use shape='login' for free "
            "login-node work.")

def _login_wait_s_UNUSED() -> float:
    """How long a tool call waits for a browser login to land before returning needs_login. Long enough
    for a real IdP round-trip (password + Duo), short enough to stay well under the flow's TTL and any
    MCP tool timeout (run_shell already blocks far longer)."""
    return config.login_wait_s()

def _login_notice(start: LoginStart, flow_error: str | None, *, facility: str | None = None,
                  waited_s: float | None = None) -> str:
    """The agent-facing instructions for a login, by mode. Same discipline as needs_preauth: relay
    the link, never ask for a password, never handle a token; in paste mode only a one-time code
    (not a token) passes through the chat."""
    for_what = f" before {facility!r} can be reached" if facility else ""
    head = (f"A Globus login is needed{for_what} (first use, or a stored credential missing a scope an "
            "endpoint needs). ")
    tail = (" The login happens ONLY in the browser: never ask the user for a Globus password, and "
            "never paste the link into a shell.")
    prefix = f"({flow_error}) " if flow_error else ""
    if start.mode == "browser":
        waited = (f"A browser window opened to Globus and I waited {waited_s:.0f}s for the login to land; it "
                  "hasn't yet. ") if waited_s else "A browser window should have opened to Globus. "
        return prefix + head + waited + (
            "If the browser already shows the login finished ('you may close this window'), just call "
            "connect_facility again — nothing to paste. If no browser opened, give the USER this link to "
            f"open: {start.login_url}\nThe link is SINGLE-USE: once the page says they can return, do not "
            "reopen it (it will fail — that is not an error). It is valid for ~10 minutes; a new "
            "connect_facility issues a fresh one and waits again."
        ) + tail
    return prefix + head + (
        f"Give the USER this link to open: {start.login_url}\nAfter they log in and approve, Globus shows "
        "a one-time authorization CODE. Ask them to paste that code here and call complete_login(code). "
        "It is single-use and expires in minutes."
    ) + tail

def _needs_login_result(facility: str, start: LoginStart, flow_error: str | None,
                        waited_s: float | None = None) -> ConnectFacilityResult:
    return ConnectFacilityResult(
        phase="needs_login",
        facility=facility,
        login_url=start.login_url,
        login_mode=start.mode,
        notice=_login_notice(start, flow_error, facility=facility, waited_s=waited_s),
    )

def _needs_preauth_result(facility: str, target, *, otp_ok: bool = False) -> ConnectFacilityResult:
    """Surface a one-time interactive-auth handoff (password / MFA / Duo). The user opens a
    ControlMaster in THEIR OWN terminal (entering the secret there); hpc-bridge then multiplexes
    over it. The agent relays the command and NEVER handles the secret — see the credential-handling
    policy in the vault (`Planned/MFA and interactive SSH auth`)."""
    if not getattr(target, "control_dir", None):  # multiplexing off -> a pre-opened master can't be shared
        return ConnectFacilityResult(
            phase="needs_preauth",
            facility=facility,
            notice=f"{target.host} needs an interactive login (password/MFA), but SSH multiplexing is "
            "off. Set HPC_BRIDGE_SSH_CONTROL_PERSIST (e.g. 3600) so a pre-opened master is reusable, "
            "then call connect_facility again.",
        )
    cmd = target.preauth_command()
    pinned = ""
    alias = getattr(target, "host_key_alias", None)
    if alias and alias != target.host:
        pinned = (f"The endpoint runs on login node {target.host} (the facility placed it there); the code opens a "
                  f"shared connection to that node itself, not to {alias} — one code per session, not per command. ")
    if otp_ok:
        notice = pinned + (
            f"{target.host} needs a one-time login step: the facility accepts a ONE-TIME CODE (TOTP / Duo "
            "passcode). Ask the USER for the CURRENT code from their authenticator and call "
            "complete_preauth(code) — the code is single-use and expires in seconds. NEVER ask for a password; if the "
            "facility asks for one, complete_preauth refuses and the user runs this in THEIR OWN terminal instead:\n"
            f"    {cmd}\nOnce the connection is open, call connect_facility again — the session rides it with no "
            "further auth."
        )
    else:
        notice = pinned + (
            f"{target.host} needs a one-time interactive login (a password and/or MFA/Duo). Ask the "
            "USER to run this in THEIR OWN terminal — they enter the secret directly; never ask for, "
            f"type, or run it with their password yourself:\n    {cmd}\n"
            "It authenticates once and opens a reusable connection. When they confirm it's connected, "
            "call connect_facility again — the session then rides that connection with no further auth."
        )
    return ConnectFacilityResult(phase="needs_preauth", facility=facility, preauth_command=cmd,
                                 preauth_code_ok=otp_ok, notice=notice)

def _dispatch_error_suffix(canary: CanaryResult | None) -> str:
    """A suffix naming a NON-timeout canary failure, else ''. A timeout is the normal cold-start wait
    and stays silent ('allocating nodes…'); anything else means a submit was refused or the dispatch
    path broke — the caller must see WHY rather than keep waiting on a block that will never come."""
    if canary is None or canary.ok or not canary.error or canary.error == "timeout":
        return ""
    if _transient_dispatch_failure(canary.error):
        return (f" — last dispatch was refused as TRANSIENT: {canary.error}. The endpoint is still processing "
                "a previous start request — wait ~10 s and call again (not a config problem)")
    return f" — last dispatch failed: {canary.error}. Not a queue wait: fix the config/partition and retry"

def _transient_dispatch_failure(error: str | None) -> bool:
    """The web service's 409 RESOURCE_CONFLICT ('Endpoint … is already in use: possibly due to concurrent
    requests -- please try again'): seen live when a submit followed another within ~2 s."""
    e = (error or "").lower()
    return "resource_conflict" in e or ("already in use" in e and ("endpoint" in e or "concurrent requests" in e))

# The MEP manager's failure notices when it cannot start a user endpoint for the caller's identity
# (globus_compute_endpoint/endpoint/endpoint_manager.py; delivered by the web service as the task's
# failure reason, so our canary future raises with this text). None of them is a queue wait, and
# nothing on our side changes them: the facility must grant an account / add the identity mapping.
_NO_ACCOUNT_MARKERS = (
    "failed to map to a local user",          # identity mapping found no local user
    "local user does not exist",              # mapped, but the account isn't on the machine
    "untrusted identity",                     # single-user endpoint: not the owner's identity
)

# The scheduler REFUSED the block's submission: parsl's provider could not read a job id from the submit
# command (sbatch/qsub exited non-zero — a bad account, partition or QOS, a missing resource request), or the
# scheduler said so in words. Not a queue wait, not an identity problem: the config is wrong for this facility.
# Seen live on the fake MEP (accounting enforced; account = a login name, 2026-09-09): the client kept saying
# "allocating nodes…" for five polls with this text buried in the notice's suffix.
_SUBMIT_REJECTED_MARKERS = (
    "could not read job id from submit command",
    "failed to start block",
    "cannot launch job",
    "batch job submission failed",
    "invalid account",
    "invalid qos",
    "invalid partition",
    "requested node configuration is not available",
)


def _submit_rejected(error: str | None) -> bool:
    e = (error or "").lower()
    return any(m in e for m in _SUBMIT_REJECTED_MARKERS)


def _submit_rejection_cause(error: str | None) -> str:
    """The one line worth showing: what follows the provider's 'failed due to:' (or the error itself), no traceback."""
    e = str(error or "")
    if "failed due to:" in e:
        e = e.split("failed due to:", 1)[1]
    e = " ".join(e.replace("+", " ").split())
    return e[:220]


def _submit_rejected_notice(partition: str | None, account: str | None, error: str | None) -> str:
    where = f" on {partition!r}" if partition else ""
    charged = f" for account {account!r}" if account else " with no account"
    return (f"the scheduler REJECTED the block submission{where}{charged}: {_submit_rejection_cause(error)}. "
            "Nothing is queued and nothing was billed. Not a queue wait: the account, partition or QOS is wrong for "
            "this facility, or the partition needs a resource request. Ask the user / check the facility's docs, then "
            "change it and confirm again — ensure_endpoint_up(account=…, partition=…, confirm_spend=True). Do not "
            "retry unchanged.")


def _no_account_failure(error: str | None) -> bool:
    e = (error or "").lower()
    return any(m in e for m in _NO_ACCOUNT_MARKERS)

_GLOBUS_USERNAME_RE = re.compile(r"Globus username:\s*([^\s'\"),]+)")

def _identity_from_error(error: str | None) -> str | None:
    """The web service echoes the submitter in the 422 ('Globus username: alice@example.edu')."""
    m = _GLOBUS_USERNAME_RE.search(error or "")
    return m.group(1) if m else None

def _no_account_notice(app: AppCtx | None, error: str | None, identity: str | None) -> str:
    identity = _identity_from_error(error) or identity
    who = f" ({identity})" if identity else ""
    eid = app.state.endpoint_id if app is not None else None
    where = f" {eid}" if eid else ""
    return (
        f"NO ACCOUNT at this facility: its multi-user endpoint{where} could not map the user's Globus "
        f"identity{who} to a local user — the facility's manager said: {(error or '').strip()[:320]}. "
        "This is TERMINAL, not a queue wait: no retry or poll from here changes it. The user needs an account "
        "on this machine with their Globus identity added to the endpoint's identity mapping — tell them to "
        "ask the facility's support, quoting that identity. Do not call ensure_endpoint_up again until they have."
    )

def _allocating_notice(partition: str | None, elapsed_s: float, *, facility_mep: bool) -> str:
    """The provisioning notice. After five minutes on a facility-run endpoint, say what the client cannot
    see: a scheduler REJECTION there looks exactly like an endless allocation (Anvil gpu-debug, live
    2026-09-04: every sbatch failed the QOS policy — no GPU request — while the client saw 'allocating')."""
    notice = f"allocating nodes on {partition!r}…" if partition else "allocating nodes…"
    if facility_mep and elapsed_s >= 300:
        notice += (f" Still allocating after {int(elapsed_s)}s. On a facility-run endpoint a scheduler rejection is "
                   "invisible from here and looks exactly like this: check the account, the partition, and whether "
                   "the partition needs a resource request (a GPU partition usually needs scheduler_options like "
                   "'#SBATCH --gpus-per-node=1'); or try the facility's default partition.")
    return notice


def _cold_outcome(block: BlockState, canary: CanaryResult | None = None) -> ShellOutcome:
    if canary is not None and _no_account_failure(canary.error):
        from .login import globus_identity_label

        return ShellOutcome(phase="failed", block_state=block,
                            notice=_no_account_notice(None, canary.error, globus_identity_label(fetch=False)))
    if canary is not None and _submit_rejected(canary.error):
        return ShellOutcome(phase="failed", block_state=block,
                            notice="scheduler compute shape: " + _submit_rejected_notice(None, None, canary.error))
    return ShellOutcome(
        phase="cold_start",
        block_state=block,
        est_wait_s=60,
        notice="allocating nodes…" + _dispatch_error_suffix(canary),
    )

def _needs_account_notice(app: AppCtx | None = None) -> str:
    """The account floor fired: the facility requires an allocation account and none is set — nothing started.
    Names what an account is NOT (a login name — the live confusion) and where the real one comes from."""
    return ("this facility requires an allocation account for every block and none is set — nothing was started. "
            "A LOGIN NAME is not an account: an account is the project/allocation the scheduler charges (a project "
            "id from the facility's allocation tool or portal, or the ACCESS project id). Ask the user which one to "
            "charge, then call ensure_endpoint_up(account=…, partition=…, confirm_spend=True).")


def _needs_account_outcome(app: AppCtx | None = None) -> ShellOutcome:
    """A billed shape on an account-required facility with no account: the command is NOT dispatched and no block
    is started."""
    return ShellOutcome(
        phase="needs_account",
        block_state="cold",
        notice="scheduler compute shape: " + _needs_account_notice(app),
    )


def _needs_confirmation_outcome(app: AppCtx | None = None) -> ShellOutcome:
    """A billed shape whose spend wasn't acknowledged: the command is NOT dispatched and no
    block is started. The agent must run the budget gate and confirm via ensure_endpoint_up."""
    return ShellOutcome(
        phase="needs_confirmation",
        block_state="cold",
        notice="scheduler compute shape: spend not confirmed, so nothing ran. " + _spend_floor_guidance(app),
    )

def _busy_session_outcome(task_id: str, shape: str, session_id: str) -> ShellOutcome:
    return ShellOutcome(
        phase="failed",
        block_state="warm",
        exit_code=None,
        notice=(f"session {session_id!r} on shape {shape!r} still has a task running "
                f"(task_id={task_id!r}); poll_task it, or run in a different session_id. Two commands "
                "can't share one session's cwd/env at once."),
    )

def _running_outcome(app: AppCtx, task_id: str, ceiling_s: float) -> ShellOutcome:
    out = ShellOutcome(
        phase="running",
        block_state="warm",
        task_id=task_id,
        notice=(f"still running past the ~{int(SYNC_WAIT_S)}s sync-wait — it was NOT cut. Poll for its "
                f"result with poll_task({task_id!r}). It runs up to ~{int(ceiling_s)}s (the block "
                "walltime) then is killed (exit 124); submit a batch job for anything longer. The "
                "block stays warm while it runs."),
    )
    return _with_spend(app, out)

def _shape_reject_outcome(notice: str) -> ShellOutcome:
    return ShellOutcome(phase="failed", block_state="cold", exit_code=None, notice=notice)

def _orphaned_outcome(app: AppCtx, task_id: str) -> ShellOutcome:
    return _with_spend(app, ShellOutcome(
        phase="failed", block_state="cold", exit_code=None,
        notice=(
            f"task {task_id!r} is ORPHANED: the endpoint it was dispatched to is offline or gone, so its "
            "result can never arrive — stop polling. Its block state is unknown (a stopped/deleted "
            "endpoint takes its blocks with it; a facility outage may leave one billing). If you "
            "stopped or tore down the endpoint, this is expected; otherwise connect_facility again "
            "and re-run the command."
        ),
    ))

def _error_outcome(exc: Exception) -> ShellOutcome:
    return ShellOutcome(
        phase="failed",
        block_state="cold",
        exit_code=1,
        notice=f"hpc-bridge error: {type(exc).__name__}: {exc}"[:500],
    )
