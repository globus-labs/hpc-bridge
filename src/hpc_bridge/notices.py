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
import time
from collections.abc import Collection
from typing import Literal

from . import config
from .config import SYNC_WAIT_S, _parse_hhmmss, _task_ceiling_s
from .context import AppCtx, LeftBlock, ShapeRuntime, _has_login_shape, _idle_release_s
from .cost import _block_nodes, _with_spend
from .lifecycle import BlockState
from .login import LoginStart
from .models import AccountSource, BlockTerms, ConnectFacilityResult, QueueSource, ShellOutcome, Submission
from .runner import CanaryResult
from .scheduler_ops import _submission_refused

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
    # These already say what to do; their quoted text (whoami's, a build's last error line) could otherwise be misread
    # as an SSH denial or a network error ("error: Failed to fetch … connection timed out" from PyPI is not the host).
    if type(exc).__name__ in ("RemoteTokenStoreExists", "EndpointVenvError"):
        return raw
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
    if "revoked" in low and ("host key" in low or "revoked by file" in low):
        # never "not in known_hosts yet, connect and accept": the key is on a revocation list (known_hosts @revoked
        # or RevokedHostKeys) — a stolen-key signature. Not prefixed UNKNOWN HOST KEY, so the pin is not dropped.
        return (f"REVOKED HOST KEY for {host}: the key it presents is on your revocation list ({ssh_line[:160]}). "
                "Do NOT connect; contact the facility. Nothing was started or billed.")
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
        return (f"TIMED OUT waiting for the login node {host}: a bootstrap step did not finish in time (a slow login "
                "node or filesystem). Every step is safe to repeat: an unfinished build of hpc-bridge's endpoint venv "
                "is stopped and never takes the venv's place, and the next connect builds it again — call "
                "connect_facility again in a minute or two. Nothing was billed.")
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
        f"parsl{canary.worker_parsl}" if getattr(canary, "worker_parsl", None) else None,
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
    ceiling = int(_task_ceiling_s(rt.user_endpoint_config))
    return (f"billed block bounds — a task runs up to ~{ceiling}s (the block walltime); one that "
            f"outlives the ~{int(SYNC_WAIT_S)}s sync-wait returns a poll handle (poll_task), it is NOT "
            f"cut. The block idle-releases after {_idle_window_text(app)} once nothing runs or is queued, so run "
            "long work as a foreground task — don't detach it (a detached process isn't a Compute task).")

def _idle_window_text(app: AppCtx) -> str:
    """The idle-release window as the agent should relay it: a number only when it is KNOWN. A facility endpoint's
    window is the facility's (its template), and hpc-bridge's own 600 s default is not it (review 2026-09-05 #6a)."""
    idle = _idle_release_s(app)
    if idle is not None:
        return f"~{idle}s"
    return "the facility's own idle window (its template sets none hpc-bridge can read)"

def _block_terms(app: AppCtx, rt: ShapeRuntime, *, named: Collection[str] = ()) -> BlockTerms:
    """The block `rt` would start, or did (BlockTerms): its queue and account and where each came from, its size and
    walltime, read from the shape's own config — what the scheduler is asked for. `named`: the keys ("partition",
    "account") THIS call passed; a key chosen by an earlier call this binding is in `app.compute_choice` (a runtime
    seeded after a stop carries it too); anything else is the facility's default, whose source was recorded with the
    binding (`app.terms_origin`)."""
    uec = rt.user_endpoint_config
    origin = app.terms_origin

    def chosen(key: str) -> Literal["this_call", "earlier_call"] | None:
        return "this_call" if key in named else "earlier_call" if key in app.compute_choice else None

    queue = str(uec.get("partition") or "") or None
    queue_source: QueueSource
    if queue is None:
        queue_source = "facility_template"
    else:
        queue_source = chosen("partition") or (origin.partition if origin is not None else "registry")
    account = str(uec.get("account") or "") or None
    account_source: AccountSource
    if account is not None:  # a configured default account is only ever the HPC_BRIDGE_ACCOUNT preset
        account_source = chosen("account") or "env"
    elif origin is not None and origin.account_needed is False and not getattr(app.facility, "account_required", False):
        account_source = "none_needed"
    else:
        account_source = "none_set"
    nodes = _block_nodes(rt, app)
    walltime = str(uec.get("walltime") or "") or None
    secs = _parse_hhmmss(walltime)
    return BlockTerms(
        facility=app.machine or config.machine() or getattr(app.facility, "name", None),
        queue=queue, queue_source=queue_source, account=account, account_source=account_source,
        account_required=bool(getattr(app.facility, "account_required", False)),
        account_hint=origin.account_hint if origin is not None else None,
        nodes=nodes, walltime=walltime, max_node_hours=round(nodes * secs / 3600, 2) if secs > 0 else None,
        compute_only=not _has_login_shape(app),
    )

# How a source reads in a notice (BlockTerms' vocabulary: models.py).
_QUEUE_FROM = {
    "registry": "the registry's default",
    "env": "preset by HPC_BRIDGE_PARTITION",
    "byo_details": "from the facility details you confirmed",
    "discovered": "proposed by the login-node probe",
    "this_call": "set by this call",
    "earlier_call": "chosen earlier this session",
}
_ACCOUNT_FROM = {"this_call": "set by this call", "earlier_call": "chosen earlier this session",
                 "env": "preset by HPC_BRIDGE_ACCOUNT"}

def _queue_word(app: AppCtx) -> str:
    """'queue' on PBS, 'partition' elsewhere — the facility's own word for it."""
    fac = app.facility
    sched = getattr(fac, "scheduler", None) or getattr(getattr(fac, "profile", None), "scheduler", None)
    return "queue" if sched == "pbs" else "partition"

def _terms_line(app: AppCtx, t: BlockTerms, *, compute_note: bool = True, lead: str = "Block terms") -> str:
    """The block in one sentence, queue and account first, each with where it came from — the same words for
    ensure_endpoint_up, run_shell and connect_facility, so the question to the user can name them. On a facility
    endpoint it adds what compute-only means for spend (`compute_note`; connect's own notice already says it)."""
    word = _queue_word(app)
    where = (f"{word} '{t.queue}' ({_QUEUE_FROM.get(t.queue_source, t.queue_source)})" if t.queue
             else f"the facility's default {word} (hpc-bridge sends none)")
    if t.account:
        acct = f"account '{t.account}' ({_ACCOUNT_FROM.get(t.account_source, t.account_source)})"
    elif t.account_source == "none_needed":
        acct = "no account needed here"
    elif t.account_required:
        acct = "account: none set (required here)" + (f" — {t.account_hint}" if t.account_hint else "")
    else:
        acct = "no account set (the scheduler's default account applies, if it has one)"
    size = f"{t.nodes} node{'' if t.nodes == 1 else 's'}"
    size += (f" for up to {t.walltime}" + (f" (at most {t.max_node_hours:g} node-hours)" if t.max_node_hours else "")
             if t.walltime else " for the facility's default walltime")
    line = f"{lead}: {where}; {acct}; {size}."
    if t.compute_only and compute_note:
        line += (" Compute-only through this channel: every command bills, and the block stays warm between commands "
                 "until the facility's idle window ends it.")
    return line + " "

def _connect_terms_note(app: AppCtx, terms: BlockTerms, *, listed: bool = False) -> str:
    """connect_facility's statement of the next block's terms, before any spend — and that the user is asked first
    (never a confirm in the first call: the agent asks for what the user has not said, then starts). `listed`: the
    result lists allocations to choose from."""
    word = _queue_word(app)
    if terms.account is None and (terms.account_required or listed):
        which = "which listed allocation" if listed else "which account"
        ask = f"{which} to charge and whether that {word} is OK"
    elif terms.account is not None:
        ask = f"whether that {word} and account are OK"
    else:
        ask = f"whether that {word} is OK"
    return ("Nothing is started or billed yet. "
            + _terms_line(app, terms, compute_note=False, lead="The next block's terms")
            + f"Before the first block, ask the user {ask}, unless they already said; then call ensure_endpoint_up "
            "with their answer (it starts nothing without their spend confirmation).")

def _needs_confirmation_notice(app: AppCtx, rt: ShapeRuntime, terms: BlockTerms | None = None) -> str:
    """The spend-floor notice: the block's terms first (queue and account with their sources, size, walltime), then why
    spend is asked again if it is, then what to do. Names the free login shape as the alternative ONLY where one
    exists — on a compute-only facility every shape is billed, so pointing at shape='login' is a dead-end. The size is
    the block's own (its config), not the never-set profile default (review 2026-09-05 #6b)."""
    terms = terms or _block_terms(app, rt)
    return (_terms_line(app, terms) + _reask_prefix(app, rt) + "Spend not yet confirmed: nothing was started. "
            + _spend_floor_guidance(app))

def _reask_prefix(app: AppCtx | None, rt: ShapeRuntime | None) -> str:
    """Why spend is being asked AGAIN: the confirmation covered one block, and that block is gone — or the partition or
    account has changed since (`rt.switched_terms`), which is a different block, unless it is back to the terms the
    confirmation covered. A block a facility-endpoint switch left is named either way (`rt.switch_left`), while it may
    still bill (`_still_left`)."""
    if rt is None:
        return ""
    out = ""
    left = rt.switch_left if app is not None and _still_left(app, rt.switch_left) else None
    if rt.reaped is not None:
        # Found by a check (kicked): a replacement may already be running, and billing from when it started.
        bills = ("a replacement bills from the moment it starts, confirmed or not" if rt.reap_kicked else
                 "a new block bills again")
        out = (f"Confirm spend again with the user: {rt.reaped}. The earlier confirmation covered that block only; "
               f"{bills}. ")
    if rt.switched_terms is not None:
        uec = rt.user_endpoint_config
        if rt.switched_terms == (uec.get("partition"), uec.get("account")):
            return out + ("Confirm spend again with the user: a partition or account switch dropped the confirmation, "
                          f"and the terms are back to the ones it covered ({_terms(*rt.switched_terms)}). Confirming "
                          "again uses that block if it is still up, or starts a new one. ")
        out += ("Confirm spend again with the user: the partition or account has changed since spend was confirmed, "
                "and that is a different block (the earlier confirmation covered only the one on "
                f"{_terms(*rt.switched_terms)}). ")
    elif rt.reap_terms is not None and rt.reap_terms != (rt.user_endpoint_config.get("partition"),
                                                         rt.user_endpoint_config.get("account")):  # a switch since
        out += "The partition or account was switched since, so a confirmation now starts a block on the new terms. "
    if app is not None and (rt.switched_terms is not None or left is not None):
        out += _switch_fate(app, left, confirmed=False)
    return out

def _still_left(app: AppCtx, left: LeftBlock | None) -> bool:
    """A block a switch left is still one a stop would name: still in `app.left_blocks`, and not past its walltime's
    end (the rule warmth._prune_left applies)."""
    return left is not None and left in app.left_blocks and (left.ends_by is None or left.ends_by > time.monotonic())

def _switch_fate(app: AppCtx, left: LeftBlock | None, *, confirmed: bool) -> str:
    """What became of the block a partition or account switch moved away from: nothing cancels it. On an SSH endpoint it
    idle-releases on its own (min_blocks=0), and stop_endpoint cancels every block of the endpoint by its marker (the
    new choice survives the stop). A facility endpoint's `left` block is named as a reconnect names one
    (`_left_block_clause`); None: no block of ours was known there. `confirmed`: the new block is already confirmed (a
    stop now ends it too)."""
    if _has_login_shape(app):
        idle = f"it idle-releases {_idle_window_text(app)} after its last task (min_blocks=0)"
        if confirmed:
            return (f"Nothing cancelled the previous block: {idle}; stop_endpoint cancels every block of this "
                    "endpoint, this new one included. ")
        return (f"Nothing cancels the previous block on a switch, even one still queued: {idle}. To end it now, call "
                "stop_endpoint before confirming the new block: it cancels this endpoint's blocks and keeps the new "
                "partition and account for the next one. ")
    if left is None:
        return ""
    clause = _left_block_clause(left, getattr(app.facility, "scheduler", None), subject="that block")
    return clause[:1].upper() + clause[1:] + "; stop_endpoint names it. "

def _switch_receipt(app: AppCtx, rt: ShapeRuntime) -> str:
    """For the call that switched and confirmed at once (server._ensure_endpoint_up): the old terms' block is not what
    this confirmation covers, and what became of it — no needs_confirmation answer told it. Empty when there was no
    confirmed block to move from, or the terms are back to the ones it covered."""
    terms = rt.switched_terms
    uec = rt.user_endpoint_config
    if terms is None or terms == (uec.get("partition"), uec.get("account")):
        return ""
    return (f"This confirmation is for a new block: the partition or account changed from {_terms(*terms)}. "
            + _switch_fate(app, rt.switch_left, confirmed=True))

def _spend_floor_guidance(app: AppCtx | None) -> str:
    """What to do about an unconfirmed spend — ONE text for ensure_endpoint_up and run_shell/reset
    (they used to drift). Names the free login shape only where one exists: on a compute-only
    facility every shape is billed, so pointing at shape='login' is a dead-end."""
    if app is not None and not _has_login_shape(app):
        return ("This facility is compute-only through this channel: hpc-bridge reaches it only via its compute "
                "endpoint, so there is no free login shape here (the facility's login nodes are outside this "
                "channel). Confirm these terms with the user, then call ensure_endpoint_up(confirm_spend=True) before "
                "running work.")
    return ("Ask the user, naming these terms (with the allocation balance, where the facility's balance tool shows "
            "one: it runs free on shape='login'), then call ensure_endpoint_up(confirm_spend=True) to proceed — or "
            "use shape='login' for free login-node work.")

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

def _multiplexing_off_notice(host: str, resume: str = "connect_facility") -> str:
    """A host that wants an interactive login while SSH multiplexing is off: no pre-opened master can be shared, so
    neither a one-time code (complete_preauth) nor the user's own `ssh -fN` helps — enabling multiplexing does. One
    sentence for the connect result and the no-facility notice."""
    return (f"{host} needs an interactive login (password/MFA), but SSH multiplexing is "
            "off. Set HPC_BRIDGE_SSH_CONTROL_PERSIST (e.g. 3600) so a pre-opened master is reusable, "
            f"then call {resume} again.")

def _needs_preauth_result(facility: str, target, *, otp_ok: bool = False) -> ConnectFacilityResult:
    """Surface a one-time interactive-auth handoff (password / MFA / Duo). The user opens a
    ControlMaster in THEIR OWN terminal (entering the secret there); hpc-bridge then multiplexes
    over it. The agent relays the command and NEVER handles the secret — see the credential-handling
    policy in the vault (`Planned/MFA and interactive SSH auth`)."""
    if not getattr(target, "control_dir", None):  # multiplexing off -> a pre-opened master can't be shared
        return ConnectFacilityResult(phase="needs_preauth", facility=facility,
                                     notice=_multiplexing_off_notice(target.host))
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
            "The new block was not created: nothing is queued and it bills nothing (time an earlier block ran stays "
            "in session_spend). Not a queue wait: the account, partition or QOS is wrong for "
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

def _allocating_notice(partition: str | None, elapsed_s: float, *, facility_mep: bool,
                       stale: str | None = None, job: str | None = None, job_verified: bool = False,
                       scheduler: str | None = None, user: str | None = None) -> str:
    """The provisioning notice. After five minutes on a facility-run endpoint, say what the client cannot
    see: a scheduler REJECTION there looks exactly like an endless allocation (Anvil gpu-debug, live
    2026-09-04: every sbatch failed the QOS policy — no GPU request — while the client saw 'allocating'), and so
    does a block that IS running but whose worker cannot hand results back (a package mismatch with the
    facility's endpoint, Anvil and Delta 2026-10-06) — which bills, and which the facility keeps relaunching while
    a task stays queued. `stale`: the attach's STALE ENTRY note, the likeliest cause of the latter. `job`: a block
    hpc-bridge saw for this partition and account (warmth._known_block_job), named with the scheduler's own cancel
    command only when `job_verified` (it is this shape's confirmed block) — an unverified one may be gone, and an
    `scancel` of a wrong id can end someone else's job (#178). `user`: the local account the facility maps the
    identity to (its jobs run as that account, not the user's login name)."""
    notice = f"allocating nodes on {partition!r}…" if partition else "allocating nodes…"
    if facility_mep and elapsed_s >= 300:
        cancel, _check, by_user = _scheduler_cmds(scheduler)
        mine = by_user.format(user=user) if user else by_user.format(user="<the account the facility maps you to>")
        if job and job_verified:
            known = (f" The block hpc-bridge last confirmed here is scheduler job {job} (`{cancel.format(job=job)}` "
                     "ends it).")
        elif job:
            known = (f" hpc-bridge last confirmed scheduler job {job} on this partition before it was let go; it may "
                     "be gone by now (not verified).")
        else:
            known = ""
        target = cancel.format(job=job if job and job_verified else "<job id>")
        notice += (f" Still allocating after {int(elapsed_s)}s.{known} On a facility-run endpoint the scheduler is "
                   "invisible from here, so three different situations look exactly like this: (1) a long queue (a "
                   "busy GPU partition can take longer); (2) a scheduler rejection — wrong account or partition, or a "
                   "missing resource request (a GPU partition usually needs one, such as `#SBATCH --gpus-per-node=1`, "
                   "which hpc-bridge cannot add to a facility endpoint's block yet); (3) a block that is RUNNING and "
                   "billing while its worker cannot return results "
                   "(a package mismatch with the facility's endpoint), which the facility keeps relaunching while our "
                   "check task stays queued — hpc-bridge cannot cancel either. Don't poll indefinitely: ask the user "
                   f"to check their jobs on the facility ({mine}, with the REASON column; {_PARSL_NAME_NOTE}). "
                   "PENDING for "
                   "Priority/Resources = (1), keep waiting or try a less busy partition (a policy reason, e.g. an "
                   "exhausted allocation, will not clear by waiting); no job at all = most likely (2), or a worker "
                   "that crashes at start; a RUNNING job that never answers = (3): call stop_endpoint and have the "
                   f"user cancel it ({target}) — that ends the current block, but while "
                   "our check stays queued the facility may start another, so if blocks keep reappearing, contact the "
                   "facility.")
        if stale:
            notice += f" Likely here — {stale}."
    return notice


# The user's own commands for a facility's block, by scheduler: end it, check it, list the account's jobs (#178).
_SCHEDULER_CMDS = {
    "slurm": ("scancel {job}", "squeue -j {job}", "squeue -u {user}"),
    "pbs": ("qdel {job}", "qstat {job}", "qstat -u {user}"),
    "lsf": ("bkill {job}", "bjobs {job}", "bjobs -u {user}"),
}
# What a Globus Compute block's scheduler job is called (parsl status_handling `parsl.<executor label>.block-<n>`, plus
# the provider's timestamp): `parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.1760…`, which squeue's default
# NAME column cuts to `parsl.Gl`. It does NOT single out ours: every Globus Compute block of a SHARED mapped account
# (globus-labs maps to glabs-gc) has it, and a facility's own --job-name can override it. Only for a block whose job id
# is unknown — a named job id is always preferred.
_PARSL_NAME_NOTE = ("Globus Compute blocks are named `parsl.GlobusComputeEngine-…` (squeue shows `parsl.Gl`) unless "
                    "the facility renames them, and others on a shared account may show the same prefix: check the "
                    "start time and partition before cancelling")

def _scheduler_cmds(scheduler: str | None) -> tuple[str, str, str]:
    return _SCHEDULER_CMDS.get(str(scheduler or "slurm").lower(), _SCHEDULER_CMDS["slurm"])

def _clock_time(t: float, now: float, *, rel: bool = True) -> str:
    """A time.monotonic() instant as the user's own clock time, for a notice the agent relays: `21:35 (in ~11 min)`,
    with the date when it is not today (`Oct 10 21:24`). `rel`: with how far off it is."""
    at = time.localtime(time.time() + (t - now))
    today = time.localtime()
    hhmm = time.strftime("%H:%M" if at[:3] == today[:3] else "%b %d %H:%M", at)
    if not rel:
        return hhmm
    mins = round(abs(t - now) / 60)
    off = "about now" if mins == 0 else (f"in ~{mins} min" if t > now else f"~{mins} min ago")
    return f"{hhmm} ({off})"

def _left_state(b: LeftBlock, now: float) -> str:
    """'up': a block that may still bill (its latest end ahead, or unknown); 'released': presumed gone by its latest
    end, not verified; 'queued': no block seen, only a check of ours queued."""
    if b.kind == "check":
        return "queued"
    return "up" if b.until is None or b.until > now else "released"

def _left_spend_stopped(blocks: list[LeftBlock], now: float) -> bool | None:
    """`spend_stopped` for a facility-endpoint stop or detach: never True (nothing here cancels a block). False while a
    block it left may still bill or a check of ours is queued (the facility starts or keeps a block for it); None when
    every block is presumed released by now, or none is known."""
    if any(_left_state(b, now) in ("up", "queued") or b.check_queued for b in blocks):
        return False
    return None

def _left_block_state(blocks: list[LeftBlock], now: float) -> BlockState:
    """`block_state` for a facility-endpoint stop or detach: the block's PRESUMED state, never 'cold' while one may
    still run — 'warm' (a block may still bill), 'provisioning' (a check of ours queued: the facility may be starting
    one), else 'cold'."""
    states = {_left_state(b, now) for b in blocks}
    if "up" in states:
        return "warm"
    return "provisioning" if "queued" in states or any(b.check_queued for b in blocks) else "cold"

def _left_job(blocks: list[LeftBlock], now: float) -> str | None:
    """`block_job_id`: the first named block that may still bill (the stop's own first), else the first named at all."""
    for b in blocks:
        if b.job and _left_state(b, now) == "up":
            return b.job
    return next((b.job for b in blocks if b.job), None)

_UNTIL_WHY = {
    "idle": "",
    "walltime": " (its walltime: the facility's idle window is unknown here)",
    "busy": " (its walltime: work of ours may still be running on it)",
}

# At most this many earlier queued checks are named one by one (most recent first); the rest are counted.
_CHECKS_NAMED = 5

def _terms(partition: str | None, account: str | None) -> str:
    """A block's partition (and account), for a notice."""
    where = f"partition {partition!r}" if partition else "the default partition"
    return where + (f", account {account!r}" if account else "")

def _where(b: LeftBlock) -> str:
    """The partition (and account) a left block or check belongs to, for a notice."""
    return _terms(b.partition, b.account)

def _left_block_clause(left: LeftBlock, scheduler: str | None, *, subject: str = "its block") -> str:
    """What a facility endpoint did not do to a block it let go of (a reconnect, a switch): "its block, scheduler job
    N, was not cancelled — no cancel channel on a facility endpoint: it bills until that facility's idle-release
    (`scancel N` on that facility ends it)", the cancel per scheduler. A check still queued is said as one."""
    if left.kind == "check":
        return ("a check of ours was still queued there and was not cancelled — no cancel channel on a facility "
                "endpoint: a block the facility starts for it bills until that facility's idle-release")
    which = f"{subject}, scheduler job {left.job}," if left.job else subject
    end = f" (`{_scheduler_cmds(scheduler)[0].format(job=left.job)}` on that facility ends it)" if left.job else ""
    return (f"{which} was not cancelled — no cancel channel on a facility endpoint: it bills until that facility's "
            f"idle-release{end}")

def _no_cancel_notice(app: AppCtx, blocks: list[LeftBlock], now: float, *, current: LeftBlock | None = None,
                      lost: str = "", op: str = "stop") -> str:
    """What a stop or detach on a facility multi-user endpoint leaves billing, said so the agent can relay it (#178).
    It LEADS with the fact that no cancel was issued, then names each block left (`blocks`: this call's own —
    `current` — first, then the ones an earlier stop, detach, reconnect or switch left): its scheduler job, the latest
    it can bill until as a clock time (possibly sooner, not verified), the user's own commands to end it and check on
    it there — and never a check from here: run_shell or a confirmed ensure_endpoint_up after a stop is a task, which
    can keep the block alive or start a new one. Only this call's own block is said to be in `session_spend` now (a new
    binding starts it at 0): an earlier one was counted in the spend reported when it was left, and a block a queued
    check may start is never counted. `op`: "stop" (draining, final) or "detach"."""
    cancel, check, by_user = _scheduler_cmds(getattr(app.facility, "scheduler", None))
    if app.worker_user:
        find = f"`{by_user.format(user=app.worker_user)}`"
    else:
        find = (f"`{by_user.format(user='<your account there>')}` (the account the facility maps your identity to, "
                "maybe not your login name)")
    find += f" on the facility ({_PARSL_NAME_NOTE})"
    unknown_end = f"ends it with `{cancel.format(job='<id>')}`; relay both."
    out = ["NO CANCEL WAS ISSUED: hpc-bridge has no cancel channel on a facility endpoint."]
    if lost:
        out.append(lost.strip())
    spend, earlier_named, checks = "", False, []
    for b in blocks:
        state, own = _left_state(b, now), b is current
        if state == "queued":
            if own:
                out.append(f"A check of ours is still queued there ({_where(b)}): the facility will start (or keep) a "
                           "block for it, which bills until its idle window passes — session_spend does not count "
                           f"that block; its job id is unknown here. The user finds it with {find} and {unknown_end}")
            else:
                checks.append(b)
            continue
        earlier = "" if own else f"Left by an earlier {b.how} at {_clock_time(b.left_at, now, rel=False)}: "
        earlier_named = earlier_named or not own
        name = f"scheduler job {b.job}" if b.job else "a block of ours (its job id is unknown here)"
        name = (name[:1].upper() + name[1:]) if own else name
        least = " (at least this one: the endpoint can run more than one block)" if b.multi else ""
        if state == "released":
            assert b.until is not None
            how = (f"the user checks with `{check.format(job=b.job)}` on the facility and ends it with "
                   f"`{cancel.format(job=b.job)}` if it is still there." if b.job else
                   f"the user checks with {find}.")
            out.append(f"{earlier}{name}{least} is presumed released by {_clock_time(b.until, now)} (not verified); "
                       f"{how}")
        else:
            if b.until is not None:
                when = (f"until {_clock_time(b.until, now)} at the latest{_UNTIL_WHY.get(b.bound or 'idle', '')}, "
                        "possibly sooner (not verified)")
                if own:
                    spend = (f"session_spend counts it to {_clock_time(b.until, now, rel=False)} (an upper bound; a "
                             "block confirmed again after a stop counts from its new confirmation).")
            else:
                when = ("— when it ends is unknown here (the facility's idle window and its walltime are both unknown"
                        + ("; session_spend counts it only up to now)" if own else ")"))
            how = (f"The user ends it now with `{cancel.format(job=b.job)}`, or checks it with "
                   f"`{check.format(job=b.job)}`, on the facility; relay both." if b.job else
                   f"The user finds it with {find} and {unknown_end}")
            out.append(f"{earlier}{name}{least} may still be running and billing {when}. {how}")
        if b.check_queued:
            out.append("A check of ours is also still queued there, so the facility may keep this block or start "
                       "another.")
    if checks:  # earlier queued checks, newest first: dated and hedged, never dropped (the queue wait is unbounded)
        named = [f"{_where(b)} at {_clock_time(b.left_at, now, rel=False)}" for b in checks[:_CHECKS_NAMED]]
        more = f", and {len(checks) - _CHECKS_NAMED} more" if len(checks) > _CHECKS_NAMED else ""
        one = len(checks) == 1
        out.append(f"{'A check' if one else 'Checks'} of ours {'was' if one else 'were'} still queued there "
                   f"({'; '.join(named)}{more}) when an earlier stop, detach, reconnect or switch left "
                   f"{'it' if one else 'them'}; {'it' if one else 'each'} may have run since, and a block "
                   f"{'it' if one else 'one'} started bills until its idle window passes — session_spend does not "
                   f"count it. The user finds {'it' if one else 'them'} with {find}, started after "
                   f"{'that time' if one else 'those times'}, and {unknown_end}")
    if not blocks:
        out.append("No block of ours is known to be running there.")
    probe = ("don't check from here (run_shell or a confirmed ensure_endpoint_up is a task that can keep the block "
             "alive or start one)")
    if op == "stop":
        out.append(f"'draining' is final: don't call stop_endpoint again, and {probe}.")
    else:
        out.append(f"Detached: nothing of ours to tear down (the facility runs the endpoint); {probe}. "
                   "connect_facility re-attaches with zero SSH.")
    if spend:
        out.append(spend)
    if earlier_named:
        out.append("A block left earlier was counted in the spend reported when it was left.")
    if op == "stop":
        out.append("The endpoint stays available.")
    return " ".join(out)


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

def _unapplied_presets_note(*, named: bool, facility_endpoint: bool, own_partition: bool) -> str:
    """One clause for a connect when an env preset (HPC_BRIDGE_ACCOUNT, HPC_BRIDGE_PARTITION) is set but not used here,
    so the user is not surprised: this facility is not the one HPC_BRIDGE_MACHINE names (`named`); or it is, but the
    partition preset never applies to a facility endpoint (whose entry sets it), nor over a partition given in
    details= (`own_partition`). Names the variables, never their values; empty when every preset set is used."""
    account, partition = config.account() is not None, config.partition() is not None
    if not named:
        unused = [n for n, is_set in (("HPC_BRIDGE_ACCOUNT", account), ("HPC_BRIDGE_PARTITION", partition)) if is_set]
        if not unused:
            return ""
        machine = config.machine()
        target = (f"HPC_BRIDGE_MACHINE={machine}" if machine else
                  "the facility HPC_BRIDGE_MACHINE names, and HPC_BRIDGE_MACHINE is unset")
        one = len(unused) == 1
        return (f"({' and '.join(unused)} {'applies' if one else 'apply'} only to {target}, so "
                f"{'it was' if one else 'they were'} not used here.) ")
    if partition and facility_endpoint:
        return ("(HPC_BRIDGE_PARTITION never applies to a facility endpoint, whose entry sets the partition, so it was "
                "not used here.) ")
    if partition and own_partition:
        return "(HPC_BRIDGE_PARTITION gives way to the partition given in details=, so it was not used here.) "
    return ""

def _needs_account_notice(app: AppCtx | None = None, terms: BlockTerms | None = None) -> str:
    """The account floor fired: the facility requires an allocation account and none is set — nothing started.
    Leads with the block's terms (which carry the registry entry's account hint, where it has one). Names what an
    account is NOT (a login name — the live confusion) and where the real one comes from: on a facility with a login
    shape (SSH), connect_facility's allocations or the facility's own tool run free there."""
    if app is not None and _has_login_shape(app):
        source = ("from connect_facility's allocations when it listed some, from the facility's allocation tool, which "
                  "runs free on the login shape with run_shell(…, shape='login'), or from its portal")
    else:
        source = "from the facility's allocation tool or portal"
    head = _terms_line(app, terms) if app is not None and terms is not None else ""
    return (head + "This facility requires an allocation account for every block and none is set — nothing was "
            "started. A LOGIN NAME is not an account: an account is the project/allocation the scheduler charges, a "
            f"project id {source}, or the ACCESS project id. Ask the user which one to "
            "charge, then call ensure_endpoint_up(account=…, partition=…, confirm_spend=True).")


def _needs_account_outcome(app: AppCtx | None = None, rt: ShapeRuntime | None = None) -> ShellOutcome:
    """A billed shape on an account-required facility with no account: the command is NOT dispatched and no block
    is started. Its `block` and notice name the terms, as ensure_endpoint_up's do."""
    terms = _block_terms(app, rt) if app is not None and rt is not None else None
    return ShellOutcome(
        phase="needs_account",
        block_state="cold",
        block=terms,
        notice=_needs_account_notice(app, terms),
    )


def _needs_confirmation_outcome(app: AppCtx | None = None, rt: ShapeRuntime | None = None) -> ShellOutcome:
    """A billed shape whose spend wasn't acknowledged: the command is NOT dispatched and no
    block is started by it. The agent must run the budget gate and confirm via ensure_endpoint_up.
    After a reap found by a check, that check may already be bringing a block up — say so in block_state. Its `block`
    and the head of its notice name the terms, in the words ensure_endpoint_up's use."""
    terms = _block_terms(app, rt) if app is not None and rt is not None else None
    head = _terms_line(app, terms) if app is not None and terms is not None else ""
    return ShellOutcome(
        phase="needs_confirmation",
        block_state="provisioning" if rt is not None and rt.reap_kicked else "cold",
        block=terms,
        notice=head + _reask_prefix(app, rt) + "Spend not confirmed, so nothing ran. " + _spend_floor_guidance(app),
    )

# A task whose call the client cancelled: the agent never saw its id, so a notice naming one says where it came from.
_CLIENT_CANCELLED = "the command of a call the client cancelled — cancelling a call does not stop its command"


def _busy_session_outcome(task_id: str, shape: str, session_id: str, *, client_cancelled: bool = False,
                          internal: bool = False) -> ShellOutcome:
    if internal:  # hpc-bridge's own login session: nothing the agent chose or can poll its way around
        return ShellOutcome(
            phase="failed", block_state="warm", exit_code=None,
            notice=(f"the login channel is busy, not cold: an earlier scheduler command of hpc-bridge's own "
                    f"(task_id={task_id!r}) is still running in its login session. Try again in a little while."),
        )
    origin = f": {_CLIENT_CANCELLED}" if client_cancelled else ""
    return ShellOutcome(
        phase="failed",
        block_state="warm",
        exit_code=None,
        notice=(f"session {session_id!r} on shape {shape!r} still has a task running "
                f"(task_id={task_id!r}{origin}); poll_task it, or run in a different session_id. Two commands "
                "can't share one session's cwd/env at once."),
    )

def _reserved_session_outcome(session_id: str) -> ShellOutcome:
    return ShellOutcome(
        phase="failed", block_state="cold", exit_code=None,
        notice=(f"session_id {session_id!r} is reserved for hpc-bridge's own scheduler commands (it would make them "
                "wait on yours): use another session_id. Nothing was run."),
    )

def _running_outcome(app: AppCtx, task_id: str, ceiling_s: float, *, client_cancelled: bool = False,
                     reset: bool = False, block_state: BlockState = "warm") -> ShellOutcome:
    """`block_state`: "cold" once the block it went to was released under it (the shape is gone)."""
    state = "the reset is still queued or running" if reset else "still running"
    why = f"({_CLIENT_CANCELLED})" if client_cancelled else f"past the ~{int(SYNC_WAIT_S)}s sync-wait"
    tail = ("The session stays busy until it ends." if reset else
            f"It runs up to ~{int(ceiling_s)}s (the block walltime) then is killed (exit 124); for anything longer, "
            "checkpoint and resume across blocks, or submit a batch job (billed to the allocation like a block, so "
            "only with the user's spend confirmation)."
            + (" The block stays warm while it runs." if block_state == "warm" else ""))
    return _with_spend(app, ShellOutcome(
        phase="running", block_state=block_state, task_id=task_id,
        notice=f"{state} {why} — it was NOT cut. Poll for its result with poll_task({task_id!r}). {tail}",
    ))

def _past_ceiling_outcome(app: AppCtx, task_id: str, ceiling_s: float, *, shape: str, session_id: str,
                          facility_mep: bool, lost_in: float, block_state: BlockState = "warm") -> ShellOutcome:
    """A task still pending past its ceiling, counted from its submit. Its ceiling runs from when it STARTS, so it may
    only be queued (behind another task, or a relaunching block) — or lost. Still `running`: keep polling, or abandon
    it, which depends on what it holds. A free login-shape task does not hold the stop, only its session and the login
    worker. A compute task holds the SSH stop (teardown abandons it). On a facility endpoint, `lost_in` seconds on, a
    compute task is presumed lost and no longer holds stop or the detach (warmth._overdue)."""
    if shape != "compute":
        abandon = (f"On the free {shape} shape it does not hold stop_endpoint, but it holds its session "
                   f"({session_id!r}) and, while it runs or waits, the {shape} worker: the next {shape} command (and "
                   "a stop's cancel) may queue behind it. To leave it, stop polling and run the next command in "
                   "another session_id")
    elif not facility_mep:
        abandon = ("To abandon it, teardown_endpoint (stop_endpoint refuses while it is outstanding: releasing the "
                   "block would not end it)")
    elif lost_in > 0:
        abandon = (f"Nothing here can cancel it; if no result arrives within ~{int(lost_in)}s it is presumed lost, and "
                   "stop_endpoint / teardown_endpoint then abandon it")
    else:
        abandon = ("It is now presumed lost: stop_endpoint / teardown_endpoint no longer wait for it and would abandon "
                   "it (its result is then no longer retrievable here)")
    return _with_spend(app, ShellOutcome(
        phase="running", block_state=block_state, task_id=task_id,
        notice=(f"task {task_id!r} has no result past its ~{int(ceiling_s)}s ceiling — queued behind another task or a "
                "relaunching block, or lost (the ceiling counts from when it starts). Keep polling: a relaunching "
                f"block still reads running. {abandon}."),
    ))

def _released_under_outcome(app: AppCtx, out: ShellOutcome | None, ceiling_s: float) -> ShellOutcome:
    """The call's block was released while it waited — by a teardown (its release step) or a re-bind; a stop looks
    under the lock first and refuses. Nothing is noted on the dropped shape (rebuilding it would revive an unconfirmed
    runtime), and the block it went to reads "cold". `out` is what the dispatch got: a result (it stays a real result,
    on a block now gone), a `running` handle for a sent task still tracked (its endpoint is bound again or still), or
    None for a command still running that nothing here can track (its endpoint is no longer the bound one)."""
    if out is None:
        return _with_spend(app, ShellOutcome(
            phase="failed", block_state="cold", exit_code=None,
            notice=("this command was still running when the endpoint it was sent to stopped being the bound one "
                    "(teardown_endpoint or connect_facility ran while the call waited), so its result can no longer "
                    "be retrieved here (no task_id was kept). If that endpoint is still up it may still run the "
                    f"command — relaunching a block to do so (spend) — until it ends or reaches its ~{int(ceiling_s)}s "
                    "ceiling. Tell the user; do not assume spend has stopped."),
        ))
    out.block_state = "cold"
    if out.phase == "running":
        note = ("its block was released while this call waited, by a teardown or a re-connect to the same endpoint; "
                "the command had already been sent, so it stays tracked until it ends or a teardown abandons it, and "
                "the endpoint may relaunch a block to run it meanwhile, which bills")
    else:
        note = ("its block was released while this call waited, by a teardown or re-connect: the next command starts "
                "on a new block (a billed one asks for spend again)")
    out.notice = f"{out.notice} ({note})" if out.notice else f"{note}."
    return _with_spend(app, out)

def _never_sent_outcome(app: AppCtx) -> ShellOutcome:
    """The SDK cancelled the task before sending it: its runner was closed under the call. Nothing ran."""
    return _with_spend(app, ShellOutcome(
        phase="failed", block_state="cold", exit_code=None,
        notice=("this command never reached the endpoint: its runner was closed while the call waited (a teardown or "
                "re-connect ran meanwhile, or the runner was rebuilt for a new endpoint) — nothing ran. Run it again "
                "if it is still wanted."),
    ))

def _scheduler_submit_notice(sub: Submission, *, inside_block: bool, phase: str, exit_code: int | None,
                             stdout: str = "") -> str:
    """What a command that started a scheduler job BY HAND (sbatch/qsub/salloc, or srun outside a block) did to the
    user's allocation, worded for what the result can actually show: sbatch/qsub refused (only when the scheduler's
    own answer is the line's exit code and no job receipt was printed), submitted, possibly submitted (a non-zero exit
    of a longer line), or still running; srun/salloc, which wait for their job, still running or over. The spend floor
    never saw it — the login shape is free, and from inside a block it is a new job beside the block. Advisory, never a
    refusal: the user may have asked for exactly this job."""
    command = sub.command
    where = "from inside the compute block (a new job beside this block)" if inside_block else "from the login node"
    confirm = "Like a block, it needs the user's spend confirmation first (allocation, partition, size, walltime)"
    queued = "qstat -u $USER" if command == "qsub" else "squeue -u $USER"
    cancel = "qdel" if command == "qsub" else "scancel"
    untracked = ("hpc-bridge neither tracks nor stops it (session_spend does not count it; stop_endpoint does not "
                 "cancel it)")
    if command in ("sbatch", "qsub"):
        if phase == "complete" and _submission_refused(sub, exit_code, stdout):
            return (f"`{command}` here exited {exit_code} with no job id in its output, so the scheduler refused the "
                    "job: none was created and nothing is billed. A batch job bills the user's allocation like a "
                    "compute block — confirm that spend with the user (allocation, partition, size, walltime) before "
                    "submitting again.")
        if phase == "running":
            did = (f"`{command}` here submits (or has already submitted) a scheduler job {where} — the command is "
                   "still running")
        elif exit_code in (0, None):
            did = f"`{command}` here submitted a scheduler job {where}"
        else:
            did = (f"`{command}` here may have submitted scheduler jobs {where} (the command exited {exit_code}, which "
                   f"is not the scheduler's answer alone — `{queued}` lists what was queued)")
        return (f"{did}: an accepted job bills the user's allocation like a compute block, but {untracked}. {confirm}: "
                f"if they have not given it, tell them about this job now. Cancel it with `{cancel} <jobid>` when it "
                "is no longer wanted.")
    if phase == "running":
        return (f"`{command}` here is running as a scheduler job {where}: it bills the user's allocation like a "
                "compute block while it holds its nodes, and hpc-bridge neither tracks nor stops it (stop_endpoint "
                f"does not cancel it). {confirm}: if they have not given it, tell them now, and cancel it with "
                f"`scancel <jobid>` (`{queued}` lists it) if it is not wanted.")
    ended = ("has finished" if exit_code in (0, None)
             else f"has ended (exit {exit_code}; if the scheduler refused it, nothing was billed)")
    return (f"`{command}` here ran as a scheduler job {where} and {ended}: it billed the user's allocation like a "
            "compute block for as long as it held its nodes, outside hpc-bridge's spend tracking (session_spend did "
            f"not count it). {confirm}: if they had not given it, tell them it ran.")

def _with_submission_notice(out: ShellOutcome, sub: Submission | None, shape: str) -> ShellOutcome:
    """Append the submission notice to an outcome whose command RAN (complete or running) — run_shell's, or a
    finished task's on poll_task. Never a refusal: the user may have asked for exactly this job."""
    if sub is not None and out.phase in ("complete", "running"):
        note = _scheduler_submit_notice(sub, inside_block=shape != "login", phase=out.phase, exit_code=out.exit_code,
                                        stdout=out.stdout)
        out.notice = f"{out.notice} {note}" if out.notice else note
    return out

def _shape_reject_outcome(notice: str) -> ShellOutcome:
    return ShellOutcome(phase="failed", block_state="cold", exit_code=None, notice=notice)

def _no_facility_notice(app: AppCtx) -> str:
    """No facility is bound (context._no_facility_bound): the ONE wording for every tool that needs one — login_shell,
    run_shell, reset_session, ensure_endpoint_up. Where the last connect_facility stopped short of a bind it is named,
    so the agent finishes THAT step: a PROPOSED config (the probe's draft) is finished with details=…; a pending
    one-time login (needs_preauth) with complete_preauth or the user's own terminal (the exact command is given) — a
    fresh connect_facility would try the SSH login again, and repeated failed logins can get the address banned
    (fail2ban). With SSH multiplexing off neither works; the notice then says what the connect result says."""
    if app.proposed_facility:
        proposed = app.proposed_facility
        return (f"No facility connected yet: connect_facility({proposed!r}) only PROPOSED a config "
                "(proposed_facility_details) — nothing is bound until you finish it. Review the draft WITH THE USER, "
                f"then call connect_facility({proposed!r}, details=…) with the confirmed config. Nothing was started.")
    if app.pending_preauth is not None:
        facility, target = app.pending_preauth
        resume = app.preauth_resume or f"connect_facility({facility!r})"
        head = (f"No facility connected yet: connect_facility({facility!r}) is waiting on a one-time login to its host "
                "(needs_preauth). ")
        if not getattr(target, "control_dir", None):  # multiplexing off: no master to share — no code, no command
            return head + _multiplexing_off_notice(target.host, resume) + " Nothing was started."
        return head + ("Finish that step, not a new connect: a one-time code goes to complete_preauth(code) — ask the "
                       "USER for the current code from their authenticator, never a password — otherwise the user "
                       f"runs this in THEIR OWN terminal:\n    {target.preauth_command()}\nThen call {resume} again. "
                       "Calling connect_facility before that tries the SSH login again, and repeated failed logins "
                       "can get the address banned. Nothing was started.")
    return ("No facility connected. Call connect_facility(facility, ssh_host=…) FIRST — it's "
            "the entry point: for a facility you've used before it reuses the endpoint over the web "
            "with ZERO SSH (no re-auth), and it decides whether SSH is even needed. Don't reach for "
            "login_shell or a manual SSH before that. Nothing was started. (Or pin one via HPC_BRIDGE_MACHINE=<id>.)")

def _no_facility_outcome(app: AppCtx) -> ShellOutcome:
    """run_shell / reset_session with no facility bound: nothing dispatched, no endpoint stood up anywhere."""
    return ShellOutcome(phase="failed", block_state="cold", exit_code=None, notice=_no_facility_notice(app))

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
