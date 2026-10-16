"""Scheduler operations over the free login shape (split step 6, 2026-09-03): cancel THIS endpoint's
blocks (Slurm scancel / PBS qdel, scoped by the `uep.<eid>` StdOut marker Parsl writes), and read the
pilot job's status to tell a normal cold-start wait from a rejected/held submission (#32). Also recognises a
scheduler submission in the agent's OWN login-shape command (`_scheduler_submission`), which the spend floor
never sees.

The login-shape channel is INJECTED (`run_login`: a coroutine taking the command) rather than imported
from `server`, so this module has no upward import; `server._login_runner(app)` builds it at call time,
which is also why tests that patch `server._run_shell` still reach these paths. Tests that patch the
release call patch `scheduler_ops._release_blocks_over_login`.
"""
from __future__ import annotations

import asyncio
import itertools
import re
import shlex
from collections.abc import Awaitable, Callable
from typing import NamedTuple

from . import config
from .config import PROVISION_GRACE_S
from .context import AppCtx
from .facility.endpoint_venv import WORKER_SAID
from .models import ShellOutcome, Submission

LoginRunner = Callable[[str], Awaitable[ShellOutcome]]

def _release_cmd(scheduler: str, eid: str) -> str:
    """Login-shape shell one-liner that cancels THIS endpoint's scheduler block(s), matched
    precisely by the `uep.<eid>` StdOut marker Parsl writes under the UEP dir. Scheduler-specific:
    Slurm reads squeue/scancel; PBS reads qstat -f (unwrapping its 80-col line continuations so a
    wrapped Output_Path can't split the marker) and qdel."""
    m = shlex.quote(f"uep.{eid}")
    if scheduler == "pbs":
        # NB: `qstat -f -u $USER` yields NOTHING on PBS Pro — the -u filter suppresses full-format
        # output entirely (unlike Slurm's `squeue -u`), which silently no-ops the cancel and lets the
        # block burn to walltime (caught in live Polaris validation). Use bare `qstat -f` (all jobs)
        # and let the endpoint-unique `uep.<eid>` marker scope the match to only our jobs.
        query, cancel = "qstat -f", "qdel"
        pick = ("sed ':a;N;$!ba;s/\\n\\t//g' "
                f"| awk -v m={m} 'BEGIN{{RS=\"Job Id: \"}} index($0,m){{print $1}}'")
    else:
        query, cancel = 'squeue -u "$USER" -h -O "JobID:30,StdOut:1024"', "scancel"
        pick = f"awk -v m={m} 'index($0,m){{print $1}}'"
    # A scheduler command that FAILS (slurmctld / pbs_server unreachable, a refused cancel) says so in its own line —
    # never the "released none" an empty listing piped through `2>/dev/null` used to print, which read as a confirmed
    # release (and now also feeds the `down` after a rejected submission and a pilot watch's end). Every id goes on
    # the one line (`$(echo $ids)`), so a release that cancels several says all of them.
    return (f'if q=$({query} 2>/dev/null); then ids=$(printf \'%s\\n\' "$q" | {pick}); '
            'if [ -z "$ids" ]; then echo "released none"; '
            f'elif {cancel} $ids; then echo "released $(echo $ids)"; '
            f'else rc=$?; echo "release failed: {cancel} $(echo $ids) exited $rc"; fi; '
            f'else echo "release failed: {query.split()[0]} could not be read"; fi')

def _release_cmd_for(app: AppCtx, eid: str) -> str:
    """The release one-liner for the bound facility's scheduler. The scheduler lives on the facility's MachineProfile
    (SlurmFacility.profile.scheduler); a facility without one (LocalFacility/dev, or test doubles) has never spoken
    anything but Slurm's squeue/scancel, so default there instead of assuming an attribute that isn't part of the
    Facility protocol."""
    return _release_cmd(getattr(getattr(app.facility, "profile", None), "scheduler", "slurm"), eid)

async def _release_blocks_over_login(
    app: AppCtx, eid: str, run_login: LoginRunner, *, expect_block: bool = False
) -> tuple[bool, str]:
    """Cancel this endpoint's scheduler block(s) by running the scheduler's cancel (scancel/qdel)
    on the **login shape (AMQP)** — never SSH. That's the whole point of the login-node endpoint:
    talk to the cluster over Compute, not a fresh SSH. Matches blocks precisely by the UEP StdOut
    marker (`uep.<eid>`) so it never touches another endpoint's jobs.

    A cold login worker can't dispatch on the first try — it returns cold_start ("allocating
    nodes…"), not `complete`. But that first hit WAKES the worker, so we retry a bounded few times
    to *confirm* the cancel instead of walking away while the block keeps burning. Returns
    `(confirmed, detail)`: `confirmed=False` means the channel stayed cold across the retries and the
    cancel was NOT verified — the caller must report that honestly (never "down"; see #24). An
    unconfirmed cancel is still backstopped by idle-release (`min_blocks=0` + `max_idletime`), and
    re-calling stop (channel now warming) confirms it. Retry budget: HPC_BRIDGE_RELEASE_ATTEMPTS
    (default 3) × HPC_BRIDGE_RELEASE_BACKOFF_S (default 6s)."""
    cmd = _release_cmd_for(app, eid)
    # `expect_block`: a block was requested but never confirmed running (stop-during-provisioning). Its sbatch may not
    # be in the scheduler yet, so a scancel that finds nothing ("released none") has NOT confirmed the block gone — the
    # pilot can land a moment later and burn (the spend_revoked race). Keep polling for it to appear (a longer budget),
    # and only count a run that actually CANCELLED a job as confirmed. Without a block expected, "released none" is a
    # genuine confirm (nothing was ever there).
    attempts = config.provisioning_release_attempts() if expect_block else config.release_attempts()
    backoff = config.release_backoff_s()
    detail = "unconfirmed"
    for i in range(attempts):
        out = await run_login(cmd)
        if out.phase == "complete" and out.exit_code == 0:
            lines = (out.stdout or "").strip().splitlines()
            last = (lines[-1] if lines else "").strip()
            if not last.startswith("released "):
                # a scheduler command failed (the line says which), or nothing came back: not a confirm
                detail = last or "the release printed nothing"
            elif last != "released none" or not expect_block:
                return True, last  # cancelled a job, or none was expected — the cancel is confirmed
            else:
                detail = f"{_NO_PILOT_YET} (block still being submitted)"  # expect_block: keep polling for it
        else:
            detail = out.notice or out.phase or "unconfirmed"
        if i + 1 < attempts and backoff > 0:
            await asyncio.sleep(backoff)  # let the woken login worker register / the sbatch land, then re-confirm
    return False, f"cancel not confirmed ({detail}); idle-release will reclaim it"

# The unconfirmed release's detail when its last look DID read the scheduler (no pilot of ours in it), not a cold
# channel: a stop tells the two apart by it (server._stop_endpoint).
_NO_PILOT_YET = "no pilot in the scheduler yet"


def _released_jobs(detail: str) -> list[str]:
    """The jobs a CONFIRMED release cancelled, from its detail (`released <ids>`); [] for `released none`."""
    words = detail.split()
    if words[:1] == ["released"]:
        words = words[1:]
    return [] if words == ["none"] else words

# A finished pilot's own job may say why it died: hpc-bridge's env_setup line prints one `hpc-bridge: endpoint venv …`
# line when the venv does not run on the compute node. The rows commands emit `HPCB_ERR <jobid> <stderr path>` for each
# pilot that FINISHED with an exit status of its own (Slurm: sacct's SubmitLine names the script, parsl writes
# `<script>.stderr`; PBS: the record's Error_Path), and this loop turns them into `HPCB_SAID <jobid> <line>` — scoped to
# that job, so a later pilot (another partition) is never told what an earlier one said.
_SAID_LOOP = (
    "while IFS= read -r l; do case $l in (\"HPCB_ERR \"*) set -- $l; "
    "s=$(sed -n '/^hpc-bridge: /p' \"$3\" 2>/dev/null | tail -n 1); [ -n \"$s\" ] && echo \"HPCB_SAID $2 $s\";; "
    # a PBS pilot's creation time as qstat prints it, as epoch seconds (GNU date; elsewhere it is left out, unknown)
    "(\"HPCB_QT \"*) set -- $l; j=$2; shift 2; t=$(date -d \"$*\" +%s 2>/dev/null) && echo \"HPCB_SUB $j $t\";; "
    "(*) echo \"$l\";; esac; done; true"
)


def _pilot_status_cmd(scheduler: str, eid: str) -> str:
    """Login-shape one-liner that prints THIS endpoint's pilot block(s) as `STATE JOBID` lines,
    matched by the same `uep.<eid>` StdOut marker `_release_cmd` uses (so it never reads another
    endpoint's jobs). Read-only — the diagnostic twin of `_release_cmd`. Empty output ⇒ no pilot is
    in the scheduler (submission rejected, or not yet registered). `HPCB_SAID <jobid> <line>` rows carry a finished
    pilot's own `hpc-bridge: …` line (`_SAID_LOOP`)."""
    return "{ " + _pilot_rows_cmd(scheduler, eid) + "; } | " + _SAID_LOOP


def _pilot_rows_cmd(scheduler: str, eid: str) -> str:
    # Around the rows: `HPCB_NOW <epoch>` (the login node's clock, so a pilot's submit time can be compared with a
    # duration measured here — no clock skew), `HPCB_SUB <job> <epoch>` per finished pilot (when it was submitted), and
    # `HPCB_FAIL <command>` for a scheduler query that FAILED — never an empty listing that reads as "no pilot".
    m = shlex.quote(f"uep.{eid}")
    now = 'echo "HPCB_NOW $(date +%s)"; '
    if scheduler == "pbs":
        # Bare `qstat -f` (the -u filter suppresses full-format output on PBS Pro); unwrap the 80-col
        # line continuations, split on records, and for records carrying the marker print the
        # job_state letter (R/Q/H) + the job id.
        return (
            # -x: finished jobs too — a pilot that RAN AND DIED (a broken worker_init, a missing module) is otherwise
            # invisible and read as "never submitted"; the summary prefers a live state when both are present.
            now + "if q=$(qstat -x -f 2>/dev/null); then printf '%s\\n' \"$q\" | sed ':a;N;$!ba;s/\\n\\t//g' "
            # …plus the scheduler's `comment` for the record: a HELD pilot's comment is the site's own explanation
            # (a Polaris-style hook: "requires -l filesystems=…"), which the agent otherwise has to dig out of qstat -f.
            f"| awk -v m={m} 'BEGIN{{RS=\"Job Id: \"}} index($0,m){{"
            's="?"; if (match($0,/job_state = [A-Za-z]/)) s=substr($0,RSTART+12,1); '
            'x="-"; if (match($0,/Exit_status = -?[0-9]+/)) x=substr($0,RSTART+14,RLENGTH-14); '
            'c=""; if (match($0,/comment = [^\\n]*/)) c=substr($0,RSTART+10,RLENGTH-10); '
            "print s\" \"$1\" \"x\" \"c; "
            # a finished pilot's creation time, as qstat prints it (`_SAID_LOOP` turns it into HPCB_SUB)
            'if (s ~ /^[FEX]$/ && match($0,/ctime = [^\\n]*/)) print "HPCB_QT "$1" "substr($0,RSTART+8,RLENGTH-8); '
            # where a pilot that ran and died wrote its stderr (Error_Path is host:path)
            'if (s ~ /^[FEX]$/ && x != "-" && match($0,/Error_Path = [^\\n]*/)) '
            '{p=substr($0,RSTART+13,RLENGTH-13); sub(/^[^:]*:/,"",p); print "HPCB_ERR "$1" "p}}\'; '
            'else echo "HPCB_FAIL qstat"; fi; true'
        )
    # Live pilots from squeue — `STATE JOBID - REASON` (the PENDING reason tells a normal queue wait from a job the
    # scheduler will never start: PartitionTimeLimit, AssocMaxJobsLimit, JobHeldUser…) — plus FINISHED pilots from
    # accounting (sacct's SubmitLine carries the script path, hence the marker) as `F JOBID EXIT STATE`, so a pilot
    # that ran and died is not read as "never submitted" (the PBS twin, 0.1.9). EXIT is `-` for what is not a
    # diagnosis: CANCELLED (our own release / re-bind), TIMEOUT (the walltime), COMPLETED with 0 (a worker that
    # simply ended). Filter by the marker INSIDE awk (not `grep -F | awk`): grep exits non-zero on no-match, which
    # under a `set -o pipefail` shell would mask an empty result as an error and swallow the "no pilot" signal.
    # sacct is best-effort (no accounting daemon ⇒ `HPCB_FAIL sacct` and no rows). Submit comes LAST, after SubmitLine,
    # as epoch seconds (SLURM_TIME_FORMAT is an strftime format).
    return (
        now + 'if q=$(squeue -u "$USER" -h -O "State:20,JobID:24,Reason:60,StdOut:1024" 2>/dev/null); '
        f"then printf '%s\\n' \"$q\" | awk -v m={m} 'index($0,m){{print $1\" \"$2\" - \"$3}}'; "
        'else echo "HPCB_FAIL squeue"; fi; '
        'if a=$(SLURM_TIME_FORMAT=%s sacct -X -n -P -u "$USER" -S now-6hours -o State,JobID,ExitCode,SubmitLine,Submit '
        "2>/dev/null); "
        f"then printf '%s\\n' \"$a\" | awk -F'|' -v m={m} 'index($4,m){{st=$1; sub(/ .*/,\"\",st); "
        'if (st ~ /^(PENDING|RUNNING|COMPLETING|CONFIGURING|SUSPENDED)$/) next; '
        'split($3,e,":"); x=e[1]; '
        'if (st ~ /^(CANCELLED|TIMEOUT|PREEMPTED|NODE_FAIL|DEADLINE|REQUEUED|REVOKED)$/) x="-"; '
        'if (st == "COMPLETED" && x == "0") x="-"; '
        "print \"F \"$2\" \"x\" \"st; "
        'if ($5 ~ /^[0-9]+$/) print "HPCB_SUB "$2" "$5; '
        # where a pilot that ran and died wrote its stderr: parsl's `<script>.stderr`, the script being the last word
        # of SubmitLine
        'if (x != "-") {n=split($4,w," "); print "HPCB_ERR "$2" "w[n]".stderr"}}\'; '
        'else echo "HPCB_FAIL sacct"; fi; true'
    )

# Slurm PENDING reasons that mean "never, as submitted" (Slurm leaves such a job PENDING forever unless
# EnforcePartLimits rejects it at submit) — the Slurm analogue of a PBS hold.
_NEVER_PENDING = re.compile(
    r"PartitionTimeLimit|PartitionNodeLimit|PartitionConfig|QOS\w*Limit|Assoc\w*Limit|JobHeld|BadConstraints|"
    r"InvalidAccount|InvalidQOS|ReqNodeNotAvail|DependencyNeverSatisfied|AccountNotAllowed|PartitionDown", re.I)

# A finished row's state: PBS's F/E/X (with -x), and the `F` the Slurm rows command prints for sacct's finished pilots.
_DONE_STATES = frozenset("FEX")


def _pilot_rows(stdout: str) -> list[list[str]]:
    """`_pilot_status_cmd`'s pilot rows, split (its HPCB_ lines left out). Slurm rows are `STATE JOBID - REASON` (live)
    or `F JOBID EXIT STATE` (finished); PBS rows are `STATE JOBID EXIT [comment…]` (EXIT = Exit_status, or `-` when the
    job never ran)."""
    return [ln.split(None, 3) for ln in stdout.splitlines() if ln.strip() and not ln.startswith("HPCB_")]


def _died(rows: list[list[str]]) -> list[list[str]]:
    """The pilots that FINISHED with an exit status of their own: the worker died there. Finished rows are read by WHY
    they finished: `-` (deleted before it ever ran — a held pilot cancelled by a re-bind) or 271 (killed/qdel'd: our
    own release, or the walltime) is a leftover of an earlier block, not a diagnosis, and is left out."""
    return [r for r in rows if r and r[0][:1].upper() in _DONE_STATES and len(r) > 2 and r[2] not in ("-", "271", "?")]


def _summarize_pilot(stdout: str, provisioning_elapsed_s: float, *, own_config: bool = True) -> tuple[str, str]:
    """(category, notice-suffix) from `_pilot_status_cmd` output. category ∈ {starting, queued, held,
    rejected, finished}. A visible pilot (Q/R/H) is reported at once; a MISSING pilot is only called
    `rejected` once the block has been cold past `PROVISION_GRACE_S` — before that it's a normal
    cold-start gap (empty suffix ⇒ the caller leaves 'allocating nodes…' unchanged). `own_config`: the facility's
    configuration is the user's own (a bring-your-own facility, changed with connect_facility(details=…)); a registry
    entry's scheduler directives and walltime cannot be changed per session."""
    said: dict[str, str] = {}
    for ln in stdout.splitlines():
        if ln.startswith("HPCB_SAID "):
            _tag, jid, line = (ln.split(None, 2) + ["", ""])[:3]
            said[jid] = line.strip()
    rows = _pilot_rows(stdout)
    live = [r for r in rows if r and r[0][:1].upper() not in _DONE_STATES]
    died = _died(rows)
    if not live:
        if died:  # every pilot this endpoint submitted has FINISHED with an exit status of its own: it ran and died
            r = died[-1]
            m = WORKER_SAID.match(said.get(r[1], ""))
            if m:  # the block said why itself: hpc-bridge's env_setup line, in its worker_init
                return "finished", f"— pilot {r[1]} already FINISHED (exit status {r[2]}): " + _venv_verdict(m)
            return "finished", (
                f"— pilot {r[1]} already FINISHED (exit status {r[2]}): the block started and its worker exited "
                "(a failed worker_init, an environment the compute node lacks, a network the worker cannot reach). "
                "Not a queue wait: read that job's stdout/stderr in the endpoint's submit_scripts directory "
                "(run_shell shape='login')."
            )
        if provisioning_elapsed_s < PROVISION_GRACE_S:
            return "starting", ""  # normal cold-start window — pilot not visible yet, don't cry wolf
        return "rejected", (
            f"— but NO pilot job is in the scheduler after ~{int(provisioning_elapsed_s)}s. The block "
            "submission was likely REJECTED (e.g. inactive allocation, wrong account, or bad queue) "
            "rather than queued. Check run_shell('qstat -u $USER', shape='login') (squeue on Slurm) "
            "and the endpoint log."
        )
    return _summarize_live(live, provisioning_elapsed_s, own_config=own_config)


def _venv_verdict(m: re.Match[str]) -> str:
    """What a pilot's `hpc-bridge: endpoint venv … (built on A) does not run on <node> (B)` means, and what helps. On
    another architecture than the login node a rebuild cannot help: the venv is built on the login node again."""
    built, arch, host = m.group("built"), m.group("arch"), m.group("host")
    head = f"its job said {m.group(0)[:300]!r}. Not a queue wait, and a new block will fail the same way: "
    if built and built != "unknown" and built != arch:
        return head + (
            f"the endpoint's venv is built on the login node ({built}) and these compute nodes ({arch}) cannot run "
            "it. Do NOT change env_setup (connect_facility details): no env_setup changes the architecture the venv "
            "was built for, the running endpoint keeps the one it was started with, and an emptied or rewritten one "
            "only hides the endpoint's own CLI from later calls. Rebuilding does not help either (it is built on the "
            f"login node again). What works: a partition whose nodes are {built}; or this facility's own multi-user "
            "endpoint, if list_facilities has one for it (its workers bring their own environment). A venv per "
            "architecture is planned, not built (see 'does not run on' in docs/user/troubleshooting.md).")
    return head + (
        f"the venv runs on the login node but not on {host}. Tear the endpoint down and connect again (the bring-up "
        "rebuilds it if it is broken on the login node too); if it still fails, the compute nodes lack something the "
        "login node has.")


def _summarize_live(rows: list[list[str]], provisioning_elapsed_s: float, *, own_config: bool = True) -> tuple[str, str]:  # noqa: E501
    """`_summarize_pilot` for the live rows (at least one)."""
    states = {r[0][:1].upper() for r in rows}
    jid = rows[0][1] if len(rows[0]) > 1 else "?"
    if "H" in states:
        held = next((r for r in rows if r[0][:1].upper() == "H"), rows[0])
        comment = held[3].strip() if len(held) > 3 else ""
        why = (f" The scheduler's comment: {comment[:300]!r}." if comment else
               " A held job usually means a bad scheduler directive (e.g. filesystems/account) — inspect qstat -f / "
               "the #PBS|#SBATCH directives.")
        hjid = held[1] if len(held) > 1 else jid
        fix = ("Fix the facility's scheduler_options (connect_facility with details=) or the account/queue, then start "
               "again." if own_config else
               "The scheduler directives come from the registry entry, which hpc-bridge cannot change per session yet: "
               "if the account or queue is the cause, change it (ensure_endpoint_up(account=…, partition=…, "
               "confirm_spend=True)); otherwise tell the user the facility needs a directive this entry does not send.")
        return "held", f"— pilot {hjid} is HELD and will not start on its own.{why} {fix}"
    if "R" in states:
        return "starting", f"— pilot {jid} is RUNNING; the worker is starting, retry shortly."
    pend = next((r for r in rows if r[0][:1].upper() == "P"), rows[0])
    reason = pend[3].strip() if len(pend) > 3 else ""
    pjid = pend[1] if len(pend) > 1 else jid
    if reason and _NEVER_PENDING.search(reason):
        fix = ("Fix the facility's walltime/partition/account (connect_facility with details=), then start again."
               if own_config else
               "Change the partition or account if that is the cause (ensure_endpoint_up(account=…, partition=…, "
               "confirm_spend=True)); a walltime or size comes from the registry entry, which hpc-bridge cannot change "
               "per session yet: tell the user.")
        return "held", (
            f"— pilot {pjid} is PENDING with reason {reason!r}: the scheduler will not start it as submitted (a "
            f"walltime or size over the partition's or QOS's limit, an account/QOS it may not use, a held job). {fix}"
        )
    tail = f" (reason {reason})" if reason and reason.lower() != "none" else ""
    return "queued", f"— pilot {pjid} is queued (PENDING{tail}); waiting on the scheduler."

class PilotProbe(NamedTuple):
    """One read of THIS endpoint's pilots (`_pilot_status_cmd`): the summary a provisioning notice carries, every pilot
    job listed (live or finished), each that FINISHED with an exit status of its own as (job, exit status) in the
    listing's order, the live ones, and the scheduler queries that FAILED (`HPCB_FAIL`) — so the server can record the
    pilot a FINISHED verdict names, and a stop can tell a pilot the endpoint submitted since from one it already knew
    (server._stop_endpoint). A failed live listing reads `unknown`: it says nothing about the pilot."""

    category: str
    suffix: str
    jobs: frozenset[str]
    died: tuple[tuple[str, str], ...]
    live: frozenset[str] = frozenset()
    failed: frozenset[str] = frozenset()

    @property
    def dead(self) -> tuple[str, str] | None:
        """(job, exit status) of the pilot a FINISHED verdict names — the newest that died, as `_summarize_pilot` names
        it; None for any other verdict."""
        return self.died[-1] if self.category == "finished" and self.died else None


# A finished pilot submitted this long before the confirmation it is read against still counts as this block's: the
# confirmation is measured here and the submit time on the login node, so the slack absorbs the probe's round trip.
# A pilot of an earlier session (sacct keeps 6 h, PBS's history about two weeks) is minutes to days older.
_SUBMIT_SLACK_S = 60.0


async def _probe_pilots_over_login(app: AppCtx, eid: str, elapsed_s: float, run_login: LoginRunner, *,
                                   since_s: float | None = None) -> PilotProbe | None:
    """Ask the scheduler (over the login shape — AMQP, no SSH) what state THIS endpoint's pilots are in. Best-effort:
    None when it can't tell (login worker cold, scheduler unreachable). `elapsed_s` is how long the block has been
    provisioning — it gates the rejection hint past the cold-start grace. `since_s`: how long ago the block's spend was
    confirmed; a pilot that FINISHED and was submitted before that (an earlier session's, still in the scheduler's
    history) is not this block's and is left out of the verdict. A pilot whose submit time is unknown counts."""
    scheduler = getattr(getattr(app.facility, "profile", None), "scheduler", "slurm")
    out = await run_login(_pilot_status_cmd(scheduler, eid))
    if out.phase != "complete" or out.exit_code != 0:
        return None
    machine = getattr(app, "machine", None)  # bring-your-own (a session facility): its config is the user's to change
    own = machine is not None and machine in getattr(app, "session_facilities", {})
    stdout = out.stdout or ""
    meta = [ln.split() for ln in stdout.splitlines() if ln.startswith(("HPCB_NOW ", "HPCB_SUB ", "HPCB_FAIL "))]
    failed = frozenset(w[1] for w in meta if w[0] == "HPCB_FAIL" and len(w) > 1)
    rows = _pilot_rows(stdout)
    old: set[str] = set()
    now = next((float(w[1]) for w in meta if w[0] == "HPCB_NOW" and len(w) > 1 and w[1].isdigit()), None)
    if since_s is not None and now is not None:
        cutoff = now - since_s - _SUBMIT_SLACK_S
        old = {w[1] for w in meta if w[0] == "HPCB_SUB" and len(w) > 2 and w[2].isdigit() and float(w[2]) < cutoff}
    # this block's finished pilots only; every live row stays (a live pilot is never an earlier session's leftover)
    kept = "\n".join(ln for ln in stdout.splitlines()
                     if not ((r := ln.split(None, 3)) and len(r) > 1 and r[1] in old
                             and r[0][:1].upper() in _DONE_STATES and not ln.startswith("HPCB_")))
    live = frozenset(r[1] for r in rows if len(r) > 1 and r[0][:1].upper() not in _DONE_STATES)
    if failed & {"squeue", "qstat"}:  # the live listing failed: no verdict, never "no pilot"
        category, suffix = "unknown", ""
    else:
        category, suffix = _summarize_pilot(kept, elapsed_s, own_config=own)
    return PilotProbe(category, suffix, frozenset(r[1] for r in rows if len(r) > 1),
                      tuple((r[1], r[2]) for r in _died(_pilot_rows(kept))), live=live, failed=failed)

async def _pilot_status_over_login(app: AppCtx, eid: str, elapsed_s: float, run_login: LoginRunner) -> tuple[str, str] | None:  # noqa: E501
    """(category, notice-suffix) of THIS endpoint's pilot (`_probe_pilots_over_login`); None when it can't tell, so the
    caller leaves its notice unchanged."""
    probe = await _probe_pilots_over_login(app, eid, elapsed_s, run_login)
    return (probe.category, probe.suffix) if probe is not None and probe.category != "unknown" else None

async def _augment_provisioning(app: AppCtx, eid: str, notice: str, elapsed_s: float, run_login: LoginRunner, *,
                                since_s: float | None = None) -> tuple[str, PilotProbe | None]:
    """Enrich a still-cold BILLED block's 'allocating nodes…' with the pilot's ACTUAL scheduler state,
    so a rejected/held submission isn't silently indistinguishable from a queue wait ([#32]), and hand back the probe
    (None: it could not tell) for the server to record a pilot that ran and died (`since_s`: since the confirmation,
    `_probe_pilots_over_login`). A diagnostic must never break the result it annotates, so any failure — or an empty
    suffix (the normal cold-start window) — leaves the notice as-is."""
    try:
        probe = await _probe_pilots_over_login(app, eid, elapsed_s, run_login, since_s=since_s)
    except Exception:  # noqa: BLE001 - the pilot probe is advisory; never fail provisioning on it
        return notice, None
    suffix = probe.suffix if probe else ""
    note = getattr(app.facility, "worker_init_note", "")
    if probe and probe.category == "finished" and isinstance(note, str) and note:
        # a worker that died on a REUSED endpoint whose env_setup was edited: its blocks run the endpoint's own one, so
        # editing it again changes nothing — say so (only for a pilot that ran and died, not for a queue wait)
        suffix += " " + note.rstrip()
    return (f"{notice} {suffix}" if suffix else notice), probe

async def _augment_provisioning_notice(app: AppCtx, eid: str, notice: str, elapsed_s: float, run_login: LoginRunner) -> str:  # noqa: E501
    """`_augment_provisioning`'s notice alone."""
    return (await _augment_provisioning(app, eid, notice, elapsed_s, run_login))[0]


# --- a scheduler SUBMISSION in the agent's own command ------------------------------------------------------------
# A job submitted by hand — from the login shape (free, so the spend floor never sees what runs there) or from inside a
# compute block (a NEW job beside the block) — bills the user's allocation like a block, and hpc-bridge neither tracks
# nor cancels it. `srun` inside a block is a step of the block's own job, not a new one.
_SUBMIT_COMMANDS = frozenset({"sbatch", "srun", "salloc", "qsub"})
# Options that only print (or, --test-only, validate without submitting), read only BEFORE the first positional word:
# after it (`srun -N1 python --version`, `sbatch job.sh --help`) they belong to the program or the script. `-h`/`-V` are
# Slurm's alone: on PBS, `qsub -h` HOLDS the job it submits and `qsub -V` exports the environment into it.
_SLURM_INFO = frozenset({"--help", "-h", "--usage", "--version", "-V", "--test-only"})
_INFO_FLAGS = {"sbatch": _SLURM_INFO, "srun": _SLURM_INFO, "salloc": _SLURM_INFO,
               "qsub": frozenset({"--help", "--version"})}
# Options whose value is the NEXT word, so that word is not the first positional (`-p debug`, `--account lab`). A long
# option not listed is read as a flag; `--opt=value` carries its own value.
_SLURM_VALUE_SHORT = frozenset("AaBbCcDdeFGiJLMmNnopqSTtwx")
_SLURM_VALUE_LONG = frozenset({
    "--account", "--array", "--begin", "--chdir", "--clusters", "--comment", "--constraint", "--cpus-per-task",
    "--dependency", "--error", "--exclude", "--export", "--gpus", "--gpus-per-node", "--gres", "--input", "--job-name",
    "--jobid", "--licenses", "--mail-type", "--mail-user", "--mem", "--mem-per-cpu", "--nodelist", "--nodes",
    "--ntasks", "--ntasks-per-node", "--output", "--partition", "--qos", "--reservation", "--signal", "--time",
    "--time-min", "--wrap"})
_PBS_VALUE_SHORT = frozenset("AacCdDeJjklMmNoPpqrRSuvW")
_NO_LONG: frozenset[str] = frozenset()
# Words after which the next word is still in command position (`if sbatch …`, `do qsub …`).
_LEADING_KEYWORDS = frozenset({"if", "then", "else", "elif", "do", "while", "until", "!", "{"})
# Wrappers that run their arguments as the command, with their options that take a separate value.
_WRAPPERS: dict[str, frozenset[str]] = {
    "nohup": frozenset(), "exec": frozenset({"-a"}), "command": frozenset(), "setsid": frozenset(),
    "env": frozenset({"-u", "-C", "-S"}), "nice": frozenset({"-n"}), "stdbuf": frozenset({"-i", "-o", "-e"}),
    "time": frozenset({"-f", "-o"}), "timeout": frozenset({"-s", "-k"}),
    "xargs": frozenset({"-a", "-d", "-E", "-I", "-L", "-n", "-P", "-s"}),
    "sudo": frozenset({"-u", "-g", "-C", "-D", "-h", "-p", "-r", "-t", "-U", "-T"}),
}
_SHELLS = frozenset({"sh", "bash", "dash", "ksh", "zsh"})
_FIND_EXEC = frozenset({"-exec", "-execdir", "-ok", "-okdir"})
_ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=")
# A redirection word (`2>/dev/null`, `>log`, `2>&1`, `<in`); group 1 is its target when attached.
_REDIRECT = re.compile(r"\d*(?:&>>?|>>?&?|<&?|<>|>\|)(.*)", re.S)
_DASH_C = re.compile(r"-[A-Za-z]*c[A-Za-z]*")
_HEREDOC_OP = re.compile(r"<<(-?)[ \t]*\\?(['\"]?)([A-Za-z_][\w.-]*)\2")
_PLAIN = re.compile(r"[^\\'\"`$<>;|()&\s#]+")   # a run of ordinary word characters: the lexer's fast path
_DQ_PLAIN = re.compile(r"[^\"\\$`]+")
_MAX_DEPTH = 6   # nested substitutions / `bash -c` levels read; anything deeper is not analysed
_Token = tuple[bool, str]   # (is an operator, text): a word with its quotes removed, or ; & | ( ) or a newline


def _subst_end(text: str, j: int) -> int:
    """Index of the character that closes the command substitution opening at `j` (`$(` or a backtick), or len. A
    parenthesis inside quotes or escaped does not count: `$(echo ")")`, `$(printf '%s' 'a)b')`."""
    n = len(text)
    if text[j] == "`":
        k = j + 1
        while k < n and text[k] != "`":
            k += 2 if text[k] == "\\" else 1
        return min(k, n)
    depth, k = 0, j + 1
    while k < n:
        c = text[k]
        if c == "\\":
            k += 2
            continue
        if c == "'":
            q = text.find("'", k + 1)
            k = n if q < 0 else q + 1
            continue
        if c == '"':
            k += 1
            while k < n and text[k] != '"':
                k += 2 if text[k] == "\\" else 1
            k += 1
            continue
        if c in "()":
            depth += 1 if c == "(" else -1
            if depth == 0:
                return k
        k += 1
    return n


def _lex(text: str, subs: list[str]) -> list[_Token]:
    """Tokens of a command line, in one linear pass. Comments and here-document bodies (a job script being WRITTEN)
    are dropped — a `<<EOF` inside quotes or a comment is not one; command substitutions ($(…) and backticks, quoted
    or not) are cut out into `subs`, leaving an inert `_` in the word."""
    toks: list[_Token] = []
    word: list[str] | None = None
    heredocs: list[tuple[str, bool]] = []   # (delimiter, strip leading tabs) — bodies start after this line
    i, n = 0, len(text)

    def add(part: str) -> None:
        nonlocal word
        if word is None:
            word = []
        word.append(part)

    def end_word() -> None:
        nonlocal word
        if word is not None:
            toks.append((False, "".join(word)))
            word = None

    def subst(j: int) -> int:
        k = _subst_end(text, j)
        subs.append(text[j + (2 if text[j] == "$" else 1) : k])
        add("_")
        return k + 1

    while i < n:
        c = text[i]
        if m := _PLAIN.match(text, i):
            add(m.group(0))
            i = m.end()
        elif c == "\\":
            if text[i + 1 : i + 2] != "\n":  # a backslash-newline joins the lines; any other escape is literal
                add(text[i + 1 : i + 2])
            i += 2
        elif c == "'":
            k = text.find("'", i + 1)
            k = n if k < 0 else k
            add(text[i + 1 : k])
            i = k + 1
        elif c == '"':
            add("")
            i += 1
            while i < n and text[i] != '"':
                if m := _DQ_PLAIN.match(text, i):
                    add(m.group(0))
                    i = m.end()
                elif text[i] == "\\":
                    add(text[i + 1 : i + 2])
                    i += 2
                elif text.startswith("$(", i) or text[i] == "`":
                    i = subst(i)
                else:
                    add(text[i])
                    i += 1
            i += 1
        elif text.startswith("$(", i) or c == "`":
            i = subst(i)
        elif c == "#" and word is None:  # a comment runs to the end of its line
            k = text.find("\n", i)
            i = n if k < 0 else k
        elif text.startswith("<<", i) and not text.startswith("<<<", i) and text[i - 1 : i] != "<":
            if m := _HEREDOC_OP.match(text, i):
                end_word()
                heredocs.append((m.group(3), m.group(1) == "-"))
                i = m.end()
            else:
                add("<<")
                i += 2
        elif c == "\n":
            end_word()
            toks.append((True, "\n"))
            i += 1
            for delim, tabs in heredocs:  # skip each body, through its delimiter line
                while i < n:
                    k = text.find("\n", i)
                    k = n if k < 0 else k
                    line, i = text[i:k], k + 1
                    if (line.lstrip("\t") if tabs else line) == delim:
                        break
            heredocs = []
        elif c in " \t\r":
            end_word()
            i += 1
        elif c in ";|()" or (c == "&" and text[i - 1 : i] not in (">", "<") and text[i + 1 : i + 2] != ">"):
            end_word()  # a command boundary (`>&2`, `&>log` are redirections)
            toks.append((True, c))
            i += 1
        else:
            add(c)
            i += 1
    end_word()
    return toks


def _group(toks: list[_Token]) -> list[list[str]]:
    """The simple commands among the tokens — leaving out the body of a function DEFINED here (`f() { sbatch …; }`
    runs nothing until it is called; a later call is not followed) and the patterns of a `case` (`*/qsub) …` after
    `;;` is a pattern, not a command)."""
    cmds: list[list[str]] = []
    cur: list[str] = []
    body = 0             # brace depth inside a function body
    fn_pending = False   # `name ( )` seen: the next `{` opens its body
    cases = 0            # open `case … esac`
    pattern = False      # the words up to the next `)` are a case pattern
    prev: _Token | None = None
    skip = 0
    for k, (is_op, t) in enumerate(toks):
        if skip:
            skip -= 1
        elif is_op:
            named = len(cur) == 1 or (len(cur) == 2 and cur[0] == "function")
            header = bool(cur) and cur[0] == "case"
            if t == "(" and named and not pattern and k + 1 < len(toks) and toks[k + 1] == (True, ")"):
                fn_pending, cur, skip = True, [], 1
            elif t == ")" and (pattern or header):   # a pattern ends (the `case … in` header may share its line)
                cur, pattern = [], False
            elif not (t == "(" and pattern and not cur):   # `(slurm)`: a pattern's optional opening paren
                if cur and not body and not pattern:
                    cmds.append(cur)
                if header or (cases and t in (";", "&") and prev == (True, ";")):
                    pattern = True                   # after `case … in`, and after `;;` / `;&`, comes a pattern
                cur = []
        elif t == "{" and (fn_pending or (len(cur) == 2 and cur[0] == "function")):
            body, fn_pending, cur = body + 1, False, []
        else:
            if not cur and body:
                body += {"{": 1, "}": -1}.get(t, 0)
            if not cur and t == "case":
                cases += 1
            elif not cur and t == "esac" and cases:
                cases, pattern = cases - 1, False
            fn_pending = False
            cur.append(t)
        prev = (is_op, t)
    if cur and not body and not pattern:
        cmds.append(cur)
    return cmds


def _simple_commands(command: str, depth: int = 0) -> list[list[str]]:
    """Each simple command `command` runs, as words with the quotes removed — command substitutions included."""
    if depth > _MAX_DEPTH:
        return []
    subs: list[str] = []
    cmds = _group(_lex(command, subs))
    for sub in subs:
        cmds += _simple_commands(sub, depth + 1)
    return cmds


def _past_wrapper(words: list[str], k: int, wrapper: str) -> int:
    """Index of the command a wrapper runs: past its options (and the value of each that takes one), env/sudo's
    VAR=value and timeout's DURATION."""
    takes, duration = _WRAPPERS[wrapper], wrapper == "timeout"
    while k < len(words):
        w = words[k]
        if w == "--":
            return k + 1
        if w.startswith("-") and len(w) > 1:
            k += 2 if w in takes else 1
        elif wrapper in ("env", "sudo") and _ASSIGNMENT.match(w):
            k += 1
        elif duration:
            duration, k = False, k + 1
        else:
            return k
    return k


def _leading_options(command: str, args: list[str]) -> list[str]:
    """A scheduler command's own options: those before its first positional word (the script or program)."""
    short, long_ = (_PBS_VALUE_SHORT, _NO_LONG) if command == "qsub" else (_SLURM_VALUE_SHORT, _SLURM_VALUE_LONG)
    opts: list[str] = []
    k = 0
    while k < len(args):
        a = args[k]
        if (r := _REDIRECT.fullmatch(a)) is not None:
            k += 1 if r.group(1) else 2
            continue
        if a == "--" or not a.startswith("-") or a == "-":
            break
        opts.append(a)
        k += 2 if (len(a) == 2 and a[1] in short) or a in long_ else 1
    return opts


def _starts_job(command: str, args: list[str], inside_job: bool) -> bool:
    """Whether `command args…` starts a NEW job: not a help/version/--test-only query, and not an `srun` step in an
    allocation that already exists (the block's own job, or one named with --jobid)."""
    if command == "srun" and inside_job:
        return False
    opts = _leading_options(command, args)
    if any(o in _INFO_FLAGS[command] for o in opts):
        return False
    return not (command == "srun" and any(o == "--jobid" or o.startswith("--jobid=") for o in opts))


class _Call(NamedTuple):
    name: str          # sbatch / qsub / salloc / srun
    args: list[str]
    direct: bool       # its exit code is the line's: not under `timeout`/`xargs`, not inside `bash -c`/`eval`/`find`


# Wrappers whose exit code is not the wrapped command's: `timeout` (124 on a timeout), `xargs` (123 for any failure).
_OWN_EXIT_HIDDEN = frozenset({"timeout", "xargs"})


def _submit_call(words: list[str], depth: int, inside_job: bool) -> _Call | None:
    """The submit command one simple command runs, with its arguments — past keywords, VAR=value, redirections and
    wrappers; into `bash -c '…'`, `eval` and `find -exec`."""
    k, direct = 0, True
    while k < len(words):
        w, base = words[k], words[k].rsplit("/", 1)[-1]
        if w in _LEADING_KEYWORDS or _ASSIGNMENT.match(w):
            k += 1
        elif (r := _REDIRECT.fullmatch(w)) is not None:
            k += 1 if r.group(1) else 2
        elif base in _WRAPPERS:
            own = itertools.takewhile(lambda a: a.startswith("-"), words[k + 1 :])
            if base == "command" and {"-v", "-V"} & set(own):
                return None  # `command -v sbatch` looks the name up; it runs nothing
            direct = direct and base not in _OWN_EXIT_HIDDEN
            k = _past_wrapper(words, k + 1, base)
        else:
            break
    if k >= len(words):
        return None
    name, args = words[k].rsplit("/", 1)[-1], words[k + 1 :]
    if name in _SUBMIT_COMMANDS:
        return _Call(name, args, direct) if _starts_job(name, args, inside_job) else None
    if depth >= _MAX_DEPTH:
        return None
    if name in _SHELLS:  # `bash -lc 'module load slurm; sbatch job.sh'`
        script = next((args[j + 1] for j, a in enumerate(args[:-1]) if _DASH_C.fullmatch(a)), None)
        inner = _first_call(script, depth + 1, inside_job) if script else None
        return inner._replace(direct=False) if inner else None
    if name == "eval":
        inner = _first_call(" ".join(args), depth + 1, inside_job)
        return inner._replace(direct=False) if inner else None
    if name == "find":  # `find . -name '*.sh' -exec sbatch {} \;`
        for j, a in enumerate(args):
            if a in _FIND_EXEC:
                end = next((e for e in range(j + 1, len(args)) if args[e] in (";", "+")), len(args))
                if found := _submit_call(args[j + 1 : end], depth + 1, inside_job):
                    return found._replace(direct=False)
    return None


def _first_call(command: str, depth: int, inside_job: bool) -> _Call | None:
    for words in _simple_commands(command, depth):
        if found := _submit_call(words, depth, inside_job):
            return found
    return None


def _scheduler_submission(command: str, *, inside_job: bool = False, depth: int = 0) -> str | None:
    """The scheduler command — sbatch, srun, salloc or qsub — with which `command` STARTS a new job, or None.
    `inside_job`: the command runs inside a compute block, where `srun` is a step of the block's own job. Read only at
    a command position, never in a comment, a quoted string, a here-document body or a function body defined here,
    and not for a help/version/--test-only query of the scheduler's own. Linear in the command's length. Precision
    first: a submission hidden in a script the command runs, behind a variable (`$SB job.sh`), in another language
    (`python -c`) or over `ssh` is missed; a mention (`man sbatch`, `grep sbatch`, `command -v sbatch`) never counts."""
    found = _first_call(command, depth, inside_job)
    return found[0] if found else None


def _waits(command: str, args: list[str]) -> bool:
    """`sbatch --wait` / `qsub -W block=true` wait for the JOB: their exit code is the job's, not the submission's."""
    opts = _leading_options(command, args)
    if command == "qsub":
        return any(o.startswith("-W") for o in opts) and any("block=true" in a for a in args)
    return command == "sbatch" and ("--wait" in opts or "-W" in opts)


def _submission(command: str, *, inside_job: bool = False) -> Submission | None:
    """The scheduler job `command` starts by hand (see `_scheduler_submission`), how many submit calls the line makes,
    and whether the line's exit code is that one submission's own answer: the line's LAST command, its only submit,
    run directly (not under `timeout`/`xargs`/`bash -c`), not waiting for the job (then the exit code is the job's)."""
    calls = [c for words in _simple_commands(command) if (c := _submit_call(words, 0, inside_job)) is not None]
    if not calls:
        return None
    top = _group(_lex(command, []))
    last = _submit_call(top[-1], 0, inside_job) if top else None
    own_exit = len(calls) == 1 and last is not None and last.direct and not _waits(last.name, last.args)
    return Submission(calls[0].name, len(calls), own_exit)


# A scheduler's receipt for an accepted job: Slurm's "Submitted batch job N" (a bare id with --parsable), a PBS job id.
_RECEIPT = re.compile(r"Submitted batch job \d+|^\s*\d+(?:;\S+)?\s*$|^\s*\d+(?:\[\d*\])?\.[\w.-]+\s*$", re.M)


def _submission_refused(sub: Submission, exit_code: int | None, stdout: str) -> bool:
    """May the result say the scheduler REFUSED the job (none was created)? Only when the line's exit code is the one
    sbatch/qsub call's own answer (`Submission.own_exit`), it is non-zero, and no job receipt was printed — a loop or a
    dependency chain whose last submit failed, a failing check after a submit, or a `timeout` may still have queued
    jobs."""
    return (sub.command in ("sbatch", "qsub") and sub.own_exit and exit_code not in (0, None)
            and not _RECEIPT.search(stdout or ""))


