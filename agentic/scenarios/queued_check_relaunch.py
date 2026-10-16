"""queued_check_relaunch — does `stop_endpoint` answer `down` while hpc-bridge's own CHECK is still queued on the endpoint,
so that the endpoint submits a NEW billed pilot right after hpc-bridge said spend stopped? A suspected spend-honesty bug
(review, 2026-10-15) to be PROVEN live before any design. Scripted (HPCB_OPERATOR=script, harness/script_runner.py): no
agent, one exact sequence. Fake cluster, Slurm: the `default` profile, and `site`.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh queued_check_relaunch

THE CLAIM (main at 7ae46c2):
- `ensure_endpoint_up(confirm_spend=True)` makes the endpoint start a block by sending it a check: the canary, a real
  Globus Compute task (warmth._confirm_worker). The block is cold, the check gives up after CANARY_TIMEOUT_S (8 s), the
  call answers `provisioning`, and the canary stays QUEUED on the endpoint (warmth._check_queued).
- `stop_endpoint` on an SSH endpoint cancels the pilot over the login shape (scancel by the `uep.<eid>` marker,
  scheduler_ops._release_cmd) and answers `down`, `spend_stopped=True`: the canary is no task handle of ours, so nothing
  holds the stop (server._block_work).
- But a Globus Compute task cannot be cancelled once registered (the SDK's Executor.shutdown docstring); parsl's
  HTEX.outstanding() counts it; and a cancelled job reads CANCELLED (Slurm) or COMPLETED (PBS), which does not trip
  parsl's error handler. So parsl's strategy (parsl/jobs/strategy.py, case 2b: no active block, work outstanding)
  submits a NEW pilot once it next reads the scheduler: SlurmProvider reads it at most every 60 s, and the strategy runs
  every 5 s (`strategy_period: 5`, facility/remote.py), so within about 60–65 s of the cancel. That block would run the
  canary, then idle until max_idletime (600 s on this endpoint: Profile.max_idletime_s) before scaling in — billing
  while hpc-bridge says it stopped.
- Field evidence: HANDOFF.md, 2026-10-06 — Delta's endpoint kept relaunching A40 blocks for queued canaries (about
  1.7 GPU-h, stopped by hand). Never reproduced on the fake cluster before this cell.

EXPECTED on main if the claim holds: FAILED — `world:no_relaunch_after_down` names a new pilot of this endpoint submitted
about 60–65 s after the `down`, and the universal `world:stop_honesty_no_pilot_left` finds it still queued. This is a
regression check that should pass only once the product fixes the hazard.

WHAT THE FIRST LIVE RUN SHOWED (bundle 1792117501-3824-56199, fake/default, 2026-10-15): no relaunch — but not for the
claim's reason, and its watch was blind (below). The pilot (job 1, held) was cancelled by the stop's `down`; 56 s later,
at parsl's next job-status poll, the user endpoint's own log says: "Job is marked as MISSING since the workers failed
to register to the executor" → TooManyJobFailuresError → "Setting bad state" → "Not strategizing for executor because
bad state", and the user endpoint shut down — consistent with parsl's HTEX rewriting a finished block that never
connected to MISSING, and its error handler's threshold being max(1, init_blocks) = 1 here (batch mode: init_blocks 0),
so ONE cancelled, never-connected block puts the executor in bad state. The canary is then left queued at the service for a user endpoint that has exited.
`strategy_after_stop` now records those lines from the endpoint log in every run, so the outcome is read, not inferred.

THE SCRIPT (SCRIPT below; the `{facility}`/`{ssh_host}` of every BYO scenario):
1. connect_facility(fid, ssh_host) → the proposal; connect_facility(fid, ssh_host, details=<the proposal>, env_setup
   left out as the tool asks, plus the HOLD below); then connect_facility(fid, ssh_host) again until `needs_account`:
   the login shape is up.
2. ensure_endpoint_up(shape="compute", confirm_spend=True) — with the first non-GPU allocation connect listed (`site`'s
   mybalance; `default` lists none and needs none).
3. Poll the queue on the free login shape (`squeue`, matched by the `uep.<eid>` marker, as _release_cmd matches) until a
   pilot of this endpoint is in it — never an ensure_endpoint_up poll: one that found the block up would run the canary.
4. stop_endpoint() at once. Its status, `spend_stopped` and notice are recorded (`stop_answer`).
5. No teardown: it would end the manager, and parsl's loop with it, hiding the very relaunch this cell watches for.

THE HOLD (why every pilot is held PENDING): on idle fake nodes a pilot never waits. In stop_while_running's codex bundle
(1792105266-23823-81296, fake/default, 2026-10-15) the user endpoint scaled out 6 s after it started; ensure_endpoint_up
returned 2.7 s after the sbatch already saying "pilot 2 is RUNNING; the worker is starting", and the canary was answered
within about 5 s of the sbatch (the next ensure_endpoint_up said `up` at once). A squeue poll and a stop are each a
login-shape round trip (1–2 s), so the worker could run the canary first and leave nothing queued. So the script adds
`scheduler_options: "#SBATCH --begin=now+<hold>"` to the confirmed details: Slurm holds every pilot of this endpoint
PENDING (reason BeginTime), no worker starts, the canary stays queued BY CONSTRUCTION, and the relaunched pilot (same
template) is held too, so the cell occupies no node. A pending pilot is what a busy facility gives anyway, and parsl
reads a cancelled PENDING block as it reads a cancelled RUNNING one whose workers have not connected yet — and while the
canary is queued none has (a connected, idle worker takes it at once), so the hold does not change which path parsl
takes. HPCB_RELAUNCH_HOLD_S sets the hold (default 1800 s,
at least the watch + 900 s); 0 drops it — the script then stops the moment the job id appears (most likely RUNNING), and
`canary_before_stop` says whether the canary could have run first (a premise that cannot be shown fails the cell).

THE OBSERVATION (world checks, after the script, BEFORE the harness' teardown — run.py runs WORLD_CHECKS ahead of the
POSTCHECKS and the teardown, whose `gce stop` would end parsl's loop):
- the ADMIN TAP (harness/admin_tap.py) samples the scheduler as root on slurmctld every 10 s for the whole cell — the
  harness' admin channel, never hpc-bridge: `squeue -t all` (queued and running jobs, and those finished within
  MinJobAge, 300 s here), `sacct` over the last 2 h in EVERY state (`-s …`: without it sacct lists only jobs that were
  eligible in the window, and a held pilot never is — the first run's sacct saw nothing), and — once `relaunch_watch`
  asks for the cell's own pilot by id — `sacct -j <id>`, which finds a job whatever its eligibility or age;
- `relaunch_watch` waits until the tap has covered HPCB_RELAUNCH_WATCH_S (default 420 s: several 60 s job-status polls)
  after the stop's answer. Every job of this endpoint (its `uep.<eid>` marker in squeue's batch script or sacct's
  SubmitLine) is recorded with its submit time, state and id;
- then it reads the endpoint's own logs over the world channel (cluster_ops.capture_logs_cmd: the manager's, each user
  endpoint's, the blocks' output) — before the teardown deletes them — for parsl's decisions after the stop.
Then the teardown runs as usual (it cancels a relaunched pilot by the same marker), so nothing is left on the fake cluster.

GRADED: `world:no_relaunch_after_down` (gates, as every world row): FAIL when the stop answered `down` (or
spend_stopped=True) and a new pilot of this endpoint was submitted after it — naming the job, its submit time relative to
the stop's answer and its state. A stop that answered `draining` (or refused) is a different premise: report-only,
PASS, said so. A `down` with no relaunch passes only when the watch could have seen one: the tap saw this cell's OWN
pilot by its marker (else "watch blind" — the first run passed blind: its pilot lived 4.6 s between two samples), the
endpoint's log shows no scale-out the tap missed, the window was covered, and the canary was demonstrably still queued at
the stop. REPORTED: `stop_answer`, `pilot_state_at_stop`, `canary_before_stop`, `relaunch_watch`, `strategy_after_stop`
(the endpoint log's scale-outs, error-handler and user-endpoint lines after the stop).
"""
import asyncio
import json
import os
import re
import shlex
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from admin_tap import SlurmJob, slurm_ids_cmd, slurm_sample_cmd
from cluster_ops import capture_logs_cmd
from invariants import Result, ToolCall, Trace

OPERATORS = ("script",)        # a scripted proof cell: an agent operator is refused (run.py: SKIPPED)
TARGETS = ("fake",)            # the admin tap needs the fake cluster's admin channel
REQUIRES = {"scheduler": "slurm", "mfa": "none"}   # squeue/sacct, and an autonomous BYO bring-up (no one-time code)
KIND = "regression"
SUMMARY = ("spend honesty: stop_endpoint answers `down` while the canary is still queued — does the endpoint relaunch a "
           "pilot after it? (scripted; the admin tap watches the queue)")
TAGS = ["fake", "stop", "spend", "canary", "scripted", "admin"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1                  # no agent: the script runs once
TEARDOWN = "delete"            # AFTER the world checks (run.py): the watch must see parsl's loop still running
POSTCHECK_DELAY_S = 10


def _env_s(name: str, default: float) -> float:
    try:
        return max(0.0, float(os.environ.get(name, "") or default))
    except ValueError:
        return default


# 420 s: several of parsl's 60 s job-status polls plus its 5 s strategy period after the stop (the first live run's
# executor acted 56 s after it)
WATCH_S = _env_s("HPCB_RELAUNCH_WATCH_S", 420.0)
PILOT_WAIT_S = _env_s("HPCB_RELAUNCH_PILOT_WAIT_S", 180.0)   # how long step 3 waits for a pilot to be in the queue
LOGIN_UP_WAIT_S = 300.0                                       # how long step 1 waits for the login shape


def _hold_s() -> int:
    """The `--begin` hold on every pilot (see THE HOLD): HPCB_RELAUNCH_HOLD_S, default max(1800, watch + 900); 0 = none."""
    raw = os.environ.get("HPCB_RELAUNCH_HOLD_S", "").strip()
    if raw:
        try:
            return max(0, int(float(raw)))
        except ValueError:
            pass
    return int(max(1800.0, WATCH_S + 900.0))


# Held, no pilot ever starts (no node); without the hold the pilot and its relaunch run for the whole watch.
NEEDS_COMPUTE_NODE = _hold_s() == 0
# The admin tap (run_smoke.sh starts it before the cell; harness/admin_tap.py): the pool user's queue and history as
# root on slurmctld, every 10 s, with the controller's clock.
ADMIN_TAP = {"every_s": 10, "cmd": slurm_sample_cmd(), "ids_cmd": slurm_ids_cmd()}

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "bring up the login shape from the probe's proposal (every pilot held PENDING by an #SBATCH --begin hold), start a "
    "compute block with confirm_spend=True, wait on the login shape until its pilot is in the queue, and stop the "
    "endpoint at once. No teardown: the harness watches the scheduler as its admin for a relaunched pilot."
)

_PILOT = re.compile(r"^HPCB_PILOT (\S+) (\S+)(?: (.*))?$", re.M)
_RELEASED = re.compile(r"released ([0-9][0-9_ ]*)")


def pilot_poll_cmd(eid: str) -> str:
    """The login-shape poll: this endpoint's pilots as `HPCB_PILOT <job> <state> <reason>` lines (squeue's batch script
    carries the `uep.<eid>` marker, the one scheduler_ops._release_cmd matches the StdOut path on). Exits 0 on no match."""
    marker = shlex.quote(f"uep.{eid}")
    return ("squeue -u \"$USER\" -h -o '%i|%T|%r|%o' 2>/dev/null "
            f"| awk -F'|' -v m={marker} 'index($4,m){{print \"HPCB_PILOT \"$1\" \"$2\" \"$3}}'; true")


def _pilots(c: ToolCall) -> list[tuple[str, str, str]]:
    """(job, state, reason) of each pilot a poll saw."""
    return [(m.group(1), m.group(2), (m.group(3) or "").strip()) for m in _PILOT.finditer(str((c.result or {}).get("stdout") or ""))]


def _proposal_details(r: dict, hold_s: int) -> dict:
    """The proposal as the confirming details=: env_setup left out (the server keeps the proposed one — the tool's own
    instruction), unset fields dropped, and the HOLD added."""
    details = {k: v for k, v in dict(r.get("proposed_details") or {}).items() if v is not None and k != "env_setup"}
    if hold_s:   # after any directive the proposal carried (one per line, as the field says)
        details["scheduler_options"] = "\n".join(x for x in (details.get("scheduler_options"),
                                                             f"#SBATCH --begin=now+{hold_s}") if x)
    return details


def _account(r: dict) -> str | None:
    """The allocation to charge: the first non-GPU one connect listed (site: hpcb), else none (default lists none)."""
    accounts = [str(a.get("account")) for a in (r.get("allocations") or []) if isinstance(a, dict) and a.get("account")]
    return next((a for a in accounts if "gpu" not in a.lower()), accounts[0] if accounts else None)


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    fid, host, hold = ops.facility, ops.ssh_host, _hold_s()
    # 1. the login shape, from the probe's proposal
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if r.get("phase") != "proposed_facility_details":
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {str(r.get('notice') or r.get('text'))[:200]!r}")
    r = await ops.call("connect_facility", facility=fid, ssh_host=host, details=_proposal_details(r, hold))
    deadline = time.monotonic() + LOGIN_UP_WAIT_S
    while r.get("phase") != "needs_account":
        if r.get("phase") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the login shape did not come up: phase={r.get('phase')!r} "
                      f"{str(r.get('notice') or r.get('text'))[:200]!r}")
        await ops.sleep(5)
        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    ops.say(f"login shape up; every pilot is held: {'#SBATCH --begin=now+' + str(hold) if hold else 'no hold'}")
    # 2. the billed block, confirmed: its canary goes out and (the block cold) stays queued
    args: dict = {"shape": "compute", "confirm_spend": True}
    if (account := _account(r)) is not None:
        args["account"] = account
    e = await ops.call("ensure_endpoint_up", **args)
    eid = str(e.get("endpoint_id") or "")
    if e.get("status") not in ("provisioning", "up") or not eid:
        ops.abort(f"no block was started: status={e.get('status')!r} {str(e.get('notice') or e.get('text'))[:200]!r}")
    # 3. the queue, on the free login shape, until this endpoint's pilot is in it
    deadline = time.monotonic() + PILOT_WAIT_S
    while True:
        q = await ops.call("run_shell", shape="login", command=pilot_poll_cmd(eid))
        pilots = _pilots(ToolCall("run_shell", {}, q))
        if pilots or time.monotonic() > deadline:
            break
        await ops.sleep(1)
    if not pilots:
        ops.abort(f"no pilot of uep.{eid} entered the queue within {PILOT_WAIT_S:.0f} s (last poll: "
                  f"{str(q.get('stdout') or q.get('notice') or q.get('text'))[:200]!r})")
    seen = ", ".join(f"{j} {s}" + (f" ({why})" if why and why != "None" else "") for j, s, why in pilots)
    # 4. stop at once
    s = await ops.call("stop_endpoint")
    ops.say(f"pilot(s) at the stop: {seen}. stop_endpoint answered status={s.get('status')!r}, "
            f"spend_stopped={s.get('spend_stopped')!r}: {str(s.get('notice') or s.get('text'))[:300]}")
    # 5. no teardown: the harness watches the queue first, then tears down
    return (f"stopped with {seen} in the queue; stop_endpoint said {s.get('status')!r} "
            f"(spend_stopped={s.get('spend_stopped')!r}). Not torn down: the world check watches for a relaunch.")


# ---- what the trace says ----------------------------------------------------------------------------------------------

def _stop(t: Trace) -> tuple[int, ToolCall] | None:
    stops = t.named("stop_endpoint")
    return stops[0] if stops else None


def _claims_stopped(c: ToolCall) -> bool:
    r = c.result or {}
    return str(r.get("status")) == "down" or r.get("spend_stopped") is True


def _released(c: ToolCall) -> set[str]:
    """The job ids the stop's own notice says it cancelled (`released 7 8`)."""
    m = _RELEASED.search(str((c.result or {}).get("notice") or ""))
    return set(m.group(1).split()) if m else set()


def _last_poll(t: Trace, before: int) -> tuple[int, list[tuple[str, str, str]]] | None:
    """The last login-shape squeue poll before call `before` that saw a pilot."""
    for i, c in reversed(t.named("run_shell")):
        if i < before and (p := _pilots(c)):
            return i, p
    return None


def _seen_before(t: Trace, before: int) -> set[str]:
    return {j for i, c in t.named("run_shell") if i < before for j, _, _ in _pilots(c)}


def canary_state(t: Trace) -> tuple[str, str]:
    """Was hpc-bridge's check still queued when the stop came? `queued` (every pilot PENDING at the last poll before the
    stop, and no worker had answered: none could have run it), `ran` (a worker answered before the stop: an
    ensure_endpoint_up said up/warm, or compute work completed), or `unknown` (a pilot was RUNNING: its worker may have
    run it first) — with why."""
    stop = _stop(t)
    before = stop[0] if stop else len(t.calls)
    for i, c in t.named("ensure_endpoint_up"):
        r = c.result or {}
        if i < before and str(c.input.get("shape") or "compute") == "compute" and (
                str(r.get("status")) == "up" or str(r.get("block_state")) == "warm"):
            return "ran", f"ensure_endpoint_up (call {i}) answered status={r.get('status')!r}: a worker answered the check"
    for i, c in t.named("run_shell"):
        if i < before and str(c.input.get("shape") or "compute") == "compute" and (c.result or {}).get("phase") == "complete":
            return "ran", f"compute work completed (call {i}): a worker was up"
    last = _last_poll(t, before)
    if last is None:
        return "unknown", "no squeue poll before the stop saw a pilot of this endpoint"
    i, pilots = last
    desc = ", ".join(f"job {j} {s}" + (f" ({w})" if w and w != "None" else "") for j, s, w in pilots)
    if all(s == "PENDING" for _, s, _ in pilots):
        return "queued", f"{desc} at the last poll before the stop (call {i}): no worker had started, so the check was queued"
    return "unknown", f"{desc} at the last poll before the stop (call {i}): its worker may have run the check first"


def stop_answer(t: Trace) -> Result:
    """REPORTED: the stop's full answer."""
    stop = _stop(t)
    if stop is None:
        return Result("stop_answer", False, "no stop_endpoint call")
    i, c = stop
    keep = ("status", "spend_stopped", "block_state", "block_job_id", "endpoint_id", "session_spend", "notice", "text",
            "is_error")
    return Result("stop_answer", True, f"call {i}: " + json.dumps({k: v for k, v in (c.result or {}).items() if k in keep},
                                                                    default=str))


def pilot_state_at_stop(t: Trace) -> Result:
    """REPORTED: what the last squeue poll before the stop saw of this endpoint's pilot(s)."""
    stop = _stop(t)
    last = _last_poll(t, stop[0] if stop else len(t.calls))
    if last is None:
        return Result("pilot_state_at_stop", False, "no squeue poll saw a pilot of this endpoint before the stop")
    i, pilots = last
    return Result("pilot_state_at_stop", True, f"call {i}: " + ", ".join(
        f"job {j} {s}" + (f" ({w})" if w and w != "None" else "") for j, s, w in pilots))


def canary_before_stop(t: Trace) -> Result:
    """REPORTED: whether the canary could have run before the stop (canary_state)."""
    state, why = canary_state(t)
    return Result("canary_before_stop", state == "queued", f"{state}: {why}")


# ---- what the scheduler says (the admin tap) ---------------------------------------------------------------------------

NO_RELAUNCH = "world:no_relaunch_after_down"


@dataclass
class LogLine:
    at: float          # epoch seconds (the login node's clock; its logs are UTC)
    where: str         # "manager", "uep 9f7c4349", …
    text: str          # the line, with its continuation lines (a traceback's message) folded in


@dataclass
class Watch:
    """What the admin tap saw of this endpoint around the stop, and what the endpoint's own log says. Times are the
    controller's clock (the jail's and the login node's too: one Docker VM kernel)."""
    called_at: float | None = None      # the stop call started
    answered_at: float | None = None    # it answered
    window_s: float = WATCH_S
    covered_until: float | None = None  # the controller clock of the latest sample read
    samples: int = 0
    jobs: list[SlurmJob] = field(default_factory=list)   # this endpoint's jobs (by its marker), queue and history merged
    unverifiable: str | None = None
    own_ids: set[str] = field(default_factory=set)       # the cell's own pilot(s): the script's poll + the stop's release
    seen_unmarked: set[str] = field(default_factory=set)  # own ids the tap saw WITHOUT this endpoint's marker
    log_lines: list[LogLine] | None = None                # strategy lines after the stop (None: the log was not read)
    log_error: str = ""


def _stop_times(messages: list) -> tuple[float | None, float | None]:
    """(started, answered) of the run's first stop_endpoint, in epoch seconds: the server journal's own stamps (`ts`,
    `ms`), else the script's step record."""
    for m in messages or []:
        row = m.get("journal") if isinstance(m, dict) else None
        if isinstance(row, dict) and row.get("tool") == "stop_endpoint" and isinstance(row.get("ts"), int | float):
            return float(row["ts"]), float(row["ts"]) + float(row.get("ms") or 0) / 1000.0
    for m in messages or []:
        n = m.get("native") if isinstance(m, dict) else None
        if isinstance(n, dict) and n.get("name") == "stop_endpoint" and n.get("t_end") is not None:
            return n.get("t_start"), float(n["t_end"])
    return None, None


def own_pilots(t: Trace) -> set[str]:
    """The cell's own pilot job ids: what the script's squeue polls saw before the stop, and what the stop released."""
    stop = _stop(t)
    if stop is None:
        return set()
    return _released(stop[1]) | _seen_before(t, stop[0])


def endpoint_jobs(samples: list, eids: list[str]) -> list[SlurmJob]:
    """Every job of these endpoints the samples show (`uep.<eid>` in the batch script / SubmitLine), merged by id: its
    first submit time and sighting, and its LATEST state — samples are in time order, and within one the queue (the live
    view) has the last word over the history, which lists a job still queued too."""
    marks = [f"uep.{e}" for e in eids if e]
    by: dict[str, SlurmJob] = {}
    for s in samples:
        for j in [*s.history, *s.queue]:
            if not any(m in j.command for m in marks):
                continue
            have = by.get(j.job_id)
            if have is None:
                by[j.job_id] = SlurmJob(**vars(j))
                continue
            have.state = j.state
            have.reason = j.reason or have.reason
            have.name = j.name or have.name
            have.submit = have.submit if have.submit is not None else j.submit
            seen = [x for x in (have.seen_at, j.seen_at) if x is not None]
            have.seen_at = min(seen) if seen else None
            have.start, have.end = j.start or have.start, j.end or have.end
            if j.source not in have.source:
                have.source = f"{have.source}+{j.source}"
    return sorted(by.values(), key=lambda j: (j.submit or 0.0, j.job_id))


def relaunches(t: Trace, w: Watch) -> list[SlurmJob]:
    """Pilots of this endpoint the stop did not account for: not among those it released or the script saw before it,
    and submitted once the stop had started (a whole-second Slurm stamp, so 1 s of slack) — or, with no submit time, first
    seen after its answer."""
    stop = _stop(t)
    if stop is None or w.answered_at is None:
        return []
    known = own_pilots(t)
    since = (w.called_at if w.called_at is not None else w.answered_at) - 1.0
    return [j for j in w.jobs if j.job_id not in known and (
        (j.submit is not None and j.submit >= since) or (j.submit is None and (j.seen_at or 0) >= w.answered_at))]


def _describe(j: SlurmJob, w: Watch) -> str:
    at = (f"submitted {j.submit - w.answered_at:+.0f} s after the stop answered" if j.submit is not None and w.answered_at
          else f"first seen {(j.seen_at or 0) - (w.answered_at or 0):+.0f} s after the stop answered")
    why = f" ({j.reason})" if j.reason and j.reason != "None" else ""
    return f"job {j.job_id} {at}, {j.state}{why}"


# ---- the endpoint's own log: parsl's strategy decisions after the stop -------------------------------------------------

# A parsl / Globus Compute log line: `2026-10-16 02:27:55,056648 ERROR MainProcess-443 …` (UTC on the fake login node:
# run 2's scale-out at 02:26:54 sits 8 s after its ensure_endpoint_up call in the journal's epoch).
_LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{1,6}) ")
_LOG_FILE = re.compile(r"^=== (\S+)")
# What decides a relaunch, as GCE 4.18 / parsl 2026.10 log it at INFO: the strategy's scale-out (`Scaling out by N
# blocks`, `Allocated block ID n`), its scale-in, the error handler (a cancelled block that never connected is rewritten
# to MISSING and, with init_blocks 0, puts the executor in bad state: `Not strategizing … because bad state`), the
# interchange's outstanding work, and the manager (re)starting or losing a user endpoint.
_STRATEGY = re.compile(
    r"Scaling (?:out|in) by \d+|Allocated block ID|[Ss]trategy case|Requesting \d+ more blocks?|[Ii]dle time|"
    r"[Ss]caling in|bad state|TooManyJobFailures|marked as MISSING|Not strategizing|[Oo]utstanding|"
    r"Creating new user endpoint|Command stopped|Endpoint shutdown|[Ss]ubmitted job|[Ll]aunched block")
_SCALE_OUT = re.compile(r"Scaling out by [1-9]\d* blocks?|Allocated block ID|Requesting [1-9]\d* more blocks?")


def _where(path: str) -> str:
    m = re.search(r"uep\.[0-9a-f-]{36}\.([0-9a-f]{8})", path)
    if "/submit_scripts/" in path:
        return "block " + path.rsplit("/", 1)[-1][:60]
    return f"uep {m.group(1)}" if m else "manager"


def strategy_lines(logs: str, since: float) -> list[LogLine]:
    """The strategy-relevant lines (`_STRATEGY`) of the endpoint logs (cluster_ops.capture_logs_cmd's `=== path`
    sections) stamped at or after `since` (epoch), in time order across the files, each with its untimestamped
    continuation lines folded in — a traceback's message says why the executor went to bad state."""
    out: list[LogLine] = []
    where, cur = "manager", None

    def keep(rec: LogLine | None) -> None:
        if rec is not None and rec.at >= since and _STRATEGY.search(rec.text):
            out.append(rec)

    for raw in logs.splitlines():
        if (f := _LOG_FILE.match(raw)) is not None:
            keep(cur)
            where, cur = _where(f.group(1)), None
            continue
        if (m := _LOG_TS.match(raw)) is not None:
            keep(cur)
            try:
                # timezone.utc, not datetime.UTC: scenario_knobs.py imports this module on the host's python3
                at = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()  # noqa: UP017
            except ValueError:
                cur = None
                continue
            cur = LogLine(at + int(m.group(2).ljust(6, "0")) / 1e6, where, raw[m.end():].strip())
        elif cur is not None and raw.strip() and cur.text.count(" ⏎ ") < 3:
            cur.text += " ⏎ " + raw.strip()
    keep(cur)
    return sorted(out, key=lambda x: x.at)   # the manager's log and each user endpoint's, interleaved in time


def _short_log(x: LogLine, w: Watch) -> str:
    # drop the logger's process / thread / module columns: `INFO MainProcess-443 JobStatusPoller-… parsl.x:207 fn msg`
    text = re.sub(r"^(\w+) MainProcess-\d+ \S+ \S+ \S+ ", r"\1 ", x.text)
    return f"{x.at - (w.answered_at or x.at):+.0f} s [{x.where}] {text[:220]}"


def strategy_after_stop(w: Watch) -> Result:
    """REPORTED: what the endpoint itself decided after the stop — parsl's scale-outs, the error handler, the user
    endpoint stopping or restarting — the direct evidence of a relaunch, or of why there was none."""
    if w.log_lines is None:
        return Result("strategy_after_stop", False, f"UNVERIFIABLE — the endpoint's logs were not read: "
                                                    f"{w.log_error or 'no stop to read them against'}")
    scale = [x for x in w.log_lines if _SCALE_OUT.search(x.text)]
    lead = ("SCALE-OUT after the stop" if scale else "no scale-out after the stop") + (
        f" ({len(w.log_lines)} strategy line(s)): " if w.log_lines else
        " — no strategy line at all after it (GCE logs a scale-out at INFO as 'Scaling out by N blocks')")
    return Result("strategy_after_stop", not scale, lead + " · ".join(_short_log(x, w) for x in w.log_lines[:14])
                  + (f" · … {len(w.log_lines) - 14} more" if len(w.log_lines) > 14 else ""))


# ---- the gated grader ---------------------------------------------------------------------------------------------------

def no_relaunch_after_down(t: Trace, w: Watch) -> Result:
    """GATED (a world row): a stop that answered `down` (or spend_stopped=True) must not be followed by a new pilot of
    this endpoint. A stop that did not claim spend stopped (`draining`, a refusal) is another premise: report-only, PASS,
    said so. A `down` with no relaunch passes only when the watch could have seen one — the tap saw this cell's OWN
    pilot by its marker (the first live run watched blind and passed: its pilot lived 4.6 s between two samples), the
    endpoint's log shows no scale-out the tap missed, and it covered the window — and the check was demonstrably still
    queued at the stop. Anything less proves nothing."""
    stop = _stop(t)
    if stop is None:
        return Result(NO_RELAUNCH, False, "premise not met: no stop_endpoint in the trace — the script never reached the "
                                          "stop (see the run's final message)")
    i, c = stop
    r = c.result or {}
    said = f"the stop (call {i}) answered status={r.get('status')!r}, spend_stopped={r.get('spend_stopped')!r}"
    new = relaunches(t, w)
    if not _claims_stopped(c):
        seen = ("; the endpoint DID submit pilot(s) after it: " + "; ".join(_describe(j, w) for j in new) if new
                else "; no new pilot was seen" if w.unverifiable is None else f"; the watch: {w.unverifiable}")
        return Result(NO_RELAUNCH, True, f"report-only: {said} — it did not claim spend stopped, so this cell's premise "
                                         f"(a `down` with the check still queued) does not hold{seen}")
    if new:
        return Result(NO_RELAUNCH, False, f"{said}, and the endpoint submitted a NEW pilot after it: "
                      + "; ".join(_describe(j, w) for j in new)
                      + f" — billing that hpc-bridge said had stopped ({canary_state(t)[1]})")
    scale = [x for x in (w.log_lines or []) if _SCALE_OUT.search(x.text)]
    if scale:
        return Result(NO_RELAUNCH, False, f"watch blind: {said}, and the endpoint's own log shows a scale-out after it "
                                          f"({_short_log(scale[0], w)}) that the admin tap never saw as a new pilot")
    if w.unverifiable:
        return Result(NO_RELAUNCH, False, f"UNVERIFIABLE — {w.unverifiable}")
    seen = {j.job_id: j for j in w.jobs if j.job_id in w.own_ids}
    if not w.own_ids:
        return Result(NO_RELAUNCH, False, "watch blind: no pilot id of this cell is known (no squeue poll saw one and the "
                                          "stop released none), so nothing shows the tap can see this endpoint's jobs")
    if not seen:
        unmarked = sorted(w.own_ids & w.seen_unmarked)
        return Result(NO_RELAUNCH, False, f"watch blind: the tap never saw this endpoint's own pilot (job "
                      f"{', '.join(sorted(w.own_ids))}) in {w.samples} sample(s), by squeue or sacct"
                      + (f" — it saw job {', '.join(unmarked)} but without the uep.<eid> marker in its script / "
                         "SubmitLine, so a relaunch would have gone unmatched too" if unmarked else "")
                      + " — so its silence after the stop proves nothing")
    covered = (w.covered_until or 0.0) - (w.answered_at or 0.0)
    if covered < w.window_s - 15:
        return Result(NO_RELAUNCH, False, f"UNVERIFIABLE — the admin tap covered only {covered:.0f} s of the "
                                          f"{w.window_s:.0f} s after the stop answered")
    state, why = canary_state(t)
    if state == "ran":
        return Result(NO_RELAUNCH, False, f"premise not met: {why} — no check of ours was queued at the stop, so 'no "
                                          "relaunch' proves nothing")
    if state != "queued":
        return Result(NO_RELAUNCH, False, f"premise unverified: {why} — 'no relaunch' proves nothing unless the check "
                                          "was still queued (keep the hold: HPCB_RELAUNCH_HOLD_S)")
    own = ", ".join(f"job {j.job_id} {j.state} [{j.source}]" for j in seen.values())
    return Result(NO_RELAUNCH, True, f"ok: {said}; no new pilot of this endpoint was submitted in the {covered:.0f} s "
                                     f"after it — the tap saw its own pilot ({own}), so it could have seen one ({why})")


def relaunch_report(w: Watch) -> Result:
    """REPORTED: every job of this endpoint the tap saw, with its submit time against the stop's answer, and the cover."""
    jobs = "; ".join(f"job {j.job_id} {j.state}{' (' + j.reason + ')' if j.reason and j.reason != 'None' else ''} "
                     + (f"submitted {j.submit - w.answered_at:+.0f} s" if j.submit is not None and w.answered_at else
                        "submit time unknown") + f" [{j.source}]" for j in w.jobs) or "none"
    cover = (f"{(w.covered_until or 0) - (w.answered_at or 0):.0f} s after the stop answered"
             if w.covered_until is not None and w.answered_at is not None else "no sample")
    own = ", ".join(sorted(w.own_ids)) or "none known"
    return Result("relaunch_watch", True, f"{w.samples} admin sample(s), covering {cover}; the cell's own pilot(s): {own}; "
                  f"this endpoint's jobs: {jobs}" + (f"; {w.unverifiable}" if w.unverifiable else ""))


def _endpoint_ids(t: Trace) -> list[str]:
    out: list[str] = []
    for c in t.calls:
        e = str((c.result or {}).get("endpoint_id") or "")
        if e and e not in out:
            out.append(e)
    return out


async def _read_logs(ctx, eids: list[str], w: Watch) -> None:
    """The endpoint's own logs (manager + user endpoints + blocks), read over the world channel at the END of the watch
    and BEFORE the harness' teardown deletes them; their strategy lines after the stop go into `w`."""
    ssh = getattr(ctx, "ssh", None)
    name = str(getattr(ctx, "endpoint_name", "") or "")
    if ssh is None or not name or w.called_at is None:
        w.log_error = "no world channel, endpoint name or stop time"
        return
    rc, logs = await asyncio.to_thread(ssh, capture_logs_cmd(name, eids), 120)
    if rc != 0 or "=== " not in logs:
        w.log_error = f"rc={rc}: {logs.strip()[-160:]!r}"
        return
    w.log_lines = strategy_lines(logs, w.called_at - 1.0)


async def relaunch_watch(ctx) -> list[Result]:
    """WORLD CHECK: ask the tap for the cell's own pilot by id, wait until it has covered WATCH_S after the stop's
    answer, read the endpoint's own log, then grade. `ctx`: run.py's WorldContext."""
    t: Trace = ctx.trace
    called, answered = _stop_times(ctx.messages)
    w = Watch(called_at=called, answered_at=answered, own_ids=own_pilots(t))
    eids = _endpoint_ids(t)
    if _stop(t) is None or answered is None:
        w.unverifiable = "nothing to watch: no stop_endpoint answer in the run"
    elif ctx.tap.path is None:
        w.unverifiable = ("no admin tap: HPCB_ADMIN_TAP is unset — run the cell through run_smoke.sh on the fake target, "
                          "which samples the scheduler through its admin channel")
    else:
        # by id as well (sacct -j): the window and squeue's MinJobAge may both miss a job that lived seconds
        asked = ctx.tap.request_ids(sorted(w.own_ids))
        left = answered + w.window_s - time.time()
        print(f"  relaunch watch: reading the admin tap until {w.window_s:.0f} s after the stop answered "
              f"(~{max(0.0, left):.0f} s more){'; asked for job ' + ', '.join(sorted(w.own_ids)) + ' by id' if asked else ''}",
              flush=True)
        samples = await ctx.tap.wait_until(answered + w.window_s, deadline_s=max(0.0, left) + 90.0)
        good = [s for s in samples if s.now is not None]
        w.samples = len(samples)
        w.covered_until = max((s.now for s in good), default=None)
        w.jobs = endpoint_jobs(good, eids)
        w.seen_unmarked = {j.job_id for s in good for j in [*s.queue, *s.history] if j.job_id in w.own_ids}
        if not any(s.now >= answered for s in good if s.now is not None):
            errors = [s.error for s in samples if s.error][-2:]
            w.unverifiable = (f"the admin tap has no sample after the stop answered ({len(samples)} sample(s)"
                              + (f"; {' | '.join(errors)}" if errors else "") + ")")
    await _read_logs(ctx, eids, w)
    return [no_relaunch_after_down(t, w), relaunch_report(w), strategy_after_stop(w)]


WORLD_CHECKS = [relaunch_watch]
EXTRA_INVARIANTS = [stop_answer, pilot_state_at_stop, canary_before_stop]
# world:no_relaunch_after_down gates as every world row does; agent_engaged, run_completed (the script finished — an
# abort is a premise unmet) and the safety floor always gate. The rest is the report.
EXPECT_OK = ["agent_engaged"]
