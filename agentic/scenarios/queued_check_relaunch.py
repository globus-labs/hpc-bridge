"""queued_check_relaunch — does `stop_endpoint` answer `down` while hpc-bridge's own CHECK is still queued on the endpoint,
so that the endpoint submits a NEW billed pilot right after hpc-bridge said spend stopped? A suspected spend-honesty bug
(review, 2026-10-15) — PROVEN live on 2026-10-15 (below): before the fix (#204) phase `second` failed on main, every
run; with it, phase B lands in a fresh compute user endpoint and the cell passes (live 2026-10-16). It is the fix's
regression check. Scripted (HPCB_OPERATOR=script, harness/script_runner.py): no
agent, one exact sequence. Fake cluster, Slurm: the `default` profile, and `site`. Two phases (HPCB_RELAUNCH_PHASE):

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh queued_check_relaunch
    HPCB_RELAUNCH_PHASE=first HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default \
        ./agentic/run_smoke.sh queued_check_relaunch

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

THE RESULT ON MAIN (fake/default, 2026-10-15):
- phase `second` — FAILED, the bug proven (run 1792120434): phase B's stop answered `down` ("released 4", spend_stopped
  True) while its check was queued; 53 s later the user endpoint logged "Scaling out by 1 blocks" and submitted job 5,
  a new pilot of the same endpoint (held by the tap, so it never ran). `world:no_relaunch_after_down` fails on it, and
  the universal `world:stop_honesty_no_pilot_left` finds it queued. This cell is the regression check for the fix: it
  passes only once a `down` is no longer followed by a relaunch.
- phase `first` — passes on main, by the bad-state path (run 1792117501, below): no relaunch, because the user endpoint
  shut down; the canary is left queued at the service.

WHAT THE FIRST LIVE RUN SHOWED (bundle 1792117501-3824-56199, fake/default, 2026-10-15; now phase `first`): no relaunch
— but not for the claim's reason, and its watch was blind (below). The pilot (job 1, held) was cancelled by the stop's
`down`; 56 s later, at parsl's next job-status poll, the user endpoint's own log says: "Job is marked as MISSING since
the workers failed to register to the executor" → TooManyJobFailuresError → "Setting bad state" → "Not strategizing for
executor because bad state", and the user endpoint shut down. The maintainer confirmed why in parsl 2026.04.20
(HighThroughputExecutor.status(), executor.py ~880–895): any TERMINAL job whose block never connected (and is not
SCALED_IN) is rewritten to MISSING, and simple_error_handler sets bad state when total ≥ max(1, init_blocks) AND every
job is FAILED/MISSING. So a cancelled FIRST pilot of a user endpoint trips bad state, fails the canary and ends it.

THE REMAINING CANDIDATE — phase `second` (the default): a user endpoint that ALREADY has a job that is not failed (an
earlier block that connected and was then cancelled by a stop: CANCELLED, connected, not rewritten) does not go to bad
state when its next, never-connected pilot is cancelled; with the canary still outstanding its strategy should scale out
again — which fits Delta's field incident. Phase A gives the user endpoint that job; phase B stops on its second pilot.

THE SCRIPT (SCRIPT below; the `{facility}`/`{ssh_host}` of every BYO scenario):
1. connect_facility(fid, ssh_host) → the proposal; connect_facility(fid, ssh_host, details=<the proposal>, env_setup
   left out as the tool asks, plus the phase's directives); then connect_facility(fid, ssh_host) again until
   `needs_account`: the login shape is up.
`first`:
2. ensure_endpoint_up(shape="compute", confirm_spend=True) — with the first non-GPU allocation connect listed (`site`'s
   mybalance; `default` lists none and needs none). Its canary goes out; the pilot is held (THE HOLD), so it stays queued.
3. Poll the queue on the free login shape (`squeue`, matched by the `uep.<eid>` marker, as _release_cmd matches) until a
   pilot of this endpoint is in it — never an ensure_endpoint_up poll: one that found the block up would run the canary.
4. stop_endpoint() at once (the judged stop).
`second`:
A. ensure_endpoint_up(confirm_spend=True), then ensure_endpoint_up polls until `up` (the block runs and its worker
   answers the canary: the job CONNECTED), run_shell(compute, `hostname`), stop_endpoint() — nothing of ours outstanding,
   so an honest `down` (`phase_a_stop`; re-stopped while `draining`, as an agent would).
B. THE SENTINEL: every pilot of this endpoint is named `parsl-hpcb-qcr` with `#SBATCH --dependency=singleton` (in the
   confirmed details, so phase A's ran too — nothing else of that name existed); phase B first starts, as the pool user on
   the login shape, a one-core job of the same name on the pilots' partition, account and walltime (terms the scheduler
   already took for them: `site`'s 30-min `debug` cannot refuse it), so the next pilot waits on it (reason Dependency) —
   an idle fake node starts a job within a second of its sbatch, faster than any hold could follow. Then the script asks
   the ADMIN TAP to `scontrol hold` every new PENDING pilot of `uep.<eid>` from the sentinel's id on (`<tap>.hold`), so
   it never runs whatever ends. Then ensure_endpoint_up(confirm_spend=True) with the SAME terms: hpc-bridge dropped the
   compute shape at phase A's stop, so this builds a new runtime seeded from the facility defaults plus the session's
   partition/account choice (warmth._seed_uec: `compute_choice` outlives a stop) — the same user_endpoint_config, so the
   manager hands the canary to the same user endpoint (checked, not assumed: the `uep.<eid>.<uep id>` dir of both pilots'
   batch scripts). Poll until its pilot (block 2) is queued; stop_endpoint() at once (the judged stop).
Then no teardown: it would end the manager, and parsl's loop with it, hiding the very relaunch this cell watches for.

THE HOLD (phase `first`: why every pilot is held PENDING): on idle fake nodes a pilot never waits. In stop_while_running's codex bundle
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
`canary_before_stop` says whether the canary could have run first (a premise that cannot be shown fails the cell). Phase
`first` only: phase `second` cannot hold every pilot this way — phase A's block must run.

THE OBSERVATION (world checks, after the script, BEFORE the harness' teardown — run.py runs WORLD_CHECKS ahead of the
POSTCHECKS and the teardown, whose `gce stop` would end parsl's loop; the watch starts at the JUDGED stop):
- the ADMIN TAP (harness/admin_tap.py) samples the scheduler as root on slurmctld every 10 s for the whole cell — the
  harness' admin channel, never hpc-bridge: `squeue -t all` (queued and running jobs, and those finished within
  MinJobAge, 300 s here) — what sees a PENDING relaunch — and `sacct` from the tap's own start to now (`-S … -E now
  -s CA,CD,F,PD,R,TO`: without the state list sacct lists only jobs that were eligible in the window, and a held pilot
  never is; with it but no `-E` it lists nothing — slurm 23.11.4), which lists the jobs that have ENDED (a held pilot
  only once cancelled: in both phase-`second` bundles the held relaunch was in squeue in 38 samples, in sacct once),
  each query ending with its own exit status; and — once `relaunch_watch` asks for the stop's own pilot by id — `sacct -j <id>`, under
  its own tag: it says what became of that job, but a lookup by a known id could never see a relaunch, so it is never
  the sighting that makes a watch not blind;
- `relaunch_watch` waits until the tap has covered HPCB_RELAUNCH_WATCH_S (default 420 s: several 60 s job-status polls)
  after the stop's answer. Every job of this endpoint (its `uep.<eid>` marker in squeue's batch script or sacct's
  SubmitLine) is recorded with its submit time, state and id;
- then it reads the endpoint's own logs over the world channel (cluster_ops.capture_logs_cmd: the manager's, each user
  endpoint's, the blocks' output) — before the teardown deletes them — for parsl's decisions after the stop.
- phase `second`: once the watch is over the world check holds any pilot submitted since the tap's last tick (one
  last tick, as the pool user), cancels the sentinel and waits until it has left the queue; a relaunched pilot stays
  held. (A COMPLETING sentinel once read as a pilot left behind to the universal postcheck; that check now waits out a
  job Slurm lists as CG, up to the target's bound, so this wait is partly redundant — it stays so the world check says
  itself whether the sentinel went, `HPCB_SENTINEL gone|STILL <state>`.)
Then the teardown runs as usual — its cancel-by-marker (`cluster_ops.scoped_cancel_cmd`: squeue with no state filter, so
PENDING and held jobs too) cancels a relaunched pilot — and CLEANUP cancels anything of the singleton name, so nothing is
left on the fake cluster.

GRADED: `world:no_relaunch_after_down` (gates, as every world row) judges the JUDGED stop (phase B's in `second`): FAIL
when it answered `down` (or spend_stopped=True) and a new pilot of this endpoint was submitted after it — naming the
job, its submit time relative to the stop's answer and its state. A stop that answered `draining` (or refused) is a
different premise: report-only, PASS, said so. A `down` with no relaunch passes only when the watch could have seen one:
a CLEAN sample's squeue (or, for a pilot that has ended, its window sacct) listed the stop's OWN pilot by its marker (else "watch blind" — the first run
passed blind: its pilot lived 4.6 s between two samples; a sample with an error line, a failed query, a missing exit
status or no clock covers no time), the endpoint's log was read, the endpoint's log shows no scale-out the tap missed, the window was covered, and the premise held
— the canary was still queued at the stop, and in `second` phase A's block had answered in the SAME user endpoint.
REPORTED: `stop_answer`, `phase_a_stop`, `pilot_state_at_stop` (with the user endpoint), `canary_before_stop`,
`relaunch_watch`, `strategy_after_stop` (the endpoint log's scale-outs, error-handler and user-endpoint lines after the
stop), `uep_job_history` (phase A's job — connected —, the stop's own, any relaunch; held ones; MISSING / bad state).
On main `second` FAILS this way (above). A stop that answered `draining` passes this row even with a relaunch seen —
it did not claim spend stopped; the universal postcheck still fails a pilot left queued (see the grader's docstring).
"""
import asyncio
import json
import os
import re
import shlex
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from admin_tap import TAP_ENV, SlurmJob, TapReader, is_marker, slurm_hold_cmd, slurm_ids_cmd, slurm_sample_cmd
from cluster_ops import capture_logs_cmd
from invariants import Result, ToolCall, Trace

OPERATORS = ("script",)        # a scripted proof cell: an agent operator is refused (run.py: SKIPPED)
TARGETS = ("fake",)            # the admin tap needs the fake cluster's admin channel
REQUIRES = {"scheduler": "slurm", "mfa": "none"}   # squeue/sacct, and an autonomous BYO bring-up (no one-time code)
KIND = "regression"
SUMMARY = ("spend honesty: stop_endpoint answers `down` while the canary is still queued — does the endpoint relaunch a "
           "pilot after it? (scripted; phase `second`: a user endpoint that already ran a block; the admin tap watches)")
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


def _phase() -> str:
    """HPCB_RELAUNCH_PHASE: `second` (default — the stop that matters is the one on the user endpoint's SECOND pilot,
    after a first that connected) or `first` (the stop on its first pilot: the bad-state path of the first live run)."""
    p = os.environ.get("HPCB_RELAUNCH_PHASE", "").strip().lower()
    return p if p in ("first", "second") else "second"


# 420 s: several of parsl's 60 s job-status polls plus its 5 s strategy period after the stop (the first live run's
# executor acted 56 s after it)
WATCH_S = _env_s("HPCB_RELAUNCH_WATCH_S", 420.0)
PILOT_WAIT_S = _env_s("HPCB_RELAUNCH_PILOT_WAIT_S", 180.0)   # how long a phase waits for its pilot to be in the queue
WARM_WAIT_S = 300.0                                           # phase A: how long the block gets to answer a check
LOGIN_UP_WAIT_S = 300.0                                       # how long the login shape gets to come up
SENTINEL_WAIT_S = 120.0                                       # phase B: how long the sentinel gets to start running
# Phase `second`: every pilot of this endpoint, and the sentinel, carry this name; with `--dependency=singleton` a pilot
# waits for any job of the same user and name that runs. The name keeps `parsl` in it, so the universal postcheck
# (`stop_honesty_no_pilot_left`: no job named *parsl* left) still sees a pilot left queued.
SINGLETON_NAME = "parsl-hpcb-qcr"


def _hold_s() -> int:
    """Phase `first`: the `--begin` hold on every pilot (see THE HOLD): HPCB_RELAUNCH_HOLD_S, default max(1800,
    watch + 900); 0 = none."""
    raw = os.environ.get("HPCB_RELAUNCH_HOLD_S", "").strip()
    if raw:
        try:
            return max(0, int(float(raw)))
        except ValueError:
            pass
    return int(max(1800.0, WATCH_S + 900.0))


# Phase `second` runs a real block (phase A) on one node for a minute; `first`, held, never starts one (unless the hold
# is dropped). The sentinel takes one core.
NEEDS_COMPUTE_NODE = _phase() == "second" or _hold_s() == 0
# The admin tap (run_smoke.sh starts it before the cell; harness/admin_tap.py): the pool user's jobs and history as root
# on slurmctld every 10 s, with the controller's clock; jobs asked for by id; and, once phase B asks, `scontrol hold` on
# every new PENDING pilot of this endpoint.
ADMIN_TAP = {"every_s": 10, "cmd": slurm_sample_cmd(), "ids_cmd": slurm_ids_cmd(), "hold_cmd": slurm_hold_cmd()}
# Before the cell and after it, as the pool user: no job of the singleton name may be left (one from a cell killed before
# its cleanup would keep this cell's first pilot from ever starting).
SETUP = [f"scancel -u \"$(whoami)\" --name={SINGLETON_NAME} 2>/dev/null; echo ok"]
CLEANUP = [f"scancel -u \"$(whoami)\" --name={SINGLETON_NAME} 2>/dev/null; echo ok"]

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "bring up the login shape from the probe's proposal; phase A: start a compute block, let it answer, run `hostname` "
    "on it, stop the endpoint; phase B: start a block again in the same user endpoint (its pilot kept PENDING), wait on "
    "the login shape until the pilot is in the queue, and stop the endpoint at once. No teardown: the harness watches "
    "the scheduler as its admin for a relaunched pilot."
)

# `HPCB_PILOT <job> <state> <uep dir or -> <reason…>` (pilot_poll_cmd); the first run's lines had no uep field
_PILOT = re.compile(r"^HPCB_PILOT (\S+) (\S+)(?: (uep\.\S+|-))?(?: (.*))?$", re.M)
_RELEASED = re.compile(r"released ([0-9][0-9_ ]*)")


def pilot_poll_cmd(eid: str) -> str:
    """The login-shape poll: this endpoint's pilots as `HPCB_PILOT <job> <state> <uep> <reason>` lines (squeue's batch
    script carries the `uep.<eid>` marker, the one scheduler_ops._release_cmd matches the StdOut path on, and names the
    user endpoint: `uep.<eid>.<uep id>/submit_scripts/…`). Exits 0 on no match."""
    marker = shlex.quote(f"uep.{eid}")
    return ("squeue -u \"$USER\" -h -o '%i|%T|%r|%o' 2>/dev/null "
            f"| awk -F'|' -v m={marker} 'index($4,m){{u=\"-\"; "
            "if (match($4, /uep\\.[0-9a-f.-]+\\/submit_scripts/)) u=substr($4, RSTART, RLENGTH-15); "
            "print \"HPCB_PILOT \"$1\" \"$2\" \"u\" \"$3}'; true")


@dataclass(frozen=True)
class Pilot:
    job: str
    state: str
    uep: str       # the user endpoint's dir, `uep.<eid>.<uep id>` ("" when the poll did not say)
    reason: str


def _pilots(c: ToolCall) -> list[Pilot]:
    """The pilots a poll saw."""
    return [Pilot(m.group(1), m.group(2), "" if m.group(3) in (None, "-") else m.group(3), (m.group(4) or "").strip())
            for m in _PILOT.finditer(str((c.result or {}).get("stdout") or ""))]


def _desc(ps: list[Pilot]) -> str:
    return ", ".join(f"job {p.job} {p.state}" + (f" ({p.reason})" if p.reason and p.reason != "None" else "")
                     for p in ps)


def _proposal_details(r: dict, hold_s: int, phase: str = "first") -> dict:
    """The proposal as the confirming details=: env_setup left out (the server keeps the proposed one — the tool's own
    instruction), unset fields dropped, and the phase's directives after any the proposal carried (one per line, as the
    field says): `first` — the `--begin` HOLD; `second` — the singleton name and dependency (THE SENTINEL)."""
    details = {k: v for k, v in dict(r.get("proposed_details") or {}).items() if v is not None and k != "env_setup"}
    extra = ([f"#SBATCH --job-name={SINGLETON_NAME}", "#SBATCH --dependency=singleton"] if phase == "second" else
             [f"#SBATCH --begin=now+{hold_s}"] if hold_s else [])
    if extra:
        details["scheduler_options"] = "\n".join(x for x in (details.get("scheduler_options"), *extra) if x)
    return details


def _account(r: dict) -> str | None:
    """The allocation to charge: the first non-GPU one connect listed (site: hpcb), else none (default lists none)."""
    accounts = [str(a.get("account")) for a in (r.get("allocations") or []) if isinstance(a, dict) and a.get("account")]
    return next((a for a in accounts if "gpu" not in a.lower()), accounts[0] if accounts else None)


_WALLTIME = re.compile(r"^(?:\d+-)?\d{1,2}:\d{2}:\d{2}$")
SENTINEL_WALLTIME = "00:30:00"   # when the block's terms name none: the fake profiles' shortest partition limit (debug)


def sentinel_cmd(partition: str | None, account: str | None, walltime: str | None = None) -> str:
    """Phase B's sentinel, as the pool user on the login shape: one core, the pilots' name, on the pilots' partition
    and account, for the pilots' own walltime — the terms the scheduler already accepted for them, so a partition
    limit (`site`'s debug: 30 min, EnforcePartLimits=ALL) cannot refuse it. The world check cancels it once the watch
    is over; CLEANUP again."""
    wt = walltime if walltime and _WALLTIME.match(walltime) else SENTINEL_WALLTIME
    opts = [f"-J {SINGLETON_NAME}", "-N1", "-n1", f"-t {wt}", "-o /dev/null", "-e /dev/null"]
    if partition:
        opts.append(f"-p {shlex.quote(partition)}")
    if account:
        opts.append(f"-A {shlex.quote(account)}")
    return f"sbatch --parsable {' '.join(opts)} --wrap 'sleep 86400'"   # the walltime ends it, not the sleep


def _brief(r: dict) -> str:
    return str(r.get("notice") or r.get("text") or r.get("stdout") or "")[:200]


async def _poll_pilots(ops, eid: str, *, exclude: set[str]) -> list[Pilot]:
    """Poll the queue on the free login shape until a pilot of this endpoint not in `exclude` is in it (or abort)."""
    deadline = time.monotonic() + PILOT_WAIT_S
    while True:
        q = await ops.call("run_shell", shape="login", command=pilot_poll_cmd(eid))
        pilots = [p for p in _pilots(ToolCall("run_shell", {}, q)) if p.job not in exclude]
        if pilots:
            return pilots
        if time.monotonic() > deadline:
            ops.abort(f"no new pilot of uep.{eid} entered the queue within {PILOT_WAIT_S:.0f} s (last poll: {_brief(q)!r})")
        await ops.sleep(1)


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    phase = _phase()
    fid, host, hold = ops.facility, ops.ssh_host, _hold_s()
    # 1. the login shape, from the probe's proposal
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if r.get("phase") != "proposed_facility_details":
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {_brief(r)!r}")
    r = await ops.call("connect_facility", facility=fid, ssh_host=host, details=_proposal_details(r, hold, phase))
    deadline = time.monotonic() + LOGIN_UP_WAIT_S
    while r.get("phase") != "needs_account":
        if r.get("phase") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the login shape did not come up: phase={r.get('phase')!r} {_brief(r)!r}")
        await ops.sleep(5)
        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    account = _account(r)
    args: dict = {"shape": "compute", "confirm_spend": True, **({"account": account} if account else {})}
    if phase == "first":
        ops.say(f"phase first; login shape up; every pilot is held: {'#SBATCH --begin=now+' + str(hold) if hold else 'no hold'}")
        return await _first_pilot(ops, args)
    ops.say(f"phase second; login shape up; every pilot is named {SINGLETON_NAME} with --dependency=singleton")
    return await _second_pilot(ops, args, account)


async def _first_pilot(ops, args: dict) -> str:
    """The stop on the user endpoint's FIRST pilot, held PENDING by the `--begin` hold (the first live run's sequence)."""
    e = await ops.call("ensure_endpoint_up", **args)   # its canary goes out and (the block cold) stays queued
    eid = str(e.get("endpoint_id") or "")
    if e.get("status") not in ("provisioning", "up") or not eid:
        ops.abort(f"no block was started: status={e.get('status')!r} {_brief(e)!r}")
    pilots = await _poll_pilots(ops, eid, exclude=set())
    s = await ops.call("stop_endpoint")   # at once
    ops.say(f"pilot(s) at the stop: {_desc(pilots)}. stop_endpoint answered status={s.get('status')!r}, "
            f"spend_stopped={s.get('spend_stopped')!r}: {_brief(s)}")
    return (f"stopped with {_desc(pilots)} in the queue; stop_endpoint said {s.get('status')!r} "
            f"(spend_stopped={s.get('spend_stopped')!r}). Not torn down: the world check watches for a relaunch.")


async def _second_pilot(ops, args: dict, account: str | None) -> str:
    """Phase A: a block that runs, answers the check and a command, then an honest stop — the user endpoint now has a
    job that connected. Phase B: the same user endpoint's next pilot, kept PENDING (the sentinel, then the tap's hold),
    and the stop while its check is queued."""
    # ---- phase A
    e = await ops.call("ensure_endpoint_up", **args)
    eid = str(e.get("endpoint_id") or "")
    if e.get("status") not in ("provisioning", "up") or not eid:
        ops.abort(f"phase A: no block was started: status={e.get('status')!r} {_brief(e)!r}")
    partition = e.get("partition")   # the block's queue: the sentinel must share it to share the pilots' nodes
    walltime = (e.get("block") or {}).get("walltime")   # the block's walltime (the confirm's receipt): fits the partition
    poll_args = {k: v for k, v in args.items() if k != "confirm_spend"}
    deadline = time.monotonic() + WARM_WAIT_S
    while e.get("status") != "up":
        if e.get("status") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"phase A: the block never answered a check: status={e.get('status')!r} {_brief(e)!r}")
        await ops.sleep(5)
        e = await ops.call("ensure_endpoint_up", **poll_args)
        partition = e.get("partition") or partition
    h = await ops.call("run_shell", shape="compute", command="hostname")
    if h.get("phase") != "complete":
        ops.abort(f"phase A: hostname did not complete on the block: phase={h.get('phase')!r} {_brief(h)!r}")
    first = await _poll_pilots(ops, eid, exclude=set())
    a = await ops.call("stop_endpoint")
    for _ in range(3):   # nothing of ours is outstanding: an honest stop ends `down` (a cold channel may first drain)
        if a.get("status") != "draining":
            break
        await ops.sleep(5)
        a = await ops.call("stop_endpoint")
    ops.say(f"phase A: block {_desc(first)} on {first[0].uep or '?'} answered and ran hostname on "
            f"{str(h.get('stdout') or '').strip()!r}; stop_endpoint said {a.get('status')!r} "
            f"(spend_stopped={a.get('spend_stopped')!r}): {_brief(a)}")
    # ---- phase B: the sentinel first, so the next pilot cannot start (an idle node starts a job within a second)
    sub = await ops.call("run_shell", shape="login", command=sentinel_cmd(partition, account, walltime))
    sid = str(sub.get("stdout") or "").strip().split(";")[0].strip()
    if not sid.isdigit():
        ops.abort(f"phase B: the sentinel was not submitted: {_brief(sub)!r}")
    deadline = time.monotonic() + SENTINEL_WAIT_S
    while True:
        q = await ops.call("run_shell", shape="login", command=f"squeue -j {sid} -h -o %T 2>/dev/null; true")
        if str(q.get("stdout") or "").strip() == "RUNNING":
            break
        if time.monotonic() > deadline:
            ops.abort(f"phase B: the sentinel (job {sid}) never started running: {_brief(q)!r}")
        await ops.sleep(2)
    held = TapReader(os.environ.get(TAP_ENV, "").strip() or None).request_hold(f"uep.{eid}", int(sid) + 1)
    ops.say(f"phase B: sentinel job {sid} running; " + (f"the admin tap holds every new pilot of uep.{eid} from job "
            f"{int(sid) + 1}" if held else "NO admin tap to hold the next pilots (only the sentinel keeps them PENDING)"))
    b = await ops.call("ensure_endpoint_up", **args)   # the same terms: the same user endpoint, a new canary
    if b.get("status") not in ("provisioning", "up"):
        ops.abort(f"phase B: no block was started: status={b.get('status')!r} {_brief(b)!r}")
    second = await _poll_pilots(ops, eid, exclude={p.job for p in first} | _released(ToolCall("stop_endpoint", {}, a)))
    s = await ops.call("stop_endpoint")   # at once: the check is queued behind a pilot that cannot start
    ops.say(f"phase B: pilot(s) at the stop: {_desc(second)} on {second[0].uep or '?'}. stop_endpoint answered "
            f"status={s.get('status')!r}, spend_stopped={s.get('spend_stopped')!r}: {_brief(s)}")
    return (f"phase A stop said {a.get('status')!r}; phase B stopped with {_desc(second)} in the queue and said "
            f"{s.get('status')!r} (spend_stopped={s.get('spend_stopped')!r}). Not torn down: the world check watches.")


# ---- what the trace says ----------------------------------------------------------------------------------------------

def _is_sentinel(c: ToolCall) -> bool:
    cmd = str(c.input.get("command") or "")
    return c.name == "run_shell" and "sbatch" in cmd and f"-J {SINGLETON_NAME}" in cmd and "--wrap" in cmd


def _sentinel_index(t: Trace) -> int | None:
    """Where phase B began (its sentinel's submission); None in phase `first` (or before phase B)."""
    return next((i for i, c in enumerate(t.calls) if _is_sentinel(c)), None)


def _sentinel_job(t: Trace) -> str | None:
    i = _sentinel_index(t)
    sid = str((t.calls[i].result or {}).get("stdout") or "").strip().split(";")[0].strip() if i is not None else ""
    return sid if sid.isdigit() else None


def _stop(t: Trace) -> tuple[int, ToolCall] | None:
    """The JUDGED stop: in phase `second` the first one after the sentinel (phase B's); else the first one."""
    stops = t.named("stop_endpoint")
    s = _sentinel_index(t)
    if s is not None:
        return next(((i, c) for i, c in stops if i > s), None)
    return stops[0] if stops else None


def _phase_a_stops(t: Trace) -> list[tuple[int, ToolCall]]:
    s = _sentinel_index(t)
    return [(i, c) for i, c in t.named("stop_endpoint") if s is not None and i < s]


def _window(t: Trace) -> tuple[int, int]:
    """(start, end) of the calls that judge the judged stop's premise: from phase B's start (or the run's), to it."""
    s = _sentinel_index(t)
    stop = _stop(t)
    return (s if s is not None else -1), (stop[0] if stop else len(t.calls))


def _claims_stopped(c: ToolCall) -> bool:
    r = c.result or {}
    return str(r.get("status")) == "down" or r.get("spend_stopped") is True


def _released(c: ToolCall) -> set[str]:
    """The job ids the stop's own notice says it cancelled (`released 7 8`)."""
    m = _RELEASED.search(str((c.result or {}).get("notice") or ""))
    return set(m.group(1).split()) if m else set()


def _polls(t: Trace, after: int, before: int) -> list[tuple[int, list[Pilot]]]:
    return [(i, p) for i, c in t.named("run_shell") if after < i < before and (p := _pilots(c))]


def _last_poll(t: Trace) -> tuple[int, list[Pilot]] | None:
    """The last squeue poll in the judged stop's window that saw a pilot."""
    polls = _polls(t, *_window(t))
    return polls[-1] if polls else None


def _seen_before(t: Trace, before: int) -> set[str]:
    return {p.job for _, ps in _polls(t, -1, before) for p in ps}


def _answered(t: Trace, after: int, before: int) -> tuple[int, str] | None:
    """A worker answered between those calls: an ensure_endpoint_up said up/warm, or compute work completed."""
    for i, c in enumerate(t.calls):
        if not (after < i < before) or str(c.input.get("shape") or "compute") != "compute":
            continue
        r = c.result or {}
        if c.name == "ensure_endpoint_up" and (str(r.get("status")) == "up" or str(r.get("block_state")) == "warm"):
            return i, f"ensure_endpoint_up (call {i}) answered status={r.get('status')!r}: a worker answered the check"
        if c.name == "run_shell" and r.get("phase") == "complete":
            return i, f"compute work completed (call {i}): a worker was up"
    return None


def canary_state(t: Trace) -> tuple[str, str]:
    """Was hpc-bridge's check still queued when the JUDGED stop came? `queued` (every pilot PENDING at the last poll
    before it, in its window, and no worker had answered there), `ran` (a worker answered in that window), or `unknown`
    (a pilot was RUNNING: its worker may have run it first) — with why. Phase A's answers are before the window."""
    start, end = _window(t)
    if (got := _answered(t, start, end)) is not None:
        return "ran", got[1]
    last = _last_poll(t)
    if last is None:
        return "unknown", "no squeue poll before the stop saw a pilot of this endpoint"
    i, pilots = last
    if all(p.state == "PENDING" for p in pilots):
        return "queued", (f"{_desc(pilots)} at the last poll before the stop (call {i}): no worker had started, so the "
                          "check was queued")
    return "unknown", f"{_desc(pilots)} at the last poll before the stop (call {i}): its worker may have run the check first"


def same_user_endpoint(t: Trace) -> tuple[bool | None, str]:
    """Phase `second`: did phase B's pilot come from the SAME user endpoint as phase A's (the `uep.<eid>.<uep id>` dir
    of their batch scripts)? None when a side is unknown."""
    s = _sentinel_index(t)
    if s is None:
        return None, "no phase B"
    a = {p.uep for _, ps in _polls(t, -1, s) for p in ps if p.uep}
    b = {p.uep for _, ps in _polls(t, *_window(t)) for p in ps if p.uep}
    if not a or not b:
        return None, f"the user endpoint is unknown (phase A: {sorted(a) or '?'}, phase B: {sorted(b) or '?'})"
    return a == b, (f"phase A and phase B both ran in {sorted(a)[0]}" if a == b else
                    f"phase A ran in {sorted(a)}, phase B in {sorted(b)}: a NEW user endpoint")


def stop_answer(t: Trace) -> Result:
    """REPORTED: the judged stop's full answer."""
    stop = _stop(t)
    if stop is None:
        return Result("stop_answer", False, "no judged stop_endpoint call")
    i, c = stop
    keep = ("status", "spend_stopped", "block_state", "block_job_id", "endpoint_id", "session_spend", "notice", "text",
            "is_error")
    return Result("stop_answer", True, f"call {i}: " + json.dumps({k: v for k, v in (c.result or {}).items() if k in keep},
                                                                    default=str))


def phase_a_stop(t: Trace) -> Result:
    """REPORTED: phase A's stop(s) — nothing of ours outstanding, so an honest `down`."""
    stops = _phase_a_stops(t)
    if not stops:
        return Result("phase_a_stop", True, "no phase A (HPCB_RELAUNCH_PHASE=first, or phase B never started)")
    last = stops[-1][1].result or {}
    return Result("phase_a_stop", _claims_stopped(stops[-1][1]),
                  " · ".join(f"call {i}: status={(c.result or {}).get('status')!r}, spend_stopped="
                             f"{(c.result or {}).get('spend_stopped')!r}, released {sorted(_released(c)) or 'none'}"
                             for i, c in stops) + (f" — {str(last.get('notice') or '')[:160]}" if last else ""))


def pilot_state_at_stop(t: Trace) -> Result:
    """REPORTED: what the last squeue poll before the judged stop saw of this endpoint's pilot(s), and their user
    endpoint (phase `second`: whether it is phase A's)."""
    last = _last_poll(t)
    if last is None:
        return Result("pilot_state_at_stop", False, "no squeue poll saw a pilot of this endpoint before the stop")
    i, pilots = last
    _, why = same_user_endpoint(t)
    return Result("pilot_state_at_stop", True, f"call {i}: {_desc(pilots)}"   # reported: a new user endpoint is the fix
                  + (f" in {pilots[0].uep}" if pilots[0].uep else "") + (f"; {why}" if _sentinel_index(t) is not None
                                                                          else ""))


def canary_before_stop(t: Trace) -> Result:
    """REPORTED: whether the canary could have run before the judged stop (canary_state)."""
    state, why = canary_state(t)
    return Result("canary_before_stop", state == "queued", f"{state}: {why}")


# ---- what the scheduler says (the admin tap) and what the endpoint says (its log) -------------------------------------

NO_RELAUNCH = "world:no_relaunch_after_down"


@dataclass
class LogLine:
    at: float          # epoch seconds (the login node's clock; its logs are UTC)
    where: str         # "manager", "uep 9f7c4349", …
    text: str          # the line, with its continuation lines (a traceback's message) folded in


@dataclass
class Watch:
    """What the admin tap saw of this endpoint around the judged stop, and what the endpoint's own log says. Times are
    the controller's clock (the jail's and the login node's too: one Docker VM kernel)."""
    called_at: float | None = None      # the judged stop call started
    answered_at: float | None = None    # it answered
    window_s: float = WATCH_S
    covered_until: float | None = None  # the controller clock of the latest sample read
    samples: int = 0
    jobs: list[SlurmJob] = field(default_factory=list)   # this endpoint's jobs (by its marker), queue and history merged
    unverifiable: str | None = None
    own_ids: set[str] = field(default_factory=set)       # the judged stop's own pilot(s): its window's polls + release
    seen_unmarked: set[str] = field(default_factory=set)  # own ids the tap saw WITHOUT this endpoint's marker
    log_lines: list[LogLine] | None = None                # strategy lines after the stop (None: the log was not read)
    log_error: str = ""
    phase: str = "first"                                  # HPCB_RELAUNCH_PHASE as the run was made
    held: set[str] = field(default_factory=set)           # jobs the tap held (phase B's hold request)
    # The own pilot as a CLEAN sample's squeue (else, once ended, its window sacct) listed it by its marker — the proof the
    # sampling could
    # see a relaunch. `sacct -j <own id>` (`by_id`) never counts: a lookup by a known id cannot see an unknown one.
    own_seen: dict[str, str] = field(default_factory=dict)   # job id -> "STATE [squeue]" (or "[sacct]": ended)
    by_id: dict[str, str] = field(default_factory=dict)      # job id -> "STATE" from sacct -j (reported only)
    unclean: list[str] = field(default_factory=list)         # the errors of the samples that were not clean
    sentinel: str = ""                                        # phase B's sentinel at the end: "gone" | "STILL …" | ""


def _stop_times(messages: list, nth: int = 0) -> tuple[float | None, float | None]:
    """(started, answered) of the run's `nth` stop_endpoint (0-based, in the order the calls started), in epoch seconds:
    the server journal's own stamps (`ts`, `ms`), else the script's step record."""
    rows = sorted((m["journal"] for m in messages or [] if isinstance(m, dict) and isinstance(m.get("journal"), dict)
                   and m["journal"].get("tool") == "stop_endpoint" and isinstance(m["journal"].get("ts"), int | float)),
                  key=lambda r: r["ts"])
    if len(rows) > nth:
        r = rows[nth]
        return float(r["ts"]), float(r["ts"]) + float(r.get("ms") or 0) / 1000.0
    steps = [m["native"] for m in messages or [] if isinstance(m, dict) and isinstance(m.get("native"), dict)
             and m["native"].get("name") == "stop_endpoint" and m["native"].get("t_end") is not None]
    if len(steps) > nth:
        return steps[nth].get("t_start"), float(steps[nth]["t_end"])
    return None, None


def _judged_nth(t: Trace) -> int:
    """Which stop_endpoint call (0-based) the judged stop is."""
    stop = _stop(t)
    return sum(1 for i, _ in t.named("stop_endpoint") if stop is not None and i < stop[0])


def own_pilots(t: Trace) -> set[str]:
    """The judged stop's own pilot job ids: what the squeue polls in its window saw, and what it released."""
    stop = _stop(t)
    if stop is None:
        return set()
    return _released(stop[1]) | {p.job for _, ps in _polls(t, *_window(t)) for p in ps}


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
    """Pilots of this endpoint no stop accounted for: not among those any stop released or a poll saw before the judged
    stop, and submitted once it had started (a whole-second Slurm stamp, so 1 s of slack) — or, with no submit time,
    first seen after its answer."""
    stop = _stop(t)
    if stop is None or w.answered_at is None:
        return []
    known = _seen_before(t, stop[0]) | {j for _, c in t.named("stop_endpoint") for j in _released(c)}
    since = (w.called_at if w.called_at is not None else w.answered_at) - 1.0
    return [j for j in w.jobs if j.job_id not in known and (
        (j.submit is not None and j.submit >= since) or (j.submit is None and (j.seen_at or 0) >= w.answered_at))]


def _describe(j: SlurmJob, w: Watch) -> str:
    at = (f"submitted {j.submit - w.answered_at:+.0f} s after the stop answered" if j.submit is not None and w.answered_at
          else f"first seen {(j.seen_at or 0) - (w.answered_at or 0):+.0f} s after the stop answered")
    why = f" ({j.reason})" if j.reason and j.reason != "None" else ""
    return f"job {j.job_id} {at}, {j.state}{why}"


# A parsl / Globus Compute log line: `2026-10-16 02:27:55,056648 ERROR MainProcess-443 …` (UTC on the fake login node:
# run 2's scale-out at 02:26:54 sits 8 s after its ensure_endpoint_up call in the journal's epoch).
_LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{1,6}) ")
_LOG_FILE = re.compile(r"^=== (\S+)")
# What decides a relaunch, as GCE 4.18 / parsl 2026.10 log it at INFO: the strategy's scale-out (`Scaling out by N
# blocks`, `Allocated block ID n`), its scale-in, the error handler (a finished block that never connected is rewritten
# to MISSING and, when every job is FAILED/MISSING, puts the executor in bad state: `Not strategizing … because bad
# state`), the interchange's outstanding work, and the manager (re)starting or losing a user endpoint.
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
    """REPORTED: what the endpoint itself decided after the judged stop — parsl's scale-outs, the error handler, the user
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


def uep_job_history(t: Trace, w: Watch) -> Result:
    """REPORTED: the user endpoint's jobs as the scheduler and the trace know them — phase A's (did a worker answer on
    it: it connected), the judged stop's own, the relaunched — with their states; and the endpoint log's MISSING / bad
    state lines (parsl rewrites a finished block that never connected to MISSING)."""
    s = _sentinel_index(t)
    a_ids = {p.job for _, ps in _polls(t, -1, s) for p in ps} | {j for _, c in _phase_a_stops(t) for j in _released(c)} \
        if s is not None else set()
    answered = _answered(t, -1, s) if s is not None else None
    new = {j.job_id for j in relaunches(t, w)}

    def label(j: SlurmJob) -> str:
        if j.job_id in a_ids:
            return "phase A" + (", connected (a worker answered on it)" if answered else ", never answered")
        if j.job_id in w.own_ids:
            return "the judged stop's own pilot"
        return "RELAUNCHED after the stop" if j.job_id in new else "other"

    jobs = "; ".join(f"job {j.job_id} {j.state}{' (' + j.reason + ')' if j.reason and j.reason != 'None' else ''}"
                     f"{' HELD by the tap' if j.job_id in w.held else ''} — {label(j)}" for j in w.jobs) or "none seen"
    bad = [x for x in (w.log_lines or []) if re.search(r"MISSING|bad state", x.text)]
    return Result("uep_job_history", True, jobs + (f"; the endpoint log after the stop: {len(bad)} MISSING / bad-state "
                                                   f"line(s), first: {_short_log(bad[0], w)}" if bad else ""))


# ---- the gated grader ---------------------------------------------------------------------------------------------------

def no_relaunch_after_down(t: Trace, w: Watch) -> Result:
    """GATED (a world row): a JUDGED stop that answered `down` (or spend_stopped=True) must not be followed by a new pilot
    of this endpoint. A stop that did not claim spend stopped (`draining`, a refusal) is another premise: report-only,
    PASS, said so — EVEN WHEN A RELAUNCH WAS SEEN. Why: this row tests one claim, that a `down` is honest; `draining`
    is hpc-bridge saying spend may continue and to call stop again (#24), which a later pilot does not contradict. A
    pilot left queued after such a stop is still caught, by the universal `world:stop_honesty_no_pilot_left` (no job
    named *parsl* left), which gates every cell. The detail names any relaunch either way. A `down` with no relaunch passes only when the watch could have seen one — the tap saw the stop's OWN
    pilot by its marker (the first live run watched blind and passed: its pilot lived 4.6 s between two samples), the
    endpoint's log shows no scale-out the tap missed, and it covered the window — and the premise held: the check was
    still queued at the stop, and in phase `second` phase A's block had connected in the SAME user endpoint."""
    if w.phase == "second" and _sentinel_index(t) is None:
        return Result(NO_RELAUNCH, False, "premise not met: phase B never started (no sentinel in the trace) — see the "
                                          "run's final message")
    stop = _stop(t)
    if stop is None:
        return Result(NO_RELAUNCH, False, "premise not met: no judged stop_endpoint in the trace — the script never "
                                          "reached it (see the run's final message)")
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
    if w.log_lines is None:   # a PASS needs the endpoint's own account of its strategy, not only the queue's silence
        return Result(NO_RELAUNCH, False, f"UNVERIFIABLE — the endpoint's logs were not read "
                                          f"({w.log_error or 'no reason recorded'}), so a scale-out the tap missed "
                                          "could not be ruled out")
    seen = dict(w.own_seen)
    if not w.own_ids:
        return Result(NO_RELAUNCH, False, "watch blind: no pilot id of this stop is known (no squeue poll saw one and it "
                                          "released none), so nothing shows the tap can see this endpoint's jobs")
    if not seen:
        unmarked = sorted(w.own_ids & w.seen_unmarked)
        by_id = (f" (sacct -j found {', '.join(f'job {j} {st}' for j, st in sorted(w.by_id.items()))} — a lookup by a "
                 "known id, which could never see a relaunch)" if w.by_id else "")
        return Result(NO_RELAUNCH, False, f"watch blind: no clean sample's squeue or window sacct listed this "
                      f"endpoint's own pilot (job {', '.join(sorted(w.own_ids))}) in {w.samples} sample(s)"
                      + (f", {len(w.unclean)} of them not clean" if w.unclean else "") + by_id
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
                                          "was still queued")
    if _sentinel_index(t) is not None:
        if _answered(t, -1, _sentinel_index(t)) is None:
            return Result(NO_RELAUNCH, False, "premise not met: phase A's block never answered a check, so the user "
                                              "endpoint had no job that connected — that is the first-pilot (bad-state) "
                                              "path, not the one phase `second` tests")
        same, how = same_user_endpoint(t)
        if same is None:
            return Result(NO_RELAUNCH, False, f"premise unverified: {how} — which user endpoint phase B's pilot came "
                                              "from decides which case this stop tested")
        # A NEW user endpoint for phase B is the fix's design (a fresh compute user endpoint per block request, so the
        # judged pilot is that endpoint's FIRST and a cancel trips parsl's bad state), not a broken premise: judge the
        # relaunch all the same — the integration run of 2026-10-16 failed here on exactly that, with nothing relaunched.
        fresh = (f"; {how} — fresh per block request, so the later-pilot case cannot arise there"
                 if same is False else "")
    else:
        fresh = ""
    own = ", ".join(f"job {j} {st}" for j, st in sorted(seen.items()))
    return Result(NO_RELAUNCH, True, f"ok: {said}; no new pilot of this endpoint was submitted in the {covered:.0f} s "
                                     f"after it — the tap saw its own pilot ({own}), so it could have seen one ({why})"
                                     + fresh)


def relaunch_report(w: Watch) -> Result:
    """REPORTED: every job of this endpoint the tap saw, with its submit time against the stop's answer, and the cover."""
    jobs = "; ".join(f"job {j.job_id} {j.state}{' (' + j.reason + ')' if j.reason and j.reason != 'None' else ''} "
                     + (f"submitted {j.submit - w.answered_at:+.0f} s" if j.submit is not None and w.answered_at else
                        "submit time unknown") + f" [{j.source}]" for j in w.jobs) or "none"
    cover = (f"{(w.covered_until or 0) - (w.answered_at or 0):.0f} s after the stop answered"
             if w.covered_until is not None and w.answered_at is not None else "no sample")
    own = ", ".join(sorted(w.own_ids)) or "none known"
    return Result("relaunch_watch", True, f"{w.samples} admin sample(s) ({len(w.unclean)} not clean), covering {cover}; "
                  f"the stop's own pilot(s): {own}"
                  + (f" — seen as {', '.join(f'{j} {st}' for j, st in sorted(w.own_seen.items()))}" if w.own_seen else "")
                  + (f"; by id (sacct -j): {', '.join(f'{j} {st}' for j, st in sorted(w.by_id.items()))}" if w.by_id
                     else "")
                  + f"; this endpoint's jobs: {jobs}" + (f"; held: {', '.join(sorted(w.held))}" if w.held else "")
                  + (f"; sentinel at the end: {w.sentinel}" if w.sentinel else "")
                  + (f"; first error: {w.unclean[0][:200]}" if w.unclean else "")
                  + (f"; {w.unverifiable}" if w.unverifiable else ""))


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


SENTINEL_GONE_S = 60.0   # how long the world check waits for the cancelled sentinel to leave the queue


def end_sentinel_cmd(sid: str | None, marker: str, from_job: str | None) -> str:
    """Phase `second`'s last world step, as the pool user: hold every pending pilot of `marker` from `from_job` on ONE
    last time (a relaunch submitted after the tap's last tick would otherwise start the moment the sentinel ends — a
    user's own hold, JobHeldUser), then cancel the sentinel and wait until it has left the queue. The universal
    postcheck (`run._no_pilot_left`) now waits out a sentinel still COMPLETING (CG) itself, so this wait is partly
    redundant: it stays to say whether the sentinel went, and a sentinel still RUNNING — its cancel refused — is
    still a pilot left behind to that check. Prints `HPCB_HELD <id>` for each pilot it held and
    `HPCB_SENTINEL gone|STILL <state>`."""
    hold = ""
    if from_job and from_job.isdigit() and is_marker(marker):   # never a bare `uep.`: it would match every pilot
        hold = (slurm_hold_cmd().replace("{user}", '"$(whoami)"').replace("{marker}", shlex.quote(marker))
                .replace("{from}", from_job).replace('$2 != "JobHeldAdmin"', '$2 !~ /^JobHeld/') + "; ")
    target = sid if sid and sid.isdigit() else ""
    cancel = (f"scancel {target}" if target else
              f"scancel -u \"$(whoami)\" --name={SINGLETON_NAME} --state=RUNNING")
    probe = (f"squeue -h -j {target} -o %T 2>/dev/null" if target else
             f"squeue -u \"$(whoami)\" -h -n {SINGLETON_NAME} -t R,CG -o %T 2>/dev/null")
    tries = max(1, int(SENTINEL_GONE_S // 2))
    return (f"{hold}{cancel} 2>&1; s=; for i in $(seq 1 {tries}); do s=$({probe}); [ -z \"$s\" ] && break; sleep 2; "
            "done; [ -z \"$s\" ] && echo 'HPCB_SENTINEL gone' || echo \"HPCB_SENTINEL STILL $s\"; true")


async def _end_sentinel(ctx, t: Trace, w: Watch) -> None:
    """Phase `second`: the watch is over — a last hold tick, then cancel the sentinel and wait until it is gone; a
    relaunched pilot stays held until the teardown cancels it (its cancel-by-marker covers held jobs). CLEANUP cancels
    the singleton name again."""
    ssh = getattr(ctx, "ssh", None)
    if ssh is None or _sentinel_index(t) is None:
        return
    sid = _sentinel_job(t)
    eid = next(iter(_endpoint_ids(t)), "")
    # no endpoint id, no hold tick: an empty one would make the bare marker `uep.`, which every pilot carries
    from_job = str(int(sid) + 1) if sid and eid else None
    _rc, out = await asyncio.to_thread(ssh, end_sentinel_cmd(sid, f"uep.{eid}", from_job), int(SENTINEL_GONE_S) + 60)
    w.held |= {ln.split()[1] for ln in out.splitlines() if ln.startswith("HPCB_HELD ") and len(ln.split()) == 2}
    w.sentinel = next((ln[len("HPCB_SENTINEL "):] for ln in out.splitlines() if ln.startswith("HPCB_SENTINEL ")),
                      f"unknown: {out.strip()[-120:]!r}")


# A pilot that has ended, as sacct says it: only then is the window sacct's row a sighting — it lists ENDED jobs, never
# a held PENDING one, so a sacct row of a live pilot would not show the sampling could see a pending relaunch.
_ENDED = frozenset({"CANCELLED", "COMPLETED", "FAILED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY",
                    "BOOT_FAIL", "DEADLINE"})


def own_sightings(clean: list, eids: list[str], own: set[str]) -> dict[str, str]:
    """The stop's own pilot(s) as CLEAN samples listed them, by this endpoint's marker: squeue first — it is what sees a
    pending relaunch, so its sighting is the proof the watch could see one — else the window sacct, for a pilot that
    has already ended."""
    marks = [f"uep.{e}" for e in eids if e]
    seen: dict[str, str] = {}
    for s in clean:
        for j in s.queue:
            if j.job_id in own and any(m in j.command for m in marks):
                seen[j.job_id] = f"{j.state} [squeue]"
    for s in clean:
        for j in s.history:
            if j.job_id in own and j.job_id not in seen and j.state in _ENDED and any(m in j.command for m in marks):
                seen[j.job_id] = f"{j.state} [sacct]"
    return seen


def fold_samples(w: Watch, samples: list, eids: list[str]) -> None:
    """The tap's samples into `w`. A relaunch is positive evidence from ANY sample (`jobs`); coverage and the own-pilot
    sighting come from CLEAN samples only — a sample with an error line, a failed query, a missing exit status or no
    clock covers nothing and proves nothing; `sacct -j` rows (`by_id`) are reported, never a sighting."""
    clean = [s for s in samples if getattr(s, "clean", not s.error) and s.now is not None]
    w.samples = len(samples)
    w.unclean = [s.error for s in samples if s.error]
    w.covered_until = max((s.now for s in clean), default=None)
    w.jobs = endpoint_jobs(samples, eids)
    w.held = {j for s in samples for j in getattr(s, "held", [])}
    w.own_seen = own_sightings(clean, eids, w.own_ids)
    w.by_id = {j.job_id: j.state for s in samples for j in getattr(s, "by_id", []) if j.job_id in w.own_ids}
    w.seen_unmarked = {j.job_id for s in clean for j in [*s.queue, *s.history] if j.job_id in w.own_ids}
    if w.answered_at is not None and not any(s.now >= w.answered_at for s in clean):
        w.unverifiable = (f"the admin tap has no CLEAN sample after the stop answered ({len(samples)} sample(s), "
                          f"{len(w.unclean)} not clean" + (f": {' | '.join(w.unclean[-2:])[:400]}" if w.unclean else "")
                          + ")")


async def relaunch_watch(ctx) -> list[Result]:
    """WORLD CHECK: ask the tap for the judged stop's own pilot by id, wait until it has covered WATCH_S after the stop's
    answer, read the endpoint's own log, end the sentinel, then grade. `ctx`: run.py's WorldContext."""
    t: Trace = ctx.trace
    called, answered = _stop_times(ctx.messages, _judged_nth(t))
    w = Watch(called_at=called, answered_at=answered, own_ids=own_pilots(t), phase=_phase())
    eids = _endpoint_ids(t)
    if _stop(t) is None or answered is None:
        w.unverifiable = "nothing to watch: no judged stop_endpoint answer in the run"
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
        fold_samples(w, samples, eids)
    await _read_logs(ctx, eids, w)
    await _end_sentinel(ctx, t, w)
    return [no_relaunch_after_down(t, w), relaunch_report(w), strategy_after_stop(w), uep_job_history(t, w)]


WORLD_CHECKS = [relaunch_watch]
EXTRA_INVARIANTS = [stop_answer, phase_a_stop, pilot_state_at_stop, canary_before_stop]
# world:no_relaunch_after_down gates as every world row does; agent_engaged, run_completed (the script finished — an
# abort is a premise unmet) and the safety floor always gate. The rest is the report.
EXPECT_OK = ["agent_engaged"]
