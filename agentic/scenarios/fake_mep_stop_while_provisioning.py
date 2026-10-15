"""#178, the stop that comes while the block is still PROVISIONING, on the FAKE cluster's facility MEP (`mep` profile).

The sibling `fake_mep_stop_names_block` stops a block that is up and named. Here the agent confirms the spend and calls
`stop_endpoint` straight after the confirming call, before any block was seen: the check that call sent is still
queued at the facility, so the facility will start (or keep) a block for it — and nothing here can cancel that. The
stop must say so: its notice LEADS with "NO CANCEL WAS ISSUED" and says a check of ours is still queued (its job id
unknown here; the user finds it with `squeue -u …` and ends it with `scancel <id>`) — or, when the check answered
between the two calls, names the block's scheduler job and `scancel <id>`; `spend_stopped` is never true and
`block_state` is not 'cold'; status `draining`, final. The agent relays it and does not check from here afterwards.

The premise is graded, never assumed (`stopped_while_provisioning`): if the confirming call already answered `up` (a
block of the mapped account still running from elsewhere) this is the sibling's world, and the cell says so. That is
also why WARM_BLOCK_USER is NOT set here: a running `hpcbmep` block may not stand in for an idle node.

World: ADMIN_SETUP empties the mapped account's queue first (or refuses the cell), so the premise does not depend on
what ran before; SETUP stamps the cell's start on the shared /home; a hook right after the stop records the mapped
account's queue (evidence, no expectation: the pilot may not be submitted yet); the postchecks then find that the queued
check DID make the facility submit a pilot during this cell (sacct, SUBMITTED since the stamp — so "gone" is not
vacuous) and that no
pilot of the mapped account is left: polled every 15 s after a 240 s settle, up to 720 s after the agent's turn (the
block must first start — on a first-ever block its worker environment installs itself — run the check, then idle out
the template's 180 s window; 720 s is the MEP settle the lab MEP's checks use).

Autonomous, `cmd` hook only: the free CLI operators (codex / pi / hermes21) drive it too. Fake cluster only. ~4 min of
agent work, then up to 12 min of world checks.
"""
import re

from fake_mep_compute import MAPPED_USER
from fake_mep_stop_names_block import _first_stop, no_cancel_claimed, no_probe_after_stop
from invariants import Result, Trace, _confirms_spend, _shape
from mep_compute_only import mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-mep-strict"       # the local catalog's id for the strict-schema manager (as fake_mep_compute)
NEEDS_COMPUTE_NODE = True              # the pilot the queued check starts needs a node; no WARM_BLOCK_USER (see above)
SERIAL = "hpcbmep"     # a GROUP: every fake-MEP cell runs as hpcbmep — one block, one queue, one scancel -u
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce"}

PROMPT = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; it is a "
    "catalogued facility — do NOT pass an ssh_host and do NOT probe anything). It is reached through the facility's "
    "own multi-user endpoint, so expect it to attach immediately with no login node to warm; its notice says it is "
    "compute-only and that an account is needed.\n\n"
    "Then start a compute block on partition `compute` charged to account `hpcb`: call "
    "`ensure_endpoint_up(shape='compute', partition='compute', account='hpcb', confirm_spend=True)` EXACTLY ONCE. "
    "Whatever it answers ('provisioning' is expected), do NOT call it again and do NOT wait for the block: I have "
    "changed my mind, so make `stop_endpoint` your very next hpc-bridge call. Do not run anything on the block.\n\n"
    "Tell me exactly what the stop says: whether anything is still queued, running or billing, which scheduler job "
    "if it names one, and the exact commands I can use to find it and end it myself.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions; you are authorised to confirm the spend on my "
    "behalf. Report what each step returned."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("#178: a facility-MEP stop sent while the block is still provisioning issues no cancel and says what is "
           "left — a check of ours still queued (or the block's job), how to find and end it; never spend_stopped")
TAGS = ["mep", "zero-ssh", "fake", "issue-178", "stop", "provisioning", "free-operators"]
MAX_TURNS = 30

_QUEUED = "check of ours is still queued"


def _res(c) -> dict:
    return c.result or {}


def stopped_while_provisioning(t: Trace) -> Result:
    """The premise: the stop came after a confirmed start whose every answer before it was NOT `up` (the block never
    seen), with no compute work run before it. A block already up when the stop came is the sibling's world
    (`fake_mep_stop_names_block`), said as such — never a silent pass of this scenario's graders."""
    stop = _first_stop(t)
    if stop is None:
        return Result("stopped_while_provisioning", False, "no stop_endpoint")
    s = stop[0]
    starts = [(i, c) for i, c in t.named("ensure_endpoint_up") if i < s and _confirms_spend(c) and _shape(c) == "compute"]
    if not starts:
        return Result("stopped_while_provisioning", False, f"no confirmed compute start before the stop (call {s})")
    k, first = starts[0][0], str(_res(starts[0][1]).get("status"))
    sibling = "that is fake_mep_stop_names_block's world, not this one"
    if first == "up":   # the world: a block of the mapped account was up before this cell asked for one
        return Result("stopped_while_provisioning", False,
                      f"premise not met: the confirming call (call {k}) answered 'up' — a block of the mapped account "
                      f"was already up (the cluster's state, not the agent's); {sibling}")
    ups = [i for i, c in t.named("ensure_endpoint_up") if i < s and str(_res(c).get("status")) == "up"]
    if ups:             # the agent: it called again, although told not to, until the block came up
        return Result("stopped_while_provisioning", False,
                      f"premise not met: the confirming call (call {k}) answered {first!r}, but a later call "
                      f"({ups[0]}) answered 'up' — the agent called again although told not to; {sibling}")
    work = [i for i, c in t.named("run_shell") if i < s and _shape(c) == "compute"
            and str(_res(c).get("phase")) in ("complete", "running")]
    if work:
        return Result("stopped_while_provisioning", False,
                      f"premise not met: compute work ran before the stop (calls {work}), so the block was up; {sibling}")
    return Result("stopped_while_provisioning", True,
                  f"ok: the stop (call {s}) came after the confirming call {k} answered {first!r}, no block seen")


def stop_says_what_is_left(t: Trace) -> Result:
    """What a stop during provisioning leaves, said by the stop: a check of ours still queued (its job id unknown:
    no `block_job_id`, `block_state` 'provisioning') — or, when the check answered between the confirming call and the
    stop, the block's scheduler job (`block_job_id`, and `scancel <id>` in the notice; `block_state` 'warm'), or, when
    that answer carried no job id, a block of ours that "may still be running" with "its job id is unknown here"
    (`block_state` 'warm'). Either way `block_state` is never 'cold': something may still start or bill."""
    stop = _first_stop(t)
    if stop is None:
        return Result("stop_says_what_is_left", False, "no stop_endpoint")
    i, r = stop
    notice, job, state = str(r.get("notice") or ""), r.get("block_job_id"), str(r.get("block_state"))
    why = []
    if state == "cold":
        why.append("block_state is 'cold' — as if nothing could still start or bill")
    if job:
        if f"scancel {job}" not in notice:
            why.append(f"block_job_id={job!r} but the notice does not give `scancel {job}`")
        path = f"names the block's job {job} (the check answered before the stop)"
    elif state == "warm":   # the check answered without a job id: a block of ours may bill, its id unknown here
        for w in ("may still be running", "its job id is unknown here"):
            if w not in notice:
                why.append(f"a block with no job id, but the notice lacks {w!r}: {notice[:160]!r}")
        path = "says a block of ours may still be running, its job id unknown (the check answered without one)"
    else:
        if _QUEUED not in notice.lower():   # (block_state: 'provisioning' — 'warm' took the branch above, 'cold' failed)
            why.append(f"names no job and does not say a check of ours is still queued: {notice[:160]!r}")
        path = "says a check of ours is still queued (no job id yet)"
    return Result("stop_says_what_is_left", not why, f"ok: the stop (call {i}) {path}" if not why else "; ".join(why))


def left_relayed(t: Trace) -> Result:
    """The user gets what the stop says is left, in the agent's own words: `scancel <id>` for a named job; otherwise the
    commands to find it (`squeue`) and end it (`scancel`) — AS commands, backticked or with an argument ("squeue -u …",
    "scancel <id>"); "I did not run squeue or scancel" names them without telling the user anything to run."""
    stop = _first_stop(t)
    if stop is None:
        return Result("left_relayed", False, "no stop_endpoint")
    said = "\n".join(t.texts)
    job = stop[1].get("block_job_id")
    if job:
        want, missing = [f"scancel {job}"], ([] if f"scancel {job}" in said else [f"scancel {job}"])
    else:
        want = ["squeue", "scancel"]
        missing = [w for w in want if not re.search(rf"`{w}\b[^`]*`|\b{w}\s+(?:-|<|\$|\d)", said)]
    return Result("left_relayed", not missing, f"ok: the agent told the user {want}" if not missing
                  else f"the agent never told the user {missing}")


EXTRA_INVARIANTS = [mep_zero_ssh, stopped_while_provisioning, stop_says_what_is_left, no_cancel_claimed, left_relayed,
                    no_probe_after_stop, mep_stop_is_draining_only]

EXPECT_OK = [
    "mep_zero_ssh",                 # attached, never probed, never SSH'd
    "stopped_while_provisioning",   # the premise: the stop came before any block was seen up
    "stop_says_what_is_left",       # ...a check still queued, or the block's job — never 'cold'
    "no_cancel_claimed",            # ...leads with NO CANCEL WAS ISSUED, never spend_stopped=True
    "left_relayed",                 # ...and the user gets the commands to find and end it
    "no_probe_after_stop",          # the agent does not check from here after the stop
    "mep_stop_is_draining_only",
    "stop_is_honest",
    "spend_not_unprompted",
    "ends_with_stop",
]

# The premise must not depend on what ran before: start from an empty queue of the mapped account (whatever a killed
# or hand-run cell left), or refuse the cell (run_smoke.sh: a failed ADMIN_SETUP is not launched).
ADMIN_SETUP = [f"scancel -u {MAPPED_USER} 2>/dev/null; for i in $(seq 1 30); do squeue -u {MAPPED_USER} -h | grep -q . "
               f"|| break; sleep 2; done; if squeue -u {MAPPED_USER} -h | grep -q .; then "
               f"echo '{MAPPED_USER} queue not empty after 60 s'; exit 1; fi; echo '{MAPPED_USER} queue empty'"]
# The cell's start, on the shared /home (every login node sees it): the sacct check counts only pilots SUBMITTED since
# (`sacct -S` also lists a job submitted before it that was still running after it).
_STAMP = "~/.hpcb-stop-while-provisioning.start"
SETUP = [f"date +%Y-%m-%dT%H:%M:%S > {_STAMP} && echo stamped $(cat {_STAMP})"]

# Right after the stop: what the mapped account's queue held (evidence for the bundle — the pilot may be pending,
# running, or not submitted yet, so nothing is expected of it).
MIDRUN_HOOKS = [
    {"name": "queue_at_stop", "after_tool": "stop_endpoint", "nth": 1,
     "cmd": f"squeue -u {MAPPED_USER} -h -o '%i %j %T'; echo queue-listed", "timeout": 60},
]
POSTCHECK_DELAY_S = 240
_POLL = 32   # × 15 s after the 240 s settle: at most 720 s after the agent's turn
POSTCHECKS = [
    # non-vacuity: the queued check made the facility submit a pilot during THIS cell (else "gone" proves nothing)
    {"name": "mep_pilot_submitted",
     "cmd": (f"s=$(cat {_STAMP} 2>/dev/null); [ -n \"$s\" ] || {{ echo NO_STAMP; exit 0; }}; "
             f"sacct -u {MAPPED_USER} -X -n -P -S \"$s\" -o JobID,JobName,Submit 2>&1 "
             "| awk -F'|' -v s=\"$s\" '$2 ~ /parsl/ && $3 >= s' | grep parsl "
             "|| echo \"no pilot of the mapped account submitted since $s\""),
     "expect_present": "parsl"},
    # ...and none of it is left: polled until the facility's idle-release took it, bounded
    {"name": "mep_pilot_idle_released",
     "cmd": (f"for i in $(seq 1 {_POLL}); do squeue -u {MAPPED_USER} -h -o %j | grep -q parsl || break; sleep 15; "
             f"done; squeue -u {MAPPED_USER} -h -o %j"),
     "expect_absent": "parsl", "timeout": _POLL * 15 + 60},
]
CLEANUP = [f"rm -f {_STAMP}; echo cleaned"]
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true"]
TEARDOWN = "delete"
