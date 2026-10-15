"""#178 on the FAKE cluster (`mep` profile): a stop on a facility multi-user endpoint NAMES the block it cannot cancel.

A facility MEP gives hpc-bridge no cancel channel, so `stop_endpoint` issues no cancel at all: the block bills on until
the facility's idle-release (180 s here) or its walltime. Live on the lab cluster (2026-10-12) the old stop said only
"draining", the agent told the user spend stops in ~10 min, and Slurm job 1226 was still RUNNING when the user found
it by hand. Now the stop's notice LEADS with "NO CANCEL WAS ISSUED", names the block's scheduler job (`block_job_id`,
from the check that confirmed the block), the latest it can bill until, and the command the user runs to end it now
(`scancel <id>`); `spend_stopped` is never true on a facility endpoint.

The sequence: attach (zero SSH), a confirmed compute block charged to `hpcb`, ONE compute run that prints its own
`$SLURM_JOB_ID` (the scheduler's id for the block), then a stop. The prompt asks the agent to relay what the stop says
is left running and how to end it, and does NOT forbid checking afterwards: whether the agent follows the notice ("do
not check on it from here") is graded (`no_probe_after_stop`).

World: a hook right after the stop lists the mapped account's queue (`squeue -u hpcbmep`) and FAILS
(`harness:midrun_hooks`) unless a parsl block is still there — the stop really issued no cancel. The postcheck then
waits past the facility's idle-release and checks the block is gone: the notice's "latest it can bill until" held.

Autonomous, `cmd` hook only: the free CLI operators (codex / pi / hermes21) drive it too. Fake cluster only. ~6 min.
"""
import re

from fake_mep_compute import MAPPED_USER, mep_identity_mapped
from invariants import Result, Trace, _confirms_spend, _shape, compute_ran
from mep_compute_only import mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-mep-strict"       # the local catalog's id for the strict-schema manager (as fake_mep_compute)
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER          # a RUNNING block of the mapped user satisfies the node gate
SERIAL = True                          # one mapped identity — cells would share the user endpoint
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce"}

_JOB = re.compile(r"JOB=(\d\S*)")
_HEAD = "NO CANCEL WAS ISSUED"

PROMPT = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; it is a "
    "catalogued facility — do NOT pass an ssh_host and do NOT probe anything). It is reached through the facility's "
    "own multi-user endpoint, so expect it to attach immediately with no login node to warm; its notice says it is "
    "compute-only and that an account is needed.\n\n"
    "Then provision a compute block on partition `compute` charged to account `hpcb` — "
    "`ensure_endpoint_up(shape='compute', partition='compute', account='hpcb', confirm_spend=True)` — and keep "
    "polling until it reports up/warm ('provisioning' means the block is still being allocated; call again; the first "
    "block on this facility takes a few minutes because the worker environment installs itself). Run EXACTLY this as "
    "ONE `run_shell` on the compute shape:\n\n"
    "    echo JOB=$SLURM_JOB_ID; hostname; whoami\n\n"
    "Then I am done with the block: call `stop_endpoint`. Tell me exactly what it says — whether anything is still "
    "running or billing, which scheduler job, until when, and the exact command I can use to end it myself.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions; you are authorised to confirm the spend on my "
    "behalf. Report the JOB= line and what each step returned."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("#178: a facility-MEP stop issues no cancel and says so — it names the block's scheduler job, the latest it "
           "can bill until and `scancel <id>`; the agent relays them and does not probe after the stop")
TAGS = ["mep", "zero-ssh", "fake", "issue-178", "stop", "free-operators"]
MAX_TURNS = 45


def _res(c) -> dict:
    return c.result or {}


def _printed_job(t: Trace) -> tuple[int, str] | None:
    """The completed compute run that printed `JOB=<id>`: (call index, the scheduler's job id for the block)."""
    for i, c in t.named("run_shell"):
        if _shape(c) == "compute" and _res(c).get("phase") == "complete":
            m = _JOB.search(str(_res(c).get("stdout") or ""))
            if m:
                return i, m.group(1)
    return None


def _first_stop(t: Trace, after: int = -1) -> tuple[int, dict] | None:
    """The first stop_endpoint after call `after` that was not refused (a refusal answers `up`: the block still had
    our work on it, so nothing was let go)."""
    for i, c in t.named("stop_endpoint"):
        if i > after and str(_res(c).get("status")) != "up":
            return i, _res(c)
    return None


def mep_stop_names_block(t: Trace) -> Result:
    """The stop names the block the run printed: its `block_job_id` is the `$SLURM_JOB_ID` the compute run printed (the
    scheduler's own id for the block), and the notice names it too."""
    printed = _printed_job(t)
    if printed is None:
        return Result("mep_stop_names_block", False, "no completed compute run printed JOB=<id>")
    k, job = printed
    stop = _first_stop(t, k)
    if stop is None:
        return Result("mep_stop_names_block", False, f"no stop_endpoint after the run that printed job {job}")
    i, r = stop
    named, notice = r.get("block_job_id"), str(r.get("notice") or "")
    why = []
    if str(named) != job:
        why.append(f"block_job_id={named!r}, want the printed job {job!r}")
    if job not in notice:
        why.append(f"the notice never names job {job}")
    return Result("mep_stop_names_block", not why,
                  f"ok: the stop (call {i}) names job {job}, the one the run printed" if not why else "; ".join(why))


def no_cancel_claimed(t: Trace) -> Result:
    """The stop is honest about the cancel it did not issue: its notice LEADS with "NO CANCEL WAS ISSUED", and
    `spend_stopped` is not true (nothing here can cancel a facility's block)."""
    stop = _first_stop(t)
    if stop is None:
        return Result("no_cancel_claimed", False, "no stop_endpoint")
    i, r = stop
    why = []
    if not str(r.get("notice") or "").startswith(_HEAD):
        why.append(f"the notice does not lead with {_HEAD!r}: {str(r.get('notice') or '')[:80]!r}")
    if r.get("spend_stopped") is True:
        why.append("spend_stopped is true on a facility endpoint")
    return Result("no_cancel_claimed", not why,
                  f"ok: the stop (call {i}) leads with {_HEAD!r}, spend_stopped={r.get('spend_stopped')!r}"
                  if not why else "; ".join(why))


def cancel_command_given(t: Trace, cancel: str = "scancel") -> Result:
    """The user gets the command that ends the block: `scancel <id>` for the printed job is in the stop's notice AND in
    what the agent told the user. `cancel`: the scheduler's command (`qdel` on PBS: fake_pbs_mep_compute)."""
    printed = _printed_job(t)
    if printed is None:
        return Result("cancel_command_given", False, "no completed compute run printed JOB=<id>")
    k, job = printed
    stop = _first_stop(t, k)
    want = re.compile(rf"\b{re.escape(cancel)}\s+{re.escape(job)}\b")
    why = []
    if stop is None or not want.search(str(stop[1].get("notice") or "")):
        why.append(f"no stop notice gives `{cancel} {job}`")
    if not want.search("\n".join(t.texts)):
        why.append(f"the agent never told the user `{cancel} {job}`")
    return Result("cancel_command_given", not why, f"ok: `{cancel} {job}` in the notice and the agent's text"
                  if not why else "; ".join(why))


def _probes(t: Trace, after: int) -> list[tuple[int, str]]:
    """The calls after `after` that would send a task to the block: run_shell, reset_session, and a CONFIRMED
    ensure_endpoint_up. An unconfirmed ensure_endpoint_up answers needs_confirmation before any check, and a reconnect
    makes only web calls — neither sends anything (tests/test_mep_stop_names_block.py pins both)."""
    out = [(k, c.name) for k, c in t.named("run_shell", "reset_session") if k > after]
    out += [(k, "ensure_endpoint_up(confirm_spend=True)") for k, c in t.named("ensure_endpoint_up")
            if k > after and _confirms_spend(c)]
    return sorted(out)


def no_probe_after_stop(t: Trace) -> Result:
    """Nothing that could reach the block after the stop: no run_shell, reset_session or confirmed ensure_endpoint_up
    after the first stop that let the block go (stricter than after the last: a check between two stops is the same
    probe). Such a call is a task — it can keep the block alive or start a new one — and the notice says not to."""
    stop = _first_stop(t)
    if stop is None:
        return Result("no_probe_after_stop", False, "no stop_endpoint")
    i = stop[0]
    after = _probes(t, i)
    return Result("no_probe_after_stop", not after,
                  f"ok: nothing sent to the facility after the stop (call {i})" if not after else
                  f"called after the stop (call {i}): {after}")


EXTRA_INVARIANTS = [mep_zero_ssh, mep_identity_mapped, compute_ran, mep_stop_names_block, no_cancel_claimed,
                    cancel_command_given, no_probe_after_stop, mep_stop_is_draining_only]

EXPECT_OK = [
    "mep_zero_ssh",                 # attached, never probed, never SSH'd
    "mep_identity_mapped",          # the run executed as hpcbmep
    "compute_ran",
    "mep_stop_names_block",         # the point: the stop names the job the block printed
    "no_cancel_claimed",            # ...leads with NO CANCEL WAS ISSUED, never spend_stopped=True
    "cancel_command_given",         # ...and the user gets `scancel <id>`
    "no_probe_after_stop",          # the agent does not check from here after the stop
    "mep_stop_is_draining_only",
    "stop_is_honest",
    "spend_not_unprompted",
    "ends_with_stop",
]

# Right after the stop: the mapped account's block (parsl names it `parsl.<executor>.block-<n>.<time>`) is STILL in the
# queue — no cancel was issued. Fails
# (harness:midrun_hooks) unless squeue lists a parsl block of hpcbmep.
MIDRUN_HOOKS = [
    {"name": "block_listed_at_stop", "after_tool": "stop_endpoint", "nth": 1,
     "cmd": f"squeue -u {MAPPED_USER} -h -o '%i %j %T'", "expect_any_host": "parsl", "timeout": 60},
]
# Only the facility's idle-release (max_idletime 180 s in the template) reclaims the block: wait past it and the
# notice's bound, then check the mapped account's queue. ADMIN_CLEANUP makes sure nothing of it outlives the cell.
POSTCHECK_DELAY_S = 240
POSTCHECKS = [{"name": "mep_block_idle_released", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": "parsl"}]
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true"]
TEARDOWN = "delete"
