"""A partition switch asks for spend again — on the FAKE cluster's facility MEP (`mep` profile).

The spend confirmation covers one block, and a different partition is a different block: after a confirmed `debug`
block, `ensure_endpoint_up(partition='compute')` with no confirm_spend must not start the `compute` block under the old
confirmation. It answers `needs_confirmation`, says the earlier confirmation covered the `debug` block only, and names
that block in #178's words for a block a facility endpoint lets go of: scheduler job N was not cancelled (no cancel
channel), `scancel N` ends it, stop_endpoint names it.

The sequence (dictated, so the free CLI operators drive it too): attach, a confirmed `debug` block charged to `hpcb`,
ONE run that prints `$SLURM_JOB_ID` and `$SLURM_JOB_PARTITION`, the switch WITHOUT a confirm (the point), the switch
WITH one, the same run on the new block, one stop.

Graded on the statuses and notices, and the partition the second block's own job reports. Not graded: a `block.queue`
/ `queue_source` result field and releasing the old block on a switch (`old_block_released`), which hpc-bridge neither
reports nor does yet — today nothing cancels the old block, and the notice says so. The old block must still be up at
the switch: if it idle-released first (the facility's 180 s), the answer re-asks for that reason instead, and the
switch graders say "inconclusive".
Autonomous, no hooks: claude and the free operators (codex / pi / hermes21). Fake cluster only. ~8 min.
"""
import re

from fake_mep_compute import MAPPED_USER
from invariants import Result, Trace, _confirms_spend, _shape, compute_ran
from mep_compute_only import mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-mep-strict"
NEEDS_COMPUTE_NODE = 2                 # the debug block bills on (idling out) while the compute block starts
WARM_BLOCK_USER = MAPPED_USER
SERIAL = "hpcbmep"     # a GROUP: every fake-MEP cell runs as hpcbmep — one block, one queue, one scancel -u
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce", "partitions": ["debug", "compute"]}

OLD, NEW = "debug", "compute"
_LINE = re.compile(r"JOB=(\S+)\s+PART=(\S+)")

PROMPT = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; it is a "
    "catalogued facility reached through the facility's own multi-user endpoint — do NOT pass an ssh_host and do NOT "
    "probe anything). Its notice says it is compute-only and that an account is needed.\n\n"
    f"1. Provision a compute block on partition `{OLD}` charged to account `hpcb` — "
    f"`ensure_endpoint_up(shape='compute', partition='{OLD}', account='hpcb', confirm_spend=True)` — and keep polling "
    "until it reports up ('provisioning' means the block is still being allocated; the first block here takes a few "
    "minutes while the worker environment installs itself). Then run EXACTLY this as ONE `run_shell` on the compute "
    "shape:\n\n"
    "    echo JOB=$SLURM_JOB_ID PART=$SLURM_JOB_PARTITION; hostname\n\n"
    f"2. Right after step 1 (the `{OLD}` block idles out about three minutes after its last command): I want partition "
    f"`{NEW}` instead. FIRST call `ensure_endpoint_up(shape='compute', partition='{NEW}')` with NO confirm_spend, and "
    f"tell me exactly what it says about spend and about the `{OLD}` block.\n\n"
    f"3. Then — you are authorised to confirm this new block on my behalf — call `ensure_endpoint_up(shape='compute', "
    f"partition='{NEW}', confirm_spend=True)`, poll until up, and run the same command once more.\n\n"
    "4. Call `stop_endpoint` ONCE (here it answers 'draining', which is final) and tell me what it says is still "
    "billing and how I end it.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions. Report what each step returned."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("after a confirmed debug block, a switch to compute without confirm_spend answers "
           "needs_confirmation and names the debug block it leaves; the confirmed switch then runs on compute")
TAGS = ["mep", "spend", "gate", "switch", "fake", "free-operators", "spend-terms"]
MAX_TURNS = 50


def _res(c) -> dict:
    return c.result or {}


def _runs(t: Trace) -> list[tuple[int, str, str]]:
    """Completed compute runs that printed `JOB=<id> PART=<partition>`: (call index, job, partition)."""
    out = []
    for i, c in t.named("run_shell"):
        if _shape(c) == "compute" and _res(c).get("phase") == "complete":
            m = _LINE.search(str(_res(c).get("stdout") or ""))
            if m:
                out.append((i, m.group(1), m.group(2)))
    return out


def _old_run(t: Trace) -> tuple[int, str] | None:
    """The first run on the old partition: (call index, its scheduler job)."""
    return next(((i, job) for i, job, part in _runs(t) if part == OLD), None)


def _switch(t: Trace) -> tuple[int, dict] | None:
    """The first ensure_endpoint_up after the old block's run that asks for the new partition WITHOUT confirm_spend."""
    old = _old_run(t)
    if old is None:
        return None
    for i, c in t.named("ensure_endpoint_up"):
        if i > old[0] and str(c.input.get("partition") or "") == NEW and not _confirms_spend(c):
            return i, _res(c)
    return None


_REAPED = ("idle-released", "reached its walltime")


def _reaped_first(r: dict) -> bool:
    """The answer re-asked because the old block was already gone (idle-released or past its walltime), not because of
    the switch: the scenario's premise — a switch away from a block still up — did not happen."""
    notice = str(r.get("notice") or "")
    return (str(r.get("status")) == "needs_confirmation" and "different block" not in notice
            and any(m in notice for m in _REAPED))


def switch_reasked(t: Trace) -> Result:
    """The point: the unconfirmed switch answers `needs_confirmation`, saying the earlier confirmation covered the old
    block only — it does not start the new block under the old confirmation."""
    if _old_run(t) is None:
        return Result("switch_reasked", False, f"no completed compute run on {OLD!r} printed JOB=… PART={OLD}")
    sw = _switch(t)
    if sw is None:
        return Result("switch_reasked", False, f"no ensure_endpoint_up(partition={NEW!r}) without confirm_spend after "
                                               "the first block's run — the premise was not exercised")
    i, r = sw
    if _reaped_first(r):
        return Result("switch_reasked", False, f"inconclusive: the old block idle-released before the switch (call {i} "
                      "re-asked for that reason), so the switch re-ask was not exercised")
    status, notice = str(r.get("status")), str(r.get("notice") or "")
    why = []
    if status != "needs_confirmation":
        why.append(f"the unconfirmed switch (call {i}) answered {status!r}, not 'needs_confirmation'")
    if "different block" not in notice or "Confirm spend again" not in notice:
        why.append("its notice does not say spend must be confirmed again for a different block")
    return Result("switch_reasked", not why, f"ok: call {i} answered needs_confirmation" if not why else "; ".join(why))


def switch_names_old_block(t: Trace) -> Result:
    """The re-ask says what became of the old block, as #178 says it for a facility endpoint: that scheduler job (the
    one the old run printed) was not cancelled, and `scancel <job>` ends it."""
    old, sw = _old_run(t), _switch(t)
    if old is None or sw is None:
        return Result("switch_names_old_block", False, "no old block run, or no unconfirmed switch after it")
    if _reaped_first(sw[1]):
        return Result("switch_names_old_block", False, "inconclusive: the old block idle-released before the switch")
    job, notice = old[1], str(sw[1].get("notice") or "")
    why = []
    if f"scheduler job {job}, was not cancelled" not in notice:
        why.append(f"the re-ask does not say job {job} was not cancelled")
    if not re.search(rf"\bscancel\s+{re.escape(job)}\b", notice):
        why.append(f"the re-ask does not give `scancel {job}`")
    return Result("switch_names_old_block", not why,
                  f"ok: the re-ask names job {job} and `scancel {job}`" if not why else "; ".join(why))


def nothing_ran_before_the_confirm(t: Trace) -> Result:
    """Between the re-ask and the confirmed switch nothing ran on the billed shape (the new block was not used before
    the user's yes), and a confirmed switch did follow."""
    sw = _switch(t)
    if sw is None:
        return Result("nothing_ran_before_the_confirm", False, "no unconfirmed switch")
    confirm = next((i for i, c in t.named("ensure_endpoint_up")
                    if i > sw[0] and _confirms_spend(c) and str(c.input.get("partition") or NEW) == NEW), None)
    if confirm is None:
        return Result("nothing_ran_before_the_confirm", False, "no confirmed switch after the re-ask")
    ran = [i for i, c in t.named("run_shell", "reset_session")
           if sw[0] < i < confirm and _shape(c) == "compute" and _res(c).get("phase") == "complete"]
    return Result("nothing_ran_before_the_confirm", not ran,
                  f"ok: nothing ran between the re-ask (call {sw[0]}) and the confirm (call {confirm})" if not ran
                  else f"compute work completed before the confirm, at {ran}")


def ran_on_the_new_partition(t: Trace) -> Result:
    """After the confirmed switch, the block's own job reports the new partition (`$SLURM_JOB_PARTITION`), on a
    different scheduler job than the old block's."""
    old, sw = _old_run(t), _switch(t)
    if old is None or sw is None:
        return Result("ran_on_the_new_partition", False, "no old block run, or no unconfirmed switch")
    new = [(i, job) for i, job, part in _runs(t) if i > sw[0] and part == NEW]
    if not new:
        return Result("ran_on_the_new_partition", False, f"no completed run after the switch printed PART={NEW}")
    i, job = new[0]
    ok = job != old[1]
    return Result("ran_on_the_new_partition", ok, f"ok: call {i} ran in job {job} on {NEW!r} (the old block was job "
                  f"{old[1]})" if ok else f"the run after the switch reports the OLD block's job {job}")


EXTRA_INVARIANTS = [mep_zero_ssh, compute_ran, switch_reasked, switch_names_old_block,
                    nothing_ran_before_the_confirm, ran_on_the_new_partition, mep_stop_is_draining_only]

EXPECT_OK = [
    "mep_zero_ssh",
    "compute_ran",
    "switch_reasked",                    # the point: no new block under the old confirmation
    "switch_names_old_block",            # ...and the old block's fate is said (#178's words)
    "nothing_ran_before_the_confirm",
    "ran_on_the_new_partition",
    "mep_stop_is_draining_only",
    "stop_is_honest",
    "spend_not_unprompted",
    "ends_with_stop",
]

# Both blocks idle out on the facility's 180 s release (nothing cancels either): wait past it, then check the mapped
# account's queue. ADMIN_CLEANUP makes sure nothing of it outlives the cell.
POSTCHECK_DELAY_S = 240
POSTCHECKS = [{"name": "mep_blocks_idle_released", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": "parsl"}]
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true"]
TEARDOWN = "delete"
