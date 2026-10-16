"""The PBS facility-MEP path on the FAKE cluster (`pbs_mep` profile) — ALCF Polaris's shape (#179), locally: a root-run
multi-user endpoint on the OpenPBS login node, our Globus identity mapped to `hpcbmep`, and a user-endpoint template
like Polaris's published one (PBSProProvider; `queue` and `account` REQUIRED; `#PBS -l filesystems=home` by default;
240 s idle-release; the facility's own endpoint venv on the workers' PATH, so the catalog entry sends no worker_init).
Before this, the Polaris entry had only been proven agent-free (registry_reprove.py): nothing exercised, through an
agent, the `compute.key_map` rename (partition -> queue) at the wire, `account_required` on PBS, the filesystems
default, the 240 s window, or a facility-endpoint stop that names a PBS job and `qdel`.

The sequence: attach (zero SSH) -> a confirmed block charged to `hpcb` on the facility's DEFAULT queue (the agent passes
no partition: the registry entry's default, `debug`, must reach PBS as `-q debug` through the key_map — the template
has no default queue, and the strict schema refuses a `partition` key) -> ONE compute run printing `$PBS_JOBID`, the
queue PBS ran it in, the mapped user -> a stop that issues no cancel and says so, naming the PBS job and `qdel <id>`.

Graded on what the plugin delivers: the receipt's `block` (queue `debug` from the registry, account `hpcb` set by the
call) and the PBS word "queue" in every terms line; the queue PBS reports from inside the job; #178's stop graders
(fake_mep_stop_names_block), with `qdel`. World: right after the stop, a hook lists the mapped account's RUNNING job in
queue `debug` charged to `hpcb` (no cancel was issued; the queue and the account reached the scheduler) — or, when
the operator waited past the idle window before stopping, a block that had idled out before the stop (the premise
not met, said so); after the idle window the queue is empty (the 240 s release held). On this cluster a pilot without
a filesystems request is HELD (the polaris rule), so a compute result also proves the template's filesystems
default rendered.

Autonomous, `cmd` hook only: the free CLI operators (codex / pi / hermes21) drive it too. Fake cluster only. ~6 min.
"""
import re

from fake_mep_compute import MAPPED_USER, mep_identity_mapped, strict_keys_dropped
from fake_mep_stop_names_block import cancel_command_given as _cancel_command_given
from fake_mep_stop_names_block import mep_stop_names_block, no_cancel_claimed, no_probe_after_stop
from invariants import Result, Trace, _billed_start_idxs, _shape, compute_ran, first_result_s
from mep_compute_only import mep_no_login_shape_submit, mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-pbs-mep"          # the pbs_mep profile's local catalog entry (hpcb-pbs-mep-catalog)
DEFAULT_QUEUE = "debug"               # the entry's defaults.partition — NOT the PBS server's default queue (workq)
ACCOUNT = "hpcb"
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER          # a RUNNING block of the mapped user satisfies the node gate
SERIAL = "hpcbmep"                     # the fake-MEP cells' group: one mapped account, one user endpoint, one queue (here
                                       # the PBS stack's hpcbmep; the Slurm `mep` stack never runs beside it)
TARGETS = ("fake",)
# pbs: the PBS stack; mep: a facility endpoint; site_rule: the polaris rule HOLDS a pilot with no filesystems request,
# which is what makes the template's `filesystems=home` default load-bearing here
REQUIRES = {"scheduler": "pbs", "mep": "consent-free", "site_rule": "filesystems"}

PROMPT = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; it is a "
    "catalogued facility — do NOT pass an ssh_host and do NOT probe anything). It is a PBS cluster reached through "
    "the facility's own multi-user endpoint, so expect it to attach immediately with no login node to warm; it is "
    "compute-only and needs a project (account) to charge.\n\n"
    "Then start a compute block charged to my project `hpcb` on the facility's DEFAULT queue — do not pass a "
    "partition or a queue: `ensure_endpoint_up(shape='compute', account='hpcb', confirm_spend=True)` — and keep "
    "polling `ensure_endpoint_up(shape='compute')` until it reports up/warm ('provisioning' means the block is still "
    "being allocated; call again). Tell me which queue the block uses and where that choice came from. Run EXACTLY "
    "this as ONE `run_shell` on the compute shape:\n\n"
    "    echo JOB=$PBS_JOBID QUEUE=$PBS_QUEUE; whoami; hostname\n\n"
    "Then I am done with the block: call `stop_endpoint`. Tell me exactly what it says — whether anything is still "
    "running or billing, which scheduler job, until when, and the exact command I can use to end it myself.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions; you are authorised to confirm the spend on my "
    "behalf. Report the JOB= line and what each step returned."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("PBS facility MEP (Polaris-shaped): zero-SSH attach; the registry's default queue reaches PBS as `-q` "
           "through key_map, charged to the account; compute as the mapped user; the stop names the job and `qdel`")
TAGS = ["mep", "zero-ssh", "pbs", "polaris", "key-map", "fake", "issue-179", "issue-178", "spend-terms", "free-operators"]
MAX_TURNS = 45

_RUN = re.compile(r"JOB=(\S*)\s+QUEUE=(\S*)")
_PBS_JOB = re.compile(r"^\d+(?:\[\d*\])?\.\S+$")   # PBS job ids carry their server: 3.pbsserver (an array: 4[].pbsserver)
# A tool's statement of a block's terms (notices._terms_line): "<lead>: queue 'debug' (…)" or "… the facility's default
# queue (hpc-bridge sends none)". The word is the facility's: "queue" on PBS, "partition" elsewhere.
_TERMS_WORD = re.compile(r"(?:Block terms|The next block's terms|The current terms|The new block's terms): "
                         r"(?:the facility's default )?(partition|queue)\b")


def _res(c) -> dict:
    return c.result or {}


def _receipt(t: Trace) -> tuple[int, dict] | None:
    """The first billed start whose result carries `block` — the plugin's receipt of the block it confirmed."""
    for k in _billed_start_idxs(t):
        block = _res(t.calls[k]).get("block")
        if isinstance(block, dict):
            return k, block
    return None


def default_queue_receipt(t: Trace) -> Result:
    """The confirmed start's receipt (`block`) names the registry's default queue (`debug`, queue_source `registry`: the
    agent passed no partition) and the account the call set (`hpcb`, this call's or an earlier one's), on a
    compute-only facility that requires an account."""
    got = _receipt(t)
    if got is None:
        return Result("default_queue_receipt", False, "no confirmed start carried a `block` receipt")
    k, b = got
    why = []
    if b.get("queue_source") in ("this_call", "earlier_call"):
        # the prompt says not to; the start is fine, but it says nothing about the default reaching PBS
        why.append(f"the agent named the queue itself ({b.get('queue')!r}, queue_source {b.get('queue_source')!r}): "
                   "the registry-default path was not exercised")
    elif b.get("queue") != DEFAULT_QUEUE or b.get("queue_source") != "registry":
        why.append(f"queue {b.get('queue')!r} from {b.get('queue_source')!r} (want {DEFAULT_QUEUE!r} from 'registry')")
    if b.get("account") != ACCOUNT or b.get("account_source") not in ("this_call", "earlier_call"):
        why.append(f"account {b.get('account')!r} from {b.get('account_source')!r} (want {ACCOUNT!r}, set by a call)")
    if b.get("account_required") is not True or b.get("compute_only") is not True:
        why.append(f"account_required={b.get('account_required')!r}, compute_only={b.get('compute_only')!r} (want both true)")
    return Result("default_queue_receipt", not why,
                  f"ok: call {k} started queue {DEFAULT_QUEUE!r} (the registry's default) charged to {ACCOUNT!r}"
                  if not why else f"call {k}: " + "; ".join(why))


def terms_say_queue(t: Trace) -> Result:
    """The tools state the block's terms in PBS's word: connect_facility's `block` names the default queue from the
    registry, and every terms line in any notice says "queue", never "partition" (#193: "on a PBS facility every one
    of these texts says 'queue'")."""
    lines = []
    for i, c in enumerate(t.calls):
        for m in _TERMS_WORD.finditer(str(_res(c).get("notice") or "")):
            lines.append((i, m.group(1)))
    connect = next((_res(c).get("block") for _i, c in t.named("connect_facility")
                    if isinstance(_res(c).get("block"), dict)), None)
    why = []
    if connect is None:
        why.append("no connect_facility result stated the next block's terms (`block`)")
    elif connect.get("queue") != DEFAULT_QUEUE or connect.get("queue_source") != "registry":
        why.append(f"connect's terms name queue {connect.get('queue')!r} from {connect.get('queue_source')!r}")
    if not lines:
        why.append("no notice stated the block's terms")
    wrong = [i for i, w in lines if w != "queue"]
    if wrong:
        why.append(f"terms said 'partition' on a PBS facility at call(s) {wrong}")
    return Result("terms_say_queue", not why,
                  f"ok: {len(lines)} terms line(s), all 'queue'; connect named {DEFAULT_QUEUE!r} from the registry"
                  if not why else "; ".join(why))


def _printed_run(t: Trace) -> tuple[int, str, str] | None:
    """The completed compute run that printed `JOB=<id> QUEUE=<queue>`: (call index, job id, queue)."""
    for i, c in t.named("run_shell"):
        if _shape(c) == "compute" and _res(c).get("phase") == "complete":
            m = _RUN.search(str(_res(c).get("stdout") or ""))
            if m:
                return i, m.group(1), m.group(2)
    return None


def queue_reached_scheduler(t: Trace) -> Result:
    """PBS ran the block in the queue the receipt names: the run's `$PBS_QUEUE` is the default queue, and `$PBS_JOBID`
    is a PBS job id. The template has no default queue and its schema refuses `partition`, so this is the key_map's
    rename (partition -> queue) arriving at `qsub -q`."""
    got = _printed_run(t)
    if got is None:
        return Result("queue_reached_scheduler", False, "no completed compute run printed JOB=<id> QUEUE=<queue>")
    i, job, queue = got
    rec = _receipt(t)
    want = (rec[1].get("queue") if rec else None) or DEFAULT_QUEUE
    why = []
    if queue != want:
        why.append(f"PBS ran it in queue {queue!r}, the receipt says {want!r}")
    if not _PBS_JOB.match(job):
        why.append(f"JOB={job!r} is not a PBS job id (<n>.<server>)")
    return Result("queue_reached_scheduler", not why,
                  f"ok: call {i} ran in PBS job {job} on queue {queue!r}" if not why else f"call {i}: " + "; ".join(why))


def cancel_command_given(t: Trace) -> Result:
    """#178 on PBS: the stop's notice and the agent's words give `qdel <id>` for the job the run printed."""
    return _cancel_command_given(t, cancel="qdel")


def queue_source_relayed(t: Trace) -> Result:
    """REPORTED: the agent told the user the queue and that it was the default (the prompt asks where it came from)."""
    blob = "\n".join(t.texts)
    ok = bool(re.search(rf"\b{DEFAULT_QUEUE}\b", blob)) and bool(re.search(r"\bdefault\b|\bregistry\b", blob, re.I))
    return Result("queue_source_relayed", ok, f"ok: the agent named {DEFAULT_QUEUE!r} as the default" if ok
                  else f"the agent's text never names {DEFAULT_QUEUE!r} as the default/registry queue")


# REPORTED (not in EXPECT_OK): strict_keys_dropped (the plugin's note that `interface` was dropped for the strict
# schema), first_result_s, queue_source_relayed (phrasing)
EXTRA_INVARIANTS = [mep_zero_ssh, mep_no_login_shape_submit, mep_identity_mapped, compute_ran, default_queue_receipt,
                    terms_say_queue, queue_reached_scheduler, mep_stop_names_block, no_cancel_claimed,
                    cancel_command_given, no_probe_after_stop, mep_stop_is_draining_only, strict_keys_dropped,
                    queue_source_relayed, first_result_s]

EXPECT_OK = [
    "mep_zero_ssh",                 # attached, never probed, never SSH'd
    "mep_no_login_shape_submit",    # the login shape was refused, not dispatched
    "mep_identity_mapped",          # the run executed as hpcbmep
    "compute_ran",                  # the strict PBS manager accepted what the plugin sent; the pilot was not HELD
    "default_queue_receipt",        # the receipt: debug from the registry, hpcb set by the call
    "terms_say_queue",              # PBS's word in every terms line
    "queue_reached_scheduler",      # PBS ran it in debug: the key_map rename reached qsub -q
    "mep_stop_names_block",         # the stop names the PBS job the run printed
    "no_cancel_claimed",            # ...leads with NO CANCEL WAS ISSUED, never spend_stopped=True
    "cancel_command_given",         # ...and the user gets `qdel <id>`
    "no_probe_after_stop",          # nothing sent to the facility after the stop
    "mep_stop_is_draining_only",
    "stop_is_honest",
    "spend_not_unprompted",
    "ends_with_stop",
]

# Right after the stop: the mapped account's block is STILL RUNNING — no cancel was issued — in queue `debug`, charged
# to `hpcb` (qselect filters on both, so the queue and the account reached PBS). Pool users can see hpcbmep's jobs: the
# profile sets query_other_jobs. The premise can fail on its own: an operator that waits past the 240 s idle window
# between the run and the stop finds the block already released, which is not the plugin's doing — so a block that
# FINISHED at least 30 s before this check (job history: qselect -x, its mtime) also passes, said as the premise not
# met. One that is exiting now, or finished within the last 30 s, ended at the stop: that fails (the check fires right
# after the stop's result). The token `BLOCK_AT_STOP:` is printed only on a pass.
_Q = f"-u {MAPPED_USER} -q {DEFAULT_QUEUE} -A {ACCOUNT}"
_AGE = ("python3 -c 'import sys, time; print(int(time.time() - time.mktime(time.strptime(sys.argv[1], "
        "\"%a %b %d %H:%M:%S %Y\"))))' \"$end\" 2>/dev/null || echo -1")
_AT_STOP = (
    f"ids=$(qselect {_Q} -s R 2>/dev/null); "
    "if [ -n \"$ids\" ] && qstat -f $ids 2>/dev/null | grep -q 'Job_Name = parsl'; then "
    f"echo \"BLOCK_AT_STOP: still running in queue {DEFAULT_QUEUE}, account {ACCOUNT} (no cancel was issued):\" $ids; "
    "qstat -f $ids | grep -E 'Job_Name|queue =|Account_Name|job_state'; "
    f"elif ex=$(qselect {_Q} -s E 2>/dev/null) && [ -n \"$ex\" ]; then "
    "echo \"no block at the stop: job(s)\" $ex \"EXITING right now — ended at the stop\"; "
    f"else last=$(qselect -x {_Q} -s F 2>/dev/null | sort -t. -k1,1n | tail -n 1); "
    f"if [ -z \"$last\" ]; then echo \"no block at the stop: no parsl job of {MAPPED_USER} in {DEFAULT_QUEUE}/{ACCOUNT}, "
    "running or finished\"; "
    "else info=$(qstat -x -f \"$last\" 2>/dev/null); "
    "end=$(printf '%s\\n' \"$info\" | sed -n 's/^ *mtime = //p' | head -n 1); "
    f"age=$({_AGE}); "
    "if ! printf '%s\\n' \"$info\" | grep -q 'Job_Name = parsl'; then "
    f"echo \"no block at the stop: the newest finished job of {MAPPED_USER} in {DEFAULT_QUEUE}/{ACCOUNT}, $last, is not a "
    "parsl block\"; "
    "elif [ \"$age\" -ge 30 ]; then "
    "echo \"BLOCK_AT_STOP: premise not met — job $last had already idled out ${age} s before this check (before the "
    "stop: there was nothing left to cancel)\"; "
    "else echo \"no block at the stop: job $last finished ${age} s before this check — at the stop\"; fi; fi; fi"
)
MIDRUN_HOOKS = [
    # `when_endpoint`: the first stop that reached the facility's endpoint (any answer — a lying `down` too: that is
    # when the evidence matters most). A stop sent before anything was attached answers "no endpoint was up" with no
    # endpoint_id; read there, the queue holds no block and the hook fails while the real stop goes unchecked
    # (a blind opening batch: hermes21, live 2026-10-15).
    {"name": "block_listed_at_stop", "after_tool": "stop_endpoint", "when_endpoint": True, "nth": 1, "cmd": _AT_STOP,
     "expect_any_host": "BLOCK_AT_STOP:", "timeout": 60},
]
# Only the facility's idle-release (max_idletime 240 s in the template) reclaims the block: wait past it (the idle clock
# starts at the last command, before the stop and the agent's reply), then check the mapped account's queue. An exit 3
# when qstat is missing, so an empty listing can only mean an empty queue. ADMIN_CLEANUP (root on the PBS server) makes
# sure nothing of the mapped user outlives the cell — a HELD pilot would otherwise sit there for good.
POSTCHECK_DELAY_S = 300
POSTCHECKS = [{"name": "mep_block_idle_released",
               "cmd": f"command -v qstat >/dev/null || {{ echo NO-QSTAT; exit 3; }}; qstat -u {MAPPED_USER} -w 2>&1; true",
               "expect_absent": "parsl"}]
ADMIN_CLEANUP = [f"ids=$(qselect -u {MAPPED_USER} 2>/dev/null); [ -z \"$ids\" ] || qdel $ids 2>/dev/null; true"]
TEARDOWN = "delete"
