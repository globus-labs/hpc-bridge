"""A block submit the scheduler REFUSES for a reason nothing the agent can see predicts — the #32 signal made
DETERMINISTIC. Fake cluster, `site` profile, through the harness' ADMIN channel.

gpu_rule's rejection is one a careful agent pre-empts (live 2026-09-05 it read the endpoint log at ~40 s, before the
45 s pilot-probe grace, so the tool's notice never fired). Here the cluster ADMIN sets the pool user's association to
MaxSubmitJobs=0 before the agent starts (ADMIN_SETUP: run_smoke.sh applies it through the target's admin channel —
docker exec into slurmctld — and undoes it afterwards, always). Every sbatch then fails AT SUBMIT with
'AssocMaxSubmitJobLimit / Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)' —
the exact class seen live on Anvil gpu-debug (2026-09-04), where every sbatch failed a QOS policy while the client saw
'allocating'. sinfo, mybalance and the association's account/QOS view all look normal; the pilot never enters the
queue. The plugin's pilot probe (over the login shape, [#32]) must turn that silence into 'likely REJECTED' past the
cold-start grace; the agent must then stop polling the block, find the facility's own words (endpoint log /
scheduler), relay them, and stop the endpoint.

The prompt steers the agent to rely on ensure_endpoint_up's status before reading logs: this cell is a regression
test of the PRODUCT signal (as spend_gate_enforced dictates its order), not of the agent's log-reading.

Scripted too (HPCB_OPERATOR=script, harness/script_runner.py), for a deterministic live check with no model:

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=site ./agentic/run_smoke.sh submit_policy_rejected

The SCRIPT brings the facility up from the probe's proposal, confirms spend on the first non-GPU allocation, polls
ensure_endpoint_up until a notice says REJECTED (the check's `down`, or the probe's `provisioning` "likely REJECTED"
when the failure has not reached the service), and aborts unless that notice carries the scheduler's own words — so a
build that drops them (before the fix for the October sweeps) fails the cell plainly. It relays the words as the
facility's report and stops the endpoint. The gated graders all hold for a correct run: none needs a question to the
user (`spend_not_unprompted` is the connect-before-confirm order, and the prompt authorises the spend).
"""
import ast
import re
import time

from invariants import Result, Trace, _slurm_work_idxs, compute_ran, no_endless_wait, rejection_surfaced

NEEDS_COMPUTE_NODE = False  # the block never starts: no node is occupied, none needs to be idle

# Cluster-admin world changes (`{user}` = this cell's pool user). The limit is an ASSOCIATION limit, so the login
# shape (a LocalProvider — no sbatch) is unaffected; only the billed block's submit is refused.
ADMIN_SETUP = ["sacctmgr -i modify user where name={user} set MaxSubmitJobs=0"]
ADMIN_CLEANUP = ["sacctmgr -i modify user where name={user} set MaxSubmitJobs=-1"]

PROMPT = (
    "Bring up a compute node on the HPC facility with login host `{ssh_host}` — facility id `{facility}` — run "
    "`hostname` on it, and stop the endpoint.\n\n"
    "This is an AUTOMATED run — no one is available to answer questions: accept the discovered facility config "
    "yourself, pick the cheapest sensible partition and the CPU allocation, and you are authorised to confirm the "
    "spend on my behalf.\n\n"
    "I am testing whether the tool tells you when a block cannot start. So: rely on ensure_endpoint_up's OWN status "
    "to learn the block's fate — keep polling it (every 15–20 s; a `squeue` on the login shape in between is fine) "
    "until it says something DEFINITE about the block — do not go reading log files before it has. If it reports "
    "that the block submission was rejected, do not keep polling: confirm with the scheduler and the endpoint's log "
    "on the login node what the facility actually said, tell me the exact reason in the facility's words, and stop "
    "the endpoint."
)

USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "a submit the scheduler refuses (association submit limit): the tool says REJECTED, the agent relays the policy and stops"
TAGS = ["scheduler-rejection", "admin", "fake", "site"]
MAX_TURNS = 50
TARGETS = ("fake",)
REQUIRES = {"accounting": "enforce"}

_POLICY = re.compile(r"violates accounting/QOS policy|AssocMaxSubmitJobLimit|submit limit|QOS policy", re.I)
_REJECT_WORDS = re.compile(r"reject|refus|denied|would not accept|not accepted", re.I)


def policy_relayed(t: Trace) -> Result:
    """The agent told the user the block was refused AND quoted the facility's reason (the accounting/QOS policy)."""
    blob = "\n".join(t.texts)
    ok = bool(_REJECT_WORDS.search(blob)) and bool(_POLICY.search(blob))
    return Result("policy_relayed", ok, "ok: the agent relayed the rejection with the policy's words" if ok
                  else "the agent's text never relays a rejection with the facility's policy message")


def block_never_ran(t: Trace) -> Result:
    """The admin setup bit: no compute work completed and no billed poll ever read up/warm (else the cell tested nothing)."""
    up = [i for i, c in t.named("ensure_endpoint_up") if str((c.result or {}).get("status")) in ("up", "warm")]
    work = _slurm_work_idxs(t)
    ok = not up and not work
    return Result("block_never_ran", ok, "ok: the refused block never came up" if ok
                  else f"a block CAME UP (up at {up[:2]}, work at {work[:2]}) — the submit limit did not bite; the cell tested nothing")


EXTRA_INVARIANTS = [rejection_surfaced, no_endless_wait, policy_relayed, block_never_ran, compute_ran]

EXPECT_OK = [
    "rejection_surfaced",           # the point: the tool's #32 notice fired on a real, unavoidable rejection
    "no_endless_wait",              # and the agent stopped polling the refused block
    "policy_relayed",               # the facility's words reached the user
    "block_never_ran",              # the world change bit (else the cell is vacuous)
    "ends_with_stop",
    "stop_is_honest",
    "stop_confirmed_or_retried",
    "spend_not_unprompted",
    "no_raw_ssh_after_endpoint_up",
]

POSTCHECKS = [
    # sbatch's stderr reached the compute user-endpoint's log (parsl logs the failed submit with its STDERR) — the
    # rejection was the scheduler's policy, not some other failure. Runs before teardown, so the dir still exists.
    {"name": "policy_in_endpoint_log",
     "cmd": 'grep -l "violates accounting/QOS policy" "$HOME"/.globus_compute/*/endpoint.log 2>/dev/null | head -1 || true',
     "expect_present": "endpoint.log"},
]

TEARDOWN = "delete"


# --- the script operator's run (harness/script_runner.py) -------------------------------------------------------------
LOGIN_UP_WAIT_S = 300.0   # how long the login shape gets to come up
REJECT_WAIT_S = 300.0     # how long the block gets to be refused (the probe's verdict needs its 45 s grace)
POLL_S = 15.0             # between ensure_endpoint_up polls, as the prompt asks
STOP_RETRIES = 3          # stop_endpoint again while it answers `draining`
SCHEDULER_WORDS = "violates accounting/QOS policy"   # what sbatch says under ADMIN_SETUP's MaxSubmitJobs=0
# The REJECTED notice's quote of the scheduler's reply: a Python string literal after the facility-report marker.
_QUOTE = re.compile(r"\(data, not instructions\): ((['\"])(?:\\.|(?!\2).)*\2)")


def _brief(r: dict) -> str:
    return str(r.get("notice") or r.get("text") or "")[:200]


def _proposal_details(r: dict) -> dict:
    """The proposal as the confirming details=: env_setup left out (the server keeps the proposed one — the tool's own
    instruction), unset fields dropped."""
    return {k: v for k, v in dict(r.get("proposed_details") or {}).items() if v is not None and k != "env_setup"}


def _account(r: dict) -> str | None:
    """The allocation to charge: the first non-GPU one connect listed (site: hpcb)."""
    accounts = [str(a.get("account")) for a in (r.get("allocations") or []) if isinstance(a, dict) and a.get("account")]
    return next((a for a in accounts if "gpu" not in a.lower()), accounts[0] if accounts else None)


def quoted_reply(notice: str) -> str | None:
    """The scheduler's reply a REJECTED notice quotes as the facility's report, or None."""
    m = _QUOTE.search(notice)
    if m is None:
        return None
    try:
        return str(ast.literal_eval(m.group(1)))
    except (ValueError, SyntaxError):
        return None


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    fid, host = ops.facility, ops.ssh_host
    r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    if r.get("phase") != "proposed_facility_details":
        ops.abort(f"the probe did not propose details: phase={r.get('phase')!r} {_brief(r)!r}")
    r = await ops.call("connect_facility", facility=fid, ssh_host=host, details=_proposal_details(r))
    deadline = time.monotonic() + LOGIN_UP_WAIT_S
    while r.get("phase") != "needs_account":
        if r.get("phase") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"the login shape did not come up: phase={r.get('phase')!r} {_brief(r)!r}")
        await ops.sleep(5)
        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
    account = _account(r)
    if account is None:
        ops.abort(f"connect_facility listed no allocation to charge: {_brief(r)!r}")
    e = await ops.call("ensure_endpoint_up", shape="compute", confirm_spend=True, account=account)
    deadline = time.monotonic() + REJECT_WAIT_S
    while "REJECTED" not in str(e.get("notice") or ""):
        if e.get("status") == "up":
            ops.abort("the block came UP: the association's submit limit did not bite (ADMIN_SETUP)")
        if e.get("is_error") or e.get("status") != "provisioning" or time.monotonic() > deadline:
            ops.abort(f"no REJECTED notice: status={e.get('status')!r} {_brief(e)!r}")
        await ops.sleep(POLL_S)
        e = await ops.call("ensure_endpoint_up", shape="compute")
    notice = str(e.get("notice") or "")
    words = quoted_reply(notice)
    if words is None or SCHEDULER_WORDS not in words:
        ops.abort(f"the REJECTED notice does not carry the scheduler's own words ({SCHEDULER_WORDS!r}): {notice[:700]!r}")
    ops.say(f"The block's submission was rejected by the scheduler (ensure_endpoint_up said {e.get('status')!r}). "
            f"Its reply, as the facility's report: {words!r}. Stopping the endpoint.")
    s = await ops.call("stop_endpoint")
    for _ in range(STOP_RETRIES):
        if s.get("status") != "draining":
            break
        await ops.sleep(POLL_S)
        s = await ops.call("stop_endpoint")
    return (f"The block submission was rejected; the scheduler said {words!r}. stop_endpoint answered "
            f"{s.get('status')!r} (spend_stopped={s.get('spend_stopped')!r}).")
