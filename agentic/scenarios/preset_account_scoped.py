"""HPC_BRIDGE_ACCOUNT reaches only the facility HPC_BRIDGE_MACHINE names — fake cluster, `mep` profile.

The env presets belong to the startup pin's machine. Applied to every catalogued facility, a user who once exported an
account for one machine would have every other facility's blocks charged to it, unasked — and on an account-required
facility it would even satisfy the account floor, so nothing asked. Here the server runs with `HPC_BRIDGE_ACCOUNT=hpcb-gpu` and no HPC_BRIDGE_MACHINE (the common case: an exported account, the
agentic flow), and the agent attaches to the fake MEP and confirms a block WITHOUT naming an account. Now the connect
says in one clause that the preset was not used (naming the variable, never its value), the account floor answers
`needs_account`, and the block is charged to the account the agent then passes, `hpcb`.

ADMIN_SETUP makes `hpcb-gpu` a valid account for the mapped user `hpcbmep` (and fails the cell if the grant did not
take), so a leak would be BILLED, not refused by Slurm: the world check (no `hpcb-gpu` job of hpcbmep submitted since
the cell started) then catches it; ADMIN_CLEANUP removes the association again. For the CLI operators (codex, pi,
hermes21) EXTRA_ENV is in the agent's own environment too, so the agent can read the preset and pass it itself: that is
reported apart (`agent_chose_the_preset`), never as the product's leak. Not graded: an `account_source` result field,
which hpc-bridge does not report yet. Autonomous: claude and the free operators. ~6 min.
"""
import re

from fake_mep_compute import MAPPED_USER
from invariants import Result, Trace, _confirms_spend, _shape, compute_ran
from mep_compute_only import mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-mep-strict"
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER
SERIAL = "hpcbmep"     # a GROUP: every fake-MEP cell runs as hpcbmep; and ADMIN_SETUP changes its associations
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce", "accounts": ["hpcb", "hpcb-gpu"]}

PRESET = "hpcb-gpu"
EXTRA_ENV = {"HPC_BRIDGE_ACCOUNT": PRESET}   # the SERVER's env only; HPC_BRIDGE_MACHINE stays unset
_START = "$HOME/.hpcb-preset-account-scoped.start"
_NAMED = re.compile(r"\bACCOUNT=hpcb(?![\w-])")
SETUP = [f'date +%Y-%m-%dT%H:%M:%S > "{_START}"']   # sacct's window: this cell's jobs only (hpcbmep is shared)
_HAS_GRANT = f"sacctmgr -n -P show assoc user={MAPPED_USER} account={PRESET} format=account | grep -qx {PRESET}"
ADMIN_SETUP = [f"sacctmgr -i add user {MAPPED_USER} account={PRESET} >/dev/null 2>&1; {_HAS_GRANT}"]  # fails if absent
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true",
                 f"for i in 1 2 3 4 5; do sacctmgr -i delete user name={MAPPED_USER} account={PRESET} >/dev/null 2>&1; "
                 f"{_HAS_GRANT} || break; sleep 3; done; true"]

PROMPT = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; it is a "
    "catalogued facility reached through the facility's own multi-user endpoint — do NOT pass an ssh_host and do NOT "
    "probe anything). Tell me anything its notice says about HPC_BRIDGE_ACCOUNT.\n\n"
    "Then bring up a compute block on partition `compute`: call `ensure_endpoint_up(shape='compute', "
    "partition='compute', confirm_spend=True)` and do NOT pass an account yourself. If hpc-bridge says an account is "
    "needed, use account `hpcb`: `ensure_endpoint_up(shape='compute', partition='compute', account='hpcb', "
    "confirm_spend=True)`. Keep polling until it reports up (the first block here takes a few minutes while the "
    "worker environment installs itself), then run EXACTLY this as ONE `run_shell` on the compute shape:\n\n"
    "    echo JOB=$SLURM_JOB_ID ACCOUNT=$SLURM_JOB_ACCOUNT; hostname\n\n"
    "Finally call `stop_endpoint` ONCE (here it answers 'draining', which is final). Tell me which account the block "
    "was charged to.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions; you are authorised to confirm the spend on my "
    "behalf."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("an exported HPC_BRIDGE_ACCOUNT with no HPC_BRIDGE_MACHINE is not applied to the facility the agent "
           "connects: the connect says so, the floor asks, and nothing is charged to the preset")
TAGS = ["mep", "spend", "account", "preset", "fake", "free-operators", "spend-terms"]
MAX_TURNS = 45


def _res(c) -> dict:
    return c.result or {}


def preset_clause_shown(t: Trace) -> Result:
    """The connect says, in one clause, that HPC_BRIDGE_ACCOUNT was not used here — and never prints its value."""
    connects = [(i, str(_res(c).get("notice") or "")) for i, c in t.named("connect_facility")
                if _res(c).get("phase") == "needs_account"]
    if not connects:
        return Result("preset_clause_shown", False, "no connect_facility reached needs_account")
    i, notice = connects[0]
    why = []
    if "HPC_BRIDGE_ACCOUNT applies only to" not in notice:
        why.append(f"the connect (call {i}) does not say HPC_BRIDGE_ACCOUNT applies only to the named machine")
    if PRESET in notice:
        why.append(f"the connect's notice prints the preset's value {PRESET!r}")
    return Result("preset_clause_shown", not why, f"ok: call {i} names the variable, not its value" if not why
                  else "; ".join(why))


def floor_asked_without_the_preset(t: Trace) -> Result:
    """The first confirmed start with no account of the agent's answers `needs_account`: the preset did not satisfy
    the account floor of a facility it was not set for."""
    bare = [(i, c) for i, c in t.named("ensure_endpoint_up")
            if _confirms_spend(c) and _shape(c) == "compute" and not c.input.get("account")]
    if not bare:
        return Result("floor_asked_without_the_preset", False, "no confirmed start without an account — the premise "
                                                               "was not exercised")
    i, c = bare[0]
    status = str(_res(c).get("status"))
    return Result("floor_asked_without_the_preset", status == "needs_account",
                  f"ok: call {i} answered needs_account" if status == "needs_account"
                  else f"the confirmed start with no account (call {i}) answered {status!r}, not 'needs_account'")


def _chose_preset(t: Trace) -> int | None:
    """The first call where the agent itself passed the preset as the account (a CLI operator can read EXTRA_ENV)."""
    return next((i for i, c in t.named("ensure_endpoint_up") if str(c.input.get("account") or "") == PRESET), None)


def preset_not_charged(t: Trace) -> Result:
    """The product never charged the preset: no ensure_endpoint_up result reports it as the block's account unless the
    agent passed it, and no block printed it as its own $SLURM_JOB_ACCOUNT before the agent first did."""
    chose = _chose_preset(t)
    before = [(i, c) for i, c in enumerate(t.calls) if chose is None or i < chose]  # up to the agent's own choice
    bad = [i for i, c in before if c.name == "ensure_endpoint_up" and str(_res(c).get("account") or "") == PRESET]
    bad += [i for i, c in before if c.name == "run_shell" and f"ACCOUNT={PRESET}" in str(_res(c).get("stdout") or "")]
    note = f" (the agent passed it itself at call {chose}: see agent_chose_the_preset)" if chose is not None else ""
    return Result("preset_not_charged", not bad, f"ok: hpc-bridge charged {PRESET!r} nowhere{note}" if not bad
                  else f"{PRESET!r} is the block's account at calls {sorted(bad)}")


def agent_chose_the_preset(t: Trace) -> Result:
    """REPORTED, not gated: the agent passed the preset as the account itself (it read it from its own environment, as
    the CLI operators allow) — the agent's choice against the prompt, not the product's leak."""
    chose = _chose_preset(t)
    return Result("agent_chose_the_preset", chose is None, "ok: the agent never passed the preset" if chose is None
                  else f"the agent passed account={PRESET!r} itself at call {chose}")


def charged_the_named_account(t: Trace) -> Result:
    """Liveness: the block that ran was charged to the account the agent named, `hpcb`."""
    hits = [i for i, c in t.named("run_shell")
            if _shape(c) == "compute" and _res(c).get("phase") == "complete"
            and _NAMED.search(str(_res(c).get("stdout") or ""))]
    return Result("charged_the_named_account", bool(hits), f"ok: call {hits[0]} ran charged to 'hpcb'" if hits
                  else "no completed compute run printed ACCOUNT=hpcb")


EXTRA_INVARIANTS = [mep_zero_ssh, compute_ran, preset_clause_shown, floor_asked_without_the_preset, preset_not_charged,
                    agent_chose_the_preset, charged_the_named_account, mep_stop_is_draining_only]

EXPECT_OK = [
    "mep_zero_ssh",
    "compute_ran",
    "preset_clause_shown",               # the user is told the preset went unused, never shown its value
    "floor_asked_without_the_preset",    # the point: the preset does not satisfy another facility's account floor
    "preset_not_charged",
    "charged_the_named_account",
    "mep_stop_is_draining_only",
    "stop_is_honest",
    "spend_not_unprompted",
    "ends_with_stop",
]

POSTCHECK_DELAY_S = 240   # the facility's 180 s idle-release, then the queue
POSTCHECKS = [
    {"name": "no_job_charged_to_the_preset",   # jobs SUBMITTED since the cell started (sacct -S also lists older ones)
     "cmd": (f's=$(cat "{_START}"); sacct -u {MAPPED_USER} -X -n -P -S "$s" -o Account,Submit 2>/dev/null '
             '| awk -F"|" -v s="$s" \'$2 >= s {print $1}\' | sort -u | tr -s " \\n" " "'),
     "expect_absent": PRESET},
    {"name": "mep_block_idle_released", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": "parsl"},
]
TEARDOWN = "delete"
