"""HPC_BRIDGE_ACCOUNT reaches only the facility HPC_BRIDGE_MACHINE names (hole 2, 2026-10-14) — fake cluster, `mep`.

The env presets are for the startup pin's machine. Before the fix the agentic connect applied HPC_BRIDGE_ACCOUNT to
EVERY catalogued facility: a user who once exported it for one machine had every other facility's blocks charged to
it, unasked — and on an account-required facility it even satisfied the account floor, so nothing asked. Here the
server runs with `HPC_BRIDGE_ACCOUNT=hpcb-gpu` and no HPC_BRIDGE_MACHINE (the common case: an exported account, the
agentic flow), and the agent attaches to the fake MEP and confirms a block WITHOUT naming an account. Now the connect
says in one clause that the preset was not used (naming the variable, never its value), the account floor answers
`needs_account`, and the block is charged to the account the agent then passes, `hpcb`.

ADMIN_SETUP makes `hpcb-gpu` a valid account for the mapped user `hpcbmep`, so a leak would be BILLED, not refused by
Slurm — the world check (no `hpcb-gpu` job of hpcbmep in sacct since the cell started) then catches it; ADMIN_CLEANUP
removes the association again. Not yet graded, for the spend-terms PR that adds it: the start's
`block.account_source` (it will read `this_call`, never `env`). Autonomous: claude and the free operators. ~6 min.
"""
import re

from fake_mep_compute import MAPPED_USER
from invariants import Result, Trace, _confirms_spend, _shape, compute_ran
from mep_compute_only import mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-mep-strict"
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER
SERIAL = True                          # one mapped identity; and ADMIN_SETUP changes its associations
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce", "accounts": ["hpcb", "hpcb-gpu"]}

PRESET = "hpcb-gpu"
EXTRA_ENV = {"HPC_BRIDGE_ACCOUNT": PRESET}   # the SERVER's env only; HPC_BRIDGE_MACHINE stays unset
_START = "$HOME/.hpcb-preset-account-scoped.start"
_NAMED = re.compile(r"\bACCOUNT=hpcb(?![\w-])")
SETUP = [f'date +%Y-%m-%dT%H:%M:%S > "{_START}"']   # sacct's window: this cell's jobs only (hpcbmep is shared)
ADMIN_SETUP = [f"sacctmgr -i add user {MAPPED_USER} account={PRESET} >/dev/null 2>&1 || true"]
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true",
                 f"sacctmgr -i delete user name={MAPPED_USER} account={PRESET} >/dev/null 2>&1 || true"]

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
SUMMARY = ("hole 2: an exported HPC_BRIDGE_ACCOUNT with no HPC_BRIDGE_MACHINE is not applied to the facility the agent "
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


def preset_not_charged(t: Trace) -> Result:
    """Nothing was charged to the preset: no ensure_endpoint_up result reports it as the block's account, and no block
    printed it as its own $SLURM_JOB_ACCOUNT."""
    bad = [i for i, c in t.named("ensure_endpoint_up") if str(_res(c).get("account") or "") == PRESET]
    bad += [i for i, c in t.named("run_shell") if f"ACCOUNT={PRESET}" in str(_res(c).get("stdout") or "")]
    return Result("preset_not_charged", not bad, f"ok: {PRESET!r} charged nowhere" if not bad
                  else f"{PRESET!r} is the block's account at calls {sorted(bad)}")


def charged_the_named_account(t: Trace) -> Result:
    """Liveness: the block that ran was charged to the account the agent named, `hpcb`."""
    hits = [i for i, c in t.named("run_shell")
            if _shape(c) == "compute" and _res(c).get("phase") == "complete"
            and _NAMED.search(str(_res(c).get("stdout") or ""))]
    return Result("charged_the_named_account", bool(hits), f"ok: call {hits[0]} ran charged to 'hpcb'" if hits
                  else "no completed compute run printed ACCOUNT=hpcb")


EXTRA_INVARIANTS = [mep_zero_ssh, compute_ran, preset_clause_shown, floor_asked_without_the_preset, preset_not_charged,
                    charged_the_named_account, mep_stop_is_draining_only]

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
    {"name": "no_job_charged_to_the_preset",
     "cmd": f'sacct -u {MAPPED_USER} -X -n -S "$(cat "{_START}")" -o Account%20 2>/dev/null | sort -u | tr -s " \\n" " "',
     "expect_absent": PRESET},
    {"name": "mep_block_idle_released", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": "parsl"},
]
TEARDOWN = "delete"
