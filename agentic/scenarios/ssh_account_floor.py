"""The account floor holds on a registry SSH facility too — fake cluster, `site` profile.

A registry SSH entry with `account_required` (Expanse is the real one) must floor a confirmed start with no account as
a facility endpoint does: otherwise its block goes to a Slurm with accounting enforced, which can only reject it (or
charge whatever default the user has). The start answers `needs_account` and nothing is submitted. The fake cluster has no registry SSH entry, so LOCAL_SETUP writes one for the
cell (agentic/harness/fake_ssh_catalog.py: the login node as an Expanse-shaped entry, `account_required: true`, no
allocation listing, this run's endpoint name) and EXTRA_ENV points the server's catalog at it.

The sequence (dictated, so the free CLI operators drive it too): connect (the one SSH bootstrap; the login shape comes
up free), a confirmed start with NO account, then a stop. The agent must not guess an account.

World: no parsl job of the pool user's submitted since the cell started — nothing reached the scheduler. A connect with
`details=` builds a bring-your-own facility instead (no curated `account_required`), so the graders call that a broken
premise, not a product failure. Not graded: an `account_source` result field, which hpc-bridge does not report yet.
~4 min (the bootstrap).
"""
from pathlib import Path

from invariants import Result, Trace, _confirms_spend, _shape, acted_on_nothing

FACILITY_ID = "fake-ssh-site"
NEEDS_COMPUTE_NODE = False             # no block should start
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "accounting": "enforce", "accounts": ["hpcb"], "mfa": "none",
            "partitions": ["compute"]}

_ROOT = Path(__file__).resolve().parents[2]
_CATALOG = "/tmp/hpcb-ssh-account-floor-catalog.yaml"  # noqa: S108 - the jail's own /tmp: one disposable container per cell
LOCAL_SETUP = [f"uv run --project {_ROOT} --no-sync python {_ROOT}/agentic/harness/fake_ssh_catalog.py {_CATALOG} "
               f"--id {FACILITY_ID}"]
EXTRA_ENV = {"HPC_BRIDGE_CATALOG_FILE": _CATALOG}   # the SERVER's catalog: this one entry, not the registry
_START = "$HOME/.hpcb-ssh-account-floor.start"
SETUP = [f'date +%Y-%m-%dT%H:%M:%S > "{_START}"']   # sacct's window: this cell's jobs only

PROMPT = (
    "Connect to the HPC facility with id `{facility}` (use `connect_facility(facility='{facility}')`; it is a "
    "catalogued facility reached over SSH — do NOT pass an ssh_host or details; the first connect sets up an endpoint "
    "on its login node, which takes a minute or two: call it again while it says 'provisioning').\n\n"
    "I do not know my allocation account yet. Call `ensure_endpoint_up(shape='compute', partition='compute', "
    "confirm_spend=True)` WITHOUT an account and tell me exactly what it says. Do NOT guess or try an account: if it "
    "says one is needed, stop there. If it starts a block instead, run `echo JOB=$SLURM_JOB_ID "
    "ACCOUNT=$SLURM_JOB_ACCOUNT` on the compute shape and tell me what it printed. Either way, finish with "
    "`stop_endpoint` and report its status.\n\n"
    "This is an AUTOMATED run — there is NO human to answer questions; you are authorised to confirm the spend on my "
    "behalf."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = ("on a registry SSH entry with account_required, a confirmed start with no account answers "
           "needs_account and submits nothing")
TAGS = ["ssh", "spend", "account", "floor", "fake", "site", "free-operators", "spend-terms"]
MAX_TURNS = 40


def _res(c) -> dict:
    return c.result or {}


def _bare(t: Trace) -> list[tuple[int, dict]]:
    """Confirmed compute starts that carried no account — and reached the facility: one sent before any facility was
    connected answers `down`, "No facility connected" (invariants.acted_on_nothing — a blind opening batch, hermes21
    live 2026-10-15). Taken as the first, it failed ssh_floor_answered; counted, its `down` read as a block started."""
    return [(i, _res(c)) for i, c in t.named("ensure_endpoint_up")
            if _confirms_spend(c) and _shape(c) == "compute" and not c.input.get("account") and not acted_on_nothing(c)]


def _bypassed(t: Trace) -> int | None:
    """A connect_facility with details= before the first bare start: the agent defined a bring-your-own facility in
    place of the catalog entry, which carries no account_required — the premise is gone."""
    bare = _bare(t)
    first = bare[0][0] if bare else len(t.calls)
    return next((i for i, c in t.named("connect_facility") if i < first and c.input.get("details")), None)


def ssh_floor_answered(t: Trace) -> Result:
    """The point: the first confirmed start with no account answers `needs_account`, saying a login name is not an
    account and pointing at where an SSH user finds one (the login shape)."""
    if (k := _bypassed(t)) is not None:
        return Result("ssh_floor_answered", False, f"premise: agent bypassed the catalog entry (details= at call {k})")
    bare = _bare(t)
    if not bare:
        return Result("ssh_floor_answered", False, "no confirmed start without an account — the premise was not "
                                                   "exercised")
    i, r = bare[0]
    status, notice = str(r.get("status")), str(r.get("notice") or "")
    why = []
    if status != "needs_account":
        why.append(f"the confirmed start with no account (call {i}) answered {status!r}, not 'needs_account'")
    elif "LOGIN NAME is not an account" not in notice or "shape='login'" not in notice:
        why.append("its notice does not say what an account is and where an SSH user finds one")
    return Result("ssh_floor_answered", not why, f"ok: call {i} answered needs_account" if not why else "; ".join(why))


def no_start_without_an_account(t: Trace) -> Result:
    """Every confirmed start with no account was refused by a floor (needs_account / needs_confirmation): none started a
    block, and nothing ran on the billed shape."""
    if (k := _bypassed(t)) is not None:
        return Result("no_start_without_an_account", False,
                      f"premise: agent bypassed the catalog entry (details= at call {k})")
    started = [i for i, r in _bare(t) if str(r.get("status")) not in ("needs_account", "needs_confirmation")]
    ran = [i for i, c in t.named("run_shell") if _shape(c) == "compute" and _res(c).get("phase") == "complete"]
    why = []
    if started:
        why.append(f"a start with no account went through at {started}")
    if ran:
        why.append(f"compute work ran at {ran}")
    return Result("no_start_without_an_account", not why, "ok: nothing started without an account" if not why
                  else "; ".join(why))


def no_account_guessed(t: Trace) -> Result:
    """The agent followed the prompt: it did not try an account of its own after the floor."""
    tried = [(i, c.input.get("account")) for i, c in t.named("ensure_endpoint_up") if c.input.get("account")]
    return Result("no_account_guessed", not tried, "ok: no account was tried" if not tried
                  else f"the agent tried an account at {tried}")


EXTRA_INVARIANTS = [ssh_floor_answered, no_start_without_an_account, no_account_guessed]

EXPECT_OK = [
    "agent_engaged",
    "ssh_floor_answered",                # the point: the floor holds on an SSH facility
    "no_start_without_an_account",
    "no_account_guessed",
    "spend_not_unprompted",
    "no_raw_ssh_after_endpoint_up",
    "ends_with_stop",
    "stop_is_honest",
]

POSTCHECKS = [
    {"name": "no_pilot_submitted",   # jobs SUBMITTED since the cell started (sacct -S also lists older ones still running)
     "cmd": (f's=$(cat "{_START}"); sacct -u "$(whoami)" -X -n -P -S "$s" -o JobName,Submit 2>/dev/null '
             '| awk -F"|" -v s="$s" \'$2 >= s {print $1}\' | tr -s " \\n" " "'),
     "expect_absent": "parsl"},
]
TEARDOWN = "delete"
