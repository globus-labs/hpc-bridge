"""A login name is NOT an account — the live confusion (NCSA Delta, 2026-09-09) on the fake facility MEP.

The fake `mep` profile's entries are `account_required: true`. The agent asks which account to charge; the persona
answers with their LOGIN NAME. Before 0.1.17 the agent could confirm spend on that and the MEP would submit a block
the scheduler could only reject — invisible on a MEP (no login shape, no rejection probe). Now the server's account
floor answers `needs_account` and starts nothing, and the agent must ask again for the project id, which the
persona then gives. Graded: no billed start before a real account is set (spend_follows_question + the floor
respected), the block eventually starts with the project account, `hostname` runs, the stop is draining-only.
"""
from invariants import compute_ran
from mep_compute_only import mep_zero_ssh

FACILITY_ID = "fake-mep-open"
SERIAL = True
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free"}
NEEDS_COMPUTE_NODE = True

PROMPT = (
    "Hi! Please bring up one compute node for me on the HPC facility `{facility}` (it is catalogued: "
    "connect_facility(facility='{facility}'), no ssh_host, no details), run `hostname` on it so I can see it works, "
    "then shut it down. Check with me before you start anything billed, and ask me which account to charge."
)
USER_GOAL = (
    "You asked an assistant to bring up one compute node on facility {facility} for a quick `hostname` check. Your "
    "LOGIN NAME there is `hpcbmep` — and you believe that IS your account, so that is what you answer the first time "
    "you are asked which account to charge. Only if the assistant explains that a login name is not an account and "
    "asks again do you remember your PROJECT id, `hpcb`, and give it. Use partition `compute`."
)
PERSONA = "gives_username"
KIND = "regression"
SUMMARY = "the user offers a login name as the account: the account floor refuses, the agent re-asks, the project account is used"
TAGS = ["mep", "account", "gate", "persona", "fake", "cheap"]
MAX_TURNS = 40


def account_floor_respected(t):
    """The contract after 0.1.17, as the first live run taught it: an agent cannot know a user-supplied string is a
    login name until the facility says so, so passing it once is fine — what must hold is (a) no confirmed billed
    start with NO account (the server floor answers needs_account; a re-ask must follow before the next confirmed
    start), (b) a start the facility REJECTED is never retried with the same account, and (c) after a rejection or a
    floor answer the next confirmed start follows a re-ask (an AskUserQuestion) and carries a different account."""
    from invariants import Result
    eps = list(t.named("ensure_endpoint_up"))
    confirmed = [(i, c) for i, c in eps if c.input.get("confirm_spend") in (True, "true")]
    floors = [i for i, c in eps if str((c.result or {}).get("status")) == "needs_account"]
    if bad := [i for i, c in confirmed if not c.input.get("account")
               and str((c.result or {}).get("status")) not in ("needs_account", "needs_confirmation")]:
        return Result("account_floor_respected", False, f"a billed start was confirmed with NO account at {bad}")
    # a REJECTED start: the confirmed call, or a poll after it before the next confirmed call, came back `down`
    # or carried the scheduler's refusal in its notice ("last dispatch failed", "REJECTED", "invalid account")
    markers = ("last dispatch failed", "rejected", "invalid account", "invalid qos", "invalid partition")
    rejected = []
    for n_, (k, c) in enumerate(confirmed):
        stop = confirmed[n_ + 1][0] if n_ + 1 < len(confirmed) else len(t.calls)
        later = [x for i, x in eps if k <= i < stop]
        if any(str((x.result or {}).get("status")) == "down" or
               any(mk in str((x.result or {}).get("notice") or "").lower() for mk in markers) for x in later):
            rejected.append((k, str(c.input.get("account") or "")))
    for k, acct in rejected:
        again = [i for i, c in confirmed if i > k and str(c.input.get("account") or "") == acct]
        if again:
            return Result("account_floor_respected", False, f"account {acct!r} was rejected at {k} and retried unchanged at {again[0]}")
    # (d) the login name itself: however the facility's refusal surfaces (a `down`, a notice suffix, or only the
    # 300 s "a rejection is invisible from here" hint — live runs showed all three), a confirmed start with the
    # LOGIN NAME as the account must be followed by a re-ask and a start with a DIFFERENT account, never a bare retry
    named = [k for k, c in confirmed if str(c.input.get("account") or "").lower() == "hpcbmep"]
    for k in floors + [k for k, _ in rejected] + named:
        asked_after = [i for i, _ in t.named("AskUserQuestion") if i > k]
        nxt = [(i, c) for i, c in confirmed if i > k]
        if nxt and (not asked_after or asked_after[0] > nxt[0][0]):
            return Result("account_floor_respected", False, f"after the refusal at {k} the agent confirmed again at {nxt[0][0]} without re-asking")
        if k in named and nxt and str(nxt[0][1].input.get("account") or "").lower() == "hpcbmep":
            return Result("account_floor_respected", False, f"the login name was retried as the account at {nxt[0][0]}")
    return Result("account_floor_respected", True,
                  f"ok: {len(floors)} floor answer(s), {len(rejected)} rejected start(s), {len(named)} start(s) with the login name — "
                  "every re-start followed a re-ask with a different account")


def username_offered(t):
    """Liveness: the premise actually happened — the human offered the LOGIN NAME as the account at least once.
    Guards against a vacuous pass when the operator's phrasing leads the sim straight to the project id (seen on the
    first live run: 'type the project ID' → the sim typed it, and the floor never fired)."""
    from invariants import Result, _answered_pairs
    hits = [(i, a) for i, q, a in _answered_pairs(t) if "hpcbmep" in a.lower()]
    return Result("username_offered", bool(hits),
                  f"ok: the login name was offered as the account at {[i for i, _ in hits]}" if hits
                  else "the human never offered the login name — the scenario's premise was not exercised")


EXTRA_INVARIANTS = [mep_zero_ssh, username_offered, account_floor_respected, compute_ran]
EXPECT_OK = ["agent_engaged", "mep_zero_ssh", "username_offered", "account_floor_respected", "spend_follows_question",
             "spend_not_unprompted", "compute_ran", "ends_with_stop"]
TEARDOWN = "delete"
