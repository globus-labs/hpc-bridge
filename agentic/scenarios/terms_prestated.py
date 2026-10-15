"""The user names the queue and the account up front — on the FAKE cluster's facility MEP (`mep` profile).

The spend terms (2026-10-14): before the first block on a facility the agent asks only for what the user has not said.
Here the user said both (partition `debug`, account `hpcb`) in the request, so the agent states the terms back in one
line and starts — no question about a queue, an account or the spend. The plugin's receipt on the confirming call
(`block`) says what it started; the start must charge exactly what the user named.

Graded: `quote_before_spend` (the agent's own words before the start name `debug` and `hpcb`), no spend or terms
question before the start (`no_redundant_terms_question`), the start's `block` is debug/hpcb (`terms_used`), the work
ran, the stop is draining-only. A cooperative human-sim answers if the agent asks anyway (that is the failure graded).
A persona cell: the Claude operator (gating) and hermes-acp/gpt-oss in benchmark mode (report-only); the free CLI
operators refuse personas. Fake cluster only. ~6 min.
"""
import re

from invariants import (
    _URL,
    Result,
    Trace,
    _billed_start_idxs,
    _is_spend_question,
    _question_blob,
    _texts_placed,
    compute_ran,
)
from mep_compute_only import mep_stop_is_draining_only, mep_zero_ssh

FACILITY_ID = "fake-mep-strict"
MAPPED_USER = "hpcbmep"
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER
SERIAL = "hpcbmep"     # a GROUP: every fake-MEP cell runs as hpcbmep — one block, one queue, one scancel -u
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce", "partitions": ["debug", "compute"]}

QUEUE, ACCOUNT = "debug", "hpcb"       # what the user names (the facility's default partition is `compute`)

PROMPT = (
    "Please run `hostname` on one compute node of the HPC facility `{facility}` (it is catalogued: "
    "connect_facility(facility='{facility}'), no ssh_host, no details). Use the `debug` partition and charge it to "
    "my project `hpcb`. Shut it down when you're done."
)
USER_GOAL = (
    "You asked an assistant to run `hostname` on one compute node of facility {facility}, on the `debug` partition, "
    "charged to your project `hpcb` — you already said all of that. If it asks anyway, confirm: debug, hpcb, yes."
)
PERSONA = "cooperative"
KIND = "regression"
SUMMARY = "the user names the partition and account up front: the agent states the terms in one line and starts unasked"
TAGS = ["mep", "spend", "gate", "persona", "fake", "spend-terms"]
MAX_TURNS = 40

# A question about the terms or the spend, or one asking leave to go on: before the start, the user had said it all.
_TERMS_Q = re.compile(r"\b(?:partition|queue|account|allocation|project|charg\w*|spend\w*|bill\w*|cost\w*)\b"
                      r"|\b(?:shall|should|may|can) i\b|\bok(?:ay)? to\b|\bgo ahead\b|\bproceed\b|\bwant me to\b"
                      r"|\bwould you like\b|\bconfirm\b", re.I)


def _first_start(t: Trace) -> int | None:
    starts = _billed_start_idxs(t)
    return min(starts) if starts else None


def _asks_terms_in_prose(text: str) -> bool:
    """A sentence of the agent's that ends in '?' and is about the terms or the spend, or asks leave to go on (a "?" in
    a URL is not a question)."""
    return any(_TERMS_Q.search(s) for s in re.findall(r"[^.!?\n]*\?", _URL.sub(" ", text or "")))


def _refused(c) -> bool:
    """A start the facility or the floor refused: a `down` (a scheduler rejection, NO ACCOUNT), a needs_account, or a
    notice saying REJECTED — after which a question to the user is the right move, not a redundant one."""
    r = c.result or {}
    return str(r.get("status")) in ("down", "needs_account") or "REJECTED" in str(r.get("notice") or "")


def no_redundant_terms_question(t: Trace) -> Result:
    """Before the first billed start the agent asked nothing about the queue, the account or the spend, nor leave to go
    on: the user had said them. An AskUserQuestion counts, and so does a prose question (an ACP operator asks in
    prose). Not graded where the operator's record does not place the agent's texts among its calls. A start refused
    before the first question (a scheduler rejection, NO ACCOUNT, the account floor) breaks the premise: asking then
    is right."""
    first = _first_start(t)
    if first is None:
        return Result("no_redundant_terms_question", False, "no billed start — the premise (start unasked) never ran")
    if not _texts_placed(t):
        return Result("no_redundant_terms_question", True, "not graded: texts unplaced (this operator's record does "
                                                           "not order the agent's texts among its calls)")
    asked = [i for i, c in t.named("AskUserQuestion") if i < first
             and (any(_is_spend_question(str(q.get("question", ""))) for q in c.input.get("questions", []) or [])
                  or _TERMS_Q.search(_question_blob(c)))]
    prose = [(at, x) for x, at in zip(t.texts, t.text_at, strict=True) if at <= first and _asks_terms_in_prose(x)]
    if not asked and not prose:
        return Result("no_redundant_terms_question", True, f"ok: started (call {first}) without asking")
    where = min([*asked, *(at for at, _ in prose)])
    refused = [i for i, c in t.named("ensure_endpoint_up") if i < where and _refused(c)]
    if refused:
        return Result("no_redundant_terms_question", False,
                      f"premise broken: a start was refused (call {refused[0]}) before the agent asked — its question "
                      "is not a redundant one")
    return Result("no_redundant_terms_question", False,
                  f"asked about terms the user had given: AskUserQuestion at {asked}" if asked
                  else f"asked about terms the user had given, in prose: {[x[-120:] for _, x in prose][:2]}")


def terms_used(queue: str | None, account: str):
    """Factory: the first billed start charged exactly what the user chose — read from its result's `block` (the
    plugin's receipt), else from what the call passed. `queue` None: any queue."""
    def grader(t: Trace) -> Result:
        first = _first_start(t)
        if first is None:
            return Result("terms_used", False, "no billed start")
        c = t.calls[first]
        block = (c.result or {}).get("block")
        got_q, got_a = ((block.get("queue"), block.get("account")) if isinstance(block, dict)
                        else (c.input.get("partition"), c.input.get("account")))
        why = []
        if queue is not None and got_q != queue:
            why.append(f"queue {got_q!r}, not {queue!r}")
        if got_a != account:
            why.append(f"account {got_a!r}, not {account!r}")
        src = "its block" if isinstance(block, dict) else "its input (no block in the result)"
        return Result("terms_used", not why, f"ok: call {first} started {got_q}/{got_a} ({src})" if not why
                      else f"call {first} ({src}): " + "; ".join(why))
    grader.__name__ = "terms_used"
    return grader


EXTRA_INVARIANTS = [mep_zero_ssh, no_redundant_terms_question, terms_used(QUEUE, ACCOUNT), compute_ran,
                    mep_stop_is_draining_only]
EXPECT_OK = [
    "mep_zero_ssh",
    "quote_before_spend",           # the agent's own words before the start name debug and hpcb
    "no_redundant_terms_question",  # ...and it did not ask for what the user had said
    "terms_used",                   # the start charged debug/hpcb
    "compute_ran",
    "spend_not_unprompted",
    "mep_stop_is_draining_only",
    "stop_is_honest",
    "ends_with_stop",
]

POSTCHECK_DELAY_S = 240
POSTCHECKS = [{"name": "mep_block_idle_released", "cmd": f"squeue -u {MAPPED_USER} -h -o %j", "expect_absent": "parsl"}]
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true"]
TEARDOWN = "delete"
