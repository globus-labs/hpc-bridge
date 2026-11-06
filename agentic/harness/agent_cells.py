"""The AGENT cells for batch row sizing and block environments: a model drives the tools, and a SCRIPTED user answers
what its turns leave open. Pure and SDK-free (scenarios import it on the host, as they import batch_checks).

WHY A CHAIN. The free operators (pi, codex, hermes21) run one-shot turns: no persona, no AskUserQuestion, no user
message mid-turn (midrun_hooks.operator_refusal). What they CAN drive is a seeded chain — a scripted first phase, then
fresh one-shot sessions of the cell's operator (the returning-user cells' shape). So a cell here is:

    phase 0  scripted: the user connected earlier (the probe's proposal confirmed, the free login shape up) — the
             agent's turns start at a reconnect, never at a configuration it would have to put to the user;
    phase 1  the agent, with the user's request (the scenario's TASK);
    phase 2… the agent again, once per open question — the user's answer (`UserSim`), composed when the phase runs.

A follow-up turn is a NEW session (a fresh MCP server), as each `hermes -z` turn of the hermes runner is: its prompt
replays the conversation so far — the request, the agent's last words of each turn, the user's answers — and the
user's answer to what the last turn left open. The server restart has consequences the user would not see in a
long-lived session: the spend acknowledgement, the session's block size and its env live in the server process, so a
batch carried over asks for the yes again and runs on whatever block the new session confirms. The prompt says the
session's settings start fresh; the users' answers that carry a batch over say the block they approved still stands.

THE USER'S ANSWERS are a pure function of the trace so far (`decide`): what the agent's LAST turn left open, from
hpc-bridge's own answers in it — a spend question (`needs_confirmation`), a size (`needs_sizing` with no proposal), a
proposal (`needs_sizing` with one), an account (`needs_account`), a failed env build — and, when none is open (or the
user has nothing scripted for it) and the turn ended without stopping the endpoint (a question in prose, a pause), a
go-ahead, at most GO_AHEADS times a run. A careful agent asks in WORDS before any tool call: a user whose sim says
`sizes_in_words` answers a turn whose words ask about each task's size, or put sizes of the agent's own, with the
user's sizes. The graders recompute every answer the same way (`answers_given`), so a regrade needs no stored dialogue
and the bundle's chain markers only show it.

A follow-up runs only when the turn before it left a trace (`latest_phase`) and something open; the first that does
not stops the chain (every later one sees the same trace).
"""
from __future__ import annotations

import hashlib
import json
import re
import shlex
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import batch_checks as bc
from invariants import ToolCall, Trace, _billed_start_idxs

FIRST_AGENT_PHASE = 1
FOLLOW_UPS = 5                      # turns after the first: sizes → a go-ahead → yes → proposal → yes
GO_AHEADS = 2                       # a careful agent may ask twice in words; a stuck one is not carried further
SIZE_ARGS = ("cores", "memory_gb", "gpus", "concurrency")
ENV_TOOLS = ("ensure_endpoint_up", "run_batch", "submit_job", "export_handoff")
# the answers that ask the user to spend (or for the account to charge): hpc-bridge's spend floor
_ASKERS = ("ensure_endpoint_up", "run_batch", "status", "run_shell", "poll_task", "submit_job", "reset_session")
_ENDS = ("stop_endpoint", "teardown_endpoint")
CONSENT = ("spend", "go_ahead")     # the user's answers that approve a block
MAX_SAID = 4000                     # one of the agent's turns, replayed: its head and tail when longer

INTRO = ("We are in the middle of a task on the HPC facility `{facility}` (login host `{ssh_host}`). This is a new "
         "session: the results of your earlier tool calls are gone. What you started on the facility is still there — "
         "the endpoint, any compute block, any batch (hpc-bridge keeps its record) and anything it built — but what "
         "this session had set (a block's size, a software environment, a spend approval) starts fresh: set it again "
         "where you need it. Reconnect to the facility and check where things stand with your tools rather than "
         "starting over. Our conversation so far:")
CONNECTED = ("I connected to the HPC facility `{facility}` (login host `{ssh_host}`) with hpc-bridge earlier today, "
             "so it is set up: connect to it again (no need to probe it or confirm a configuration).")
# How the conversation works, said by the user at the top of every turn: they are there BETWEEN turns. The operators'
# own framing agrees (`user_between_turns` on every agent phase: no "no human" lead, no tool that answers for the
# user — hermes' `clarify` answered "decide yourself" in the qwen pass of 2026-11-05).
TURNS = ("How we work: I'm here, between your turns. When you need something from me — a yes, a size, a choice — ask "
         "me in plain text and end your turn; my answer comes at the start of your next turn. Don't decide those for "
         "me.")


@dataclass(frozen=True)
class UserSim:
    """What the user answers, by what the agent's last turn left open. None: the user has nothing scripted to say to
    it — the go-ahead is said instead (at most GO_AHEADS times a run)."""

    sizes: str | None = None
    agree: str = "That size is fine — run the rest at it. The compute block I approved is still approved."
    yes: str = "Yes — go ahead and start that block."
    account: str | None = None
    env_failed: str | None = "Don't install anything by hand. Please just stop the endpoint for now."
    go_ahead: str = "Yes — please carry on with what I asked."
    # a user who KNOWS the size, asked to approve a block to measure one row first (a pilot's spend question): the size
    # instead of the yes — an answer of kind "sizes"
    pilot_reply: str | None = None
    # the user answers a turn whose WORDS ask about, or propose, each task's cores / memory / how many at once — not
    # only hpc-bridge's `needs_sizing` — with `sizes` (until it has given them once): a careful agent asks in words
    # first, before any tool call (the first live pass, 2026-11-05: qwen proposed its own sizes and asked "Shall I
    # start…?", and a plain yes let them through)
    sizes_in_words: bool = False
    # the answer when the agent put sizes of ITS OWN to the user (in words, or a sized run_batch's spend question);
    # None: `sizes`
    sizes_over_own: str | None = None
    # a version the user asked for that does not exist: a turn whose WORDS say so (the agent checked the index itself
    # and asked before building anything — the careful way) gets `env_failed`, as a failed build does
    unbuildable: str | None = None

    def say(self, kind: str) -> str | None:
        return {"sizes": self.sizes, "proposal": self.agree, "spend": self.yes, "account": self.account,
                "env_failed": self.env_failed, "go_ahead": self.go_ahead}.get(kind)


# ---- phase 0: the user connected earlier ---------------------------------------------------------------------------

async def connect_first(ops) -> str:
    """The scripted first phase: connect_facility from the probe's proposal until the free login shape is up."""
    r = await bc.connect_byo(ops)
    ops.say(f"(earlier) connected to {ops.facility} via {ops.ssh_host}: {r.get('phase')}")
    return "the facility is connected and its login shape is up"


# ---- reading a chain -----------------------------------------------------------------------------------------------

def text_phases(t: Trace) -> list[int]:
    """The phase of each of the agent's texts: the chain's stamps (`Trace.text_phase`), else the phase of the last call
    before it (`text_at`), else -1 (not known)."""
    if len(t.text_phase) == len(t.texts):
        return list(t.text_phase)
    if len(t.text_at) == len(t.texts):
        return [t.calls[at - 1].phase if 0 < at <= len(t.calls) else 0 for at in t.text_at]
    return [-1] * len(t.texts)


def latest_phase(t: Trace) -> int:
    """The last phase that left a trace: a call, or words of the agent's. -1: none."""
    return max([c.phase for c in t.calls] + text_phases(t) + [-1])


def said_in(t: Trace, phase: int, after: int | None = None) -> list[str]:
    """The agent's words in `phase` (after call `after`, where the texts are placed among the calls)."""
    placed = len(t.text_at) == len(t.texts)
    return [x for k, (x, p) in enumerate(zip(t.texts, text_phases(t), strict=True))
            if p == phase and (after is None or not placed or t.text_at[k] > after)]


def final_text(t: Trace, phase: int) -> str:
    said = said_in(t, phase)
    return said[-1] if said else ""


def prefix(t: Trace, n: int) -> Trace:
    """The trace as it stood before phase `n` ran: its calls and the texts said in phases before it."""
    phases = text_phases(t)
    keep = [k for k, p in enumerate(phases) if 0 <= p < n]
    calls = [c for c in t.calls if c.phase < n]
    placed = len(t.text_at) == len(t.texts)
    return Trace(calls, [t.texts[k] for k in keep], [t.text_at[k] for k in keep] if placed else [],
                 events=list(t.events), t0=t.t0, text_phase=[phases[k] for k in keep],
                 scripted_phases=[p for p in t.scripted_phases if p < n], preapproved_blocks=t.preapproved_blocks)


def calls_in(t: Trace, phase: int) -> list[tuple[int, ToolCall]]:
    return [(i, c) for i, c in enumerate(t.calls) if c.phase == phase]


def agent_calls(t: Trace) -> list[tuple[int, ToolCall]]:
    return [(i, c) for i, c in enumerate(t.calls) if c.phase >= FIRST_AGENT_PHASE]


# ---- what a call asked and answered --------------------------------------------------------------------------------

def answer_state(c: ToolCall) -> str:
    r = c.result or {}
    return str(r.get("phase") or r.get("status") or "")


def env_arg(c: ToolCall) -> dict | None:
    """The env a call asked for (a JSON string decoded: some operators send an object argument as text)."""
    e = c.input.get("env")
    if isinstance(e, str):
        try:
            e = json.loads(e)
        except ValueError:
            return None
    return e if isinstance(e, dict) else None


def env_of(r: dict | None) -> dict:
    """The env an answer states: its own, else its block's (envs_checks.env_of)."""
    r = r or {}
    return dict(r.get("env") or (r.get("block") or {}).get("env") or {})


# A build that failed AFTER the call that asked for it (a pending env: envs.choose / envs._promote) is said once, in
# the notice of the next ensure_endpoint_up or run_shell — whose `env` is the session's, not the failed one.
_ENV_NOT_BUILT = re.compile(r"was not built, so the session's env was not changed|session's env was not changed: env ",
                            re.I)


def env_failure(c: ToolCall) -> bool:
    """This answer says an env build failed: its env `failed`, or its notice says a pending one was not built."""
    r = c.result or {}
    if c.name in ENV_TOOLS and env_of(r).get("state") == "failed":
        return True
    return c.name in (*ENV_TOOLS, "run_shell") and bool(_ENV_NOT_BUILT.search(str(r.get("notice") or "")))


def _num(v: Any) -> float | None:
    try:
        return None if v is None or isinstance(v, bool) or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def size_args(c: ToolCall) -> dict[str, float]:
    """The size a call named: cores, memory_gb, gpus, concurrency (numbers; a string sent for one read as its number)."""
    out = {}
    for k in SIZE_ARGS:
        v = _num(c.input.get(k))
        if v is not None:
            out[k] = v
    return out


def is_pilot(c: ToolCall) -> bool:
    return c.name == "run_batch" and (_num(c.input.get("pilot")) or 0) >= 1


def is_new_batch(c: ToolCall) -> bool:
    return c.name == "run_batch" and not c.input.get("batch_id") and bool(c.input.get("command"))


def is_resize(c: ToolCall) -> bool:
    return c.name == "run_batch" and bool(c.input.get("batch_id")) and bool(size_args(c))


def pending(t: Trace) -> tuple[str, int] | None:
    """What the agent's LAST turn left open for the user, and the call that asked it: the latest of a spend question,
    an account, a size, a proposal and a failed env build that nothing later in the turn settled. A stop or teardown
    settles everything: the agent wrapped up."""
    last = latest_phase(t)
    if last < FIRST_AGENT_PHASE:
        return None
    billed = set(_billed_start_idxs(t))
    open_: dict[str, int] = {}
    for i, c in calls_in(t, last):
        r = c.result or {}
        state = answer_state(c)
        if c.name in _ENDS:
            open_.clear()
            continue
        if i in billed:
            open_.pop("spend", None)
            open_.pop("account", None)
        if is_new_batch(c) and (size_args(c) or is_pilot(c)):
            open_.pop("sizes", None)
        if is_resize(c) or (is_new_batch(c) and size_args(c) and "proposal" in open_):
            open_.pop("proposal", None)
        if c.name == "cancel":
            open_.pop("proposal", None)
            open_.pop("sizes", None)
        if env_of(r).get("state") == "ready":
            open_.pop("env_failed", None)
        if c.name in _ASKERS and state == "needs_confirmation":
            open_["spend"] = i
        if c.name in _ASKERS and state == "needs_account":
            open_["account"] = i
        if c.name in ("run_batch", "status") and state == "needs_sizing":
            open_["proposal" if r.get("proposal") else "sizes"] = i
        if env_failure(c):
            open_["env_failed"] = i
    if not open_:
        return None
    kind = max(open_, key=lambda k: open_[k])
    return kind, open_[kind]


# What the agent's WORDS say about each task's size. Its own: a number with a unit per task ("1 CPU per row", "0.5 GB
# each"), or how many at once ("4 rows at a time"). A question: how many / how much / what … cores, memory, at once.
_PER = r"(?:per\s+(?:row|task|run|job|check|sample|input|word|dataset|analysis|item)s?|each)"
_UNIT = r"(?:v?cpus?|cores?|threads?|gb|gib|mb|mib|g)\b"
_OWN_SIZE = re.compile(
    rf"\d+(?:\.\d+)?\s*-?\s*{_UNIT}[^.\n?]{{0,40}}?\b{_PER}\b"
    rf"|\b{_PER}\b[^.\n?]{{0,40}}?\d+(?:\.\d+)?\s*-?\s*{_UNIT}"
    r"|\b\d+\s+(?:\w+\s+){0,2}?(?:at (?:a|one) time|at once|in parallel|concurrently|simultaneously)\b", re.I)
_ASKS_SIZE = re.compile(r"\b(?:how many|how much|what)\b[^.?\n]{0,60}?\b(?:v?cpus?|cores?|memory|ram|gb|threads?|"
                        r"sizes?|resources|at (?:a|one) time|at once|in parallel|concurrent\w*)", re.I)


def own_sizes_said(text: str) -> bool:
    """The words put a size of the agent's own: a number with a unit per task, or how many at once."""
    return bool(_OWN_SIZE.search(text or ""))


def own_sizes_snippet(text: str) -> str:
    """Where the words put a size of the agent's own, with a little around it ("" when they do not)."""
    m = _OWN_SIZE.search(text or "")
    return text[max(0, m.start() - 30): m.end() + 30] if m else ""


def asks_sizes(text: str) -> bool:
    """The words ASK about each task's size (a question on cores, memory or how many at once)."""
    return "?" in (text or "") and bool(_ASKS_SIZE.search(text or ""))


# Why a version cannot be built, in the agent's words: no such version, does not exist, unsatisfiable, …
UNBUILDABLE_WORDS = re.compile(
    r"no (?:matching |such )?(?:version|release|distribution)|(?:does|do|did)\s*n[o']?t exist|doesn'?t exist|not exist"
    r"|not (?:be )?found|unsatisfiable|no solution|not available|isn'?t available|unavailable|not on pypi|"
    r"not published|never (?:been )?released|(?:could ?n[o']?t|cannot|can'?t) (?:find|resolve)|invalid version", re.I)


def says_unbuildable(text: str, version: str) -> bool:
    """The words name `version` and why it cannot be built."""
    return version in (text or "") and bool(UNBUILDABLE_WORDS.search(text or ""))


def _put_own_sizes(t: Trace, phase: int) -> bool:
    """In `phase` the agent put sizes of its own to the user: in its last words, or a run_batch it sized itself whose
    spend question the turn ended on."""
    sized = any(is_new_batch(c) and size_args(c) and answer_state(c) == "needs_confirmation"
                for _i, c in calls_in(t, phase))
    return sized or own_sizes_said(final_text(t, phase))


def decide(t: Trace, sim: UserSim) -> tuple[str, str] | None:
    """The user's answer to the agent's last turn: (kind, words), or None — nothing to say, the chain ends. A turn that
    stopped the endpoint wrapped up (every request here ends with a stop); one that ended otherwise, on a question in
    prose or a pause, gets the go-ahead — at most GO_AHEADS times a run, so a stuck agent is not carried along. With
    `sizes_in_words`, a turn whose words ask about each task's size, or put sizes of the agent's own, gets the user's
    sizes (once) — before a spend question's yes, which would let the agent's own sizes through."""
    last = latest_phase(t)
    if last < FIRST_AGENT_PHASE:
        return None
    p = pending(t)
    given = answers_given(t, sim)
    sized = any(k == "sizes" for _q, k, _w in given)
    if (p is not None and p[0] == "spend" and sim.pilot_reply and is_pilot(t.calls[p[1]]) and not sized):
        return "sizes", sim.pilot_reply
    measured = any(c.name in ("run_batch", "status") and answer_state(c) == "needs_sizing"
                   and (c.result or {}).get("proposal") for c in t.calls)   # then the talk is of hpc-bridge's proposal
    if (sim.sizes_in_words and sim.sizes and not sized and not measured and (p is None or p[0] == "spend")
            and not any(c.name in _ENDS for _i, c in calls_in(t, last))):
        if _put_own_sizes(t, last):
            return "sizes", sim.sizes_over_own or sim.sizes
        if asks_sizes(final_text(t, last)):
            return "sizes", sim.sizes
    if p is not None and sim.say(p[0]):
        return p[0], str(sim.say(p[0]))
    if any(c.name in _ENDS for _i, c in calls_in(t, last)):
        return None
    if (sim.unbuildable and sim.env_failed and says_unbuildable(final_text(t, last), sim.unbuildable)
            and not any(k == "env_failed" for _q, k, _w in given)):
        return "env_failed", sim.env_failed
    if sum(1 for _q, k, _w in given if k == "go_ahead") < GO_AHEADS:
        return "go_ahead", sim.go_ahead
    return None


def answers_given(t: Trace, sim: UserSim) -> list[tuple[int, str, str]]:
    """(phase, kind, words) for every follow-up turn the trace shows ran: the user's answer that opened it, computed
    from the trace as it stood before it (the same function the chain used live)."""
    out = []
    for q in range(FIRST_AGENT_PHASE + 1, latest_phase(t) + 1):
        got = decide(prefix(t, q), sim)
        out.append((q, *(got or ("?", ""))))
    return out


def consent_phases(t: Trace, sim: UserSim) -> list[int]:
    """The phases whose prompt carried the user's yes to a block."""
    return [q for q, kind, _w in answers_given(t, sim) if kind in CONSENT]


# ---- the follow-up turns -------------------------------------------------------------------------------------------

def _clip(text: str) -> str:
    return text if len(text) <= MAX_SAID else text[: MAX_SAID // 2] + "\n…\n" + text[-MAX_SAID // 2:]


def compose(t: Trace, sim: UserSim, task: str) -> str:
    """A follow-up turn's prompt: the conversation so far, replayed, and the user's answer to the last turn."""
    parts = [INTRO, TURNS, f"ME (my request): {task}"]
    given = {q: w for q, _k, w in answers_given(t, sim)}
    for k in range(FIRST_AGENT_PHASE, latest_phase(t) + 1):
        if k in given:
            parts.append(f"ME: {given[k]}")
        parts.append(f"YOU: {_clip(final_text(t, k)) or '(you ended that turn without a word to me)'}")
    now = decide(t, sim)
    parts.append(f"ME: {now[1] if now else sim.go_ahead}")
    parts.append("Carry on from there, acting on my latest message.")
    return "\n\n".join(parts)


def _follow_up(q: int, sim: UserSim, task: str) -> dict:
    def when(t: Trace) -> bool:
        return latest_phase(t) == q - 1 and decide(t, sim) is not None

    def prompt(t: Trace) -> str:
        return compose(t, sim, task)
    return {"prompt": prompt, "when": when, "label": f"turn {q}: the user answers", "user_between_turns": True}


def conversation(task: str, sim: UserSim, *, follow_ups: int = FOLLOW_UPS,
                 connect: Callable[..., Any] = connect_first) -> list[dict]:
    """A cell's PHASES: the scripted connect (the premise), the user's request, then up to `follow_ups` answers. Every
    agent phase says the user answers between turns (TURNS, and `user_between_turns` for the operator)."""
    phases: list[dict] = [{"script": connect, "premise": True, "label": "earlier: the user connected (scripted)"},
                          {"prompt": f"{TURNS}\n\n{task}", "label": "turn 1: the user's request",
                           "user_between_turns": True}]
    phases += [_follow_up(q, sim, task) for q in range(FIRST_AGENT_PHASE + 1, FIRST_AGENT_PHASE + 1 + follow_ups)]
    return phases


# ---- the words the graders look for --------------------------------------------------------------------------------

_NUMBER_WORDS = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine",
                 10: "ten", 12: "twelve", 16: "sixteen"}
_CPU = r"(?:v?cpus?|cores?|processors?|threads?)"


def mentions_count(text: str, n: float | int | None, unit: str = _CPU) -> bool:
    """`text` names `n` of `unit` — "3 CPUs", "3-core", "three cores", "CPUs: 3", "cores_per_node 3"."""
    if n is None:
        return False
    k = int(n) if float(n).is_integer() else n
    nums = [re.escape(str(k))] + ([_NUMBER_WORDS[int(k)]] if isinstance(k, int) and k in _NUMBER_WORDS else [])
    num = rf"(?<![\w.])(?:{'|'.join(nums)})(?!\w|\.\d)"
    return bool(re.search(rf"{num}[\s\-×x*]*(?:[\w\-]+\s+){{0,2}}?{unit}\b", text, re.I)
                or re.search(rf"\b{unit}\w*\W{{1,4}}(?:[\w\-]+\W+){{0,2}}?{num}", text, re.I))


def mentions_memory(text: str, gb: float | None) -> bool:
    """`text` names `gb` of memory — in GB ("0.5 GB", "0.50 GiB", "1.5GB", "half a GB"), in MB (512 or 500 per GB),
    as the argument or field it is (`memory_gb=1.5`, "memory: 1.5"), or as a share of a whole ("1.5 of 3.9 GB" — codex
    offered the proposal so, 2026-11-05)."""
    if gb is None:
        return False
    forms = {f"{gb:g}", f"{gb:.2f}"} | ({f"{gb:.1f}"} if float(f"{gb:.1f}") == gb else set())
    if float(gb).is_integer():
        forms.add(str(int(gb)))
    num = rf"(?<![\w.])(?:{'|'.join(re.escape(f) for f in forms)})(?![\d])"
    gb_pat = rf"{num}\s*(?:gb|gib|g\b|gigabytes?)"
    field = rf"\b(?:memory(?:_gb)?|mem|ram)\b\W{{0,4}}{num}(?!\.\d)"
    share = rf"{num}\s*(?:of|/)\s*~?\d+(?:\.\d+)?\s*(?:gb|gib|g\b)"
    mb = {round(gb * 1024), round(gb * 1000)}
    mb_pat = rf"(?<![\w.])(?:{'|'.join(str(m) for m in mb)})\s*(?:mb|mib|m\b|megabytes?)"
    half = gb == 0.5 and re.search(r"\bhalf (?:a )?(?:gb|gigabyte|gib)\b", text, re.I)
    return bool(any(re.search(p, text, re.I) for p in (gb_pat, field, share, mb_pat)) or half)


# ---- what the cells give the agent to run (the user's work) ---------------------------------------------------------

WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]
WORDS_SAID = "alpha, bravo, charlie, delta, echo and foxtrot"
WORD_TASK = "sleep 5; echo <word> | sha256sum | cut -c1-12"


def word_digests(words: list[str]) -> dict[str, str]:
    """What WORD_TASK prints for each word: only running it gives these, so the agent cannot relay them unrun."""
    return {w: hashlib.sha256(f"{w}\n".encode()).hexdigest()[:12] for w in words}


WORK_DIR = "$HOME/hpcb-agent-work"     # the user's own scripts, written by a cell's SETUP (shared home: every node)
WORK_SAID = "~/hpcb-agent-work"


def write_file_cmd(name: str, lines: list[str]) -> str:
    """A SETUP command that writes the user's script `name` into WORK_DIR (no here-document: each line quoted)."""
    return (f'mkdir -p "{WORK_DIR}" && printf \'%s\\n\' {" ".join(shlex.quote(x) for x in lines)} '
            f'> "{WORK_DIR}/{name}"')


SIMULATE = [   # one simulation run: two busy CPUs for 6 s and ~200 MB held, then its sample's digest
    "#!/bin/bash",
    "timeout 6 yes > /dev/null &",
    "timeout 6 yes > /dev/null &",
    "python3 -c 'import time; b = b\"x\" * (200 << 20); time.sleep(6)' &",
    "wait",
    "echo \"simulated $1 $(printf '%s' \"$1\" | sha256sum | cut -c1-10)\"",
]


def simulate_digests(samples: list[str]) -> dict[str, str]:
    return {s: hashlib.sha256(s.encode()).hexdigest()[:10] for s in samples}


def analyze_script(mb: int) -> list[str]:
    """One analysis: its dataset held whole in memory (`mb` MB, touched at once: the RSS is there within a second),
    then its digest."""
    return ["import hashlib, sys, time",
            "name = sys.argv[1]",
            f"data = b'x' * ({int(mb)} << 20)",
            "time.sleep(5)",
            "print('analyzed', name, hashlib.sha256(name.encode() + data[:8]).hexdigest()[:10])"]


def analyze_digests(names: list[str]) -> dict[str, str]:
    return {n: hashlib.sha256(n.encode() + b"x" * 8).hexdigest()[:10] for n in names}


PKG, PKG_VERSION = "tomli", "2.0.1"    # small, pure Python, a wheel on PyPI; in neither the endpoint venv (Python 3.11:
#                                        only older Pythons pull it in) nor the fake nodes' system Python
PKG_CODE = "import tomli; print('tomli', tomli.__version__, 'from', tomli.__file__)"   # the user's own code
PKG_LINE = f'python3 -c "{PKG_CODE}"'
