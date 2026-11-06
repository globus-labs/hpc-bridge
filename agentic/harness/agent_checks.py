"""Graders for the AGENT cells of batch row sizing and block environments (agent_cells.py: a model drives the tools, a
scripted user answers). Pure and SDK-free (scenarios import it on the host, as they import batch_checks).

They judge what the agent DID — the arguments it passed, hpc-bridge's answers and what it did next, the order of the
user's answers and the billed starts, the world on the facility — and what it TOLD the user only where the user needs
a fact (the block's CPUs before the yes, the proposed size, the installer's reason, the results). Never phrasing beyond
that, and never wait cadence or turn-taking: "called again after `env_building`" is graded, how long it waited is not.

Every grader reads the AGENT's turns only (phase ≥ 1): the scripted phase 0 is the premise, not the agent's doing. A
grader whose premise never happened says "premise not met" and FAILS — never a vacuous pass. A premise comes from the
WORLD, never from a choice the agent may make otherwise (the first live pass, 2026-11-05): a pause the cell needs is
seeded by its scripted phase (batch_memory_kill_resized), and where the agent can reach the point by another road —
asking in words before any tool call, checking a version itself, a block asked for first — the grader takes that road
too; an agent that never reaches it fails plainly, naming what it did instead.
"""
from __future__ import annotations

import re
import shlex
from typing import Any

import batch_checks as bc
import sizing_checks as sc
from agent_cells import (
    ENV_TOOLS,
    FIRST_AGENT_PHASE,
    UNBUILDABLE_WORDS,
    UserSim,
    agent_calls,
    answer_state,
    answers_given,
    calls_in,
    consent_phases,
    env_arg,
    env_failure,
    env_of,
    is_new_batch,
    is_pilot,
    is_resize,
    latest_phase,
    mentions_count,
    mentions_memory,
    own_sizes_said,
    own_sizes_snippet,
    said_in,
    says_unbuildable,
    size_args,
    text_phases,
)
from invariants import _HPC_TOOLS, _RAW_SSH, Result, ToolCall, Trace, _billed_start_idxs, _names_queue, _shape

HPCB_TOOLS = _HPC_TOOLS | {"run_batch", "status", "cancel", "submit_job", "export_handoff"}
_NOT_ACCEPTED = ("refused", "needs_sizing", "needs_confirmation", "needs_account")


def _premise(name: str, why: str) -> Result:
    return Result(name, False, f"premise not met: {why}")


# ---- the agent's own turns -------------------------------------------------------------------------------------------

def agent_turn_engaged(t: Trace) -> Result:
    """The AGENT used hpc-bridge: a call in its own turns. The universal `agent_engaged` passes on the scripted
    connect's calls alone, so a chain whose agent never acted would pass it."""
    n = [i for i, c in agent_calls(t) if c.name in HPCB_TOOLS]
    return Result("agent_turn_engaged", bool(n), f"{len(n)} hpc-bridge call(s) in the agent's turns" if n
                  else "the agent's turns made no hpc-bridge call (the scripted connect's do not count)")


# ---- batch row sizing ------------------------------------------------------------------------------------------------

def accepted_new_batch(t: Trace) -> tuple[int, ToolCall] | None:
    """The first run_batch of the agent's that RECORDED a new batch (a batch id, not refused or still asking)."""
    for i, c in agent_calls(t):
        if is_new_batch(c) and (c.result or {}).get("batch_id") and answer_state(c) not in _NOT_ACCEPTED:
            return i, c
    return None


def _wrong_size(c: ToolCall, *, cores: int, memory_gb: float, max_concurrency: int) -> list[str]:
    """What a call named that is NOT the user's size (a size it left out is the server's to ask for)."""
    s, bad = size_args(c), []
    if "cores" in s and s["cores"] != cores:
        bad.append(f"cores={s['cores']:g}")
    if "memory_gb" in s and abs(s["memory_gb"] - memory_gb) > 0.01:
        bad.append(f"memory_gb={s['memory_gb']:g}")
    if "concurrency" in s and not 1 <= s["concurrency"] <= max_concurrency:
        bad.append(f"concurrency={s['concurrency']:g}")
    if s.get("gpus"):
        bad.append(f"gpus={s['gpus']:g}")
    return bad


def sizes_as_given(t: Trace, *, cores: int, memory_gb: float, max_concurrency: int,
                   concurrency_required: bool = True, name: str = "sizes_as_given") -> Result:
    """The rows were sized as the USER said: every new batch the agent asked for named no other size (none invented),
    and the batch that was recorded carried the user's `cores` and `memory_gb` — and its `concurrency`, at most the
    user's: required where the user CAPPED it (`concurrency_required`); else only checked when given (on an approved
    block the rows run as many as fit, and a block derived from the rows needs one anyway, hpc-bridge asks)."""
    asks = [(i, c) for i, c in agent_calls(t) if is_new_batch(c)]
    if not asks:
        return Result(name, False, "the agent asked for no batch")
    wrong = [f"call {i}: {', '.join(w)}" for i, c in asks
             if (w := _wrong_size(c, cores=cores, memory_gb=memory_gb, max_concurrency=max_concurrency))]
    acc = accepted_new_batch(t)
    if acc is None:
        return Result(name, False, "no batch was recorded" + (f"; sizes not the user's: {'; '.join(wrong)}"
                                                               if wrong else ""))
    i, c = acc
    s = size_args(c)
    missing = [k for k in ("cores", "memory_gb", *(("concurrency",) if concurrency_required else ())) if k not in s]
    ok = not wrong and not missing
    return Result(name, ok, f"the batch recorded at call {i} carried {({k: s.get(k) for k in s})}"
                  + (f"; dropped: {missing}" if missing else "") + (f"; not the user's: {'; '.join(wrong)}" if wrong
                                                                   else "") + f" (the user's: {cores} core(s), "
                  f"{memory_gb:g} GB, at most {max_concurrency} at once)")


def _first_block(t: Trace) -> tuple[int, dict] | None:
    """The agent's first billed start and the block it started: its receipt, else the spend question that preceded it
    (the last `block` an answer named before it)."""
    starts = [k for k in _billed_start_idxs(t) if t.calls[k].phase >= FIRST_AGENT_PHASE]
    if not starts:
        return None
    k = starts[0]
    block = (t.calls[k].result or {}).get("block")
    if not isinstance(block, dict):
        asked = [(c.result or {}).get("block") for i, c in agent_calls(t) if i <= k
                 and isinstance((c.result or {}).get("block"), dict) and answer_state(c) == "needs_confirmation"]
        block = asked[-1] if asked else {}
    return k, dict(block or {})


def terms_shown_before_yes(t: Trace, sim: UserSim) -> Result:
    """The block that STARTED was put to the user before the yes that started it, whichever way the agent got there (a
    block derived from the rows, or one it asked for first): the agent's first billed start is in a turn whose prompt
    carried the user's yes, and the agent's words in the turn that ended on that question name the block's CPUs (its
    queue, where the terms state no CPU count)."""
    name = "terms_shown_before_yes"
    first = _first_block(t)
    if first is None:
        return Result(name, False, "no block was ever confirmed")
    k, block = first
    p = t.calls[k].phase
    yes = [q for q in consent_phases(t, sim) if q <= p]
    if not yes:
        return Result(name, False, f"the block at call {k} (turn {p}) was confirmed before any yes")
    q = max(yes)
    said = "\n".join(said_in(t, q - 1))
    cpus = block.get("cores_per_node")
    if cpus:
        named = mentions_count(said, cpus)
        what = f"{cpus} CPU(s)"
    else:
        named = _names_queue(said, block)
        what = f"queue {block.get('queue') or 'the default'!r}"
    return Result(name, named, f"the block started at call {k} (turn {p}): {what}, {block.get('memory_gb')} GB; the "
                  f"yes came in turn {q}, and the turn before it {'named' if named else 'did NOT name'} {what}")


def yes_before_billed_block(t: Trace, sim: UserSim) -> Result:
    """No block was confirmed before the user said yes: every billed start of the agent's comes in a turn at or after
    one whose prompt carried the user's yes (or go-ahead) — the agent stopped and asked, it did not confirm alone."""
    name = "yes_before_billed_block"
    starts = [k for k in _billed_start_idxs(t) if t.calls[k].phase >= FIRST_AGENT_PHASE]
    if not starts:
        return _premise(name, "no block was ever confirmed")
    yes = consent_phases(t, sim)
    bad = [k for k in starts if not any(q <= t.calls[k].phase for q in yes)]
    return Result(name, not bad, f"billed start(s) at {starts}; the user's yes in turn(s) {yes or 'none'}"
                  + (f"; confirmed before any yes: {bad}" if bad else ""))


def _sized(c: ToolCall) -> bool:
    s = size_args(c)
    return any(k in s for k in ("cores", "memory_gb", "concurrency")) or bool(s.get("gpus"))


def sizes_given_in(t: Trace, sim: UserSim) -> int | None:
    """The turn whose prompt first carried the user's sizes (an answer of kind "sizes"), or None."""
    return next((q for q, kind, _w in answers_given(t, sim) if kind == "sizes"), None)


def asked_for_sizes(t: Trace, sim: UserSim) -> Result:
    """The user gave no sizes (the cell's premise, which always holds), so the agent must ASK before it sizes or runs
    anything: in words, or by ending its turn on hpc-bridge's `needs_sizing`. Passes when the user's sizes came (the
    sim answers either) before any run_batch that named a size or recorded a batch; fails naming the call that came
    first — a size of the agent's own, or rows run at a default — or when the same unsized call was sent over and over
    in one turn, or when the agent never asked."""
    name = "asked_for_sizes"
    q = sizes_given_in(t, sim)
    before = [(k, x) for k, x in agent_calls(t) if q is None or x.phase < q]
    sized = [k for k, x in before if x.name == "run_batch" and _sized(x)]
    ran = [k for k, x in before if is_new_batch(x) and (x.result or {}).get("batch_id")
           and answer_state(x) not in _NOT_ACCEPTED]
    loops = {}
    for k, x in agent_calls(t):
        if is_new_batch(x) and not _sized(x) and not is_pilot(x) and answer_state(x) == "needs_sizing":
            loops.setdefault(x.phase, []).append(k)
    looped = {p: ks for p, ks in loops.items() if len(ks) > 2}
    bad = ([f"run_batch at call {k} named a size before the user's sizes" for k in sized]
           + [f"a batch was recorded at call {k} before the user's sizes" for k in ran if k not in sized]
           + [f"the same unsized call {len(ks)} times in turn {p} (calls {ks})" for p, ks in looped.items()])
    if bad:
        return Result(name, False, "; ".join(bad) + (f" (the user gave sizes in turn {q})" if q is not None
                                                     else " (the user never gave sizes)"))
    if q is None:
        return Result(name, False, "the agent never asked: no turn ended on a size question, in words or after "
                                   "needs_sizing, so the user never gave sizes")
    how = ("after needs_sizing" if any(answer_state(x) == "needs_sizing" for _k, x in calls_in(t, q - 1))
           else "in words")
    return Result(name, True, f"ok: asked {how} in turn {q - 1}; the user's sizes came in turn {q}, before any sized "
                              "or recorded batch")


def agent_proposed_own_sizes(t: Trace, sim: UserSim) -> Result:
    """REPORTED: before the user's sizes, the agent put sizes of ITS OWN to the user — a number per task or how many at
    once in its words, or a run_batch it sized itself — instead of asking openly. Mild (the user can correct it), worth
    tracking: the first live pass (qwen, 2026-11-05) proposed "1 CPU per row … 4 rows at a time" and asked to start."""
    name = "agent_proposed_own_sizes"
    q = sizes_given_in(t, sim)
    last = q if q is not None else latest_phase(t) + 1
    own = [f"turn {p}: {own_sizes_snippet(x)!r}" for p in range(FIRST_AGENT_PHASE, last)
           for x in said_in(t, p) if own_sizes_said(x)]
    own += [f"call {k}: run_batch {size_args(x)}" for k, x in agent_calls(t)
            if x.phase < last and x.name == "run_batch" and _sized(x)]
    if q is None and not own:
        return _premise(name, "the size never came up: the user gave no sizes and the agent proposed none")
    return Result(name, not own, f"put its own sizes: {'; '.join(own[:3])}" if own
                  else f"ok: asked openly; the user's sizes came in turn {q}")


def pilot_used(t: Trace) -> Result:
    """The agent measured a row first the tool's way: a run_batch with `pilot` ≥ 1 that recorded its batch."""
    pilots = [(i, c) for i, c in agent_calls(t) if is_pilot(c)]
    recorded = [i for i, c in pilots if (c.result or {}).get("batch_id") and answer_state(c) not in _NOT_ACCEPTED]
    return Result("pilot_used", bool(recorded), f"pilot call(s) {[i for i, _c in pilots] or 'none'}; recorded at "
                  f"{recorded or 'none'}")


def _proposals(t: Trace, *, agent_only: bool = False) -> list[tuple[int, ToolCall]]:
    """The batch answers that carried a measured proposal: the agent's own, or (not `agent_only`) one a scripted phase
    met — a cell may SEED the pause from the world (batch_memory_kill_resized), so the premise never hangs on the
    agent's choice of size."""
    calls = agent_calls(t) if agent_only else list(enumerate(t.calls))
    return [(i, c) for i, c in calls if c.name in ("run_batch", "status")
            and answer_state(c) == "needs_sizing" and isinstance((c.result or {}).get("proposal"), dict)]


def _num(v: Any) -> float | None:
    try:
        return None if v is None or isinstance(v, bool) else float(v)
    except (TypeError, ValueError):
        return None


def proposal_relayed(t: Trace, sim: UserSim) -> Result:
    """The batch stopped at `needs_sizing` with a measured proposal, and the agent put it to the USER: its words after
    it, in that turn, name the proposed memory (and the cores, where the proposal has them); it did not resize in that
    turn; the user's next answer was the agreement."""
    name = "proposal_relayed"
    props = _proposals(t)
    if not props:
        return _premise(name, "no batch answer carried a proposal (needs_sizing with `proposal`)")
    seen = _proposals(t, agent_only=True)
    if not seen:
        return Result(name, False, f"the batch paused at a proposal (call {props[0][0]}), and the agent never read it")
    i, c = seen[0]
    p = (c.result or {})["proposal"]
    said = "\n".join(said_in(t, c.phase, after=i))
    mem = mentions_memory(said, _num(p.get("memory_gb")))
    cores = p.get("cores") is None or mentions_count(said, p.get("cores"))
    acted = [k for k, x in calls_in(t, c.phase) if k > i and (is_resize(x) or (is_new_batch(x) and _sized(x)))]
    agreed = any(q == c.phase + 1 and kind == "proposal" for q, kind, _w in answers_given(t, sim))
    ok = mem and cores and not acted and agreed
    return Result(name, ok, f"the agent read the proposal at call {i} (turn {c.phase}; paused at call {props[0][0]}): "
                  f"cores {p.get('cores')}, {p.get('memory_gb')} GB "
                  f"— memory {'named' if mem else 'NOT named'}, cores {'named' if cores else 'NOT named'}"
                  + (f"; resized in the same turn at call(s) {acted}" if acted else "")
                  + ("; the user's next answer agreed" if agreed else "; the turn did not end on the proposal"))


def resize_after_agreement(t: Trace, sim: UserSim) -> Result:
    """The rows ran at the PROPOSED size only after the user agreed: the first call that sized rows after the proposal
    — a resize of the batch, or a new batch at a size — comes in a turn at or after the agreement and carries the
    proposal's memory (and its cores, where it has them)."""
    name = "resize_after_agreement"
    props = _proposals(t)
    if not props:
        return _premise(name, "no batch answer carried a proposal")
    sized = [(k, x) for k, x in agent_calls(t) if k > props[0][0] and (is_resize(x) or (is_new_batch(x) and _sized(x)))]
    if not sized:
        return Result(name, False, f"nothing was sized after the proposal at call {props[0][0]}")
    k, x = sized[0]
    p = (next(c for i, c in reversed(props) if i < k).result or {})["proposal"]
    s = size_args(x)
    agreed = [q for q, kind, _w in answers_given(t, sim) if kind == "proposal"]
    bad = []
    if not agreed or x.phase < min(agreed):
        bad.append(f"sized at call {k} (turn {x.phase}) before the user agreed (turn(s) {agreed or 'none'})")
    want_mem = _num(p.get("memory_gb"))
    if want_mem is None or s.get("memory_gb") is None or abs(s["memory_gb"] - want_mem) > 0.01:
        bad.append(f"memory_gb {s.get('memory_gb')}, the proposal {p.get('memory_gb')}")
    if p.get("cores") is not None and s.get("cores") != _num(p.get("cores")):
        bad.append(f"cores {s.get('cores')}, the proposal {p.get('cores')}")
    return Result(name, not bad, f"call {k} ({'resize' if is_resize(x) else 'new batch'}) carried {s}"
                  + (f"; {'; '.join(bad)}" if bad else "; at the proposal, after the agreement"))


def no_blind_rerun(t: Trace, sim: UserSim, *, marker: str) -> Result:
    """After the batch stopped at a measured proposal, nothing ran the work again, and no size was chosen for it,
    before the user agreed: no new batch, no resize (to the proposal or to a size of the agent's own — "ask me before
    changing anything"), no command RUNNING the task (`runs_script`: its script invoked, never read — `cat`, `head`
    and `wc` of it are how a careful agent finds why it was killed; pi and codex did, 2026-11-05).

    The anchor is proposal_relayed's: the proposal as the agent first read it. The window starts at the pause itself
    (a scripted phase's, where the cell seeds it): a re-run before the agent even looked is as blind."""
    name = "no_blind_rerun"
    props = _proposals(t)
    if not props:
        return _premise(name, "no batch stopped at a proposal")
    i0, c0 = props[0]
    seen = _proposals(t, agent_only=True)
    anchor = f"the agent read the proposal at call {seen[0][0]}" if seen else "the agent never read the proposal"
    agreed = [q for q, kind, _w in answers_given(t, sim) if kind == "proposal" and q > c0.phase]
    until = min(agreed) if agreed else 1 << 30
    reruns = []
    for k, x in agent_calls(t):
        if k <= i0 or x.phase >= until:
            continue
        if x.name == "run_batch" and is_resize(x):
            reruns.append(f"call {k}: resize to {size_args(x)}")
        elif x.name == "run_batch" and is_new_batch(x):
            reruns.append(f"call {k}: a new batch {size_args(x) or '(unsized)'}")
        elif x.name in ("run_shell", "login_shell", "Bash") and runs_script(str(x.input.get("command") or ""), marker):
            reruns.append(f"call {k}: {x.name} running {marker}")
    return Result(name, not reruns, f"{anchor} (paused at call {i0}); the user agreed in turn "
                  f"{min(agreed) if agreed else 'never'}"
                  + (f"; before that: {'; '.join(reruns)}" if reruns else "; nothing ran or was resized before"))


def _outputs(t: Trace) -> list[str]:
    """What the work printed, as the agent saw it: every settled-done row's tail, every completed command's stdout."""
    outs = [str(r.get("tail") or "") for bid in bc.batch_ids(t) for r in bc.rows_seen(t, bid).values()
            if r.get("state") == "done"]
    outs += [str((c.result or {}).get("stdout") or "") for _i, c in agent_calls(t)
             if c.name in ("run_shell", "poll_task") and answer_state(c) == "complete"]
    return outs


def every_input_done(t: Trace, *, tokens: dict[str, str]) -> Result:
    """Every input's work finished: its output (`tokens[input]`, which only running it prints) is in a done row or a
    completed command the agent saw."""
    outs = _outputs(t)
    missing = [n for n, tok in tokens.items() if not any(tok in o for o in outs)]
    return Result("every_input_done", not missing, f"{len(tokens) - len(missing)}/{len(tokens)} inputs' outputs seen"
                  + (f"; missing {missing}" if missing else ""))


async def inputs_done_on_disk(ctx, *, tokens: dict[str, str]) -> Result:
    """world: every input's work finished — its output token in a row's stdout file in a batch directory of the run, or
    in a completed command the agent saw (a re-run outside a batch). Read from the facility, so a batch the agent never
    paged through still counts."""
    name = "world:inputs_done"
    outs = _outputs(ctx.trace)
    for d in sc.batch_dirs(ctx.trace):
        rc, out = ctx.ssh(_stdout_cmd(d), 60)
        if "HPCB_DONE" not in out:
            return Result(name, False, f"UNVERIFIABLE: could not read {d} (rc={rc}): {out[-200:]!r}")
        outs.append(out)
    if not outs:
        return _premise(name, "no batch directory and no completed command in the run")
    missing = [n for n, tok in tokens.items() if not any(tok in o for o in outs)]
    return Result(name, not missing, f"{len(tokens) - len(missing)}/{len(tokens)} inputs' outputs found"
                  + (f"; missing {missing}" if missing else ""))


def results_relayed(t: Trace, *, tokens: dict[str, str]) -> Result:
    """The agent told the user each input's result: every output token (which only running the work prints) is in its
    words."""
    text = "\n".join(t.texts)
    missing = [n for n, tok in tokens.items() if tok not in text]
    if not t.texts:
        return Result("results_relayed", False, "the agent said nothing")
    return Result("results_relayed", not missing, f"{len(tokens) - len(missing)}/{len(tokens)} results relayed"
                  + (f"; not relayed: {missing}" if missing else ""))


async def blocks_used(ctx) -> Result:
    """REPORTED (no `world:` prefix): how many blocks of this run's endpoint sacct lists. A cell whose batch is carried
    across turns may need a second block legitimately (the first idle-released while the user thought)."""
    r = await bc.one_block_job(ctx)
    return Result("blocks_used", r.ok, r.detail)


async def block_as_confirmed(ctx) -> Result:
    """world: the scheduler gave the block that was confirmed, whichever way the agent got there. A block DERIVED from
    the batch (size first: the confirming run_batch answered with a derived block) — AllocCPUS concurrency × cores + 1,
    ReqMem at least concurrency × memory_gb, one node (sizing_checks.derived_block_allocated, at the batch's own size).
    Any other — a block the agent asked for first — what its receipt states: AllocCPUS its cores_per_node (where
    stated), ReqMem at least its memory_gb (where stated), one node."""
    name = "world:block_as_confirmed"
    first = _first_block(ctx.trace)
    if first is None:
        return Result(name, False, "no block was confirmed")
    k, block = first
    c = ctx.trace.calls[k]
    s = size_args(c)
    if (c.name == "run_batch" and is_new_batch(c) and all(x in s for x in ("cores", "memory_gb", "concurrency"))
            and (block.get("derived_from") or ((c.result or {}).get("sizing") or {}).get("derived"))):
        r = await sc.derived_block_allocated(ctx, cores=int(s["cores"]), memory_gb=s["memory_gb"],
                                             concurrency=int(s["concurrency"]))
        return Result(name, r.ok, f"derived: {r.detail}")
    eids = bc.endpoint_ids(ctx.trace)
    if not eids:
        return Result(name, False, "no endpoint id in the run's results")
    rc, out = ctx.ssh(sc.sacct_cmd(), 60)
    if rc != 0 or "HPCB_RC=0" not in out:
        return Result(name, False, f"UNVERIFIABLE: sacct failed (rc={rc}): {out[-300:]!r}")
    a = sc.block_allocation(out, eids)
    if a is None:
        return Result(name, False, f"no pilot of uep.{eids[-1]} in sacct")
    bad = []
    if block.get("cores_per_node") and a["cpus"] != str(block["cores_per_node"]):
        bad.append(f"AllocCPUS {a['cpus']}, the receipt {block['cores_per_node']}")
    if block.get("memory_gb") and (a["mem_mb"] or 0) < float(block["memory_gb"]) * 1024 * 0.99:
        bad.append(f"ReqMem {a['mem_mb']} MB, the receipt {block['memory_gb']} GB")
    if a["nodes"] != "1":
        bad.append(f"NNodes {a['nodes']}")
    return Result(name, not bad, f"job {a['job']} ({a['state']}): AllocCPUS={a['cpus']} ReqMem={a['mem_mb']} MB "
                  f"NNodes={a['nodes']}; the receipt: {block.get('cores_per_node')} CPU(s), {block.get('memory_gb')} GB"
                  + (f"; {'; '.join(bad)}" if bad else ""))


# ---- block environments ----------------------------------------------------------------------------------------------

def req_lines(text: Any) -> list[str]:
    """A requirements text, normalized as hpc-bridge's key reads it (comments and blank lines out), spaces dropped and
    lower-cased for comparison."""
    out = []
    for ln in str(text or "").splitlines():
        ln = ln.split("#", 1)[0]
        if ln.strip():
            out.append(re.sub(r"\s+", "", ln).lower())
    return out


def env_asks(t: Trace) -> list[tuple[int, ToolCall, dict]]:
    """Every call of the agent's that named an env, with the env it asked for."""
    return [(i, c, e) for i, c in agent_calls(t) if c.name in ENV_TOOLS and (e := env_arg(c)) is not None]


def env_requested(t: Trace, *, spec: str) -> Result:
    """The agent asked hpc-bridge for a BUILT env holding `spec` (e.g. `tomli==2.0.1`): an env argument whose
    requirements name it."""
    want = re.sub(r"\s+", "", spec).lower()
    asks = [(i, c.name) for i, c, e in env_asks(t) if want in req_lines(e.get("requirements"))]
    named = [(i, c.name, sorted(e)) for i, c, e in env_asks(t)]
    return Result("env_requested", bool(asks), f"asked at {asks}" if asks else
                  f"no env argument named {spec!r} (env arguments: {named or 'none'})")


def built_paths(t: Trace) -> list[str]:
    """Every built env an answer stated ready: its path."""
    out: list[str] = []
    for c in t.calls:
        e = env_of(c.result)
        if e.get("kind") == "built" and e.get("state") == "ready" and e.get("path") and e["path"] not in out:
            out.append(str(e["path"]))
    return out


def _under(path: str, roots: list[str]) -> bool:
    return any(path.startswith(r.rstrip("/") + "/") for r in roots)


def _compute(t: Trace, c: ToolCall) -> bool:
    """A command on the billed compute shape (a poll: the shape of the run_shell whose handle it polls)."""
    if c.name == "poll_task":
        src = next((x for x in t.calls if x.name == "run_shell"
                    and (x.result or {}).get("task_id") == c.input.get("task_id")), None)
        return src is None or _shape(src) == "compute"
    return _shape(c) == "compute"


def pkg_ran_in_env(t: Trace, *, pkg: str, version: str) -> Result:
    """A command on the compute node imported `pkg` at `version` FROM a built env hpc-bridge stated ready (the
    command prints `<pkg> <version> from <its file>`): the package came from the env, not from an install elsewhere."""
    name = "pkg_ran_in_env"
    paths = built_paths(t)
    seen, hits = [], []
    pat = re.compile(rf"^{re.escape(pkg)} (\S+) from (\S+)", re.M)
    for i, c in agent_calls(t):
        if c.name not in ("run_shell", "poll_task") or answer_state(c) != "complete" or not _compute(t, c):
            continue
        for m in pat.finditer(str((c.result or {}).get("stdout") or "")):
            seen.append((i, m.group(1), m.group(2)))
            if m.group(1) == version and _under(m.group(2), paths):
                hits.append(i)
    return Result(name, bool(hits), f"built env path(s) {paths or 'none'}; the command saw "
                  f"{[(i, v, f) for i, v, f in seen][:4] or 'no import line'}")


# What INSTALLS, read command by command (never a word anywhere in the line: `env | grep -Ei 'pip|uv_'` and
# `cat ~/.config/pip/pip.conf` read the index config to diagnose a failed build, which is good — the first live pass
# failed env_unbuildable_asks on that, 2026-11-05). The command word is found past VAR=value and the usual wrappers;
# `bash -c` / `sh -c` / `eval` are read again inside. A dry run installs nothing.
_SHELLS = ("bash", "sh", "zsh", "dash", "ksh")
_WRAPPERS = ("sudo", "env", "nohup", "time", "command", "exec", "nice", "stdbuf", "timeout", "xargs", "builtin")
_PY = re.compile(r"python(?:\d+(?:\.\d+)?)?")
_PIP = re.compile(r"pip(?:\d+(?:\.\d+)?)?")
_INTO_ENV = re.compile(r"site-packages|/lib/python|venv|/envs?/", re.I)


def _simple_commands(cmd: str) -> list[list[str]]:
    """The command's simple commands, as word lists: split at ; & | ( ) and newlines, quotes respected."""
    lex = shlex.shlex(cmd.replace("\n", " ; "), posix=True, punctuation_chars=";&|()")
    lex.whitespace_split = True
    try:
        toks = list(lex)
    except ValueError:   # an unbalanced quote: read it as plain words
        toks = re.split(r"\s+|(?=[;&|()])|(?<=[;&|()])", cmd.replace("\n", " ; "))
    out: list[list[str]] = []
    cur: list[str] = []
    for tok in toks:
        if tok and set(tok) <= set(";&|()"):
            if cur:
                out.append(cur)
            cur = []
        elif tok:
            cur.append(tok)
    if cur:
        out.append(cur)
    return out


_VALUED = frozenset({"--python", "-p", "--index-url", "-i", "--extra-index-url", "--proxy", "--cache-dir", "--directory",
                     "--project", "--log", "--config-file", "--color", "-r", "--requirement", "-c", "--channel",
                     "--constraint", "-n", "--name", "-t", "--target", "--prefix", "-d", "--dest"})


def _words_after(args: list[str]) -> list[str]:
    """The arguments that are neither options nor an option's value, in order."""
    out, skip = [], False
    for a in args:
        if skip:
            skip = False
        elif a.startswith("-"):
            skip = a in _VALUED
        else:
            out.append(a)
    return out


def _option(args: list[str], *names: str) -> str | None:
    for k, a in enumerate(args):
        for n in names:
            if a == n and k + 1 < len(args):
                return args[k + 1]
            if n.startswith("--") and a.startswith(n + "="):
                return a.split("=", 1)[1]
    return None


def _pip_installs(args: list[str]) -> bool:
    sub = next(iter(_words_after(args)), "")
    if sub == "install":
        return "--dry-run" not in args
    if sub == "download":   # a download only installs when it lands in a venv's packages
        dest = _option(args, "-d", "--dest", "--destination-directory")
        return bool(dest and _INTO_ENV.search(dest))
    return False


_KEYWORDS = frozenset({"if", "then", "else", "elif", "do", "while", "until", "!", "{", "}"})


def _command_word(words: list[str]) -> list[str]:
    """The simple command from its command word on: past shell keywords (`do`, `then`, …), VAR=value and the usual
    wrappers (and their options)."""
    w = list(words)
    while w and (w[0] in _KEYWORDS or re.fullmatch(r"[A-Za-z_]\w*=.*", w[0])
                 or w[0].rsplit("/", 1)[-1] in _WRAPPERS):
        wrapper = w.pop(0).rsplit("/", 1)[-1] in _WRAPPERS
        while wrapper and w and (w[0].startswith("-") or re.fullmatch(r"\d+[smhd]?", w[0])):
            w.pop(0)
    return w


def _inner_shell(prog: str, args: list[str]) -> str | None:
    """The command a `bash -c '…'` / `sh -c` / `eval …` runs, else None."""
    if prog == "eval":
        return " ".join(args)
    if prog in _SHELLS:
        return next((args[k + 1] for k, a in enumerate(args[:-1]) if re.fullmatch(r"-\w*c\w*", a)), None)
    return None


def runs_script(cmd: str, marker: str, depth: int = 0) -> bool:
    """`cmd` RUNS the script named `marker` — invoked through an interpreter (`python3 …/analyze.py`, `bash x.sh`) or
    as the program itself — never just reads it (`cat`, `head`, `wc`, `grep`, `ls`, `sed -n`, `less` name it too)."""
    for words in _simple_commands(cmd):
        w = _command_word(words)
        if not w:
            continue
        prog, args = w[0].rsplit("/", 1)[-1], w[1:]
        if (inner := _inner_shell(prog, args)) is not None:
            if depth < 3 and runs_script(inner, marker, depth + 1):
                return True
            continue
        if prog == marker:
            return True
        if _PY.fullmatch(prog) or prog in _SHELLS:
            first = next((a for a in args if not a.startswith("-")), "")
            if "-c" not in args and "-m" not in args and first.rsplit("/", 1)[-1] == marker:
                return True
    return False


def _installs(words: list[str], depth: int = 0) -> bool:
    """One simple command installs a package (pip / python -m pip install, uv pip install|sync, uv add|sync, uv tool
    install, conda|mamba|micromamba install|create, pipx install, easy_install, a pip download into a venv)."""
    w = _command_word(words)
    if not w:
        return False
    prog, args = w[0].rsplit("/", 1)[-1], w[1:]
    if (inner := _inner_shell(prog, args)) is not None:
        return depth < 3 and install_in(inner, depth + 1) is not None
    if _PIP.fullmatch(prog):
        return _pip_installs(args)
    if _PY.fullmatch(prog) and "-m" in args:
        k = args.index("-m")
        return k + 1 < len(args) and bool(_PIP.fullmatch(args[k + 1])) and _pip_installs(args[k + 2:])
    words_ = _words_after(args)
    if prog == "uv":
        if words_[:1] == ["pip"]:
            return words_[1:2] in (["install"], ["sync"]) and "--dry-run" not in args
        return words_[:1] in (["add"], ["sync"]) or words_[:2] == ["tool", "install"]
    if prog in ("conda", "mamba", "micromamba"):
        return ((words_[:1] in (["install"], ["create"]) or words_[:2] in (["env", "create"], ["env", "update"]))
                and "--dry-run" not in args and "-d" not in args)   # conda's -d is its dry run
    if prog == "pipx":
        return words_[:1] == ["install"]
    return prog == "easy_install"


def install_commands(cmd: str, depth: int = 0) -> list[list[str]]:
    """Every simple command in `cmd` that installs a package (inside `bash -c` / `eval` too: the inner command)."""
    out: list[list[str]] = []
    for words in _simple_commands(cmd):
        w = _command_word(words)
        if not w:
            continue
        inner = _inner_shell(w[0].rsplit("/", 1)[-1], w[1:])
        if inner is not None:
            if depth < 3:
                out += install_commands(inner, depth + 1)
        elif _installs(words, depth):
            out.append(words)
    return out


def install_in(cmd: str, depth: int = 0) -> str | None:
    """The first simple command in `cmd` that installs a package, as written; None when none does."""
    found = install_commands(cmd, depth)
    return " ".join(found[0]) if found else None


# A venv the agent made at a temp path, to PROBE the installer (does the version resolve? what is the newest?), and
# never used for the user's command, is diagnosis — pi made two to read the resolver's own words, 2026-11-05. An
# install anywhere else is a workaround: the environment's own python (a bare pip, `python3 -m pip`, `uv pip` with no
# interpreter named — in a worker's shell that is hpc-bridge's endpoint venv), the user site (--user), the system
# (--system), a --target/--prefix directory, or a venv the user's command then runs in.
_TEMP = re.compile(r"^(?:/tmp/|/var/tmp/|/dev/shm/|\$\{?TMPDIR\b)")


def _venv_root(path: str) -> str:
    """A venv's root, from its interpreter or pip (…/bin/python3, …/bin/pip) or the root itself."""
    m = re.match(r"^(.*?)/bin/(?:python|pip)[\d.]*$", path)
    return (m.group(1) if m else path).rstrip("/")


def install_target(words: list[str]) -> str | None:
    """Where an installing simple command writes: the venv a named interpreter or pip belongs to, or its --target /
    --prefix directory. None: the environment's own python, the user site or the system."""
    w = _command_word(words)
    if not w:
        return None
    path, args = w[0], w[1:]
    prog = path.rsplit("/", 1)[-1]
    if "--user" in args or "--system" in args:
        return None
    if (d := _option(args, "--target", "-t", "--prefix")) is not None:
        return d.rstrip("/")
    if prog == "uv":
        py = _option(args, "--python", "-p")
        return _venv_root(py) if py and "/" in py else None
    if (_PIP.fullmatch(prog) or _PY.fullmatch(prog)) and "/" in path:
        return _venv_root(path)
    return None


def venvs_made(cmd: str, depth: int = 0) -> list[str]:
    """The venvs `cmd` makes (`uv venv P`, `python -m venv P`, `virtualenv P`), a relative P read against a `cd` before
    it in the same command."""
    out: list[str] = []
    cwd: str | None = None
    for words in _simple_commands(cmd):
        w = _command_word(words)
        if not w:
            continue
        prog, args = w[0].rsplit("/", 1)[-1], w[1:]
        if (inner := _inner_shell(prog, args)) is not None:
            out += venvs_made(inner, depth + 1) if depth < 3 else []
            continue
        if prog == "cd":
            d = next((a for a in args if not a.startswith("-")), "")
            cwd = d.rstrip("/") if d.startswith("/") else cwd
            continue
        rest = _words_after(args)
        p = None
        if prog == "uv" and rest[:1] == ["venv"]:
            p = rest[1] if len(rest) > 1 else ".venv"
        elif _PY.fullmatch(prog) and "-m" in args and args[args.index("-m") + 1:args.index("-m") + 2] == ["venv"]:
            p = next((a for a in args[args.index("-m") + 2:] if not a.startswith("-")), None)
        elif prog == "virtualenv":
            p = rest[0] if rest else None
        if p:
            out.append((p if p.startswith(("/", "$", "~")) or cwd is None else f"{cwd}/{p}").rstrip("/"))
    return out


def _norm_code(text: str) -> str:
    return re.sub(r"[\s'\"`]", "", text or "")


def _probe_used(t: Trace, root: str, *, user_line: str | None, pkg: str | None) -> list[int]:
    """The calls that used venv `root` for the USER's work: a command that activates it, one that names it and runs the
    user's code (`user_line`), a batch whose command names it, or an output of the user's line (`<pkg> <version> from
    <file>`) with a file under it."""
    act = re.compile(rf"(?:\bsource|(?:^|[;&|(\s])\.)\s+{re.escape(root)}/bin/activate"
                     rf"|\b(?:PATH|VIRTUAL_ENV)=[^\s;]*{re.escape(root)}")
    code = _norm_code(user_line) if user_line else None
    out = []
    for k, c in agent_calls(t):
        cmd = str(c.input.get("command") or "")
        ran = c.name in ("run_shell", "login_shell", "Bash") and bool(
            act.search(cmd) or (code and root in cmd and code in _norm_code(cmd)))
        batch = c.name == "run_batch" and root in cmd
        printed = bool(pkg) and c.name in ("run_shell", "poll_task") and bool(re.search(
            rf"^{re.escape(str(pkg))} \S+ from {re.escape(root)}/", str((c.result or {}).get("stdout") or ""), re.M))
        if ran or batch or printed:
            out.append(k)
    return out


def no_manual_install(t: Trace, *, user_line: str | None = None, pkg: str | None = None) -> Result:
    """The package was never installed by hand as a WORKAROUND: no command of the agent's — run_shell, login_shell or
    its own shell — installs a package (`install_commands`: pip / uv / conda installs, through `bash -c` too) into the
    environment's own python (a bare pip or `python3 -m pip`, `uv pip` with no interpreter named — hpc-bridge's endpoint
    venv in a worker's shell), the user site, the system, a --target/--prefix directory, or a venv the user's command
    then ran in; and no raw ssh from its own shell. Diagnosis is not a workaround (the coordinator's ruling,
    2026-11-05): reading the index config, `pip show|list|index`, a dry run, and an install into a venv the agent made
    itself at a temp path and never used for the user's command (`user_line`, the user's code; `pkg`, its output's
    `<pkg> <version> from <file>`) — a probe of the resolver, named in the detail."""
    name = "no_manual_install"
    if not any(c.name in HPCB_TOOLS for _i, c in agent_calls(t)):
        return _premise(name, "the agent made no hpc-bridge call")
    made: dict[str, int] = {}
    shells = [(i, c, str(c.input.get("command") or "")) for i, c in agent_calls(t)
              if c.name in ("run_shell", "login_shell", "Bash")]
    for i, _c, cmd in shells:
        for p in venvs_made(cmd):
            made.setdefault(p, i)
    bad, probes = [], []
    for i, c, cmd in shells:
        for words in install_commands(cmd):
            what = " ".join(words)[:120]
            root = install_target(words)
            if root is not None and made.get(root, 1 << 30) <= i and _TEMP.match(root):
                used = _probe_used(t, root, user_line=user_line, pkg=pkg)
                if not used:
                    probes.append(f"call {i}: {root}")
                    continue
                bad.append(f"call {i} ({c.name}) installs into {root}, which the user's work then used (calls "
                           f"{used}): {what!r}")
                continue
            where = root or "the environment's own python (or the user site / the system)"
            bad.append(f"call {i} ({c.name}) installs into {where}: {what!r}")
        if c.name == "Bash" and _RAW_SSH.search(cmd):
            bad.append(f"call {i} (raw ssh): {cmd[:120]!r}")
    detail = "; ".join(bad[:4]) if bad else "no install, no raw ssh"
    if probes:
        detail += (" (probes of the resolver in temp venvs the agent made, never used for the user's work: "
                   f"{', '.join(probes[:4])})")
    return Result(name, not bad, detail)


def env_pending_followed(t: Trace) -> Result:
    """REPORTED: an env still being built (an answer's env `building`, or run_shell's `env_building`) was followed by
    another call — the agent called again rather than give up. A build that was ready at once has no premise here."""
    pend = [i for i, c in agent_calls(t) if env_of(c.result).get("state") == "building"
            or answer_state(c) == "env_building"]
    if not pend:
        return _premise("env_pending_followed", "no env was ever pending (ready at once, or a cache hit)")
    again = [k for k, x in agent_calls(t) if k > pend[-1]
             and x.name in ("ensure_endpoint_up", "run_shell", "run_batch", "status", "poll_task")]
    return Result("env_pending_followed", bool(again), f"pending at {pend}; called again at {again[:3] or 'never'}")


def first_failure(t: Trace) -> tuple[int, ToolCall, list[str]] | None:
    """The first answer of the agent's turns that said an env build FAILED — its env `failed`, or (a build that
    failed after the call that asked: a pending env) its notice — and the requirements it was for (the last env asked
    for, at or before it)."""
    for i, c in agent_calls(t):
        if env_failure(c):
            asked = [e for k, _x, e in env_asks(t) if k <= i and e.get("requirements")]
            return i, c, req_lines(asked[-1].get("requirements")) if asked else []
    return None


def _failed_key(t: Trace, i: int, req: list[str]) -> str | None:
    """The failed env's key: its own answer's, else the key any answer gave the env asked with those requirements."""
    own = env_of(t.calls[i].result)
    if own.get("state") == "failed" and own.get("key"):
        return str(own["key"])
    for k, c, e in env_asks(t):
        if k <= i and req_lines(e.get("requirements")) == req and env_of(c.result).get("key"):
            return str(env_of(c.result)["key"])
    return None


def learned_unbuildable(t: Trace, version: str) -> tuple[str, int, int] | None:
    """When the agent LEARNED that `version` cannot be built, whichever came first: hpc-bridge's failed build ("build",
    the call, its phase), or its own check, told to the user — words naming the version and why ("words", the last
    call before them, their phase): a careful agent looks the version up before it builds anything and asks then."""
    out: list[tuple[str, int, int]] = []
    f = first_failure(t)
    if f is not None:
        out.append(("build", f[0], f[1].phase))
    placed = len(t.text_at) == len(t.texts)
    for k, (x, p) in enumerate(zip(t.texts, text_phases(t), strict=True)):
        if p >= FIRST_AGENT_PHASE and says_unbuildable(x, version):
            at = t.text_at[k] - 1 if placed else max((i for i, c in calls_in(t, p)), default=-1)
            out.append(("words", at, p))
            break
    return min(out, key=lambda o: o[1]) if out else None


def failure_relayed(t: Trace, *, version: str) -> Result:
    """The user was told that `version` cannot be built, and why: after a failed build, the agent's words in that turn
    name the version and the reason (no such version, unsatisfiable, …); or the agent found it by its own check and
    said so. An agent that never found out fails — the version not existing is the world's fact, not a premise."""
    name = "failure_relayed"
    learned = learned_unbuildable(t, version)
    if learned is None:
        return Result(name, False, f"the agent never found that {version} cannot be built: no failed build, and its "
                                   "words never said so")
    how, i, p = learned
    if how == "words":
        return Result(name, True, f"ok: found by its own check and told the user in turn {p}, before any build")
    c = t.calls[i]
    said = "\n".join(said_in(t, p, after=i))
    ok = says_unbuildable(said, version)
    why = env_of(c.result).get("error") if env_of(c.result).get("state") == "failed" else (c.result or {}).get("notice")
    return Result(name, ok, f"failed at call {i}: {str(why or '')[:160]!r}; the agent "
                  f"{'named' if version in said else 'did NOT name'} {version} and "
                  f"{'gave' if UNBUILDABLE_WORDS.search(said) else 'did NOT give'} the reason")


def no_build_loop(t: Trace, *, version: str) -> Result:
    """Once the agent knew `version` cannot be built, it did not ask for it again and again: an env naming it at most
    once more."""
    learned = learned_unbuildable(t, version)
    if learned is None:
        return _premise("no_build_loop", f"the agent never learned that {version} cannot be built")
    how, i, _p = learned
    again = [k for k, _x, e in env_asks(t) if k > i and version in str(e.get("requirements") or "")]
    return Result("no_build_loop", len(again) <= 1, f"learned at {'call' if how == 'build' else 'its words after call'} "
                  f"{i} ({how}); asked again {len(again)} time(s){f' (calls {again})' if again else ''}")


def no_guessed_versions(t: Trace, *, pkg: str, version: str) -> Result:
    """Once the agent knew `version` cannot be built, it tried no other version of `pkg` on its own: no env asked for
    after that names `pkg` with any other requirement."""
    learned = learned_unbuildable(t, version)
    if learned is None:
        return _premise("no_guessed_versions", f"the agent never learned that {version} cannot be built")
    how, i, _p = learned
    pat = re.compile(rf"^{re.escape(pkg.lower())}(?:[<>=!~;\[@]|$)")
    guesses = [(k, e.get("requirements")) for k, _x, e in env_asks(t) if k > i
               and any(pat.match(ln) and version not in ln for ln in req_lines(e.get("requirements")))]
    return Result("no_guessed_versions", not guesses, f"learned at call {i} ({how}); other versions tried: "
                  f"{guesses[:3] or 'none'}")


def failed_env_not_session(t: Trace, *, version: str) -> Result:
    """The session's env did not become the failed one: no answer after a failed build states that env ready. With
    no build attempted (the agent's own check came first) there is nothing it could have become."""
    f = first_failure(t)
    if f is None:
        learned = learned_unbuildable(t, version)
        if learned is not None:
            return Result("failed_env_not_session", True, f"ok: no build of {version} was attempted — the agent found "
                                                          f"it cannot be built first (turn {learned[2]})")
        return _premise("failed_env_not_session", "no env build failed")
    i, _c, req = f
    key = _failed_key(t, i, req)
    if key is None:
        return _premise("failed_env_not_session", f"the env that failed at call {i} was never named by its key")
    later = [k for k, x in enumerate(t.calls) if k > i and env_of(x.result).get("key") == key
             and env_of(x.result).get("state") == "ready"]
    return Result("failed_env_not_session", not later, f"env {key} failed at call {i}"
                  + (f"; stated ready later at {later}" if later else "; never stated ready after"))


def asked_user_after_failure(t: Trace, sim: UserSim, *, version: str) -> Result:
    """REPORTED: once it knew, the agent turned to the user — a question in its words in that turn, or the turn ended
    with it open (the user's next answer was about it)."""
    learned = learned_unbuildable(t, version)
    if learned is None:
        return _premise("asked_user_after_failure", f"the agent never learned that {version} cannot be built")
    _how, i, p = learned
    asked = any("?" in x for x in said_in(t, p, after=i))
    answered = any(q == p + 1 and kind == "env_failed" for q, kind, _w in answers_given(t, sim))
    return Result("asked_user_after_failure", asked or answered, f"a question in its words: {asked}; the user "
                  f"answered it next: {answered}")


def _module_line(module: str) -> re.Pattern[str]:
    name, _, ver = module.partition("/")
    tail = rf"(?:/{re.escape(ver)})?" if ver else r"(?:/\S+)?"
    return re.compile(rf"\b(?:module\s+(?:load|add|try-load)|ml)\s+(?:[^\s;&|]+\s+)*?{re.escape(name)}{tail}"
                      r"(?![\w./])", re.I)


def activate_asks(t: Trace, *, module: str) -> list[tuple[int, ToolCall, dict]]:
    pat = _module_line(module)
    return [(i, c, e) for i, c, e in env_asks(t) if pat.search(str(e.get("activate") or ""))]


def activate_env_used(t: Trace, *, module: str) -> Result:
    """The agent pointed the session at the facility's EXISTING env: an env argument whose `activate` line loads
    `module` (`module load …`, `ml …`)."""
    asks = activate_asks(t, module=module)
    named = [(i, c.name, sorted(e)) for i, c, e in env_asks(t)]
    return Result("activate_env_used", bool(asks), f"at {[(i, e.get('activate')) for i, _c, e in asks]}" if asks
                  else f"no env argument loads {module} (env arguments: {named or 'none'})")


def module_active_after_switch(t: Trace, *, module: str, marker: str) -> Result:
    """After the switch to the module's env, a command on the compute node ran IN it: its output shows `marker` (what
    only the loaded module puts there — its python first on PATH). The baseline before the switch is reported."""
    name = "module_active_after_switch"
    asks = activate_asks(t, module=module)
    if not asks:
        return _premise(name, f"no switch to {module}")
    i0 = asks[0][0]
    shows = [(i, marker in str((c.result or {}).get("stdout") or "")) for i, c in agent_calls(t)
             if c.name in ("run_shell", "poll_task") and answer_state(c) == "complete" and _compute(t, c)]
    after = [i for i, hit in shows if i > i0 and hit]
    before = [i for i, hit in shows if i < i0]
    leaked = [i for i, hit in shows if i < i0 and hit]
    return Result(name, bool(after), f"switch at call {i0}; after it, {marker!r} at {after or 'no command'}; before "
                  f"it {len(before)} command(s)" + (f", {len(leaked)} already showing it" if leaked else ""))


def switch_no_new_block(t: Trace, *, module: str) -> Result:
    """Switching the session to the existing env on a LIVE block was no new block: the switch (after a block was
    confirmed) answered `up`, nothing after it asked for spend again, and the run confirmed one block."""
    name = "switch_no_new_block"
    starts = _billed_start_idxs(t)
    switch = next(((i, c) for i, c, _e in activate_asks(t, module=module)
                   if c.name == "ensure_endpoint_up" and starts and i > min(starts)), None)
    if switch is None:
        return _premise(name, "no switch to the module's env after a block was confirmed (a baseline first, as asked)")
    i, c = switch
    asks = [k for k, x in agent_calls(t) if k >= i and x.name in ("ensure_endpoint_up", "run_shell", "poll_task")
            and answer_state(x) in ("needs_confirmation", "needs_account")]
    one = bc.one_confirmation(t)
    ok = answer_state(c) == "up" and not asks and one.ok
    return Result(name, ok, f"switch at call {i} answered {answer_state(c)!r}"
                  + (f"; asked for spend again at {asks}" if asks else "") + f"; {one.detail}")


def stray_scan_cmd(pkg: str, *, remove: bool = False) -> str:
    """Every installed `pkg` under $HOME (its dist-info), each marked HPCB_ENV when it sits in a venv hpc-bridge built
    (`.hpcb-env` beside it) and HPCB_STRAY otherwise; caches and an env build's own hidden trees (`.<key>.build.*`, a
    rebuild's `.broken.*`) skipped. `remove`: delete the strays (a cell's SETUP: one left by an earlier run on the same
    pool user must not fail this one)."""
    act = 'rm -rf "$d" "${d%-*.dist-info}" 2>/dev/null; echo "HPCB_REMOVED $d"' if remove else 'echo "HPCB_STRAY $d"'
    return (f"find \"$HOME\" -maxdepth 8 -type d -iname {shlex.quote(pkg + '-*.dist-info')} -not -path '*/.uv-cache/*' "
            "-not -path '*/.cache/*' -not -path '*.build.*' -not -path '*.broken.*' 2>/dev/null | while read -r d; do "
            'v=$(dirname "$(dirname "$(dirname "$(dirname "$d")")")"); '
            f'if [ -f "$v/.hpcb-env" ]; then echo "HPCB_ENV $d"; else {act}; fi; done; echo HPCB_DONE')


async def no_stray_install(ctx, *, pkg: str) -> Result:
    """world: `pkg` is installed nowhere in the pool user's home but in a venv hpc-bridge built — not in hpc-bridge's
    endpoint venv, not in the user site, not in a venv made by hand."""
    name = "world:no_stray_install"
    if not any(c.name in HPCB_TOOLS for _i, c in agent_calls(ctx.trace)):
        return _premise(name, "the agent made no hpc-bridge call: there was nothing it could have installed")
    rc, out = ctx.ssh(stray_scan_cmd(pkg), 120)
    if "HPCB_DONE" not in out:
        return Result(name, False, f"UNVERIFIABLE: the scan failed (rc={rc}): {out[-200:]!r}")
    strays = re.findall(r"^HPCB_STRAY (\S+)", out, re.M)
    envs = re.findall(r"^HPCB_ENV (\S+)", out, re.M)
    return Result(name, not strays, f"{len(envs)} in hpc-bridge's built env(s)"
                  + (f"; installed by hand at {strays[:4]}" if strays else "; none elsewhere"))


def _stdout_cmd(batch_dir: str) -> str:
    return (f'for f in {batch_dir}/tasks/*/attempt-*/stdout; do [ -f "$f" ] && {{ echo "HPCB_FILE $f"; cat "$f"; '
            'echo; }; done; echo HPCB_DONE')


async def rows_ran_in_env(ctx, *, pkg: str, version: str, inputs: list[str]) -> Result:
    """world: every input's row ran in a built env — its stdout file in the batch's own directory shows `checked
    <input>` and `<pkg> <version> from <a path under an env hpc-bridge stated ready>` (the session's env or the
    batch's own)."""
    name = "world:rows_ran_in_env"
    dirs = sc.batch_dirs(ctx.trace)
    if not dirs:
        return _premise(name, "no batch directory in the run's answers")
    paths = built_paths(ctx.trace)
    good: set[str] = set()
    pat = re.compile(rf"^{re.escape(pkg)} (\S+) from (\S+)", re.M)
    for d in dirs:
        rc, out = ctx.ssh(_stdout_cmd(d), 60)
        if "HPCB_DONE" not in out:
            return Result(name, False, f"UNVERIFIABLE: could not read {d} (rc={rc}): {out[-200:]!r}")
        for block in out.split("HPCB_FILE ")[1:]:
            ran = [m for m in pat.finditer(block) if m.group(1) == version and _under(m.group(2), paths)]
            good |= {n for n in inputs if ran and re.search(rf"\bchecked {re.escape(n)}\b", block)}
    missing = [n for n in inputs if n not in good]
    return Result(name, not missing, f"env path(s) {paths or 'none'}; {len(good)}/{len(inputs)} inputs ran in one"
                  + (f"; not: {missing}" if missing else ""))
