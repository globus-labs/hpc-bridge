"""Graders for the AGENT cells of batch row sizing and block environments (agent_cells.py: a model drives the tools, a
scripted user answers). Pure and SDK-free (scenarios import it on the host, as they import batch_checks).

They judge what the agent DID — the arguments it passed, hpc-bridge's answers and what it did next, the order of the
user's answers and the billed starts, the world on the facility — and what it TOLD the user only where the user needs
a fact (the block's CPUs before the yes, the proposed size, the installer's reason, the results). Never phrasing beyond
that, and never wait cadence or turn-taking: "called again after `env_building`" is graded, how long it waited is not.

Every grader reads the AGENT's turns only (phase ≥ 1): the scripted connect in phase 0 is the premise, not the agent's
doing. A grader whose premise never happened (no `needs_sizing`, no proposal, no failed build, no switch on a live
block) says "premise not met" and FAILS — never a vacuous pass.
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
    mentions_count,
    mentions_memory,
    said_in,
    size_args,
)
from invariants import _HPC_TOOLS, _RAW_SSH, Result, ToolCall, Trace, _billed_start_idxs, _shape

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
                   name: str = "sizes_as_given") -> Result:
    """The rows were sized as the USER said: every new batch the agent asked for named no other size (none invented),
    and the batch that was recorded carried all of it — `cores`, `memory_gb`, and a `concurrency` of at most the
    user's."""
    asks = [(i, c) for i, c in agent_calls(t) if is_new_batch(c)]
    if not asks:
        return _premise(name, "the agent asked for no batch")
    wrong = [f"call {i}: {', '.join(w)}" for i, c in asks
             if (w := _wrong_size(c, cores=cores, memory_gb=memory_gb, max_concurrency=max_concurrency))]
    acc = accepted_new_batch(t)
    if acc is None:
        return Result(name, False, "no batch was recorded" + (f"; sizes not the user's: {'; '.join(wrong)}"
                                                               if wrong else ""))
    i, c = acc
    s = size_args(c)
    missing = [k for k in ("cores", "memory_gb", "concurrency") if k not in s]
    ok = not wrong and not missing
    return Result(name, ok, f"the batch recorded at call {i} carried {({k: s.get(k) for k in s})}"
                  + (f"; dropped: {missing}" if missing else "") + (f"; not the user's: {'; '.join(wrong)}" if wrong
                                                                   else "") + f" (the user's: {cores} core(s), "
                  f"{memory_gb:g} GB, at most {max_concurrency} at once)")


def _derived_asks(t: Trace) -> list[tuple[int, ToolCall]]:
    out = []
    for i, c in agent_calls(t):
        r = c.result or {}
        b = r.get("block")
        if (c.name == "run_batch" and answer_state(c) == "needs_confirmation" and isinstance(b, dict)
                and (b.get("derived_from") or (r.get("sizing") or {}).get("derived"))):
            out.append((i, c))
    return out


def terms_shown_before_yes(t: Trace, sim: UserSim) -> Result:
    """The spend question for the block DERIVED from the rows reached the user before any of it started: the agent's
    words after hpc-bridge asked, in that turn, name the block's CPUs; it started nothing in that turn; and the user's
    next answer was the yes."""
    name = "terms_shown_before_yes"
    asks = _derived_asks(t)
    if not asks:
        return _premise(name, "no run_batch asked for a block derived from the rows")
    i, c = asks[0]
    block = (c.result or {}).get("block") or {}
    cpus = block.get("cores_per_node")
    said = "\n".join(said_in(t, c.phase, after=i))
    named = mentions_count(said, cpus)
    early = [k for k in _billed_start_idxs(t) if k > i and t.calls[k].phase == c.phase]
    yes = (c.phase + 1) in consent_phases(t, sim)
    ok = named and not early and yes
    return Result(name, ok, f"asked at call {i} (turn {c.phase}): {cpus} CPU(s), {block.get('memory_gb')} GB; "
                  f"{'named' if named else 'NOT named'} to the user"
                  + (f"; started in the same turn at call(s) {early}" if early else "")
                  + ("; the user's next answer was the yes" if yes else "; the user's next answer was not the yes"))


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


def asked_for_sizes(t: Trace, sim: UserSim) -> Result:
    """hpc-bridge answered `needs_sizing` (no size, no block) and the agent put it to the USER: in that turn it named
    no size of its own, started nothing, sent the same unsized call at most twice, and ended the turn on the size
    question — the user's next answer was the sizes."""
    name = "asked_for_sizes"
    first = next(((i, c) for i, c in agent_calls(t) if c.name in ("run_batch", "status")
                  and answer_state(c) == "needs_sizing" and not (c.result or {}).get("proposal")), None)
    if first is None:
        return _premise(name, "no run_batch answered needs_sizing (the agent never met the size question)")
    i, c = first
    later = [(k, x) for k, x in calls_in(t, c.phase) if k > i]
    invented = [k for k, x in later if x.name == "run_batch" and _sized(x)]
    early = [k for k in _billed_start_idxs(t) if k > i and t.calls[k].phase == c.phase]
    again = [k for k, x in calls_in(t, c.phase) if is_new_batch(x) and not _sized(x) and not is_pilot(x)
             and answer_state(x) == "needs_sizing"]
    answered = any(q == c.phase + 1 and kind == "sizes" for q, kind, _w in answers_given(t, sim))
    ok = not invented and not early and len(again) <= 2 and answered
    return Result(name, ok, f"needs_sizing at call {i} (turn {c.phase})"
                  + (f"; sized it alone at call(s) {invented}" if invented else "")
                  + (f"; started a block at call(s) {early}" if early else "")
                  + (f"; the same unsized call {len(again)} times" if len(again) > 2 else "")
                  + ("; the user's next answer gave the sizes" if answered else "; the turn did not end on the size "
                     "question (the user never gave sizes)"))


def pilot_used(t: Trace) -> Result:
    """The agent measured a row first the tool's way: a run_batch with `pilot` ≥ 1 that recorded its batch."""
    pilots = [(i, c) for i, c in agent_calls(t) if is_pilot(c)]
    recorded = [i for i, c in pilots if (c.result or {}).get("batch_id") and answer_state(c) not in _NOT_ACCEPTED]
    return Result("pilot_used", bool(recorded), f"pilot call(s) {[i for i, _c in pilots] or 'none'}; recorded at "
                  f"{recorded or 'none'}")


def _proposals(t: Trace) -> list[tuple[int, ToolCall]]:
    return [(i, c) for i, c in agent_calls(t) if c.name in ("run_batch", "status")
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
    i, c = props[0]
    p = (c.result or {})["proposal"]
    said = "\n".join(said_in(t, c.phase, after=i))
    mem = mentions_memory(said, _num(p.get("memory_gb")))
    cores = p.get("cores") is None or mentions_count(said, p.get("cores"))
    acted = [k for k, x in calls_in(t, c.phase) if k > i and (is_resize(x) or (is_new_batch(x) and _sized(x)))]
    agreed = any(q == c.phase + 1 and kind == "proposal" for q, kind, _w in answers_given(t, sim))
    ok = mem and cores and not acted and agreed
    return Result(name, ok, f"proposal at call {i} (turn {c.phase}): cores {p.get('cores')}, {p.get('memory_gb')} GB "
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
    """After the batch stopped at a measured proposal, nothing ran the work again before the user agreed: no new batch,
    no resize, no compute command running the task (`marker`, its script's name)."""
    name = "no_blind_rerun"
    props = _proposals(t)
    if not props:
        return _premise(name, "no batch stopped at a proposal")
    i0, c0 = props[0]
    agreed = [q for q, kind, _w in answers_given(t, sim) if kind == "proposal" and q > c0.phase]
    until = min(agreed) if agreed else 1 << 30
    reruns = [k for k, x in agent_calls(t) if k > i0 and x.phase < until
              and ((x.name == "run_batch" and (is_new_batch(x) or is_resize(x)))
                   or (x.name in ("run_shell", "login_shell") and marker in str(x.input.get("command") or ""))
                   or (x.name == "Bash" and marker in str(x.input.get("command") or "")))]
    return Result(name, not reruns, f"proposal at call {i0}; the user agreed in turn {min(agreed) if agreed else 'never'}"
                  + (f"; ran again before that at call(s) {reruns}" if reruns else "; nothing ran again before"))


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


async def derived_block_as_confirmed(ctx) -> Result:
    """world: the scheduler gave the block derived from the rows the agent recorded — AllocCPUS concurrency × cores +
    1, ReqMem at least concurrency × memory_gb, one node (sizing_checks.derived_block_allocated, at the batch's own
    size)."""
    acc = accepted_new_batch(ctx.trace)
    if acc is None:
        return _premise("world:derived_block_allocated", "no batch was recorded")
    s = size_args(acc[1])
    if not all(k in s for k in ("cores", "memory_gb", "concurrency")):
        return _premise("world:derived_block_allocated", f"the recorded batch was not sized ({s})")
    return await sc.derived_block_allocated(ctx, cores=int(s["cores"]), memory_gb=s["memory_gb"],
                                            concurrency=int(s["concurrency"]))


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


_INSTALL = re.compile(
    r"(?:^|[\s;&|(`'\"/=])(?:pip(?:3(?:\.\d+)?)?|uv\s+pip|python(?:3(?:\.\d+)?)?\s+-m\s+pip|conda|mamba|micromamba)"
    r"\s+install\b|\beasy_install\b|\bpipx\s+install\b|\buv\s+(?:add|tool\s+install)\b|\bensurepip\b", re.I)


def no_manual_install(t: Trace) -> Result:
    """The package was never installed by hand: no command of the agent's — run_shell, login_shell or its own shell —
    runs an installer (pip, `uv pip`, conda, …), and no raw ssh from its own shell. hpc-bridge's built env is the way;
    an install into the session or into hpc-bridge's endpoint venv is what the cell exists to catch."""
    name = "no_manual_install"
    if not any(c.name in HPCB_TOOLS for _i, c in agent_calls(t)):
        return _premise(name, "the agent made no hpc-bridge call")
    bad = []
    for i, c in agent_calls(t):
        cmd = str(c.input.get("command") or "")
        if c.name in ("run_shell", "login_shell", "Bash") and _INSTALL.search(cmd):
            bad.append(f"call {i} ({c.name}): {cmd[:120]!r}")
        elif c.name == "Bash" and _RAW_SSH.search(cmd):
            bad.append(f"call {i} (raw ssh): {cmd[:120]!r}")
    return Result(name, not bad, "no installer, no raw ssh" if not bad else "; ".join(bad[:4]))


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


_REASON = re.compile(
    r"no (?:matching |such )?(?:version|release|distribution)|(?:does|do|did)\s*n[o']?t exist|doesn'?t exist|not exist"
    r"|not (?:be )?found|unsatisfiable|no solution|not available|isn'?t available|unavailable|not on pypi|"
    r"not published|never (?:been )?released|(?:could ?n[o']?t|cannot|can'?t) (?:find|resolve)|invalid version", re.I)


def failure_relayed(t: Trace, *, version: str) -> Result:
    """The build failure reached the user with the installer's reason: the agent's words after it, in that turn, name
    the version and why it could not be built (no such version, unsatisfiable, …)."""
    name = "failure_relayed"
    f = first_failure(t)
    if f is None:
        return _premise(name, "no env build failed")
    i, c, _req = f
    said = "\n".join(said_in(t, c.phase, after=i))
    ok = version in said and bool(_REASON.search(said))
    why = env_of(c.result).get("error") if env_of(c.result).get("state") == "failed" else (c.result or {}).get("notice")
    return Result(name, ok, f"failed at call {i}: {str(why or '')[:160]!r}; the agent "
                  f"{'named' if version in said else 'did NOT name'} {version} and "
                  f"{'gave' if _REASON.search(said) else 'did NOT give'} the reason")


def no_build_loop(t: Trace) -> Result:
    """The failed env was not asked for again and again: the same requirements at most once more after the failure."""
    f = first_failure(t)
    if f is None:
        return _premise("no_build_loop", "no env build failed")
    i, _c, req = f
    again = [k for k, _x, e in env_asks(t) if k > i and req_lines(e.get("requirements")) == req]
    return Result("no_build_loop", len(again) <= 1, f"failed at call {i}; the same requirements asked again "
                  f"{len(again)} time(s){f' (calls {again})' if again else ''}")


def no_guessed_versions(t: Trace, *, pkg: str) -> Result:
    """After the failure the agent tried no other version of `pkg` on its own: no env asked for after it names `pkg`
    with any other requirement line."""
    f = first_failure(t)
    if f is None:
        return _premise("no_guessed_versions", "no env build failed")
    i, _c, req = f
    pat = re.compile(rf"^{re.escape(pkg.lower())}(?:[<>=!~;\[@]|$)")
    guesses = [(k, e.get("requirements")) for k, _x, e in env_asks(t) if k > i
               and any(pat.match(ln) for ln in req_lines(e.get("requirements")))
               and req_lines(e.get("requirements")) != req]
    return Result("no_guessed_versions", not guesses, f"failed at call {i} ({req}); other versions tried: "
                  f"{guesses[:3] or 'none'}")


def failed_env_not_session(t: Trace) -> Result:
    """The session's env did not become the failed one: no answer after the failure states that env ready."""
    f = first_failure(t)
    if f is None:
        return _premise("failed_env_not_session", "no env build failed")
    i, _c, req = f
    key = _failed_key(t, i, req)
    if key is None:
        return _premise("failed_env_not_session", f"the env that failed at call {i} was never named by its key")
    later = [k for k, x in enumerate(t.calls) if k > i and env_of(x.result).get("key") == key
             and env_of(x.result).get("state") == "ready"]
    return Result("failed_env_not_session", not later, f"env {key} failed at call {i}"
                  + (f"; stated ready later at {later}" if later else "; never stated ready after"))


def asked_user_after_failure(t: Trace, sim: UserSim) -> Result:
    """REPORTED: after the failure the agent turned to the user — a question in its words in that turn, or the turn
    ended with the failure open (the user's next answer was about it)."""
    f = first_failure(t)
    if f is None:
        return _premise("asked_user_after_failure", "no env build failed")
    i, c, _req = f
    asked = any("?" in x for x in said_in(t, c.phase, after=i))
    answered = any(q == c.phase + 1 and kind == "env_failed" for q, kind, _w in answers_given(t, sim))
    return Result("asked_user_after_failure", asked or answered, f"a question in its words: {asked}; the user "
                  f"answered the failure next: {answered}")


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
