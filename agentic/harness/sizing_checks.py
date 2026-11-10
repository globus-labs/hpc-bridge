"""Batch row sizing (PR 2; vault `Planned/Batch and swarm execution.md`, "Batch row sizing"): what the sizing scenarios
share — the probe each row prints, the graders that read a trace, and the world checks that read each row's own
lines from its stdout file in the batch's results directory.

Pure and SDK-free (scenarios import it on the host, as they import batch_checks).

The tool surface the graders read (the stage-A checkpoint fixes it): run_batch(…, cores, memory_gb, gpus, concurrency,
pin, pilot) and status(id) answer a BatchStatus with `phase` (… | needs_sizing), `sizing` {cores, memory_gb, gpus,
concurrency, pin, slots, bound_by, defaulted, missing, derived}, `facts` {source, cpus, physical_cores, memory_gb,
gpus, workers, candidates}, `proposal` {cores, memory_gb, gpus, concurrency}, `block` (BlockTerms: cores_per_node,
memory_gb, gpus_per_node, whole_node, derived_from), and `results` rows with why, peak_rss_mb, cpu_s, wall_s, nproc,
cpus, gpu_ids, over_size.

THE ROW PROBE. Each sized scenario's command starts and ends with its own lines, so the world checks need nothing from
the wrapper's internals (they hold on main's fallback too, where they fail on their own terms):

    HPCB_ROW <n> k=<cores> start=<epoch.ns> nproc=<nproc as the row sees it> aff=<Cpus_allowed_list> \
        cpus=<HPCB_SLOT_CPUS> gpu=<CUDA_VISIBLE_DEVICES or unset>
    … the row's work …
    HPCB_ROW_END <n> end=<epoch.ns>

`nproc` is GNU nproc in the row's environment (it honours OMP_NUM_THREADS); `aff` is the kernel's mask — both must
equal the row's cores. Overlap is counted from each row's own start and end (a LOWER bound on concurrency: rows that
ran together), never from the server's admissions (an upper bound only).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import batch_checks as bc
from invariants import Result, Trace, _billed_start_idxs

MEMORY_RC = 87           # the wrapper's rc for a row it killed at its memory (batch.MEMORY_RC; why "memory")


def probe(cores: int, work: str, *, field: str = "n") -> str:
    """A row command: the start probe line, `work`, the end line. `{field}` is the row's own number (a bare word, as
    run_batch's template reader requires)."""
    return (f"echo HPCB_ROW {{{field}}} k={cores} start=$(date +%s.%N) "
            "nproc=$(nproc) aff=$(awk '/^Cpus_allowed_list/ {print $2}' /proc/self/status) "
            "cpus=${HPCB_SLOT_CPUS:-none} gpu=${CUDA_VISIBLE_DEVICES-unset}; "
            f"{work}; echo HPCB_ROW_END {{{field}}} end=$(date +%s.%N)")


# ---- the script's steps ----------------------------------------------------------------------------------------------

ENDED = ("done", "cancelled", "refused", "needs_confirmation", "needs_sizing")


async def accept(ops, **args: Any) -> dict:
    """run_batch(**args), which must RECORD the batch (on main, or with a sizing argument the server does not take, the
    cell aborts here: its premise is the sized batch)."""
    r = await ops.call("run_batch", **args)
    if not r.get("batch_id") or str(r.get("phase")) in ("refused", "needs_sizing", "needs_confirmation"):
        ops.abort(f"run_batch did not accept the sized batch: phase={r.get('phase')!r} {bc._brief(r)!r}")
    ops.say(f"HPCB_RESULTS_DIR {r.get('results_dir')}")
    ops.say(f"HPCB_SIZING {json.dumps(r.get('sizing'), default=str)}")
    return r


async def finish(ops, bid: str, rows: int, *, timeout_s: float = 600.0) -> tuple[dict, dict[int, dict]]:
    """status(bid, wait_s) until the batch stops, then every row."""
    st = await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ENDED, timeout_s=timeout_s)
    got = (await bc.all_rows(ops, bid, rows))["rows"]
    ops.say(f"{bid}: phase {st.get('phase')!r}; rows " + json.dumps(
        {i: (r.get("state"), r.get("exit_code"), r.get("why"), r.get("cpus")) for i, r in sorted(got.items())}))
    return st, got


# ---- reading a trace -------------------------------------------------------------------------------------------------

def run_batch_answers(t: Trace) -> list[tuple[int, dict]]:
    return [(i, c.result or {}) for i, c in t.named("run_batch")]


def accepted(t: Trace) -> list[tuple[int, dict]]:
    """run_batch answers that recorded a batch (not needs_sizing / needs_confirmation / refused)."""
    return [(i, r) for i, r in run_batch_answers(t)
            if r.get("batch_id") and str(r.get("phase")) not in ("refused", "needs_sizing", "needs_confirmation")]


def sizing_of(r: dict) -> dict:
    return dict(r.get("sizing") or {})


def slots_announced(t: Trace) -> int | None:
    """The slots the first accepted batch announced on its block (`sizing.slots`, else its `capacity`)."""
    for _i, r in accepted(t):
        s = sizing_of(r).get("slots")
        if isinstance(s, int):
            return s
        if isinstance(r.get("capacity"), int):
            return int(r["capacity"])
    return None


def statuses(t: Trace, bid: str | None = None) -> list[dict]:
    return [c.result or {} for _i, c in t.named("status", "run_batch")
            if (c.result or {}).get("batch_id") and (bid is None or (c.result or {}).get("batch_id") == bid)]


def rows_reported(t: Trace) -> dict[str, dict[int, dict]]:
    """batch id -> index -> the latest result row any answer carried."""
    out: dict[str, dict[int, dict]] = {}
    for r in statuses(t):
        for row in r.get("results") or []:
            if isinstance(row, dict) and isinstance(row.get("index"), int):
                out.setdefault(str(r["batch_id"]), {})[row["index"]] = row
    return out


# ---- reading the rows' own lines on the facility ---------------------------------------------------------------------

_ROW = re.compile(r"^HPCB_ROW (\d+) k=(\d+) start=([0-9.]+) nproc=(\S*) aff=(\S*) cpus=(\S*) gpu=(\S*)\s*$", re.M)
_END = re.compile(r"^HPCB_ROW_END (\d+) end=([0-9.]+)\s*$", re.M)


@dataclass
class RowRun:
    batch_dir: str
    index: int
    cores: int
    start: float
    end: float | None
    nproc: str
    aff: str
    cpus: str
    gpu: str

    def aff_set(self) -> set[int]:
        return parse_cpu_list(self.aff)


def parse_cpu_list(text: str) -> set[int]:
    """`0-3,8,10-11` -> {0, 1, 2, 3, 8, 10, 11}; anything unreadable -> empty."""
    out: set[int] = set()
    for part in (text or "").split(","):
        part = part.strip()
        if not part:
            continue
        a, _, b = part.partition("-")
        if not a.isdigit() or (b and not b.isdigit()):
            return set()
        out |= set(range(int(a), int(b or a) + 1))
    return out


def parse_rows(batch_dir: str, out: str) -> list[RowRun]:
    """The probe lines of every attempt's stdout under one batch dir, as `rows_cmd` lists them."""
    runs: list[RowRun] = []
    for block in out.split("HPCB_FILE ")[1:]:
        ends = {int(m.group(1)): float(m.group(2)) for m in _END.finditer(block)}
        for m in _ROW.finditer(block):
            i = int(m.group(1))
            runs.append(RowRun(batch_dir, i, int(m.group(2)), float(m.group(3)), ends.get(i), m.group(4), m.group(5),
                               m.group(6), m.group(7)))
    return runs


def rows_cmd(batch_dir: str) -> str:
    return (f'for f in {batch_dir}/tasks/*/attempt-*/stdout; do [ -f "$f" ] && {{ echo "HPCB_FILE $f"; '
            'grep "^HPCB_ROW" "$f"; }; done; echo HPCB_DONE')


def batch_dirs(t: Trace) -> list[str]:
    """Every accepted batch's own directory (`results_dir` in its answers)."""
    out: list[str] = []
    for r in statuses(t):
        d = r.get("results_dir")
        if d and d not in out:
            out.append(str(d))
    return out


def read_rows(ctx) -> tuple[list[RowRun] | None, str]:
    dirs = batch_dirs(ctx.trace)
    if not dirs:
        return None, "premise not met: no batch directory in the run's answers"
    runs: list[RowRun] = []
    for d in dirs:
        rc, out = ctx.ssh(rows_cmd(d), 60)
        if "HPCB_DONE" not in out:
            return None, f"UNVERIFIABLE: could not read {d} (rc={rc}): {out[-200:]!r}"
        runs += parse_rows(d, out)
    return runs, ""


def peak_overlap(runs: list[RowRun], *, weight=lambda r: 1, until: float | None = None) -> tuple[int, str]:
    """The most rows (or the most of `weight`, e.g. their cores) running at one instant, from each row's own start and
    end (a row with no end line ran until `until`, else its start). At a tie an end counts before a start: rows only
    touching are not counted together."""
    marks: list[tuple[float, int, int, str]] = []
    for r in runs:
        end = r.end if r.end is not None else (until if until is not None else r.start)
        marks += [(r.start, 1, weight(r), f"row {r.index}"), (end, 0, -weight(r), f"row {r.index}")]
    live, peak, at = 0, 0, ""
    for _ts, _order, step, label in sorted(marks, key=lambda m: (m[0], m[1])):
        live += step
        if live > peak:
            peak, at = live, label
    return peak, at


def overlapping_pairs(runs: list[RowRun]) -> list[tuple[RowRun, RowRun]]:
    out = []
    for i, a in enumerate(runs):
        for b in runs[i + 1:]:
            ae = a.end if a.end is not None else a.start
            be = b.end if b.end is not None else b.start
            if a.start < be and b.start < ae:
                out.append((a, b))
    return out


# ---- graders (trace) -------------------------------------------------------------------------------------------------

def needs_sizing_first(t: Trace) -> Result:
    """The first run_batch, with no size and no block, answered `needs_sizing` with the block's facts (CPUs, memory and
    the concurrency each candidate size gives) — before any block was confirmed, and recording nothing."""
    calls = run_batch_answers(t)
    if not calls:
        return Result("needs_sizing_first", False, "no run_batch call")
    i, r = calls[0]
    facts = dict(r.get("facts") or {})
    starts = [k for k in _billed_start_idxs(t) if k < i]
    ok = (r.get("phase") == "needs_sizing" and not r.get("batch_id") and isinstance(facts.get("cpus"), int)
          and facts.get("memory_gb") is not None and bool(facts.get("candidates")) and not starts)
    return Result("needs_sizing_first", ok, f"first run_batch: phase={r.get('phase')!r} batch_id={r.get('batch_id')!r} "
                  f"facts cpus={facts.get('cpus')!r} memory_gb={facts.get('memory_gb')!r} "
                  f"candidates={len(facts.get('candidates') or [])}; billed starts before it: {starts}")


def derived_terms(t: Trace, *, cores: int, memory_gb: float, gpus: int = 0, concurrency: int) -> Result:
    """The spend question for a sized batch with no block states the DERIVED block — N × k + 1 CPUs, at least N × m GB,
    N × g GPUs, one node — and the concurrency; the confirming call's receipt is that same block."""
    name = "derived_terms"
    want_cores, want_gpus = concurrency * cores + 1, concurrency * gpus
    asked = [(i, r) for i, r in run_batch_answers(t) if r.get("phase") == "needs_confirmation" and r.get("block")]
    if not asked:
        return Result(name, False, "no run_batch answered needs_confirmation with a block")
    _i, r = asked[-1]
    b, s = dict(r.get("block") or {}), sizing_of(r)
    bad = []
    if b.get("cores_per_node") != want_cores:
        bad.append(f"cores_per_node {b.get('cores_per_node')!r}, want {want_cores}")
    if not isinstance(b.get("memory_gb"), int | float) or b["memory_gb"] < concurrency * memory_gb:
        bad.append(f"memory_gb {b.get('memory_gb')!r}, want at least {concurrency * memory_gb:g}")
    if (b.get("gpus_per_node") or 0) != want_gpus:
        bad.append(f"gpus_per_node {b.get('gpus_per_node')!r}, want {want_gpus}")
    if b.get("nodes") not in (1, None):
        bad.append(f"nodes {b.get('nodes')!r}")
    if s.get("concurrency") != concurrency or s.get("derived") is not True:
        bad.append(f"sizing concurrency={s.get('concurrency')!r} derived={s.get('derived')!r}")
    receipts = [c.result or {} for k in _billed_start_idxs(t) for c in [t.calls[k]] if (c.result or {}).get("block")]
    if not receipts:
        bad.append("no confirming call carried a receipt")
    elif (receipts[-1].get("block") or {}).get("cores_per_node") != want_cores:
        bad.append(f"the receipt's cores_per_node {(receipts[-1].get('block') or {}).get('cores_per_node')!r}")
    return Result(name, not bad, "ok: " + f"{want_cores} CPUs, {b.get('memory_gb')} GB, {want_gpus} GPU(s), "
                  f"{concurrency} at once" if not bad else "; ".join(bad))


def sizing_announced(t: Trace, **want: Any) -> Result:
    """Every accepted batch's answer states its sizing, with these values (e.g. slots=2, bound_by="cpus")."""
    got = [sizing_of(r) for _i, r in accepted(t)]
    if not got:
        return Result("sizing_announced", False, "no accepted run_batch")
    bad = [f"{k}={s.get(k)!r} (want {v!r})" for s in got for k, v in want.items() if s.get(k) != v]
    return Result("sizing_announced", not bad, "ok: " + json.dumps(got[0], default=str)[:200] if not bad
                  else "; ".join(bad))


def batch_done(t: Trace, *, rows: int) -> Result:
    """The sized batch ran every row to done."""
    last = {}
    for r in statuses(t):
        if r.get("batch_id") and r.get("phase") not in ("refused", "needs_sizing"):
            last = r
    c = dict(last.get("counts") or {})
    ok = last.get("phase") == "done" and c.get("done") == rows
    return Result("batch_done", ok, f"last answer phase={last.get('phase')!r} counts={c}")


def memory_killed_with_why(t: Trace, *, index: int) -> Result:
    """The row over its memory was killed BY THE WRAPPER at its limit: failed, why=memory, the memory rc."""
    rows = {i: r for b in rows_reported(t).values() for i, r in b.items()}
    r = rows.get(index)
    if r is None:
        return Result("memory_killed_with_why", False, f"row {index} never reported")
    ok = r.get("state") == "failed" and r.get("why") == "memory" and r.get("exit_code") == MEMORY_RC
    return Result("memory_killed_with_why", ok, f"row {index}: state={r.get('state')!r} why={r.get('why')!r} "
                  f"rc={r.get('exit_code')!r} peak={r.get('peak_rss_mb')!r} MB")


def neighbour_survived(t: Trace, *, index: int, word: str) -> Result:
    """The neighbour row on the same block finished cleanly with its whole output."""
    rows = {i: r for b in rows_reported(t).values() for i, r in b.items()}
    r = rows.get(index)
    if r is None:
        return Result("neighbour_survived", False, f"row {index} never reported")
    ok = r.get("state") == "done" and r.get("exit_code") == 0 and word in str(r.get("tail") or "")
    return Result("neighbour_survived", ok, f"row {index}: state={r.get('state')!r} rc={r.get('exit_code')!r} "
                  f"tail={str(r.get('tail') or '')[-80:]!r}")


def srun_ran(t: Trace, *, rows: int) -> Result:
    """Each row's own `srun` step ran (the Anvil live test, F3): every row done, rc 0, the step's own line and srun's rc
    0 in its tail (on the base the step died: `cpus-per-task set by two different environment variables`)."""
    got = {i: r for b in rows_reported(t).values() for i, r in b.items()}
    bad = []
    for i in range(rows):
        r = got.get(i)
        tail = str((r or {}).get("tail") or "")
        if r is None or r.get("state") != "done" or r.get("exit_code") != 0 or "HPCB_STEP_OK" not in tail \
                or "HPCB_SRUN_RC=0" not in tail:
            bad.append(f"row {i}: " + ("never reported" if r is None else
                                       f"state={r.get('state')!r} rc={r.get('exit_code')!r} tail={tail[-120:]!r} "
                                       f"stderr={str(r.get('stderr_tail') or '')[-160:]!r}"))
    return Result("srun_ran_in_each_row", not bad, "; ".join(bad) or f"all {rows} rows ran their step")


def srun_steps_said(t: Trace, *, rows: int) -> Result:
    """Each row's result says it ran srun steps (`srun_steps`), and an answer says where such a step runs: on CPUs Slurm
    picks within the block, not the row's slot (the documented limit of a row's own srun)."""
    got = {i: r for b in rows_reported(t).values() for i, r in b.items()}
    flagged = [i for i in range(rows) if (got.get(i) or {}).get("srun_steps") is True]
    said = any("srun steps" in str(r.get("notice") or "") and "not the row's slot" in str(r.get("notice") or "")
               for r in statuses(t))
    ok = len(flagged) == rows and said
    return Result("srun_steps_said", ok, f"rows saying srun_steps: {flagged} of {rows}; an answer said where a step "
                                         f"runs: {said}")


def pilot_proposed(t: Trace, *, cores: int, memory_gb: tuple[float, float]) -> Result:
    """A pilot ended at needs_sizing with a proposal: cores = ⌈CPU-s ÷ wall⌉, memory = 1.25 × peak RSS (in range)."""
    props = [dict(r.get("proposal") or {}) for r in statuses(t) if r.get("phase") == "needs_sizing" and r.get("proposal")]
    if not props:
        return Result("pilot_proposed", False, "no needs_sizing answer carried a proposal")
    p = props[-1]
    lo, hi = memory_gb
    ok = p.get("cores") == cores and isinstance(p.get("memory_gb"), int | float) and lo <= p["memory_gb"] <= hi
    return Result("pilot_proposed", ok, f"proposal cores={p.get('cores')!r} memory_gb={p.get('memory_gb')!r} "
                  f"concurrency={p.get('concurrency')!r}; want cores {cores}, memory in [{lo}, {hi}]")


# ---- world checks (ctx: after the script, before the teardown) -------------------------------------------------------

async def rows_overlap(ctx, *, at_least: int) -> Result:
    """world: rows really ran together — at least `at_least` at one instant, from each row's own start and end (the
    LOWER bound: a window that admits N but runs them one after another fails here) — and never more than the batch
    announced."""
    name = "world:rows_overlap"
    runs, why = read_rows(ctx)
    if runs is None:
        return Result(name, False, why)
    peak, at = peak_overlap(runs)
    slots = slots_announced(ctx.trace)
    ok = bool(runs) and peak >= at_least and (slots is None or peak <= slots)
    return Result(name, ok, f"{len(runs)} row(s); at most {peak} ran at once (at {at or '-'}); want at least "
                  f"{at_least}" + (f", at most the {slots} announced" if slots is not None else ""))


async def cpus_never_oversubscribed(ctx, *, block_cpus: int) -> Result:
    """world: the cores of the rows running at any instant never sum past the block's CPUs, and rows that ran together
    had disjoint CPU masks (each pinned to its own slot)."""
    name = "world:cpus_never_oversubscribed"
    runs, why = read_rows(ctx)
    if runs is None:
        return Result(name, False, why)
    peak, at = peak_overlap(runs, weight=lambda r: r.cores)
    shared = [f"rows {a.index}/{b.index} ({a.aff} & {b.aff})" for a, b in overlapping_pairs(runs)
              if not a.aff_set() or not b.aff_set() or a.aff_set() & b.aff_set()]
    ok = bool(runs) and peak <= block_cpus and not shared
    return Result(name, ok, f"at most {peak} CPU(s) of rows at once (at {at or '-'}) on a {block_cpus}-CPU block; "
                  f"overlapping rows sharing a CPU: {', '.join(shared[:6]) or 'none'}")


async def nproc_matches_cores(ctx) -> Result:
    """world: every row saw exactly its cores — `nproc` in its environment and the kernel's CPU mask."""
    name = "world:nproc_matches_cores"
    runs, why = read_rows(ctx)
    if runs is None:
        return Result(name, False, why)
    bad = [f"row {r.index} (k={r.cores}): nproc={r.nproc} mask={r.aff}" for r in runs
           if r.nproc != str(r.cores) or len(r.aff_set()) != r.cores]
    return Result(name, bool(runs) and not bad, f"{len(runs)} row(s); not their cores: {'; '.join(bad[:6]) or 'none'}")


async def gpus_one_per_row(ctx, *, gpus: tuple[str, ...]) -> Result:
    """world: each row saw ONE GPU of the block's, and rows that ran together saw different ones."""
    name = "world:gpus_one_per_row"
    runs, why = read_rows(ctx)
    if runs is None:
        return Result(name, False, why)
    bad = [f"row {r.index}: gpu={r.gpu}" for r in runs if r.gpu not in gpus]
    clash = [f"rows {a.index}/{b.index} both on {a.gpu}" for a, b in overlapping_pairs(runs) if a.gpu == b.gpu]
    ok = bool(runs) and not bad and not clash
    return Result(name, ok, f"{len(runs)} row(s); wrong GPU: {', '.join(bad[:6]) or 'none'}; "
                  f"overlapping rows on one GPU: {', '.join(clash[:6]) or 'none'}")


async def neighbour_ran_beside_the_kill(ctx, *, killed: int, neighbour: int) -> Result:
    """world: the neighbour row was running when the other was killed (it started before and ended after the kill),
    so surviving it means something."""
    name = "world:neighbour_ran_beside_the_kill"
    runs, why = read_rows(ctx)
    if runs is None:
        return Result(name, False, why)
    by = {r.index: r for r in runs}
    k, n = by.get(killed), by.get(neighbour)
    if k is None or n is None:
        return Result(name, False, f"rows seen: {sorted(by)}")
    # the two started together (one admission), the killed one printed no end line, the neighbour ran past the kill
    ok = k.end is None and n.end is not None and abs(n.start - k.start) < 10 and n.end > k.start + 2
    return Result(name, ok, f"killed row {killed}: start {k.start:.1f}, end line {'absent' if k.end is None else k.end}; "
                  f"neighbour {neighbour}: {n.start:.1f}–{n.end}")


SACCT_FIELDS = "JobID,JobName%60,Partition,AllocCPUS,ReqMem,NNodes,State,SubmitLine%600"


def sacct_cmd() -> str:
    """This pool user's jobs of the last day, one parsable row each (the pilot's SubmitLine carries `uep.<eid>`)."""
    return f'sacct -u "$(whoami)" -X -n -P -S now-1days -o {SACCT_FIELDS} 2>&1; echo "HPCB_RC=$?"'


def mem_mb(req: str) -> float | None:
    m = re.match(r"^\s*(\d+(?:\.\d+)?)([KMGT]?)", req or "", re.I)
    if not m:
        return None
    return float(m.group(1)) * {"": 1, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}[m.group(2).upper()]


def block_allocation(out: str, eids: list[str]) -> dict[str, Any] | None:
    """The pilot of this run that ran (a parsl job whose SubmitLine names one of its endpoints, `uep.<eid>`): its
    AllocCPUS, ReqMem (MB), NNodes, partition and state."""
    names = re.sub(r"%[0-9]+", "", SACCT_FIELDS).split(",")
    rows = []
    for line in out.splitlines():
        cols = line.split("|")
        if len(cols) < len(names):
            continue
        row = dict(zip(names, cols[: len(names) - 1] + ["|".join(cols[len(names) - 1:])], strict=True))
        if "parsl" in row["JobName"] and any(f"uep.{e}" in row["SubmitLine"] for e in eids):
            rows.append(row)
    ran = [r for r in rows if r["State"].split()[0] in ("RUNNING", "COMPLETED", "CANCELLED", "TIMEOUT")]
    if not (ran or rows):
        return None
    r = (ran or rows)[-1]
    return {"job": r["JobID"], "cpus": r["AllocCPUS"], "mem_mb": mem_mb(r["ReqMem"]), "nodes": r["NNodes"],
            "partition": r["Partition"], "state": r["State"]}


async def derived_block_allocated(ctx, *, cores: int, memory_gb: float, concurrency: int) -> Result:
    """world: the scheduler gave the block the terms named — AllocCPUS N × k + 1, ReqMem at least N × m — on one
    node (sacct, the pilot found by this run's `uep.<eid>` marker)."""
    name = "world:derived_block_allocated"
    eids = bc.endpoint_ids(ctx.trace)
    if not eids:
        return Result(name, False, "premise not met: no endpoint id in the run's results")
    rc, out = ctx.ssh(sacct_cmd(), 60)
    if rc != 0 or "HPCB_RC=0" not in out:
        return Result(name, False, f"UNVERIFIABLE: sacct failed (rc={rc}): {out[-300:]!r}")
    a = block_allocation(out, eids)
    if a is None:
        return Result(name, False, f"no pilot of uep.{eids[-1]} in sacct")
    want = concurrency * cores + 1
    ok = a["cpus"] == str(want) and (a["mem_mb"] or 0) >= concurrency * memory_gb * 1024 and a["nodes"] == "1"
    return Result(name, ok, f"job {a['job']} on {a['partition']} ({a['state']}): AllocCPUS={a['cpus']} "
                  f"ReqMem={a['mem_mb']} MB NNodes={a['nodes']}; want {want} CPUs, ≥ {concurrency * memory_gb:g} GB")
