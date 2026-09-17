"""What the ACP client's event log (`AcpCapture.events`, persisted as `acp-updates.jsonl`) can vouch for.

The stream is NOT a grading source (adapters drop raw I/O — see acp_client.AcpCapture), but it is the client's own
ordered record of every tool call the operator made, which makes it an INSTRUMENT CHECK on the graded trace: the
graded Trace comes from the agent's post-run store (hermes state.db / the Claude CLI transcript), and that store
can lag a flush or miss a session — exactly the failure that produced a `compute_ran` false-fail when a mid-run
state.db read ended a turn early (2026-09-07). Counting hpc-bridge calls on both sides catches it.
"""
from __future__ import annotations

from hermes_trace import _unwrap
from invariants import HPC_BRIDGE_TOOL_NAMES, Result, Trace, logical_name


def tool_calls_from_events(events: list[dict] | None) -> list[tuple[str, dict]]:
    """(logical tool name, args) for every `tool_call` event, in order, with hermes' `tool_call` dispatcher form
    unwrapped (the real name + args are nested in raw_input). Args are `{}` when the adapter sent no raw_input."""
    out: list[tuple[str, dict]] = []
    for e in events or []:
        if e.get("event") != "tool_call":
            continue
        raw_input = e.get("raw_input")
        name, args = _unwrap(str(e.get("title") or ""), raw_input if isinstance(raw_input, dict) else {})
        out.append((logical_name(name), args if isinstance(args, dict) else {}))
    return out


def hpc_bridge_calls_from_events(events: list[dict] | None) -> list[str]:
    return [n for n, _ in tool_calls_from_events(events) if n in HPC_BRIDGE_TOOL_NAMES]


def capture_crosscheck(events: list[dict] | None, trace: Trace) -> Result:
    """Do the graded trace and the ACP stream agree on the hpc-bridge calls the operator made? Compares the ordered
    sequence of hpc-bridge tool NAMES (the one thing every adapter carries). A mismatch means the trace source
    lagged, truncated or picked the wrong session — the grading is then suspect, whatever the verdict says."""
    if not events:
        return Result("harness:acp_capture", True, "no ACP event log (not an ACP run)")
    seen = hpc_bridge_calls_from_events(events)
    graded = [c.name for c in trace.calls if c.name in HPC_BRIDGE_TOOL_NAMES]
    if seen == graded:
        return Result("harness:acp_capture", True, f"the ACP stream and the graded trace agree: {len(graded)} hpc-bridge call(s)")
    # first divergence, for the reader
    k = next((i for i, (a, b) in enumerate(zip(seen, graded, strict=False)) if a != b), min(len(seen), len(graded)))
    return Result("harness:acp_capture", False,
                  f"MISMATCH: the ACP stream saw {len(seen)} hpc-bridge call(s), the graded trace has {len(graded)}; "
                  f"they diverge at #{k} (stream {seen[k:k + 3]} vs trace {graded[k:k + 3]}) — the trace source may have "
                  f"lagged/truncated or picked the wrong session; treat this run's grading as suspect")
