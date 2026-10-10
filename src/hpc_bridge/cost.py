from __future__ import annotations

import time

from .context import AppCtx, ShapeRuntime
from .models import ShellOutcome


def estimate_spend(elapsed_s: float, nodes: int, charge_factor: float) -> float:
    """Estimated node-hours for a warm block held `elapsed_s` seconds.

    `charge_factor` is the facility's QOS multiplier (0.0 for local dev = free).
    """
    return (elapsed_s / 3600.0) * nodes * charge_factor


def _line_count(text: str) -> int:
    """Lines as the SDK's `readlines()` counts them: one per newline, plus an unterminated last line."""
    return text.count("\n") + (1 if text and not text.endswith("\n") else 0)


def _n(count: int, unit: str) -> str:
    """`1 line`, `2,001 lines`."""
    return f"{count:,} {unit}" + ("" if count == 1 else "s")


def cut_output(text: str, max_chars: int, *, stream: str = "output", sdk_lines: int | None = None) -> tuple[str, bool]:
    """Bound one output stream for the agent: (the text to hand back, whether any of it was dropped).

    Keeps the END: errors, tracebacks and summaries land there, and the SDK's own silent cut (`sdk_lines`, its
    `snippet_lines`: past it only the last lines come back) keeps the end too, so the two compose into the true tail.
    The kept part starts at a whole line when that still keeps half the cap; otherwise (a long line just before a
    short last one: a one-line JSON result, then a trailer) it starts mid-line and says so. A cut is never silent: the
    text opens with a marker line naming the stream and what was dropped — counted "at least" when the received text
    is `sdk_lines` long, because the SDK may already have dropped lines it does not report."""
    sdk_cut = sdk_lines is not None and _line_count(text) >= sdk_lines
    if len(text) <= max_chars and not sdk_cut:
        return text, False
    start = max(len(text) - max_chars, 0)
    if start and text[start - 1] != "\n":  # mid-line: begin at the next whole line, if that keeps half the cap
        nl = text.find("\n", start)
        if nl >= 0 and len(text) - (nl + 1) >= max_chars // 2:
            start = nl + 1
    kept = text[start:]
    n_kept, n_dropped = _line_count(kept), text.count("\n", 0, start)  # `start` chars precede the kept part
    them = "it" if n_kept == 1 else "them"
    least = "at least " if sdk_cut else ""
    if start and text[start - 1] != "\n":  # the first kept line is shown from its middle
        first = "starting mid-line" if n_kept == 1 else "the first starting mid-line"
        shown = f"its last {_n(len(kept), 'char')} ({_n(n_kept, 'line')}, {first})"
        was = "was" if start == 1 else "were"
        gone = f"{least}{_n(start, 'char')} before {them} {was} dropped"
    elif start:
        shown = f"its last {_n(n_kept, 'line')} ({_n(len(kept), 'char')})"
        was = "was" if n_dropped == 1 else "were"
        gone = f"{least}{_n(n_dropped, 'line')} ({least}{_n(start, 'char')}) before {them} {was} dropped"
    else:  # only the SDK's line limit cut it: how much it dropped is unknown
        shown = f"its last {_n(n_kept, 'line')} ({_n(len(kept), 'char')})"
        gone = (f"the worker returns at most {sdk_lines:,} lines, so earlier ones may have been dropped "
                "(how many is unknown)")
    return f"[hpc-bridge: {stream} too long — showing only {shown}; {gone}]\n{kept}", True


def cut_streams(
    stdout: str, stderr: str, max_chars: int, *, sdk_lines: int | None = None
) -> tuple[str, str, str | None]:
    """Bound both streams of a result: (stdout, stderr, the notice when either was cut, else None). The marker in a
    cut stream says how much was dropped; the notice says how to read the whole output."""
    out, out_cut = cut_output(stdout, max_chars, stream="stdout", sdk_lines=sdk_lines)
    err, err_cut = cut_output(stderr, max_chars, stream="stderr", sdk_lines=sdk_lines)
    cut = [name for name, was_cut in (("stdout", out_cut), ("stderr", err_cut)) if was_cut]
    if not cut:
        return out, err, None
    return out, err, (
        f"{' and '.join(cut)} too long for one result: only the end is kept (errors and summaries land there), "
        "opening with a marker that says what was dropped. To read all of it, redirect it to a file on the "
        "facility (`cmd > out.log 2>&1`; the file stays for later calls) and read it in ranges: `wc -l out.log`, "
        "`head -n 100 out.log`, `sed -n '101,200p' out.log`, `tail -n 100 out.log`."
    )


# --- the session spend clock (split step 4, 2026-09-03: it belonged with estimate_spend) ---

def _block_nodes(rt: ShapeRuntime, app: AppCtx) -> int:
    """How many nodes this shape's block holds: its own config (what the scheduler is asked for), not the
    never-set `Profile` default — a 4-node block billed as 1 under-reported spend 4x (review 2026-09-05 #6b)."""
    try:
        return max(1, int(rt.user_endpoint_config.get("nodes_per_block") or app.profile.nodes_per_block or 1))
    except (TypeError, ValueError):
        return max(1, int(app.profile.nodes_per_block or 1))

def _bank_warm_interval(rt: ShapeRuntime, app: AppCtx, *, until: float | None = None) -> None:
    """Fold the elapsed warm interval into accrued spend and stop the clock. `until` (monotonic) ends the
    interval earlier than now — when the block is known to have been released at an estimated time (its idle
    window or walltime ran out while nobody was calling), so the gap after it is not billed."""
    if rt.warm_since is not None:
        end = time.monotonic() if until is None else min(time.monotonic(), until)
        rt.spend_accrued += estimate_spend(
            max(0.0, end - rt.warm_since), _block_nodes(rt, app), app.charge_factor
        )
        rt.warm_since = None

def _billable(rt: ShapeRuntime) -> bool:
    """LocalProvider (login-node) shapes consume no allocation, so they don't bill."""
    return rt.user_endpoint_config.get("provider_type") != "LocalProvider"

def _settle_billing(rt: ShapeRuntime, app: AppCtx, block: str) -> None:
    """Drive the session-spend clock from TRUE worker presence (the canary), not manager
    liveness. Banking on warm->not-warm makes spend survive an idle block release without
    over-counting the idle gap (the clock stays stopped while cold) — closes the over-report
    without the symmetric under-report of simply resetting. Login (LocalProvider) shapes are
    not billable, so their clock never starts and nothing accrues."""
    if block == "warm" and _billable(rt):
        if rt.warm_since is None:
            rt.warm_since = time.monotonic()
    else:
        _bank_warm_interval(rt, app)

def _session_spend(rt: ShapeRuntime, app: AppCtx) -> float:
    spent = rt.spend_accrued
    if rt.warm_since is not None:
        spent += estimate_spend(
            time.monotonic() - rt.warm_since, _block_nodes(rt, app), app.charge_factor
        )
    return spent

def _total_session_spend(app: AppCtx) -> float:
    """Total spend across every shape, plus the blocks already released on this binding — the cost the agent sees
    on outcomes/status."""
    return sum(_session_spend(rt, app) for rt in app.shapes.values()) + getattr(app, "released_spend", 0.0)

def _with_spend(app: AppCtx, out: ShellOutcome) -> ShellOutcome:
    out.session_spend = _total_session_spend(app)
    return out
