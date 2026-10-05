from __future__ import annotations

import time

from .context import AppCtx, ShapeRuntime
from .models import ShellOutcome


def estimate_spend(elapsed_s: float, nodes: int, charge_factor: float) -> float:
    """Estimated node-hours for a warm block held `elapsed_s` seconds.

    `charge_factor` is the facility's QOS multiplier (0.0 for local dev = free).
    """
    return (elapsed_s / 3600.0) * nodes * charge_factor


def cap_output(text: str, max_chars: int) -> str:
    """Bound a stdout/stderr snippet (Globus has a hard 10 MB result limit)."""
    if len(text) <= max_chars:
        return text
    dropped = len(text) - max_chars
    return text[:max_chars] + (
        f"\n…[truncated {dropped} chars; redirect verbose output to a file"
        " and read it back in bounded chunks]"
    )


# --- the session spend clock (split step 4, 2026-09-03: it belonged with estimate_spend) ---

def _block_nodes(rt: ShapeRuntime, app: AppCtx) -> int:
    """How many nodes this shape's block holds: its own config (what the scheduler is asked for), not the
    never-set `Profile` default — a 4-node block billed as 1 under-reported spend 4x (review 2026-09-05 #6b)."""
    try:
        return max(1, int(rt.user_endpoint_config.get("nodes_per_block") or app.profile.nodes_per_block or 1))
    except (TypeError, ValueError):
        return max(1, int(app.profile.nodes_per_block or 1))

def _bank_warm_interval(rt: ShapeRuntime, app: AppCtx) -> None:
    """Fold the elapsed warm interval into accrued spend and stop the clock."""
    if rt.warm_since is not None:
        rt.spend_accrued += estimate_spend(
            time.monotonic() - rt.warm_since, _block_nodes(rt, app), app.charge_factor
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
