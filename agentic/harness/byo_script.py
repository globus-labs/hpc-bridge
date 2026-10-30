"""Steps a scripted bring-your-own scenario shares (the Perlmutter report's fake cells, 2026-10-29): a connect from the
probe's proposal, polling a bring-up to its end, and the jail's own leftover local state (~/.hpc-bridge's
facilities.json and endpoints.json, which the script reads and edits as the user would by hand).

Pure but for the jail's state dir; scenarios import it (scenario_knobs.py imports scenarios on the host)."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

PROPOSED = "proposed_facility_details"
UP = "needs_account"   # an SSH facility's login shape is up: the connect lists (or asks for) the allocations


def brief(r: dict, n: int = 240) -> str:
    return str(r.get("notice") or r.get("text") or "")[:n]


def proposal_details(r: dict, **over: Any) -> dict:
    """The probe's proposal as the confirming details=: env_setup and partitions left out (the server keeps the proposed
    ones — the tool's own instruction), unset fields dropped; `over` replaces or adds fields (a None drops one)."""
    d = {k: v for k, v in dict(r.get("proposed_details") or {}).items()
         if v is not None and k not in ("env_setup", "partitions")}
    d.update(over)
    return {k: v for k, v in d.items() if v is not None}


async def until_settled(ops, r: dict, again: dict, *, wait_s: float, poll_s: float = 5.0) -> dict:
    """Poll a bring-up — `connect_facility(**again)` while the answer is `provisioning` — until it settles (any other
    phase) or `wait_s` passes. Returns the last answer (still `provisioning` on a timeout)."""
    deadline = time.monotonic() + wait_s
    while r.get("phase") == "provisioning" and time.monotonic() < deadline:
        await ops.sleep(poll_s)
        r = await ops.call("connect_facility", **again)
    return r


def state_dir() -> Path:
    return Path(os.environ.get("HPC_BRIDGE_STATE_DIR") or (Path.home() / ".hpc-bridge"))


def _load(name: str) -> dict:
    try:
        data = json.loads((state_dir() / name).read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def login_pin(alias: str) -> str | None:
    """The login node the plugin pinned an endpoint reached through `alias` to (endpoints.json), or None."""
    for row in _load("endpoints.json").values():
        if isinstance(row, dict) and row.get("alias") == alias and row.get("login_host"):
            return str(row["login_host"])
    return None


def cached_hosts() -> list[str]:
    """The ssh hosts facilities.json holds a proven config for."""
    return sorted(_load("facilities.json"))


def forget_cached(host: str) -> bool:
    """The user's hand edit: remove `host`'s record from facilities.json. True when it was there and is gone."""
    data = _load("facilities.json")
    if data.pop(host, None) is None:
        return False
    p = state_dir() / "facilities.json"
    p.write_text(json.dumps(data, indent=2, sort_keys=True))
    return host not in _load("facilities.json")
