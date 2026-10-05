#!/usr/bin/env python3
"""Print a scenario's HOST-side knobs as shell assignments (eval'd by run_smoke.sh before `docker run`):

    HPCB_KNOB_NO_GLOBUS_DB=1                       # NO_GLOBUS_DB = True  -> mount no storage.db (logged-out run)
    HPCB_KNOB_GLOBUS_DB_SECRET=HPCB_TEST_GLOBUS_DB_NOACCOUNT   # GLOBUS_DB_SECRET -> mount THAT env var's path instead
    HPCB_KNOB_SERIAL=1                             # SERIAL = True -> shares a facility-side identity; run one at a time
    HPCB_KNOB_COOLDOWN_S=660                       # COOLDOWN_S -> run_suite waits this long after each cell (fail2ban findtime)
    HPCB_KNOB_NEEDS_NODE=<n>                       # nodes the cell occupies: NEEDS_COMPUTE_NODE (True=1, an int, False=0),
                                                   #   else DERIVED: 1 when `compute_ran` is among EXTRA_INVARIANTS
    HPCB_KNOB_WARM_BLOCK_USER=glabs-gc                # WARM_BLOCK_USER -> a running block of that user satisfies the need
                                                   #   (a facility MEP's warm block is what the cell reuses)

Everything else a scenario declares (EXTRA_ENV, SEED_FACILITY_CACHE, …) is applied INSIDE the container
by run.py. Scenario modules import only `invariants` (pure), so this is importable on the host."""
from __future__ import annotations

import importlib
import shlex
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    here = Path(__file__).resolve().parent
    sys.path.insert(0, str(here))
    sys.path.insert(0, str(here.parent / "scenarios"))
    raw = argv[0] if argv else ""
    if not raw:
        print("usage: scenario_knobs.py <scenario>", file=sys.stderr)
        return 2
    # the SAME normalisation run.py applies (a path, a trailing .py or '.', dashes) — a mismatch used to
    # drop every knob silently, and run_smoke then mounted the WRONG Globus store (review 2)
    name = Path(raw).stem if ("/" in raw or raw.endswith(".py")) else raw
    name = name.rstrip(".").replace("-", "_")
    try:
        mod = importlib.import_module(name)
    except ModuleNotFoundError:
        print(f"scenario_knobs: unknown scenario {raw!r}", file=sys.stderr)
        return 2
    if getattr(mod, "NO_GLOBUS_DB", False):
        print("HPCB_KNOB_NO_GLOBUS_DB=1")
    secret = getattr(mod, "GLOBUS_DB_SECRET", None)
    if secret:
        print(f"HPCB_KNOB_GLOBUS_DB_SECRET={shlex.quote(str(secret))}")
    if getattr(mod, "SERIAL", False):
        print("HPCB_KNOB_SERIAL=1")
    cooldown = int(getattr(mod, "COOLDOWN_S", 0) or 0)
    if cooldown > 0:
        print(f"HPCB_KNOB_COOLDOWN_S={cooldown}")
    n = needs_nodes(mod)
    if n > 0:
        print(f"HPCB_KNOB_NEEDS_NODE={n}")
    warm = getattr(mod, "WARM_BLOCK_USER", None)
    if warm:
        print(f"HPCB_KNOB_WARM_BLOCK_USER={shlex.quote(str(warm))}")
    if getattr(mod, "TRUST_HOST_KEY", True) is False:
        print("HPCB_KNOB_COLD_HOST_KEY=1")  # informational: this cell starts with an EMPTY known_hosts
    tg = getattr(mod, "TARGETS", None)
    if tg:
        print(f"HPCB_KNOB_TARGETS={shlex.quote(','.join(tg))}")  # the scenario runs ONLY on these targets (chaos: fake only)
    req = getattr(mod, "REQUIRES", None)
    if req:
        import json
        print(f"HPCB_KNOB_REQUIRES={shlex.quote(json.dumps(req, sort_keys=True))}")  # cluster capabilities the scenario needs
    # Cluster-ADMIN world changes (run by run_smoke.sh through the target's admin channel, `{user}` = the pool user):
    # ADMIN_SETUP before the agent starts, ADMIN_CLEANUP always afterwards. No admin channel on the target ⇒ skipped.
    for attr, knob in (("ADMIN_SETUP", "HPCB_KNOB_ADMIN_SETUP"), ("ADMIN_CLEANUP", "HPCB_KNOB_ADMIN_CLEANUP")):
        cmds = list(getattr(mod, attr, []) or [])
        if cmds:
            import json
            print(f"{knob}={shlex.quote(json.dumps(cmds))}")
    return 0


def needs_nodes(mod) -> int:
    """How many idle compute nodes a cell of this scenario needs at launch. Explicit `NEEDS_COMPUTE_NODE`
    wins (True -> 1, an int -> that many, False -> 0 even if the scenario grades compute); otherwise DERIVED:
    a scenario that gates on `compute_ran` brings up a block. Review 2026-09-05: six of eight block-bringing
    scenarios had never declared the knob and ran ungated (the 09-03 node starvation)."""
    explicit = getattr(mod, "NEEDS_COMPUTE_NODE", None)
    if explicit is not None:
        if explicit is True:
            return 1
        if explicit is False:
            return 0
        return max(0, int(explicit))
    names = {getattr(f, "__name__", "") for f in getattr(mod, "EXTRA_INVARIANTS", [])}
    return 1 if "compute_ran" in names else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
