#!/usr/bin/env python3
"""Sweep the agentic harness' Globus Compute residue: orphan endpoint managers left in the FAKE cluster's login
containers, and stale endpoint RECORDS under the (shared) Globus identity.

Why: a run's teardown deregisters its record from inside the jail, but when the jail can't build a Globus Compute
client ("records left for the sweep") the `hpc-bridge-fake-<runid>` record stays; and a manager that outlived its
run keeps heart-beating from the fake login container, so the web service reports the record "online" for days
(found 2026-09-08: 8 online fake records, 24 offline ones, and 19 duplicate `hpc-bridge-dev` registrations).

Scope — deliberately narrow, the identity is the maintainer's own:
  * records named `hpc-bridge-fake-*` (fake-cluster runs) and `hpc-bridge-dev` (pre-#27 dev registrations)
  * NEVER the real facilities (KEEP below) and never a name this script doesn't recognise
  * managers/endpoint dirs only inside the fake cluster's login containers (disposable by design)

Dry-run by default: prints exactly what it would do. `--apply` executes. Run from the repo root:
    uv run --extra integration python agentic/sweep_endpoints.py            # plan
    uv run --extra integration python agentic/sweep_endpoints.py --apply    # do it
"""
from __future__ import annotations

import argparse
import re
import shlex
import subprocess
import sys

KEEP = {"globus-cluster-mep", "hpc-bridge-anvil", "hpc-bridge-aurora", "hpc-bridge-globus1", "hpc-bridge-midway3"}
FAKE_LOGIN_CONTAINERS = ("hpcb-fake-login-1", "hpcb-fake-login02-1")


def _harness_record(name: str) -> bool:
    return name.startswith("hpc-bridge-fake-") or name == "hpc-bridge-dev"


def _docker(container: str, script: str) -> tuple[int, str]:
    p = subprocess.run(["docker", "exec", container, "sh", "-c", script], capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr).strip()


def _manager_name(args: str) -> str | None:
    """The endpoint name from a manager's process title: `Globus Compute Endpoint *(<uuid>, <name>) [...]`."""
    m = re.search(r"Globus Compute Endpoint \*?\(\s*[0-9a-f-]{36}\s*,\s*([^)]+?)\s*\)", args)
    return m.group(1) if m else None


def sweep_fake_managers(apply: bool) -> None:
    """Kill orphaned HARNESS managers in the fake login containers — by PID, and only those whose endpoint name is a
    harness record. The `mep` profile runs two root facility MEPs (`hpcb-mep-strict`, `hpcb-mep-open`) in the same
    container for its whole life; a blanket `pkill -f 'Globus Compute Endpoint'` killed them too (2026-09-22)."""
    print("== fake login containers: orphan endpoint managers ==")
    for c in FAKE_LOGIN_CONTAINERS:
        rc, ps = _docker(c, "ps -eo pid=,user=,etime=,args= | grep 'Globus Compute Endpoint' | grep -v grep")
        if rc not in (0, 1):
            print(f"  {c}: not reachable ({ps[:80]}) — skipped")
            continue
        harness, kept = [], []
        for line in (ln for ln in ps.splitlines() if ln.strip()):
            pid, _user, _etime, args = line.split(None, 3)
            name = _manager_name(args)
            (harness if name and _harness_record(name) else kept).append((pid, name, line))
        print(f"  {c}: {len(harness)} harness manager(s), {len(kept)} kept")
        for _pid, name, line in kept:
            print(f"    keep    {name or '?':40} {line[:90]}")
        for _pid, name, line in harness:
            print(f"    {'kill' if apply else 'would kill'} {name:40} {line[:90]}")
        if apply and harness:
            pids = " ".join(pid for pid, _n, _l in harness)
            names = " ".join(f"/home/*/.globus_compute/{shlex.quote(n)}" for _p, n, _l in harness)
            _docker(c, f"kill {pids} 2>/dev/null; sleep 3; kill -9 {pids} 2>/dev/null; true")
            # only the harness endpoints' own dirs and the POOL users' user-endpoint dirs — never the MEP accounts'
            rc, out = _docker(c, f"rm -rf {names} /home/hpcbridge-test*/.globus_compute/uep.*; "
                                 f"ls -d {names} 2>/dev/null | wc -l")
            print(f"    -> killed {len(harness)}; {out.strip()} endpoint dir(s) left")


def sweep_records(apply: bool) -> None:
    import globus_compute_sdk as g  # the `integration` extra

    print("== Globus Compute endpoint records under this identity ==")
    client = g.Client()
    eps = list(client.get_endpoints())
    todo, kept = [], []
    for d in eps:
        name, uuid = str(d.get("name") or "?"), d["uuid"]
        if name in KEEP or not _harness_record(name):
            kept.append((name, uuid))
        else:
            todo.append((name, uuid))
    print(f"  {len(eps)} records: {len(todo)} harness records to delete, {len(kept)} kept")
    for name, uuid in sorted(kept):
        print(f"    keep    {name:50} {uuid}")
    deleted, failed = 0, []
    for name, uuid in sorted(todo):
        if not apply:
            print(f"    would delete {name:45} {uuid}")
            continue
        try:
            client.delete_endpoint(uuid)
            deleted += 1
        except Exception as exc:  # noqa: BLE001 - report, keep sweeping
            msg = str(exc)
            if "404" in msg or "not found" in msg.lower():
                deleted += 1   # already gone
            else:
                failed.append((name, uuid, f"{type(exc).__name__}: {msg[:80]}"))
    if apply:
        print(f"  deleted {deleted}; failed {len(failed)}")
        for f in failed:
            print("    FAILED", *f)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true", help="execute (default: dry-run plan)")
    ap.add_argument("--records-only", action="store_true", help="skip the fake-container manager sweep")
    args = ap.parse_args()
    if not args.apply:
        print("DRY RUN — pass --apply to execute\n")
    if not args.records_only:
        sweep_fake_managers(args.apply)
    sweep_records(args.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
