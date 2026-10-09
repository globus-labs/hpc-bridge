#!/usr/bin/env python
"""Re-prove a registry facility live, and record what it was proven against. The PAID tier of registry health:
one real block on the facility's multi-user endpoint, through hpc-bridge's real functions (no agent).

    uv run python agentic/registry_reprove.py ENTRY_ID [--account ACCT] [--record] [--wait-s 900]

connect → confirm spend → wait for a worker → `hostname` on the block → stop (draining on a facility endpoint).
On a PASS with --record, the seed file gains (or refreshes) the entry's `verification` block — the facility's live
endpoint version, Python, template/schema and manager-config digests, and what the worker reported (Python, dill,
parsl, node) — and `last_validated` becomes today; `worker_env.verified_with` follows the endpoint version. Comments
in the seed are kept (a targeted text edit, not a YAML round-trip). Commit + ingest the seed afterwards.

The free tier, `hpc-bridge-registry-health`, compares the facility against that block every run; this tool is what
catches what no published field shows (the endpoint's parsl, a QOS rule). Charges: one block, at most the entry's
walltime, on the given account (the user's allocation). Facility endpoints only — an SSH entry needs the user's
credentials (Expanse: a one-time code) and is re-proven by hand.

Prints one JSON line last: {"entry", "pass", "worker", "fingerprint", "recorded"}.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import os
import re
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SEED_DIR = REPO / "src" / "hpc_bridge" / "catalog" / "seed"


def seed_file_for(entry_id: str, seed_dir: Path = SEED_DIR) -> Path:
    import yaml

    for f in sorted(seed_dir.glob("*.yaml")):
        rows = yaml.safe_load(f.read_text()) or []
        if any(isinstance(r, dict) and r.get("id") == entry_id for r in rows):
            return f
    raise SystemExit(f"no seed defines id {entry_id!r} under {seed_dir}")


def record_verification(text: str, entry_id: str, block: dict, on: datetime.date) -> str:
    """Rewrite one entry of a seed file's TEXT: `last_validated` → `on`, `worker_env.verified_with` → the endpoint
    version, and a fresh `verification:` block (replacing any old one, blank lines and comments inside it included)
    right after `last_validated`. Everything else — other entries, comments, key order — is left byte-for-byte."""
    lines = text.splitlines(keepends=True)
    id_re = re.compile(rf"""^- id:\s*["']?{re.escape(entry_id)}["']?\s*(#.*)?$""")
    start = next((i for i, ln in enumerate(lines) if id_re.match(ln.rstrip("\r\n"))), None)
    if start is None:
        raise ValueError(f"no `- id: {entry_id}` line (the id must be the entry's first key) in the seed")
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("- ")), len(lines))
    body = lines[start:end]

    # drop an existing verification block: from its key to the next sibling key (2-space indent, not a comment), the
    # next entry, or the end — keeping only trailing blank lines (the separator before the next entry)
    vi = next((i for i, ln in enumerate(body) if re.match(r"^  verification:", ln)), None)
    if vi is not None:
        # the block = its key plus every line indented deeper than the entry's keys, with any blank lines or comments
        # BETWEEN such lines; it ends at the first line that is neither (a sibling key, a comment before the next
        # key or entry, a separator) — those, and trailing blanks, are kept
        last = vi
        for j in range(vi + 1, len(body)):
            ln = body[j]
            if ln.startswith("    ") and ln.strip():
                last = j
            elif ln.strip() and not ln.lstrip().startswith("#"):
                break
        del body[vi:last + 1]

    ver = block.get("endpoint_version")
    if ver:
        body = [re.sub(r'^(\s+verified_with:\s*)"?[^"\s#]+"?', rf'\g<1>"{ver}"', ln) for ln in body]
    li = next((i for i, ln in enumerate(body) if re.match(r"^  last_validated:", ln)), None)
    if li is None:
        raise ValueError(f"entry {entry_id!r} has no `last_validated:` line")
    body[li] = f"  last_validated: {on.isoformat()}\n"
    new = ["  verification:                    # written by agentic/registry_reprove.py — what the live run met\n",
           f"    verified_on: {on.isoformat()}\n"]
    for key in ("endpoint_version", "python_version", "template_sha256", "config_sha256", "worker"):
        if block.get(key):
            new.append(f'    {key}: "{block[key]}"\n')
    body[li + 1:li + 1] = new
    return "".join(lines[:start] + body + lines[end:])


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("entry_id")
    ap.add_argument("--account", default=None, help="the allocation to charge (account-required facilities)")
    ap.add_argument("--record", action="store_true", help="on a pass, write the verification block into the seed")
    ap.add_argument("--wait-s", type=int, default=900)
    ap.add_argument("--globus-dir", default=None,
                    help="GLOBUS_COMPUTE_USER_DIR holding the Globus login to use (default: the SDK's own)")
    a = ap.parse_args(argv)
    if a.globus_dir:
        os.environ["GLOBUS_COMPUTE_USER_DIR"] = str(Path(a.globus_dir).expanduser())
    for k in ("HPC_BRIDGE_ENDPOINT_ID", "HPC_BRIDGE_ENDPOINT_NAME", "HPC_BRIDGE_MACHINE", "HPC_BRIDGE_ACCOUNT",
              "HPC_BRIDGE_SSH_HOST"):
        os.environ.pop(k, None)

    import yaml

    from hpc_bridge.catalog.entry import CatalogEntry, facility_fingerprint
    from hpc_bridge.endpoint import EndpointCLI
    from hpc_bridge.facility.local import LocalFacility
    from hpc_bridge.login import LoginFlow, globus_identity_label
    from hpc_bridge.profile import Profile
    from hpc_bridge.server import (
        AppCtx,
        _connect_facility,
        _ensure_endpoint_up,
        _run_shell,
        _shape_runtime,
        _stop_endpoint,
    )

    seed = seed_file_for(a.entry_id)
    rows = yaml.safe_load(seed.read_text())
    entry = CatalogEntry.model_validate(next(r for r in rows if r.get("id") == a.entry_id))
    if not entry.compute_mep_uuid:
        print(f"{a.entry_id} is an SSH entry: re-prove it by hand (it needs the user's credentials)")
        return 2
    if LoginFlow().login_required():
        print("no Globus login in the selected store")
        return 2
    print(f"identity: {globus_identity_label()}  entry: {entry.subject} ({seed.name})", flush=True)
    if a.record:  # fail BEFORE the paid run if the seed cannot be edited
        record_verification(seed.read_text(), entry.id, {"endpoint_version": "0"}, datetime.date.today())

    async def run() -> dict:
        from globus_compute_sdk import Client

        before = facility_fingerprint(await asyncio.to_thread(Client().get_endpoint_metadata, entry.compute_mep_uuid))
        app = AppCtx(facility=LocalFacility(EndpointCLI()), profile=Profile())
        app.session_facilities[entry.id] = entry  # prove the SEED as written, not whatever the index serves
        res = await _connect_facility(app, entry.id)
        print(f"connect: {res.phase} — {(res.notice or '')[:240]}", flush=True)
        result = {"entry": entry.id, "pass": False, "worker": None, "fingerprint": before, "recorded": False}
        if res.phase not in ("needs_account", "connected", "provisioning"):
            return result
        t0 = time.monotonic()
        st = None
        try:  # from the FIRST submit on: the wait loop is where the billed check goes out (and where Ctrl-C lands)
            while True:
                kw: dict = {"shape": "compute", "confirm_spend": True}
                if a.account:
                    kw["account"] = a.account
                st = await _ensure_endpoint_up(app, **kw)
                print(f"  +{int(time.monotonic() - t0)}s {st.status}: {(st.notice or '')[:160]}", flush=True)
                if st.status in ("up", "down", "needs_account") or time.monotonic() - t0 > a.wait_s:
                    break
                await asyncio.sleep(30)
            canary = _shape_runtime(app, "compute").last_canary
            if st.status == "up":
                out = await _run_shell(app, "hostname; whoami; echo SLURM_JOB_ID=$SLURM_JOB_ID", shape="compute")
                print(f"  run_shell: {out.phase} exit={out.exit_code} stdout={out.stdout!r}", flush=True)
                result["pass"] = out.phase == "complete" and out.exit_code == 0
            if canary is not None and canary.ok:
                result["worker"] = " ".join(x for x in (
                    f"py{canary.worker_python}" if canary.worker_python else "",
                    f"dill{canary.worker_dill}" if canary.worker_dill else "",
                    f"parsl{canary.worker_parsl}" if canary.worker_parsl else "",
                    f"gce{canary.worker_gce}" if canary.worker_gce else "",
                    f"on {canary.worker_host}" if canary.worker_host else "") if x)
        finally:
            try:
                stp = await _stop_endpoint(app)  # always: an error, a timeout, Ctrl-C
                print(f"  stop: {stp.status} — {(stp.notice or '')[:400]}", flush=True)
            except BaseException as exc:  # noqa: BLE001 - the warning below must still be printed
                print(f"  stop FAILED: {type(exc).__name__}: {exc}", flush=True)
            finally:
                if not result["pass"]:
                    # On a facility endpoint stop only drains: a check task still queued there can keep the facility
                    # starting (billed) blocks — up to its own hard limit (Delta: 48 h; the 2026-10-06 runaway).
                    print("  WARNING: the run did not pass. A check task may still be queued at the facility, which "
                          "can keep starting billed blocks for it. Check the facility's queue for your user (e.g. "
                          "`squeue -u $USER`) and cancel any hpc-bridge (parsl) jobs; if they keep reappearing, "
                          "contact the facility.", flush=True)
        after = facility_fingerprint(await asyncio.to_thread(Client().get_endpoint_metadata, entry.compute_mep_uuid))
        if after != before:  # the facility changed during the run: the proof is of neither state
            print("  the facility's metadata changed during the run — not recording", flush=True)
            result["pass"] = False
        return result

    result = asyncio.run(run())
    if result["pass"] and a.record:
        block = dict(result["fingerprint"], worker=result["worker"])
        seed.write_text(record_verification(seed.read_text(), entry.id, block, datetime.date.today()))
        result["recorded"] = True
        print(f"recorded the verification block in {seed.relative_to(REPO)}", flush=True)
    print("VERDICT:", "PASS" if result["pass"] else "FAIL")
    print(json.dumps(result))
    sys.stdout.flush()
    os._exit(0 if result["pass"] else 1)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
