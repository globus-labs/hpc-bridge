#!/usr/bin/env python3
"""Write a LOCAL CATALOG (seed-format YAML) naming the fake cluster's login node as a REGISTRY SSH entry — the shape of
Expanse's entry (an SSH host, no facility endpoint, no allocation listing, `account_required: true`), which the fake
cluster otherwise only meets as a bring-your-own facility. The plugin's HPC_BRIDGE_CATALOG_FILE seam makes it the
catalog for one cell; the `mep` profile's `hpcb-mep-catalog` does the same for its facility endpoints.

Run INSIDE the jail (a scenario's LOCAL_SETUP), so the entry carries what only this cell knows: the endpoint name is
this run's HPC_BRIDGE_ENDPOINT_NAME (the harness tears the endpoint down by that name — a catalog entry otherwise
names its own), the login host is HPC_BRIDGE_SSH_HOST as the jail reaches it, and env_setup is hpc-bridge's own line at
this client's endpoint version and Python (what discovery proposes; the venv is built over SSH at the bootstrap).

    uv run --project <repo> --no-sync python agentic/harness/fake_ssh_catalog.py <out.yaml> [--id fake-ssh-site]
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys


def entry_yaml(*, facility_id: str, endpoint_name: str, ssh_host: str, account_required: bool = True,
               partition: str = "compute") -> str:
    from hpc_bridge.facility.endpoint_venv import short_env_setup

    def q(v: str) -> str:
        return json.dumps(v)  # a JSON string is a YAML double-quoted scalar

    host, env_setup, name, part = q(ssh_host), q(short_env_setup("{venv}")), q(endpoint_name), q(partition)
    required = "true" if account_required else "false"
    return f"""# hpc-bridge fake cluster — LOCAL catalog written by agentic/harness/fake_ssh_catalog.py for one cell
- id: {facility_id}
  facility_key: fake
  facility: "hpc-bridge fake cluster"
  description: "the fake site cluster over SSH, as a registry SSH entry (Expanse's shape): accounting enforced, an allocation account required; no allocation listing"
  display_name: "fake cluster (SSH, account required)"
  compute_mep_uuid: null
  ssh_host: {host}
  auth_method: ssh-key
  allocation: null
  account_required: {required}
  compute:
    scheduler: slurm
    interface: eth0
    env_setup: {env_setup}
    scratch_root: "/home/{{user}}/.hpc-bridge"
    endpoint_name: {name}
  defaults:
    partition: {part}
    walltime: "00:30:00"
    max_workers_per_node: 2
    nodes_per_block: 1
    max_blocks: 1
  provenance: curated
  last_validated: {datetime.date.today().isoformat()}
"""


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("out")
    ap.add_argument("--id", default="fake-ssh-site")
    ap.add_argument("--no-account-required", action="store_true")
    a = ap.parse_args(argv)
    name = os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip()
    if not name:
        print("fake_ssh_catalog: HPC_BRIDGE_ENDPOINT_NAME is unset — the harness could not tear the endpoint down",
              file=sys.stderr)
        return 2
    text = entry_yaml(facility_id=a.id, endpoint_name=name, ssh_host=os.environ.get("HPC_BRIDGE_SSH_HOST") or "login",
                      account_required=not a.no_account_required)
    with open(a.out, "w") as fh:
        fh.write(text)
    print(f"fake_ssh_catalog: {a.id} → {a.out} (endpoint {name})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
