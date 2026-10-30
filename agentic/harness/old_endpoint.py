"""Stand up an endpoint the way hpc-bridge 0.1.23 did — `endpoint_upgrade_reuse`'s LOCAL_SETUP, run inside the jail
with the plugin's own environment:

    uv run --project <repo> --no-sync python agentic/harness/old_endpoint.py '<FacilityDetails JSON>'

A per-phase build of an older plugin is too heavy for a cell, so this drives the CURRENT code with exactly what 0.1.23
wrote: the env_setup line its discovery proposed (`[ -d {venv} ] || uv venv …; command -v globus-compute-endpoint ||
uv pip install …`), which would build the unkeyed `{venv}` itself on the bring-up's first gce call (the scenario's SETUP
builds it first with `texttable<1.7.2`, `cluster_ops.build_unkeyed_venv_cmd`, and the line's guards leave it), and a
UEP template whose worker_init default is JSON-encoded ONCE (0.1.24 encodes it twice). The profile is built directly,
so the upgrade-on-read never touches the line; the line is not hpc-bridge's new one, so the Python build never runs.
The rest — seeding the token store, configure, start, the endpoint record — is the plugin's own bootstrap, as it was.
The endpoint's name is the run's (HPC_BRIDGE_ENDPOINT_NAME), so the agent's connect finds it online over the web and
reuses it.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import replace


def old_discovery_line(python: str, gce: str) -> str:
    """The uv line 0.1.23's discovery proposed (`_UV_ENV_SETUP` then), `{venv}` templated."""
    return (f"[ -d {{venv}} ] || uv venv {{venv}} --python {python}; . {{venv}}/bin/activate; "
            f"command -v globus-compute-endpoint >/dev/null 2>&1 || uv pip install -q 'globus-compute-endpoint=={gce}'")


def main(argv: list[str]) -> int:
    from hpc_bridge import binding, config
    from hpc_bridge.facility.endpoint_venv import LEGACY_REMOTE_VENV, client_gce, client_python
    from hpc_bridge.facility.remote import profile_from_catalog_entry
    from hpc_bridge.models import FacilityDetails
    from hpc_bridge.profile import Profile

    details = FacilityDetails(**json.loads(argv[1]))
    host = details.ssh_host
    user = config.ssh_user() or binding._ssh_config_user(host)
    entry = binding._entry_from_details("upgrade-phase-1", details)
    prof = profile_from_catalog_entry(entry, user=user, account="", venv=config.remote_venv())
    venv = config.remote_venv() or LEGACY_REMOTE_VENV.replace("{user}", user)  # 0.1.23's `{venv}` default
    old = old_discovery_line(client_python(), client_gce() or "").replace("{venv}", venv)
    prof = replace(prof, env_setup=old, worker_init=old)
    fac = binding._slurm_facility(prof, alias=host, user=user)
    template = fac.config_template

    def old_template(hpc):  # 0.1.23: the worker_init default encoded once
        tmpl, defaults = template(hpc)
        return tmpl.replace(json.dumps(json.dumps(old)), json.dumps(old)), defaults

    fac.config_template = old_template
    handle = asyncio.run(fac.bootstrap(Profile()))
    print(f"old-style endpoint {handle.name} {handle.endpoint_id} (reused={handle.reused}); worker_init: {old}")
    return 0


if __name__ == "__main__":
    rc = main(sys.argv)
    sys.stdout.flush()
    os._exit(rc)  # the SDK's threads must not hold the setup step open
