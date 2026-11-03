"""The SSH target hpc-bridge's own code builds, and whether it works from here — run in the PROJECT's environment
(hpc_bridge importable), in the jail, by topology_roundtrip's world checks:

    uv run --frozen --offline --directory <repo> --extra integration python agentic/harness/topology_probe.py <alias> [pin …]

For the alias alone, and for each pin: the facility binding.py builds for a facility reached through `alias`
(`_slurm_facility`, the server's env: HPC_BRIDGE_SSH_USER / _KEY, the ControlMaster dir), rebound to the pin exactly as
binding.py rebinds a stored record's (`_routable_pin`, then `rebind`). Then:
- `reach`: the target's own argv (`SshTarget.argv`) with multiplexing off — a fresh connection, so a master already up
  can never stand in for the path — running `echo HPCB_REACHED $(hostname)`;
- `preauth`: the target's `preauth_command()` (what a `needs_preauth` answer would hand the user), run as
  topology.run_preauth runs it (BatchMode, a scratch ControlPath).
One JSON line per target: {"pin", "kept", "host", "host_key_alias", "reach_rc", "reach_out", "preauth_command",
"preauth_rc", "preauth_out"}. Nothing here writes hpc-bridge's state (the probe's endpoint name has no record).
"""
from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import topology  # beside this file, after the path insert

PROBE_ENDPOINT = "hpcb-topology-probe"   # no record under this name: the alias target, unless a pin is given


def target_for(alias: str, pin: str | None):
    """(the SshTarget hpc-bridge would use, the pin `_routable_pin` kept or None)."""
    from hpc_bridge import binding, config
    from hpc_bridge.facility.remote import _routable_pin

    user = (config.ssh_user() or "") or binding._ssh_config_user(alias)
    profile = SimpleNamespace(name="topology-probe", env_setup="", endpoint_name=PROBE_ENDPOINT)
    fac = binding._slurm_facility(profile, alias=alias, user=user)
    kept = _routable_pin(pin) if pin else None
    if kept:
        fac.cli.rebind(kept)
    return fac.cli.target, kept


def check(alias: str, pin: str | None) -> dict:
    target, kept = target_for(alias, pin)
    row: dict = {"pin": pin, "kept": kept, "host": target.host, "host_key_alias": target.host_key_alias}
    try:
        r = subprocess.run(replace(target, control_dir=None).argv("echo HPCB_REACHED $(hostname)"),
                           stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=60, check=False)
        row.update(reach_rc=r.returncode, reach_out=(r.stdout + r.stderr).strip()[-300:])
    except subprocess.TimeoutExpired:
        row.update(reach_rc=124, reach_out="timed out after 60 s")
    cmd = target.preauth_command()
    rc, out = topology.run_preauth(cmd)
    row.update(preauth_command=cmd, preauth_rc=rc, preauth_out=out.strip()[-300:])
    return row


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    alias, pins = argv[0], [p for p in argv[1:] if p and p != "-"]
    for pin in [None, *pins]:
        print(json.dumps(check(alias, pin)), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
