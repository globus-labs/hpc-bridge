#!/usr/bin/env bash
# Delete the fake PBS cluster's MEP REGISTRATION (hpcb-mep-pbs) with the Globus Compute service before its state is
# wiped. Run by bin/down.sh --wipe (convention: a profile's deregister.sh runs before `down -v`), or by hand. The `mep`
# profile's deregister.sh on THIS profile's volumes (`pbs-mep-state`: the manager's endpoint dir + the owner's login;
# `pbs-mep-tools`: the endpoint venv, installed here if missing) and the PBS image: it works with the stack DOWN, from
# a one-off container, and the record disappears from the owner's endpoint list instead of orphaning when --wipe mints
# a new UUID. The delete loop is the mep profile's, verbatim (test_pbs_mep.py keeps the two the same).
# HPCB_FAKE_KEEP_REGISTRATIONS=1 skips it.
set -euo pipefail
[ "${HPCB_FAKE_KEEP_REGISTRATIONS:-}" = 1 ] && { echo "deregister: kept (HPCB_FAKE_KEEP_REGISTRATIONS=1)"; exit 0; }
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="${HPCB_FAKE_PROJECT:-hpcb-fake}"
docker volume inspect "${PROJECT}_pbs-mep-state" >/dev/null 2>&1 || { echo "deregister: no ${PROJECT}_pbs-mep-state volume — nothing registered from here"; exit 0; }
echo "deregister: deleting the fake PBS MEP registration (one-off container on the pbs-mep-state + pbs-mep-tools volumes)…"
docker run --rm --entrypoint /bin/bash \
  -v "${PROJECT}_pbs-mep-state:/root/.globus_compute" -v "${PROJECT}_pbs-mep-tools:/opt/hpcb-mep" \
  -v "$HERE/mep/tools.sh:/run/hpcb/tools.sh:ro" \
  -e HPCB_MEP_GCE_VERSION="${HPCB_MEP_GCE_VERSION:-4.16.0}" \
  "${HPCB_FAKE_PBS_IMAGE:-hpcb-fake-pbs:latest}" -c '
    set -u
    source /run/hpcb/tools.sh
    mep_tools_install "$HPCB_MEP_GCE_VERSION" || { echo "deregister: could not install the endpoint tooling"; exit 1; }
    rc=0
    for d in /root/.globus_compute/hpcb-mep-*/; do
      [ -d "$d" ] || continue
      name="$(basename "$d")"
      rm -f "$d/daemon.pid"   # the stack is down: a stale pidfile would make delete think the manager is running
      uuid="$(sed -n "s/.*\"endpoint_id\": *\"\([0-9a-f-]*\)\".*/\1/p" "$d/endpoint.json" 2>/dev/null | head -n 1)"
      out="$("$MEP_GCE/bin/globus-compute-endpoint" delete --yes "$name" 2>&1 </dev/null)"; erc=$?
      if printf "%s" "$out" | grep -q "is currently running"; then
        # The stack is already down, so the service is reporting a heartbeat it has not yet seen stop. With no
        # pidfile, --force only skips that status check; a failed web delete still prints and is caught below.
        out="$("$MEP_GCE/bin/globus-compute-endpoint" delete --yes --force "$name" 2>&1 </dev/null)"; erc=$?
      fi
      printf "%s\n" "$out" | tail -n 2 | sed "s/^/deregister: [$name] /"
      if printf "%s" "$out" | grep -q "ENDPOINT_NOT_FOUND"; then
        echo "deregister: $name was already gone from the service"   # deleted earlier: nothing left to remove
      elif [ "$erc" = 0 ] && ! printf "%s" "$out" | grep -q "could not be deleted from the web service"; then
        echo "deregister: $name deleted"
      else
        echo "deregister: $name FAILED (record ${uuid:-?} may remain — delete it in the Globus web app)"; rc=1
      fi
    done
    exit $rc'
