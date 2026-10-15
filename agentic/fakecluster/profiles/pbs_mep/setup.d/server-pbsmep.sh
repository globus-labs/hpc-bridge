#!/usr/bin/env bash
# `pbs_mep` overlay, server (sourced as root by the entrypoint, after the base queues; the entrypoint runs
# `set -euo pipefail`, so every step that may fail is guarded). Two things:
# - the MAPPED account exists here too (uid 2100, as on the login node and the moms: same uids everywhere);
# - every user may READ other users' jobs (`query_other_jobs`, as on Polaris): the harness' world checks run as a pool
#   user and must see the mapped account's block — PBS's default (false) hides it, and an "is the block gone?" check
#   would then pass on an empty listing.
set -u
id hpcbmep >/dev/null 2>&1 || useradd -M -u 2100 -g hpcb -s /bin/bash -p '*' hpcbmep \
  || echo "[pbs_mep] WARNING: could not create the mapped account hpcbmep"
/opt/pbs/bin/qmgr -c "set server query_other_jobs = true" >/dev/null 2>&1 \
  || echo "[pbs_mep] WARNING: could not set query_other_jobs — pool users will not see hpcbmep's jobs"
echo "[pbs_mep] mapped account hpcbmep (uid 2100); query_other_jobs = $(/opt/pbs/bin/qmgr -c 'list server query_other_jobs' 2>/dev/null | awk -F'= ' '/query_other_jobs/{print $2}')"
