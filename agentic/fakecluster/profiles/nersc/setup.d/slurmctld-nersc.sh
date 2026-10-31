#!/usr/bin/env bash
# `nersc` overlay, controller (after site's slurmctld.sh): Perlmutter's QOS. Every pool user may use debug, regular and
# shared (regular by default); realtime exists but no one is granted it, so realtime_ss11 (AllowQos=realtime) takes
# none of their jobs. The submit filter (job_submit.lua) sets each job's QOS. Idempotent.
set -u
for q in regular shared realtime; do sacctmgr -i add qos "$q" >/dev/null 2>&1 || true; done
for u in "${POOL_USERS[@]}"; do
  sacctmgr -i modify user where name="$u" set qos=normal,debug,regular,shared defaultqos=regular >/dev/null 2>&1 || true
done
echo "[nersc] QOS debug/regular/shared for every pool user, realtime for none"
