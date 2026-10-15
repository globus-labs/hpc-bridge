#!/usr/bin/env bash
# `pbs_mep` overlay, moms: the mapped account must resolve where its jobs run (uid 2100 everywhere). The facility's
# endpoint install (/opt/hpcb-mep, the `pbs-mep-tools` volume) is mounted read-only here by the compose overlay; the
# template's worker_init puts its bin/ on the workers' PATH.
set -u
id hpcbmep >/dev/null 2>&1 || useradd -M -u 2100 -g hpcb -s /bin/bash -p '*' hpcbmep \
  || echo "[pbs_mep] WARNING: could not create the mapped account hpcbmep"
