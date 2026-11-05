#!/usr/bin/env bash
# `pbs_mep` overlay, login node (sourced as root before sshd; the entrypoint runs `set -euo pipefail`, so every step
# that may fail is guarded — a broken MEP must not take the login node down with it). The `mep` profile's
# setup.d/login-mep.sh, for ONE manager on the PBS stack's single login node:
#
# the mapped account + its home (shared /home); the endpoint software pinned to the version the plugin's SDK runs
# (HPCB_MEP_GCE_VERSION), installed on the `pbs-mep-tools` volume (/opt/hpcb-mep: readable by the mapped user, whom the
# manager forks each user endpoint as; mounted read-only on the moms, where the template's worker_init puts its bin/ on
# the workers' PATH); the owner's Globus login staged into /root/.globus_compute (the `pbs-mep-state` volume, so the
# UUID survives a restart); the endpoint dir (config.yaml, identity_mapping.json, user_config_template.yaml.j2,
# user_config_schema.json); the manager started in the background. `hpcb-pbs-mep-catalog` prints the plugin-side
# catalog once endpoint.json (the minted UUID) exists.
set -u
MEPUSER=hpcbmep
id "$MEPUSER" >/dev/null 2>&1 || useradd -M -u 2100 -g hpcb -s /bin/bash -p '*' "$MEPUSER" \
  || echo "[pbs_mep] WARNING: could not create the mapped account $MEPUSER"
h="/home/$MEPUSER"
if [ ! -d "$h" ]; then
  { mkdir -p "$h" && cp -rT /etc/skel "$h" && chown -R "$MEPUSER:hpcb" "$h" && chmod 700 "$h"; } \
    || echo "[pbs_mep] WARNING: could not create $h"
fi
install -m 755 "$PROFILE_DIR/mep/hpcb-pbs-mep-catalog" /usr/local/bin/hpcb-pbs-mep-catalog \
  || echo "[pbs_mep] WARNING: could not install hpcb-pbs-mep-catalog"

VER="${HPCB_MEP_GCE_VERSION:-4.16.0}"
IDENTITY="${HPCB_MEP_IDENTITY:-gusellerm@uchicago.edu}"
EMAIL="${HPCB_MEP_EMAIL:-}"   # the manager's contact address: REQUIRED by 4.16+ and registered with Globus — a real one, from agentic/.env
# shellcheck disable=SC1091
source "$PROFILE_DIR/mep/tools.sh"
mep_tools_install "$VER" || echo "[pbs_mep] WARNING: endpoint install failed"
mkdir -p /root/.globus_compute && chmod 700 /root/.globus_compute
if [ ! -s /root/.globus_compute/storage.db ]; then
  if [ -s /run/hpcb/mep-storage.db ]; then install -m 600 /run/hpcb/mep-storage.db /root/.globus_compute/storage.db \
    || echo "[pbs_mep] WARNING: could not stage the owner's login"
  else echo "[pbs_mep] WARNING: no /run/hpcb/mep-storage.db mounted — the MEP cannot log in"; fi
fi
if [ -z "$EMAIL" ]; then
  echo "[pbs_mep] WARNING: HPCB_MEP_EMAIL is unset — NOT starting the manager (a Globus registration needs a real contact address; set it in agentic/.env)"
  return 0
fi
ready=""
name=hpcb-mep-pbs; d="/root/.globus_compute/$name"
if mkdir -p "$d" && chmod 700 "$d" \
   && sed -e "s/@@EMAIL@@/$EMAIL/g" "$PROFILE_DIR/mep/pbs_config.yaml" > "$d/config.yaml" \
   && python3 "$PROFILE_DIR/mep/idmap.py" "$IDENTITY" "$MEPUSER" > "$d/identity_mapping.json" \
   && chmod 600 "$d/identity_mapping.json" \
   && cp "$PROFILE_DIR/mep/pbs_user_config_template.yaml.j2" "$d/user_config_template.yaml.j2" \
   && cp "$PROFILE_DIR/mep/pbs_user_config_schema.json" "$d/user_config_schema.json"; then
  rm -f "$d/daemon.pid"   # a stale pidfile from a killed container blocks the start for 90 s
  ready="$ready $name"
else
  echo "[pbs_mep] WARNING: could not write $d — the manager was NOT started"
fi
# The SECOND manager (block environments, PR 3): the same Polaris-shaped template, which ALSO builds a client's
# `requirements` into the workers' env when a block starts (NCSA Delta's and Purdue Anvil's kind, after
# globus-compute-endpoint 4.17's default) — `fake-pbs-mep-req` in the catalog. Its own endpoint dir and registration,
# the same identity mapping and mapped account; its template's @@VER@@ is this manager's endpoint version (the venv it
# builds installs the endpoint at it, so the worker still matches the user endpoint). A failure here leaves the first
# manager running: only the env cells need this one.
name=hpcb-mep-pbs-req; d="/root/.globus_compute/$name"
if mkdir -p "$d" && chmod 700 "$d" \
   && sed -e "s/@@EMAIL@@/$EMAIL/g" -e "s#/hpcb-mep-pbs/#/$name/#" \
          -e "s/^display_name: .*/display_name: hpc-bridge fake PBS cluster MEP (Polaris-shaped template + requirements)/" \
          "$PROFILE_DIR/mep/pbs_config.yaml" > "$d/config.yaml" \
   && python3 "$PROFILE_DIR/mep/idmap.py" "$IDENTITY" "$MEPUSER" > "$d/identity_mapping.json" \
   && chmod 600 "$d/identity_mapping.json" \
   && sed "s/@@VER@@/$VER/g" "$PROFILE_DIR/mep/pbs_user_config_template_req.yaml.j2" > "$d/user_config_template.yaml.j2" \
   && cp "$PROFILE_DIR/mep/pbs_user_config_schema_req.json" "$d/user_config_schema.json"; then
  rm -f "$d/daemon.pid"
  ready="$ready $name"
else
  echo "[pbs_mep] WARNING: could not write $d — the manager $name was NOT started"
fi
# ...and so do their user endpoints' pid files (rc 73 at the next cell's first task): they died with this container,
# and only this node runs the managers, so none can be alive. Before either manager starts, so neither has forked one.
mep_clear_stale_uep_pids "$h" /root/.globus_compute/hpcb-mep-pbs /root/.globus_compute/hpcb-mep-pbs-req \
  || echo "[pbs_mep] WARNING: could not clear stale user-endpoint pid files"
for name in $ready; do
  ( cd / && nohup "$MEP_GCE/bin/globus-compute-endpoint" start "$name" >"/var/log/$name.log" 2>&1 & ) || true
  echo "[pbs_mep] started $name (log /var/log/$name.log)"
done
