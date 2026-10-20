#!/usr/bin/env bash
# `mep` overlay, login nodes (sourced as root before sshd; the entrypoint runs `set -euo pipefail`, so every step
# that may fail is guarded — a broken MEP must not take the login node down with it).
#
# Every login node: the mapped account + its home (shared /home). login01 ONLY: the two MEP managers — the endpoint
# software pinned to the version the plugin's SDK runs (HPCB_MEP_GCE_VERSION), the owner's Globus login staged into
# /root/.globus_compute (the `mep-state` volume, so UUIDs survive a restart), one endpoint dir per manager
# (config.yaml, identity_mapping.json, user_config_template.yaml.j2, user_config_schema.json), each started in the
# background. `hpcb-mep-catalog` prints the plugin-side catalog once endpoint.json (the minted UUID) exists.
set -u
MEPUSER=hpcbmep
id "$MEPUSER" >/dev/null 2>&1 || useradd -M -u 2100 -g hpcb -s /bin/bash -p '*' "$MEPUSER"
h="/home/$MEPUSER"
if [ ! -d "$h" ]; then mkdir -p "$h" && cp -rT /etc/skel "$h" && chown -R "$MEPUSER:hpcb" "$h" && chmod 700 "$h"; fi
install -m 755 "$PROFILE_DIR/mep/hpcb-mep-catalog" /usr/local/bin/hpcb-mep-catalog

if [ "$(hostname -s)" = login01 ]; then
  VER="${HPCB_MEP_GCE_VERSION:-4.16.0}"
  IDENTITY="${HPCB_MEP_IDENTITY:-gusellerm@uchicago.edu}"
  EMAIL="${HPCB_MEP_EMAIL:-}"   # the manager's contact address: REQUIRED by 4.16 and registered with Globus — a real one, from agentic/.env
  # The endpoint venv + its python live on the `mep-tools` volume (/opt/hpcb-mep): readable by the MAPPED user (the
  # manager forks each user endpoint as that account and execs the same globus-compute-endpoint — uv's default python
  # store /root/.local is 0700 and made the child exit 77), and available to bin/deregister.sh when the stack is down.
  # shellcheck disable=SC1091
  source "$PROFILE_DIR/mep/tools.sh"
  mep_tools_install "$VER" || echo "[mep] WARNING: endpoint install failed"
  mkdir -p /root/.globus_compute && chmod 700 /root/.globus_compute
  if [ ! -s /root/.globus_compute/storage.db ]; then
    if [ -s /run/hpcb/mep-storage.db ]; then install -m 600 /run/hpcb/mep-storage.db /root/.globus_compute/storage.db
    else echo "[mep] WARNING: no /run/hpcb/mep-storage.db mounted — the MEP cannot log in"; fi
  fi
  if [ -z "$EMAIL" ]; then
    echo "[mep] WARNING: HPCB_MEP_EMAIL is unset — NOT starting the managers (a Globus registration needs a real contact address; set it in agentic/.env)"
    return 0
  fi
  # The user endpoints both managers forked died with this container, but their pid files stayed on the shared /home
  # and would refuse the next cell's user endpoint (rc 73). Here, on login01 only (login02 shares /home but runs no
  # manager; a pid file removed under a LIVE user endpoint stops it), and before either manager starts.
  mep_clear_stale_uep_pids "$h" /root/.globus_compute/hpcb-mep-strict /root/.globus_compute/hpcb-mep-open \
    || echo "[mep] WARNING: could not clear stale user-endpoint pid files"
  for kind in strict open; do
    name="hpcb-mep-$kind"; d="/root/.globus_compute/$name"
    mkdir -p "$d" && chmod 700 "$d"
    sed -e "s/@@KIND@@/$kind/g" -e "s/@@EMAIL@@/$EMAIL/g" "$PROFILE_DIR/mep/config.yaml" > "$d/config.yaml"
    python3 "$PROFILE_DIR/mep/idmap.py" "$IDENTITY" "$MEPUSER" > "$d/identity_mapping.json" && chmod 600 "$d/identity_mapping.json"
    sed "s/@@VER@@/$VER/g" "$PROFILE_DIR/mep/user_config_template.yaml.j2" > "$d/user_config_template.yaml.j2"
    cp "$PROFILE_DIR/mep/schema-$kind.json" "$d/user_config_schema.json"
    rm -f "$d/daemon.pid"   # a stale pidfile from a killed container blocks the start for 90 s
    ( cd / && nohup "$MEP_GCE/bin/globus-compute-endpoint" start "$name" >"/var/log/$name.log" 2>&1 & ) || true
    echo "[mep] started $name (log /var/log/$name.log)"
  done
fi
