#!/usr/bin/env bash
# `f2b` overlay, login nodes (sourced as root before sshd starts): fail2ban on the port-22 sshd.
#   * sshd logs auth events to /var/log/auth.log (SSHD_LOGFILE — the entrypoint passes -E; no syslog in a container)
#   * jail `sshd`: maxretry 3 / findtime 600 / bantime 600, banaction iptables-multiport on port ssh ONLY — the
#     harness' key-only sshd on :2200 stays reachable from a banned address (that is how CLEANUP can unban)
#   * /var/log/fail2ban.log world-readable: postchecks count `Found`/`Ban` lines as the pool user
#   * the pool users may `sudo fail2ban-client` (status / unban --all) — a test-cluster convenience, not a site's
#   * the harness sshd on :2200 (`-p` — a command-line port makes sshd ignore the config's Port lines), key-only
#     whatever the drop-in says by then (`totp_f2b` layers totp's keyboard-interactive on the same config)
set -u
export SSHD_LOGFILE=/var/log/auth.log
touch "$SSHD_LOGFILE" && chmod 644 "$SSHD_LOGFILE"
cat > /etc/fail2ban/jail.d/hpcb-sshd.conf <<'JAIL'
[DEFAULT]
backend = polling
allowipv6 = auto
banaction = iptables-multiport
[sshd]
enabled = true
port = ssh
logpath = /var/log/auth.log
# (the entrypoint writes syslog-style lines — "<date> <host> sshd[pid]: Invalid user …" — because the sshd filter's
# prefix regex never matches sshd's bare -E output, whatever the datepattern; 0 failures were recorded, 2026-09-06)
mode = normal
maxretry = 3
findtime = 600
bantime = 600
JAIL
cat > /etc/fail2ban/fail2ban.d/hpcb.conf <<'F2B'
[Definition]
logtarget = /var/log/fail2ban.log
loglevel = INFO
F2B
mkdir -p /run/fail2ban && rm -f /run/fail2ban/fail2ban.sock /run/fail2ban/fail2ban.pid   # a container restart keeps the stale socket
if fail2ban-server -x -b >/dev/null 2>&1; then   # -x: start even if a stale socket is found
  for _ in $(seq 1 20); do fail2ban-client ping >/dev/null 2>&1 && break; sleep 0.5; done
  chmod 644 /var/log/fail2ban.log 2>/dev/null || true
  echo "[f2b] fail2ban up: $(fail2ban-client status sshd 2>/dev/null | grep -E 'Currently (failed|banned)' | tr -s ' \n' ' ')"
else
  echo "[f2b] WARNING: fail2ban-server did not start"
fi
printf '%%hpcb ALL=(root) NOPASSWD: /usr/bin/fail2ban-client\n' > /etc/sudoers.d/hpcb-f2b && chmod 440 /etc/sudoers.d/hpcb-f2b
if (exec 3<>/dev/tcp/127.0.0.1/2200) 2>/dev/null; then   # listening already (a pidfile can outlive a container restart)
  echo "[f2b] harness sshd on :2200 already running (outside the fail2ban jail); :22 is watched (maxretry 3, ban 600 s)"
elif /usr/sbin/sshd -p 2200 -o UsePAM=no -o KbdInteractiveAuthentication=no -o AuthenticationMethods=publickey -o PidFile=/run/sshd-harness.pid; then
  echo "[f2b] harness sshd on :2200 (outside the fail2ban jail); :22 is watched (maxretry 3, ban 600 s)"
else
  echo "[f2b] WARNING: the harness sshd on :2200 did not start"
fi
