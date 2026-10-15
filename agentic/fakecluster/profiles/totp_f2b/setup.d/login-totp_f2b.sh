#!/usr/bin/env bash
# `totp_f2b` overlay, login nodes (sourced as root with the other layers' login-*.sh, before the port-22 sshd starts;
# its place among them does not matter — it only adds LogLevel, which neither totp nor f2b sets): make a WRONG
# one-time code visible to fail2ban.
#   Layered as-is, fail2ban saw nothing: three wrong codes from one address, `Found` 0 (throwaway container,
#   2026-10-14). sshd logs a failed keyboard-interactive auth ("Failed keyboard-interactive/pam for <user> from <ip>
#   port <n> ssh2") only at LogLevel VERBOSE, and its PAM line ("PAM: Authentication failure for <user> from <ip>")
#   leaves `sshd -e` without the "error: " prefix syslog gives it, which fail2ban's sshd filter requires. On a real
#   site syslog keeps that prefix and the PAM line counts: ONE failure per wrong code. VERBOSE gives the same count
#   here, from the "Failed keyboard-interactive/pam" line (a refused or partial publickey stays a non-failure in the
#   filter's default mode), so three wrong codes in ten minutes ban the address, as they would at such a site.
set -u
D=/etc/ssh/sshd_config.d/00-hpcb-fake.conf
grep -q '^LogLevel' "$D" || echo 'LogLevel VERBOSE' >> "$D"
echo "[totp_f2b] sshd LogLevel VERBOSE: a wrong one-time code is a fail2ban failure (one per code)"
