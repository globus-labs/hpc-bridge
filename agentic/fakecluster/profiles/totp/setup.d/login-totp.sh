#!/usr/bin/env bash
# `totp` overlay, login nodes (both; sourced as root before the port-22 sshd starts).
#   * every pool user enrolled with the fixture secret (~/.google_authenticator, 0600 — the module insists)
#   * PAM: sshd's auth stack is the authenticator ONLY (the pool users have no password; `@include common-auth` off)
#   * sshd on 22: UsePAM + keyboard-interactive, AuthenticationMethods publickey,keyboard-interactive:pam — the key
#     is accepted, then "Verification code:" — so a BatchMode ssh is denied with `(keyboard-interactive)` remaining,
#     exactly what the plugin reads as needs_preauth with a one-time code offered. sshd_config.d is FIRST-VALUE-WINS,
#     so the base drop-in is edited in place rather than shadowed by a later file.
#   * a second, key-only sshd on 2200 for the harness (sshd -o beats the config file), pidfile apart.
set -u
SECRET="${HPCB_TOTP_SECRET:-}"
if [ -z "$SECRET" ]; then echo "[totp] WARNING: HPCB_TOTP_SECRET unset — MFA NOT enabled on this login node"; return 0; fi
for u in "${POOL_USERS[@]}"; do
  h="/home/$u"; [ -d "$h" ] || continue
  printf '%s\n" TOTP_AUTH\n" WINDOW_SIZE 3\n' "$SECRET" > "$h/.google_authenticator"
  chown "$u:hpcb" "$h/.google_authenticator" && chmod 600 "$h/.google_authenticator"
done
if ! grep -q pam_google_authenticator /etc/pam.d/sshd; then
  sed -i 's|^@include common-auth|# @include common-auth   (totp profile: no password auth)\nauth required pam_google_authenticator.so|' /etc/pam.d/sshd
fi
D=/etc/ssh/sshd_config.d/00-hpcb-fake.conf
sed -i -e 's/^UsePAM no/UsePAM yes/' -e 's/^KbdInteractiveAuthentication no/KbdInteractiveAuthentication yes/' "$D"
grep -q '^AuthenticationMethods' "$D" || echo 'AuthenticationMethods publickey,keyboard-interactive:pam' >> "$D"
# `-p 2200` (not `-o Port=`): a command-line port makes sshd IGNORE the config's Port lines — `-o Port=2200` merely
# ADDED a listener and this daemon took :22 too, so the MFA sshd could not bind (crash loop, 2026-09-06).
# A profile layering totp with f2b (`totp_f2b`) runs f2b's script first, which starts the same key-only daemon.
if (exec 3<>/dev/tcp/127.0.0.1/2200) 2>/dev/null; then   # listening already (a pidfile can outlive a container restart)
  echo "[totp] enrolled ${#POOL_USERS[@]} users; :22 = key + one-time code (PAM google-authenticator); :2200 = key-only harness sshd (already running)"
elif /usr/sbin/sshd -p 2200 -o UsePAM=no -o KbdInteractiveAuthentication=no -o AuthenticationMethods=publickey -o PidFile=/run/sshd-harness.pid; then
  echo "[totp] enrolled ${#POOL_USERS[@]} users; :22 = key + one-time code (PAM google-authenticator); :2200 = key-only harness sshd"
else
  echo "[totp] WARNING: the harness sshd on :2200 did not start"
fi
