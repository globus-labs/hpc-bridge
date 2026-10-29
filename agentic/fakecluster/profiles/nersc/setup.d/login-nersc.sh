#!/usr/bin/env bash
# `nersc` overlay, login nodes (both; sourced as root after the homes exist, before the port-22 sshd starts).
#   * a stale ~/.gitconfig.lock in every home (a git that died mid-write): the `git config` lines of the noisy .bashrc
#     then print git's real "could not lock config file …/.gitconfig: File exists" on every shell's stderr
#   * port 22 takes a KEY or a PASSWORD + ONE-TIME CODE: PAM's sshd stack is the password (common-auth) and then
#     google-authenticator; publickey, password and keyboard-interactive are all on offer, with no
#     AuthenticationMethods, so a key-less BatchMode login is denied `(publickey,password,keyboard-interactive)` —
#     NERSC's shape. Every pool user gets a RANDOM password and code secret on each boot, known to nobody: the path
#     exists, and no cell completes it. sshd_config.d is first-value-wins, so the base drop-in is edited in place.
#   * a key-only harness sshd on :2200 whose keys come from a ROOT-OWNED file per user, not ~/.ssh/authorized_keys: a
#     cell revokes the key on port 22 by moving that file aside (a key the site stopped accepting), and the harness'
#     world channel (setup, postchecks, teardown) is untouched
#   * site's Anvil-format `mybalance` removed: not NERSC's tool
set -u
rm -f /usr/local/bin/mybalance
for u in "${POOL_USERS[@]}"; do
  h="$(getent passwd "$u" | cut -d: -f6)"; [ -d "$h" ] || continue
  if [ ! -e "$h/.gitconfig.lock" ]; then : > "$h/.gitconfig.lock" && chown "$u:hpcb" "$h/.gitconfig.lock"; fi
  echo "$u:$(head -c 24 /dev/urandom | base64 | tr -d '/+=')" | chpasswd
  if [ ! -s "$h/.google_authenticator" ]; then
    printf '%s\n" TOTP_AUTH\n' "$(head -c 10 /dev/urandom | base32)" > "$h/.google_authenticator"
    chown "$u:hpcb" "$h/.google_authenticator" && chmod 600 "$h/.google_authenticator"
  fi
  if [ -s /run/hpcb/authorized_keys ]; then
    install -D -m 644 -o root -g root /run/hpcb/authorized_keys "/etc/ssh/hpcb-harness-keys/$u"
  fi
done
if ! grep -q pam_google_authenticator /etc/pam.d/sshd; then
  sed -i 's|^@include common-auth|@include common-auth\nauth required pam_google_authenticator.so|' /etc/pam.d/sshd
fi
D=/etc/ssh/sshd_config.d/00-hpcb-fake.conf
sed -i -e 's/^UsePAM no/UsePAM yes/' -e 's/^KbdInteractiveAuthentication no/KbdInteractiveAuthentication yes/' \
       -e 's/^PasswordAuthentication no/PasswordAuthentication yes/' "$D"
# `-p 2200` (not `-o Port=`): a command-line port makes sshd IGNORE the config's Port lines (see totp's login script)
if (exec 3<>/dev/tcp/127.0.0.1/2200) 2>/dev/null; then   # listening already (a pidfile can outlive a container restart)
  echo "[nersc] :22 = key, or password + one-time code; :2200 = key-only harness sshd (already running)"
elif /usr/sbin/sshd -p 2200 -o UsePAM=no -o PasswordAuthentication=no -o KbdInteractiveAuthentication=no \
       -o AuthenticationMethods=publickey -o 'AuthorizedKeysFile=/etc/ssh/hpcb-harness-keys/%u' \
       -o PidFile=/run/sshd-harness.pid; then
  echo "[nersc] :22 = key, or password + one-time code; :2200 = key-only harness sshd"
else
  echo "[nersc] WARNING: the harness sshd on :2200 did not start"
fi
