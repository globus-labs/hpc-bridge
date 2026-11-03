#!/usr/bin/env bash
# The `bastion` topology's jump host (topologies/bastion.yml): the image's pool users, each with a home of its own (no
# shared /home here) holding the test key, and sshd — key-only, the image's drop-in — with TCP forwarding on, which is
# all `ProxyJump` asks of it (`ssh -W host:22`). Nothing else runs here.
set -euo pipefail
POOL_USERS=(hpcbridge-test $(for i in $(seq -f "%02g" 0 9); do echo "hpcbridge-test-$i"; done))
log() { echo "[bastion] $*" >&2; }
[ -s /run/hpcb/authorized_keys ] || log "WARNING: no /run/hpcb/authorized_keys mounted — nobody can jump through here"
for u in "${POOL_USERS[@]}"; do
  h="/home/$u"
  mkdir -p "$h/.ssh"
  [ -s /run/hpcb/authorized_keys ] && install -m 600 /run/hpcb/authorized_keys "$h/.ssh/authorized_keys"
  chown -R "$u:hpcb" "$h" && chmod 700 "$h" "$h/.ssh"
done
# ProxyJump is a direct-tcpip channel: forwarding must be on (it is by default; said here so no drop-in turns it off)
printf 'AllowTcpForwarding yes\n' > /etc/ssh/sshd_config.d/10-hpcb-bastion.conf
mkdir -p /run/sshd
log "starting sshd ($(hostname), $(ip -o -4 addr show scope global | awk '{print $2"="$4}' | tr '\n' ' '))"
exec /usr/sbin/sshd -D -e
