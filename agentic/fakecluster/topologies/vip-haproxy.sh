#!/bin/sh
# The `vip` topology's TCP proxy (topologies/vip.yml): haproxy in TCP mode. :22 round-robins over the login nodes named
# in HPCB_VIP_BACKENDS (the profile's login_hosts, compose_files.sh) — the address a client reaches is no node's own;
# :2201, :2202, … go to one node each, in that order — the harness' path to a given node (topology.VIP_NODE_PORT0).
# Backends are resolved through Docker's DNS at run time (init-addr none): the proxy starts before a node answers.
set -eu
CFG="${HPCB_VIP_CFG:-/tmp/haproxy.cfg}"   # the hermetic test writes it elsewhere
{
  printf 'global\n  log stdout format raw local0 info\n'
  printf 'defaults\n  mode tcp\n  log global\n  option tcplog\n'
  printf '  timeout connect 5s\n  timeout client 4h\n  timeout server 4h\n'
  printf 'resolvers docker\n  nameserver dns 127.0.0.11:53\n  hold valid 10s\n'
  printf 'frontend vip\n  bind :22\n  default_backend logins\n'
  printf 'backend logins\n  balance roundrobin\n'
  i=0
  for h in ${HPCB_VIP_BACKENDS:-login}; do
    i=$((i + 1))
    printf '  server node%s %s:22 resolvers docker init-addr last,libc,none\n' "$i" "$h"
  done
  i=0
  for h in ${HPCB_VIP_BACKENDS:-login}; do
    i=$((i + 1))
    printf 'listen node%s\n  bind :%s\n  server node%s %s:22 resolvers docker init-addr last,libc,none\n' \
      "$i" "$((2200 + i))" "$i" "$h"
  done
} > "$CFG"
echo "[vip] fronting: ${HPCB_VIP_BACKENDS:-login}" >&2
exec haproxy -db -f "$CFG"
