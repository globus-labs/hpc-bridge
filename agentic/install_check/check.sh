#!/usr/bin/env bash
# The install-experience check, run inside the clean container (see Dockerfile). It follows docs/user/other-hosts.md
# for each harness, step for step, and reports whether each harness ends up with hpc-bridge's 12 tools.
#   REF   the hpc-bridge git ref to install (default main)
set -uo pipefail
REF="${REF:-main}"
SRC="git+https://github.com/globus-labs/hpc-bridge@${REF}"
CMD=(uvx --python 3.13 --from "$SRC" hpc-bridge)
EXPECT=12
fails=0
say()  { printf '%s\n' "$*"; }
pass() { say "PASS  $*"; }
fail() { say "FAIL  $*"; fails=$((fails + 1)); }

say "== versions: codex $(codex --version 2>&1 | head -1) | pi $(pi --version 2>&1 | head -1) | hermes $(hermes --version 2>&1 | head -1)"
say "== hpc-bridge ref: $REF"

# 1. pre-warm: the first uvx build takes longer than some harnesses wait for a server to start
t0=$SECONDS
if uvx --python 3.13 --from "$SRC" python -c "import hpc_bridge, globus_compute_sdk as g, sys; print(sys.version.split()[0], g.__version__)"; then
  pass "pre-warm: hpc-bridge built in $((SECONDS - t0)) s"
else
  fail "pre-warm: uvx could not build hpc-bridge from $SRC"
fi

# 2. the server itself, over MCP, with the documented command
out=$(uvx --with 'mcp>=1.28,<2' python mcp_probe.py direct "${CMD[@]}" 2>/dev/null | tail -1)
say "    $out"
n=$(jq -r '.tools | length' <<<"$out" 2>/dev/null || echo 0)
[[ "$n" == "$EXPECT" ]] && pass "server: $n tools" || fail "server: $n tools (want $EXPECT)"
[[ "$(jq -r '.guidance_is_skill' <<<"$out" 2>/dev/null)" == "true" ]] && pass "server: guidance resource is the skill" || fail "server: guidance resource"
nf=$(jq -r '.facilities | length' <<<"$out" 2>/dev/null || echo 0)
[[ "$nf" -ge 1 ]] && pass "server: the public registry lists $nf facilities (anonymous)" || fail "server: list_facilities returned none"

# the skill, installed where Codex and Pi look (~/.agents/skills) and where Hermes looks (~/.hermes/skills)
for d in "$HOME/.agents/skills/driving-hpc" "$HOME/.hermes/skills/driving-hpc"; do
  mkdir -p "$d" && curl -fsSL "https://raw.githubusercontent.com/globus-labs/hpc-bridge/${REF}/skills/driving-hpc/SKILL.md" -o "$d/SKILL.md"
done
[[ -s "$HOME/.agents/skills/driving-hpc/SKILL.md" ]] && pass "skill: downloaded" || fail "skill: download"

# 3. Pi: add with direct exposure (the default `codemode` hides the tools from the model), then a 300 s timeout
export PI_OFFLINE=1 PI_SKIP_VERSION_CHECK=1 PI_TELEMETRY=0
pi mcp add hpc-bridge --exposure direct -- "${CMD[@]}" >/dev/null 2>&1 || fail "pi: mcp add"
python3 - <<'PY'
import json, pathlib
p = pathlib.Path.home() / ".pi/agent/mcp.json"
d = json.loads(p.read_text()); d["mcpServers"]["hpc-bridge"]["timeout"] = 300; p.write_text(json.dumps(d, indent=2))
PY
pl=$(pi mcp list 2>&1 | grep -A1 "^hpc-bridge" | head -2)
say "    $pl"
grep -q "connected, $EXPECT tools" <<<"$pl" && pass "pi: connected, $EXPECT tools" || fail "pi: not connected with $EXPECT tools"

# 4. Hermes: add with a longer connect timeout (default 60 s); answer its enable-tools prompt
printf 'y\n' | hermes mcp add hpc-bridge --connect-timeout 180 --env 'SSH_AUTH_SOCK=${SSH_AUTH_SOCK}' \
    --command uvx --args --python 3.13 --from "$SRC" hpc-bridge >/dev/null 2>&1 \
  || fail "hermes: mcp add"
ht=$(hermes mcp test hpc-bridge 2>&1)
say "$(head -8 <<<"$ht" | sed 's/^/    /')"
found=0
for t in ensure_endpoint_up authenticate complete_login complete_preauth list_facilities connect_facility stop_endpoint \
         teardown_endpoint login_shell run_shell poll_task reset_session; do
  grep -qE "\b$t\b" <<<"$ht" && found=$((found + 1))
done
[[ "$found" == "$EXPECT" ]] && pass "hermes: mcp test lists all $EXPECT tools" || fail "hermes: mcp test lists $found of $EXPECT tools"
sl=$(hermes skills list 2>&1)
grep -E "driving-hpc.*enabled" <<<"$sl" >/dev/null && pass "hermes: skill driving-hpc installed and enabled" || fail "hermes: skill driving-hpc not enabled"

# 5. Codex: add, then the settings an unattended or first run needs (30 s startup; approval for every tool call)
codex mcp add hpc-bridge -- "${CMD[@]}" >/dev/null 2>&1 || fail "codex: mcp add"
python3 - <<'PY'
import pathlib, re
p = pathlib.Path.home() / ".codex/config.toml"
t = p.read_text()
extra = 'startup_timeout_sec = 180\ntool_timeout_sec = 600\ndefault_tools_approval_mode = "approve"\n'
t = re.sub(r'(\[mcp_servers\.hpc-bridge\]\n(?:[^\[]*\n)?)', lambda m: m.group(1) + extra, t, count=1)
p.write_text(t)
PY
co=$(uvx --with 'mcp>=1.28,<2' python mcp_probe.py codex hpc-bridge 2>/dev/null | tail -1)
say "    $co"
nc=$(jq -r '.tools | length' <<<"$co" 2>/dev/null || echo 0)
[[ "$nc" == "$EXPECT" ]] && pass "codex: app-server reports $nc tools" || fail "codex: app-server reports $nc tools (want $EXPECT)"

say ""
[[ $fails -eq 0 ]] && say "RESULT: OK (ref $REF)" || say "RESULT: $fails step(s) failed (ref $REF)"
exit $fails
