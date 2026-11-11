#!/usr/bin/env bash
# Remake the README's media.
#
#   scripts/readme-media/build.sh               the stills, from their HTML: docs/media/wordmark.png, wordmark-dark.png
#                                               and how-it-works.png (free: a headless Chromium, no network)
#   scripts/readme-media/build.sh --warm        BILLS CLAUDE USAGE. An interactive session, before the first take: you
#                                               connect it to the fake cluster once, so its settings are cached and its
#                                               endpoint is up, and each take's connect is a zero-SSH reattach
#   scripts/readme-media/build.sh --record      BILLS CLAUDE USAGE. Records hero.tape (a real session on the fake
#                                               cluster), then makes docs/media/demo.gif from it
#   scripts/readme-media/build.sh --gif <mp4>   only the last step: docs/media/demo.gif from a take already recorded
#
# The stills need Node and Playwright with its Chromium. Playwright is not a dependency of hpc-bridge: point NODE_PATH
# at any node_modules that has it. They are set in macOS's system fonts; a render elsewhere sets other faces.
#
# --warm and --record need: the fake cluster up with its `default` profile and the ssh alias hpcb-fake (both from
# agentic/fakecluster/bin/try_batch.sh, which this script never starts or stops), a Globus login on this machine
# (~/.globus_compute, shared with your other sessions), and claude, vhs and ffmpeg on PATH. Each session runs in
# HPCB_DEMO_DIR (default ~/hpcb-batch-demo, outside the repo, so the repo's .claude/settings.local.json, which disables
# the dev server, does not apply) with its own hpc-bridge state (<demo>/state: your ~/.hpc-bridge is never touched),
# this checkout as the plugin, and a throwaway Claude Code config seeded so nothing asks first (onboarding done, the
# demo folder trusted, hpc-bridge's tools allowed), authenticated by CLAUDE_CODE_OAUTH_TOKEN (from the environment, or
# the main checkout's agentic/.env). HERO_OWN_CONFIG=1 uses your own ~/.claude instead: then disable a marketplace
# install of hpc-bridge for the session (/plugin), so only this checkout's tools load.
#
# Knobs (--record, --gif):
#   HERO_WAIT_START, HERO_WAIT_ASK, HERO_WAIT_RESULTS, HERO_WAIT_RELEASE   seconds for hero.tape's hidden waits
#                                    (defaults in the tape: 20, 150, 240, 60): only long enough matters, they're cut
#   HERO_SPEED   play the take this many times faster in the GIF (default 1)
#   HERO_FPS     the GIF's frame rate (default 12)
#   HERO_FULLSCREEN=1   Claude Code's flicker-free full-screen renderer (CLAUDE_CODE_NO_FLICKER=1), if the classic one
#                       flickers in the take
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
MEDIA="$REPO/docs/media"
export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

DEMO="${HPCB_DEMO_DIR:-$HOME/hpcb-batch-demo}"
STATE="$DEMO/state"   # keep it short: ssh's ControlPath (<state>/cm/<40 hex>) must fit ~104 bytes
TAKES="$DEMO/recordings"
ALLOW="mcp__plugin_hpc-bridge_endpoint,mcp__plugin_hpc-bridge_endpoint__*,Skill"
DENY_RECORD="AskUserQuestion,Bash,Edit,Write,NotebookEdit,WebFetch,WebSearch"   # one plain-text question; no prompts
DENY_WARM="Bash,Edit,Write,NotebookEdit,WebFetch,WebSearch"
WARM_PROMPT="Connect me to the cluster at hpcb-fake. Use the settings you discover, bring the endpoint up, and stop there: don't start a compute block."

die() { echo "build.sh: $*" >&2; exit 1; }

stills() {
  command -v node >/dev/null || die "node is not on PATH"
  node -e 'try { require("playwright") } catch { require("@playwright/test") }' 2>/dev/null \
    || die "Playwright not found: set NODE_PATH to a node_modules that has \`playwright\` (with its Chromium installed)"
  node "$HERE/render.mjs" marks "$MEDIA/wordmark.html" "$MEDIA"
  node "$HERE/render.mjs" figure "$MEDIA/how-it-works.html" "$MEDIA/how-it-works.png"
}

gif() {  # $1: the take (mp4) → docs/media/demo.gif
  local mp4="$1" out="$MEDIA/demo.gif" speed="${HERO_SPEED:-1}" fps="${HERO_FPS:-12}"
  [ -f "$mp4" ] || die "no take at $mp4"
  command -v ffmpeg >/dev/null || die "ffmpeg is not on PATH"
  # One palette from the whole take, undithered: text stays crisp, and a frame that doesn't change costs nothing.
  ffmpeg -v error -y -i "$mp4" -filter_complex \
    "[0:v]setpts=PTS/${speed},fps=${fps},split[a][b];[a]palettegen=max_colors=128:stats_mode=diff[p];[b][p]paletteuse=dither=none:diff_mode=rectangle" \
    -loop 0 "$out"
  local secs bytes
  secs="$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$out")"
  bytes="$(wc -c < "$out" | tr -d ' ')"
  printf 'made %s: %.1f s, %.1f MB\n' "$out" "$secs" "$(echo "$bytes / 1000000" | bc -l)"
  [ "$bytes" -le 5000000 ] || echo "  over 5 MB: lower HERO_FPS, or raise HERO_SPEED, and run --gif $mp4 again"
  awk -v s="$secs" 'BEGIN { exit !(s > 32) }' && echo "  over 32 s: shorten the shown stretches in hero.tape, or raise HERO_SPEED" || true
}

preflight() {  # what --warm and --record need, checked, never started
  command -v claude >/dev/null || die "the claude CLI is not on PATH"
  command -v uv >/dev/null || die "uv is not on PATH (the plugin runs its server with uv)"
  command -v docker >/dev/null && docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^hpcb-fake-login-1$' \
    || die "the fake cluster isn't up: run agentic/fakecluster/bin/try_batch.sh (profile default) first"
  ssh -o BatchMode=yes -o ConnectTimeout=10 hpcb-fake true 2>/dev/null \
    || die "ssh hpcb-fake failed: try_batch.sh adds the alias to ~/.ssh/config and checks it"
  mkdir -p "$DEMO" "$STATE" "$TAKES"
  # The server's environment is resolved once, before the take, so its first start fits the start-up wait.
  uv sync --quiet --locked --extra integration --directory "$REPO"
}

session_env() {  # $1: the tools to deny. Exports what the session (and hero.tape) reads.
  # Your own hpc-bridge settings stay out (an HPC_BRIDGE_SSH_HOST or endpoint id would bypass the demo's facility).
  for v in $(env | grep -oE '^HPC_BRIDGE_[A-Z_]+' || true); do unset "$v"; done
  export HPC_BRIDGE_STATE_DIR="$STATE" HPCB_DEMO_DIR="$DEMO" HPCB_PLUGIN_DIR="$REPO" HPCB_ALLOW="$ALLOW" HPCB_DENY="$1"
  export DISABLE_AUTOUPDATER=1 CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
  [ "${HERO_FULLSCREEN:-}" = 1 ] && export CLAUDE_CODE_NO_FLICKER=1
  [ "${HERO_OWN_CONFIG:-}" = 1 ] && return 0
  if [ -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" ]; then
    local main env_file
    main="$(cd "$(git -C "$REPO" rev-parse --path-format=absolute --git-common-dir)/.." && pwd)"
    env_file="$main/agentic/.env"
    [ -f "$env_file" ] && CLAUDE_CODE_OAUTH_TOKEN="$(grep '^CLAUDE_CODE_OAUTH_TOKEN=' "$env_file" | head -1 | cut -d= -f2- || true)"
    [ -n "${CLAUDE_CODE_OAUTH_TOKEN:-}" ] \
      || die "no CLAUDE_CODE_OAUTH_TOKEN (environment, or $env_file); mint one with \`claude setup-token\`, or set HERO_OWN_CONFIG=1"
  fi
  export CLAUDE_CODE_OAUTH_TOKEN
  CFG="$(mktemp -d "${TMPDIR:-/tmp}/hpcb-hero-claude.XXXXXX")"
  trap 'rm -rf "$CFG"' EXIT
  export CLAUDE_CONFIG_DIR="$CFG"
  # Onboarding done, the release notes seen (the versions copied from your own config, nothing else), the demo
  # folder trusted, and the tools allowed, so the session opens on the prompt and nothing asks before the agent does.
  python3 - "$CFG" "$(cd "$DEMO" && pwd -P)" "$ALLOW" "$1" <<'PY'
import json, os, sys
cfg, demo, allow, deny = sys.argv[1:]
own = {}
try:
    own = json.load(open(os.path.expanduser("~/.claude.json")))
except Exception:
    pass
seed = {"hasCompletedOnboarding": True, "theme": "dark", "projects": {demo: {"hasTrustDialogAccepted": True, "allowedTools": []}}}
for k in ("lastOnboardingVersion", "lastReleaseNotesSeen"):
    if k in own:
        seed[k] = own[k]
json.dump(seed, open(os.path.join(cfg, ".claude.json"), "w"), indent=1)
settings = {"permissions": {"allow": allow.split(","), "deny": deny.split(",")}, "spinnerTipsEnabled": False}
json.dump(settings, open(os.path.join(cfg, "settings.json"), "w"), indent=1)
PY
}

warm() {
  preflight
  session_env "$DENY_WARM"
  cat <<EOF
== A warm-up session (bills Claude usage). Paste this, answer its questions, and quit (/exit) once the endpoint is up:

  $WARM_PROMPT

EOF
  cd "$DEMO"
  command claude --plugin-dir "$REPO" --allowedTools "$ALLOW" --disallowedTools "$DENY_WARM"
  if grep -q 'hpcb-fake' "$STATE/facilities.json" 2>/dev/null; then
    echo "The cluster's settings are cached in $STATE. Next: $0 --record"
  else
    echo "No settings for hpcb-fake cached in $STATE yet: the take's connect would ask about them. Run --warm again." >&2
  fi
}

record() {
  command -v vhs >/dev/null || die "vhs is not on PATH"
  command -v ffmpeg >/dev/null || die "ffmpeg is not on PATH"
  preflight
  grep -q 'hpcb-fake' "$STATE/facilities.json" 2>/dev/null \
    || die "no settings for hpcb-fake cached in $STATE: run $0 --warm first"
  session_env "$DENY_RECORD"
  local take="$TAKES/hero-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$take"
  # The tape with each `# wait:<name>` Sleep set from HERO_WAIT_<NAME>, where given.
  awk '
    /^# wait:[a-z]+$/ { name = toupper(substr($0, 8)); print; next }
    name != "" && /^Sleep / { v = ENVIRON["HERO_WAIT_" name]; if (v != "") $0 = "Sleep " v "s"; name = "" }
    { print }
  ' "$HERE/hero.tape" > "$take/hero.tape"
  echo "== Recording (bills Claude usage; several minutes of wall-clock time, about 28 s of GIF) into $take"
  (cd "$take" && vhs hero.tape)
  echo "The take: $take/hero.mp4. Look at it before you keep the GIF."
  gif "$take/hero.mp4"
}

case "${1:-}" in
  "") stills ;;
  --warm) warm ;;
  --record) record ;;
  --gif) [ -n "${2:-}" ] || die "--gif needs the take: build.sh --gif <hero.mp4>"; gif "$2" ;;
  -h|--help) sed -n '2,37p' "$0" ;;
  *) die "unknown argument: $1 (try --help)" ;;
esac
