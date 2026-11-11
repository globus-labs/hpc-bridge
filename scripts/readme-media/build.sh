#!/usr/bin/env bash
# Remake the README's media.
#
#   scripts/readme-media/build.sh               the stills, from their HTML: docs/media/wordmark.png, wordmark-dark.png
#                                               and how-it-works.png (free: a headless Chromium, no network)
#   scripts/readme-media/build.sh --warm        BILLS CLAUDE USAGE. Runs warm.tape, unattended and not for the README:
#                                               a session connects to the fake cluster once, so its settings are cached
#                                               and its endpoint is up, and each take's connect then asks nothing
#   scripts/readme-media/build.sh --record      BILLS CLAUDE USAGE. Records hero.tape (a real session on the fake
#                                               cluster), then makes docs/media/demo.gif from it
#   scripts/readme-media/build.sh --gif <mp4>   only the last step: docs/media/demo.gif from a take already recorded
#
# The stills need Node and Playwright with its Chromium. Playwright is not a dependency of hpc-bridge: point NODE_PATH
# at any node_modules that has it. They are set in macOS's system fonts; a render elsewhere sets other faces.
#
# --warm and --record run Claude Code as YOU, with your own ~/.claude (its login, settings and trusted folders; the
# demo folder must be trusted, as try_batch.sh's first --launch leaves it). They never read a token: CLAUDE_CODE_OAUTH_TOKEN
# and ANTHROPIC_API_KEY are unset for the session, so it bills your Claude Code plan. Nothing under ~/.claude is edited.
# Per session only (`--settings`, a file in the take's folder): every plugin your settings enable is turned off,
# the installed hpc-bridge (hpc-bridge@hpc-bridge) included, so the one hpc-bridge loaded is this checkout
# (`--plugin-dir`, which Claude Code names hpc-bridge@inline). hpc-bridge's tools are allowed; AskUserQuestion, Bash
# and the editing and web tools are denied, so every question is plain text and nothing waits on a permission prompt.
# HERO_OWN_CONFIG=1 names this mode; it is the only one.
#
# They also need: the fake cluster up with its `default` profile and the ssh alias hpcb-fake (both from
# agentic/fakecluster/bin/try_batch.sh, which this script never starts or stops), a Globus login on this machine
# (~/.globus_compute), and claude, vhs and ffmpeg on PATH. Each session runs in HPCB_DEMO_DIR (default
# ~/hpcb-batch-demo, outside the repo, so the repo's .claude/settings.local.json, which disables the dev server, does
# not apply) with its own hpc-bridge state (<demo>/state: your ~/.hpc-bridge is never touched).
#
# Knobs, in seconds unless said:
#   WARM_WAIT_START, WARM_WAIT_ASK, WARM_WAIT_UP         warm.tape's waits (defaults 20, 120, 420)
#   HERO_WAIT_START, HERO_WAIT_ASK, HERO_WAIT_RESULTS, HERO_WAIT_RELEASE
#                                                        hero.tape's hidden waits (defaults 20, 150, 240, 60): only long
#                                                        enough matters, they're cut from the GIF
#   HERO_SPEED   play the take this many times faster in the GIF (default 1)
#   HERO_FPS     the GIF's frame rate (default 12)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
MEDIA="$REPO/docs/media"
export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

DEMO="${HPCB_DEMO_DIR:-$HOME/hpcb-batch-demo}"
STATE="$DEMO/state"   # keep it short: ssh's ControlPath (<state>/cm/<40 hex>) must fit ~104 bytes
TAKES="$DEMO/recordings"
ALLOW="mcp__plugin_hpc-bridge_endpoint,mcp__plugin_hpc-bridge_endpoint__*,Skill"
DENY="AskUserQuestion,Bash,Edit,Write,NotebookEdit,WebFetch,WebSearch"

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
  command -v vhs >/dev/null || die "vhs is not on PATH"
  command -v uv >/dev/null || die "uv is not on PATH (the plugin runs its server with uv)"
  command -v docker >/dev/null && docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^hpcb-fake-login-1$' \
    || die "the fake cluster isn't up: run agentic/fakecluster/bin/try_batch.sh (profile default) first"
  ssh -o BatchMode=yes -o ConnectTimeout=10 hpcb-fake true 2>/dev/null \
    || die "ssh hpcb-fake failed: try_batch.sh adds the alias to ~/.ssh/config and checks it"
  [ -f "$HOME/.globus_compute/storage.db" ] \
    || die "no Globus login on this machine (~/.globus_compute/storage.db): log in once in an ordinary session first"
  mkdir -p "$DEMO" "$STATE" "$TAKES"
  local demo; demo="$(cd "$DEMO" && pwd -P)"
  python3 - "$demo" <<'PY' || echo "build.sh: $DEMO may not be a trusted folder in ~/.claude.json: the tape would type into the trust prompt. Open claude there once and trust it." >&2
import json, os, sys
d = json.load(open(os.path.expanduser("~/.claude.json")))
sys.exit(0 if d.get("projects", {}).get(sys.argv[1], {}).get("hasTrustDialogAccepted") else 1)
PY
  # The server's environment is resolved once, before the take, so its first start fits the start-up wait.
  uv sync --quiet --locked --extra integration --directory "$REPO"
}

session_env() {  # $1: the take's folder. Exports what the tapes read; edits nothing under ~/.claude.
  # Your own hpc-bridge settings stay out (an HPC_BRIDGE_SSH_HOST or endpoint id would bypass the demo's facility),
  # and so do tokens: the session logs in as your ~/.claude does.
  for v in $(env | grep -oE '^HPC_BRIDGE_[A-Z_]+' || true); do unset "$v"; done
  unset CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_API_KEY CLAUDE_CONFIG_DIR
  # The per-session settings: every plugin your settings enable is off for this session, and the installed
  # hpc-bridge whether or not they name it (a project's settings may), so only --plugin-dir's hpc-bridge loads.
  python3 - "$1/session-settings.json" <<'PY'
import json, os, sys
enabled = {}
for f in ("~/.claude/settings.json", "~/.claude/settings.local.json"):
    try:
        enabled.update(json.load(open(os.path.expanduser(f))).get("enabledPlugins") or {})
    except Exception:
        pass
off = {name: False for name in enabled}
off["hpc-bridge@hpc-bridge"] = False
json.dump({"enabledPlugins": off, "spinnerTipsEnabled": False}, open(sys.argv[1], "w"), indent=1)
print("plugins off for the session:", ", ".join(sorted(off)))
PY
  export HPC_BRIDGE_STATE_DIR="$STATE" HPCB_DEMO_DIR="$DEMO" HPCB_PLUGIN_DIR="$REPO"
  export HPCB_SETTINGS="$1/session-settings.json" HPCB_ALLOW="$ALLOW" HPCB_DENY="$DENY" DISABLE_AUTOUPDATER=1
}

with_waits() {  # $1: the tape, $2: the env prefix (HERO_WAIT_ or WARM_WAIT_): each `# wait:<name>` Sleep from the env
  awk -v prefix="$2" '
    /^# wait:[a-z]+$/ { name = toupper(substr($0, 8)); print; next }
    name != "" && /^Sleep / { v = ENVIRON[prefix name]; if (v != "") $0 = "Sleep " v "s"; name = "" }
    { print }
  ' "$1"
}

warm() {
  preflight
  local take; take="$TAKES/warm-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$take"
  session_env "$take"
  with_waits "$HERE/warm.tape" WARM_WAIT_ > "$take/warm.tape"
  echo "== Warm-up (bills Claude usage; about ten minutes, unattended) in $take"
  (cd "$take" && vhs warm.tape)
  if grep -q 'hpcb-fake' "$STATE/facilities.json" 2>/dev/null; then
    echo "The cluster's settings are cached in $STATE. Next: $0 --record"
  else
    die "no settings for hpcb-fake cached in $STATE: watch $take/warm.mp4 for why; a longer WARM_WAIT_UP if the bring-up was still going"
  fi
}

record() {
  command -v ffmpeg >/dev/null || die "ffmpeg is not on PATH"
  preflight
  grep -q 'hpcb-fake' "$STATE/facilities.json" 2>/dev/null \
    || die "no settings for hpcb-fake cached in $STATE: run $0 --warm first"
  local take; take="$TAKES/hero-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$take"
  session_env "$take"
  with_waits "$HERE/hero.tape" HERO_WAIT_ > "$take/hero.tape"
  echo "== Recording (bills Claude usage; about eight minutes, about 28 s of GIF) in $take"
  (cd "$take" && vhs hero.tape)
  echo "The take: $take/hero.mp4. Look at it before you keep the GIF."
  gif "$take/hero.mp4"
}

case "${1:-}" in
  "") stills ;;
  --warm) warm ;;
  --record) record ;;
  --gif) [ -n "${2:-}" ] || die "--gif needs the take: build.sh --gif <hero.mp4>"; gif "$2" ;;
  -h|--help) awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0" ;;
  *) die "unknown argument: $1 (try --help)" ;;
esac
