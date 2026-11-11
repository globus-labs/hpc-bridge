# The warm-up before hero.tape's takes: unattended, and not for the README. A Claude Code session connects to the fake
# cluster (ssh alias hpcb-fake) once, confirms the settings it discovers, and brings the endpoint up with no compute
# block, so hpc-bridge caches the cluster's settings in the demo's state and leaves its endpoint online. Every take's
# connect is then a zero-SSH reattach that asks nothing.
#
# Run it through build.sh: `scripts/readme-media/build.sh --warm`, which exports what hero.tape's header lists and
# checks afterwards that the settings were cached. It bills Claude usage. warm.mp4 (4 frames a second) is kept in the
# warm-up's folder, to see what happened if the cache is missing afterwards.
#
# The waits are fixed sleeps (hero.tape's header says why VHS's Wait can't be used): long enough is all that matters.
# A "yes" typed when the agent asked nothing is harmless (it is queued, and answered); if the bring-up outlasts
# WARM_WAIT_UP, /exit ends the session before the settings are cached, and build.sh says so: set it longer.

Output warm.mp4

Require bash
Require claude
Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 740
Set Framerate 4
Set TypingSpeed 15ms

Type `cd "$HPCB_DEMO_DIR" && claude() { command claude --plugin-dir "$HPCB_PLUGIN_DIR" --settings "$HPCB_SETTINGS" --allowedTools "$HPCB_ALLOW" --disallowedTools "$HPCB_DENY" "$@"; } && clear`
Enter
Sleep 1s
Type "claude"
Enter
# wait:start
Sleep 20s

Type "Connect me to the cluster at hpcb-fake. Use the settings you discover, bring the endpoint up, and stop there: don't start a compute block."
Enter
# wait:ask
Sleep 120s

Type "Yes, use the settings you discovered, and bring the endpoint up."
Enter
# wait:up
Sleep 420s

Type "/exit"
Sleep 1s
Enter
Sleep 5s
