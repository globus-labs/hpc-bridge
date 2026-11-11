# The README's hero recording (docs/media/demo.gif): a Claude Code session with hpc-bridge connects to a small Slurm
# test cluster (the fake cluster's `default` profile, ssh alias hpcb-fake), shows the terms of the block a batch needs,
# gets a yes, runs six rows two at a time, and releases the block.
#
# Run it through build.sh, never on its own: `scripts/readme-media/build.sh --record`. build.sh checks the cluster
# and the alias, seeds a throwaway Claude Code config (no first-run prompts, the demo folder trusted), and exports what
# this tape reads: HPCB_DEMO_DIR (the session's folder, outside the repo), HPCB_PLUGIN_DIR (this checkout),
# HPCB_ALLOW / HPCB_DENY (the tools the session may and may not use), HPC_BRIDGE_STATE_DIR (the demo's own state).
# It bills Claude usage, and needs the warm-up first (build.sh --warm): with the cluster's settings cached and its
# endpoint up, the connect here is a zero-SSH reattach that asks nothing, and the one question is the spend question.
#
# The long waits (Claude Code starting, the agent working, the block starting) are cut: hidden while they run, so
# the GIF holds only the shown stretches (about 28 s). They are fixed sleeps, not `Wait` regexes: VHS 0.12.1's
# `Wait+Screen` reads the first rows of the terminal's buffer, not the scrolled view, so once Claude Code's transcript
# scrolls it never sees new text (and under a full-screen program it reads a stale screen). A sleep only has to be
# long enough; a longer one costs wall-clock time, never GIF length, and a reply typed while the agent still works is
# queued by Claude Code, not lost. build.sh sets each one from HERO_WAIT_<NAME> (seconds): the `# wait:<name>` line
# above a Sleep names it.

Output hero.mp4

Require bash
Require claude
Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 740
Set Padding 18
Set Margin 22
Set MarginFill "#10233a"
Set WindowBar Colorful
Set WindowBarSize 34
Set BorderRadius 12
Set Framerate 24
Set TypingSpeed 30ms
Set Theme { "name": "hpc-bridge", "background": "#0d1722", "foreground": "#dce6f0", "cursor": "#56d0d6", "selection": "#2a3b4f", "black": "#1b2735", "red": "#ff7b72", "green": "#7ee787", "yellow": "#e3b341", "blue": "#79c0ff", "magenta": "#d2a8ff", "cyan": "#56d0d6", "white": "#c9d4df", "brightBlack": "#6b7a8b", "brightRed": "#ffa198", "brightGreen": "#aff5b4", "brightYellow": "#f8e3a1", "brightBlue": "#a5d6ff", "brightMagenta": "#e2c5ff", "brightCyan": "#a8eef0", "brightWhite": "#f0f6fc" }

# Off camera: the demo folder, a short prompt, and `claude` as this checkout's plugin with the tools build.sh allows.
Hide
Type `cd "$HPCB_DEMO_DIR" && claude() { command claude --plugin-dir "$HPCB_PLUGIN_DIR" --allowedTools "$HPCB_ALLOW" --disallowedTools "$HPCB_DENY" "$@"; } && PS1='\[\e[36m\]~/hpcb-batch-demo\[\e[0m\] \$ ' && clear`
Enter
Sleep 800ms
Show

# 1. Start Claude Code. Its start-up, and hpc-bridge's server connecting, are cut.
Sleep 700ms
Type "claude"
Sleep 300ms
Enter
Hide
# wait:start
Sleep 20s
Show
Sleep 1.2s

# 2. One request: connect, and a batch sized per row. The agent works (cut) until it has asked the spend question.
Type "Connect me to the cluster at hpcb-fake and run sha256sum on each of these six words as a batch, one core and 1 GB per row, two at a time: alpha bravo charlie delta echo foxtrot"
Sleep 400ms
Enter
Sleep 3s
Hide
# wait:ask
Sleep 150s
Show
Sleep 4.5s

# 3. The yes. The block starts, the rows run and the agent reports (cut).
Type "Yes, go ahead."
Sleep 300ms
Enter
Sleep 2.5s
Hide
# wait:results
Sleep 240s
Show
Sleep 5s

# 4. Release the block (cut until the agent has said so).
Type "Release the block."
Sleep 300ms
Enter
Sleep 2s
Hide
# wait:release
Sleep 60s
Show
Sleep 4s

# Off camera: leave Claude Code.
Hide
Ctrl+C
Sleep 500ms
Ctrl+C
Sleep 1s
