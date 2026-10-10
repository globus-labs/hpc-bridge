"""EDGE: output longer than a tool result can carry, and a result over Globus Compute's size limit. Fake cluster only.

Before 2026-10 the SDK's ShellFunction kept the last 1000 lines of a stream and said nothing, so `cat` of a longer file
came back as its tail, passing for the whole file. Now each stream the agent gets keeps its END with a marker saying
what was dropped and how to read the rest (redirect to a file, read ranges), and a result over Compute's 10 MiB limit
is reported as what it is: the command ran, its exit code is unknown. Login shape only — nothing billed.
"""
from invariants import Result, Trace

_MARK = "HPCB_LONGOUT"
_BIG = "HPCB_BIG"
_GEN = "python3 -c \"print('\\n'.join('" + _MARK + " line %d' % i for i in range(50000)))\""

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — connect (if it isn't catalogued, "
    "probe the login node and accept the discovered configuration yourself) and wait until the login node is up. Run "
    "every command with `run_shell(command=..., shape='login')` — not login_shell, and do NOT bring up a compute "
    "block. Then:\n\n"
    "1. Run EXACTLY this and report what you got back, including any note about the output being cut:\n\n"
    "    " + _GEN + "\n\n"
    "2. Read the whole output the way the result tells you to: run\n\n"
    "    " + _GEN + " > hpcb_longout.txt; wc -l hpcb_longout.txt\n\n"
    "   and then\n\n"
    "    sed -n '25001,25003p' hpcb_longout.txt\n\n"
    "   and report both answers.\n"
    "3. Run EXACTLY this and report exactly what hpc-bridge says about it:\n\n"
    "    python3 -c \"import sys; sys.stdout.write('" + _BIG + "' + 'x' * 12000000)\"\n\n"
    "This is an AUTOMATED run — no one is available to answer questions."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "edge: 50k lines come back as a marked tail and are read in ranges from a file; a >10 MiB result is reported honestly"
TAGS = ["fake", "output", "login"]
MAX_TURNS = 30
TARGETS = ("fake",)


def _runs(t: Trace, needle: str) -> list:
    """Shell calls (run_shell, or login_shell — the agent boundary cuts both) whose command contains `needle`."""
    return [c for _, c in sorted([*t.named("run_shell"), *t.named("login_shell")], key=lambda x: x[0])
            if needle in str(c.input.get("command", ""))]


def long_output_marked(t: Trace) -> Result:
    """The 50,000-line result keeps its END (the last line is there), opens with the cut marker, and carries the notice
    on how to read the rest — never a tail passing for the whole output."""
    runs = [c for c in _runs(t, _MARK) if ">" not in str(c.input.get("command", ""))]
    # a login_shell result has no phase; it ran when it carries an exit code
    done = [c for c in runs if str((c.result or {}).get("phase")) == "complete"
            or (c.name == "login_shell" and (c.result or {}).get("exit_code") is not None)]
    if not done:
        return Result("long_output_marked", False, "the 50k-line command never completed")
    r = done[0].result or {}
    out, notice = str(r.get("stdout", "")), str(r.get("notice", "")).lower()
    ok = (out.startswith("[hpc-bridge: stdout too long") and f"{_MARK} line 49999" in out
          and "file" in notice and len(out) < 20_000)
    return Result("long_output_marked", ok,
                  "ok: the end was kept, marked, and the notice says how to read it all" if ok
                  else f"stdout starts {out[:80]!r} ({len(out)} chars); notice {notice[:120]!r}")


def ranges_read(t: Trace) -> Result:
    """Reading the file in ranges works and comes back whole: the line count and the middle of the output."""
    outs = [str((c.result or {}).get("stdout", "")) for _, c in t.named("run_shell")
            if str((c.result or {}).get("phase")) == "complete"]
    counted = any("50000" in o and "hpcb_longout.txt" in o for o in outs)
    middle = any(f"{_MARK} line 25000" in o and "too long" not in o for o in outs)
    ok = counted and middle
    return Result("ranges_read", ok,
                  "ok: wc -l saw 50000 lines and sed returned the middle uncut" if ok
                  else f"line count seen: {counted}; middle lines seen: {middle}")


def oversize_reported(t: Trace) -> Result:
    """A result over Compute's limit is reported as what it is: failed, exit code unknown, the limit named, and the
    remedy — not a generic "task failed" with exit 1."""
    runs = _runs(t, _BIG)
    if not runs:
        return Result("oversize_reported", False, "the >10 MiB command was never run")
    r = runs[0].result or {}
    notice = str(r.get("notice", "")).lower()
    ok = (str(r.get("phase")) == "failed" and r.get("exit_code") is None
          and "limit" in notice and "file" in notice)
    return Result("oversize_reported", ok,
                  "ok: failed, exit code unknown, the limit and the remedy named" if ok
                  else f"phase={r.get('phase')!r} exit_code={r.get('exit_code')!r} notice={notice[:160]!r}")


EXTRA_INVARIANTS = [long_output_marked, ranges_read, oversize_reported]
EXPECT_OK = ["agent_engaged", "long_output_marked", "ranges_read", "oversize_reported", "spend_not_unprompted",
             "no_raw_ssh_after_endpoint_up"]
TEARDOWN = "delete"
