from dataclasses import dataclass

import pytest

from hpc_bridge.dispatch import complete_outcome, execute, failure_outcome, for_agent
from hpc_bridge.models import LoginShellResult, ShellOutcome
from hpc_bridge.runner import MAX_OUTPUT_CHARS, SNIPPET_LINES


@dataclass
class FakeResult:
    returncode: int
    stdout: str
    stderr: str


class FakeRunner:
    def __init__(self, result):
        self.result = result
        self.commands = []

    async def run(self, command):
        self.commands.append(command)
        return self.result


async def test_execute_builds_complete_outcome():
    runner = FakeRunner(FakeResult(0, "hi\n", ""))
    out = await execute("echo hi", runner)
    assert out.phase == "complete"
    assert out.exit_code == 0
    assert out.stdout == "hi\n"
    assert out.block_state == "warm"
    assert runner.commands == ["echo hi"]


async def test_execute_preserves_nonzero_exit_and_stderr():
    runner = FakeRunner(FakeResult(2, "", "boom\n"))
    out = await execute("false", runner)
    assert out.exit_code == 2
    assert out.stderr_snippet == "boom\n"


def _numbered(n):
    return "".join(f"line {i}\n" for i in range(1, n + 1))


async def test_complete_outcome_keeps_the_streams_whole_for_internal_callers():
    # hpc-bridge's own consumers (the pilot probe, the allocation parsers) read the result uncut; only the tool
    # boundary (for_agent) cuts what the agent sees
    big = _numbered(20_000)
    out = await execute("cat big", FakeRunner(FakeResult(0, big, big)))
    assert out.stdout == big and out.stderr_snippet == big and out.notice is None


def test_for_agent_long_stdout_keeps_its_end_with_a_marker_and_a_notice():
    raw = ShellOutcome(phase="complete", exit_code=0, stdout=_numbered(2000), block_state="warm")
    out = for_agent(raw, 105)
    marker, kept = out.stdout.split("\n", 1)
    assert marker == ("[hpc-bridge: stdout too long — showing only its last 10 lines (100 chars); "
                      "1,990 lines (18,793 chars) before them were dropped]")
    assert kept == "".join(f"line {i}\n" for i in range(1991, 2001))  # the true end, not the start
    assert out.stderr_snippet == "" and out.exit_code == 0
    assert out.notice and out.notice.startswith("stdout too long for one result")
    assert "> out.log 2>&1" in out.notice  # the remedy: a file on the facility, read in ranges
    assert raw.stdout == _numbered(2000)  # the internal result itself is untouched


def test_for_agent_long_stderr_keeps_its_end_with_a_marker_and_a_notice():
    raw = ShellOutcome(phase="complete", exit_code=2, stdout="ok\n", stderr_snippet=_numbered(2000), block_state="warm")
    out = for_agent(raw, 105)  # a long build log whose error is on the last line
    assert out.exit_code == 2 and out.stdout == "ok\n"
    assert out.stderr_snippet.startswith("[hpc-bridge: stderr too long — showing only its last 10 lines")
    assert out.stderr_snippet.endswith("line 2000\n")
    assert out.notice and out.notice.startswith("stderr too long for one result")


def test_for_agent_leaves_output_within_the_cap_unchanged():
    raw = ShellOutcome(phase="complete", exit_code=0, stdout=_numbered(1500), stderr_snippet="w\n", block_state="warm")
    assert for_agent(raw) is raw and raw.notice is None  # 1,500 lines: past the SDK's old 1,000, within the cap


def test_for_agent_leaves_a_failure_alone():
    # a failed outcome's text is an exception's, not command output: no tail cut, no "redirect cmd" remedy — even
    # past the command-output cap
    raw = ShellOutcome(phase="failed", exit_code=1, stderr_snippet=_numbered(3000), block_state="warm",
                       notice="The remote task failed to execute.")
    assert len(raw.stderr_snippet) > MAX_OUTPUT_CHARS
    assert for_agent(raw) is raw


def test_an_appended_cut_notice_starts_a_new_sentence():
    raw = ShellOutcome(phase="complete", exit_code=0, stdout=_numbered(2000), block_state="warm", notice="Done.")
    assert (for_agent(raw, 105).notice or "").startswith("Done. Stdout too long for one result")


def test_exception_text_keeps_its_head_where_the_diagnosis_is():
    # the SDK's own deserialization failure carries the whole serialized result after its one explanatory line
    blob = "QUJD" * 50_000
    out = failure_outcome(Exception("Malformed or unexpected data structure. Data: " + blob), "warm")
    assert out.stderr_snippet.startswith("Malformed or unexpected data structure. Data: QUJD")
    assert f"\n[hpc-bridge: error text cut — {200_046 - 4_000:,} chars from its middle]\n" in out.stderr_snippet
    assert len(out.stderr_snippet) < 4_100
    assert for_agent(out) is out and out.notice == "Dispatch error: Exception"  # no command-output remedy
    short = failure_outcome(RuntimeError("kaboom"), "warm")
    assert short.stderr_snippet == "kaboom"  # within the bound: untouched


def _chained_remote_traceback(depth):
    """What the endpoint sends for a chained error (traceback.format_exception): `depth` distinct frames of a dill
    load, then the wrapping DeserializationError — the line that says what happened comes LAST."""
    import traceback

    from globus_compute_sdk.errors import DeserializationError

    # each frame its own site-packages file (3.13 collapses repeated frames); exec of our own literal source only
    ns = {}
    exec(compile("def f0():\n    raise TypeError('code expected at most 16 arguments, got 18')\n",  # noqa: S102
                 "/lus/proj/venv/lib/python3.11/site-packages/dill/_dill.py", "exec"), ns)
    for i in range(1, depth):
        exec(compile(f"def f{i}():\n    return f{i - 1}()  # obj = self.load_reduce_or_build(frame)\n",  # noqa: S102
                     f"/lus/proj/venv/lib/python3.11/site-packages/pickle_mod{i}.py", "exec"), ns)
    try:
        try:
            ns[f"f{depth - 1}"]()
        except TypeError as e:
            raise DeserializationError("Deserialization failed: dill loads failed") from e
    except Exception as exc:  # noqa: BLE001 - render it as the endpoint does
        return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


@pytest.mark.parametrize("depth", [40, 120])
def test_a_long_remote_traceback_keeps_its_head_and_its_final_exception(depth):
    from globus_compute_sdk.errors import TaskExecutionFailed

    remote = _chained_remote_traceback(depth)
    exc = TaskExecutionFailed(remote, "0")
    assert len(str(exc)) > 4_000  # past the bound: the SDK's serialization help is appended at the END
    out = failure_outcome(exc, "warm")
    s = out.stderr_snippet
    assert out.notice == "The remote task failed to execute." and out.exit_code == 1
    assert s.startswith(str(exc)[:200])  # the head: where the traceback starts
    assert remote.rstrip().splitlines()[-1].strip() in s  # the final exception line: what actually failed
    assert "AllCodeStrategies" in s  # the SDK's serialization help, appended after it
    assert "chars from its middle]" in s and len(s) < 4_100


def test_an_oversize_error_text_is_bounded_too():
    from globus_compute_sdk.errors import TaskExecutionFailed

    remote = "x" * 6_000 + "\nMaxResultSizeExceeded(12000000, 10485760)"  # a long rendering, the verdict last
    out = failure_outcome(TaskExecutionFailed(remote, "0"), "warm")
    assert out.exit_code is None and "chars from its middle]" in out.stderr_snippet
    assert out.stderr_snippet.endswith("MaxResultSizeExceeded(12000000, 10485760)") and len(out.stderr_snippet) < 4_100


def test_for_agent_cuts_a_login_shell_result_without_the_sdk_caveat():
    out = for_agent(LoginShellResult(exit_code=0, stdout=_numbered(SNIPPET_LINES)), 105)
    assert out.stdout.startswith("[hpc-bridge: stdout too long") and "at least" not in out.stdout  # SSH: no SDK cut
    assert out.notice and out.notice.startswith("stdout too long for one result")


def test_real_shellfunction_output_reaches_the_agent_whole_or_as_its_marked_end(tmp_path, monkeypatch):
    """End to end through the SDK's real ShellFunction, built the way GlobusRunner builds it and run locally.
    1,500 lines used to come back as their last 1,000 with nothing said (the SDK's default snippet_lines)."""
    pytest.importorskip("globus_compute_sdk")
    from hpc_bridge.runner import GlobusRunner

    class _Capture:
        def __init__(self):
            self.fns = []

        def submit(self, fn):
            self.fns.append(fn)

    monkeypatch.chdir(tmp_path)  # the SDK chdirs into the sandbox dir; monkeypatch restores the cwd afterwards
    monkeypatch.setenv("GC_TASK_SANDBOX_DIR", str(tmp_path))
    ex = _Capture()
    runner = GlobusRunner("eid", executor_factory=lambda: ex)

    def run(cmd):
        runner.submit(cmd)
        return for_agent(complete_outcome(ex.fns[-1](), "warm"))

    whole = run("seq 1 1500")
    assert whole.stdout == "".join(f"{i}\n" for i in range(1, 1501)) and whole.notice is None

    # short lines: the SDK's line limit binds first (12,006 chars < the char cap) — marked, count unknown
    short = run("seq 1 40000; echo done >&2")
    marker, kept = short.stdout.split("\n", 1)
    assert kept == "".join(f"{i}\n" for i in range(38_000, 40_001))  # the true last 2,001 lines
    assert marker == ("[hpc-bridge: stdout too long — showing only its last 2,001 lines (12,006 chars); the worker "
                      "returns at most 2,001 lines, so earlier ones may have been dropped (how many is unknown)]")
    assert short.stderr_snippet == "done\n"

    # 20-char lines: both cuts bind — the char cap keeps the true end of the SDK's tail, counted "at least"
    wide = run("for i in $(seq 1 5000); do printf '%019d\\n' $i; done")
    marker, kept = wide.stdout.split("\n", 1)
    assert kept == "".join(f"{i:019d}\n" for i in range(4201, 5001))  # 16,000 chars = the last 800 lines
    assert marker == ("[hpc-bridge: stdout too long — showing only its last 800 lines (16,000 chars); "
                      "at least 1,201 lines (at least 24,020 chars) before them were dropped]")
    assert wide.notice and wide.notice.startswith("stdout too long for one result")


class RaisingRunner:
    def __init__(self, exc):
        self.exc = exc

    async def run(self, command, timeout=None):
        raise self.exc


async def test_execute_translates_timeout_to_structured_failure():
    out = await execute("sleep 999", RaisingRunner(TimeoutError()))
    assert out.phase == "failed"
    assert out.exit_code == 124
    assert "ensure_endpoint_up" in (out.notice or "")


async def test_execute_translates_generic_exception_to_failure():
    out = await execute("boom", RaisingRunner(RuntimeError("kaboom")))
    assert out.phase == "failed"
    assert "kaboom" in (out.stderr_snippet or "")


class _NamedError(Exception):
    pass


@pytest.mark.parametrize("remote", [
    "MaxResultSizeExceeded(12000000, 10485760)",  # what the endpoint sends: repr() of an internal error class
    "Traceback (most recent call last):\n  ...\nglobus_compute_sdk.errors.error_types.MaxResultSizeExceeded: "
    "Task result of 12000000B exceeded current limit of 10485760B\n",  # a traceback rendering of the same
    "Task result of 12000000B exceeded current limit of 10485760B",  # the error's message alone (its str())
])
async def test_a_result_over_computes_limit_is_reported_as_run_with_its_exit_code_lost(remote):
    # Client side the SDK wraps the endpoint's error in TaskExecutionFailed — it never raises MaxResultSizeExceeded
    from globus_compute_sdk.errors import TaskExecutionFailed

    out = await execute("cat huge", RaisingRunner(TaskExecutionFailed(remote)))
    assert out.phase == "failed"
    assert out.exit_code is None  # unknown — not a made-up 1
    n = out.notice or ""
    assert n.startswith("The command RAN, but its result (12,000,000 bytes serialized; the limit is 10,485,760)")
    assert "not its exit code" in n and "> out.log 2>&1" in n and "sed -n" in n
    assert "safe to run twice" in n  # check for its side effects before re-running it
    assert "12000000" in out.stderr_snippet  # the error itself, as it came
    assert out._worker_answered  # the worker ran it: a liveness proof (warmth must not void the block)


async def test_the_sdks_own_result_size_error_is_recognised_too():
    from globus_compute_sdk.errors import MaxResultSizeExceeded

    out = await execute("cat huge", RaisingRunner(MaxResultSizeExceeded(12_000_000, 10_485_760)))
    assert out.exit_code is None and (out.notice or "").startswith("The command RAN, but its result (12,000,000 b")


async def test_a_result_just_over_the_limit_reads_as_over_it():
    from globus_compute_sdk.errors import TaskExecutionFailed

    out = await execute("cat huge", RaisingRunner(TaskExecutionFailed("MaxResultSizeExceeded(10485800, 10485760)")))
    assert "(10,485,800 bytes serialized; the limit is 10,485,760)" in (out.notice or "")


async def test_other_remote_failures_stay_generic():
    from globus_compute_sdk.errors import TaskExecutionFailed

    out = await execute("x", RaisingRunner(TaskExecutionFailed("ZeroDivisionError: division by zero")))
    assert out.notice == "The remote task failed to execute." and out.exit_code == 1
    assert not out._worker_answered
