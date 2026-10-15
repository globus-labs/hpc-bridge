"""Hermetic: the ALCF probe run_smoke.sh runs before a free operator's cell (alcf_probe.py) — no network: the HTTP layer,
the clock and the sleep are stubbed — and run_smoke.sh's wiring of it (stub `uv` and `docker` on PATH)."""
from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import urllib.error
from pathlib import Path

import alcf_probe
import pytest
from infra import INFRA_RC, PREFLIGHT_ROW

HERE = Path(__file__).resolve().parent
SECRET = "alcf-" + "s3cr3t" * 6   # a stand-in bearer: it must never be printed, written or put in argv
BASE = "https://inference.example/v1"


class _Resp:
    """A urlopen() answer: a context manager, iterable by line (a stream) or read() whole."""

    def __init__(self, lines=(), body=b"", status=200, stall_after=None):
        self.lines, self.body, self.status, self.stall_after = list(lines), body, status, stall_after

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def __iter__(self):
        for n, line in enumerate(self.lines):
            if self.stall_after is not None and n >= self.stall_after:
                raise TimeoutError("timed out")
            yield line

    def read(self):
        return self.body


def _sse(*objs):
    return [f"data: {o if isinstance(o, str) else json.dumps(o)}\n".encode() for o in objs] + [b"\n"]


def _chunk(content=None, finish=None):
    return {"choices": [{"index": 0, "delta": {"content": content} if content else {}, "finish_reason": finish}]}


def _probe(resp=None, *, stream=True, raises=None):
    seen = {}

    def opener(req, timeout):
        seen["req"] = req
        if raises is not None:
            raise raises
        return resp

    ok, detail = alcf_probe.probe(BASE, SECRET, "openai/gpt-oss-120b", stream=stream, timeout_s=5, opener=opener)
    assert SECRET not in detail
    return ok, detail, seen.get("req")


def test_a_healthy_stream():
    ok, detail, req = _probe(_Resp(_sse(_chunk("o"), _chunk("k"), _chunk(finish="stop"), "[DONE]")))
    assert ok and detail.startswith("finish_reason=stop")
    assert req.full_url == BASE + "/chat/completions" and req.get_method() == "POST"
    assert json.loads(req.data)["stream"] is True and req.get_header("Authorization") == f"Bearer {SECRET}"


def test_http_200_carrying_an_error_object_is_unhealthy():
    # Sophia 2026-10-14/15: HTTP 200, and the stream carried the gateway's StreamingTimeoutError
    err = {"error": {"type": "StreamingTimeoutError", "code": 504,
                     "message": "No data received from compute endpoint after 30 seconds"}}
    ok, detail, _ = _probe(_Resp(_sse(err)))
    assert not ok and "StreamingTimeoutError 504 No data received" in detail
    ok, detail, _ = _probe(_Resp(_sse(_chunk("o"), {"object": "error", "message": "late failure"}, _chunk(finish="stop"))))
    assert not ok and "late failure" in detail   # an error anywhere in the stream, even before a finish_reason


def test_a_non_200_success_status_is_unhealthy():
    ok, detail, _ = _probe(_Resp(_sse(_chunk(finish="stop")), status=204))
    assert not ok and detail == "HTTP 204"


def test_a_stream_without_finish_reason_is_unhealthy():
    ok, detail, _ = _probe(_Resp(_sse(_chunk("o"), _chunk("k"), "[DONE]")))
    assert not ok and detail == "the stream ended without a finish_reason"
    ok, detail, _ = _probe(_Resp([]))
    assert not ok and "without a finish_reason" in detail


@pytest.mark.parametrize("code", [500, 502, 503, 504, 429])
def test_an_http_error_status_is_unhealthy(code):
    exc = urllib.error.HTTPError(BASE, code, "x", {}, io.BytesIO(f'{{"detail": "no worker; key {SECRET}"}}'.encode()))
    ok, detail, _ = _probe(raises=exc)
    assert not ok and detail.startswith(f"HTTP {code}: ") and "<redacted>" in detail   # the echoed bearer is redacted


def test_a_timeout_or_a_stalled_stream_is_unhealthy():
    ok, detail, _ = _probe(raises=TimeoutError("timed out"))
    assert not ok and detail == "no answer within 5 s"
    ok, detail, _ = _probe(_Resp(_sse(_chunk("o"), _chunk(finish="stop")), stall_after=1))
    assert not ok and detail == "no answer within 5 s"
    ok, detail, _ = _probe(raises=urllib.error.URLError("Name or service not known"))
    assert not ok and detail == "unreachable: Name or service not known"


def test_the_plain_probe_for_a_non_streaming_operator():
    ok, detail, req = _probe(_Resp(body=json.dumps({"choices": [{"finish_reason": "length"}]}).encode()), stream=False)
    assert ok and detail.startswith("finish_reason=length") and json.loads(req.data)["stream"] is False
    ok, detail, _ = _probe(_Resp(body=b'{"error": {"code": 504, "message": "upstream"}}'), stream=False)
    assert not ok and detail == "error in the answer: 504 upstream"
    ok, detail, _ = _probe(_Resp(body=b"<html>bad gateway</html>"), stream=False)
    assert not ok and detail.startswith("not JSON")


class _Clock:
    def __init__(self):
        self.t, self.slept = 0.0, []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


def test_wait_then_healthy_within_the_window():
    clock = _Clock()
    answers = iter([(False, "HTTP 504"), (False, "HTTP 504"), (True, "finish_reason=stop")])
    ok, detail, n = alcf_probe.wait_healthy(lambda: next(answers), 100, interval_s=30, sleep=clock.sleep, clock=clock,
                                            log=lambda s: None)
    assert ok and n == 3 and clock.slept == [30, 30] and detail == "finish_reason=stop"


def test_the_wait_runs_out_and_zero_means_one_probe():
    clock = _Clock()
    ok, detail, n = alcf_probe.wait_healthy(lambda: (False, "HTTP 504"), 70, interval_s=30, sleep=clock.sleep,
                                            clock=clock, log=lambda s: None)
    assert not ok and n == 4 and clock.slept == [30, 30, 10] and detail == "HTTP 504"
    clock = _Clock()
    ok, _, n = alcf_probe.wait_healthy(lambda: (False, "x"), 0, sleep=clock.sleep, clock=clock, log=lambda s: None)
    assert not ok and n == 1 and clock.slept == []


def _env(**kw):
    return {"ALCF_INFERENCE_TOKEN": SECRET, "HPCB_ALCF_BASE_URL": BASE, "HPCB_ALCF_MODEL": "openai/gpt-oss-120b", **kw}


def test_unhealthy_writes_an_infra_bundle_that_regrade_reproduces_and_never_the_bearer(tmp_path, capsys):
    from regrade import regrade

    runs = tmp_path / "runs"
    argv = ["--stream", "--wait-s", "0", "--runs-dir", str(runs), "--runid", "r9", "--scenario", "happy_path",
            "--operator", "pi", "--target", "fake", "--profile", "default", "--pool-user", "hpcbridge-test-03"]
    rc = alcf_probe.main(argv, env=_env(), check=lambda b, t, m, s: (False, f"HTTP 401: bad bearer {t}"))
    out = capsys.readouterr().out
    assert rc == INFRA_RC == 4
    assert "RESULT: INFRA: ALCF streaming unhealthy: HTTP 401: bad bearer <redacted>" in out and SECRET not in out
    d = runs / "r9-happy_path"
    rec = json.loads((d / "record.json").read_text())
    assert rec["result"] == "INFRA" and rec["rc"] == 4 and rec["failed"] == [PREFLIGHT_ROW]
    assert rec["infra_reason"].startswith("ALCF streaming unhealthy: HTTP 401")
    assert rec["config"]["operator"] == "pi" and rec["config"]["scenario"] == "happy_path"
    assert all(SECRET not in p.read_text() for p in d.iterdir() if p.is_file())
    assert SECRET not in " ".join(argv)
    regrade(runs)
    out = capsys.readouterr().out
    assert "[INFRA] r9-happy_path" in out and "ALCF streaming unhealthy" in out and "verdict change" not in out


def test_healthy_launches_and_a_host_tunnel_is_skipped(tmp_path, capsys):
    seen = []
    rc = alcf_probe.main(["--no-stream", "--runs-dir", str(tmp_path)], env=_env(),
                         check=lambda b, t, m, s: (seen.append(s), (True, "finish_reason=stop"))[1])
    assert rc == 0 and seen == [False] and "healthy" in capsys.readouterr().out and not list(tmp_path.iterdir())
    rc = alcf_probe.main(["--stream"], env=_env(HPCB_ALCF_BASE_URL="http://host.docker.internal:44497/v1"),
                         check=lambda *a: pytest.fail("a host tunnel is the jail's to reach"))
    assert rc == 0 and "skipped" in capsys.readouterr().out


def test_wait_s_reaches_the_retry_loop(capsys):
    answers = iter([(False, "HTTP 504"), (True, "finish_reason=stop")])
    slept = []
    rc = alcf_probe.main(["--stream", "--wait-s", "60", "--interval-s", "5"], env=_env(),
                         check=lambda *a: next(answers), sleep=slept.append)
    assert rc == 0 and slept == [5] and "after 2 probes" in capsys.readouterr().out


# ---- run_smoke.sh's wiring: probe before a free operator's cell, exit 4 without launching it -------------------------

def _smoke_tree(tmp_path):
    """A copy of run_smoke.sh beside the real harness and scenarios (no agentic/.env), and stub `uv` / `docker`."""
    root = tmp_path / "repo"
    (root / "agentic" / "runs").mkdir(parents=True)
    shutil.copy(HERE.parent / "run_smoke.sh", root / "agentic" / "run_smoke.sh")
    (root / "agentic" / "harness").symlink_to(HERE)
    (root / "agentic" / "scenarios").symlink_to(HERE.parent / "scenarios")
    stub = tmp_path / "bin"
    stub.mkdir()
    (stub / "uv").write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@" > "$STUB_DIR/uv.args"\n'
                             'env > "$STUB_DIR/uv.env"\nexit "${STUB_UV_RC:-0}"\n')
    (stub / "docker").write_text('#!/usr/bin/env bash\necho "$*" >> "$STUB_DIR/docker.calls"\n'
                                 'case "$1 $2" in "image inspect") echo sha256:stub ;; esac\nexit 0\n')
    for f in ("uv", "docker"):
        (stub / f).chmod(0o755)
    key = tmp_path / "key"
    key.write_text("not a key\n")
    return root, stub, key


def _smoke(tmp_path, operator, uv_rc=0, **extra):
    root, stub, key = _smoke_tree(tmp_path)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("HPCB_", "HPC_BRIDGE_", "ALCF_", "CLAUDE_CODE", "ANTHROPIC"))}
    env.update(PATH=f"{stub}:{env['PATH']}", STUB_DIR=str(tmp_path), STUB_UV_RC=str(uv_rc), HPCB_OPERATOR=operator,
               HPCB_TARGET="globus1", HPCB_TEST_SSH_KEY=str(key), HPCB_SKIP_BUILD="1", HPCB_RUNID="r1",
               ALCF_INFERENCE_TOKEN=SECRET, **extra)
    r = subprocess.run(["bash", str(root / "agentic" / "run_smoke.sh"), "happy_path"], env=env, capture_output=True,
                       text=True, timeout=120)
    uv_args = (tmp_path / "uv.args").read_text().split("\n") if (tmp_path / "uv.args").exists() else None
    docker = (tmp_path / "docker.calls").read_text() if (tmp_path / "docker.calls").exists() else ""
    return r, uv_args, docker


def test_run_smoke_probes_a_streaming_operator_and_exits_infra_without_launching(tmp_path):
    r, uv_args, docker = _smoke(tmp_path, "pi", uv_rc=4, HPCB_ALCF_WAIT_S="7")
    assert r.returncode == 4, r.stdout + r.stderr
    assert uv_args and uv_args[uv_args.index("python") + 1].endswith("agentic/harness/alcf_probe.py")
    assert "--stream" in uv_args and uv_args[uv_args.index("--wait-s") + 1] == "7"
    assert uv_args[uv_args.index("--operator") + 1] == "pi" and uv_args[uv_args.index("--scenario") + 1] == "happy_path"
    assert SECRET not in "\n".join(uv_args) and f"ALCF_INFERENCE_TOKEN={SECRET}" in (tmp_path / "uv.env").read_text()
    assert " run " not in f" {docker} "   # the cell was not launched


def test_run_smoke_launches_after_a_healthy_probe_and_a_plain_probe_for_codex(tmp_path):
    r, uv_args, docker = _smoke(tmp_path, "codex", uv_rc=0)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "--no-stream" in uv_args and uv_args[uv_args.index("--wait-s") + 1] == "0"   # default: probe once
    assert "run --rm" in docker


def test_run_smoke_launches_anyway_when_the_probe_cannot_run(tmp_path):
    r, _, docker = _smoke(tmp_path, "hermes21", uv_rc=1)
    assert r.returncode == 0 and "the ALCF probe could not run (rc=1)" in r.stdout and "run --rm" in docker


def test_run_smoke_does_not_probe_for_the_claude_operator(tmp_path):
    r, uv_args, docker = _smoke(tmp_path, "claude", CLAUDE_CODE_OAUTH_TOKEN="sk-ant-oat01-" + "x" * 20)
    assert r.returncode == 0, r.stdout + r.stderr
    assert uv_args is None and "run --rm" in docker
