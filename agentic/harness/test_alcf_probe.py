"""Hermetic: the ALCF probe run_smoke.sh runs before a free operator's cell (alcf_probe.py) — no network: the HTTP layer,
the clock and the sleep are stubbed — and run_smoke.sh's wiring of it (stub `uv` and `docker` on PATH)."""
from __future__ import annotations

import http.client
import http.server
import io
import json
import os
import shutil
import socket
import subprocess
import threading
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

    def __init__(self, lines=(), body=b"", status=200, stall_after=None, raise_after=None, exc=None):
        self.lines, self.body, self.status, self.stall_after = list(lines), body, status, stall_after
        self.raise_after, self.exc = raise_after, exc

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def __iter__(self):
        for n, line in enumerate(self.lines):
            if self.stall_after is not None and n >= self.stall_after:
                raise TimeoutError("timed out")
            if self.raise_after is not None and n >= self.raise_after:
                raise self.exc
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

    state, detail = alcf_probe.probe(BASE, SECRET, "openai/gpt-oss-120b", stream=stream, timeout_s=5, opener=opener)
    assert SECRET not in detail
    return state, detail, seen.get("req")


H, DOWN, C = alcf_probe.HEALTHY, alcf_probe.OUTAGE, alcf_probe.CONFIG


def test_a_healthy_stream():
    state, detail, req = _probe(_Resp(_sse(_chunk("o"), _chunk("k"), _chunk(finish="stop"), "[DONE]")))
    assert state == H and detail.startswith("finish_reason=stop")
    assert req.full_url == BASE + "/chat/completions" and req.get_method() == "POST"
    assert json.loads(req.data)["stream"] is True and req.get_header("Authorization") == f"Bearer {SECRET}"


def test_http_200_carrying_an_error_object_is_unhealthy():
    # Sophia 2026-10-14/15: HTTP 200, and the stream carried the gateway's StreamingTimeoutError
    err = {"error": {"type": "StreamingTimeoutError", "code": 504,
                     "message": "No data received from compute endpoint after 30 seconds"}}
    state, detail, _ = _probe(_Resp(_sse(err)))
    assert state == DOWN and "StreamingTimeoutError 504 No data received" in detail
    state, detail, _ = _probe(_Resp(_sse(_chunk("o"), {"object": "error", "message": "late failure"}, _chunk(finish="stop"))))
    assert state == DOWN and "late failure" in detail   # an error anywhere in the stream, even before a finish_reason
    state, detail, _ = _probe(_Resp(_sse({"error": {"code": 404, "message": "model not found"}})))
    assert state == C and "model not found" in detail   # an error that says the REQUEST is wrong


def test_a_non_200_success_status_is_unhealthy():
    state, detail, _ = _probe(_Resp(_sse(_chunk(finish="stop")), status=204))
    assert state == DOWN and detail == "HTTP 204"


def test_a_stream_without_finish_reason_is_unhealthy():
    state, detail, _ = _probe(_Resp(_sse(_chunk("o"), _chunk("k"), "[DONE]")))
    assert state == DOWN and detail == "the stream ended without a finish_reason"
    state, detail, _ = _probe(_Resp([]))
    assert state == DOWN and "without a finish_reason" in detail


@pytest.mark.parametrize("code,state", [(500, DOWN), (502, DOWN), (503, DOWN), (504, DOWN), (429, DOWN), (408, DOWN),
                                        (400, C), (401, C), (403, C), (404, C)])
def test_an_http_error_status_is_an_outage_or_a_refused_request(code, state):
    exc = urllib.error.HTTPError(BASE, code, "x", {}, io.BytesIO(f'{{"detail": "no worker; key {SECRET}"}}'.encode()))
    got, detail, _ = _probe(raises=exc)
    assert got == state and detail.startswith(f"HTTP {code}: ") and "<redacted>" in detail   # the echoed bearer is redacted


def test_a_timeout_a_stall_or_a_cut_off_answer_is_an_outage():
    state, detail, _ = _probe(raises=TimeoutError("timed out"))
    assert state == DOWN and detail == "no answer within 5 s"
    state, detail, _ = _probe(_Resp(_sse(_chunk("o"), _chunk(finish="stop")), stall_after=1))
    assert state == DOWN and detail == "no answer within 5 s"
    state, detail, _ = _probe(raises=urllib.error.URLError("Name or service not known"))
    assert state == DOWN and detail == "unreachable: Name or service not known"
    for exc in (http.client.IncompleteRead(b"partial", 255), ValueError("invalid literal for int() with base 16")):
        state, detail, _ = _probe(_Resp(_sse(_chunk("o")), raise_after=1, exc=exc))
        assert state == DOWN and detail.startswith("the answer was cut off: ")


def test_the_plain_probe_for_a_non_streaming_operator():
    state, detail, req = _probe(_Resp(body=json.dumps({"choices": [{"finish_reason": "length"}]}).encode()), stream=False)
    assert state == H and detail.startswith("finish_reason=length") and json.loads(req.data)["stream"] is False
    state, detail, _ = _probe(_Resp(body=b'{"error": {"code": 504, "message": "upstream"}}'), stream=False)
    assert state == DOWN and detail == "error in the answer: 504 upstream"
    state, detail, _ = _probe(_Resp(body=b"<html>bad gateway</html>"), stream=False)
    assert state == DOWN and detail.startswith("not JSON")


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
    answers = iter([(DOWN, "HTTP 504"), (DOWN, "HTTP 504"), (H, "finish_reason=stop")])
    state, detail, n = alcf_probe.wait_healthy(lambda: next(answers), 100, interval_s=30, sleep=clock.sleep,
                                               clock=clock, log=lambda s: None)
    assert state == H and n == 3 and clock.slept == [30, 30] and detail == "finish_reason=stop"


def test_the_wait_runs_out_zero_means_one_probe_and_a_refused_request_is_not_waited_out():
    clock = _Clock()
    state, detail, n = alcf_probe.wait_healthy(lambda: (DOWN, "HTTP 504"), 70, interval_s=30, sleep=clock.sleep,
                                               clock=clock, log=lambda s: None)
    assert state == DOWN and n == 4 and clock.slept == [30, 30, 10] and detail == "HTTP 504"
    clock = _Clock()
    state, _, n = alcf_probe.wait_healthy(lambda: (DOWN, "x"), 0, sleep=clock.sleep, clock=clock, log=lambda s: None)
    assert state == DOWN and n == 1 and clock.slept == []
    clock = _Clock()
    state, _, n = alcf_probe.wait_healthy(lambda: (C, "HTTP 401"), 3600, sleep=clock.sleep, clock=clock,
                                          log=lambda s: None)
    assert state == C and n == 1 and clock.slept == []


def _env(**kw):
    return {"ALCF_INFERENCE_TOKEN": SECRET, "HPCB_ALCF_BASE_URL": BASE, "HPCB_ALCF_MODEL": "openai/gpt-oss-120b", **kw}


def test_unhealthy_writes_an_infra_bundle_that_regrade_reproduces_and_never_the_bearer(tmp_path, capsys):
    from regrade import regrade

    runs = tmp_path / "runs"
    argv = ["--stream", "--wait-s", "0", "--runs-dir", str(runs), "--runid", "r9", "--scenario", "happy_path",
            "--operator", "pi", "--target", "fake", "--profile", "default", "--pool-user", "hpcbridge-test-03"]
    rc = alcf_probe.main(argv, env=_env(), check=lambda b, t, m, s: (DOWN, f"HTTP 503: no worker for {t}"))
    out = capsys.readouterr().out
    assert rc == INFRA_RC == 4
    assert "RESULT: INFRA: ALCF streaming unhealthy: HTTP 503: no worker for <redacted>" in out and SECRET not in out
    d = runs / "r9-happy_path"
    rec = json.loads((d / "record.json").read_text())
    assert rec["result"] == "INFRA" and rec["rc"] == 4 and rec["failed"] == [PREFLIGHT_ROW]
    assert rec["infra_reason"].startswith("ALCF streaming unhealthy: HTTP 503")
    assert rec["config"]["operator"] == "pi" and rec["config"]["scenario"] == "happy_path"
    assert all(SECRET not in p.read_text() for p in d.iterdir() if p.is_file())
    assert SECRET not in " ".join(argv)
    regrade(runs)
    out = capsys.readouterr().out
    assert "[INFRA] r9-happy_path" in out and "ALCF streaming unhealthy" in out and "verdict change" not in out


def test_a_refused_request_is_setup_failed_not_infra(tmp_path, capsys):
    rc = alcf_probe.main(["--stream", "--wait-s", "600", "--runs-dir", str(tmp_path)], env=_env(),
                         check=lambda b, t, m, s: (C, f"HTTP 401: expired {t}"), sleep=lambda s: pytest.fail("waited"))
    out = capsys.readouterr().out
    assert rc == alcf_probe.SETUP_RC == 2 and not list(tmp_path.iterdir())   # no bundle, no wait
    assert "RESULT: SETUP FAILED — ALCF streaming refused the request: HTTP 401: expired <redacted>" in out
    assert SECRET not in out
    rc = alcf_probe.main(["--no-stream"], env=_env(ALCF_INFERENCE_TOKEN=""), check=lambda *a: pytest.fail("probed"))
    assert rc == 2 and "no ALCF_INFERENCE_TOKEN" in capsys.readouterr().out


@pytest.mark.parametrize("wait", ["inf", "nan", "-1", "14401", "soon"])
def test_the_wait_is_capped_and_must_be_a_number(wait, capsys):
    with pytest.raises(SystemExit) as exc:
        alcf_probe.main(["--stream", "--wait-s", wait], env=_env(), check=lambda *a: pytest.fail("probed"))
    out = capsys.readouterr()
    assert exc.value.code == 2 and "RESULT: SETUP FAILED" in out.out and "HPCB_ALCF_WAIT_S" in out.out
    assert alcf_probe.main(["--stream", "--wait-s", "14400"], env=_env(), check=lambda *a: (H, "ok")) == 0


def test_healthy_launches_and_a_host_tunnel_is_skipped(tmp_path, capsys):
    seen = []
    rc = alcf_probe.main(["--no-stream", "--runs-dir", str(tmp_path)], env=_env(),
                         check=lambda b, t, m, s: (seen.append(s), (H, "finish_reason=stop"))[1])
    assert rc == 0 and seen == [False] and "healthy" in capsys.readouterr().out and not list(tmp_path.iterdir())
    rc = alcf_probe.main(["--stream"], env=_env(HPCB_ALCF_BASE_URL="http://host.docker.internal:44497/v1"),
                         check=lambda *a: pytest.fail("a host tunnel is the jail's to reach"))
    assert rc == 0 and "skipped" in capsys.readouterr().out


def test_wait_s_reaches_the_retry_loop(capsys):
    answers = iter([(DOWN, "HTTP 504"), (H, "finish_reason=stop")])
    slept = []
    rc = alcf_probe.main(["--stream", "--wait-s", "60", "--interval-s", "5"], env=_env(),
                         check=lambda *a: next(answers), sleep=slept.append)
    assert rc == 0 and slept == [5] and "after 2 probes" in capsys.readouterr().out


# ---- the review's probes against a real (local) HTTP server: chunked streams, a cut-off answer, an echoed bearer -----

class _Srv(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    mode = ""

    def log_message(self, *a):
        pass

    def _chunk(self, p):
        self.wfile.write(b"%x\r\n%s\r\n" % (len(p), p))

    def _send(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _sse_head(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
        auth, m = self.headers.get("Authorization", ""), type(self).mode
        if m == "echo401":
            self._send(401, {"error": {"message": f"bad credential {auth}"}})
        elif m == "stream_err_echo":
            self._sse_head()
            p = json.dumps({"error": {"type": "StreamingTimeoutError", "code": 504, "message": f"x {auth}"}})
            self._chunk(f"data: {p}\n\n".encode())
            self.wfile.write(b"0\r\n\r\n")
        elif m == "truncated_chunked":
            self._sse_head()
            self._chunk(b'data: {"choices":[{"delta":{"content":"o"}}]}\n\n')
            self.wfile.write(b"ff\r\npartial")
            self.wfile.flush()
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
        elif m == "ok_stream":
            self._sse_head()
            for p in (b'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":null}]}\n\n',
                      b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n', b"data: [DONE]\n\n"):
                self._chunk(p)
            self.wfile.write(b"0\r\n\r\n")
        elif m == "ok_plain":
            self._send(200, {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]})
        elif m == "stream_unsupported":
            if body.get("stream"):
                self._send(400, {"error": {"message": "streaming not supported"}})
            else:
                self._send(200, {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]})


@pytest.fixture(scope="module")
def local_srv():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Srv)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}/v1"
    srv.shutdown()


@pytest.mark.parametrize("mode,flag,rc,bundle", [
    ("echo401", "--stream", 2, False),            # an expired bearer: SETUP FAILED, nothing waited for
    ("stream_err_echo", "--stream", 4, True),     # 200 carrying StreamingTimeoutError: INFRA
    ("truncated_chunked", "--stream", 4, True),   # a chunked answer cut off (IncompleteRead): INFRA, not a probe crash
    ("ok_stream", "--stream", 0, False),
    ("ok_plain", "--no-stream", 0, False),
    ("stream_unsupported", "--no-stream", 0, False),
    ("stream_unsupported", "--stream", 2, False),  # the endpoint does not stream: the request's fault
])
def test_the_reviews_server_probes(local_srv, tmp_path, capsys, mode, flag, rc, bundle):
    _Srv.mode = mode
    runs = tmp_path / "runs"
    got = alcf_probe.main([flag, "--runs-dir", str(runs), "--runid", "r1", "--scenario", "s", "--operator", "pi",
                           "--timeout-s", "10"], env=_env(HPCB_ALCF_BASE_URL=local_srv))
    out = capsys.readouterr()
    files = "".join(f.read_text() for f in runs.rglob("*") if f.is_file()) if runs.exists() else ""
    assert got == rc, out.out
    assert bool(files) == bundle
    assert SECRET not in out.out + out.err + files   # an echoed bearer never leaks


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


def test_run_smoke_refuses_the_cell_when_the_probe_says_setup_failed(tmp_path):
    r, uv_args, docker = _smoke(tmp_path, "pi", uv_rc=2)
    assert r.returncode == 2 and uv_args and " run " not in f" {docker} "   # SETUP FAILED: not launched, not "anyway"
