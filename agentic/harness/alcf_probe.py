"""Is the ALCF model endpoint answering? One tiny chat completion before run_smoke.sh launches a free operator's cell.

ALCF's Sophia endpoint was intermittently slow on 2026-10-14/15: its STREAMING path answered HTTP 200 carrying
`StreamingTimeoutError` (code 504, "No data received from compute endpoint after 30 seconds") while plain requests
worked. Pi streams, so its cells ended with nothing and graded FAILED. This probe asks the endpoint the way the operator
will — streamed for Pi and Hermes-latest, plain for Codex (its relay asks with `stream: false`) and the hermes operator
(streaming off) — and, if it is unhealthy, waits and asks again for up to `--wait-s` seconds. Still unhealthy, the cell
is not launched: an INFRA bundle is written (agentic/runs/<runid>-<scenario>/, result INFRA, grading row
`harness:alcf_stream`) and the exit code is INFRA_RC, so a sweep records the cell without spending its wall time.

    ALCF_INFERENCE_TOKEN=… HPCB_ALCF_BASE_URL=… HPCB_ALCF_MODEL=… \\
        python alcf_probe.py --stream --wait-s 300 --runs-dir agentic/runs --runid R --scenario S --operator pi

Healthy = HTTP 200, a `finish_reason` seen, and no error object anywhere in the answer. Exit 0 healthy (or skipped: a
base URL that is a tunnel on the host, `host.docker.internal`, is the jail's to reach, not ours), INFRA_RC unhealthy.
The bearer is read from ALCF_INFERENCE_TOKEN in the environment, never from argv, and never appears in what this prints
or writes.
"""
from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from infra import INFRA_RC, PREFLIGHT_ROW

PROMPT = "Reply with the single word: ok"
MAX_TOKENS = 64   # a reasoning model may stop on `length`; any finish_reason is a finished answer


def _ssl_context() -> ssl.SSLContext:
    try:
        import certifi  # the project env has it (requests); a bare interpreter may lack the CA bundle otherwise

        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def _open(req: urllib.request.Request, timeout: float) -> Any:
    return urllib.request.urlopen(req, timeout=timeout, context=_ssl_context())


def _redact(text: str, token: str) -> str:
    return text.replace(token, "<redacted>") if token else text


def _error_text(obj: dict) -> str:
    """An error object (`{"error": {...}}`, or `{"object": "error", …}`) as one short line."""
    err = obj.get("error", obj)
    if isinstance(err, dict):
        parts = [str(err[k]) for k in ("type", "code", "message") if err.get(k) not in (None, "")]
        return " ".join(parts) or json.dumps(err)[:200]
    return str(err)


def _is_error(obj: Any) -> bool:
    return isinstance(obj, dict) and (obj.get("error") not in (None, "") or obj.get("object") == "error")


def _finish(obj: Any) -> str | None:
    for ch in (obj.get("choices") or []) if isinstance(obj, dict) else []:
        if isinstance(ch, dict) and ch.get("finish_reason"):
            return str(ch["finish_reason"])
    return None


def _read_stream(resp: Any, deadline: float, clock: Callable[[], float]) -> tuple[bool, str]:
    finish = None
    for raw in resp:
        if clock() > deadline:
            return False, "the stream did not finish in time"
        line = (raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)).strip()
        if not line or line.startswith(":"):
            continue
        payload = line[5:].strip() if line.startswith("data:") else line
        if payload == "[DONE]":
            break
        try:
            obj = json.loads(payload)
        except ValueError:
            continue
        if _is_error(obj):
            return False, f"error in the stream: {_error_text(obj)}"
        finish = _finish(obj) or finish
    return (True, f"finish_reason={finish}") if finish else (False, "the stream ended without a finish_reason")


def _read_plain(resp: Any) -> tuple[bool, str]:
    body = resp.read().decode(errors="replace")
    try:
        obj = json.loads(body)
    except ValueError:
        return False, f"not JSON: {body[:160]!r}"
    if _is_error(obj):
        return False, f"error in the answer: {_error_text(obj)}"
    finish = _finish(obj)
    return (True, f"finish_reason={finish}") if finish else (False, "the answer has no finish_reason")


def probe(base_url: str, token: str, model: str, *, stream: bool, timeout_s: float = 90.0,
          opener: Callable[[urllib.request.Request, float], Any] = _open,
          clock: Callable[[], float] = time.monotonic) -> tuple[bool, str]:
    """One chat completion. Returns (healthy, detail); the detail never carries the token."""
    req = urllib.request.Request(
        base_url.rstrip("/") + "/chat/completions", method="POST",
        data=json.dumps({"model": model, "messages": [{"role": "user", "content": PROMPT}],
                         "max_tokens": MAX_TOKENS, "stream": stream}).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                 "Accept": "text/event-stream" if stream else "application/json"})
    t0 = clock()
    try:
        resp = opener(req, timeout_s)
        with resp:
            status = getattr(resp, "status", 200)
            if status != 200:
                ok, detail = False, f"HTTP {status}"
            elif stream:
                ok, detail = _read_stream(resp, t0 + timeout_s, clock)
            else:
                ok, detail = _read_plain(resp)
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode(errors="replace")
        except Exception:  # noqa: BLE001 - the status alone says enough
            body = ""
        ok, detail = False, f"HTTP {exc.code}" + (f": {' '.join(body.split())[:200]}" if body.strip() else "")
    except TimeoutError:
        ok, detail = False, f"no answer within {timeout_s:.0f} s"
    except urllib.error.URLError as exc:
        ok, detail = False, f"unreachable: {exc.reason}"
    except OSError as exc:
        ok, detail = False, f"{type(exc).__name__}: {exc}"
    detail = f"{detail} ({clock() - t0:.1f} s)" if ok else detail
    return ok, _redact(detail, token)


def wait_healthy(check: Callable[[], tuple[bool, str]], wait_s: float, *, interval_s: float = 30.0,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
                 log: Callable[[str], None] = print) -> tuple[bool, str, int]:
    """Probe; while unhealthy and inside `wait_s`, wait `interval_s` (or what is left) and probe again. Returns
    (healthy, the last detail, probes made). `wait_s` 0: one probe."""
    end = clock() + max(0.0, wait_s)
    n = 0
    while True:
        n += 1
        ok, detail = check()
        left = end - clock()
        if ok or left <= 0:
            return ok, detail, n
        pause = min(interval_s, left)
        log(f"ALCF probe {n}: unhealthy ({detail}); probing again in {pause:.0f} s ({left:.0f} s of the wait left)")
        sleep(pause)


def write_infra_bundle(runs_dir: Path, reason: str, config: dict) -> Path | None:
    """The bundle of a cell that was never launched: result INFRA, rc INFRA_RC, one failing `harness:alcf_stream` row
    whose detail is the reason (regrade.py reproduces INFRA from it)."""
    from invariants import Result
    from provenance import write_run_record

    return write_run_record(runs_dir, config=config, messages=[], dialogue=[],
                            grading=[Result(PREFLIGHT_ROW, False, reason)], final=None, rc=INFRA_RC,
                            gating=[PREFLIGHT_ROW], failed=[PREFLIGHT_ROW], result="INFRA", infra_reason=reason)


def main(argv: list[str] | None = None, env: dict[str, str] | None = None,
         check: Callable[[str, str, str, bool], tuple[bool, str]] | None = None,
         sleep: Callable[[float], None] = time.sleep) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--stream", dest="stream", action="store_true", help="probe as a streaming operator does")
    mode.add_argument("--no-stream", dest="stream", action="store_false", help="probe with a plain request")
    ap.add_argument("--wait-s", type=float, default=0.0, help="keep re-probing an unhealthy endpoint this long (0: once)")
    ap.add_argument("--interval-s", type=float, default=30.0)
    ap.add_argument("--timeout-s", type=float, default=90.0, help="one probe's limit")
    ap.add_argument("--runs-dir", default="", help="where to write the INFRA bundle when unhealthy (none: no bundle)")
    for k in ("runid", "scenario", "operator", "target", "profile", "pool-user", "git-sha"):
        ap.add_argument(f"--{k}", default="")
    args = ap.parse_args(argv)
    env = dict(os.environ) if env is None else env
    token = env.get("ALCF_INFERENCE_TOKEN", "").strip()
    base = env.get("HPCB_ALCF_BASE_URL", "").strip()
    model = env.get("HPCB_ALCF_MODEL", "openai/gpt-oss-120b").strip()
    kind = "streaming" if args.stream else "plain request"
    label = "ALCF streaming" if args.stream else "ALCF (plain request)"
    if not base:
        print("ALCF probe: skipped (HPCB_ALCF_BASE_URL unset)")
        return 0
    if urlparse(base).hostname == "host.docker.internal":
        print("ALCF probe: skipped (the base URL is a tunnel on the host, which the jail reaches and this probe does not)")
        return 0
    if not token:
        healthy, detail, n = False, "no ALCF_INFERENCE_TOKEN in the environment", 0
    else:
        one = check or (lambda b, t, m, s: probe(b, t, m, stream=s, timeout_s=args.timeout_s))
        healthy, detail, n = wait_healthy(lambda: one(base, token, model, args.stream), args.wait_s,
                                          interval_s=args.interval_s, sleep=sleep)
        detail = _redact(detail, token)
    if healthy:
        print(f"ALCF probe ({kind}, {model}): healthy — {detail}" + (f" after {n} probes" if n > 1 else ""))
        return 0
    reason = f"{label} unhealthy: {detail}" + (f" ({n} probes over {args.wait_s:.0f} s)" if n > 1 else "")
    if args.runs_dir:
        rec = write_infra_bundle(Path(args.runs_dir), reason, {
            "runid": args.runid or "local", "scenario": args.scenario or "run", "operator": args.operator,
            "target": args.target, "profile": args.profile or None, "model": model, "pool_user": args.pool_user,
            "git_sha": args.git_sha or "unknown", "persona": None, "ablate_skill": False,
            "preflight": {"probe": kind, "wait_s": args.wait_s, "probes": n}})
        if rec is not None:
            print(f"record: {rec}")
    print(f"RESULT: INFRA: {reason} — the cell was not launched")
    return INFRA_RC


if __name__ == "__main__":
    sys.exit(main())
