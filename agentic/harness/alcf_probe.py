"""Is the ALCF model endpoint answering? One tiny chat completion before run_smoke.sh launches a free operator's cell.

ALCF's Sophia endpoint was intermittently slow on 2026-10-14/15: its STREAMING path answered HTTP 200 carrying
`StreamingTimeoutError` (code 504, "No data received from compute endpoint after 30 seconds") while plain requests
worked. Pi streams, so its cells ended with nothing and graded FAILED. This probe asks the endpoint the way the operator
will — streamed for Pi and Hermes-latest, plain for Codex and the hermes operator (streaming off) — and, if it is
unhealthy, waits and asks again for up to `--wait-s` seconds (at most 4 h). Still unhealthy, the cell is not launched:
an INFRA bundle is written (agentic/runs/<runid>-<scenario>/, result INFRA, grading row `harness:alcf_stream`) and the
exit code is INFRA_RC, so a sweep records the cell without spending its wall time. Codex's relay calls `/responses`
(unstreamed), not `/chat/completions`: the plain chat probe stands in for the same endpoint.

An answer that says the REQUEST is wrong — a 4xx other than 408 / 429 (an expired bearer, a model name the endpoint does
not serve, streaming it does not offer), or no bearer at all — is not an outage: nothing is waited for, no bundle is
written, `RESULT: SETUP FAILED — …` is printed and the exit code is 2 (run.py's SETUP FAILED).

    ALCF_INFERENCE_TOKEN=… HPCB_ALCF_BASE_URL=… HPCB_ALCF_MODEL=… \\
        python alcf_probe.py --stream --wait-s 300 --runs-dir agentic/runs --runid R --scenario S --operator pi

Healthy = HTTP 200, a `finish_reason` seen, and no error object anywhere in the answer. Exit 0 healthy (or skipped: a
base URL that is a tunnel on the host, `host.docker.internal`, is the jail's to reach, not ours), INFRA_RC unhealthy,
2 a request the endpoint refuses as wrong (or a bad argument, e.g. an HPCB_ALCF_WAIT_S that is not 0–14400 s).
The bearer is read from ALCF_INFERENCE_TOKEN in the environment, never from argv, and never appears in what this prints
or writes.
"""
from __future__ import annotations

import argparse
import http.client
import json
import math
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
MAX_WAIT_S = 4 * 3600.0
SETUP_RC = 2      # run.py's SETUP FAILED: the request is wrong (bearer, model, URL), not the endpoint down
HEALTHY, OUTAGE, CONFIG = "healthy", "outage", "config"


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


def _refused(code: Any) -> bool:
    """An HTTP status that says the REQUEST is wrong (4xx), not that the endpoint is down: 408 and 429 are the
    endpoint's load, every other 4xx is ours (an expired bearer, an unknown model, an unsupported option)."""
    try:
        n = int(code)
    except (TypeError, ValueError):
        return False
    return 400 <= n < 500 and n not in (408, 429)


def _error_state(obj: dict) -> str:
    err = obj.get("error", obj)
    code = (err.get("code") or err.get("status")) if isinstance(err, dict) else None
    return CONFIG if _refused(code) else OUTAGE


def _finish(obj: Any) -> str | None:
    for ch in (obj.get("choices") or []) if isinstance(obj, dict) else []:
        if isinstance(ch, dict) and ch.get("finish_reason"):
            return str(ch["finish_reason"])
    return None


def _read_stream(resp: Any, deadline: float, clock: Callable[[], float]) -> tuple[str, str]:
    finish = None
    for raw in resp:
        if clock() > deadline:
            return OUTAGE, "the stream did not finish in time"
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
            return _error_state(obj), f"error in the stream: {_error_text(obj)}"
        finish = _finish(obj) or finish
    return (HEALTHY, f"finish_reason={finish}") if finish else (OUTAGE, "the stream ended without a finish_reason")


def _read_plain(resp: Any) -> tuple[str, str]:
    body = resp.read().decode(errors="replace")
    try:
        obj = json.loads(body)
    except ValueError:
        return OUTAGE, f"not JSON: {body[:160]!r}"
    if _is_error(obj):
        return _error_state(obj), f"error in the answer: {_error_text(obj)}"
    finish = _finish(obj)
    return (HEALTHY, f"finish_reason={finish}") if finish else (OUTAGE, "the answer has no finish_reason")


def probe(base_url: str, token: str, model: str, *, stream: bool, timeout_s: float = 90.0,
          opener: Callable[[urllib.request.Request, float], Any] = _open,
          clock: Callable[[], float] = time.monotonic) -> tuple[str, str]:
    """One chat completion. Returns (HEALTHY | OUTAGE | CONFIG, detail); the detail never carries the token."""
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
                state, detail = (CONFIG if _refused(status) else OUTAGE), f"HTTP {status}"
            elif stream:
                state, detail = _read_stream(resp, t0 + timeout_s, clock)
            else:
                state, detail = _read_plain(resp)
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode(errors="replace")
        except Exception:  # noqa: BLE001 - the status alone says enough
            body = ""
        state = CONFIG if _refused(exc.code) else OUTAGE
        detail = f"HTTP {exc.code}" + (f": {' '.join(body.split())[:200]}" if body.strip() else "")
    except TimeoutError:
        state, detail = OUTAGE, f"no answer within {timeout_s:.0f} s"
    except urllib.error.URLError as exc:
        state, detail = OUTAGE, f"unreachable: {exc.reason}"
    except (OSError, http.client.HTTPException, ValueError) as exc:   # e.g. IncompleteRead: a chunked answer cut off
        state, detail = OUTAGE, f"the answer was cut off: {type(exc).__name__}: {exc}"
    detail = f"{detail} ({clock() - t0:.1f} s)" if state == HEALTHY else detail
    return state, _redact(detail, token)


def wait_healthy(check: Callable[[], tuple[str, str]], wait_s: float, *, interval_s: float = 30.0,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
                 log: Callable[[str], None] = print) -> tuple[str, str, int]:
    """Probe; while it is an OUTAGE and inside `wait_s`, wait `interval_s` (or what is left) and probe again. Returns
    (the last state, its detail, probes made). `wait_s` 0: one probe. A CONFIG answer is not waited out."""
    end = clock() + max(0.0, wait_s)
    n = 0
    while True:
        n += 1
        state, detail = check()
        left = end - clock()
        if state != OUTAGE or left <= 0:
            return state, detail, n
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


class _Parser(argparse.ArgumentParser):
    def error(self, message: str):  # type: ignore[override]
        """A bad argument (an HPCB_ALCF_WAIT_S of "inf", say) refuses the cell with a RESULT line a sweep can read."""
        print(f"RESULT: SETUP FAILED — the ALCF probe's arguments: {message} — the cell was not launched")
        super().error(message)


def _seconds(low: float, high: float, what: str) -> Callable[[str], float]:
    def parse(v: str) -> float:
        try:
            x = float(v)
        except ValueError:
            x = math.nan
        if not math.isfinite(x) or not low <= x <= high:
            raise argparse.ArgumentTypeError(f"{what} must be a number of seconds from {low:g} to {high:g}, not {v!r}")
        return x
    return parse


def main(argv: list[str] | None = None, env: dict[str, str] | None = None,
         check: Callable[[str, str, str, bool], tuple[str, str]] | None = None,
         sleep: Callable[[float], None] = time.sleep) -> int:
    ap = _Parser(description=__doc__.splitlines()[0] if __doc__ else None)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--stream", dest="stream", action="store_true", help="probe as a streaming operator does")
    mode.add_argument("--no-stream", dest="stream", action="store_false", help="probe with a plain request")
    ap.add_argument("--wait-s", type=_seconds(0, MAX_WAIT_S, "HPCB_ALCF_WAIT_S (--wait-s)"), default=0.0,
                    help="keep re-probing an unhealthy endpoint this long (0: once; at most 4 h)")
    ap.add_argument("--interval-s", type=_seconds(1, 3600, "--interval-s"), default=30.0)
    ap.add_argument("--timeout-s", type=_seconds(1, 600, "--timeout-s"), default=90.0, help="one probe's limit")
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
        state, detail, n = CONFIG, "no ALCF_INFERENCE_TOKEN in the environment", 0
    else:
        one = check or (lambda b, t, m, s: probe(b, t, m, stream=s, timeout_s=args.timeout_s))
        state, detail, n = wait_healthy(lambda: one(base, token, model, args.stream), args.wait_s,
                                        interval_s=args.interval_s, sleep=sleep)
        detail = _redact(detail, token)
    if state == HEALTHY:
        print(f"ALCF probe ({kind}, {model}): healthy — {detail}" + (f" after {n} probes" if n > 1 else ""))
        return 0
    if state == CONFIG:
        print(f"RESULT: SETUP FAILED — {label} refused the request: {detail}. That is the bearer, the model name "
              f"({model}) or the base URL, not an outage — the cell was not launched")
        return SETUP_RC
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
