# src/hpc_bridge/catalog/health.py
"""Registry health: is the live index what the seeds say, and is each facility still what its entry was proven
against? Zero-cost checks — reads only, no compute block, no SSH login — meant to run on a schedule (hourly during
an event) so drift is caught before a user meets it. The live re-prove (`agentic/registry_reprove.py`) is the paid
tier that catches what no published field shows (a facility's parsl, a partition's QOS).

    hpc-bridge-registry-health [--seeds DIR] [--index ID] [--json] [--state FILE] [--notify] [--skip CHECK,...]

Exit status: 0 all ok, 1 warnings, 2 failures (a facility a user would meet broken, or an index that serves
something other than the seeds).

Checks:
- index     every seed is in the live index exactly as written; nothing extra is listed there
- facility  a facility endpoint is online, and its published version / Python / template / manager config match the
            entry's `verification` (and `worker_env.verified_with`)
- ssh       an SSH entry's login host resolves and accepts TCP on port 22 (no login)
- releases  a release since the entry was proven of a package that moves workers (parsl, for `float` facilities)
- install   what a FRESH `uvx --from git+…` install resolves today (it ignores uv.lock) vs what is locked and tested
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from .bundled import BundledCatalog
from .entry import CatalogEntry, facility_fingerprint
from .search import PUBLIC_REGISTRY_INDEX

Level = Literal["ok", "warn", "fail"]
_RANK = {"ok": 0, "warn": 1, "fail": 2}
CHECKS = ("index", "facility", "ssh", "releases", "install")
REPO = "globus-labs/hpc-bridge"
# Packages whose version a FRESH install must match the lock on: a client SDK newer than the one the registry was
# proven with changes the worker an `worker_version: client` facility builds, and dill carries the task across.
_INSTALL_STRICT = ("globus-compute-sdk", "dill")
_INSTALL_WATCH = ("globus-sdk", "mcp", "pydantic")


@dataclass
class Finding:
    entry: str      # facility id, or "*" for registry-wide
    check: str
    level: Level
    detail: str

    @property
    def key(self) -> str:
        # the WHOLE detail: an escalation (4.17 → 4.18 still unresolved, a second parsl release) is a new finding
        return f"{self.entry}:{self.check}:{self.detail}"


# ---------------------------------------------------------------- index ↔ seeds

def _flatten(d: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(d, dict):
        out: dict[str, Any] = {}
        for k, v in d.items():
            out.update(_flatten(v, f"{prefix}{k}."))
        return out
    return {prefix.rstrip("."): d}


def check_index(seeds: list[CatalogEntry], search, index_id: str) -> list[Finding]:
    """The live index serves the seeds, field for field (as this client parses them), and nothing else."""
    out: list[Finding] = []
    for seed in seeds:
        try:
            resp = search.get_subject(index_id, seed.subject)
            entries = (resp.get("entries") if hasattr(resp, "get") else resp["entries"]) or []
        except Exception as exc:  # noqa: BLE001 - a 404 is "missing"; report whatever it was
            out.append(Finding(seed.id, "index", "fail",
                               f"missing: {seed.subject} not readable ({type(exc).__name__})"))
            continue
        if not entries:
            out.append(Finding(seed.id, "index", "fail", f"missing: {seed.subject} is not in the index"))
            continue
        try:
            live = CatalogEntry.model_validate(entries[0]["content"])
        except Exception as exc:  # noqa: BLE001
            out.append(Finding(seed.id, "index", "fail",
                               f"unparseable: the index copy does not validate ({exc})"[:300]))
            continue
        want, got = _flatten(seed.model_dump(mode="json")), _flatten(live.model_dump(mode="json"))
        diff = sorted(k for k in set(want) | set(got) if want.get(k) != got.get(k))
        if diff:
            out.append(Finding(seed.id, "index", "fail",
                               "differs: the index serves something other than the seed — re-ingest "
                               f"({', '.join(diff[:8])})"))
        else:
            out.append(Finding(seed.id, "index", "ok", "matches the seed"))
    try:
        resp = search.post_search(index_id, {"q": "*", "limit": 100})
        listed = {g.get("subject") for g in (resp.get("gmeta") or [])}
        extra = sorted(s for s in listed - {e.subject for e in seeds} if s)
        if extra:
            out.append(Finding("*", "index", "warn",
                               "extra: listed in the index but in no seed (retired? delete-subject): "
                               f"{', '.join(extra)}"))
    except Exception as exc:  # noqa: BLE001
        out.append(Finding("*", "index", "warn", f"listing: could not list the index ({type(exc).__name__})"))
    return out


# ---------------------------------------------------------------- facility endpoints

def _same(a: str, b: str) -> bool:
    from ..facility.mep import _same_version
    return _same_version(a, b)


_COMPARED = ("endpoint_version", "python_version", "template_sha256")


def check_facility(entry: CatalogEntry, compute) -> list[Finding]:
    """A facility multi-user endpoint is online and still what the entry was proven against."""
    eid = entry.compute_mep_uuid
    if not eid:
        return []
    try:
        status = (compute.get_endpoint_status(eid) or {}).get("status")
    except Exception as exc:  # noqa: BLE001
        return [Finding(entry.id, "facility", "fail", f"status: unreadable ({type(exc).__name__}: {exc})"[:300])]
    if status != "online":
        return [Finding(entry.id, "facility", "fail", f"offline: the facility endpoint reports {status!r}")]
    try:
        live = facility_fingerprint(compute.get_endpoint_metadata(eid))
    except Exception as exc:  # noqa: BLE001
        return [Finding(entry.id, "facility", "warn",
                        f"metadata: unreadable ({type(exc).__name__}) — online, unverified")]
    ver = entry.verification
    out: list[Finding] = []
    # A field the facility does not publish cannot be compared — say so; never let a missing value read as a match.
    unpublished = [f for f in (*_COMPARED, "config_sha256") if live[f] is None]
    if unpublished:
        out.append(Finding(entry.id, "facility", "warn",
                           f"unpublished: the facility's metadata has no {', '.join(unpublished)} — not compared"))
    verified_with = getattr(entry.compute.worker_env, "verified_with", None) or (ver.endpoint_version if ver else None)
    ev = live["endpoint_version"]
    if ev and verified_with and not _same(ev, verified_with):
        out.append(Finding(entry.id, "facility", "fail",
                           f"version: the endpoint now runs {ev}, the entry was proven with {verified_with} — "
                           "re-prove before anyone uses it"))
    if ver is None:
        out.append(Finding(entry.id, "facility", "warn",
                           "unbaselined: the entry has no `verification` block — run the re-prove to record one"))
    else:
        missing = [f for f in ("verified_on", *_COMPARED, "config_sha256") if getattr(ver, f) is None]
        if missing:
            out.append(Finding(entry.id, "facility", "warn",
                               f"incomplete: the verification block lacks {', '.join(missing)} — those are not "
                               "compared; re-prove to record them"))
        py = live["python_version"]
        if py and ver.python_version and not _same(py, ver.python_version):
            out.append(Finding(entry.id, "facility", "fail",
                               f"python: the endpoint now runs Python {py} (proven with {ver.python_version}) — a "
                               "redeploy; its packages may have moved: re-prove"))
        if live["template_sha256"] and ver.template_sha256 and live["template_sha256"] != ver.template_sha256:
            out.append(Finding(entry.id, "facility", "fail",
                               "template: the facility changed its user template or schema since the entry was proven "
                               "— re-prove (a key the entry sends may now be refused, or the worker setup changed)"))
        if live["config_sha256"] and ver.config_sha256 and live["config_sha256"] != ver.config_sha256:
            out.append(Finding(entry.id, "facility", "warn",
                               "config: the facility changed its manager config since the entry was proven"))
    if not out:
        out.append(Finding(entry.id, "facility", "ok", f"online; v{ev} py{live['python_version']}; template unchanged"))
    return out


# ---------------------------------------------------------------- SSH entries

def check_ssh(entry: CatalogEntry, *, connect: Callable[..., Any] = socket.create_connection) -> list[Finding]:
    """The login host answers on port 22. No login (an MFA facility needs the user's code for anything more)."""
    if not entry.ssh_host:
        return []
    try:
        with connect((entry.ssh_host, 22), timeout=8):
            pass
    except OSError as exc:
        return [Finding(entry.id, "ssh", "fail",
                        f"unreachable: {entry.ssh_host}:22 ({type(exc).__name__}: {exc})"[:300])]
    return [Finding(entry.id, "ssh", "ok", f"{entry.ssh_host}:22 answers (a login needs the user's credentials)")]


# ---------------------------------------------------------------- package releases

def _pypi(pkg: str) -> dict:
    with urllib.request.urlopen(f"https://pypi.org/pypi/{pkg}/json", timeout=20) as r:
        return json.loads(r.read())


def _released_after(info: dict, since: datetime.date) -> list[str]:
    out = []
    for version, files in (info.get("releases") or {}).items():
        if not files or any(f.get("yanked") for f in files):
            continue
        uploaded = min(datetime.date.fromisoformat(f["upload_time_iso_8601"][:10]) for f in files)
        if uploaded > since:
            out.append(f"{version} ({uploaded})")
    return sorted(out)


def _version_key(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r"\d+", v))


def _recorded_parsl(entry: CatalogEntry) -> str | None:
    """The parsl the worker reported on the run that proved the entry (`verification.worker`: "… parsl2026.10.5 …")."""
    m = re.search(r"\bparsl(\d[\w.]*)", (entry.verification.worker or "") if entry.verification else "")
    return m.group(1) if m else None


def check_releases(entries: list[CatalogEntry], *, pypi: Callable[[str], dict] = _pypi) -> list[Finding]:
    """A `float` facility's endpoint picks up a new parsl at its next start, while a user endpoint that was already
    running keeps the old one — the window in which our worker (which also floats) and the endpoint disagree. A parsl
    newer than the one the entry was proven with means: re-prove, and expect the race until the facility's endpoints
    cycle. Compared against the parsl the worker REPORTED (a release later the same day as the re-prove counts); by
    date when no worker parsl was recorded."""
    out: list[Finding] = []
    floats = [e for e in entries if getattr(e.compute.worker_env, "strategy", None) == "float"]
    if not floats:
        return out
    try:
        parsl = pypi("parsl")
    except Exception as exc:  # noqa: BLE001
        return [Finding("*", "releases", "warn", f"pypi: could not read parsl releases ({type(exc).__name__})")]
    latest = str((parsl.get("info") or {}).get("version") or "")
    for e in floats:
        recorded = _recorded_parsl(e)
        if recorded and latest:
            if _version_key(latest) > _version_key(recorded):
                out.append(Finding(e.id, "releases", "warn",
                                   f"parsl: {latest} is released; the entry was proven with the worker on {recorded} — "
                                   "this facility's endpoint floats: re-prove"))
            else:
                out.append(Finding(e.id, "releases", "ok", f"parsl {recorded} is still the latest"))
            continue
        since = e.verification.verified_on if e.verification and e.verification.verified_on else e.last_validated
        newer = _released_after(parsl, since)
        if newer:
            out.append(Finding(e.id, "releases", "warn",
                               f"parsl: released since the entry was proven ({', '.join(newer[-3:])}) — this "
                               "facility's endpoint floats; re-prove"))
        else:
            out.append(Finding(e.id, "releases", "ok", f"no parsl release since {since}"))
    return out


# ---------------------------------------------------------------- what a fresh install gets

def _raw(ref: str, path: str) -> str:
    url = f"https://raw.githubusercontent.com/{REPO}/{ref}/{path}"
    with urllib.request.urlopen(url, timeout=20) as r:
        return r.read().decode()


def _locked(lock_text: str, names: tuple[str, ...]) -> dict[str, str]:
    import tomllib

    pkgs = tomllib.loads(lock_text).get("package") or []
    return {p["name"]: p["version"] for p in pkgs if p.get("name") in names}


def _fresh(pyproject_text: str, python: str, names: tuple[str, ...]) -> dict[str, str]:
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "pyproject.toml").write_text(pyproject_text)
        uv = os.environ.get("UV") or shutil.which("uv")  # `uv run` sets UV; launchd's PATH may not hold uv
        if not uv:
            raise RuntimeError("uv not found (not on PATH, and UV unset)")
        try:  # cwd + a RELATIVE path: no random temp path in an error (alert keys must be stable run to run)
            res = subprocess.run([uv, "pip", "compile", "pyproject.toml", "--python-version", python, "--quiet",
                                  "--no-header"], cwd=d, capture_output=True, text=True, timeout=180, check=False)
        except subprocess.TimeoutExpired:
            raise RuntimeError("uv pip compile timed out after 180 s") from None
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip()[-300:] or f"uv pip compile rc={res.returncode}")
    out = {}
    for line in res.stdout.splitlines():
        line = line.strip()
        if "==" in line and not line.startswith("#"):
            name, version = line.split("==", 1)
            name = name.strip().lower()
            if name in names:
                out[name] = version.split(";")[0].strip()
    return out


def check_install(ref: str = "main", pythons: tuple[str, ...] = ("3.13", "3.12"), *,
                  raw: Callable[[str, str], str] = _raw,
                  fresh: Callable[[str, str, tuple[str, ...]], dict[str, str]] = _fresh) -> list[Finding]:
    """`uvx --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge` (the Pi / Hermes / Codex install)
    resolves dependencies from pyproject.toml, NOT uv.lock: a Globus release the day before an event reaches every
    such user untested. Compare that resolution with the lock."""
    names = _INSTALL_STRICT + _INSTALL_WATCH
    try:
        locked = _locked(raw(ref, "uv.lock"), names)
        pyproject = raw(ref, "pyproject.toml")
    except Exception as exc:  # noqa: BLE001
        return [Finding("*", "install", "warn", f"fetch: could not read {ref}'s pyproject/lock ({type(exc).__name__})")]
    out: list[Finding] = []
    for py in pythons:
        try:
            got = fresh(pyproject, py, names)
        except Exception as exc:  # noqa: BLE001
            out.append(Finding("*", "install", "warn", f"resolve: py{py} could not be resolved ({exc})"[:300]))
            continue
        for name in names:
            if name in got and name in locked and got[name] != locked[name]:
                level: Level = "fail" if name in _INSTALL_STRICT else "warn"
                out.append(Finding("*", "install", level,
                                   f"{name}: a fresh install on py{py} gets {got[name]}, the lock (tested) has "
                                   f"{locked[name]} — pin it for the event or re-prove on it"))
    if not out:
        out.append(Finding("*", "install", "ok",
                           "a fresh install resolves the locked "
                           + ", ".join(f"{n} {locked[n]}" for n in _INSTALL_STRICT if n in locked)))
    return out


# ---------------------------------------------------------------- run, report, alert

def run_checks(seeds: list[CatalogEntry], *, index_id: str, search=None, compute=None,
               skip: tuple[str, ...] = (), ref: str = "main") -> list[Finding]:
    out: list[Finding] = []
    if "index" not in skip and search is not None:
        out += check_index(seeds, search, index_id)
    for e in seeds:
        if "facility" not in skip and compute is not None:
            out += check_facility(e, compute)
        if "ssh" not in skip:
            out += check_ssh(e)
    if "releases" not in skip:
        out += check_releases(seeds)
    if "install" not in skip:
        out += check_install(ref)
    return out


def worst(findings: list[Finding]) -> Level:
    return max((f.level for f in findings), key=lambda lv: _RANK[lv], default="ok")


def changes(findings: list[Finding], state_file: Path | None) -> list[Finding]:
    """The findings that are NEW (not ok, and not in the previous run) — what an alert should say. Writes the state."""
    if state_file is None:
        return [f for f in findings if f.level != "ok"]
    try:
        before = set(json.loads(state_file.read_text()).get("bad", []))
    except (OSError, ValueError):
        before = set()
    bad = {f.key for f in findings if f.level != "ok"}
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps({"bad": sorted(bad), "at": datetime.datetime.now().isoformat(timespec="seconds"),
                                      "findings": [asdict(f) for f in findings]}, indent=1))
    return [f for f in findings if f.level != "ok" and f.key not in before]


def notify(title: str, message: str) -> None:
    """A desktop notification (macOS); silently nothing elsewhere. The text travels as an argument, never inside the
    AppleScript source — findings carry facility-published strings, and a backslash or quote must not break it."""
    if sys.platform == "darwin":
        subprocess.run(["osascript", "-e", "on run argv", "-e",
                        "display notification (item 1 of argv) with title (item 2 of argv)", "-e", "end run",
                        message[:240], title], capture_output=True, check=False, timeout=10)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="hpc-bridge-registry-health", description=__doc__.split("\n\n")[0])
    ap.add_argument("--seeds", default=str(Path(__file__).parent / "seed"), help="seed file or directory")
    ap.add_argument("--index", default=PUBLIC_REGISTRY_INDEX)
    ap.add_argument("--ref", default="main", help="the git ref a fresh install builds (install check)")
    ap.add_argument("--skip", default="", help=f"comma-separated checks to skip: {','.join(CHECKS)}")
    ap.add_argument("--json", action="store_true", help="print the findings as JSON")
    ap.add_argument("--state", default=None, help="remember findings here; alert only on new ones")
    ap.add_argument("--notify", action="store_true", help="desktop notification for new findings")
    a = ap.parse_args(argv)
    skip = tuple(s.strip() for s in a.skip.split(",") if s.strip())
    seeds = list(BundledCatalog(Path(a.seeds)).entries())

    search = compute = None
    pre: list[Finding] = []
    if "index" not in skip:
        import globus_sdk

        search = globus_sdk.SearchClient()  # anonymous, as every installed plugin reads it
    if "facility" not in skip:
        # Facility metadata needs an identity: the curator's own Globus login. Never let a missing/expired login (or a
        # web-service hiccup building the client) kill the run — the other checks still matter, and say why.
        try:
            from globus_compute_sdk import Client

            from ..login import LoginFlow

            if LoginFlow().login_required():
                pre.append(Finding("*", "facility", "fail",
                                   "login: no valid Globus login in the SDK's store — facility endpoints not checked; "
                                   "run `globus-compute-endpoint login` (or any hpc-bridge login) interactively"))
            else:
                compute = Client()
        except Exception as exc:  # noqa: BLE001
            pre.append(Finding("*", "facility", "fail",
                               f"login: could not build the Compute client ({type(exc).__name__}: {exc})"[:300]))
    findings = pre + run_checks(seeds, index_id=a.index, search=search, compute=compute, skip=skip, ref=a.ref)
    new = changes(findings, Path(a.state).expanduser() if a.state else None)
    level = worst(findings)
    if a.json:
        print(json.dumps([asdict(f) for f in findings], indent=1))
    else:
        mark = {"ok": "  ok ", "warn": " WARN", "fail": " FAIL"}
        for f in sorted(findings, key=lambda f: (-_RANK[f.level], f.entry, f.check)):
            print(f"{mark[f.level]}  {f.entry:<12} {f.check:<9} {f.detail}")
        print(f"\nregistry health: {level.upper()} ({sum(f.level == 'fail' for f in findings)} fail, "
              f"{sum(f.level == 'warn' for f in findings)} warn)")
    if a.notify and new:
        notify("hpc-bridge registry", "; ".join(f"{f.entry} {f.check}: {f.detail}" for f in new[:3]))
    return _RANK[level]


if __name__ == "__main__":
    raise SystemExit(main())
