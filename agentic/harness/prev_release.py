#!/usr/bin/env python3
"""The PREVIOUS release of hpc-bridge, built into the jail image beside the code under test: the returning-user cells'
first session runs it (returning.py, `origin=previous`), so leftover state is written by the plugin a user actually had
before this one — the Perlmutter report (plugin 0.1.20, 2026-10-29) was one older release's files read by a newer one.

Which release: the newest `v<X.Y.Z>[-<pre>]` tag whose version is OLDER than the version pyproject.toml carries now,
computed from the tags every build, so it moves forward with each release and never goes stale (today `v0.1.20-beta.1`
under 0.1.25). Never a hard-coded name.

Two halves:
- HOST — run_smoke.sh and run_suite.py run `prev_release.py export <repo>` before `docker build`: it exports that tag's
  installable tree (`git archive`: pyproject.toml, uv.lock, src/, skills/, README.md, LICENSE) into
  `agentic/.prev-release/src/` and its name into `agentic/.prev-release/TAG` (both gitignored; the export is skipped when
  TAG already names it). The Dockerfile copies the dir to /opt/hpcb-prev and installs it from its OWN lock
  (`uv sync --frozen --no-editable`) into /opt/hpcb-prev/venv. No tag older than the current version: nothing is
  exported, and a cell that asks for the previous release is refused in the jail (SETUP FAILED), never run on the
  current code under the other name.
- JAIL — `server_argv()`: the previous release's MCP server (/opt/hpcb-prev/venv/bin/hpc-bridge); `installed_tag()`.

Stdlib only: the host half runs on the host's python3.
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path

ROOT = Path("/opt/hpcb-prev")           # in the image
EXPORT_DIR = Path("agentic") / ".prev-release"   # in the build context (relative to the repo root)
# What the install needs from the tag: the package, its lock, the skill the wheel force-includes, the metadata files
TREE = ("pyproject.toml", "uv.lock", "src", "skills", "README.md", "LICENSE")
_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?$")


def parse_version(text: str) -> tuple[int, int, int] | None:
    m = re.match(r"^v?(\d+)\.(\d+)\.(\d+)", text.strip())
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None


def previous_release(tags: Iterable[str], current: str) -> str | None:
    """The newest release tag whose X.Y.Z is older than `current` (pyproject's version), or None. Among tags of one
    X.Y.Z a final release is newer than its pre-releases, and pre-releases order by their suffix."""
    cur = parse_version(current)
    if cur is None:
        return None
    best: tuple[tuple[int, int, int], int, str, str] | None = None
    for tag in tags:
        m = _TAG.match(tag.strip())
        if not m:
            continue
        v = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if v >= cur:
            continue
        key = (v, 0 if m.group(4) else 1, m.group(4) or "", tag.strip())
        if best is None or key > best:
            best = key
    return best[3] if best else None


def pyproject_version(repo: Path) -> str:
    m = re.search(r'^version = "([^"]+)"', (repo / "pyproject.toml").read_text(), re.M)
    if not m:
        raise SystemExit("prev_release: no version in pyproject.toml")
    return m.group(1)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True,
                          timeout=120).stdout


def export(repo: Path) -> str | None:
    """Export the previous release's tree into the build context; returns its tag (None: there is none)."""
    out = repo / EXPORT_DIR
    out.mkdir(parents=True, exist_ok=True)
    tag = previous_release(_git(repo, "tag", "--list", "v*").split(), pyproject_version(repo))
    stamp = out / "TAG"
    if tag is None:
        for p in (stamp, out / "src"):
            if p.is_dir():
                shutil.rmtree(p)
            elif p.exists():
                p.unlink()
        print("prev_release: no release tag older than this version — nothing exported", file=sys.stderr)
        return None
    if stamp.is_file() and stamp.read_text().strip() == tag and (out / "src" / "pyproject.toml").is_file():
        return tag
    tmp = Path(tempfile.mkdtemp(prefix=".export-", dir=out))
    try:
        have = set(_git(repo, "ls-tree", "--name-only", tag).split())
        paths = [p for p in TREE if p in have]
        archive = subprocess.run(["git", "-C", str(repo), "archive", "--format=tar", tag, *paths],
                                 capture_output=True, check=True, timeout=120).stdout
        subprocess.run(["tar", "-x", "-C", str(tmp)], input=archive, check=True, timeout=120)
        if (out / "src").exists():
            shutil.rmtree(out / "src")
        tmp.rename(out / "src")
    finally:
        if tmp.exists():
            shutil.rmtree(tmp)
    stamp.write_text(tag + "\n")
    print(f"prev_release: exported {tag} into {out}", file=sys.stderr)
    return tag


def installed_tag(root: Path = ROOT) -> str | None:
    """The release the image carries (jail side), or None."""
    try:
        return (root / "TAG").read_text().strip() or None
    except OSError:
        return None


def server_argv(root: Path = ROOT) -> list[str] | None:
    """The previous release's MCP server, as the image installed it — None when the image carries none."""
    exe = root / "venv" / "bin" / "hpc-bridge"
    return [str(exe)] if installed_tag(root) and exe.exists() else None


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="The previous hpc-bridge release, for the returning-user cells.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export", help="export its tree into agentic/.prev-release before `docker build`")
    e.add_argument("repo", type=Path)
    t = sub.add_parser("tag", help="print its tag (nothing when there is none)")
    t.add_argument("repo", type=Path)
    args = ap.parse_args(argv)
    if args.cmd == "export":
        export(args.repo.resolve())
        return 0
    tag = previous_release(_git(args.repo, "tag", "--list", "v*").split(), pyproject_version(args.repo))
    if tag:
        print(tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
