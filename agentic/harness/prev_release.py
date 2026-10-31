#!/usr/bin/env python3
"""OLDER releases of hpc-bridge, built into the jail image beside the code under test: the returning-user cells' first
session runs one (returning.py, `origin=previous`), so leftover state is written by the plugin a user actually had
before this one — the Perlmutter report (plugin 0.1.20, 2026-10-29) was one older release's files read by a newer one.

Which releases (image SLOTS, each its own tree and venv):
- `previous` — the release before this version: the newest RELEASE COMMIT whose version is older than the one
  pyproject.toml carries now. A release commit is where pyproject's version was bumped (first-parent history of
  pyproject.toml): the plugin a user installed at that version, not the later commits that still carry it on their way
  to the next release. Read from the history every build, so it moves forward with each release and never goes stale
  (today 0.1.24, at 9c75742). The tags are not used: three exist (the last is v0.1.20-beta.1), and choosing by them made
  `previous` 0.1.20 under 0.1.25 — four releases back (the review of 2026-10-30).
- a PINNED version (`PINNED`): a cell that must run a given release whatever `previous` is — 0.1.20, the plugin of the
  Perlmutter report, for its replay (returning_perlmutter_replay).
Versions order numerically, pre-releases included (`version_key`: 0.1.20-beta.10 after 0.1.20-beta.9, a final release
after its pre-releases).

Two halves:
- HOST — run_smoke.sh and run_suite.py run `prev_release.py export <repo>` before `docker build`: each slot's
  installable tree (`git archive` at its release commit: pyproject.toml, uv.lock, src/, skills/, README.md, LICENSE)
  goes to `agentic/.prev-release/<slot>/src/`, with its VERSION, COMMIT and DATE (the commit's date, UTC) beside it
  (gitignored; a slot already at its commit is left as it is). The Dockerfile copies the dir to /opt/hpcb-prev and
  installs each slot from its OWN lock (`uv sync --frozen --no-editable`) into /opt/hpcb-prev/<slot>/venv. A failed
  export exits non-zero with the reason (the callers say so loudly); a slot with no release (none older than this
  version) is not exported, and a cell that asks for it is refused in the jail (SETUP FAILED), never run on the current
  code under the other name.
- JAIL — `server_argv(slot)`: that slot's MCP server (/opt/hpcb-prev/<slot>/venv/bin/hpc-bridge); `installed(slot)`:
  what the export said it is; `installed_version(venv)`: what the installed package says it is (the `harness:phase_server`
  gate compares the two, run.py).

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
from dataclasses import dataclass
from pathlib import Path

ROOT = Path("/opt/hpcb-prev")           # in the image
EXPORT_DIR = Path("agentic") / ".prev-release"   # in the build context (relative to the repo root)
PREVIOUS = "previous"
# Releases a cell pins by version, each an image slot of its own beside `previous`: a second `uv sync` of an old lock
# in the image build (cached by docker until the release's tree changes — never, for a pinned one).
PINNED = ("0.1.20",)   # the Perlmutter report's plugin (returning_perlmutter_replay's session 1)
# What the install needs from the release: the package, its lock, the skill the wheel force-includes, the metadata files
TREE = ("pyproject.toml", "uv.lock", "src", "skills", "README.md", "LICENSE")
_VERSION = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)(?:[-.]?([0-9A-Za-z][0-9A-Za-z.-]*))?$")
_SLOT = re.compile(r"^(previous|\d+\.\d+\.\d+)$")


def version_key(text: str) -> tuple | None:
    """A version's order: X.Y.Z numerically, then a final release after its pre-releases, whose parts compare
    numerically where both are numbers (beta.10 after beta.9) and a number before a word (as semver orders them).
    None for what is not a version."""
    m = _VERSION.match(text.strip())
    if not m:
        return None
    pre = m.group(4)
    parts = tuple((0, int(p), "") if p.isdigit() else (1, 0, p) for p in re.split(r"[.-]", pre) if p) if pre else ()
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)), 0 if pre else 1, parts)


def parse_version(text: str) -> tuple[int, int, int] | None:
    k = version_key(text)
    return (k[0], k[1], k[2]) if k else None


@dataclass(frozen=True)
class Release:
    version: str
    commit: str
    date: str   # the release commit's date, UTC (RFC 3339): the world a user of it lived in (returning.py's uv cutoff)


def releases(history: Iterable[tuple[str, str, str]]) -> list[Release]:
    """The release commits in a first-parent history of pyproject.toml, newest first: `history` is (commit, date,
    version) newest first, and a commit is a release where its version differs from the next older commit's (the
    oldest is one too). Commits whose version does not parse are passed over."""
    rows = [(c, d, v) for c, d, v in history if version_key(v) is not None]
    return [Release(v, c, d) for k, (c, d, v) in enumerate(rows) if k + 1 == len(rows) or rows[k + 1][2] != v]


def previous_release(rels: Iterable[Release], current: str) -> Release | None:
    """The release with the highest version older than `current` (pyproject's version), or None. Of two release commits
    of one version (a bump reverted and made again), the newer."""
    cur = version_key(current)
    if cur is None:
        return None
    older = [r for r in rels if (k := version_key(r.version)) is not None and k < cur]
    return max(older, key=lambda r: version_key(r.version) or (), default=None)   # max keeps the first (newest) of ties


def pinned_release(rels: Iterable[Release], version: str) -> Release | None:
    want = version_key(version)
    return next((r for r in rels if version_key(r.version) == want), None)


def pyproject_version(repo: Path) -> str:
    m = re.search(r'^version = "([^"]+)"', (repo / "pyproject.toml").read_text(), re.M)
    if not m:
        raise SystemExit("prev_release: no version in pyproject.toml")
    return m.group(1)


def _git(repo: Path, *args: str) -> str:
    r = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise SystemExit(f"prev_release: `git {' '.join(args[:3])}` failed in {repo} (rc={r.returncode}): "
                         f"{r.stderr.strip()[-400:]}")
    return r.stdout


def history(repo: Path) -> list[tuple[str, str, str]]:
    """(commit, date UTC, version) of every first-parent commit that touched pyproject.toml, newest first."""
    out = []
    for line in _git(repo, "log", "--first-parent", "--format=%H %cd", "--date=iso-strict",
                     "HEAD", "--", "pyproject.toml").splitlines():
        commit, _, date = line.partition(" ")
        m = re.search(r'^version = "([^"]+)"', _git(repo, "show", f"{commit}:pyproject.toml"), re.M)
        if m:
            out.append((commit, date.strip(), m.group(1)))
    return out


def wanted(repo: Path) -> dict[str, Release | None]:
    """Each slot and the release it carries (None: there is none)."""
    rels = releases(history(repo))
    return {PREVIOUS: previous_release(rels, pyproject_version(repo)), **{v: pinned_release(rels, v) for v in PINNED}}


def _utc(date: str) -> str:
    """A commit date as UTC (RFC 3339, `Z`)."""
    from datetime import datetime, timezone
    # timezone.utc, not datetime.UTC (3.11+): the host's python3 runs this
    return datetime.fromisoformat(date).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: UP017


def export(repo: Path, out: Path | None = None) -> dict[str, Release | None]:
    """Export every slot's tree into the build context (`out`, default <repo>/agentic/.prev-release); returns what each
    carries. Raises SystemExit, with the reason, when git or the archive fails."""
    out = out or repo / EXPORT_DIR
    out.mkdir(parents=True, exist_ok=True)
    want = wanted(repo)
    for p in out.iterdir():   # what no slot wants: an old layout (TAG, src/ at the top), a slot no longer pinned
        if p.name != ".keep" and p.name not in {s for s, r in want.items() if r is not None}:
            shutil.rmtree(p) if p.is_dir() else p.unlink()
    for slot, rel in want.items():
        if rel is None:
            print(f"prev_release: no release for slot {slot!r} — not exported (a cell that needs it is refused)",
                  file=sys.stderr)
            continue
        d = out / slot
        if (d / "COMMIT").is_file() and (d / "COMMIT").read_text().strip() == rel.commit \
                and (d / "src" / "pyproject.toml").is_file():
            continue
        if d.exists():
            shutil.rmtree(d)
        tmp = Path(tempfile.mkdtemp(prefix=".export-", dir=out))
        tmp.chmod(0o755)   # mkdtemp's 0700 would ride into the image
        try:
            have = set(_git(repo, "ls-tree", "--name-only", rel.commit).split())
            archive = subprocess.run(["git", "-C", str(repo), "archive", "--format=tar", rel.commit,
                                      *[p for p in TREE if p in have]], capture_output=True, timeout=120)
            if archive.returncode != 0:
                raise SystemExit(f"prev_release: git archive {rel.commit[:9]} failed: {archive.stderr[-400:]!r}")
            (tmp / "src").mkdir()
            subprocess.run(["tar", "-x", "-C", str(tmp / "src")], input=archive.stdout, check=True, timeout=120)
            (tmp / "VERSION").write_text(rel.version + "\n")
            (tmp / "COMMIT").write_text(rel.commit + "\n")
            (tmp / "DATE").write_text(_utc(rel.date) + "\n")
            tmp.rename(d)
        finally:
            if tmp.exists():
                shutil.rmtree(tmp)
        print(f"prev_release: exported {slot} = {rel.version} ({rel.commit[:9]}, {rel.date}) into {d}", file=sys.stderr)
    return want


# ---- the jail side -------------------------------------------------------------------------------------------------------

def slot_dir(slot: str, root: Path = ROOT) -> Path:
    if not _SLOT.match(slot):
        raise ValueError(f"not a release slot: {slot!r} (`previous` or a pinned X.Y.Z)")
    return root / slot


def installed(slot: str = PREVIOUS, root: Path = ROOT) -> dict[str, str] | None:
    """What the export said the slot carries — {version, commit, date} — or None when the image carries none."""
    d = slot_dir(slot, root)
    try:
        got = {k.lower(): (d / k).read_text().strip() for k in ("VERSION", "COMMIT", "DATE")}
    except OSError:
        return None
    return got if all(got.values()) else None


def server_argv(slot: str = PREVIOUS, root: Path = ROOT) -> list[str] | None:
    """The slot's MCP server, as the image installed it — None when the image carries none."""
    exe = slot_dir(slot, root) / "venv" / "bin" / "hpc-bridge"
    return [str(exe)] if installed(slot, root) and exe.exists() else None


def installed_version(venv: Path) -> str | None:
    """The version the hpc-bridge package installed in `venv` reports (its dist-info), or None."""
    for meta in sorted(venv.glob("lib/python*/site-packages/hpc_bridge-*.dist-info/METADATA")):
        m = re.search(r"^Version: (\S+)", meta.read_text(errors="replace"), re.M)
        if m:
            return m.group(1)
    return None


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Older hpc-bridge releases, for the returning-user cells.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export", help="export each slot's tree into agentic/.prev-release before `docker build`")
    e.add_argument("repo", type=Path)
    e.add_argument("--out", type=Path, default=None, help="another build context's .prev-release dir")
    t = sub.add_parser("show", help="print each slot's release (nothing is exported)")
    t.add_argument("repo", type=Path)
    args = ap.parse_args(argv)
    if args.cmd == "export":
        export(args.repo.resolve(), args.out.resolve() if args.out else None)
        return 0
    for slot, rel in wanted(args.repo.resolve()).items():
        print(f"{slot}\t{rel.version}\t{rel.commit}\t{rel.date}" if rel else f"{slot}\t-")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
