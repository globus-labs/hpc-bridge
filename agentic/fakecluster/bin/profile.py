#!/usr/bin/env python3
"""Fake-cluster PROFILES with inheritance. A profile dir (profiles/<name>/) holds profile.toml, slurm.conf and
optional gres.conf / job_submit.lua / compose.override.yml / setup.d/<role>[-<tag>].sh / other fixture dirs. A profile
may name its BASE compose file (`compose = "docker-compose.pbs.yml"` — a different scheduler is a different stack) and
may declare `base = "<other>"`: it then LAYERS on that profile — files merged (the derived profile's win),
[capabilities] merged (derived keys win), every compose.override.yml in the chain passed to compose (base first), and
setup.d scripts from every layer kept (the entrypoint sources setup.d/<role>.sh then setup.d/<role>-*.sh, sorted).
`base` may also be a LIST, layered in that order: `base = ["totp", "f2b"]` is f2b layered on totp (see chain()).

  profile.py manifest <name>            -> the MERGED manifest as JSON (targets.py, REQUIRES matching)
  profile.py build <name> <outdir>      -> materialise the merged dir at <outdir> (what compose mounts as /etc/hpcb/profile)
                                           and print shell assignments: PROFILE_OVERLAYS (compose -f files, base first),
                                           PROFILE_NODES, PROFILE_LOGIN_HOSTS
Python 3.11+ (tomllib); no third-party imports — it runs on the host before any venv exists.
"""
from __future__ import annotations

import json
import shlex
import shutil
import sys
import tomllib
from pathlib import Path

PROFILES_DIR = Path(__file__).resolve().parent.parent / "profiles"
_SKIP = {"profile.toml", "compose.override.yml"}  # merged/handled separately, never copied as files


def _bases(d: Path) -> list[str]:
    """The profile's `base`: one name, a list of names (layered in that order), or none."""
    with (d / "profile.toml").open("rb") as fh:
        b = tomllib.load(fh).get("base") or []
    return [b] if isinstance(b, str) else [str(x) for x in b]


def chain(name: str) -> list[Path]:
    """[base…, derived]: the profile dirs to layer, base first. Each base brings its own chain before it, in the order
    `base` lists them, and a profile two bases share is layered ONCE, where it first appears: `totp_f2b` (base =
    ["totp", "f2b"], both on `site`) is site, totp, f2b, totp_f2b. Raises SystemExit on an unknown profile or a cycle."""
    out: list[Path] = []

    def visit(n: str, stack: tuple[str, ...]) -> None:
        if n in stack:
            raise SystemExit(f"profile.py: inheritance cycle at {n!r}")
        d = PROFILES_DIR / n
        if not (d / "profile.toml").is_file():
            have = sorted(p.name for p in PROFILES_DIR.iterdir() if (p / "profile.toml").is_file())
            raise SystemExit(f"profile.py: unknown fake-cluster profile {n!r} (have: {have})")
        for b in _bases(d):
            visit(b, (*stack, n))
        if d not in out:
            out.append(d)

    visit(name, ())
    return out


def manifest(name: str) -> dict:
    """The merged manifest: name/description of the derived profile, capabilities layered base→derived, any other
    top-level table (e.g. [catalog]) from the most-derived layer that defines it, `layers` = the chain's names."""
    merged: dict = {"capabilities": {}}
    layers = chain(name)
    for d in layers:
        with (d / "profile.toml").open("rb") as fh:
            m = tomllib.load(fh)
        caps = m.pop("capabilities", {}) or {}
        m.pop("base", None)
        merged.update(m)
        merged["capabilities"].update(caps)
    merged["layers"] = [d.name for d in layers]
    _resolve_totp_secret(merged)
    return merged


def _resolve_totp_secret(m: dict) -> None:
    """[totp] secret_file = "<path relative to fakecluster/>": the enrolment secret lives in a gitignored file, generated
    (24 base32 chars, 0600) on first use — a secret-SHAPED literal in git trips secret scanners even when it only
    guards a local-only sshd (GitGuardian on PR #112). Exposed to callers as m["totp"]["secret"]."""
    t = m.get("totp")
    if not isinstance(t, dict) or not t.get("secret_file"):
        return
    path = PROFILES_DIR.parent / str(t["secret_file"])
    if not path.is_file() or not path.read_text().strip():
        import secrets

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(secrets.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") for _ in range(24)) + "\n")
        path.chmod(0o600)
    t["secret"] = path.read_text().strip()


def build(name: str, outdir: Path) -> dict:
    """Materialise the merged profile dir (base files first, derived overwrite) IN PLACE and return the manifest.

    The dir is bind-mounted into running containers, so it is synced, never recreated: an rmtree would unlink every
    file under the containers' mount (they keep the old directory inode and suddenly see it empty — live 2026-09-05
    the login node's MEP setup lost its config mid-boot that way). Stale entries are removed one by one."""
    layers = chain(name)
    outdir.mkdir(parents=True, exist_ok=True)
    wanted: set[Path] = set()
    for d in layers:
        for src in d.rglob("*"):
            rel = src.relative_to(d)
            if rel.parts[0] in _SKIP or "__pycache__" in rel.parts:
                continue
            dst = outdir / rel
            wanted.add(dst)
            if src.is_dir():
                dst.mkdir(parents=True, exist_ok=True)
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                if not dst.is_file() or dst.read_bytes() != src.read_bytes():
                    shutil.copy2(src, dst)
    wanted.add(outdir / "profile.toml")
    for stale in sorted((p for p in outdir.rglob("*") if p not in wanted), key=lambda p: -len(p.parts)):
        if stale.is_dir():
            shutil.rmtree(stale, ignore_errors=True)
        else:
            stale.unlink(missing_ok=True)
    m = manifest(name)
    # a flat manifest for the containers / the readiness wait (no `base`, capabilities already merged)
    lines = [f'name = {json.dumps(m["name"])}', f'description = {json.dumps(m.get("description", ""))}',
             f'layers = {json.dumps(m["layers"])}', "", "[capabilities]"]
    for k, v in m["capabilities"].items():
        lines.append(f"{k} = {json.dumps(v)}")
    for k, v in m.items():
        if k in ("name", "description", "layers", "capabilities") or not isinstance(v, dict):
            continue
        lines += ["", f"[{k}]"] + [f"{kk} = {json.dumps(vv)}" for kk, vv in v.items()]
    text = "\n".join(lines) + "\n"
    if not (outdir / "profile.toml").is_file() or (outdir / "profile.toml").read_text() != text:
        (outdir / "profile.toml").write_text(text)
    return m


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[0] == "manifest":
        print(json.dumps(manifest(argv[1]), indent=2))
        return 0
    if len(argv) >= 3 and argv[0] == "build":
        m = build(argv[1], Path(argv[2]))
        overlays = [str(d / "compose.override.yml") for d in chain(argv[1]) if (d / "compose.override.yml").is_file()]
        caps = m["capabilities"]
        print(f"PROFILE_OVERLAYS={shlex.quote(' '.join(overlays))}")
        print(f"PROFILE_NODES={caps.get('nodes', 0)}")
        print(f"PROFILE_LOGIN_HOSTS={shlex.quote(' '.join(map(str, caps.get('login_hosts', []))))}")
        print(f"PROFILE_CATALOG_CMD={shlex.quote(str((m.get('catalog') or {}).get('cmd') or ''))}")
        print(f"PROFILE_HARNESS_SSH_PORT={caps.get('harness_ssh_port', 22)}")
        print(f"PROFILE_COMPOSE={shlex.quote(str(m.get('compose') or 'docker-compose.yml'))}")   # the stack's base compose file
        print(f"PROFILE_SCHEDULER={shlex.quote(str(caps.get('scheduler', 'slurm')))}")
        print(f"PROFILE_TOTP_SECRET={shlex.quote(str((m.get('totp') or {}).get('secret') or ''))}")
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
