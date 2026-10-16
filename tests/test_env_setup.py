"""hpc-bridge's short env_setup line, run in REAL shells (bash, and POSIX sh/dash where installed) against a temp home;
the discovery probe, run for real and parsed; the upgrade-on-read of lines earlier versions generated; and the
worker_init a task carries (the endpoint's own).

Found live on globus1 (2026-10-10): the old guards (`[ -d {venv} ] ||`, `command -v globus-compute-endpoint ||`) kept a
venv whose globus-sdk had drifted, so the gce CLI failed at import on every first bring-up; on a heterogeneous cluster
(x86_64 login, aarch64 compute, shared /home) the replayed worker_init could not run the venv's python and the block died
with a bare exit 2. The line now only activates; the build is Python (`test_endpoint_venv_build.py`). The stubs
(`venv_stubs.py`) behave like the real install. `HPCB_TEST_REAL_GCE_VENV=<a real gce venv>` adds a run against a real
install.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from hpc_bridge import discovery
from hpc_bridge.catalog.bundled import BundledCatalog
from hpc_bridge.facility import endpoint_venv as ev
from tests import venv_stubs as vs

PY = vs.PY
PIN, OLD = "4.16.0", "4.13.0"
SEEDS = Path(__file__).resolve().parents[1] / "src/hpc_bridge/catalog/seed"
SHELLS = vs.shells()


@pytest.fixture(scope="module")
def stubs(tmp_path_factory) -> dict:
    return vs.make_stubs(tmp_path_factory.mktemp("env_setup_stubs"))


@pytest.fixture
def box(tmp_path: Path, stubs: dict, monkeypatch) -> dict:
    monkeypatch.setenv("HPCB_TEST_TEMPLATE", str(stubs["template"]))  # for vs.venv() in this process
    b = vs.make_box(tmp_path, stubs)
    b["key"] = Path(ev.keyed_venv(str(b["venv"]), python=PY, gce=PIN))
    return b


def _line(box: dict, *, base: str | None = None, gce: str = PIN, python: str = PY) -> str:
    return ev.short_env_setup("{venv}", python=python, gce=gce).replace("{venv}", base or str(box["venv"]))


def _run(box: dict, shell: str, *, job: str | None = None, line: str | None = None, pre: str = "",
         path_extra: str = "", **env: str) -> subprocess.CompletedProcess:
    es = line or _line(box)
    # parsl's job script: worker_init on its own lines, then the worker launch runs regardless; else the plugin's own
    # `bash -lc "<env_setup> && globus-compute-endpoint …"`
    cmd = f'{pre}{es}\necho "HPCB_DONE $VIRTUAL_ENV"\n' if job else f'{pre}{es} && echo "HPCB_DONE $VIRTUAL_ENV"'
    e = vs.env(box, path_extra=path_extra, **env)
    if job:
        e[job] = "1108"
    return subprocess.run([shell, "-c", cmd], env=e, capture_output=True, text=True, timeout=120, check=False)


def _activated(r: subprocess.CompletedProcess) -> str | None:
    """The venv the line left activated, compared exactly: the unkeyed {venv}'s path is a prefix of every keyed one."""
    m = re.search(r"^HPCB_DONE (.*)$", r.stdout, re.M)
    return m.group(1) if m else None


def _untouched(box: dict) -> None:
    """The line never builds or installs anything, and never touches what is not its own."""
    assert vs.calls(box) == [] and not box["curl"].exists()
    assert (box["home"] / "keep.txt").read_text() == "not yours"


# ---- why everything checks through the entry point -----------------------------------------------------------------

def test_the_stub_cli_fails_under_python_c_like_the_real_one(box):
    v = vs.venv(box, box["key"], PIN)
    r = subprocess.run([str(v / "bin" / "python"), "-c", "import globus_compute_endpoint.cli"], capture_output=True,
                       text=True, check=False)
    assert r.returncode != 0 and "IndexError" in r.stderr
    assert vs.version_of(v) == PIN  # the entry point the manager runs works


# ---- the line ---------------------------------------------------------------------------------------------------------

def test_the_line_is_short_and_never_builds():
    line = ev.short_env_setup("{venv}", python="3.13", gce="4.18.0")
    assert len(line) <= 470, len(line)  # was ~3.9 KB (round 3); an agent echoing it back corrupted it
    for builder in ("uv ", "pip ", "curl", "python3 -m venv", "mkdir", "rm ", "mv "):
        assert builder not in line, builder
    assert line.count("{venv}") == 1 and '__hb_v="$__hb_b-endpoint-4.18.0-py3.13"' in line
    assert '[ -z "${SLURM_JOB_ID:-}${PBS_JOBID:-}" ]' in line and "exit 1" in line


@pytest.mark.parametrize("shell", SHELLS)
def test_a_login_node_activates_the_keyed_venv(box, shell):
    v = vs.venv(box, box["key"], PIN)
    vs.venv(box, box["venv"], OLD)  # the unkeyed one is there too: not used
    r = _run(box, shell)
    assert r.returncode == 0 and _activated(r) == str(v), r.stderr
    _untouched(box)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_login_node_never_checks(box, shell):
    # broken is the bring-up's business (the Python check repairs it before anything runs): the line only activates
    v = vs.venv(box, box["key"], PIN, broken=True)
    r = _run(box, shell)
    assert r.returncode == 0 and _activated(r) == str(v), r.stderr
    assert "hpc-bridge:" not in r.stderr
    _untouched(box)


@pytest.mark.parametrize("job", ["", "SLURM_JOB_ID", "PBS_JOBID"])
@pytest.mark.parametrize("shell", SHELLS)
def test_a_missing_keyed_venv_falls_back_to_the_unkeyed_one(box, shell, job):
    # an endpoint a plugin up to 0.1.23 started runs from the unkeyed {venv}; an older plugin reading this line, or a
    # block of such an endpoint, finds it
    legacy = vs.venv(box, box["venv"], OLD)
    r = _run(box, shell, job=job or None)
    assert r.returncode == 0 and _activated(r) == str(legacy), r.stderr
    assert not box["key"].exists()
    _untouched(box)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_keyed_directory_that_exists_is_never_passed_over(box, shell):
    # a block that starts while an older plugin builds in place (after `uv venv`, before the install) must not fall
    # back to the unkeyed venv — on globus1 that is the facility MEP's own 4.15.0 install
    vs.venv(box, box["venv"], OLD)
    v = vs.venv(box, box["key"], PIN)
    (v / "bin" / "globus-compute-endpoint").unlink()
    r = _run(box, shell, job="SLURM_JOB_ID")
    assert r.returncode == 1 and f"endpoint venv {v} (built on" in r.stderr and "HPCB_DONE" not in r.stdout
    r = _run(box, shell)
    assert _activated(r) == str(v)  # a login node uses it too (the bring-up checks and repairs it first)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_relocated_venv_still_runs_on_a_worker(box, shell):
    # hpc-bridge builds the venv in a directory of its own and renames it into place: a relocatable venv's entry point
    # finds its python relative to itself (uv 0.12, checked in a container), and the line activates by PATH, not by
    # `bin/activate` (a relocatable one cannot find itself under sh/dash)
    built = vs.venv(box, box["tmp"] / "build.tok", PIN, relocatable=True)
    box["key"].parent.mkdir(parents=True, exist_ok=True)
    os.rename(built, box["key"])
    r = _run(box, shell, job="SLURM_JOB_ID", line=_line(box) + '; echo "GCE=$(command -v globus-compute-endpoint)"')
    assert r.returncode == 0 and _activated(r) == str(box["key"]), r.stderr
    assert f"GCE={box['key']}/bin/globus-compute-endpoint" in r.stdout
    assert vs.version_of(box["key"]) == PIN


@pytest.mark.parametrize("job", ["SLURM_JOB_ID", "PBS_JOBID"])
@pytest.mark.parametrize("shell", SHELLS)
def test_a_worker_uses_a_healthy_keyed_venv(box, shell, job):
    v = vs.venv(box, box["key"], PIN)
    r = _run(box, shell, job=job)
    assert r.returncode == 0 and _activated(r) == str(v), r.stderr
    assert "Globus Compute endpoint version" not in r.stdout  # the check prints nothing into the job's output
    _untouched(box)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_worker_with_a_broken_venv_ends_the_job_clearly(box, shell):
    v = vs.venv(box, box["key"], PIN, broken=True)
    vs.venv(box, box["venv"], OLD)  # broken is not missing: no fallback (its manager runs from the keyed one)
    (v / ".hpcb-arch").write_text(os.uname().machine + "\n")
    r = _run(box, shell, job="SLURM_JOB_ID")
    assert r.returncode == 1 and "HPCB_DONE" not in r.stdout  # `exit 1`: the job ends here, not in a traceback later
    assert f"hpc-bridge: endpoint venv {v} (built on {os.uname().machine}) does not run on" in r.stderr
    assert "cannot import name 'X'" in r.stderr  # the cause, just above
    assert len([ln for ln in r.stderr.splitlines() if ln.startswith("hpc-bridge:")]) == 1  # one line
    _untouched(box)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_worker_of_another_architecture_names_the_mismatch(box, shell):
    # globus1, 2026-10-10: built on the x86_64 login node, replayed on an aarch64 DGX Spark over the shared home
    v = vs.venv(box, box["key"], PIN)
    (v / ".hpcb-arch").write_text("x86_64\n")
    py = v / "bin" / "python"
    py.unlink()
    py.write_bytes(b"\x00\x01\x02\x03 not an executable for this machine \x00\n")  # execve -> Exec format error
    py.chmod(0o755)
    fake = box["tmp"] / "arch"
    fake.mkdir()
    (fake / "uname").write_text('#!/bin/sh\nif [ "$1" = -m ]; then echo aarch64; else exec /usr/bin/uname "$@"; fi\n')
    (fake / "uname").chmod(0o755)
    (fake / "hostname").write_text("#!/bin/sh\necho spark1\n")
    (fake / "hostname").chmod(0o755)
    r = _run(box, shell, job="SLURM_JOB_ID", path_extra=f"{fake}:")
    assert r.returncode == 1 and "HPCB_DONE" not in r.stdout
    assert f"hpc-bridge: endpoint venv {v} (built on x86_64) does not run on spark1 (aarch64)" in r.stderr, r.stderr
    _untouched(box)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_worker_with_no_venv_at_all_fails_clearly(box, shell):
    r = _run(box, shell, job="PBS_JOBID")
    assert r.returncode == 1 and "does not run on" in r.stderr and "built on unknown" in r.stderr
    _untouched(box)


@pytest.mark.parametrize("shell", SHELLS)
def test_the_line_runs_under_set_eu(box, shell):
    # a site profile or a job script with `set -eu`: every list ends in a command that succeeds
    legacy = vs.venv(box, box["venv"], OLD)
    for job in (None, "SLURM_JOB_ID"):
        r = _run(box, shell, job=job, pre="set -eu\n")
        assert r.returncode == 0 and _activated(r) == str(legacy), (job, r.stderr)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_home_relative_venv_is_quoted_and_expands(box, shell):
    # HPC_BRIDGE_REMOTE_VENV=~/hpc-bridge/gce-venv: `~` does not expand inside the line's quotes, `$HOME` does
    line = ev.short_env_setup("~/hpc-bridge/gce-venv", python=PY, gce=PIN)
    assert '__hb_b="$HOME/hpc-bridge/gce-venv"' in line
    v = vs.venv(box, box["key"], PIN)
    r = _run(box, shell, line=line, job="SLURM_JOB_ID")
    assert r.returncode == 0 and _activated(r) == str(v), r.stderr


def test_the_line_survives_the_endpoint_template():
    # the line reaches the block through the manager's sanitized UEC and its template default: both intact
    from hpc_bridge.profile import Profile

    for wi in (ev.short_env_setup("/home/u/hpc-bridge/gce-venv", python="3.13", gce="4.18.0"),
               "type module >/dev/null 2>&1 || . /etc/profile.d/lmod.sh; module load uv/0.12.9; "
               + ev.short_env_setup("/home/u/hpc-bridge/gce-venv", python="3.13", gce="4.18.0")):
        fac = _facility(wi)
        tmpl, defaults = fac.config_template(Profile())
        from hpc_bridge.shapes import shape_config

        uec = {**defaults, **shape_config("compute")}
        assert _render(tmpl, uec)["worker_init"] == wi
        assert _render(tmpl, {k: v for k, v in uec.items() if k != "worker_init"})["worker_init"] == wi


# ---- the discovery probe reports what is there (run for real, then parsed) ------------------------------------------

def _probe(box: dict, monkeypatch, path_extra: str = "", **env: str) -> tuple:
    monkeypatch.setenv("HPC_BRIDGE_REMOTE_VENV", str(box["venv"]))
    monkeypatch.setattr(discovery, "_gce_version", lambda: PIN)  # this client's pin: the keyed path and the note
    e = {"PATH": f"{path_extra}/usr/bin:/bin", "HOME": str(box["home"]), "LANG": "C", **env}
    r = subprocess.run(["bash", "-c", discovery._probe_script()], env=e, capture_output=True, text=True,
                       timeout=60, check=False)
    assert "HPCB_PROBE_END" in r.stdout, r.stderr
    return discovery.parse_probe(r.stdout, ssh_host="h"), r.stdout


@pytest.mark.parametrize(("old", "says"), [
    ({}, "is reused"),
    ({"version": OLD}, "it will be rebuilt at start"),
    ({"broken": True}, "fails its check (ImportError: cannot import name 'X'"),
], ids=["healthy", "stale", "broken"])
def test_the_probe_reads_the_keyed_venv(box, monkeypatch, old, says):
    v = vs.venv(box, box["key"], old.get("version", PIN), broken=old.get("broken", False))
    (draft, notes), out = _probe(box, monkeypatch)
    assert f"VENV={v}" in out
    env = next(n for n in notes if n.startswith("env_setup"))
    assert says in env, env
    assert draft.env_setup == discovery._UV_ENV_SETUP or draft.env_setup.endswith(discovery._UV_ENV_SETUP)
    assert (v / "sentinel").read_text() == "old" and not (v / ".hpcb-arch").exists()  # read-only
    assert not list(v.rglob("__pycache__"))  # bytecode writes off


def test_the_probe_does_not_call_a_non_venv_ours(box, monkeypatch):
    box["key"].mkdir(parents=True)
    (box["key"] / "precious").write_text("mine")
    (_draft, notes), out = _probe(box, monkeypatch)
    assert "VENV_STATE=not a venv" in out
    assert any("is NOT a venv: hpc-bridge will not replace it" in n for n in notes)


def test_the_probe_reads_an_entry_points_interpreter_from_uvs_trampoline(box, monkeypatch):
    v = vs.venv(box, box["home"] / "tools" / "gce", PIN)
    (draft, _notes), out = _probe(box, monkeypatch, path_extra=f"{v / 'bin'}:")
    assert f"GCE_STATE={PY} {PIN}" in out
    assert draft.env_setup.startswith("source ") and draft.env_setup.endswith("/tools/gce/bin/activate")  # used


def test_the_probe_does_not_guess_the_interpreter_from_the_directory(box, monkeypatch):
    # a `pip install --user` entry point in ~/.local/bin, next to a python3 shim of another version: the entry point's
    # own trampoline names the interpreter it really runs
    v = vs.venv(box, box["home"] / "tools" / "gce", PIN)
    local = box["home"] / ".local" / "bin"
    local.mkdir(parents=True)
    shutil.copy(v / "bin" / "globus-compute-endpoint", local / "globus-compute-endpoint")
    (local / "python3").write_text("#!/bin/sh\necho 3.9\n")
    (local / "python3").chmod(0o755)
    (draft, _notes), out = _probe(box, monkeypatch, path_extra=f"{local}:")
    assert f"GCE={local}/globus-compute-endpoint" in out and f"GCE_STATE={PY} {PIN}" in out
    assert "GCE_ACTIVATE=no" in out and draft.env_setup == "true"  # used from PATH: no `activate` to source there


def test_the_probe_reads_a_plain_shebang(box, monkeypatch):
    v = vs.venv(box, box["home"] / "tools" / "gce", PIN)
    ep = v / "bin" / "globus-compute-endpoint"
    ep.write_text(f"#!{v}/bin/python\nimport sys\nfrom globus_compute_endpoint.cli import main\nsys.exit(main())\n")
    _draft, out = _probe(box, monkeypatch, path_extra=f"{v / 'bin'}:")
    assert f"GCE_STATE={PY} {PIN}" in out


@pytest.mark.parametrize("launcher", ["'''exec' \"{py}\" \"$0\" \"$@\"", "'''exec' {py} \"$0\" \"$@\""],
                         ids=["double-quoted", "bare"])
def test_the_probe_reads_pips_long_path_launcher(box, monkeypatch, launcher):
    v = vs.venv(box, box["home"] / "tools" / "gce", PIN)
    ep = v / "bin" / "globus-compute-endpoint"
    ep.write_text("#!/bin/sh\n" + launcher.format(py=v / "bin" / "python") + "\n' '''\nimport sys\n"
                  "from globus_compute_endpoint.cli import main\nsys.exit(main())\n")
    _draft, out = _probe(box, monkeypatch, path_extra=f"{v / 'bin'}:")
    assert f"GCE_STATE={PY} {PIN}" in out, out


def test_the_probe_passes_over_a_broken_endpoint_on_path(box, monkeypatch):
    v = vs.venv(box, box["home"] / "tools" / "gce", PIN, broken=True)
    (draft, notes), out = _probe(box, monkeypatch, path_extra=f"{v / 'bin'}:")
    assert f"GCE={v}/bin/globus-compute-endpoint" in out and "GCE_STATE=ImportError" in out
    assert "source " not in draft.env_setup  # hpc-bridge's own venv instead
    assert any("fails its check" in n and "not used" in n for n in notes)


def test_a_probe_that_times_out_does_not_call_a_slow_install_broken(box, monkeypatch):
    v = vs.venv(box, box["home"] / "tools" / "gce", PIN)
    slow = box["tmp"] / "slow"
    slow.mkdir()
    (slow / "timeout").write_text("#!/bin/sh\nexit 124\n")  # the 20 s cap hit
    (slow / "timeout").chmod(0o755)
    (draft, notes), out = _probe(box, monkeypatch, path_extra=f"{slow}:{v / 'bin'}:")
    assert "GCE_STATE=timed out" in out
    assert draft.env_setup.startswith("source ") and not any("fails its check" in n for n in notes)
    assert any("timed out" in n and "confirm it works" in n for n in notes)


# ---- a line an earlier hpc-bridge generated is read as the short line ---------------------------------------------

_INIT = "type module >/dev/null 2>&1 || . /etc/profile.d/lmod.sh; module load uv/0.5; "
_OLD_UV = ("[ -d {venv} ] || uv venv {venv} --python 3.13; . {venv}/bin/activate; "
           "command -v globus-compute-endpoint >/dev/null 2>&1 || uv pip install -q 'globus-compute-endpoint==4.18.0'")
_OLD_LINES = {
    "uv (earliest)": ("[ -d {venv} ] || uv venv {venv}; . {venv}/bin/activate; "
                      "command -v globus-compute-endpoint >/dev/null 2>&1 || uv pip install -q globus-compute-endpoint"),
    "uv pinned": _OLD_UV,
    "uv, resolved path": _OLD_UV.replace("{venv}", "/home/u/hpc-bridge/gce-venv"),
    "uv, home path": _OLD_UV.replace("{venv}", "~/gce"),
    "bootstrap": ('export PATH="$HOME/.local/bin:$PATH"; command -v uv >/dev/null 2>&1 || '
                  "curl -LsSf https://astral.sh/uv/install.sh | sh; " + _OLD_UV),
    "module uv": _INIT + _OLD_UV,
    "module pip": ("module load python/3.13; [ -d {venv} ] || python3 -m venv {venv}; . {venv}/bin/activate; "
                   "command -v globus-compute-endpoint >/dev/null 2>&1 || pip install -q 'globus-compute-endpoint==4.18.0'"),
}


@pytest.mark.parametrize("old", list(_OLD_LINES.values()), ids=list(_OLD_LINES))
def test_a_line_an_earlier_hpc_bridge_generated_is_upgraded(old):
    new = ev.migrate_env_setup(old, python="3.13", gce="4.18.0")
    assert new != old and ev.is_line(new) and "command -v globus-compute-endpoint" not in new
    base = "{venv}" if "{venv}" in old else ("$HOME/gce" if "~/gce" in old else "/home/u/hpc-bridge/gce-venv")
    assert f'__hb_b="{base}"; __hb_v="$__hb_b-endpoint-4.18.0-py3.13"' in new  # same venv base, this client's key
    if old.startswith(_INIT):
        assert new == _INIT + ev.short_env_setup("{venv}", python="3.13", gce="4.18.0")  # the module stays
    elif old.startswith("module load python/3.13; "):
        assert new.startswith("module load python/3.13; __hb_b=")
    else:
        assert new.startswith("__hb_b=")  # uv bootstrap and the guards: the build does that now
    assert ev.migrate_env_setup(new, python="3.13", gce="4.18.0") == new  # idempotent


def test_the_expanse_seed_keeps_its_text_and_is_read_as_the_short_line():
    # the registry is not re-ingested: plugins up to 0.1.23 keep reading the old text; this one upgrades it on read
    entry = next(e for e in BundledCatalog(SEEDS / "sdsc-expanse.yaml").entries() if e.id == "expanse")
    es = entry.compute.env_setup
    assert "command -v globus-compute-endpoint" in es and '"globus-compute-endpoint==4.16.0"' in es  # unchanged seed
    assert ev.migrate_env_setup(es, python="3.13", gce="4.18.0") == ev.short_env_setup(
        "{venv}", python="3.13", gce="4.18.0")
    from hpc_bridge.facility.remote import profile_from_catalog_entry

    prof = profile_from_catalog_entry(entry, user="alice", account="chi152")
    mv = ev.managed_venv(prof.env_setup)
    assert mv is not None and mv.base == "/home/alice/hpc-bridge/gce-venv" and prof.worker_init == prof.env_setup
    assert mv.gce == ev.client_gce() and mv.python == ev.client_python()


@pytest.mark.parametrize("hand", [
    "module load gce && source ~/gce/bin/activate",
    _OLD_UV + " ",                                          # one byte off: not ours
    "echo site; " + _OLD_UV,                                # ours, with the user's own prefix: theirs now
    _OLD_UV.replace("uv pip install -q", "uv pip install"),
    _OLD_UV.replace("{venv}/bin/activate", "/elsewhere/bin/activate"),  # two different venvs: hand-edited
    _OLD_UV.replace("{venv}", "/x/$(id)"),                  # a path the short line could not quote as it is
    # the MEP seeds' worker_init (never read through the SSH profile, and not ours to rewrite anyway)
    "[ -d $HOME/hpc-bridge/gce-venv ] || uv venv $HOME/hpc-bridge/gce-venv; . $HOME/hpc-bridge/gce-venv/bin/activate; "
    "uv pip install -q globus-compute-endpoint==4.15.0 parsl==2026.4.20",
    "",
])
def test_a_hand_written_line_is_never_touched(hand):
    assert ev.migrate_env_setup(hand, python="3.13", gce="4.18.0") == hand


def test_our_line_is_rekeyed_to_this_clients_versions():
    # a client upgrade builds a new venv beside the old one; the prefix and the base stay
    old = "module load uv; " + ev.short_env_setup("/s/gce", python="3.12", gce="4.16.0")
    new = ev.migrate_env_setup(old, python="3.13", gce="4.18.0")
    assert new == "module load uv; " + ev.short_env_setup("/s/gce", python="3.13", gce="4.18.0")


def test_a_cached_facility_gets_the_upgraded_line():
    from hpc_bridge import binding
    from hpc_bridge.facility.remote import profile_from_catalog_entry
    from hpc_bridge.models import FacilityDetails

    d = FacilityDetails(ssh_host="globus1", interface="eth0", env_setup=_OLD_LINES["uv pinned"],
                        scratch_root="/home/{user}/.x", partition="main")
    entry = binding._entry_from_details("globus1", d)
    assert entry.compute.env_setup == _OLD_LINES["uv pinned"]  # the cached text stays as it is
    prof = profile_from_catalog_entry(entry, user="u", account="")
    assert prof.env_setup == ev.short_env_setup("/home/u/hpc-bridge/gce-venv")  # read as the short line


def test_an_unsafe_remote_venv_is_refused_for_our_line():
    from hpc_bridge import binding
    from hpc_bridge.facility.remote import profile_from_catalog_entry
    from hpc_bridge.models import FacilityDetails

    d = FacilityDetails(ssh_host="h", interface="eth0", env_setup=ev.short_env_setup("{venv}"),
                        scratch_root="/home/{user}/.x", partition="main")
    entry = binding._entry_from_details("h", d)
    with pytest.raises(ValueError, match="must be an absolute or"):
        profile_from_catalog_entry(entry, user="u", account="", venv='/home/u/"$(id)"')
    prof = profile_from_catalog_entry(entry, user="u", account="", venv="~/gce")
    assert ev.managed_venv(prof.env_setup).base == "$HOME/gce"


# ---- the keyed venv collides with no seed's venv -------------------------------------------------------------------

def test_the_keyed_venv_collides_with_no_seeds_venv():
    # Delta's and Anvil's MEP worker_init build `$HOME/hpc-bridge/gce-venv-<gce>-py<X.Y>` and upgrade it on every block;
    # the globus-cluster MEP's is `$HOME/hpc-bridge/gce-venv`; alcf-polaris has none (env_setup "" by design)
    base = "/home/u/hpc-bridge/gce-venv"
    ours = {ev.keyed_venv(base, python=py, gce=g) for py in ("3.11", "3.12", "3.13", "3.14")
            for g in ("4.9.0", "4.15.0", "4.16.0", "4.18.0", None)}
    theirs: set[str] = set()
    seen = set()
    for entry in BundledCatalog(SEEDS).entries():
        seen.add(entry.id)
        for path in re.findall(r"\$HOME/hpc-bridge/[\w.{}-]+", entry.compute.env_setup):
            for py in ("3.11", "3.12", "3.13", "3.14"):
                for g in ("4.9.0", "4.15.0", "4.16.0", "4.18.0"):
                    theirs.add(path.replace("$HOME", "/home/u").replace("{gce_version}", g)
                               .replace("{python_version}", py).replace("{venv}", base))
    assert {"polaris", "delta", "anvil", "expanse", "globus-labs"} <= seen  # every seed, incl. alcf-polaris
    polaris = next(e for e in BundledCatalog(SEEDS).entries() if e.id == "polaris")
    assert polaris.compute.env_setup == ""
    assert theirs and not (ours & theirs), ours & theirs
    assert base not in ours  # never the unkeyed path either (the globus-cluster MEP's install)


# ---- the worker_init a task carries: the endpoint's own -----------------------------------------------------------

def _sanitize(opts: dict) -> dict:
    """The endpoint manager's `_sanitize_user_json`: the real one where globus-compute-endpoint is installed (Linux CI),
    else this copy of gce 4.18's (globus-compute, Apache-2.0)."""
    try:
        from globus_compute_endpoint.endpoint.config.utils import _sanitize_user_json
    except ImportError:
        def _sanitize_user_json(user_json: dict) -> dict:
            def inner(data):
                if isinstance(data, dict):
                    return {k: inner(v) for k, v in data.items()}
                if isinstance(data, list):
                    return [inner(v) for v in data]
                if isinstance(data, str):
                    return json.dumps(data)
                if isinstance(data, int | float):
                    return data
                if data is None:
                    return "null"
                raise ValueError(f"{type(data).__name__} is not a valid user config option type")
            return inner(user_json)
    return _sanitize_user_json(opts)


def _render(tmpl: str, uec: dict) -> dict:
    """The UEP config as gce 4.18's manager renders it: sanitized UEC, sandboxed Jinja, StrictUndefined, safe_load."""
    import jinja2
    import yaml
    from jinja2.sandbox import SandboxedEnvironment

    return yaml.safe_load(SandboxedEnvironment(undefined=jinja2.StrictUndefined).from_string(tmpl)
                          .render(**_sanitize(uec)))["engine"]["provider"]


def _facility(wi: str, sched: str = "slurm", client=None):
    from hpc_bridge.facility.remote import MachineProfile, SlurmFacility

    prof = MachineProfile(name="m", endpoint_name="hpc-bridge-m", env_setup=wi, interface="ib0", partition="p",
                          account="a", worker_init=wi, scheduler=sched)
    return SlurmFacility(prof, cli=None, client_factory=client)


_WORKER_INITS = {
    "the short line": ev.short_env_setup("/home/u/hpc-bridge/gce-venv", python="3.13", gce="4.18.0"),
    "the old discovery line": _OLD_UV.replace("{venv}", "/home/u/hpc-bridge/gce-venv"),
    "true": "true",
    "colon-space": 'module load gce; export PS1="h: "',
    "space-hash": "module load gce # site default",
    "quotes, backslash, $": "echo 'a' \"b\" \\x \\\\ $HOME ${X:-y} $(date) `id`",
    "newline": "module load gce\nsource /x/bin/activate",
    "jinja braces": "echo {{ 7*7 }} {% raw %}x{% endraw %} {# c #} }} %} #}",
    "leading dash": "- module load gce",
    "a JSON string": '"quoted"',
    "yes": "yes",
    "123": "123",
    "non-ascii": "export NAME='Zoë 中文'; module load gce",
}


@pytest.mark.parametrize("sched", ["slurm", "pbs"])
@pytest.mark.parametrize("wi", list(_WORKER_INITS.values()), ids=list(_WORKER_INITS))
def test_every_worker_init_reaches_the_block_intact(sched, wi):
    from hpc_bridge.profile import Profile
    from hpc_bridge.shapes import shape_config

    fac = _facility(wi, sched)
    tmpl, defaults = fac.config_template(Profile())
    uec = {**defaults, **shape_config("compute")}
    assert uec["worker_init"] == wi  # every task carries one
    assert _render(tmpl, uec)["worker_init"] == wi
    # an endpoint configured by a plugin up to 0.1.23: its template's default was encoded once, and was never what a
    # block used (the task's config always carried worker_init) — it still is not
    old_tmpl = tmpl.replace(json.dumps(json.dumps(wi)), json.dumps(wi))
    assert old_tmpl != tmpl
    assert _render(old_tmpl, uec)["worker_init"] == wi
    # and with no worker_init in the task's config, the template's own default reaches the block intact
    assert _render(tmpl, {k: v for k, v in uec.items() if k != "worker_init"})["worker_init"] == wi


@pytest.mark.parametrize("encode", [1, 2], ids=["single (up to 0.1.23)", "double"])
def test_the_configured_worker_init_is_read_back_from_the_template(encode):
    from hpc_bridge.facility.remote import configured_worker_init
    from hpc_bridge.profile import Profile

    for wi in _WORKER_INITS.values():
        tmpl, _ = _facility(wi).config_template(Profile())
        if encode == 1:
            tmpl = tmpl.replace(json.dumps(json.dumps(wi)), json.dumps(wi))
        assert configured_worker_init(tmpl, prefer=wi) == wi
        if not (wi.startswith('"') and wi.endswith('"')):
            assert configured_worker_init(tmpl) == wi  # unambiguous: no preference needed
    assert configured_worker_init("engine: {}\n") is None and configured_worker_init(None) is None
    assert configured_worker_init("    worker_init: {{ worker_init | default(not json) }}\n") is None


def test_an_ambiguous_single_encoding_prefers_the_profiles_reading():
    # a once-encoded value that is itself a JSON string reads two ways; the one equal to the profile's wins
    from hpc_bridge.facility.remote import configured_worker_init

    tmpl = '    worker_init: {{ worker_init | default(' + json.dumps('"quoted"') + ') }}\n'
    assert configured_worker_init(tmpl, prefer='"quoted"') == '"quoted"'   # single-encoded '"quoted"'
    assert configured_worker_init(tmpl, prefer="quoted") == "quoted"       # double-encoded 'quoted'
    assert configured_worker_init(tmpl, prefer="other") == "quoted"        # neither: the current (double) encoding


class _Client:
    def __init__(self, template=None, exc=None, md=None):
        self.template, self.exc, self.calls, self.md = template, exc, 0, md

    def get_endpoint_metadata(self, eid):
        self.calls += 1
        if self.exc:
            raise self.exc
        return self.md if self.md is not None else {"user_config_template": self.template}


def _old_template(wi: str) -> str:
    from hpc_bridge.profile import Profile

    return _facility(wi).config_template(Profile())[0].replace(json.dumps(json.dumps(wi)), json.dumps(wi))


async def test_a_reused_endpoint_keeps_the_worker_init_it_was_configured_with():
    hand = "module load gce-old && source /x/bin/activate"
    client = _Client(_old_template(hand))
    new = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    fac = _facility(new, client=lambda: client)  # this session's profile: an edited env_setup
    await fac.adopt_endpoint("eid-1")
    await fac.adopt_endpoint("eid-1")  # once per endpoint
    assert client.calls == 1
    sent = fac.dispatch_uec({"worker_init": new, "compute": True})
    assert sent["worker_init"] == hand  # the blocks run what the manager was started with
    note = fac.worker_init_note
    assert "Only if its blocks fail or you changed env_setup" in note and "teardown_endpoint, then connect_facility" in note
    assert fac.dispatch_uec({"compute": False}) == {"compute": False}  # nothing added where nothing was sent


async def test_a_reused_endpoint_gives_fresh_compute_user_endpoints_only_if_its_template_lets_them_exit():
    # the upgrade path: an endpoint configured by an earlier plugin keeps one compute user endpoint (a fresh one per
    # block request would never exit there); its published template says which it is — no SSH, once per endpoint
    from hpc_bridge.profile import Profile

    wi = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    ours = _facility(wi).config_template(Profile())[0]
    fac = _facility(wi, client=lambda: _Client(ours))
    await fac.adopt_endpoint("eid-1")
    assert fac.fresh_ueps is True
    earlier = "\n".join(line for line in ours.splitlines() if "idle_heartbeats_soft" not in line)
    fac = _facility(wi, client=lambda: _Client(earlier))
    await fac.adopt_endpoint("eid-1")
    assert fac.fresh_ueps is False
    fac = _facility(wi, client=lambda: _Client(exc=RuntimeError("503")))
    await fac.adopt_endpoint("eid-1")
    assert fac.fresh_ueps is False                                     # unreadable: never presumed


@pytest.mark.parametrize("current", ["", "module load gce && source /x/bin/activate"], ids=["emptied", "hand-written"])
async def test_replacing_hpc_bridges_line_on_a_running_endpoint_warns(current):
    started = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    fac = _facility(current, client=lambda: _Client(_old_template(started)))
    await fac.adopt_endpoint("eid-1")
    note = fac.worker_init_note
    assert note.startswith("this endpoint was started with a different env_setup")
    assert "WARNING: the current env_setup is not hpc-bridge's own line" in note
    assert "cannot fix a pilot that says its venv does not run on the compute nodes' architecture" in note
    assert "Teardown still stops this endpoint through the venv it was started with" in note


async def test_hpc_bridges_line_with_a_path_it_cannot_build_into_is_not_called_hand_written():
    started = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    current = ev.short_env_setup("/home/u/my dir/gce-venv")  # its own line, but a path managed_venv refuses
    assert ev.is_line(current) and ev.managed_venv(current) is None
    fac = _facility(current, client=lambda: _Client(_old_template(started)))
    await fac.adopt_endpoint("eid-1")
    assert "Only if its blocks fail" in fac.worker_init_note and "WARNING" not in fac.worker_init_note


async def test_editing_between_lines_of_hpc_bridges_or_hand_written_ones_does_not_warn():
    ours = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    for started, current in ((ours, "module load x; " + ours), ("source /a/bin/activate", "source /b/bin/activate"),
                             ("source /a/bin/activate", ours)):
        fac = _facility(current, client=lambda s=started: _Client(_old_template(s)))
        await fac.adopt_endpoint("eid-1")
        assert "Only if its blocks fail" in fac.worker_init_note and "WARNING" not in fac.worker_init_note


async def test_an_upgraded_line_is_not_reported_as_an_edit():
    # an endpoint a plugin up to 0.1.23 started (its template: the old guarded line); this plugin reads the cached config
    # as the short line. The endpoint's own line is sent; the difference is the upgrade, not an edit: no note
    own = _OLD_UV.replace("{venv}", "/home/u/hpc-bridge/gce-venv")
    profile_line = ev.migrate_env_setup(own)
    fac = _facility(profile_line, client=lambda: _Client(_old_template(own)))
    await fac.adopt_endpoint("eid-1")
    assert fac.endpoint_worker_init == own and fac.worker_init_note == ""
    assert fac.dispatch_uec({"worker_init": profile_line})["worker_init"] == own
    # likewise an endpoint this plugin started at an older SDK version (its line keyed for that version)
    older = ev.short_env_setup("/home/u/hpc-bridge/gce-venv", python=ev.client_python(), gce="4.0.0")
    fac = _facility(ev.migrate_env_setup(older), client=lambda: _Client(_old_template(older)))
    await fac.adopt_endpoint("eid-2")
    assert fac.endpoint_worker_init == older and fac.worker_init_note == ""


async def test_an_endpoint_whose_template_matches_gets_no_note_and_an_unreadable_one_the_profiles():
    from hpc_bridge.profile import Profile

    new = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    tmpl = _facility(new).config_template(Profile())[0]
    fac = _facility(new, client=lambda: _Client(tmpl))
    await fac.adopt_endpoint("eid-1")
    assert fac.worker_init_note == "" and fac.dispatch_uec({"worker_init": new})["worker_init"] == new
    for client in (_Client(exc=RuntimeError("no metadata")), _Client("not our template"), _Client(md={})):
        fac = _facility(new, client=lambda c=client: c)
        await fac.adopt_endpoint("eid-2")
        assert fac.endpoint_worker_init is None and fac.worker_init_note == ""
        assert fac.dispatch_uec({"worker_init": new})["worker_init"] == new  # the pre-change behaviour


async def test_a_failed_or_cancelled_read_is_tried_again():
    import asyncio

    new = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    hand = "source /x/bin/activate"
    client = _Client(exc=RuntimeError("503"))
    fac = _facility(new, client=lambda: client)
    await fac.adopt_endpoint("eid-1")
    assert fac.endpoint_worker_init is None
    client.exc, client.template = None, _old_template(hand)
    await fac.adopt_endpoint("eid-1")  # not cached: read again
    assert client.calls == 2 and fac.endpoint_worker_init == hand

    class _Slow(_Client):
        def get_endpoint_metadata(self, eid):
            import time
            time.sleep(0.3)
            return super().get_endpoint_metadata(eid)

    slow = _Slow(_old_template(hand))
    fac = _facility(new, client=lambda: slow)
    t = asyncio.ensure_future(fac.adopt_endpoint("eid-9"))
    await asyncio.sleep(0.05)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert fac._adopted is None  # a cancelled read is not cached
    await fac.adopt_endpoint("eid-9")
    assert fac.endpoint_worker_init == hand


async def test_a_new_endpoint_id_resets_what_was_learned():
    hand = "source /x/bin/activate"
    new = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    clients = {"a": _Client(_old_template(hand)), "b": _Client(exc=RuntimeError("down"))}
    which = {"c": "a"}
    fac = _facility(new, client=lambda: clients[which["c"]])
    await fac.adopt_endpoint("eid-a")
    assert fac.endpoint_worker_init == hand and fac.worker_init_note
    which["c"] = "b"
    await fac.adopt_endpoint("eid-b")  # another endpoint, unreadable: nothing of eid-a's carries over
    assert fac.endpoint_worker_init is None and fac.worker_init_note == ""
    assert fac.dispatch_uec({"worker_init": new})["worker_init"] == new


async def test_provision_forgets_the_adoption_after_writing_the_config():
    # a same-id restart: the template now carries the profile's worker_init; the next adopt reads it again
    from hpc_bridge.facility.remote import SlurmFacility
    from hpc_bridge.profile import Profile

    hand = "source /x/bin/activate"
    new = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    client = _Client(_old_template(hand))

    class _CLI:
        target = None

        async def status(self, name):
            return "configured"

        async def endpoint_id(self, name):
            return "eid-1"

        async def clean_uep_pidfiles(self, eid):
            pass

        async def write_config(self, name, manager, uep):
            client.template = uep  # what the web service publishes after the restart

        async def start(self, name):
            return "eid-1", None, None

    fac = _facility(new, client=lambda: client)
    fac = SlurmFacility(fac.profile, cli=_CLI(), client_factory=lambda: client)
    await fac.adopt_endpoint("eid-1")
    assert fac.endpoint_worker_init == hand and fac.worker_init_note
    await fac.provision(Profile())
    # we wrote this template and started it: its worker_init is the profile's, nothing is read back
    assert fac._adopted == "eid-1" and fac.endpoint_worker_init == new and fac.worker_init_note == ""
    await fac.adopt_endpoint("eid-1")
    assert client.calls == 1 and fac.dispatch_uec({"worker_init": new})["worker_init"] == new


async def test_a_same_id_restart_never_adopts_the_stale_registration():
    # review 2026-10-13: a stopped endpoint an older plugin configured (a login-node reboot) is restarted with this
    # plugin's template; `start` returns at once and the daemon re-registers seconds later, so the web service still
    # serves the OLD template when warmth._provision adopts. That read must not stick for the session.
    from hpc_bridge.facility.remote import SlurmFacility
    from hpc_bridge.profile import Profile

    venv = "/home/u/hpc-bridge/gce-venv"
    old = _OLD_UV.replace("{venv}", venv)
    new = ev.migrate_env_setup(old)
    client = _Client(_old_template(old))  # what the web serves until the daemon re-registers

    class _CLI:
        target = None

        async def status(self, name):
            return "configured"  # endpoint dir exists, manager not running

        async def endpoint_id(self, name):
            return "eid-1"

        async def clean_uep_pidfiles(self, eid):
            pass

        async def write_config(self, name, manager, uep):
            pass  # on disk only: the web hears of it when the daemon re-registers

        async def start(self, name):
            return "eid-1", None, None

    fac = SlurmFacility(_facility(new).profile, cli=_CLI(), client_factory=lambda: client)
    await fac.provision(Profile())
    await fac.adopt_endpoint("eid-1")  # warmth._provision, right after the bootstrap
    assert client.calls == 0
    assert fac.dispatch_uec({"worker_init": new})["worker_init"] == new  # the template we just started it with
    client.template = fac.config_template(Profile())[0]  # the daemon has re-registered: still nothing to change
    await fac.adopt_endpoint("eid-1")
    assert fac.dispatch_uec({"worker_init": new})["worker_init"] == new and fac.worker_init_note == ""


async def test_a_start_that_fails_forgets_the_old_adoption():
    # the daemon may register anyway: the retry's reuse must read the endpoint again, not keep the pre-restart line
    from hpc_bridge.facility.remote import SlurmFacility
    from hpc_bridge.profile import Profile

    hand = "source /x/bin/activate"
    new = ev.short_env_setup("/home/u/hpc-bridge/gce-venv")
    client = _Client(_old_template(hand))

    class _CLI:
        target = None

        async def status(self, name):
            return "configured"

        async def endpoint_id(self, name):
            return "eid-1"

        async def clean_uep_pidfiles(self, eid):
            pass

        async def write_config(self, name, manager, uep):
            pass

        async def start(self, name):
            raise RuntimeError("remote start failed (rc=1): boom")

    fac = SlurmFacility(_facility(new).profile, cli=_CLI(), client_factory=lambda: client)
    await fac.adopt_endpoint("eid-1")
    assert fac.endpoint_worker_init == hand
    with pytest.raises(RuntimeError, match="boom"):
        await fac.provision(Profile())
    assert fac._adopted is None and fac.endpoint_worker_init is None and fac.worker_init_note == ""


async def test_endpoint_metadata_is_a_dict_even_when_the_service_returns_nothing():
    from hpc_bridge.facility.base import endpoint_metadata

    assert await endpoint_metadata(lambda: _Client(md=None, template=None), "e") == {"user_config_template": None}

    class _Nothing:
        def get_endpoint_metadata(self, eid):
            return None

    md = await endpoint_metadata(lambda: _Nothing(), "e")
    assert md == {} and md is not None


async def test_the_endpoint_is_adopted_before_its_first_task_is_built():
    # every path to an endpoint id passes _provision; the first runner must already carry the endpoint's own
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from hpc_bridge.server import _ensure_endpoint_up
    from tests.fakes import FakeFacility

    order: list[str] = []

    class _Fac(FakeFacility):
        def config_template(self, profile):
            return "tmpl", {"worker_init": "PROFILE"}

        async def adopt_endpoint(self, eid):
            order.append(f"adopt {eid}")

        def dispatch_uec(self, uec):
            return {**uec, "worker_init": "OWN"}

    class _Runner:
        def __init__(self, eid, uec):
            order.append(f"runner {uec.get('worker_init')}")
            self.endpoint_id = eid

        async def canary(self, timeout=8.0):
            from hpc_bridge.runner import CanaryResult
            return CanaryResult(ok=True, worker_host="n1", worker_python="3.13")

        def close(self):
            pass

    f = _Fac()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _Runner(eid, user_endpoint_config)
    await _ensure_endpoint_up(app, confirm_spend=True)
    assert order[:2] == ["adopt fake-eid", "runner OWN"], order


def _app_with_note(note: str):
    class _App:
        class facility:
            worker_init_note = note
            profile = None
    return _App()


@pytest.mark.parametrize(("rows", "category"), [
    ("F 1108 2 FAILED\n", "finished"),
    ("PENDING 1108 - Priority\n", "queued"),
    ("RUNNING 1108 - None\n", "starting"),
    ("PENDING 1108 - QOSMaxJobsPerUserLimit\n", "held"),
])
def test_the_edited_env_setup_note_rides_only_a_finished_pilot(rows, category):
    import asyncio

    from hpc_bridge import scheduler_ops
    from hpc_bridge.models import ShellOutcome

    note = "this endpoint was started with a different env_setup than this facility's current one. "

    async def run_login(cmd):
        return ShellOutcome(phase="complete", exit_code=0, stdout=rows, stderr="", block_state="warm")

    assert scheduler_ops._summarize_pilot(rows, 30.0)[0] == category
    out = asyncio.run(scheduler_ops._augment_provisioning_notice(_app_with_note(note), "eid", "allocating nodes…",
                                                                  30.0, run_login))
    assert (note.strip() in out) is (category == "finished"), out


_SAID = ("hpc-bridge: endpoint venv /home/u/hpc-bridge/gce-venv-endpoint-4.18.0-py3.13 (built on x86_64) does not "
         "run on spark1 (aarch64)")


def test_a_finished_pilot_on_another_architecture_is_not_told_to_rebuild():
    from hpc_bridge import scheduler_ops

    cat, notice = scheduler_ops._summarize_pilot(f"F 1108 1 FAILED\nHPCB_SAID 1108 {_SAID}\n", 30.0)
    assert cat == "finished" and "built on x86_64) does not run on spark1 (aarch64)" in notice
    assert "built on the login node (x86_64) and these compute nodes (aarch64) cannot run it" in notice
    assert "Rebuilding does not help" in notice and "partition whose nodes are x86_64" in notice
    assert "troubleshooting" in notice and "Tear the endpoint down" not in notice
    # live on globus1 (2026-10-13): the agent emptied env_setup to "fix" it, which broke the teardown's CLI
    assert "Do NOT change env_setup" in notice and "the running endpoint keeps the one it was started with" in notice
    assert "multi-user endpoint, if list_facilities has one" in notice and "planned, not built" in notice
    assert "globus-labs" not in notice  # generic: any facility's own multi-user endpoint


@pytest.mark.parametrize("built", ["aarch64", "unknown"])
def test_a_finished_pilot_on_the_same_architecture_is_told_to_rebuild(built):
    from hpc_bridge import scheduler_ops

    said = _SAID.replace("(built on x86_64)", f"(built on {built})")
    cat, notice = scheduler_ops._summarize_pilot(f"F 1108 1 FAILED\nHPCB_SAID 1108 {said}\n", 30.0)
    assert cat == "finished" and "Tear the endpoint down and connect again" in notice
    assert "Rebuilding does not help" not in notice and "Do NOT change env_setup" not in notice


def test_the_quoted_line_is_the_dead_pilots_own_and_only_the_exact_message():
    from hpc_bridge import scheduler_ops

    # another (older) job said it; the newest died job did not: nothing is quoted
    cat, notice = scheduler_ops._summarize_pilot(f"F 1107 1 FAILED\nF 1108 2 FAILED\nHPCB_SAID 1107 {_SAID}\n", 30.0)
    assert cat == "finished" and "its job said" not in notice and "read that job's stdout/stderr" in notice
    # a line that only starts like ours is not quoted
    odd = "hpc-bridge: something else entirely"
    assert "its job said" not in scheduler_ops._summarize_pilot(f"F 1108 1 FAILED\nHPCB_SAID 1108 {odd}\n", 30.0)[1]


def test_the_quoted_block_line_is_capped():
    from hpc_bridge import scheduler_ops

    long = _SAID.replace("/home/u/hpc-bridge/", "/home/u/" + "d" * 400 + "/")
    _cat, notice = scheduler_ops._summarize_pilot(f"F 1108 1 FAILED\nHPCB_SAID 1108 {long}\n", 30.0)
    quoted = notice.split("its job said ", 1)[1].split(". Not a queue wait", 1)[0]
    assert len(quoted) <= 302 and "d" * 300 not in notice


def test_a_pending_pilot_after_a_partition_switch_is_not_told_it_will_die():
    from hpc_bridge import scheduler_ops

    out = f"PENDING 2001 - Priority\nF 1108 1 FAILED\nHPCB_SAID 1108 {_SAID}\n"  # the old pilot, another partition
    cat, notice = scheduler_ops._summarize_pilot(out, 30.0)
    assert cat == "queued" and "pilot 2001 is queued" in notice and "die the same way" not in notice
    assert "does not run on" not in notice


def test_a_pbs_pilot_in_state_w_is_still_a_pilot():
    # PBS job_state W (waiting for its start time) must not vanish under the tag the probe's own rows use
    from hpc_bridge import scheduler_ops

    cat, notice = scheduler_ops._summarize_pilot("W 4321.polaris-pbs-01 - Job is waiting for its start time\n", 120.0)
    assert cat == "queued" and "4321.polaris-pbs-01" in notice and "REJECTED" not in notice


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("scheduler", ["slurm", "pbs"])
def test_the_pilot_probe_reads_the_dead_jobs_own_stderr(tmp_path, scheduler):
    # the whole login-shape command, run for real against a fake scheduler that reports one died pilot
    from hpc_bridge import scheduler_ops

    eid = "da3df250-4013-4d69-942c-eef1568f860c"
    d = tmp_path / ".globus_compute" / f"uep.{eid}.abc" / "submit_scripts"
    d.mkdir(parents=True)
    script = d / "parsl.htex.block-0.1700000000.5"
    (Path(f"{script}.stderr")).write_text("noise\nhpc-bridge: endpoint venv /v (built on x86_64) does not run on c1 "
                                          "(aarch64)\n")
    (d / "parsl.htex.block-1.1700000099.1.stderr").write_text(f"{_SAID}\n")  # another job's: never quoted for 1108
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    if scheduler == "slurm":
        (bin_ / "squeue").write_text("#!/bin/sh\nexit 0\n")
        (bin_ / "sacct").write_text(f"#!/bin/sh\necho 'FAILED|1108|1:0|sbatch {script}'\n")
    else:
        rec = (f"Job Id: 1108.pbs\n    job_state = F\n    Exit_status = 1\n    Error_Path = login:{script}.stderr\n"
               f"    Output_Path = login:/x/uep.{eid}.abc/submit_scripts/y.stdout\n")
        (bin_ / "qstat").write_text(f"#!/bin/sh\nprintf '%s' '{rec}'\n")
    for f in bin_.iterdir():
        f.chmod(0o755)
    r = subprocess.run(["bash", "-c", scheduler_ops._pilot_status_cmd(scheduler, eid)],
                       env={"HOME": str(tmp_path), "USER": "u", "PATH": f"{bin_}:/usr/bin:/bin"},
                       capture_output=True, text=True, check=False)
    jid = "1108" if scheduler == "slurm" else "1108.pbs"
    assert f"HPCB_SAID {jid} hpc-bridge: endpoint venv /v (built on x86_64) does not run on c1 (aarch64)" in r.stdout, r
    assert "HPCB_ERR" not in r.stdout and r.returncode == 0
    cat, notice = scheduler_ops._summarize_pilot(r.stdout, 30.0)
    assert cat == "finished" and "these compute nodes (aarch64) cannot run it" in notice


# ---- a REAL install (opt-in: HPCB_TEST_REAL_GCE_VENV=<a venv with globus-compute-endpoint>) ------------------------

_REAL = os.environ.get("HPCB_TEST_REAL_GCE_VENV")


@pytest.mark.skipif(not _REAL, reason="set HPCB_TEST_REAL_GCE_VENV to a real globus-compute-endpoint venv")
def test_the_line_and_the_probe_against_a_real_install(box, monkeypatch):
    real = Path(_REAL).resolve()
    out = subprocess.run([str(real / "bin" / "globus-compute-endpoint"), "version"], capture_output=True, text=True,
                         check=True).stdout
    gce = out.strip().rsplit(": ", 1)[-1]
    py = subprocess.run([str(real / "bin" / "python"), "-c", "import sys;print('%d.%d' % sys.version_info[:2])"],
                        capture_output=True, text=True, check=True).stdout.strip()
    key = Path(ev.keyed_venv(str(box["venv"]), python=py, gce=gce))
    key.parent.mkdir(parents=True)
    key.symlink_to(real)  # the keyed venv IS the real install
    line = _line(box, gce=gce, python=py)
    for job in (None, "SLURM_JOB_ID"):
        r = _run(box, "bash", job=job, line=line)
        assert r.returncode == 0 and "HPCB_DONE" in r.stdout, (job, r.stderr)
    _untouched(box)
    r = subprocess.run([str(real / "bin" / "python"), "-c", "import globus_compute_endpoint.cli"],
                       capture_output=True, text=True, check=False)
    assert r.returncode != 0  # why nothing checks with `python -c "import …cli"`
    monkeypatch.setattr(discovery, "_client_python", lambda: py)
    monkeypatch.setattr(discovery, "_gce_version", lambda: gce)
    monkeypatch.setenv("HPC_BRIDGE_REMOTE_VENV", str(box["venv"]))
    r = subprocess.run(["bash", "-c", discovery._probe_script()], env={"PATH": "/usr/bin:/bin", "LANG": "C",
                       "HOME": str(box["home"])}, capture_output=True, text=True, timeout=120, check=False)
    assert f"VENV_STATE={py} {gce}" in r.stdout, r.stdout


# ---- details.env_setup is optional after a proposal ----------------------------------------------------------------

async def _propose(monkeypatch, app, env_setup: str, facility: str = "newfac", host: str = "login.newfac.edu"):
    from hpc_bridge import binding, config, connect, server
    from hpc_bridge.models import FacilityDetails
    from tests.fakes import FakeCatalog

    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([]))
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))

    async def fake_discover(target):
        return (FacilityDetails(ssh_host=target.host, interface="ib0", env_setup=env_setup,
                                scratch_root="/s/{user}", partition="p"), ["env_setup: proposed hpc-bridge's own venv"])

    monkeypatch.setattr(connect, "discover_facility_details", fake_discover)
    return await server._connect_facility(app, facility, ssh_host=host)


async def test_a_details_without_env_setup_takes_the_proposals(monkeypatch):
    from hpc_bridge import binding, server
    from hpc_bridge.context import AppCtx
    from hpc_bridge.models import FacilityDetails
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    line = ev.short_env_setup("{venv}")
    res = await _propose(monkeypatch, app, line)
    assert res.phase == "proposed_facility_details"
    assert "Leave env_setup OUT of details=" in (res.notice or "")
    built = {}

    def fake_facility(entry, *, account):
        built["env_setup"] = entry.compute.env_setup
        raise RuntimeError("stop here")  # the entry is what this test is about

    monkeypatch.setattr(binding, "_facility_from_entry", fake_facility)
    d = FacilityDetails(ssh_host="login.newfac.edu", interface="ib0", scratch_root="/s/{user}", partition="p")
    assert d.env_setup is None
    await server._connect_facility(app, "newfac", details=d)
    assert built["env_setup"] == line  # the server's copy, never an agent's echo of it
    assert app.pending_facility_cache["newfac"][1]["env_setup"] == line  # and that is what gets cached
    # a details= WITH env_setup is the user's change: used as given
    d2 = d.model_copy(update={"env_setup": "module load gce && source /x/bin/activate"})
    await server._connect_facility(app, "newfac", details=d2)
    assert built["env_setup"] == "module load gce && source /x/bin/activate"


async def test_a_details_without_env_setup_and_no_proposal_fails_clearly():
    from hpc_bridge import server
    from hpc_bridge.context import AppCtx
    from hpc_bridge.models import FacilityDetails
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    d = FacilityDetails(ssh_host="login.newfac.edu", interface="ib0", scratch_root="/s/{user}", partition="p")
    res = await server._connect_facility(app, "newfac", details=d)
    assert res.phase == "failed" and "no env_setup and there is no proposal" in (res.notice or "")
    assert "Nothing was started" in res.notice and "newfac" not in app.session_facilities


async def test_a_proposal_for_another_host_does_not_lend_its_env_setup(monkeypatch):
    from hpc_bridge import server
    from hpc_bridge.context import AppCtx
    from hpc_bridge.models import FacilityDetails
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    await _propose(monkeypatch, app, "module load site-a; " + ev.short_env_setup("{venv}"))
    d = FacilityDetails(ssh_host="other.newfac.edu", interface="ib0", scratch_root="/s/{user}", partition="p")
    res = await server._connect_facility(app, "newfac", details=d)
    assert res.phase == "failed" and "was for 'login.newfac.edu'" in (res.notice or "")


def test_entry_from_details_refuses_a_missing_env_setup():
    from hpc_bridge import binding
    from hpc_bridge.models import FacilityDetails

    d = FacilityDetails(ssh_host="h", interface="ib0", scratch_root="/s/{user}", partition="p")
    with pytest.raises(ValueError, match="env_setup is missing"):
        binding._entry_from_details("h", d)
