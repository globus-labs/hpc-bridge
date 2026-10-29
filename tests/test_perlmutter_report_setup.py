"""Perlmutter (NERSC) user report, 2026-10-29: remote setup on the login node — items 4, 5 and 8 (second bullet).

Proofs, not fixes. Each `xfail(strict=True)` fails on main because of the bug its reason names, through the real code
path with only SSH replaced at the boundary (`ssh_exec`), as the rest of tests/ does; once the bug is fixed the test
passes, the strict xfail turns the run red, and the marker comes off with the fix. The passing tests pin what is already
fixed (item 5's texttable half, #210) or by design (8b: the login shell).

The home is Perlmutter's shape, `/global/homes/<initial>/<user>`, never `/home/<user>`; the user is the placeholder
`alice`.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from hpc_bridge import binding, config, discovery
from hpc_bridge.facility import endpoint_venv as ev
from hpc_bridge.facility import remote
from hpc_bridge.facility.base import ConfigStepFailed
from hpc_bridge.facility.remote import RemoteEndpointCLI, SshTarget
from hpc_bridge.notices import _explain_provision_error

HOME = "/global/homes/a/alice"
HOST = "perlmutter.nersc.gov"
GCE, PY = "4.18.0", "3.13"

# What the probe prints on a Perlmutter login node (the fields discovery reads): HOME is not /home/<user>, $SCRATCH and
# $PSCRATCH are per-user, uv is the user's own, Slurm, Slingshot NICs.
_PROBE_OUT = "\n".join([
    "HPCB_PROBE_BEGIN", "USER=alice", f"HOME={HOME}", "SCRATCH=/pscratch/sd/a/alice", "WORK=",
    "PSCRATCH=/pscratch/sd/a/alice", "SCHED=slurm", "GCE=", f"UV={HOME}/.local/bin/uv",
    "PART=system|up", "PART=debug|up", "PART=regular_milan_ss11|up",
    "NIC=lo|127.0.0.1/8", "NIC=hsn0|10.249.0.1/16", "NIC=hsn1|10.250.0.1/16", "HPCB_PROBE_END", ""])

# What the user's startup files print on stderr ahead of every `bash -lc` step (report items 5 and 8b): Lmod's module
# swaps and a stale ~/.gitconfig.lock — over 600 characters before the command's own words.
_STARTUP_NOISE = (
    'Lmod is automatically replacing "PrgEnv-gnu/8.5.0" with "PrgEnv-nvidia/8.5.0".\n'
    'Lmod is automatically replacing "gcc-native/12.3" with "nvidia/24.5".\n'
    "Due to MODULEPATH changes, the following have been reloaded:\n"
    "  1) cray-libsci/24.07.0     2) cray-mpich/8.1.30\n"
    "The following have been reloaded with a version change:\n"
    "  1) cudatoolkit/12.2 => cudatoolkit/12.4     2) craype-accel-nvidia80 => craype-accel-nvidia80\n"
    + f"error: could not lock config file {HOME}/.gitconfig: File exists\n" * 3
)
assert len(_STARTUP_NOISE) > 600

_ATTR = "AttributeError: 'Texttable' object has no attribute '_width'"
_LIST_TRACEBACK = (
    "Traceback (most recent call last):\n"
    f'  File "{HOME}/gce/bin/globus-compute-endpoint", line 12, in <module>\n'
    "    sys.exit(cli_run())\n"
    f'  File "{HOME}/gce/lib/python3.13/site-packages/globus_compute_endpoint/endpoint/endpoint.py", line 906, in '
    "print_endpoint_table\n"
    "    if table._width[idx_id] < width_id:\n"
    f"{_ATTR}\n"
)


def _fake_ssh(monkeypatch, module, answer: tuple[int, str, str], seen: list[str] | None = None) -> None:
    async def fake(target, cmd, *, stdin=None, timeout=None):
        if seen is not None:
            seen.append(cmd)
        return answer

    monkeypatch.setattr(module, "ssh_exec", fake)


# ---- item 4: the default {venv} is /home/<user>/hpc-bridge/gce-venv, whatever the probe found HOME to be ------------

@pytest.mark.xfail(strict=True, reason=(
    "perlmutter report #4: profile_from_catalog_entry resolves {venv} to DEFAULT_REMOTE_VENV "
    "(/home/{user}/hpc-bridge/gce-venv, facility/endpoint_venv.py:51, facility/remote.py:380), not the HOME the probe "
    "learned; on Perlmutter /home/<user> is not writable, so the venv build fails with Permission denied"))
async def test_report_4_a_probed_facility_puts_its_venv_under_the_home_the_probe_found(monkeypatch):
    # the whole BYO chain: the probe over "SSH" -> the proposal -> details= -> the session entry (the same entry the
    # local cache, facilities.json, rebuilds from) -> the facility a bring-up runs, with its env_setup and worker_init
    monkeypatch.delenv("HPC_BRIDGE_REMOTE_VENV", raising=False)
    monkeypatch.delenv("HPC_BRIDGE_SSH_KEY", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "alice")
    _fake_ssh(monkeypatch, discovery, (0, _PROBE_OUT, ""))
    draft, _notes = await discovery.discover_facility_details(SshTarget(HOST, "alice"))
    assert draft.scratch_root == "/pscratch/sd/a/{user}/.hpc-bridge"  # the probe's facts do reach scratch_root
    assert ev.managed_venv(draft.env_setup.replace("{venv}", "/x")) is not None  # hpc-bridge's own line, {venv} open

    fac = binding._facility_from_entry(binding._entry_from_details("perlmutter", draft), account="m0000")
    mv = ev.managed_venv(fac.cli.env_setup)
    assert mv is not None and fac.profile.worker_init == fac.cli.env_setup
    # what ensure_venv builds into (and what worker_init activates on the compute node)
    assert mv.base.startswith((f"{HOME}/", "$HOME/")), mv.base


@pytest.mark.xfail(strict=True, reason=(
    "perlmutter report #4: the discovery probe looks for an existing hpc-bridge venv under /home/$(whoami) "
    "(discovery.py:132, :135, from DEFAULT_REMOTE_VENV), not under the login node's $HOME, so a venv already built "
    "in a non-/home home is never seen"))
def test_report_4_the_probe_finds_an_existing_venv_under_the_login_nodes_home(monkeypatch, tmp_path):
    # the real probe script, in a real (local, non-login) bash whose HOME is Perlmutter-shaped and holds the keyed venv
    monkeypatch.delenv("HPC_BRIDGE_REMOTE_VENV", raising=False)
    monkeypatch.setattr(discovery, "_gce_version", lambda: GCE)
    monkeypatch.setattr(discovery, "_client_python", lambda: PY)
    home = tmp_path / HOME.lstrip("/")
    venv = Path(ev.keyed_venv(str(home / "hpc-bridge" / "gce-venv"), python=PY, gce=GCE))
    venv.mkdir(parents=True)
    (venv / "pyvenv.cfg").write_text("home = /usr/bin\n")
    r = subprocess.run(["bash", "--noprofile", "--norc", "-c", discovery._probe_script()],
                       env={"HOME": str(home), "PATH": "/usr/bin:/bin", "USER": "alice", "LC_ALL": "C"},
                       capture_output=True, text=True, timeout=60, check=False)
    assert "HPCB_PROBE_END" in r.stdout, r.stderr
    assert discovery._collect(r.stdout).get("VENV") == str(venv)


# ---- item 5: a failed remote step must be reported by its END, not by the startup noise at its head --------------

async def test_report_5_a_list_traceback_after_startup_noise_names_the_exception_and_the_fix(monkeypatch):
    # FIXED on main by #210 (fbe2eb7, 0.1.25): `_cli_said` turns a traceback into its last line + the raising frame and
    # adds the texttable fix (and every build pins texttable<1.7.2). At 0.1.20 the notice was the noise's first 500
    # characters. Regression for the report's exact shape: the user's startup noise BEFORE the traceback.
    _fake_ssh(monkeypatch, remote, (1, "", _STARTUP_NOISE + _LIST_TRACEBACK))
    with pytest.raises(ConfigStepFailed) as e:
        await RemoteEndpointCLI(SshTarget(HOST, "alice"), "true").endpoint_id("hpc-bridge-perlmutter")
    notice = _explain_provision_error(e.value)
    assert _ATTR in notice and "pip install 'texttable<1.7.2'" in notice, notice
    assert "Lmod" not in notice and "gitconfig" not in notice
    assert "texttable<1.7.2" in ev.TEXTTABLE_PIN


# The steps that quote the remote's WHOLE output in their message (configure, start, and list when there is no Python
# traceback), each failing with its own decisive last line after the startup noise.
_LAST_LINES = {
    "configure": f"bash: line 1: {HOME}/gce/bin/activate: No such file or directory",
    "configure-traceback": "ImportError: cannot import name 'GlobusApp' from 'globus_sdk'",
    "start": f"bash: line 1: {HOME}/gce/bin/activate: No such file or directory",
    "list": "bash: line 1: globus-compute-endpoint: command not found",
}


@pytest.mark.xfail(strict=True, reason=(
    "perlmutter report #5: a failed configure/start/list (no traceback) puts the remote's whole stderr in its message "
    "(facility/remote.py:580-583, :738-739; _cli_said :433 keeps non-traceback text whole) and the notice keeps the "
    "message's FIRST 500 characters (notices.py:102), so the user's startup noise pushes the real error out; "
    "ConfigStepFailed.said/.tail hold the end but the notice never quotes them"))
@pytest.mark.parametrize("step", list(_LAST_LINES))
async def test_report_5_a_failed_step_after_startup_noise_is_reported_by_its_last_line(monkeypatch, step):
    last = _LAST_LINES[step]
    err = _STARTUP_NOISE + ("Traceback (most recent call last):\n  File \"x\", line 1, in <module>\n"
                            if "traceback" in step else "") + last + "\n"
    _fake_ssh(monkeypatch, remote, (1 if step != "list" else 127, "", err))
    cli = RemoteEndpointCLI(SshTarget(HOST, "alice"), f". {HOME}/gce/bin/activate")
    call = {"configure": cli.configure, "configure-traceback": cli.configure, "start": cli.start,
            "list": cli.endpoint_id}[step]
    with pytest.raises(ConfigStepFailed) as e:
        await call("hpc-bridge-perlmutter")
    assert e.value.said == last  # the step's own last line is known…
    notice = _explain_provision_error(e.value)
    assert last in notice, notice  # …and the agent must be told it


# A login node's pre-auth banner (sshd `Banner`) and OpenSSH 10's post-quantum warning reach ssh's STDERR ahead of its
# verdict; a site banner easily runs past 300 characters.
_SSH_PREAMBLE = (
    "** WARNING: connection is not using a post-quantum key exchange algorithm.\n"
    '** This session may be vulnerable to "store now, decrypt later" attacks.\n'
    "** The server may need to be upgraded. See https://openssh.com/pq.html\n"
    "*******************************************************************************\n"
    "*  This system is for authorized users only. Use of this system constitutes   *\n"
    "*  consent to monitoring. Unauthorized use is prohibited and may be reported. *\n"
    "*******************************************************************************\n"
)
assert len(_SSH_PREAMBLE) > 300


@pytest.mark.xfail(strict=True, reason=(
    "perlmutter report #5 (probe path): a probe that never ran is reported as the FIRST 300 characters of stderr "
    "(discovery.py:148, then the 400-char fallback at connect.py:793), so ssh's verdict after a long banner is cut "
    "and a refused key reads as an unexplained failure instead of NO SSH ACCESS"))
async def test_report_5_a_refused_probe_behind_a_long_banner_is_still_no_ssh_access(monkeypatch):
    from hpc_bridge.connect import _propose_or_ask

    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "alice")
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))
    _fake_ssh(monkeypatch, discovery, (255, "", _SSH_PREAMBLE + f"alice@{HOST}: Permission denied (publickey).\n"))
    res = await _propose_or_ask("perlmutter", HOST, "ask")
    assert res.phase == "failed"
    assert res.notice.startswith(f"NO SSH ACCESS to {HOST}"), res.notice


# ---- item 8b: the remote steps run in a LOGIN shell, by design -------------------------------------------------------

async def test_report_8b_remote_steps_run_in_a_login_shell_by_design(monkeypatch):
    # BY DESIGN, not a bug: `module` (Lmod's init is in /etc/profile.d), the scheduler's and the site's PATH (sbatch,
    # qsub, mybalance) and $SCRATCH/$PSCRATCH come from the login profile; module-aware discovery (the `lmod` fake
    # profile, scenario lmod_bootstrap) reads `module -t avail` there. `bash --noprofile --norc` would lose them, and
    # where bash is built with SSH_SOURCE_BASHRC sshd's own `$SHELL -c` sources ~/.bashrc before any remote command
    # anyway. The startup noise is answered by reporting a failure's END (item 5), not by skipping the profile.
    seen: list[str] = []
    _fake_ssh(monkeypatch, discovery, (0, _PROBE_OUT, ""), seen)
    await discovery.discover_facility_details(SshTarget(HOST, "alice"))
    _fake_ssh(monkeypatch, remote, (0, "No endpoints configured!", ""), seen)
    cli = RemoteEndpointCLI(SshTarget(HOST, "alice"), ev.short_env_setup(f"{HOME}/hpc-bridge/gce-venv"))
    await cli.status("hpc-bridge-perlmutter")                    # list
    await cli.configure("hpc-bridge-perlmutter")                 # configure
    await cli.login_exec("sinfo -h")                             # the free login shape's discovery
    await cli._venv_run("check", "echo HPCB_VENV_BEGIN", 10.0)   # the endpoint venv's check / build
    assert len(seen) == 5 and all(c.startswith("bash -lc ") for c in seen), seen
