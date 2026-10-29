"""Perlmutter (NERSC) user report, 2026-10-29: remote setup on the login node — items 4, 5 and 8 (second bullet).

Began as proofs: each was a strict xfail that failed on main because of its bug, through the real code path with only
SSH replaced at the boundary (`ssh_exec`), as the rest of tests/ does. Items 4 and 5b are fixed (the default venv is
`$HOME/hpc-bridge/gce-venv`; a failed remote step is quoted by its end) and their markers are off; the rest pin what was
already fixed (item 5's texttable half, #210) or is by design (8b: the login shell).

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
from hpc_bridge.facility.base import ConfigStepFailed, said_end
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


# ---- item 4: the default {venv} was /home/<user>/hpc-bridge/gce-venv, whatever the probe found HOME to be -----------
# FIXED: the default is `$HOME/hpc-bridge/gce-venv` (endpoint_venv.DEFAULT_REMOTE_VENV), expanded on the login node —
# the build, the probe's check and teardown's search use the same path. On Perlmutter /home/<user> is not writable.

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


def test_report_4_the_probe_finds_an_existing_venv_under_the_login_nodes_home(monkeypatch, tmp_path):
    # FIXED with the default: the probe looked under /home/$(whoami), so a venv in a non-/home home was never seen
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


# The steps that quoted the remote's WHOLE output in their message (configure, start, and list when there is no Python
# traceback), each failing with its own decisive last line after the startup noise. FIXED: each quotes its END
# (`_cli_said` → `base.said_end`), and the notice is cut from its middle, never its end (`notices._ends`).
_LAST_LINES = {
    "configure": f"bash: line 1: {HOME}/gce/bin/activate: No such file or directory",
    "configure-traceback": "ImportError: cannot import name 'GlobusApp' from 'globus_sdk'",
    "start": f"bash: line 1: {HOME}/gce/bin/activate: No such file or directory",
    "list": "bash: line 1: globus-compute-endpoint: command not found",
}


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
    assert len(notice) <= 500 and "MODULEPATH" not in notice  # bounded; the noise's head is gone
    if "traceback" not in step:
        assert "[hpc-bridge: output cut — " in notice, notice  # …and the cut is marked


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


async def test_report_5_a_refused_probe_behind_a_long_banner_is_still_no_ssh_access(monkeypatch):
    # FIXED: a probe that never ran was reported as the FIRST 300 characters of stderr (then a 400-char fallback), so
    # ssh's verdict after a long banner was cut and a refused key read as an unexplained failure
    from hpc_bridge.connect import _propose_or_ask

    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "alice")
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))
    _fake_ssh(monkeypatch, discovery, (255, "", _SSH_PREAMBLE + f"alice@{HOST}: Permission denied (publickey).\n"))
    res = await _propose_or_ask("perlmutter", HOST, "ask")
    assert res.phase == "failed"
    assert res.notice.startswith(f"NO SSH ACCESS to {HOST}"), res.notice


async def test_report_5_a_password_otp_denial_behind_a_long_banner_still_hands_off(monkeypatch):
    # …and NERSC's own denial behind the same banner is the pre-auth handoff, as without one
    from hpc_bridge.connect import _propose_or_ask

    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "alice")
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))
    denial = f"alice@{HOST}: Permission denied (publickey,password,keyboard-interactive).\n"
    _fake_ssh(monkeypatch, discovery, (255, "", _SSH_PREAMBLE + denial))
    res = await _propose_or_ask("perlmutter", HOST, "ask")
    assert res.phase == "needs_preauth", res.notice


@pytest.mark.parametrize("where", ["the venv check", "teardown"])
def test_report_5_ssh_refusals_on_other_steps_are_classified_from_their_end(where):
    # the endpoint venv's ssh failure (`_transport`) and teardown's (`use_working_venv`, stop) quoted the head too
    err = _SSH_PREAMBLE + f"alice@{HOST}: Permission denied (publickey).\n"
    exc = (ev._transport("check", HOST, 255, "", err) if where == "the venv check"
           else RuntimeError("ssh: " + said_end(err)))
    notice = _explain_provision_error(exc, host=HOST, user="alice")
    assert notice.startswith(f"NO SSH ACCESS to {HOST}") and "Permission denied (publickey)" in notice, notice


def test_report_5_an_unreachable_host_behind_noise_quotes_ssh_s_last_line():
    err = ("Control socket connect(/tmp/hpcb/cm-x): Connection refused\n" + _SSH_PREAMBLE
           + f"ssh: connect to host {HOST} port 22: Operation timed out\n")
    notice = _explain_provision_error(RuntimeError(f"discovery probe failed (rc=255): {said_end(err)}"),
                                      host=HOST)
    assert notice.startswith(f"CANNOT REACH {HOST}: ssh: connect to host {HOST} port 22: Operation timed out"), notice


def test_report_5_the_end_kept_and_the_cut_marked():
    from hpc_bridge.notices import _ends

    assert said_end("a\n\nb\n") == "a\nb" and said_end("") == ""  # nothing dropped: no mark
    lines = [f"{i:02d} " + "x" * 97 for i in range(10)]  # 100 characters each: the last three fit in 400 with newlines
    assert said_end("\n".join(lines)) == "[hpc-bridge: output cut — 7 earlier lines dropped]\n" + "\n".join(lines[-3:])
    assert said_end("y" * 900).endswith("y" * 400) and len(said_end("y" * 900)) == 400  # the last line always, cut
    assert said_end("z" * 399 + "\n" + "last").startswith("[hpc-bridge: output cut — 1 earlier line dropped]\n")
    # a message that opens with its cause and ends in help is kept whole when it fits (Lmod's, live on the fake cluster)
    lmod = ('Lmod has detected the following error: The following module(s) are unknown:\n"gcc/10.2.0"\n\n'
            'Please check the spelling or version number. Also try "module spider ..."\n'
            "It is also possible your cache file is out-of-date; it may help to try:\n"
            '  $ module --ignore_cache load "gcc/10.2.0"\n\n'
            "Also make sure that all modulefiles written in TCL start with the string\n#%Module\n\n\n")
    assert said_end(lmod).startswith("Lmod has detected the following error: The following module(s) are unknown:")
    long = "hpc-bridge error: ConfigStepFailed: remote configure failed: " + "noise " * 200 + "THE CAUSE"
    cut = _ends(long)
    assert len(cut) <= 500 and cut.startswith("hpc-bridge error: ConfigStepFailed: remote configure failed: ")
    assert cut.endswith("THE CAUSE") and "[hpc-bridge: notice cut — " in cut
    assert _ends("short") == "short"


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
