"""Review round 3 of the Anvil live test's fixes (on 7d4ac28), the shell side. Written failing first; the bash ones
run under bash 3.2 (macOS) and 5.2 (Linux). The server side: tests/test_anvil_live_findings_r3.py.

- R3-2: `uses_module` missed common command positions — `if module load …; then`, `elif`, `while`, `until`, `time`,
  `command`, a case pattern's `a*) module …`, a leading assignment — so on a worker shell with no module system,
  `if module load X` swallowed its rc 127 and the env silently was not loaded. And it matched `${ml}`,
  `"${module}/…"` and `cd ~/do ml`: a word inside `${…}` or a path is no command.
- R3-3: the endpoint-venv PATH scrub dropped a trailing empty entry (the current directory): field splitting loses
  a trailing empty field, so `/usr/bin:<venv>/bin:` became `/usr/bin`.
"""
from __future__ import annotations

import shutil
import subprocess

import pytest

_BASH = shutil.which("bash")


# ---- R3-2: `module` / `ml` as a command word, in every command position and never inside a word ----------------------

@pytest.mark.parametrize("line", [
    "if module load python/3.11; then echo ok; fi", "if ml cuda; then :; fi", "if false; then :; elif module load a; fi",
    "until module load a; do sleep 1; done", "while module is-loaded a; do break; done", "time module load a",
    "command module load x", "case $x in a*) module load x;; esac", "case $x in\n  a*) ml x;;\nesac",
    "case $x in (a) module load x;; esac", "case $x in a|b) ml x;; esac", "x=1 module load y", 'A="a b" B=2 ml y',
    "if ! module load a; then :; fi", "{ module load a; }", "echo $(module list)", "for m in a b; do module load $m; done",
    "if true; then :; else ml a; fi", "true & module load a",
])
def test_r3_2_module_in_any_command_position_is_seen(line):
    from hpc_bridge import envs

    assert envs.uses_module(line) is True, line


@pytest.mark.parametrize("line", [
    "echo ${ml}", 'source "${module}/init.sh"', "cd ~/do ml", "echo $(date) ml", "conda activate ml",
    "python -m module", "echo module load", "export MODULEPATH=/x", "ml_tool run", "source ~/ml/bin/activate",
    "ls ~/then/module", "echo if module", "x=ml", "echo {ml}",
])
def test_r3_2_module_inside_a_word_or_as_an_argument_is_not(line):
    from hpc_bridge import envs

    assert envs.uses_module(line) is False, line


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_r3_2_an_if_module_line_gets_the_init_and_loads(tmp_path):
    """The shell a facility's worker runs, with no module system initialised: `if module load X; then …` sources the
    first init there is and loads — it used to swallow `module: command not found` and run on without the env."""
    from tests.test_anvil_live_findings_node import _act, _init_at, _session

    _init_at(tmp_path / "sys" / "etc" / "profile.d" / "modules.sh")
    p = _session(tmp_path, 'echo "MODS=${LOADED_MODULES:-none}"',
                 _act("if module load python/3.11; then :; else echo no-module >&2; fi"))
    assert p.returncode == 0 and "MODS=python/3.11" in p.stdout, (p.returncode, p.stdout, p.stderr[-400:])


# ---- R3-3: the PATH scrub keeps every empty entry, the trailing one included -------------------------------------------

@pytest.mark.skipif(_BASH is None, reason="needs bash")
@pytest.mark.parametrize(("before", "after"), [
    ("/usr/bin:@V@:", "/usr/bin:"),
    (":/usr/bin:@V@", ":/usr/bin"),
    ("/usr/bin::@V@:/bin", "/usr/bin::/bin"),
    (":/a::@V@:/b:", ":/a::/b:"),
    ("@V@:", ""),
    ("/a:@V@::", "/a::"),
    ("/a:/b:", "/a:/b:"),            # nothing goes: PATH is not rewritten
])
def test_r3_3_the_path_scrub_keeps_leading_middle_and_trailing_empty_entries(tmp_path, before, after):
    from hpc_bridge import session_shell

    venv = tmp_path / "hpc-bridge" / "gce-venv-endpoint-4.19.0-py3.13"
    (venv / "bin").mkdir(parents=True)
    (venv / ".hpcb-arch").write_text("x86_64\n")
    path = before.replace("@V@", str(venv / "bin"))
    script = f"PATH='{path}'\n{session_shell.ENDPOINT_VENV_SCRUB}printf 'P=%s|\\n' \"$PATH\"\n"
    p = subprocess.run([_BASH, "-c", script], capture_output=True, text=True, timeout=30,
                       env={"HOME": str(tmp_path), "LANG": "C"})
    assert p.returncode == 0, p.stderr
    assert p.stdout == f"P={after}|\n", (before, p.stdout)
