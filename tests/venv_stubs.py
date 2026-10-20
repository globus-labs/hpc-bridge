"""Stub tools and venvs for the endpoint-venv tests (`test_env_setup.py`, `test_endpoint_venv_build.py`).

The stubs behave like the real install where it matters: `bin/globus-compute-endpoint` is uv's /bin/sh trampoline to
the venv's python and prints the real `version` line ("Globus Compute endpoint version: X"), and the stub `cli` reads
`sys.argv[0]` at import as gce 4.12-4.18 do (so `python -c "import …cli"` fails, as it does for real); its `list` draws
the table as gce 4.18 does, through a stub texttable that behaves as 1.7.1 or as 1.7.2 (which broke it), and an install
gets 1.7.2 unless it asks for `texttable<1.7.2`, as from PyPI since 2026-10-18. In a venv made
with `uv venv --relocatable` the trampoline finds its python relative to itself, as uv 0.12 writes it (checked against
a real uv in a container: such a venv still runs after a rename). `uv`, `python3` (for `-m venv`; anything else runs the
real interpreter) and `curl` are stubs on PATH that record every call. A venv is a copy of one real `python -m venv`
made once per module, with its `activate` re-pointed at the new path.
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

PY = f"{sys.version_info.major}.{sys.version_info.minor}"

LIB = r"""
import json, os, shutil, sys, time

def purelib(venv):
    return os.path.join(venv, "lib", "python%d.%d" % sys.version_info[:2], "site-packages")

def _write(path, text, mode=None):
    with open(path, "w") as fh:
        fh.write(text)
    if mode:
        os.chmod(path, mode)

# texttable as 1.7.1 and 1.7.2 behave for gce's `list`: 1.7.2 (2026-10-18) made `_compute_cols_width()` RETURN the
# widths, so `_width` exists only inside `draw()`
TEXTTABLE = '''__version__ = %r
class Texttable:
    def __init__(self, max_width=80):
        self._max_width, self._header, self._rows = max_width, [], []
    def header(self, row):
        self._header = [str(c) for c in row]
    def add_row(self, row):
        self._rows.append([str(c) for c in row])
    def set_max_width(self, width):
        self._max_width = width
    def _compute_cols_width(self):
        widths = [max(len(r[i]) for r in [self._header] + self._rows) for i in range(len(self._header))]
        %s
    def draw(self):
        w = self._compute_cols_width() or self._width
        sep = "+" + "+".join("-" * (n + 2) for n in w) + "+"
        rows = [self._header] + self._rows
        return chr(10).join([sep] + ["| " + " | ".join(c.ljust(n) for c, n in zip(r, w)) + " |" for r in rows] + [sep])
'''

# `list` as gce 4.18's `Endpoint.print_endpoint_table` does it (no newline escapes: this text lands in cli.py as is)
CLI_LIST = '''
def _list():
    import os
    import texttable
    d = os.path.join(os.path.expanduser("~"), ".globus_compute")
    eps = sorted(n for n in os.listdir(d) if os.path.isdir(os.path.join(d, n))) if os.path.isdir(d) else []
    if not eps:
        print("No endpoints configured!")
        return 0
    table = texttable.Texttable()
    table.header(["Endpoint ID", "Status", "Endpoint Name"])
    for n in eps:
        table.add_row(["358c89fb-2774-4a5c-8dc5-da2406ccdc9c", "Stopped", n])
    table.set_max_width(68)
    table._compute_cols_width()
    if table._width[0] < 36:
        table._width[2] -= 36 - table._width[0]
        table._width[0] = 36
    table._width[2] = max(10, table._width[2])
    print(table.draw())
    return 0
'''

def install_texttable(venv, version):
    body = "self._width = widths" if version == "1.7.1" else "return widths"
    _write(os.path.join(purelib(venv), "texttable.py"), TEXTTABLE % (version, body))

def install_gce(venv, version, broken=False, texttable=None):
    site = purelib(venv)
    pkg = os.path.join(site, "globus_compute_endpoint")
    os.makedirs(pkg, exist_ok=True)
    _write(os.path.join(pkg, "__init__.py"), f"__version__ = {version!r}\n")
    cli = 'import sys\n_PROG = sys.argv[0].rsplit("/", 1)[1]  # gce 4.12-4.18 read argv[0] at import\n'
    if broken:
        cli += "raise ImportError(\"cannot import name 'X' from 'globus_sdk' (stub)\")\n"
    cli += CLI_LIST
    cli += ('def main():\n    if sys.argv[1:] == ["list"]:\n        return _list()\n'
            '    if sys.argv[1:] == ["version"]:\n        import globus_compute_endpoint as g\n'
            '        print(f"Globus Compute endpoint version: {g.__version__}")\n        return 0\n    return 2\n')
    _write(os.path.join(pkg, "cli.py"), cli)
    if texttable:
        install_texttable(venv, texttable)
    py = os.path.join(venv, "bin", "python")  # the console script exactly as uv writes it: a /bin/sh trampoline
    with open(os.path.join(venv, "pyvenv.cfg")) as fh:
        relocatable = "relocatable = true" in fh.read()
    exe = "\"$(dirname -- \"$(realpath -- \"$0\")\")\"/'python'" if relocatable else "'%s'" % py
    _write(os.path.join(venv, "bin", "globus-compute-endpoint"),
           "#!/bin/sh\n'''exec' %s \"$0\" \"$@\"\n' '''\nimport sys\n"
           "from globus_compute_endpoint.cli import main\nsys.exit(main())\n" % exe, 0o755)

def log(*argv):
    with open(os.environ["HPCB_TEST_LOG"], "a") as fh:
        fh.write(json.dumps(list(argv)) + "\n")

def do_install(py, reqs):
    if "uv" in reqs:  # pip installing uv (a node with no curl): its binary lands in the venv's bin
        shutil.copy(os.environ["HPCB_TEST_UV_DIST"], os.path.join(os.path.dirname(py), "uv"))
        return
    if os.environ.get("HPCB_TEST_UV_FAIL") == "install":
        sys.exit("error: Failed to fetch: `https://pypi.org/simple/globus-compute-endpoint/` (stub: offline)")
    # the endpoint brings texttable (through the SDK, unbounded): PyPI's newest, 1.7.2 since 2026-10-18, unless the
    # install bounds it below that
    texttable = "1.7.1" if "texttable<1.7.2" in reqs else "1.7.2"
    for r in reqs:
        if r.startswith("globus-compute-endpoint=="):
            install_gce(os.path.dirname(os.path.dirname(py)), r.split("==", 1)[1],
                        broken=os.environ.get("HPCB_TEST_INSTALL") == "broken", texttable=texttable)

def make_venv(path, relocatable=False):
    template = os.environ["HPCB_TEST_TEMPLATE"]
    if os.environ.get("HPCB_TEST_SLOW"):
        time.sleep(float(os.environ["HPCB_TEST_SLOW"]))  # widen the window concurrent builds race in
    shutil.copytree(template, path, symlinks=True, dirs_exist_ok=True)  # `python3 -m venv` takes an empty dir
    if relocatable:
        with open(os.path.join(path, "pyvenv.cfg"), "a") as fh:
            fh.write("relocatable = true\n")
    for name in os.listdir(os.path.join(path, "bin")):
        f = os.path.join(path, "bin", name)
        if name.startswith("activate") and not os.path.islink(f):
            with open(f) as fh:
                text = fh.read()
            _write(f, text.replace(template, path))

def make_template(path, stubdir):
    import subprocess
    subprocess.check_call([sys.executable, "-m", "venv", "--without-pip", path])
    pipdir = os.path.join(purelib(path), "pip")  # `python -m pip` runs this stub
    os.makedirs(pipdir)
    _write(os.path.join(pipdir, "__init__.py"), "")
    _write(os.path.join(pipdir, "__main__.py"),
           f"import sys; sys.path.insert(0, {stubdir!r}); import hpcb_stub; hpcb_stub.pip_main(sys.argv[1:])\n")

def uv_main(args):
    old = os.environ.get("HPCB_TEST_UV_OLD")  # a uv from before `venv --relocatable` (0.2.31)
    if args == ["--version"]:
        print(f"uv {old or '0.12.9'} (stub)")
        return
    if args[:1] == ["venv"] and "--help" in args:
        print("Usage: uv venv [OPTIONS] [PATH]\n      --python <PYTHON>\n      --no-cache"
              + ("" if old else "\n      --relocatable  Make the virtual environment relocatable"))
        return
    log("uv", *args)
    if old and "--relocatable" in args:
        sys.exit("error: unexpected argument '--relocatable' found\n\nFor more information, try '--help'.")
    if args[:1] == ["venv"]:
        path, skip = None, False
        for a in args[1:]:
            if skip:
                skip = False
            elif a == "--python":
                skip = True
            elif not a.startswith("-"):
                path = a
        if os.environ.get("HPCB_TEST_UV_FAIL") == "venv":
            sys.exit("error: No interpreter found for Python (stub, as told)")
        make_venv(path, relocatable="--relocatable" in args)
    elif args[:2] == ["pip", "install"]:
        rest = args[2:]
        py = rest[rest.index("--python") + 1]
        do_install(py, [a for a in rest if not a.startswith("-") and a != py])
    else:
        sys.exit(f"uv stub: unexpected {args}")

def pip_main(args):
    log("pip", sys.executable, *args)
    do_install(sys.executable, [a for a in args[1:] if not a.startswith("-")])

def python3_main(args):  # the pip build's `python3 -m venv <path>`; the check's `-c`; HPCB_TEST_PY3 fakes its version
    fake = os.environ.get("HPCB_TEST_PY3")
    if args[:2] == ["-m", "venv"]:
        log("python3", *args)
        if fake:
            sys.exit("python3 stub: no venv module here")
        make_venv(args[2])
    elif fake and args[:1] == ["-c"]:
        print(fake)
    else:
        os.execv(sys.executable, [sys.executable, *args])
"""


def make_stubs(root: Path) -> dict:
    """The stub tools (`uv`, `python3`, `curl`) and the template venv, under `root`."""
    stub = root / "stub"
    stub.mkdir()
    (stub / "hpcb_stub.py").write_text(LIB)
    for name, fn in (("uv", "uv_main"), ("python3", "python3_main")):
        p = stub / name
        p.write_text(f"#!{sys.executable}\nimport sys; sys.path.insert(0, {str(stub)!r}); import hpcb_stub; "
                     f"hpcb_stub.{fn}(sys.argv[1:])\n")
        p.chmod(0o755)
    shutil.copy(stub / "uv", root / "uv-dist")  # what the astral installer "installs" into ~/.local/bin
    curl = stub / "curl"
    curl.write_text(f"#!/bin/sh\necho curl \"$@\" >> \"$HPCB_TEST_CURL\"\n"
                    f"echo 'mkdir -p \"$HOME/.local/bin\" && cp {root}/uv-dist \"$HOME/.local/bin/uv\"'\n")
    curl.chmod(0o755)
    lib: dict = {}
    exec(compile(LIB, "hpcb_stub", "exec"), lib)  # noqa: S102 - the tests' own stub library
    lib["make_template"](str(root / "template"), str(stub))
    return {"stub": stub, "template": root / "template", "lib": lib, "uv_dist": root / "uv-dist"}


def make_box(tmp: Path, stubs: dict) -> dict:
    """A home (no ~/hpc-bridge yet) and a per-test copy of the stub tools (a test may remove one)."""
    stub = tmp / "stub"
    shutil.copytree(stubs["stub"], stub)
    home = tmp / "home"
    home.mkdir()
    (home / "keep.txt").write_text("not yours")
    base = home / "hpc-bridge" / "gce-venv"
    return {"tmp": tmp, "stub": stub, "home": home, "venv": base, "log": tmp / "calls.jsonl",
            "curl": tmp / "curl.log", "lib": stubs["lib"], "template": stubs["template"], "uv_dist": stubs["uv_dist"]}


def env(box: dict, *, path_extra: str = "", **more: str) -> dict:
    return {"PATH": f"{path_extra}{box['stub']}:/usr/bin:/bin", "HOME": str(box["home"]), "LANG": "C",
            "HPCB_TEST_LOG": str(box["log"]), "HPCB_TEST_CURL": str(box["curl"]),
            "HPCB_TEST_TEMPLATE": str(box["template"]), "HPCB_TEST_UV_DIST": str(box["uv_dist"]), **more}


def venv(box: dict, path: Path, version: str, broken: bool = False, relocatable: bool = False,
         texttable: str | None = None) -> Path:
    """An existing venv (what an earlier run left), with a sentinel a rebuild would move away. `texttable`: the stub
    texttable it has ("1.7.1", "1.7.2"; None: none at all)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    box["lib"]["make_venv"](str(path), relocatable)
    box["lib"]["install_gce"](str(path), version, broken=broken, texttable=texttable)
    (path / "sentinel").write_text("old")
    return path


def calls(box: dict) -> list[list[str]]:
    import json

    return [json.loads(ln) for ln in box["log"].read_text().splitlines()] if box["log"].exists() else []


def version_of(v: Path) -> str | None:
    import subprocess

    r = subprocess.run([str(v / "bin" / "globus-compute-endpoint"), "version"], capture_output=True, text=True,
                       check=False)
    return r.stdout.strip().rsplit(": ", 1)[-1] if r.returncode == 0 else None


def shells() -> list[str]:
    """bash (`bash -lc`, Slurm/PBS job scripts), and sh and dash once per real binary."""
    seen: dict[str, str] = {}
    for s in ("bash", "sh", "dash"):
        if (p := shutil.which(s)):
            seen.setdefault(os.path.realpath(p), s)
    return list(seen.values())
