"""Drive a non-Claude agent HARNESS — Codex, Pi, or Hermes at its latest release — through a scenario, headlessly, and
grade it from hpc-bridge's OWN tool-call journal (HPC_BRIDGE_JOURNAL): one JSON line per call, written by the server,
so the trace is the same whatever host drives it (no per-harness session-format reader to drift).

Each harness is configured the way docs/user/other-hosts.md tells a user to (Codex: startup timeout + approval mode;
Pi: `exposure: direct` + a 300 s timeout; Hermes: a long connect timeout, the env it filters passed explicitly), the
`driving-hpc` skill installed where it looks, and the model pointed at the ALCF OpenAI-compatible endpoint
(HPCB_ALCF_BASE_URL, bearer ALCF_INFERENCE_TOKEN) — the same open model under every harness, so a difference is the
harness's, not the model's. The run happens in a neutral working directory (no CLAUDE.md/AGENTS.md of ours leaks in).

AUTONOMOUS scenarios only for now (one turn: the prompt authorizes the spend). A persona'd scenario needs the harness's
long-lived session mode (an MCP server that lives across turns, as in real use) — raised as unsupported.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from invariants import ToolCall, Trace

HARNESSES = ("codex", "pi", "hermes21")
_UV = "/usr/local/bin/uv"
# What the hpc-bridge server needs from the jail's env; Codex and Hermes filter the child env, so it is named explicitly.
_PASSTHROUGH = (
    "HOME", "HPC_BRIDGE_USER_DIR", "GLOBUS_COMPUTE_USER_DIR", "HPC_BRIDGE_SSH_USER", "HPC_BRIDGE_SSH_KEY",
    "HPC_BRIDGE_SSH_HOST", "HPC_BRIDGE_ENDPOINT_NAME", "HPC_BRIDGE_MACHINE", "HPC_BRIDGE_SEARCH_INDEX",
    "HPC_BRIDGE_CATALOG_FILE", "HPCB_HARNESS_SSH_PORT", "HPC_BRIDGE_JOURNAL", "HPC_BRIDGE_OMIT_INSTRUCTIONS",
)
TURN_TIMEOUT_S = float(os.environ.get("HPCB_CLI_TURN_TIMEOUT_S", "2700"))


@dataclass
class CliFinal:
    """The run's last word, shaped like the SDK ResultMessage the graders read (`result`, `is_error`)."""
    result: str
    is_error: bool = False
    session_id: str | None = None
    total_cost_usd: float | None = None


def _server_cmd(repo: str) -> list[str]:
    return [_UV, "run", "--directory", repo, "--extra", "integration", "hpc-bridge"]


def _install_skill(repo: Path, *dirs: Path) -> None:
    src = repo / "skills" / "driving-hpc" / "SKILL.md"
    for d in dirs:
        (d / "driving-hpc").mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, d / "driving-hpc" / "SKILL.md")


def _model() -> tuple[str, str]:
    base = os.environ.get("HPCB_ALCF_BASE_URL", "").strip()
    if not base:
        raise SystemExit("cli_runner: HPCB_ALCF_BASE_URL is unset")
    return base, os.environ.get("HPCB_ALCF_MODEL", "openai/gpt-oss-120b")


# ---------------------------------------------------------------- per-harness setup + one turn

def setup_codex(work: Path, repo: str, env: dict[str, str]) -> dict[str, str]:
    base, model = _model()
    home = work / "codex"
    home.mkdir(parents=True, exist_ok=True)
    names = sorted(k for k in _PASSTHROUGH if k in env)
    lines = [
        f"model = {json.dumps(model)}", 'model_provider = "alcf"', 'approval_policy = "never"',
        'sandbox_mode = "danger-full-access"', "tool_output_token_limit = 20000", "",
        "[model_providers.alcf]", 'name = "ALCF"', f"base_url = {json.dumps(base)}",
        'env_key = "ALCF_INFERENCE_TOKEN"', 'wire_api = "responses"', "",
        "[mcp_servers.hpc-bridge]", f"command = {json.dumps(_server_cmd(repo)[0])}",
        f"args = {json.dumps(_server_cmd(repo)[1:])}", f"env_vars = {json.dumps(names)}",
        "startup_timeout_sec = 180", "tool_timeout_sec = 600", "required = true",
        'default_tools_approval_mode = "approve"',
    ]
    (home / "config.toml").write_text("\n".join(lines) + "\n")
    return {"CODEX_HOME": str(home)}


def setup_pi(work: Path, repo: str, env: dict[str, str]) -> dict[str, str]:
    base, model = _model()
    home = work / "pi"
    home.mkdir(parents=True, exist_ok=True)
    (home / "models.json").write_text(json.dumps({"providers": {"alcf": {
        "baseUrl": base, "api": "openai-completions", "apiKey": "$ALCF_INFERENCE_TOKEN",
        "compat": {"supportsDeveloperRole": False, "supportsStore": False, "maxTokensField": "max_tokens"},
        "models": [{"id": model, "reasoning": True, "contextWindow": 131072, "maxTokens": 32768}]}}}, indent=1))
    cmd = _server_cmd(repo)
    (home / "mcp.json").write_text(json.dumps({"mcpServers": {"hpc-bridge": {
        "command": cmd[0], "args": cmd[1:], "exposure": "direct", "timeout": 300}}}, indent=1))
    return {"PI_CODING_AGENT_DIR": str(home), "PI_OFFLINE": "1", "PI_SKIP_VERSION_CHECK": "1", "PI_TELEMETRY": "0"}


def setup_hermes21(work: Path, repo: str, env: dict[str, str]) -> dict[str, str]:
    base, model = _model()
    home = work / "hermes"
    home.mkdir(parents=True, exist_ok=True)
    cmd = _server_cmd(repo)
    cfg = {
        "model": {"provider": "custom", "base_url": base, "api_key": "${ALCF_INFERENCE_TOKEN}", "default": model,
                  "context_length": 131072, "max_tokens": 4096},
        "approvals": {"mode": "off"},
        "mcp_servers": {"hpc-bridge": {"command": cmd[0], "args": cmd[1:],
                                       "env": {k: env[k] for k in _PASSTHROUGH if k in env},
                                       "connect_timeout": 180, "timeout": 600}},
    }
    (home / "config.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False))
    return {"HERMES_HOME": str(home)}


def _turn_argv(harness: str, prompt: str, work: Path, session: str) -> list[str]:
    _, model = _model()
    if harness == "codex":
        return ["codex", "exec", "--json", "--skip-git-repo-check", "-s", "danger-full-access", "-C", str(work),
                "-o", str(work / "last-message.txt"), prompt]
    if harness == "pi":
        return ["pi", "-p", "--provider", "alcf", "--model", model, "--session-dir", str(work / "pi-sessions"),
                "--session-id", session, prompt]
    return [os.environ.get("HPCB_HERMES21_BIN", str(Path.home() / ".local/bin/hermes")), "-z", prompt, "--yolo"]


def _final_text(harness: str, work: Path, stdout: str) -> str:
    if harness == "codex":
        f = work / "last-message.txt"
        return f.read_text().strip() if f.exists() else ""
    return stdout.strip()


# ---------------------------------------------------------------- the journal → a Trace

def trace_from_journal(path: Path, texts: list[str]) -> Trace:
    calls: list[ToolCall] = []
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            res = row.get("result")
            if "error" in row:
                result: dict[str, Any] | None = {"text": row["error"], "is_error": True}
            elif isinstance(res, dict):
                result = res
            elif isinstance(res, str):
                try:
                    obj = json.loads(res)
                    result = obj if isinstance(obj, dict) else {"value": obj}
                except ValueError:
                    result = {"text": res}
            else:
                result = {"value": res}
            calls.append(ToolCall.of(f"mcp__hpc-bridge__{row.get('tool')}", row.get("args") or {}, result))
    return Trace(calls, texts)


# ---------------------------------------------------------------- the operator

async def run_scenario(prompt: str, *, repo_root: Path, model: str = "default", effort: str | None = None,
                       persona: str | None = None, user_goal: str = "", ablate_skill: bool = False,
                       max_turns: int = 40, extra_env: dict[str, str] | None = None,
                       midrun_hooks: list[dict] | None = None, hook_runner=None,
                       harness: str = "codex"):
    if persona:
        raise NotImplementedError(f"the {harness} operator drives autonomous scenarios only (no persona) for now")
    if midrun_hooks:
        raise NotImplementedError(f"the {harness} operator does not support mid-run chaos hooks yet")
    if harness not in HARNESSES:
        raise ValueError(f"unknown harness {harness!r}")
    from runner import RunResult  # jail-only import chain (the Claude Agent SDK); kept off module import
    repo = str(repo_root)
    work = Path(tempfile.mkdtemp(prefix=f"hpcb-{harness}-"))
    journal = work / "journal.jsonl"
    env = dict(os.environ)
    env.update({k: v for k, v in (extra_env or {}).items() if v is not None})
    env["HPC_BRIDGE_JOURNAL"] = str(journal)
    if ablate_skill:
        env["HPC_BRIDGE_OMIT_INSTRUCTIONS"] = "1"
    env.update({"codex": setup_codex, "pi": setup_pi, "hermes21": setup_hermes21}[harness](work, repo, env))
    if not ablate_skill:  # where each looks: ~/.agents/skills (Codex, Pi), $HERMES_HOME/skills (Hermes)
        _install_skill(repo_root, Path.home() / ".agents" / "skills",
                       Path(env.get("HERMES_HOME", work / "hermes")) / "skills")
    for k in ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"):  # the operator runs on the open model only
        env.pop(k, None)

    argv = _turn_argv(harness, prompt, work, str(uuid.uuid4()))
    print(f"  → {harness}: {' '.join(a if len(a) < 80 else a[:77] + '…' for a in argv[:8])} …", flush=True)
    t0 = time.monotonic()
    try:
        proc = await asyncio.to_thread(subprocess.run, argv, cwd=work, env=env, stdin=subprocess.DEVNULL,
                                       capture_output=True, text=True, timeout=TURN_TIMEOUT_S)
        rc, out, err = proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        rc, out, err = 124, (exc.stdout or b"").decode() if isinstance(exc.stdout, bytes) else (exc.stdout or ""), \
            f"timed out after {TURN_TIMEOUT_S:.0f} s"
    (work / "harness-stdout.txt").write_text(out or "")
    (work / "harness-stderr.txt").write_text(err or "")
    text = _final_text(harness, work, out or "")
    print(f"  ← {harness}: rc={rc} in {time.monotonic() - t0:.0f}s; final: {text[:240]!r}", flush=True)
    if rc != 0:
        print(f"  stderr tail: {(err or '').strip()[-600:]}", flush=True)
    trace = trace_from_journal(journal, [text] if text else [])
    final = CliFinal(result=text or (err or "")[-400:], is_error=rc != 0)
    # the per-run dir dies with the jail: carry the journal and the harness's own output into the bundle
    rows = [json.loads(ln) for ln in journal.read_text().splitlines() if ln.strip()] if journal.exists() else []
    messages = [{"harness": harness, "argv": argv[:6], "rc": rc, "seconds": round(time.monotonic() - t0, 1),
                 "stdout_tail": (out or "")[-4000:], "stderr_tail": (err or "")[-4000:]},
                *({"journal": r} for r in rows)]
    return RunResult(trace=trace, final=final, messages=messages)
