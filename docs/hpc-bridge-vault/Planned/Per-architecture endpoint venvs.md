# Per-architecture endpoint venvs

> [!abstract] In one line
> Designed, not built: key hpc-bridge's endpoint venv by `$(uname -m)` too, so a compute partition of another CPU architecture than the login node gets a venv of its own. Today such a partition fails at once with a message naming both architectures ([[Standing up the endpoint]]).

## The problem

globus1 (2026-10-10), for an endpoint hpc-bridge stands up over SSH: the login node is x86_64, the compute nodes are aarch64 DGX Sparks, and `/home` is shared over NFS. hpc-bridge builds its venv on the login node, so its `python` is uv's x86_64 build and a compute node cannot exec it. Since 0.1.24 the block's worker_init ends the job with `hpc-bridge: endpoint venv … (built on x86_64) does not run on spark1 (aarch64)` and the pilot probe quotes it, so nobody waits on it. But compute over that SSH path is still blocked. The facility's own multi-user endpoint (`globus-labs`) is not affected: see the prior art below.

## The idea

Key the venv by architecture as well (`{venv}-endpoint-<gce>-py<X.Y>-<arch>`), and let a compute node of another architecture build its own on first use.

## Prior art: the globus-labs multi-user endpoint

globus1's facility MEP already does this on the worker. Its user template's default `worker_init` (what a user who passes none gets) self-provisions a uv venv per architecture, `~/hpc-bridge/gce-venv-$(uname -m)`, inside the job, on the architecture that will run it (the cluster docs' D-024 and D-055, in `globus-cluster-docs`). hpc-bridge's registry entry `globus-labs` passes its own line instead: a venv built inside the job and pinned to the MEP's `globus-compute-endpoint==4.15.0` ([[facility-mep]] says why the pin is unconditional). That also works, because every build there runs on a compute node. So compute on globus1 works through `globus-labs` today. Only an endpoint hpc-bridge stands up over SSH is blocked. The template is the working model for the idea above: a per-arch name and a build inside the job.

## Why it is not built

1. The build would move onto the compute node, inside a billed job, and needs outbound network there, which compute nodes often lack.
2. Concurrent blocks on first use would race. The login-node build's answer would serve (a relocatable uv venv built in its own directory, claimed by an atomic rename), but every block would pay a multi-minute build before its worker starts.
3. uv in `~/.local/bin` is the login node's architecture, so it would have to be per-architecture too, and so would the astral installer's target.
4. The worker venv must install exactly what the login venv has (parsl's interchange↔worker protocol; the 2026-10-06 skew), so it would need the login venv's frozen requirements, with wheels for all of them on the other architecture.

Until then, such a facility needs a partition of the login node's architecture, a facility-provided environment on both (a site module), or the facility's own multi-user endpoint where it has one (`globus-labs` on globus1). Editing `env_setup` cannot fix it. Emptying it, or replacing it with a hand-written line, only stops later CLI calls from finding the venv the endpoint runs from. Since 0.1.24 teardown also searches for it, but the agent is told not to try.

## See also
[[Standing up the endpoint]] · [[facility-endpoint_venv]] · [[Next priorities 2026-10]]
