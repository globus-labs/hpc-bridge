# ledger.py

> [!abstract] Role
> The durable batch ledger: SQLite at `<state_dir>/ledger.sqlite3` (0600, WAL, a busy timeout) beside [[state]]'s stores — the rows and the command of each batch, every attempt with its Globus task id and settlement, the lease that makes one server the batch's only admitter and collector, the retired block configs, and the interactive task ids `poll_task` answers after a restart. Design: [[Batch and swarm execution]] §4.

## What it does

- **`Ledger.open(state_dir)`** refuses a network filesystem by its type (`fs_type`: `/proc/self/mounts` on Linux, `mount(8)` on macOS; nfs, lustre, gpfs, cifs, smb, fuse… — `NetworkFilesystemError`, naming `HPC_BRIDGE_STATE_DIR`): WAL needs one host's mmap and locks, and the state dir is on NFS / Lustre / GPFS on login nodes and beside loam.
- **Tables:** `batch` (command, `task_walltime_s`, `results_dir`, `idempotent`, `capacity`, `state`, `pause_reason`, `uec_hash`, `task_group_id`, `blocks_seen`, the lease's `owner_host` / `owner_pid` / `heartbeat`, `cancel_requested`); `task` (one row per batch row: `row_json`, `attempt`, `state`, `task_id`, `admitted_at`, `started_at`, `settled_at`, `rc`, `tail`, `stderr_tail`, `stdout_path`, `stdout_bytes`, `truncated`, `job_id`, `host`, `workers`, `remaining_s`, `settled_by`, `evidence`); `retired` (endpoint, config hash, why); `itask` (interactive task ids); `event` (append-only). The command's text is stored — a replay record needs it, unlike `sent-checks`, which never holds one.
- **`reserve(batch_id, owner, n, uec_hash=…)`** — THE admission point: in one `BEGIN IMMEDIATE` transaction, the lease holder, the retired configs and the free window slots (`in_flight` + `submit_unknown` + `presumed_lost` hold one) are checked, and the lowest backlog rows move to `in_flight` with their next attempt. `NotLeaseHolder`, `RetiredConfig`.
- **`take_lease`** — refreshed by the holder; taken over only when the heartbeat is stale (`LEASE_STALE_S`, 30 s) AND the holder's pid is dead on THIS host (`pid_alive`, a seam). A holder on another host is never taken over.
- **`settle(…, only_from=…)`** — moves one attempt's row (its CURRENT attempt only) to a state, with its result fields.
- **`retire` / `unretire`** — a stop or a reap retires the block's config (on SSH its `hpcb_block` token makes it unique: S5 saw one stray submit with a retired config restart the user endpoint and a block within 18 s); the user's yes on a resume lifts it.

> [!warning] Only the lease holder admits or collects
> Two servers on one task group swallow each other's results (S1), and two admitters double the window. Every admission goes through `reserve`, which re-checks the lease inside its transaction; a server that loses the lease drops the batch from its process and only reads it.

## See also
[[batch]] · [[state]] · [[Batch and swarm execution]] · [[Batch spikes 2026-10-30]]
