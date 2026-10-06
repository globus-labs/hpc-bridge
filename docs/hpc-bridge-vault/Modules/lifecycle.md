# lifecycle.py

> [!abstract] Role
> Manager-level readiness: provision the endpoint if needed and probe it to a block state. Worker-level "warm" is a separate, dispatch-time concern (the canary).

## What it does

- **`EndpointState`** (`lifecycle.py:13`) — the minimal endpoint identity: `endpoint_id` (`None` until provisioned/seeded) and `reused` (carried up from the handle so `connect_facility` can surface a zero-SSH reattach — [[models]], [#20](https://github.com/globus-labs/hpc-bridge/issues/20)).
- **`probe(facility, state)`** (`:17`) — `None` endpoint → `cold`; else `manager_online` ⇒ `warm`, else `provisioning`.
- **`ensure_warm(facility, profile, state)`** (`:23`) — provision the endpoint if `endpoint_id is None`, then `probe`. Returns `(block_state, state)`.

> [!warning] "warm" here is *manager* online, not a live worker
> `ensure_warm` reports `warm` when the manager web-status is online — which precedes a usable worker in the [[MEP & templated endpoints|MEP model]]. [[server]] upgrades that to true warmth with the canary ([[Warmth, the canary & cold-start]]); never treat `ensure_warm`'s `warm` as dispatch-ready on its own.

## See also
[[server]] · [[Warmth, the canary & cold-start]] · [[facility-base]]
