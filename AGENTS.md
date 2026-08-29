# AGENTS.md — `truenas`

**TrueNAS storage module.** Manages and reports on a **fleet** of TrueNAS appliances over the official `truenas_api_client` WebSocket JSON-RPC 2.0 API.

- **Repo:** `github.com/lbockenstedt/truenas`
- **Module type:** `module_type = "storage"`
- **Canonical docs:** none yet — there is **no `lm/docs/truenas.md`**. Use this repo's `README.md`, then [`lm/docs/nw.md`](../lm/docs/nw.md) for the pattern it mirrors.
- **Fleet map:** [`../AGENTS.md`](../AGENTS.md) *(only present in a side-by-side checkout)*

## Context

This repo is **one of 16** that make up **Lab Manager (LM)** — a hub-and-spoke
"single pane of glass" orchestrator for lab/datacenter infrastructure. One hub (the `lm`
repo) runs the control plane, REST API and WebUI. Every other repo is a **spoke** wrapping
exactly one external system and dialling the hub over a WebSocket on port 443.

Read [`lm/docs/architecture-topology.md`](../lm/docs/architecture-topology.md) — a verbatim
copy also lives in this repo's `docs/` — before making structural changes.

## Layout

`src/truenas_spoke.py` (spoke), `src/control_plane.py`, `src/truenas_client.py` (JSON-RPC
client), `src/truenas_engine.py` (pools, datasets, shares, disks, alerts, services, capacity),
`src/credentials.py` (per-tenant credentials).

## truenas-specific gotchas

- **Mirrors the `nw` module pattern** — read `nw` first if the shape is unfamiliar.
- **The spoke owns all the logic** (fleet, poller, client, credentials). The hub is transport + WebUI + a **cache twin** at `lm/core/src/routes/truenas.py` and `lm/core/src/truenas_cache.py`. Changing the reported shape means updating the twin too.
- **One spoke, many appliances.** Nothing here may assume a single target.
- Management actions (create/delete datasets, SMB/NFS shares, snapshots, scrubs) are **gated** — keep them that way.
- `--secret` is required in the documented install line.

## Fleet conventions (identical in every LM repo)

- **Python 3.11**, FastAPI + `websockets` + `asyncio`. WebUI is dependency-free vanilla JS — **no npm build step exists anywhere in this project**.
- **`VERSION` is `MAJOR.NN` and branch-owned.** A bot bumps the last segment. **Never bump it by hand.** Promotion carries code only.
- **Branching: `dev -> qa -> main`.** `qa` and `main` need a PR; `ci.yml` is the required check. Direct pushes to `dev` are allowed.
- **CI runs one pytest process per component.** Components share top-level module names (`control_plane.py` exists in most repos) and collide in a single process.
- **Installers are idempotent** — re-running updates code and preserves credentials. Common flags: `--hub` (bare hostname is normalised to `wss://...:443`), `--id`/`--name`, `--secret`, `--hub-secret`, `--all-prereqs`.
- **Transport:** WebSocket on 443, mailbox pattern, **push-ack-retry — no fire-and-forget**. Heartbeat 30s; yellow at >=120s, red at >=300s. Hub queues 24h for offline spokes.
- **TLS:** encrypted but **verify-OFF by default** (self-signed hub cert). Verification is opt-in at install time via `--tls-verify` / `--tls-ca-cert` — never by hand-editing `.env`.
- **Heavy lifting belongs in the spoke, not the hub.** The hub is transport, state, policy and UI. See `lm/docs/architecture-spoke-heavy-lifting.md`.
- **API-first:** every operation exposes an API; the WebUI only ever calls that API.
- **Atomic transactions:** a mid-chain failure rolls back every preceding step and reports a before/after diff. No zombie resources.
- **Multitenancy is not optional:** isolation rides on Proxmox labels + NetBox tenant IDs. New resources carry tenant context.

## Rules

1. **One repo per change.** Cross-repo work is separate PRs, and the wire contract must stay backward-compatible because the two sides deploy independently.
2. **Read the canonical doc first** (linked above) — it is usually more current than this repo's README.
3. **Never hand-edit `VERSION`.**
4. **Check you are editing the live path,** not a preserved legacy one.
5. Match surrounding style. Comment only what needs clarifying.
