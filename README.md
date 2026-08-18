# TrueNAS Manager (`truenas`) — Lab Manager spoke

A Lab Manager spoke role that **manages and reports on TrueNAS appliances** over
the official [`truenas_api_client`](https://github.com/truenas/api_client)
WebSocket JSON-RPC 2.0 API. One spoke manages a **fleet** of appliances (pools,
datasets, shares, disks, alerts, services, capacity); gated management actions
(create/delete datasets, create SMB/NFS shares, take snapshots, run scrubs).

The spoke owns all the logic (fleet, poller, client, per-tenant credentials).
The hub is transport + WebUI surface + a cache twin (see `lm/core/src/routes/
truenas.py` + `truenas_cache.py`). Mirrors the `nw` module pattern.

<!-- INSTALLERS:START -->
## Installation

Every installer in this repo, with every flag and environment variable it accepts.
Installers are idempotent — re-running one updates code and preserves credentials.

### TrueNAS (storage) spoke — `install_truenas.sh`

```bash
curl -sSL https://raw.githubusercontent.com/lbockenstedt/truenas/main/install_truenas.sh \
  | sudo bash -s -- --hub lm-hub.lrbtechnologies.com --secret <spoke-secret>
```

| Flag | Purpose |
| :--- | :--- |
| `--hub URL` | Hub WebSocket URL. A bare host is fine — `lm-hub.example.com` becomes `wss://lm-hub.example.com:443`, `host:port` gets a `wss://` prefix, and an explicit `ws://`/`wss://` is left alone. Omit it to auto-discover the hub (DNS `lm-hub.<suffix>`, then mDNS `_lm-hub._tcp.local.`). |
| `--id`, `--name` | Pin the spoke id. Omitted, the id derives from the hostname, so a renamed clone reconnects under its new name. |
| `--secret` | Required pre-shared spoke secret. You may pass it here or set `SPOKE_SECRET`; startup fails closed if neither is present. |
| `--hub-secret` | Hub PSK for auto-approval. Without it the spoke lands in *pending approval* in the WebUI. |
| `--all-prereqs` | Accepted and ignored — kept so the hub's install-module call doesn't abort. |

**Environment overrides:** `HUB_URL` (same normalization as `--hub`), `SPOKE_ID`, `SPOKE_SECRET` (required unless `--secret` is passed).
<!-- INSTALLERS:END -->

## Layout

```
src/
  truenas_spoke.py    TruenasSpoke(BaseSpoke) — hub command dispatcher
  truenas_engine.py   TruenasEngine — fleet + per-appliance read/write
  truenas_client.py   TrueNASClient — official truenas_api_client wrapper
  credentials.py      per-tenant API-key store (0600)
  control_plane.py    TruenasControlPlane — module_type "storage", poll loop
install_truenas.sh    systemd unit lm-truenas + venv + clone
requirements.txt     + truenas-api-client (git-pinned)
```

## Hub command surface

Read: `TRUENAS_LIST_APPLIANCES`, `TRUENAS_PROBE`, `TRUENAS_GET_POOLS`,
`TRUENAS_GET_DATASETS`, `TRUENAS_GET_DISKS`, `TRUENAS_GET_SHARES`,
`TRUENAS_GET_ALERTS`, `TRUENAS_GET_SERVICES`, `TRUENAS_GET_CAPACITY`,
`TRUENAS_POLL`. Write (gated by the hub's `write_scope`):
`TRUENAS_CREATE_DATASET`, `TRUENAS_DELETE_DATASET`, `TRUENAS_CREATE_SHARE`,
`TRUENAS_CREATE_SNAPSHOT`, `TRUENAS_RUN_SCRUB`.

## TrueNAS API notes

- WebSocket JSON-RPC at `wss://<host>/api/current` (25.04+; legacy DDP
  `/websocket` on Core/24.x — the official client falls back automatically).
- Auth: API key via `auth.login_with_api_key` (SCRAM-SHA-512 on 26+, PLAIN on
  older). `verify_ssl=False` for self-signed boxes (per appliance).
- Persistent one-connection-per-appliance: API keys auto-revoke over plain
  `ws://`; the 20-auth/60s rate limit (10-min cooldown) makes per-call login
  infeasible. We force `wss://` for any non-loopback host.
- `pool.query` is the heaviest read — `select` narrows the pull. Long jobs
  (`pool.scrub.start`) pass `job=True` + the client polls `core.get_jobs`.
- Only cross-platform methods are surfaced (pool/disk/dataset/sharing/service/
  alert/system/reporting). SCALE-only (`app.*`/`vm.*`/`docker.*`) and
  Core-only (`jail.*`/`plugin.*`) namespaces are out of scope.

## Install

```bash
curl -sSL https://raw.githubusercontent.com/lbockenstedt/truenas/main/install_truenas.sh | bash -s -- --hub <hub> --id truenas-<host> --secret <spoke-secret>
```

Or load the `truenas` role on a generic agent (WebUI Setup → Agents → Load
Role). The agent shallow-clones this repo + pip-installs `requirements.txt` +
spawns a `RoleConnection` under `module_type "storage"`.