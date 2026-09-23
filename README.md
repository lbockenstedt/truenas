# truenas — TrueNAS Storage Spoke (Lab Manager Module)

The `truenas` spoke module connects TrueNAS CORE and TrueNAS SCALE storage appliances to the Lab Manager (LM) unified management platform. It manages a fleet of appliances over the official `truenas_api_client` WebSocket JSON-RPC 2.0 interface, providing centralized storage pool telemetry, dataset lifecycle and quota management, SMB/NFS file share configuration, periodic snapshot monitoring, and background asynchronous polling with hub-side caching.

---

## Architecture

The `truenas` module bridges the LM Hub control plane and multiple independent TrueNAS systems:

```
┌─────────────────┐             WebSocket / TLS (:443)             ┌───────────────────────┐
│     LM Hub      │ ◄────────────────────────────────────────────► │     truenas Spoke     │
│  Control Plane  │                                                │  (TruenasControlPlane) │
└─────────────────┘                                                └───────────┬───────────┘
                                                                               │
                                                   Persistent WebSocket JSON-RPC (/api/current)
                                                                               │
                                                       ┌───────────────────────┴───────────────────────┐
                                                       ▼                                               ▼
                                           ┌───────────────────────┐                       ┌───────────────────────┐
                                           │ TrueNAS CORE / SCALE  │                       │ TrueNAS CORE / SCALE  │
                                           │     (Appliance 1)     │                       │     (Appliance 2)     │
                                           └───────────────────────┘                       └───────────────────────┘
```

1. **Hub Integration & Control Plane (`src/control_plane.py`):**
   - Establishes a persistent outbound TLS WebSocket link to the LM Hub (`/ws/spoke` on port 443).
   - Manages heartbeat synchronization (30s interval) and push-ack-retry mailbox command delivery.
   - Executes periodic background polling loops that emit telemetry and populate cache structures.

2. **Fleet Management & Engine (`src/truenas_engine.py`):**
   - Maintains an in-memory fleet inventory populated via `UPDATE_CONFIG` from the LM Hub.
   - Enforces multi-tenant isolation, matching appliances by `tenant_id` or global `shared_tenant_id`.
   - Caches active, persistent `TrueNASClient` instances per appliance to prevent connection churn.

3. **TrueNAS API Client (`src/truenas_client.py`):**
   - Connects to TrueNAS CORE and SCALE using WebSocket JSON-RPC 2.0 at `wss://<host>/api/current` (with legacy fallback to `/websocket`).
   - Authenticates via `auth.login_with_api_key` using pre-shared API keys.
   - Manages asynchronous job tracking via `core.get_jobs` for long-running operations like pool scrubs.

4. **Credential Isolation (`src/credentials.py`):**
   - Stores per-tenant API keys in secure, permission-restricted files (`0600`).
   - Completely masks all credentials (`api_key`, `password`, `secret`) in log streams.

---

## Features

- **ZFS Storage Pool Monitoring:** Real-time visibility into pool status (`ONLINE`, `DEGRADED`, `FAULTED`), vdev layout, capacity usage, allocation rates, and background scrub progress.
- **Dataset Management & Quotas:** Create and delete ZFS datasets, inspect mount points, configure compression algorithms (LZ4, ZSTD), and manage volume quotas.
- **SMB & NFS File Sharing:** Enumerate active file exports, provision new SMB shares with guest/ACL configurations, and manage NFS exports.
- **Snapshot & Replication Visibility:** Inspect periodic snapshot schedules, query existing ZFS snapshots, and trigger manual snapshots on demand.
- **Physical Disk Telemetry:** Polls disk health, serial numbers, drive models, temperature readings, rotation speeds, and enclosure slot positions.
- **Alerts & Services:** Captures active TrueNAS system alerts and queries system service run-states (SMB, NFS, SSH, iSCSI, WebDAV).

---

## Spoke Commands Reference Table

The spoke handles the following commands dispatched from the LM Hub via `src/truenas_spoke.py`:

| Command | Category | Description |
| :--- | :--- | :--- |
| `UPDATE_CONFIG` | Configuration | Updates the appliance fleet list and multi-tenant scoping parameters. |
| `GET_VERSION` | Utility | Returns the current module version from `VERSION`. |
| `TRUENAS_LIST_APPLIANCES` | Fleet Telemetry | Returns a fleet summary with concurrent 6-second reachability and latency probes. |
| `TRUENAS_PROBE` | Appliance Telemetry | Probes a specific TrueNAS appliance and returns `system.info` metadata. |
| `TRUENAS_GET_POOLS` | Storage Read | Queries ZFS storage pools via `pool.query`. |
| `TRUENAS_GET_DATASETS` | Storage Read | Queries ZFS datasets, compression settings, and mount points via `pool.dataset.query`. |
| `TRUENAS_GET_DISKS` | Storage Read | Queries physical disks, serial numbers, and SMART attributes via `disk.query`. |
| `TRUENAS_GET_SHARES` | Storage Read | Queries active file shares via `sharing.smb.query` or `sharing.nfs.query`. |
| `TRUENAS_GET_ALERTS` | System Read | Fetches active system alarms and warnings via `alert.list`. |
| `TRUENAS_GET_SERVICES` | System Read | Queries operational status of TrueNAS background services via `service.query`. |
| `TRUENAS_GET_CAPACITY` | Storage Read | Computes pool capacities, allocated space, and free space. |
| `TRUENAS_POLL` | Aggregation | Executes complete poll: system info, pools, datasets, disks, shares, alerts, and capacity. |
| `TRUENAS_CREATE_DATASET` | Management Write | Provisions a new ZFS dataset within a target pool (`pool.dataset.create`). |
| `TRUENAS_DELETE_DATASET` | Management Write | Deletes an existing ZFS dataset (`pool.dataset.delete`). |
| `TRUENAS_CREATE_SHARE` | Management Write | Configures a new SMB or NFS file export (`sharing.<kind>.create`). |
| `TRUENAS_CREATE_SNAPSHOT` | Management Write | Creates an immediate ZFS snapshot for a dataset (`zfs.snapshot.create`). |
| `TRUENAS_RUN_SCRUB` | Management Write | Triggers an asynchronous pool scrub job (`pool.scrub.start`). |

---

## Installation & Credentials Guide

The `truenas` module can be installed as a dedicated systemd service or loaded dynamically as an agent role.

### 1. Standalone Spoke Installation (`install_truenas.sh`)

Run on the spoke host or container:

```bash
curl -sSL https://raw.githubusercontent.com/lbockenstedt/truenas/main/install_truenas.sh \
  | sudo bash -s -- --hub wss://lm-hub.example.com:443 --secret <spoke-secret>
```

| Flag | Description |
| :--- | :--- |
| `--hub URL` | LM Hub WebSocket endpoint (`wss://<host>:443`). Bare hostnames are automatically normalized. |
| `--id`, `--name` | Unique spoke identifier (defaults to `truenas-<hostname>`). |
| `--secret` | Pre-shared key for authenticating the spoke with the LM Hub. |
| `--hub-secret` | Hub PSK enabling automatic spoke approval. |
| `--all-prereqs` | Installs system dependencies and python virtual environment. |

### 2. TrueNAS Appliance Credentials

TrueNAS appliances authenticate using API Keys:

1. **Generating an API Key in TrueNAS:**
   - Log into the TrueNAS web console.
   - Navigate to **Gear Icon (top right) → API Keys**.
   - Click **Add**, name the key (e.g., `lab-manager`), and copy the generated token.

2. **Configuring Appliances in Lab Manager:**
   - In the LM WebUI, navigate to **Storage → TrueNAS Settings**.
   - Add an appliance entry specifying:
     - **Host / IP**: IP address or FQDN of the TrueNAS system.
     - **API Key**: The token generated above.
     - **Verify SSL**: Toggle off if using default TrueNAS self-signed TLS certificates.
     - **Tenant ID**: Assign to a specific tenant or leave empty for shared fleet access.
   - The LM Hub securely encrypts credentials and pushes appliance configurations to the spoke via `UPDATE_CONFIG`.

---

## Testing & Verification

Run the test suite using `pytest`:

```bash
pytest tests
```