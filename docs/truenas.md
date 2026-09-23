# truenas — TrueNAS Storage Spoke (Lab Manager Module)

`truenas` is the Lab Manager spoke module responsible for integrating TrueNAS CORE and TrueNAS SCALE storage appliances with the Lab Manager central control plane and WebUI.

---

## Architecture & Integration

The `truenas` module operates as an independent spoke communicating with TrueNAS appliances over their official JSON-RPC 2.0 WebSocket API:

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

1. **Hub-to-Spoke Transport:** Dial-out WebSocket on port 443 with push-ack-retry mailbox pattern.
2. **Spoke Engine (`src/truenas_engine.py`):** Manages a multi-tenant fleet of TrueNAS appliances. Holds lazy, persistent `TrueNASClient` instances.
3. **Appliance Client (`src/truenas_client.py`):** Speaks WebSocket JSON-RPC 2.0 (`wss://<host>/api/current`, fallback to `/websocket`). Connects using API Key authentication (`auth.login_with_api_key`) and manages long-running jobs (e.g. pool scrubs) via `core.get_jobs`.
4. **Credential Security (`src/credentials.py`):** Per-tenant API-key encryption and storage (`0600` file permissions) with full credential masking across logs.

---

## Features & Capabilities

- **ZFS Storage Pool Telemetry:** Discovers and monitors ZFS pools, healthy/degraded vdev topology, scrubbing status, and allocation percentages.
- **Dataset Lifecycle & Quotas:** Enumerates ZFS datasets, mounts, compression types, and provisions new datasets with quota configuration.
- **File Sharing Management:** Inspects and configures SMB and NFS network file shares, mapping datasets to access permissions and export options.
- **Snapshot Inspection:** Inspects periodic snapshot tasks and provides manual snapshot triggering (`TRUENAS_CREATE_SNAPSHOT`).
- **Disk Inventory & SMART Metrics:** Polls physical drive models, serial numbers, temperature, rotation speeds, and enclosure slot mapping.
- **System Services & Alerts:** Monitors core services (SMB, NFS, SSH) and gathers active hardware/system alerts.

---

## Command Reference

| Command | Type | Description |
| :--- | :--- | :--- |
| `UPDATE_CONFIG` | Config | Updates fleet appliance inventory and multi-tenancy mappings. |
| `GET_VERSION` | Utility | Returns module VERSION from file. |
| `TRUENAS_LIST_APPLIANCES` | Read | Returns appliance fleet summary and concurrent health probes. |
| `TRUENAS_PROBE` | Read | Probes reachability and system information (`system.info`). |
| `TRUENAS_GET_POOLS` | Read | Queries active ZFS storage pools (`pool.query`). |
| `TRUENAS_GET_DATASETS` | Read | Enumerates ZFS datasets and properties (`pool.dataset.query`). |
| `TRUENAS_GET_DISKS` | Read | Queries physical disks and SMART parameters (`disk.query`). |
| `TRUENAS_GET_SHARES` | Read | Queries SMB or NFS shares (`sharing.smb.query`, `sharing.nfs.query`). |
| `TRUENAS_GET_ALERTS` | Read | Gathers active system alerts (`alert.list`). |
| `TRUENAS_GET_SERVICES` | Read | Queries system services and run states (`service.query`). |
| `TRUENAS_GET_CAPACITY` | Read | Calculates pool capacity, allocation, and free space. |
| `TRUENAS_POLL` | Read | Full aggregation poll returning pools, datasets, disks, shares, alerts, and capacity. |
| `TRUENAS_CREATE_DATASET` | Write | Creates a new dataset on a designated pool (`pool.dataset.create`). |
| `TRUENAS_DELETE_DATASET` | Write | Deletes a dataset by path or ID (`pool.dataset.delete`). |
| `TRUENAS_CREATE_SHARE` | Write | Configures a new SMB or NFS file share (`sharing.<kind>.create`). |
| `TRUENAS_CREATE_SNAPSHOT` | Write | Creates an instantaneous ZFS snapshot (`zfs.snapshot.create`). |
| `TRUENAS_RUN_SCRUB` | Write | Initiates a pool scrub job (`pool.scrub.start`). |
