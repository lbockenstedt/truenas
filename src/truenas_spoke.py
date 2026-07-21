import logging
from pathlib import Path
from typing import Dict, Any

try:
    from core.src.base_spoke import BaseSpoke
except ImportError:
    from base_spoke import BaseSpoke
from truenas_engine import TruenasEngine

logger = logging.getLogger("TruenasSpoke")

# Credential field names — FULL mask in logs (never partial). Mirrors the
# nw spoke's masking precedent: leaking both ends of a credential exposes a
# meaningful fraction of a typical secret, so the whole value is replaced.
_SENSITIVE = {"api_key", "password", "secret", "hub_secret"}


class TruenasSpoke(BaseSpoke):
    """TrueNAS management + reporting spoke for Lab Manager.

    Translates Hub TRUENAS_* commands into per-appliance WebSocket JSON-RPC
    actions via :class:`TruenasEngine`. Manages a **fleet** of appliances (one
    spoke → many TrueNAS boxes) pushed from ``global_config["truenas_appliances"]``
    through UPDATE_CONFIG. Mirrors the nw spoke (fleet + _SENSITIVE mask +
    UPDATE_CONFIG stores the fleet + _log_result INFO/ERROR).
    """
    def __init__(self, spoke_id: str, config: Dict[str, Any]):
        # The engine needs the fleet before super().__init__ so any base-class
        # background worker sees it. The hub pushes appliances via UPDATE_CONFIG
        # after approval; at cold start config may carry appliances from a
        # pre-provisioned config.
        appliances = (config or {}).get("appliances", []) if isinstance(config, dict) else []
        self.engine = TruenasEngine(appliances)
        shared_tid = (config or {}).get("shared_tenant_id", "") if isinstance(config, dict) else ""
        if shared_tid:
            self.engine.shared_tenant_id = shared_tid
        super().__init__(spoke_id, config)

    # ── Logging helper: mask sensitive fields in any command data ───────────
    @staticmethod
    def _mask(data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return {k: ("********" if k in _SENSITIVE else v) for k, v in data.items()}

    async def handle_command(self, command_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch a hub TRUENAS_* command to the engine.

        Command types (case-insensitive): ``UPDATE_CONFIG`` (store the fleet,
        credentials masked in logs), ``GET_VERSION``,
        ``TRUENAS_LIST_APPLIANCES`` (fleet summary + concurrent 6s probe),
        ``TRUENAS_PROBE``, ``TRUENAS_GET_POOLS``, ``TRUENAS_GET_DATASETS``,
        ``TRUENAS_GET_DISKS``, ``TRUENAS_GET_SHARES`` (kind=smb|nfs|iscsi),
        ``TRUENAS_GET_ALERTS``, ``TRUENAS_GET_SERVICES``,
        ``TRUENAS_GET_CAPACITY``, ``TRUENAS_POLL`` (system_info + pools +
        datasets + disks + shares + alerts + services + capacity in one call,
        partial results on partial failure). Write/management (gated by the
        hub's write_scope): ``TRUENAS_CREATE_DATASET``, ``TRUENAS_DELETE_DATASET``,
        ``TRUENAS_CREATE_SHARE``, ``TRUENAS_CREATE_SNAPSHOT``,
        ``TRUENAS_RUN_SCRUB``. Unknown commands return an ERROR envelope.
        """
        normalized_cmd = (command_type or "").upper()
        log_data = self._mask(data)
        logger.info(f"Handling Truenas Command: {command_type} with data {log_data}")
        res = await self._dispatch_command(normalized_cmd, command_type, data)
        self._log_result(command_type, res)
        return res

    @staticmethod
    def _log_result(command_type: str, res: Dict[str, Any]) -> None:
        """Log every command's outcome: INFO on success, ERROR on failure. The
        ERROR line carries "error" so it surfaces in GET_ERROR_LOGS / the
        Error Log tab (same precedent as the nw spoke). ``errors`` (from
        TRUENAS_POLL) is surfaced as a sub-error count. Best-effort."""
        try:
            status = str((res or {}).get("status", "")).upper()
            msg = (res or {}).get("message", "")
            errors = (res or {}).get("errors") or []
            if status == "ERROR" or errors:
                logger.error("truenas command %s result: error — %s%s", command_type,
                             msg or "failed",
                             f" ({len(errors)} sub-error(s))" if errors else "")
            else:
                logger.info("truenas command %s result: %s", command_type,
                            status.lower() or "ok")
        except Exception:
            logger.debug("truenas log_result failed", exc_info=True)

    async def _dispatch_command(self, normalized_cmd: str, command_type: str,
                                data: Dict[str, Any]) -> Dict[str, Any]:
        # ── Lifecycle / config ──────────────────────────────────────────────
        if normalized_cmd == "UPDATE_CONFIG":
            appliances = (data or {}).get("appliances", []) if isinstance(data, dict) else []
            shared_tid = (data or {}).get("shared_tenant_id", "") if isinstance(data, dict) else ""
            summary = [{k: ("********" if k in _SENSITIVE else v)
                        for k, v in a.items()} for a in appliances] \
                if isinstance(appliances, list) else []
            logger.info(f"Updating truenas fleet configuration: "
                        f"{len(appliances if isinstance(appliances, list) else [])} "
                        f"appliance(s) -> {summary}")
            self.config = data or {}
            self.engine.set_appliances(appliances if isinstance(appliances, list) else [],
                                       shared_tenant_id=shared_tid)
            return {"status": "SUCCESS",
                    "message": "truenas configuration updated from Hub",
                    "appliance_count": len(self.engine.appliances)}

        if normalized_cmd in ("GET_VERSION", "GET-VERSION"):
            return {"status": "SUCCESS", "version": self.get_version()}

        # ── Fleet ───────────────────────────────────────────────────────────
        if normalized_cmd == "TRUENAS_LIST_APPLIANCES":
            tenant = (data or {}).get("tenant") if isinstance(data, dict) else None
            return await self.engine.list_appliances(tenant)

        # ── Per-appliance (data carries appliance_id) ───────────────────────
        d = data or {} if isinstance(data, dict) else {}
        appliance_id = d.get("appliance_id") or d.get("device_id") or ""
        tenant = d.get("tenant")

        if normalized_cmd == "TRUENAS_PROBE":
            return await self.engine.probe(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_POOLS":
            return await self.engine.get_pools(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_DATASETS":
            return await self.engine.get_datasets(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_DISKS":
            return await self.engine.get_disks(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_SHARES":
            kind = d.get("kind") or "smb"
            return await self.engine.get_shares(appliance_id, kind, tenant)

        if normalized_cmd == "TRUENAS_GET_ALERTS":
            return await self.engine.get_alerts(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_SERVICES":
            return await self.engine.get_services(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_CAPACITY":
            return await self.engine.get_capacity(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_POLL":
            return await self.engine.poll(appliance_id, tenant)

        # ── Write / management ─────────────────────────────────────────────
        if normalized_cmd == "TRUENAS_CREATE_DATASET":
            return await self.engine.create_dataset(
                appliance_id, d.get("pool", ""), d.get("name", ""),
                d.get("options"), tenant)

        if normalized_cmd == "TRUENAS_DELETE_DATASET":
            return await self.engine.delete_dataset(
                appliance_id, d.get("dataset", "") or d.get("dataset_id", ""),
                d.get("options"), tenant)

        if normalized_cmd == "TRUENAS_CREATE_SHARE":
            return await self.engine.create_share(
                appliance_id, d.get("kind", "smb"),
                d.get("dataset", "") or d.get("path", ""),
                d.get("options"), tenant)

        if normalized_cmd == "TRUENAS_CREATE_SNAPSHOT":
            return await self.engine.create_snapshot(
                appliance_id, d.get("dataset", ""), d.get("name", ""),
                d.get("options"), tenant)

        if normalized_cmd == "TRUENAS_RUN_SCRUB":
            return await self.engine.run_scrub(
                appliance_id, d.get("pool", "") or d.get("pool_id", ""), tenant)

        # ── Unknown ─────────────────────────────────────────────────────────
        logger.warning(f"Unknown Truenas command type: {command_type}")
        return {"status": "ERROR",
                "message": f"Command {command_type} not supported by truenas module"}

    async def get_status(self) -> Dict[str, Any]:
        """Native LM status report for the truenas fleet."""
        return {
            "spoke_id": self.spoke_id,
            "module": "truenas",
            "appliance_count": len(self.engine.appliances),
            "connection": "CONNECTED",
        }

    def get_version(self) -> str:
        """Current truenas module version (repo-root VERSION).

        Reads ``<repo>/VERSION`` (one dir above ``src/``). Same path pattern as
        the nw/opnsense spokes.
        """
        try:
            return (Path(__file__).resolve().parent.parent / "VERSION").read_text().strip()
        except Exception:
            logger.exception("Failed to read VERSION file")
            return "unknown"