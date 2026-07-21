# Dependency self-heal — MUST run before the third-party imports below. A skewed
# auto-update / partial install can leave the venv missing a declared dep, which
# would hard-crash at import and crash-loop the unit under Restart=always.
# dep_guard is stdlib-only; it find_spec-checks requirements.txt and pip-installs
# any missing. Best-effort — an unavailable dep_guard is skipped, never fatal.
import os as _os
try:
    try:
        from core.src.dep_guard import ensure_requirements as _ensure_requirements
    except ImportError:
        from dep_guard import ensure_requirements as _ensure_requirements
    _ensure_requirements(_os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "requirements.txt"))
except Exception:
    pass

import logging
import argparse
import asyncio
import time
from typing import Dict, Any
try:
    from core.src.messaging.control_plane import BaseControlPlane
except ImportError:
    from messaging.control_plane import BaseControlPlane
from truenas_spoke import TruenasSpoke

try:
    from logging_setup import configure_logging
except ImportError:
    try:
        from core.src.logging_setup import configure_logging
    except ImportError:
        import logging as _logging
        _FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        _DFMT = '%Y-%m-%d %H:%M:%S'
        def configure_logging(default_level=_logging.INFO, *, log_file=None, **_):
            handlers = ([_logging.FileHandler(log_file), _logging.StreamHandler()]
                        if log_file else None)
            _logging.basicConfig(level=default_level, force=True,
                                 format=_FMT, datefmt=_DFMT, handlers=handlers)
# Configure root logging at boot (same rationale as nw control_plane).
configure_logging()
logger = logging.getLogger("TruenasControlPlane")


class TruenasControlPlane(BaseControlPlane):
    """Control Plane for the TrueNAS (storage) module.

    Inherits core connectivity and routing from BaseControlPlane. The spoke
    advertises module_type "storage" so the hub routes TRUENAS_* commands +
    pushes the truenas_appliances fleet via UPDATE_CONFIG on
    connect/approve/reconnect.
    """
    def get_service_name(self) -> str:
        return "lm-truenas"

    def __init__(self, spoke_id: str, secret: str, hub_secret: str = None,
                 hub_url: str = None, config: Dict[str, Any] = None):
        # Initialize attributes before calling super().__init__ so background
        # workers started by the base class see them.
        self.config = config or {}
        super().__init__(spoke_id, secret, hub_secret, hub_url)
        self.module_type = "storage"

    # Poll-loop tick granularity + per-appliance interval floor (seconds). The
    # cadence itself is per-appliance (``poll_interval`` on each
    # truenas_appliances entry); these bound the scheduler, not the user's choice.
    _TRUENAS_POLL_TICK = 10
    _TRUENAS_POLL_FLOOR = 60          # TrueNAS pool.query is heavier than an
                                      # ARP probe — keep a 1m floor.
    # Default cadence when an appliance has no poll_interval set. An explicit
    # 0 (the UI "Off" choice) disables; only an absent/blank value defaults.
    _TRUENAS_POLL_DEFAULT = 900       # 15 minutes

    async def run_hub_mode(self):
        """Native LM Spoke behavior."""
        logger.info(f"Starting TrueNAS Module in HUB MODE -> {self.hub_url}")

        truenas_spoke = TruenasSpoke(self.spoke_id, self.config)
        self.register_module("storage", truenas_spoke)

        # Autonomous per-appliance polling (spoke-driven). Started before the
        # main loop; it idles until connected + an appliance sets poll_interval.
        asyncio.create_task(self._truenas_poll_loop())

        # Delegate to BaseControlPlane's main loop
        await self.run()

    async def _truenas_poll_loop(self):
        """Per-appliance autonomous polling done **by the spoke**.

        Each TrueNAS appliance may set ``poll_interval`` (seconds) in its
        config; this ticks every ``_TRUENAS_POLL_TICK`` and polls any appliance
        whose interval has elapsed, pushing the result to the hub
        (``TRUENAS_POLL_RESULT``) so the hub warms its per-appliance cache —
        every Storage sub-view then loads instantly instead of blocking on a
        live WS JSON-RPC round-trip. ``poll_interval`` ≤ 0 / absent = disabled
        (or inherits the module default). Intervals are floored to
        ``_TRUENAS_POLL_FLOOR`` to avoid hammering an appliance. A newly-seen
        appliance is scheduled (not polled immediately) so a fleet reload
        staggers rather than stampedes."""
        next_due: Dict[str, float] = {}
        while True:
            await asyncio.sleep(self._TRUENAS_POLL_TICK)
            try:
                module = self.modules.get("storage")
                engine = getattr(module, "engine", None)
                if engine is None or getattr(self, "_hub_ws", None) is None:
                    continue
                mod_raw = (getattr(module, "config", {}) or {}).get("default_poll_interval")
                try:
                    module_default = (self._TRUENAS_POLL_DEFAULT
                                      if mod_raw in (None, "") else int(mod_raw))
                except (TypeError, ValueError):
                    module_default = self._TRUENAS_POLL_DEFAULT
                now = time.monotonic()
                due, seen = [], set()
                for a in list(engine.appliances):
                    aid = a.get("id")
                    if not aid:
                        continue
                    seen.add(aid)
                    raw = a.get("poll_interval")
                    if raw is None or raw == "":
                        interval = module_default          # inherit module default
                    else:
                        try:
                            interval = int(raw)            # appliance wins (incl 0=Off)
                        except (TypeError, ValueError):
                            interval = module_default
                    if interval <= 0:                       # explicit Off
                        next_due.pop(aid, None)
                        continue
                    interval = max(interval, self._TRUENAS_POLL_FLOOR)
                    deadline = next_due.get(aid)
                    if deadline is None:          # first sight → stagger
                        next_due[aid] = now + interval
                    elif now >= deadline:
                        next_due[aid] = now + interval
                        due.append(aid)
                for gone in set(next_due) - seen:  # prune removed appliances
                    next_due.pop(gone, None)
                if due:
                    sem = asyncio.Semaphore(3)

                    async def _one(appliance_id):
                        async with sem:
                            await self._truenas_poll_and_push(appliance_id)
                    await asyncio.gather(*(_one(x) for x in due))
            except Exception as e:  # noqa: BLE001 - loop must never die
                logger.debug("truenas poll loop tick error: %s", e)

    async def _truenas_poll_and_push(self, appliance_id: str):
        """Run one full engine poll + push it to the hub as
        TRUENAS_POLL_RESULT."""
        module = self.modules.get("storage")
        engine = getattr(module, "engine", None)
        if engine is None:
            return
        try:
            res = await engine.poll(appliance_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("truenas auto-poll %s failed: %s", appliance_id, e)
            return
        data = res.get("data") if isinstance(res, dict) else None
        if not isinstance(data, dict):
            return
        await self.send_to_hub("TRUENAS_POLL_RESULT",
                               {"appliance_id": appliance_id, "data": data})
        logger.info("truenas auto-poll %s -> pushed (status=%s)",
                    appliance_id, res.get("status"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Spoke ID")
    parser.add_argument("--secret", nargs='?', const="lm-secret", default="lm-secret",
                        help="Authentication secret (default: lm-secret)")
    parser.add_argument("--hub-secret", nargs='?', default="", const="",
                        help="Hub authentication secret for mutual auth")
    parser.add_argument("--hub", required=True, help="Hub WebSocket URL")
    args = parser.parse_args()

    cp = TruenasControlPlane(args.id, args.secret, args.hub_secret, args.hub)
    try:
        asyncio.run(cp.run_hub_mode())
    except KeyboardInterrupt:
        pass