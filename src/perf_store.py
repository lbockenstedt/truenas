"""7 days of 5-minute performance averages on the spoke's disk (one JSON file).

The spoke is the store of record: the hub keeps a copy of what was pushed for
the default 7-day view, and asks the spoke (``TRUENAS_GET_PERF_HISTORY``) for
any other range. ``sent`` marks rows the hub has received, so averages made
while the hub was unreachable survive a spoke restart and are sent later.
Standard library only.
"""
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List

logger = logging.getLogger("TruenasPerfStore")

KEEP_S = 7 * 86400
DATA_DIR = os.getenv("LM_TRUENAS_DATA_DIR", "/var/lib/lm-truenas")


class PerfStore:
    """``{device_id: [{ts, samples, metrics, sent}]}`` (oldest first), saved
    to ``perf.json`` after every change."""

    def __init__(self, path: str = None):
        self.path = path or os.path.join(DATA_DIR, "perf.json")
        self._lock = threading.Lock()
        self._series = None

    def _load(self) -> Dict[str, List[Dict[str, Any]]]:
        if self._series is None:
            try:
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
                self._series = {k: v for k, v in data.items() if isinstance(v, list)} \
                    if isinstance(data, dict) else {}
            except FileNotFoundError:
                self._series = {}
            except (OSError, ValueError) as e:
                logger.warning("perf store %s unreadable (%s); starting empty", self.path, e)
                self._series = {}
        return self._series

    def _save(self) -> None:
        """Write atomically; on failure keep the data in memory and warn."""
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._series, f, separators=(",", ":"))
            os.replace(tmp, self.path)
        except OSError as e:
            logger.warning("perf store %s not saved: %s", self.path, e)

    def add(self, rows: List[Dict[str, Any]], now: float = None) -> None:
        """Save closed buckets (unsent) and drop anything older than 7 days."""
        now = time.time() if now is None else now
        with self._lock:
            series = self._load()
            for r in rows:
                by_ts = {x["ts"]: x for x in series.get(r["device_id"], [])}
                by_ts[int(r["ts"])] = {"ts": int(r["ts"]), "samples": r.get("samples"),
                                       "metrics": r["metrics"], "sent": False}
                series[r["device_id"]] = [by_ts[t] for t in sorted(by_ts)]
            for device_id in list(series):
                series[device_id] = [x for x in series[device_id] if x["ts"] >= now - KEEP_S]
                if not series[device_id]:
                    del series[device_id]
            self._save()

    def unsent(self, limit: int = 2000) -> List[Dict[str, Any]]:
        """Oldest buckets the hub hasn't received yet."""
        with self._lock:
            rows = [{"device_id": d, "ts": x["ts"], "samples": x["samples"], "metrics": x["metrics"]}
                    for d, xs in self._load().items() for x in xs if not x.get("sent")]
        return sorted(rows, key=lambda r: r["ts"])[:limit]

    def mark_sent(self, rows: List[Dict[str, Any]]) -> None:
        """Record that the hub received these buckets."""
        keys = {(r["device_id"], int(r["ts"])) for r in rows}
        with self._lock:
            for d, xs in self._load().items():
                for x in xs:
                    if (d, x["ts"]) in keys:
                        x["sent"] = True
            self._save()

    def history(self, device_id: str, start: float, end: float) -> List[Dict[str, Any]]:
        """The device's buckets with ``start <= ts <= end``, oldest first."""
        with self._lock:
            return [{"ts": x["ts"], "samples": x["samples"], "metrics": x["metrics"]}
                    for x in self._load().get(device_id, []) if start <= x["ts"] <= end]
