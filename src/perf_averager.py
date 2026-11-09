"""Turns frequent performance samples into 5-minute averages.

The spoke feeds it a sample per appliance every ~30 seconds. When a 5-minute
bucket ends, its averages move to ``pending`` until they are sent to the hub.
Generic on purpose: any storage spoke can feed it flat ``{metric: number}``.
"""
from typing import Any, Dict, List

BUCKET_S = 300
# Finished buckets kept while the hub is unreachable (7 days of 10 appliances).
MAX_PENDING = 7 * 24 * 12 * 10


def truenas_metrics(perf: Any) -> Dict[str, float]:
    """Flatten a TrueNAS ``performance()`` result into numeric metrics."""
    if not isinstance(perf, dict):
        return {}
    disks = perf.get("disks") if isinstance(perf.get("disks"), dict) else {}
    out = {k: disks.get(k) for k in
           ("read_ops", "write_ops", "read_bytes", "write_bytes", "busy")}
    out["cpu"] = perf.get("cpu_usage")
    ifaces = [i for i in perf.get("interfaces") or [] if isinstance(i, dict)]
    out["net_rx"] = sum(_num(i.get("received_bytes_rate")) or 0 for i in ifaces) if ifaces else None
    out["net_tx"] = sum(_num(i.get("sent_bytes_rate")) or 0 for i in ifaces) if ifaces else None
    return {k: float(v) for k, v in out.items() if _num(v) is not None}


def _num(v: Any):
    """v if it is a real number, else None."""
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


class PerfAverager:
    """Per-device 5-minute averages, queued in ``pending`` until sent."""

    def __init__(self, max_pending: int = MAX_PENDING):
        # device -> (bucket start, {metric: [sum, count]})
        self._open: Dict[str, tuple] = {}
        self.pending: List[Dict[str, Any]] = []
        self._max_pending = max_pending

    def add(self, device_id: str, metrics: Dict[str, float], now: float) -> None:
        """Add one sample to the device's current 5-minute bucket."""
        bucket = int(now // BUCKET_S) * BUCKET_S
        cur = self._open.get(device_id)
        if cur and cur[0] != bucket:
            self._close(device_id)
        _, sums = self._open.setdefault(device_id, (bucket, {}))
        for k, v in metrics.items():
            s = sums.setdefault(k, [0.0, 0])
            s[0] += v
            s[1] += 1

    def close_finished(self, now: float) -> None:
        """Close buckets that have ended (also for devices that stopped answering)."""
        current = int(now // BUCKET_S) * BUCKET_S
        for device_id in [d for d, (b, _) in self._open.items() if b < current]:
            self._close(device_id)

    def _close(self, device_id: str) -> None:
        bucket, sums = self._open.pop(device_id)
        if not sums:
            return
        self.pending.append({
            "device_id": device_id, "ts": bucket,
            "samples": max(n for _, n in sums.values()),
            "metrics": {k: round(s / n, 2) for k, (s, n) in sums.items()},
        })
        # Hub unreachable for days: drop the oldest.
        del self.pending[:-self._max_pending]
