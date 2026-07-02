"""Per-job in-memory log/partial fan-out (PLAN §1.4, §4.2).

Every subscriber gets a bounded queue; on overflow we DROP-OLDEST. A slow or
absent ``/stream`` client must never backpressure log ingestion — the GPU does
not wait for spectators.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict

log = logging.getLogger(__name__)

_MAX_QUEUE = 1024


class PubSub:
    def __init__(self) -> None:
        self._subs: dict[str, set[asyncio.Queue]] = defaultdict(set)

    def subscribe(self, job_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=_MAX_QUEUE)
        self._subs[job_id].add(q)
        return q

    def unsubscribe(self, job_id: str, q: asyncio.Queue) -> None:
        subs = self._subs.get(job_id)
        if subs is not None:
            subs.discard(q)
            if not subs:
                self._subs.pop(job_id, None)

    def publish(self, job_id: str, frame: dict) -> None:
        """Fire-and-forget. Never awaits, never blocks ingestion."""
        for q in self._subs.get(job_id, ()):
            self._offer(q, frame)

    @staticmethod
    def _offer(q: asyncio.Queue, frame: dict) -> None:
        while True:
            try:
                q.put_nowait(frame)
                return
            except asyncio.QueueFull:
                # Drop-oldest: make room, then retry.
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    return
