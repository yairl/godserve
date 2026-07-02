"""Thin depth-tracking facade over db.py (PLAN §1.4).

The single place dispatch reads jobs from — the coordinator's one FIFO queue.
"""

from __future__ import annotations

from ..db import DB


class Queue:
    def __init__(self, db: DB):
        self._db = db

    async def depth(self) -> int:
        return await self._db.queued_depth()

    async def oldest_queued(
        self,
        match: dict[str, str] | None,
        max_tier_ge: int,
        skip_window_s: float,
    ):
        return await self._db.oldest_queued(match, max_tier_ge, skip_window_s)
