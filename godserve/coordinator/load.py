"""Load-handling API: sustained-depth fill levels (ARCH §2, PLAN §3.1).

``LoadSnapshot`` is the per-tick measurement; ``LoadPolicy`` is the pluggable
decision (default ``SustainedDepthPolicy``); ``LoadController`` owns the ~1s
tick that updates hysteresis state, recomputes budgets, and exposes a synchronous
consume/refund surface to the dispatcher.

Budgets are in-memory only, fully reset each tick, and recomputed after restart
(§5). All controller mutations run on the single asyncio loop (single writer) —
no lock needed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Protocol

from ..db import DB
from ..models import LevelConfig

log = logging.getLogger(__name__)

TICK_INTERVAL_S = 1.0


@dataclass(frozen=True)
class LoadSnapshot:
    depth: int
    over_since: dict[int, float | None]
    in_flight: dict[int, int]


class LoadPolicy(Protocol):
    def budgets(self, snap: LoadSnapshot, now: float) -> dict[int, int]:
        """tier → remaining budget this tick. Pure function of the snapshot."""
        ...


class SustainedDepthPolicy:
    """Default policy (ARCH §2.2). Stickiness/hysteresis live in ``over_since``
    (controller-owned); this is a pure function of the snapshot."""

    def __init__(self, levels: list[LevelConfig]):
        self._levels = levels

    def budgets(self, snap: LoadSnapshot, now: float) -> dict[int, int]:
        out: dict[int, int] = {}
        for lvl in self._levels:
            since = snap.over_since.get(lvl.tier)
            active = since is not None and now - since >= lvl.sustain_s
            if active:
                headroom = lvl.max_inflight - snap.in_flight.get(lvl.tier, 0)
                out[lvl.tier] = max(0, min(headroom, snap.depth))
            else:
                out[lvl.tier] = 0
        return out


class LoadController:
    def __init__(
        self,
        db: DB,
        policy: LoadPolicy,
        levels: list[LevelConfig],
    ):
        self._db = db
        self._policy = policy
        self._levels = levels
        self._over_since: dict[int, float | None] = {lvl.tier: None for lvl in levels}
        # remaining budget this tick, and the ceiling refunds may not exceed.
        self._budget: dict[int, int] = {lvl.tier: 0 for lvl in levels}
        self._ceiling: dict[int, int] = {lvl.tier: 0 for lvl in levels}
        self._by_tier: dict[int, LevelConfig] = {lvl.tier: lvl for lvl in levels}
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._tick_loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # --- dispatcher-facing surface (synchronous, single-writer) -----------

    def budget(self, tier: int) -> int:
        return self._budget.get(tier, 0)

    def try_consume(self, tier: int) -> bool:
        if self._budget.get(tier, 0) > 0:
            self._budget[tier] -= 1
            return True
        return False

    def refund(self, tier: int) -> None:
        if tier not in self._budget:
            return
        # Clamp to the ceiling set at the last tick so a refund can't inflate
        # budget past what the policy allowed this tick.
        self._budget[tier] = min(self._budget[tier] + 1, self._ceiling.get(tier, 0))

    # --- tick -------------------------------------------------------------

    async def _tick_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(TICK_INTERVAL_S)
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("load tick error: %s", exc, exc_info=True)

    async def _tick(self) -> None:
        now = time.time()
        depth = await self._db.queued_depth()
        for lvl in self._levels:
            k = lvl.tier
            if depth >= lvl.depth and self._over_since[k] is None:
                self._over_since[k] = now
            elif depth < lvl.clear_below:
                self._over_since[k] = None
            # In the [clear_below, depth) band: leave over_since unchanged.

        in_flight = await self._db.inflight_by_tier()
        snap = LoadSnapshot(depth=depth, over_since=dict(self._over_since), in_flight=in_flight)
        new_budgets = self._policy.budgets(snap, now)
        # Full reset: do not carry leftover budget across ticks.
        self._budget = dict(new_budgets)
        self._ceiling = dict(new_budgets)
