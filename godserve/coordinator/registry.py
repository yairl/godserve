"""Worker table + lease-expiry sweeper (PLAN §1.4).

Keeps an in-memory mirror of connected workers (their live WS send channel) and
runs a ~1s background sweep that force-requeues expired leases via
``db.requeue_expired``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from ..db import DB
from ..protocol import Frame

log = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 1.0


@dataclass
class WorkerConn:
    worker_id: str
    tier: int
    max_slots: int
    send: Callable[[Frame], Awaitable[None]]
    warm_envs: list[str] = field(default_factory=list)
    live_sessions: list[str] = field(default_factory=list)
    running: set[str] = field(default_factory=set)
    last_seen: float = field(default_factory=time.time)
    idle: bool = False  # sent ready, currently holds an unused slot


class Registry:
    def __init__(self, db: DB, on_requeue: Callable[[list[str]], Awaitable[None]] | None = None):
        self._db = db
        self._workers: dict[str, WorkerConn] = {}
        self._on_requeue = on_requeue
        self._sweeper: asyncio.Task | None = None

    # --- worker lifecycle -------------------------------------------------

    async def register(self, conn: WorkerConn) -> None:
        self._workers[conn.worker_id] = conn
        await self._db.upsert_worker(
            conn.worker_id, conn.tier, "ready", conn.max_slots,
            conn.warm_envs, conn.live_sessions,
        )

    async def update(self, worker_id: str, warm_envs: list[str], live_sessions: list[str], state: str) -> None:
        conn = self._workers.get(worker_id)
        if conn is None:
            return
        conn.warm_envs = warm_envs
        conn.live_sessions = live_sessions
        conn.last_seen = time.time()
        await self._db.upsert_worker(
            worker_id, conn.tier, state, conn.max_slots, warm_envs, live_sessions,
        )

    def touch(self, worker_id: str) -> None:
        conn = self._workers.get(worker_id)
        if conn is not None:
            conn.last_seen = time.time()

    async def remove(self, worker_id: str) -> None:
        self._workers.pop(worker_id, None)
        await self._db.mark_worker_dead(worker_id)

    def get(self, worker_id: str) -> WorkerConn | None:
        return self._workers.get(worker_id)

    def find_by_job(self, job_id: str) -> WorkerConn | None:
        for conn in self._workers.values():
            if job_id in conn.running:
                return conn
        return None

    def idle_workers(self) -> list[WorkerConn]:
        return [c for c in self._workers.values() if c.idle]

    # --- sweeper ----------------------------------------------------------

    def start_sweeper(self) -> None:
        self._sweeper = asyncio.create_task(self._sweep_loop())

    async def stop_sweeper(self) -> None:
        if self._sweeper is not None:
            self._sweeper.cancel()
            try:
                await self._sweeper
            except asyncio.CancelledError:
                pass
            self._sweeper = None

    async def _sweep_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(SWEEP_INTERVAL_S)
                requeued = await self._db.requeue_expired(time.time())
                if requeued and self._on_requeue is not None:
                    await self._on_requeue(requeued)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("sweeper error: %s", exc, exc_info=True)
