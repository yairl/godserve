"""The single worker loop — identical for every backend (PLAN §1.5, §4).

Connects the outbound WS with backoff, sends ``hello`` (tier from
``GODSERVE_TIER``, warm envs from a disk scan), advertises ``ready`` on each free
slot, runs the backend on ``assign`` streaming frames up the WS, heartbeats at
``lease_ttl/3``, and handles ``cancel``/``shutdown``/``goodbye``. The backend is
chosen once at startup via ``GODSERVE_BACKEND``.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import socket
import uuid
from pathlib import Path

import websockets

from ..protocol import (
    Assign,
    Cancel,
    Goodbye,
    Hello,
    Heartbeat,
    NoWork,
    Output,
    Partial,
    Prepare,
    Ready,
    Result,
    Shutdown,
    parse_frame,
)
from .backends.base import JobIO, JobOutcome
from .backends.local import LocalBackend
from .envs.venv import VenvProvider

log = logging.getLogger(__name__)

DEFAULT_LEASE_TTL_S = 30


def _make_backend(provider: VenvProvider, scratch_root: str):
    """Resolve ``GODSERVE_BACKEND`` (default ``local``) into a backend instance.

    Three forms, discriminated strictly on the presence of ``:``:

    - ``local`` — the built-in :class:`LocalBackend`, constructed with the
      core-owned ``(provider, scratch_root)`` signature.
    - ``"module:attr"`` — a pluggable backend loaded by import path. The class
      is instantiated **zero-arg** (``cls()``); a loaded backend reads whatever
      it needs from its own ``GODSERVE_*`` env — core passes it nothing
      backend-specific.
    - any other bareword — a clean ``ValueError``. There is no silent fallback
      to ``local``; bad import paths are wrapped into ``ValueError`` too.
    """
    name = os.environ.get("GODSERVE_BACKEND", "local")
    if name == "local":
        return LocalBackend(provider, scratch_root)
    if ":" in name:
        module_name, _, attr = name.partition(":")
        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, attr)
        except (ImportError, AttributeError) as exc:
            raise ValueError(
                f"GODSERVE_BACKEND={name!r} could not be imported: {exc}"
            ) from exc
        return cls()
    raise ValueError(
        f"GODSERVE_BACKEND={name!r} is not valid; use 'local' or an import "
        "path of the form 'module:attr'"
    )


class Agent:
    def __init__(
        self,
        url: str,
        work_root: str,
        max_slots: int = 1,
        worker_id: str | None = None,
    ):
        self._url = url
        self._work_root = Path(work_root)
        self._work_root.mkdir(parents=True, exist_ok=True)
        self._max_slots = max_slots
        self._worker_id = worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._tier = int(os.environ.get("GODSERVE_TIER", "0"))

        self._provider = VenvProvider(str(self._work_root))
        self._backend = _make_backend(self._provider, str(self._work_root / "scratch"))
        # Defensive: import-path backends may not implement live_sessions().
        self._live_sessions = getattr(self._backend, "live_sessions", lambda: [])

        self._ws: websockets.WebSocketClientProtocol | None = None
        self._send_lock = asyncio.Lock()
        self._slots: dict[str, asyncio.Task] = {}
        self._emits: set[asyncio.Task] = set()
        # Emit tasks grouped by job_id so the terminal boundary can flush a
        # job's outstanding partials/logs before its Result (see _run_job).
        self._job_emits: dict[str, set[asyncio.Task]] = {}
        self._stopping = asyncio.Event()
        self._drain = False

    def warm_envs(self) -> list[str]:
        return self._provider.warm_keys()

    def free_slots(self) -> int:
        return self._max_slots - len(self._slots)

    async def run_forever(self) -> None:
        backoff = 0.5
        while not self._stopping.is_set():
            try:
                async with websockets.connect(self._url, max_size=None) as ws:
                    self._ws = ws
                    backoff = 0.5
                    await self._session(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("worker WS error, reconnecting: %s", exc)
            finally:
                self._ws = None
            if self._stopping.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    async def _session(self, ws) -> None:
        await self._send(Hello(
            worker_id=self._worker_id,
            tier=self._tier,
            max_slots=self._max_slots,
            warm_envs=self.warm_envs(),
            live_sessions=self._live_sessions(),
        ))
        await self._send_ready()
        hb = asyncio.create_task(self._heartbeat_loop())
        try:
            async for raw in ws:
                frame = parse_frame(raw if isinstance(raw, (bytes, str)) else bytes(raw))
                await self._on_frame(frame)
        finally:
            hb.cancel()
            try:
                await hb
            except asyncio.CancelledError:
                pass

    async def _on_frame(self, frame) -> None:
        if isinstance(frame, Assign):
            self._start_job(frame.bundle)
        elif isinstance(frame, NoWork):
            pass
        elif isinstance(frame, Cancel):
            task = self._slots.get(frame.job_id)
            if task is not None:
                task.cancel()
        elif isinstance(frame, Shutdown):
            await self._begin_drain()
        elif isinstance(frame, Prepare):
            # Deferred (§4.5) — defined but unhandled.
            pass

    def _start_job(self, bundle) -> None:
        task = asyncio.create_task(self._run_job(bundle))
        self._slots[bundle.job_id] = task

    async def _run_job(self, bundle) -> None:
        io = JobIO(
            bundle.job_id,
            emit_log=self._emit_log,
            emit_partial=self._emit_partial,
        )
        try:
            outcome = await self._backend.run(bundle, io)
        except asyncio.CancelledError:
            outcome = JobOutcome(status="canceled", error="canceled")
        except Exception as exc:
            log.error("backend error for job %s: %s", bundle.job_id, exc, exc_info=True)
            outcome = JobOutcome(status="failed", error=str(exc))

        if outcome.requeue:
            # Session crash mid-job: suppress the terminal Result so the lease
            # lapses and the coordinator's sweeper requeues (ARCH §4.2). All job
            # state transitions still flow through claim/finish/requeue_expired.
            log.warning("job %s session crashed; leaving to lease requeue", bundle.job_id)
            self._job_emits.pop(bundle.job_id, None)
        else:
            # Terminal boundary: flush this job's outstanding partial/log sends
            # so none can land on the coordinator after the Result (which
            # finalizes the job and closes stream subscribers). Handler-time
            # emission stays fire-and-forget — only this boundary is ordered.
            pending = self._job_emits.pop(bundle.job_id, None)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            await self._send(Result(
                job_id=bundle.job_id,
                status=outcome.status,
                exit_code=outcome.exit_code,
                result=outcome.result,
                error=outcome.error,
            ))
        self._slots.pop(bundle.job_id, None)
        if not self._drain:
            await self._send_ready()

    def _emit_log(self, job_id: str, stream: str, data: str) -> None:
        # Fire-and-forget: scheduling a send must never block the handler.
        self._spawn_emit(job_id, self._send(Output(job_id=job_id, stream=stream, data=data)))

    def _emit_partial(self, job_id: str, data: str) -> None:
        self._spawn_emit(job_id, self._send(Partial(job_id=job_id, data=data)))

    def _spawn_emit(self, job_id: str, coro) -> None:
        # Retain a strong ref so the loop can't GC the task mid-flight and
        # silently drop the send; discard on completion. Also group by job_id so
        # the terminal boundary in _run_job can await this job's sends.
        task = asyncio.create_task(coro)
        self._emits.add(task)
        group = self._job_emits.setdefault(job_id, set())
        group.add(task)

        def _done(t: asyncio.Task, jid: str = job_id) -> None:
            self._emits.discard(t)
            g = self._job_emits.get(jid)
            if g is not None:
                g.discard(t)

        task.add_done_callback(_done)

    async def _send_ready(self) -> None:
        if self.free_slots() > 0:
            await self._send(Ready(
                slots_free=self.free_slots(),
                warm_envs=self.warm_envs(),
                live_sessions=self._live_sessions(),
            ))

    async def _heartbeat_loop(self) -> None:
        interval = max(DEFAULT_LEASE_TTL_S / 3, 1)
        while True:
            await asyncio.sleep(interval)
            try:
                await self._send(Heartbeat(running=list(self._slots.keys())))
            except Exception:
                return

    async def _send(self, frame) -> None:
        ws = self._ws
        if ws is None:
            return
        async with self._send_lock:
            try:
                await ws.send(frame.dump())
            except Exception as exc:
                log.debug("send failed: %s", exc)

    async def _begin_drain(self) -> None:
        self._drain = True
        await self._send(Goodbye(drain=True))
        if self._slots:
            await asyncio.gather(*self._slots.values(), return_exceptions=True)
        self._stopping.set()

    async def stop(self) -> None:
        self._stopping.set()
        for task in list(self._slots.values()):
            task.cancel()
        await self._send(Goodbye(drain=False))


async def run_agent(url: str, work_root: str, max_slots: int = 1) -> None:
    agent = Agent(url, work_root, max_slots=max_slots)
    await agent.run_forever()
