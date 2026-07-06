"""The single worker loop — identical for every backend (PLAN §1.5, §4).

Connects the outbound WS with backoff, sends ``hello`` (tier from
``GODSERVE_TIER``, warm envs from a disk scan), advertises ``ready`` on each free
slot, runs the backend on ``assign`` streaming frames up the WS, heartbeats at
``lease_ttl/3``, and handles ``cancel``/``shutdown``/``goodbye``. Execution is
always the built-in :class:`LocalBackend`; ``GODSERVE_BACKEND`` is an opaque
label inherited by job subprocesses, never resolved by core.
"""

from __future__ import annotations

import asyncio
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
    """Construct the worker's execution backend.

    godserve always executes via the built-in :class:`LocalBackend`.
    ``GODSERVE_BACKEND`` is an opaque label inherited by job subprocesses
    (setup.sh + the serve process) so a handler may pick its own remoting; core
    never reads it to select a backend.
    """
    return LocalBackend(provider, scratch_root)


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

        self._ws: websockets.WebSocketClientProtocol | None = None
        self._slots: dict[str, asyncio.Task] = {}
        # Single bounded FIFO of outbound frames (agent lifetime — frames survive
        # reconnect). One sender task per connection drains it to ws.send, giving
        # end-to-end ordered lossless streaming: emit awaits `put` (backpressure).
        self._out: asyncio.Queue = asyncio.Queue(maxsize=1024)
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
        # Hello goes directly on the ws BEFORE the sender starts, so any frames
        # already queued (from a prior connection) can't precede Hello.
        await ws.send(Hello(
            worker_id=self._worker_id,
            tier=self._tier,
            max_slots=self._max_slots,
            warm_envs=self.warm_envs(),
            live_sessions=self._backend.live_sessions(),
        ).dump())
        sender = asyncio.create_task(self._sender_loop(ws))
        hb = asyncio.create_task(self._heartbeat_loop())
        await self._send_ready()
        try:
            async for raw in ws:
                frame = parse_frame(raw if isinstance(raw, (bytes, str)) else bytes(raw))
                await self._on_frame(frame)
        finally:
            hb.cancel()
            sender.cancel()
            for task in (hb, sender):
                try:
                    await task
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
        else:
            # FIFO on the single outbound queue guarantees this Result lands
            # after every partial/log for the job — no terminal flush needed.
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

    async def _emit_log(self, job_id: str, stream: str, data: str) -> None:
        await self._send(Output(job_id=job_id, stream=stream, data=data))

    async def _emit_partial(self, job_id: str, data: str) -> None:
        await self._send(Partial(job_id=job_id, data=data))

    async def _send_ready(self) -> None:
        if self.free_slots() > 0:
            await self._send(Ready(
                slots_free=self.free_slots(),
                warm_envs=self.warm_envs(),
                live_sessions=self._backend.live_sessions(),
            ))

    async def _heartbeat_loop(self) -> None:
        interval = max(DEFAULT_LEASE_TTL_S / 3, 1)
        while True:
            await asyncio.sleep(interval)
            # Never block: a full queue means frames are already flowing (which
            # renews the lease via those sends). A queue stuck behind a giant
            # Result can still expire the lease — the accepted failure mode.
            try:
                self._out.put_nowait(Heartbeat(running=list(self._slots.keys())))
            except asyncio.QueueFull:
                pass

    async def _sender_loop(self, ws) -> None:
        # The ONLY caller of ws.send after Hello. Drains the FIFO in order so
        # streaming stays lossless and ordered; cancelled in _session's finally.
        while True:
            frame = await self._out.get()
            try:
                await ws.send(frame.dump())
            except Exception as exc:
                log.debug("send failed: %s", exc)
                return

    async def _send(self, frame) -> None:
        # Backpressure: block the caller (handler emit / terminal Result) until
        # the frame is accepted into the bounded FIFO. The sender task drains it.
        await self._out.put(frame)

    def _try_send(self, frame) -> None:
        # Teardown frames (Goodbye) must never block: if the sender is gone or
        # the queue is full, drop rather than hang the stop/drain path.
        try:
            self._out.put_nowait(frame)
        except asyncio.QueueFull:
            pass

    async def _begin_drain(self) -> None:
        self._drain = True
        self._try_send(Goodbye(drain=True))
        if self._slots:
            await asyncio.gather(*self._slots.values(), return_exceptions=True)
        await self._shutdown_backend()
        self._stopping.set()

    async def stop(self) -> None:
        self._stopping.set()
        for task in list(self._slots.values()):
            task.cancel()
        if self._slots:
            await asyncio.gather(*self._slots.values(), return_exceptions=True)
        await self._shutdown_backend()
        self._try_send(Goodbye(drain=False))

    async def _shutdown_backend(self) -> None:
        try:
            await self._backend.shutdown()
        except Exception as exc:
            log.warning("backend shutdown error: %s", exc)


async def run_agent(url: str, work_root: str, max_slots: int = 1) -> None:
    agent = Agent(url, work_root, max_slots=max_slots)
    await agent.run_forever()
