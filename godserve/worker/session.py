"""Hot session layer — Session + SessionManager (PLAN §2.2, ARCH §4.2).

A serve-mode ``run.sh`` is launched **once** inside its env and becomes a live
process speaking newline-delimited JSON on **fd 3** (a single duplex UNIX
socket). Consecutive jobs matching the same ``session_key`` reuse the process,
so ``init()`` (model load) runs once. Idle sessions time out (freeing the GPU);
a crash mid-job kills the session and fails the job (the coordinator requeues).

Config knobs (all ``GODSERVE_``-prefixed):
  GODSERVE_MAX_LIVE_SESSIONS  cap on concurrent live sessions (default 1)
  GODSERVE_SESSION_IDLE_S     idle seconds before graceful shutdown (default 300)
  GODSERVE_SESSION_INIT_S     seconds to await SessionReady after spawn (default 300)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import socket
import tempfile
import time
from pathlib import Path

from ..models import JobBundle, JobSpec
from ..protocol import (
    INLINE_CAP,
    SessionJob,
    SessionPartial,
    SessionReady,
    SessionResult,
    parse_frame,
)
from .backends.base import JobIO, JobOutcome
from .envs.venv import VenvProvider

log = logging.getLogger(__name__)

_IDLE_TICK_S = 3.0


def _max_live_sessions() -> int:
    try:
        return max(1, int(os.environ.get("GODSERVE_MAX_LIVE_SESSIONS", "1")))
    except ValueError:
        return 1


def _session_idle_s() -> float:
    try:
        return float(os.environ.get("GODSERVE_SESSION_IDLE_S", "300"))
    except ValueError:
        return 300.0


def _session_init_s() -> float:
    try:
        return float(os.environ.get("GODSERVE_SESSION_INIT_S", "300"))
    except ValueError:
        return 300.0


class Session:
    """One live serve process. Owns fd-3 IPC and the stdout/stderr log pumps."""

    def __init__(
        self,
        session_key: str,
        env_key: str,
        proc: asyncio.subprocess.Process,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        self.session_key = session_key
        self.env_key = env_key
        self.proc = proc
        self.state: str = "idle"
        self.last_used: float = time.monotonic()
        self._reader = reader
        self._writer = writer
        self._current_job_id: str | None = None
        self._io: JobIO | None = None
        self._pumps: list[asyncio.Task] = []

    def start_pumps(self) -> None:
        # stdout/stderr are LOG streams for the session's whole life; each line
        # is tagged with the CURRENT job_id read at emit time (None while idle —
        # serve handlers shouldn't log between jobs).
        self._pumps = [
            asyncio.create_task(self._pump(self.proc.stdout, "stdout")),
            asyncio.create_task(self._pump(self.proc.stderr, "stderr")),
        ]

    async def _pump(self, reader: asyncio.StreamReader, stream_name: str) -> None:
        while True:
            line = await reader.readline()
            if not line:
                break
            io = self._io
            if io is not None:
                io.emit_log(stream_name, line.decode("utf-8", "replace"))

    async def await_ready(self, timeout_s: float) -> None:
        raw = await asyncio.wait_for(self._reader.readline(), timeout=timeout_s)
        if not raw:
            raise RuntimeError("session exited before ready")
        frame = parse_frame(raw)
        if not isinstance(frame, SessionReady):
            raise RuntimeError(f"expected session_ready, got {getattr(frame, 't', '?')}")

    async def run_job(self, job_id: str, inputs: dict, io: JobIO, timeout_s: float) -> JobOutcome:
        payload = json.dumps(inputs).encode("utf-8")
        if len(payload) > INLINE_CAP:
            return JobOutcome(status="failed", error="inputs exceed 256 KB inline cap")

        self._current_job_id = job_id
        self._io = io
        try:
            self._writer.write(SessionJob(job_id=job_id, inputs=inputs).dump() + b"\n")
            await self._writer.drain()
            return await asyncio.wait_for(self._job_loop(job_id, io), timeout=timeout_s)
        except asyncio.TimeoutError:
            self._dead()
            await self.close()
            return JobOutcome(status="failed", error="timeout")
        except (OSError, RuntimeError) as exc:
            # Session died mid-job (fd-3 EOF / process exit): a partial worker
            # failure. Requeue via lease lapse rather than a terminal failure.
            log.warning("session %s crashed on job %s: %s", self.session_key[:12], job_id, exc)
            self._dead()
            await self.close()
            return JobOutcome(status="failed", error="session crashed", requeue=True)
        finally:
            self._current_job_id = None
            self._io = None

    async def _job_loop(self, job_id: str, io: JobIO) -> JobOutcome:
        while True:
            raw = await self._reader.readline()
            if not raw:
                raise RuntimeError("session closed fd 3 mid-job")
            frame = parse_frame(raw)
            if isinstance(frame, SessionPartial):
                io.emit_partial(frame.data)
            elif isinstance(frame, SessionResult):
                if frame.error is not None:
                    return JobOutcome(status="failed", error=frame.error)
                return JobOutcome(status="succeeded", result=frame.result)
            # ignore unexpected frame types; keep reading

    def alive(self) -> bool:
        return self.proc.returncode is None

    def _dead(self) -> None:
        self.state = "dead"

    async def close(self) -> None:
        """Graceful: close fd 3 → child sees EOF and exits; bounded wait then
        kill the process group as a fallback."""
        with contextlib.suppress(OSError):
            self._writer.close()
        if self.proc.returncode is None:
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self._kill_group()
                with contextlib.suppress(Exception):
                    await self.proc.wait()
        for pump in self._pumps:
            pump.cancel()
        await asyncio.gather(*self._pumps, return_exceptions=True)

    def _kill_group(self) -> None:
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


class SessionManager:
    """Owns live sessions for a worker. Persistent across job runs so sessions
    survive between ``run()`` calls (PLAN §2.2)."""

    def __init__(self, provider: VenvProvider, scratch_root: str, shim_dir: str):
        self._provider = provider
        self._scratch_root = Path(scratch_root)
        self._scratch_root.mkdir(parents=True, exist_ok=True)
        self._shim_dir = shim_dir
        self._sessions: dict[str, Session] = {}
        self._lock = asyncio.Lock()
        self._idle_task: asyncio.Task | None = None

    def live_sessions(self) -> list[str]:
        return [s.session_key for s in self._sessions.values() if s.state in ("idle", "busy")]

    async def run_job(self, bundle: JobBundle, io: JobIO) -> JobOutcome:
        session = await self.acquire(bundle.spec)
        try:
            outcome = await session.run_job(
                bundle.job_id, bundle.inputs, io, bundle.timeout_s
            )
        finally:
            async with self._lock:
                if session.state == "dead" or not session.alive():
                    self._sessions.pop(session.session_key, None)
                    self._provider.release(session.env_key)
                elif session.state == "busy":
                    session.state = "idle"
                    session.last_used = time.monotonic()
        return outcome

    async def acquire(self, spec: JobSpec) -> Session:
        self._ensure_idle_task()
        async with self._lock:
            existing = self._sessions.get(spec.session_key)
            if existing is not None and existing.state == "idle" and existing.alive():
                existing.state = "busy"
                existing.last_used = time.monotonic()
                return existing

            victims = self._evict_for_capacity()

        # Fully close evicted sessions BEFORE spawning: the graceful close detaches
        # the reader transport and frees the fd, so the new session can't collide
        # with a lingering transport on a reused fd.
        for victim in victims:
            await victim.close()

        # env build + spawn happen outside the lock (slow); a second acquire for
        # the same key can't collide here because a single-slot worker serves one
        # job at a time (the config constraint documented below).
        await self._provider.ensure(spec)
        self._provider.acquire(spec.env_key)  # held for the session's whole life
        try:
            session = await self._spawn(spec)
        except Exception:
            self._provider.release(spec.env_key)
            raise

        async with self._lock:
            session.state = "busy"
            self._sessions[spec.session_key] = session
        return session

    def _evict_for_capacity(self) -> list[Session]:
        # Caller holds the lock. Evict idle-LRU until under cap; NEVER evict busy.
        # With default cap 1 on a single-slot worker, "cap reached but all busy"
        # cannot occur (one job at a time), so no waiter is needed. Returns the
        # dropped sessions for the caller to close (awaited before the new spawn).
        cap = _max_live_sessions()
        victims: list[Session] = []
        while len(self._sessions) >= cap:
            idle = [s for s in self._sessions.values() if s.state == "idle"]
            if not idle:
                break
            victim = min(idle, key=lambda s: s.last_used)
            self._sessions.pop(victim.session_key, None)
            self._provider.release(victim.env_key)
            victims.append(victim)
            log.info("evicted idle session %s (cap=%d)", victim.session_key[:12], cap)
        return victims

    async def _spawn(self, spec: JobSpec) -> Session:
        handle = self._provider._handle(spec.env_key)
        scratch = Path(tempfile.mkdtemp(dir=self._scratch_root, prefix="session-"))
        run_path = scratch / "run.sh"
        run_path.write_text(spec.run.script)

        env = os.environ.copy()
        env.update(handle.env_vars)
        env["PYTHONPATH"] = f"{self._shim_dir}{os.pathsep}{env.get('PYTHONPATH', '')}"

        parent_sock, child_sock = socket.socketpair()

        def _preexec() -> None:
            # pass_fds alone does not land the fd on 3: dup2 onto 3 and clear
            # CLOEXEC so the child inherits it. dup2 + fcntl are async-signal-safe.
            # fd 3 is also listed in pass_fds so subprocess's post-preexec fd-close
            # pass keeps it open.
            import fcntl

            os.dup2(child_sock.fileno(), 3)
            flags = fcntl.fcntl(3, fcntl.F_GETFD)
            fcntl.fcntl(3, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)

        proc = await asyncio.create_subprocess_exec(
            "bash",
            str(run_path),
            cwd=str(scratch),
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
            pass_fds=(child_sock.fileno(), 3),
            preexec_fn=_preexec,
        )
        child_sock.close()  # parent drops its copy
        parent_sock.setblocking(False)

        reader, writer = await self._stream_for(parent_sock)
        session = Session(spec.session_key, spec.env_key, proc, reader, writer)
        session.start_pumps()
        try:
            await session.await_ready(_session_init_s())
        except Exception as exc:
            log.warning("session %s failed to become ready: %s", spec.session_key[:12], exc)
            await session.close()
            raise
        return session

    async def _stream_for(
        self, sock: socket.socket
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        # A single transport owns the socket for both directions; writes go
        # through the StreamWriter and close() closes the transport (never the
        # raw socket, which the transport owns).
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport, _ = await loop.connect_accepted_socket(lambda: protocol, sock)
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        return reader, writer

    def _ensure_idle_task(self) -> None:
        if self._idle_task is None or self._idle_task.done():
            self._idle_task = asyncio.create_task(self._idle_loop())

    async def shutdown(self) -> None:
        """Cancel the idle loop and gracefully close every live session (frees
        any GPUs). Called on worker stop/drain so no child process or child-watcher
        thread lingers into interpreter shutdown."""
        task = self._idle_task
        self._idle_task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for s in sessions:
            with contextlib.suppress(Exception):
                await s.close()
            self._provider.release(s.env_key)

    async def _idle_loop(self) -> None:
        while True:
            await asyncio.sleep(_IDLE_TICK_S)
            idle_s = _session_idle_s()
            now = time.monotonic()
            async with self._lock:
                expired = [
                    s
                    for s in list(self._sessions.values())
                    if s.state == "idle" and now - s.last_used > idle_s
                ]
                for s in expired:
                    self._sessions.pop(s.session_key, None)
                    self._provider.release(s.env_key)
            for s in expired:
                await s.close()
                log.info("idle-timeout closed session %s", s.session_key[:12])
