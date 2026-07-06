"""Dependency-free serve SDK — importable as ``godserve`` (PLAN §2.1, ARCH §4.2).

The agent materializes a shim directory on ``PYTHONPATH`` whose
``godserve/__init__.py`` IS a copy of this file, so serve-mode jobs can
``from godserve import serve`` without godserve installed in their venv. Because
of that copy, this module is **stdlib-only** and MUST NOT import anything from
the godserve package (no intra-package imports).

Wire format (newline-delimited JSON on fd 3) mirrors ``godserve.protocol``
EXACTLY; the dicts are hand-built here since protocol cannot be imported:

    session_ready   {"t": "session_ready"}
    session_job     {"t": "session_job", "job_id", "inputs"}
    session_partial {"t": "session_partial", "job_id", "data"}
    session_result  {"t": "session_result", "job_id", "result"|"error"}
"""

from __future__ import annotations

import json
import logging
import os
import socket
import sys
import threading
import traceback
import types

# Cross-reference: godserve.protocol.INLINE_CAP (§4.6). Hardcoded because the
# shim is stdlib-only and cannot import the package.
INLINE_CAP = 256 * 1024

_SESSION_FD = 3

log = logging.getLogger("godserve.serve")


class Ctx:
    """Per-job handler context (§4.2, minimal v1).

    ``emit(chunk)`` streams a partial with a blocking write; ``chunk`` must be
    JSON-serializable and within the 256 KB inline cap, else emit raises and the
    job fails loudly (the session survives). No async, no coordinator handle, no
    blob API.
    """

    def __init__(self, job_id: str, scratch_dir: str, logger: logging.Logger, emit):
        self.job_id = job_id
        self.scratch_dir = scratch_dir
        self.logger = logger
        self._emit = emit

    def emit(self, chunk) -> None:
        self._emit(self.job_id, chunk)


class _Writer:
    """Single owner of fd 3 writes: a lock guards a blocking ``sendall``.

    Losslessness is the requirement, not throughput: emit blocks the handler on
    a full kernel buffer (backpressure) rather than dropping. Oversize/non-JSON
    partials raise so the job fails loudly; the session survives for the next
    job. ``OSError`` from ``sendall`` propagates (the session is dying)."""

    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._lock = threading.Lock()

    def _send(self, frame: dict) -> None:
        line = json.dumps(frame).encode("utf-8") + b"\n"
        with self._lock:
            self._sock.sendall(line)

    def emit_partial(self, job_id: str, chunk) -> None:
        try:
            data = json.dumps(chunk)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"partial is not JSON-serializable: {exc}") from exc
        if len(data.encode("utf-8")) > INLINE_CAP:
            raise ValueError("partial exceeds 256 KB inline cap; use blobs")
        self._send({"t": "session_partial", "job_id": job_id, "data": data})

    def send_result(self, frame: dict) -> None:
        # Same lock/sendall as partials: FIFO orders the result after all
        # partials for the job. OSError propagates (the session is dying).
        self._send(frame)

    def close(self) -> None:
        pass


def _read_line(rfile) -> bytes | None:
    line = rfile.readline()
    if not line:
        return None
    return line


def serve(handler, *, init=None) -> None:
    """Run a serve-mode session loop over fd 3 (§4.2).

    ``init()`` runs once before the first job; each ``session_job`` invokes
    ``handler(inputs, ctx)``. A generator handler streams each yielded chunk as a
    partial and returns its ``StopIteration.value`` as the result; a plain
    handler's return value is the result. Handler exceptions become a
    ``session_result`` error and the session SURVIVES to the next job.
    """
    sock = socket.socket(fileno=os.dup(_SESSION_FD))
    sock.setblocking(True)
    rfile = sock.makefile("rb")
    writer = _Writer(sock)

    scratch_dir = os.getcwd()

    try:
        if init is not None:
            init()
        writer.send_result({"t": "session_ready"})

        while True:
            line = _read_line(rfile)
            if line is None:
                break  # agent closed fd 3 → graceful shutdown
            try:
                frame = json.loads(line)
            except ValueError as exc:
                print(f"godserve: bad session frame: {exc}", file=sys.stderr)
                continue
            if frame.get("t") != "session_job":
                continue

            job_id = frame["job_id"]
            inputs = frame.get("inputs") or {}
            ctx = Ctx(
                job_id=job_id,
                scratch_dir=scratch_dir,
                logger=logging.getLogger(f"godserve.job.{job_id}"),
                emit=writer.emit_partial,
            )
            _run_one(handler, job_id, inputs, ctx, writer)
    finally:
        writer.close()
        try:
            rfile.close()
        finally:
            sock.close()


def _run_one(handler, job_id: str, inputs: dict, ctx: Ctx, writer: _Writer) -> None:
    try:
        out = handler(inputs, ctx)
        if _is_generator(out):
            result = _drain_generator(out, job_id, writer)
        else:
            result = out
    except Exception:  # handler failure: session survives, report error
        tb = traceback.format_exc()
        print(tb, file=sys.stderr)
        writer.send_result({"t": "session_result", "job_id": job_id, "result": None, "error": tb})
        return

    writer.send_result(
        {"t": "session_result", "job_id": job_id, "result": _as_result(result), "error": None}
    )


def _drain_generator(gen, job_id: str, writer: _Writer):
    while True:
        try:
            chunk = next(gen)
        except StopIteration as stop:
            return stop.value
        writer.emit_partial(job_id, chunk)


def _is_generator(obj) -> bool:
    return isinstance(obj, types.GeneratorType)


def _as_result(result) -> dict | None:
    if result is None:
        return None
    if isinstance(result, dict):
        return result
    return {"result": result}
