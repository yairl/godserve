"""FastAPI coordinator app (PLAN §1.4).

Routes: /v1/specs, /v1/jobs, /v1/jobs/{id}, /v1/jobs/{id}/cancel,
/v1/jobs/{id}/stream (WS), /v1/blobs, /v1/prewarm (501, deferred), and the
worker protocol handler at WS /v1/worker.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from starlette.websockets import WebSocketState

from ..db import DB
from ..models import BlobRef, JobDefaults, JobSpec, LevelConfig
from ..protocol import (
    INLINE_CAP,
    Cancel,
    Hello,
    Ready,
    Heartbeat,
    Output,
    Partial,
    Result,
    Goodbye,
    parse_frame,
)
from .blobs import BlobStore, BlobTooLarge
from .dispatcher import Dispatcher
from .load import LoadController, SustainedDepthPolicy
from .pubsub import PubSub
from .queue import Queue
from .registry import Registry, WorkerConn

log = logging.getLogger(__name__)

DEFAULT_LEASE_TTL_S = 30
# Default per-upload cap when GODSERVE_BLOB_MAX_BYTES is unset (1 GiB).
DEFAULT_BLOB_MAX_BYTES = 1024 * 1024 * 1024
# Worker-WS frame ceiling (1 GiB): a terminal result may reach ~100 MB before
# blob-spill, so the ws server must not reject it at uvicorn's 16 MB default.
WS_MAX_SIZE = 1024 * 1024 * 1024


class BlobConfig:
    """Blob-endpoint policy resolved from GODSERVE_ env at startup.

    ``token`` None → open (back-compat). ``disk_quota_bytes`` None → no quota.
    """

    def __init__(self, token: str | None, max_bytes: int, disk_quota_bytes: int | None):
        self.token = token
        self.max_bytes = max_bytes
        self.disk_quota_bytes = disk_quota_bytes


def _load_blob_config() -> BlobConfig:
    token = os.environ.get("GODSERVE_BLOB_TOKEN") or None
    max_bytes = int(os.environ.get("GODSERVE_BLOB_MAX_BYTES", DEFAULT_BLOB_MAX_BYTES))
    quota_raw = os.environ.get("GODSERVE_BLOB_DISK_QUOTA_BYTES")
    disk_quota_bytes = int(quota_raw) if quota_raw else None
    return BlobConfig(token, max_bytes, disk_quota_bytes)


def create_app(
    db_path: str, blob_root: str, levels: list[LevelConfig] | None
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = DB(db_path)
        await db.open()
        requeued = await db.recover_on_startup()
        if requeued:
            log.info("startup recovery requeued %d jobs", len(requeued))
        queue = Queue(db)
        pubsub = PubSub()
        blobs = BlobStore(blob_root)
        registry = Registry(db)
        policy = SustainedDepthPolicy(levels or [])
        load = LoadController(db, policy, levels or [])
        dispatcher = Dispatcher(db, queue, pubsub, registry, blobs, load)

        async def _on_requeue(items: list[tuple[str, int | None]]) -> None:
            for jid, claimed_tier in items:
                row = await db.get_job(jid)
                if row is not None and row["state"] == "failed":
                    dispatcher._publish_terminal(
                        jid, "failed", None, None, row["error"]
                    )
                # A lease-expired job frees the slot on a still-connected worker
                # (it no longer runs there); drop the ghost so poke can re-dispatch.
                conn = registry.find_by_job(jid)
                if conn is not None:
                    conn.running.discard(jid)
                # Return the paid tier's budget so the freed capacity is reusable.
                if claimed_tier and claimed_tier > 0:
                    load.refund(claimed_tier)
            # Requeued jobs can be picked up by idle workers immediately.
            await dispatcher.poke()

        registry._on_requeue = _on_requeue

        app.state.db = db
        app.state.queue = queue
        app.state.pubsub = pubsub
        app.state.blobs = blobs
        app.state.registry = registry
        app.state.dispatcher = dispatcher
        app.state.load = load
        app.state.blob_config = _load_blob_config()
        app.state.blob_root = blob_root

        registry.start_sweeper()
        load.start()
        try:
            yield
        finally:
            await registry.stop_sweeper()
            await load.stop()
            await db.close()

    app = FastAPI(lifespan=lifespan)
    _register_routes(app)
    return app


def _register_routes(app: FastAPI) -> None:
    @app.post("/v1/specs")
    async def post_spec(request: Request):
        body = await request.json()
        try:
            spec = JobSpec.model_validate(body.get("spec", body))
        except Exception:
            raise HTTPException(status_code=400, detail="invalid spec")
        db: DB = app.state.db
        await db.upsert_spec(spec.spec_id, spec.model_dump_json())
        return {"spec_id": spec.spec_id}

    @app.post("/v1/jobs")
    async def post_job(request: Request):
        db: DB = app.state.db
        body = await request.json()

        if "spec_id" in body and body["spec_id"]:
            spec_json = await db.get_spec(body["spec_id"])
            if spec_json is None:
                raise HTTPException(status_code=404, detail="spec_id not found")
            spec = JobSpec.model_validate_json(spec_json)
        elif "spec" in body:
            try:
                spec = JobSpec.model_validate(body["spec"])
            except Exception:
                raise HTTPException(status_code=400, detail="invalid spec")
            await db.upsert_spec(spec.spec_id, spec.model_dump_json())
        else:
            raise HTTPException(status_code=400, detail="spec or spec_id required")

        inputs = body.get("inputs", {})
        # Enforce the 256 KB inline cap at ingestion. A blob_ref passes through.
        if not (isinstance(inputs, dict) and "blob_ref" in inputs):
            raw = json.dumps(inputs).encode("utf-8")
            if len(raw) > INLINE_CAP:
                raise HTTPException(
                    status_code=413,
                    detail="inputs exceed 256 KB inline cap; upload via POST /v1/blobs and pass {\"blob_ref\": ...}",
                )

        overrides = body.get("overrides", {}) or {}
        defaults = spec.defaults
        opts = {
            "timeout_s": int(overrides.get("timeout_s", defaults.timeout_s)),
            "max_attempts": int(overrides.get("max_attempts", defaults.max_attempts)),
            "max_tier": overrides.get("max_tier", defaults.max_tier),
            "lease_ttl_s": int(overrides.get("lease_ttl_s", DEFAULT_LEASE_TTL_S)),
        }
        job_id = uuid.uuid4().hex
        await db.submit_job(
            job_id, spec.spec_id, spec.env_key, spec.session_key, inputs, opts
        )
        await app.state.dispatcher.poke()
        return {"job_id": job_id}

    @app.get("/v1/jobs/{job_id}")
    async def get_job(job_id: str):
        db: DB = app.state.db
        row = await db.get_job(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        out = {
            "job_id": job_id,
            "state": row["state"],
            "attempt": row["attempt"],
            "exit_code": row["exit_code"],
            "error": row["error"],
        }
        if row["result"] is not None:
            out["result"] = json.loads(row["result"])
        return out

    @app.post("/v1/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str):
        db: DB = app.state.db
        registry: Registry = app.state.registry
        dispatcher: Dispatcher = app.state.dispatcher
        row = await db.get_job(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        if row["state"] in ("succeeded", "failed", "canceled"):
            return {"job_id": job_id, "state": row["state"]}

        if await db.cancel_queued(job_id):
            dispatcher._publish_terminal(job_id, "canceled", None, None, "canceled")
            return {"job_id": job_id, "state": "canceled"}

        # Running/assigned: signal the worker to kill the process. The worker's
        # Result frame (status=canceled) will finalize state.
        conn = registry.find_by_job(job_id)
        if conn is not None:
            await conn.send(Cancel(job_id=job_id))
        return {"job_id": job_id, "state": "canceling"}

    @app.websocket("/v1/jobs/{job_id}/stream")
    async def stream_job(ws: WebSocket, job_id: str, from_seq: int = 0):
        await ws.accept()
        db: DB = app.state.db
        pubsub: PubSub = app.state.pubsub

        # Persisted-before-published (dispatcher) makes the DB the source of
        # truth; the pubsub tail is drop-oldest, so we gap-repair from the DB.
        # last_seq is the highest seq already delivered to this client.
        last_seq = from_seq - 1

        async def _send_log_row(lrow) -> None:
            nonlocal last_seq
            await ws.send_json({
                "t": "output" if lrow["stream"] in ("stdout", "stderr") else "partial",
                "seq": lrow["seq"],
                "stream": lrow["stream"],
                "data": lrow["data"],
            })
            last_seq = lrow["seq"]

        async def _backfill_to(upto_exclusive: int) -> None:
            # Send any persisted rows with seq in (last_seq, upto_exclusive).
            if upto_exclusive <= last_seq + 1:
                return
            for lrow in await db.read_logs(job_id, last_seq + 1):
                if lrow["seq"] >= upto_exclusive:
                    break
                await _send_log_row(lrow)

        q = pubsub.subscribe(job_id)
        try:
            row = await db.get_job(job_id)
            if row is None:
                await ws.send_json({"t": "error", "detail": "job not found"})
                return

            # Replay persisted logs from from_seq.
            for lrow in await db.read_logs(job_id, from_seq):
                await _send_log_row(lrow)

            # If already terminal, backfill any gap then send the result frame.
            row = await db.get_job(job_id)
            if row["state"] in ("succeeded", "failed", "canceled"):
                await _backfill_to(1 << 62)  # drain all persisted rows
                await _send_terminal(ws, db, row)
                return

            # Live tail. Frames already delivered (seq <= last_seq) are skipped —
            # this also drops live frames that arrived during replay (dup fix).
            while True:
                frame = await q.get()
                if frame.get("t") == "result":
                    await _backfill_to(1 << 62)  # deliver everything persisted first
                    await ws.send_json(frame)
                    return
                seq = frame.get("seq")
                if seq is None or seq <= last_seq:
                    continue
                if seq > last_seq + 1:
                    await _backfill_to(seq)  # repair the gap from the DB
                if seq <= last_seq:
                    continue  # backfill already delivered this seq
                await ws.send_json(frame)
                last_seq = seq
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            log.error("stream error for job %s: %s", job_id, exc, exc_info=True)
        finally:
            pubsub.unsubscribe(job_id, q)
            if ws.client_state != WebSocketState.DISCONNECTED:
                await ws.close()

    @app.post("/v1/blobs")
    async def post_blob(request: Request):
        db: DB = app.state.db
        blobs: BlobStore = app.state.blobs
        cfg: BlobConfig = app.state.blob_config

        if cfg.token is not None and not _blob_authorized(request, cfg.token):
            raise HTTPException(status_code=401, detail="unauthorized")

        if cfg.disk_quota_bytes is not None:
            used = _blob_dir_bytes(app.state.blob_root)
            if used >= cfg.disk_quota_bytes:
                raise HTTPException(status_code=507, detail="blob storage quota exceeded")

        try:
            blob_id, size = await blobs.store(request.stream(), cfg.max_bytes)
        except BlobTooLarge:
            raise HTTPException(status_code=413, detail="blob exceeds size limit")
        await db.upsert_blob(blob_id, size, str(blobs.path_for(blob_id)))
        return {"blob_id": blob_id, "url": f"/v1/blobs/{blob_id}"}

    @app.get("/v1/blobs/{blob_id}")
    async def get_blob(blob_id: str):
        db: DB = app.state.db
        row = await db.get_blob(blob_id)
        if row is None or not Path(row["path"]).exists():
            raise HTTPException(status_code=404, detail="blob not found")
        return FileResponse(row["path"], media_type="application/octet-stream")

    @app.post("/v1/prewarm")
    async def prewarm():
        # Deferred (§4.5). Protocol slot exists; no logic.
        return JSONResponse(status_code=501, content={"detail": "prewarm not implemented"})

    @app.websocket("/v1/worker")
    async def worker_ws(ws: WebSocket):
        await _worker_handler(app, ws)


def _blob_authorized(request: Request, token: str) -> bool:
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer ") :] == token
    return request.headers.get("x-godserve-blob-token") == token


def _blob_dir_bytes(blob_root: str) -> int:
    total = 0
    for entry in Path(blob_root, "blobs").glob("*"):
        try:
            total += entry.stat().st_size
        except OSError:
            pass
    return total


async def _send_terminal(ws: WebSocket, db: DB, row) -> None:
    result = json.loads(row["result"]) if row["result"] is not None else None
    await ws.send_json({
        "t": "result",
        "status": row["state"],
        "exit_code": row["exit_code"],
        "result": result,
        "error": row["error"],
    })


async def _worker_handler(app: FastAPI, ws: WebSocket) -> None:
    await ws.accept()
    registry: Registry = app.state.registry
    dispatcher: Dispatcher = app.state.dispatcher
    conn: WorkerConn | None = None
    send_lock = asyncio.Lock()

    async def send(frame) -> None:
        async with send_lock:
            await ws.send_bytes(frame.dump())

    try:
        while True:
            raw = await ws.receive_bytes()
            frame = parse_frame(raw)

            if isinstance(frame, Hello):
                conn = WorkerConn(
                    worker_id=frame.worker_id,
                    tier=frame.tier,
                    max_slots=frame.max_slots,
                    send=send,
                    warm_envs=frame.warm_envs,
                    live_sessions=frame.live_sessions,
                )
                await registry.register(conn)
            elif conn is None:
                log.warning("worker frame before hello: %s", type(frame).__name__)
                continue
            elif isinstance(frame, Ready):
                await registry.update(conn.worker_id, frame.warm_envs, frame.live_sessions, "ready")
                await dispatcher.on_ready(conn)
            elif isinstance(frame, Output):
                await dispatcher.on_output(conn, frame)
            elif isinstance(frame, Partial):
                await dispatcher.on_partial(conn, frame)
            elif isinstance(frame, Heartbeat):
                registry.touch(conn.worker_id)
                for jid in frame.running:
                    await dispatcher._renew(conn, jid)
            elif isinstance(frame, Result):
                await dispatcher.on_result(conn, frame)
            elif isinstance(frame, Goodbye):
                if frame.drain:
                    continue
                break
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.error("worker handler error: %s", exc, exc_info=True)
    finally:
        if conn is not None:
            await registry.remove(conn.worker_id)
