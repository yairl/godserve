"""Dispatch: budget gating + affinity + atomic claim + lease + result handling
(PLAN §1.4, §3.2).

On a worker ``ready`` we pick hot > warm > cold via ``oldest_queued`` with the
~30s skip window. Tier 0 is never gated. For tier k>0 we consume one unit of the
load budget as an admission check *around* the atomic claim; any consumed unit
that does not become a durable claim (lost race, spec-missing failure) is
refunded exactly once. On ``Output``/``Partial`` we append a log, publish to
subscribers, and renew the lease. On ``Result`` we finish the job, spilling an
oversized result to a blob (§4.6).
"""

from __future__ import annotations

import json
import logging

from ..db import DB
from ..models import BlobRef, JobBundle, JobSpec
from ..protocol import (
    INLINE_CAP,
    Assign,
    NoWork,
    Output,
    Partial,
    Result,
)
from .blobs import BlobStore
from .load import LoadController
from .pubsub import PubSub
from .queue import Queue
from .registry import Registry, WorkerConn

log = logging.getLogger(__name__)

SKIP_WINDOW_S = 30.0


class Dispatcher:
    def __init__(
        self,
        db: DB,
        queue: Queue,
        pubsub: PubSub,
        registry: Registry,
        blobs: BlobStore,
        load: LoadController,
    ):
        self._db = db
        self._queue = queue
        self._pubsub = pubsub
        self._registry = registry
        self._blobs = blobs
        self._load = load

    async def on_ready(self, conn: WorkerConn) -> None:
        """Pick a job for this ready worker and assign it, else mark idle.

        An idle worker holds a free slot; ``poke`` re-runs dispatch for it when a
        new job arrives so we never wait for the worker to re-``ready`` on its
        own.

        The ``idle`` flag is set *before* the ``_select_job`` DB read and cleared
        only on a successful assign. A concurrent ``poke`` racing the read window
        (SELECT saw an empty queue, but the job is committed just after) thus
        always sees ``idle=True`` and re-runs dispatch — closing the lost-wakeup
        window where a single job into an empty queue could hang forever.
        """
        conn.idle = True
        row = await self._select_job(conn)
        if row is None:
            await conn.send(NoWork())
            return

        # Tier 0 is never gated (§2.2, the rescue path). Tier k>0 is admitted
        # only while its load budget has a free unit; consume it *around* the
        # atomic claim, refunding any unit that never becomes a durable claim.
        consumed = conn.tier > 0
        if consumed and not self._load.try_consume(conn.tier):
            await conn.send(NoWork())
            return

        job_id = row["id"]
        claimed = await self._db.claim_job(
            job_id, conn.worker_id, conn.tier, row["lease_ttl_s"]
        )
        if not claimed:
            # Lost the race (another tier/worker took it). Slot stays idle; the
            # next poke/ready retries.
            if consumed:
                self._load.refund(conn.tier)
            await conn.send(NoWork())
            return

        spec = await self._load_spec(row["spec_id"])
        if spec is None:
            log.error("job %s references missing spec %s", job_id, row["spec_id"])
            await self._db.finish_job(job_id, "failed", None, "spec not found")
            self._publish_terminal(job_id, "failed", None, None, "spec not found")
            if consumed:
                self._load.refund(conn.tier)
            await conn.send(NoWork())
            return

        conn.idle = False
        inputs = self._decode_inputs(row["inputs"])
        bundle = JobBundle(
            job_id=job_id,
            spec=spec,
            inputs=inputs,
            timeout_s=row["timeout_s"],
            lease_ttl_s=row["lease_ttl_s"],
        )
        conn.running.add(job_id)
        await conn.send(Assign(bundle=bundle))

    async def poke(self) -> None:
        """Re-run dispatch for idle workers (new job arrived / lease returned)."""
        for conn in self._registry.idle_workers():
            if conn.idle and len(conn.running) < conn.max_slots:
                await self.on_ready(conn)

    async def _select_job(self, conn: WorkerConn):
        # hot: a live session key match
        for skey in conn.live_sessions:
            row = await self._queue.oldest_queued(
                {"session_key": skey}, conn.tier, SKIP_WINDOW_S
            )
            if row is not None:
                return row
        # warm: an env key match
        for ekey in conn.warm_envs:
            row = await self._queue.oldest_queued(
                {"env_key": ekey}, conn.tier, SKIP_WINDOW_S
            )
            if row is not None:
                return row
        # cold: oldest overall
        return await self._queue.oldest_queued(None, conn.tier, SKIP_WINDOW_S)

    async def on_output(self, conn: WorkerConn, frame: Output) -> None:
        seq = await self._db.append_log(frame.job_id, frame.stream, frame.data)
        self._pubsub.publish(
            frame.job_id,
            {"t": "output", "seq": seq, "stream": frame.stream, "data": frame.data},
        )
        await self._renew(conn, frame.job_id)

    async def on_partial(self, conn: WorkerConn, frame: Partial) -> None:
        seq = await self._db.append_log(frame.job_id, "partial", frame.data)
        self._pubsub.publish(
            frame.job_id,
            {"t": "partial", "seq": seq, "stream": "partial", "data": frame.data},
        )
        await self._renew(conn, frame.job_id)

    async def on_result(self, conn: WorkerConn, frame: Result) -> None:
        conn.running.discard(frame.job_id)
        result_payload = None
        error = frame.error
        if frame.status == "succeeded":
            result_payload = await self._maybe_blob_result(frame.result or {})
            await self._db.finish_job(frame.job_id, "succeeded", frame.exit_code, result_payload)
        else:
            await self._db.finish_job(frame.job_id, frame.status, frame.exit_code, error or "")
        self._publish_terminal(frame.job_id, frame.status, frame.exit_code, result_payload, error)

    async def _renew(self, conn: WorkerConn, job_id: str) -> None:
        row = await self._db.get_job(job_id)
        if row is not None and row["state"] in ("assigned", "running"):
            await self._db.renew_lease(job_id, row["lease_ttl_s"])

    async def _maybe_blob_result(self, result: dict) -> dict:
        raw = json.dumps(result).encode("utf-8")
        if len(raw) <= INLINE_CAP:
            return result

        async def _one():
            yield raw

        blob_id, size = await self._blobs.store(_one(), None)
        await self._db.upsert_blob(blob_id, size, str(self._blobs.path_for(blob_id)))
        return BlobRef(blob_ref=blob_id, sha256=blob_id, size=size).model_dump()

    async def _load_spec(self, spec_id: str) -> JobSpec | None:
        spec_json = await self._db.get_spec(spec_id)
        if spec_json is None:
            return None
        return JobSpec.model_validate_json(spec_json)

    @staticmethod
    def _decode_inputs(raw: str) -> dict | BlobRef:
        data = json.loads(raw)
        if isinstance(data, dict) and "blob_ref" in data:
            return BlobRef.model_validate(data)
        return data

    def _publish_terminal(self, job_id, status, exit_code, result, error) -> None:
        self._pubsub.publish(
            job_id,
            {
                "t": "result",
                "status": status,
                "exit_code": exit_code,
                "result": result,
                "error": error,
            },
        )
