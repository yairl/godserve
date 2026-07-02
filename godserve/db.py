"""SQLite state (schema per ARCH §5, primitives per PLAN §1.3).

One door in and out of ``queued``: ``claim_job`` is the single atomic UPDATE
guarded by ``state='queued'`` with a rowcount check. Every other transition off
``queued`` is a bug.
"""

from __future__ import annotations

import json
import time
from typing import Any

import aiosqlite

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    state         TEXT NOT NULL,
    spec_id       TEXT NOT NULL,
    env_key       TEXT NOT NULL,
    session_key   TEXT NOT NULL,
    inputs        TEXT,
    result        TEXT,
    exit_code     INTEGER,
    error         TEXT,
    attempt       INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 2,
    max_tier      INTEGER,
    timeout_s     INTEGER NOT NULL,
    lease_ttl_s   INTEGER NOT NULL,
    submitted_at  REAL NOT NULL,
    assigned_to   TEXT,
    claimed_tier  INTEGER,
    lease_expires REAL,
    created       REAL NOT NULL,
    updated       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS specs (
    spec_id TEXT PRIMARY KEY,
    spec    TEXT NOT NULL,
    created REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs (
    blob_id TEXT PRIMARY KEY,
    size    INTEGER NOT NULL,
    path    TEXT NOT NULL,
    created REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    id            TEXT PRIMARY KEY,
    tier          INTEGER NOT NULL,
    state         TEXT NOT NULL,
    max_slots     INTEGER NOT NULL,
    warm_envs     TEXT,
    live_sessions TEXT,
    last_seen     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS job_logs (
    job_id TEXT NOT NULL,
    seq    INTEGER NOT NULL,
    stream TEXT NOT NULL,
    data   TEXT NOT NULL,
    ts     REAL NOT NULL,
    PRIMARY KEY (job_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_jobs_queued ON jobs (state, submitted_at);
"""

_TERMINAL = ("succeeded", "failed", "canceled")


class DB:
    def __init__(self, path: str):
        self._path = path
        self._conn: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def _c(self) -> aiosqlite.Connection:
        assert self._conn is not None, "DB not opened"
        return self._conn

    # --- specs ------------------------------------------------------------

    async def upsert_spec(self, spec_id: str, spec_json: str) -> None:
        await self._c.execute(
            "INSERT OR IGNORE INTO specs (spec_id, spec, created) VALUES (?, ?, ?)",
            (spec_id, spec_json, time.time()),
        )
        await self._c.commit()

    async def get_spec(self, spec_id: str) -> str | None:
        cur = await self._c.execute(
            "SELECT spec FROM specs WHERE spec_id=?", (spec_id,)
        )
        row = await cur.fetchone()
        return row["spec"] if row else None

    # --- blobs ------------------------------------------------------------

    async def upsert_blob(self, blob_id: str, size: int, path: str) -> None:
        await self._c.execute(
            "INSERT OR IGNORE INTO blobs (blob_id, size, path, created) VALUES (?, ?, ?, ?)",
            (blob_id, size, path, time.time()),
        )
        await self._c.commit()

    async def get_blob(self, blob_id: str) -> aiosqlite.Row | None:
        cur = await self._c.execute(
            "SELECT * FROM blobs WHERE blob_id=?", (blob_id,)
        )
        return await cur.fetchone()

    # --- jobs -------------------------------------------------------------

    async def submit_job(
        self,
        job_id: str,
        spec_id: str,
        env_key: str,
        session_key: str,
        inputs: Any,
        opts: dict[str, Any],
    ) -> str:
        now = time.time()
        await self._c.execute(
            """INSERT INTO jobs
               (id, state, spec_id, env_key, session_key, inputs, attempt,
                max_attempts, max_tier, timeout_s, lease_ttl_s, submitted_at,
                created, updated)
               VALUES (?, 'queued', ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                spec_id,
                env_key,
                session_key,
                json.dumps(inputs),
                opts["max_attempts"],
                opts.get("max_tier"),
                opts["timeout_s"],
                opts["lease_ttl_s"],
                now,
                now,
                now,
            ),
        )
        await self._c.commit()
        return job_id

    async def get_job(self, job_id: str) -> aiosqlite.Row | None:
        cur = await self._c.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        return await cur.fetchone()

    async def claim_job(
        self, job_id: str, worker_id: str, tier: int, lease_ttl_s: int
    ) -> bool:
        """The single atomic queued→assigned transition. Returns True iff won."""
        now = time.time()
        cur = await self._c.execute(
            """UPDATE jobs
               SET state='assigned', assigned_to=?, claimed_tier=?,
                   lease_expires=?, updated=?
               WHERE id=? AND state='queued'""",
            (worker_id, tier, now + lease_ttl_s, now, job_id),
        )
        await self._c.commit()
        return cur.rowcount == 1

    async def renew_lease(self, job_id: str, lease_ttl_s: int) -> None:
        now = time.time()
        await self._c.execute(
            """UPDATE jobs SET lease_expires=?, updated=?,
                   state=CASE WHEN state='assigned' THEN 'running' ELSE state END
               WHERE id=? AND state IN ('assigned', 'running')""",
            (now + lease_ttl_s, now, job_id),
        )
        await self._c.commit()

    async def finish_job(
        self,
        job_id: str,
        status: str,
        exit_code: int | None,
        result_or_error: Any,
    ) -> None:
        now = time.time()
        result = None
        error = None
        if status == "succeeded":
            result = json.dumps(result_or_error)
        else:
            error = result_or_error if isinstance(result_or_error, str) else json.dumps(result_or_error)
        await self._c.execute(
            """UPDATE jobs
               SET state=?, exit_code=?, result=?, error=?, updated=?,
                   lease_expires=NULL
               WHERE id=?""",
            (status, exit_code, result, error, now, job_id),
        )
        await self._c.commit()

    async def cancel_queued(self, job_id: str) -> bool:
        """Cancel a still-queued job atomically. True iff it was queued."""
        now = time.time()
        cur = await self._c.execute(
            "UPDATE jobs SET state='canceled', updated=? WHERE id=? AND state='queued'",
            (now, job_id),
        )
        await self._c.commit()
        return cur.rowcount == 1

    async def requeue_expired(self, now: float) -> list[str]:
        """Expire stale leases. Under max_attempts → requeue (attempt++);
        else → failed. Returns the affected job_ids."""
        cur = await self._c.execute(
            """SELECT id, attempt, max_attempts FROM jobs
               WHERE state IN ('assigned', 'running') AND lease_expires < ?""",
            (now,),
        )
        rows = await cur.fetchall()
        affected: list[str] = []
        for row in rows:
            job_id = row["id"]
            if row["attempt"] + 1 < row["max_attempts"]:
                await self._c.execute(
                    """UPDATE jobs SET state='queued', attempt=attempt+1,
                           assigned_to=NULL, claimed_tier=NULL,
                           lease_expires=NULL, updated=?
                       WHERE id=?""",
                    (now, job_id),
                )
            else:
                await self._c.execute(
                    """UPDATE jobs SET state='failed', attempt=attempt+1,
                           error='lease expired: max attempts exceeded',
                           lease_expires=NULL, updated=?
                       WHERE id=?""",
                    (now, job_id),
                )
            affected.append(job_id)
        if affected:
            await self._c.commit()
        return affected

    async def queued_depth(self) -> int:
        cur = await self._c.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE state='queued'"
        )
        row = await cur.fetchone()
        return row["n"]

    async def oldest_queued(
        self,
        match: dict[str, str] | None,
        max_tier_ge: int,
        skip_window_s: float,
    ) -> aiosqlite.Row | None:
        """Oldest queued job. If ``match`` (session_key/env_key) is given, prefer
        a matching job but only skip the true head within ``skip_window_s`` (the
        starvation guard, §4.3). ``max_tier_ge`` filters jobs this tier may run.
        """
        tier_clause = "(max_tier IS NULL OR max_tier >= ?)"
        if match:
            key, val = next(iter(match.items()))
            cur = await self._c.execute(
                f"""SELECT * FROM jobs
                    WHERE state='queued' AND {key}=? AND {tier_clause}
                    ORDER BY submitted_at ASC LIMIT 1""",
                (val, max_tier_ge),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            # Only allow skipping the head job if it is younger than the window.
            head = await self._head_queued(max_tier_ge)
            if head is not None and head["id"] != row["id"]:
                if time.time() - head["submitted_at"] > skip_window_s:
                    return head
            return row
        return await self._head_queued(max_tier_ge)

    async def _head_queued(self, max_tier_ge: int) -> aiosqlite.Row | None:
        cur = await self._c.execute(
            """SELECT * FROM jobs
               WHERE state='queued' AND (max_tier IS NULL OR max_tier >= ?)
               ORDER BY submitted_at ASC LIMIT 1""",
            (max_tier_ge,),
        )
        return await cur.fetchone()

    # --- logs -------------------------------------------------------------

    async def append_log(self, job_id: str, stream: str, data: str) -> int:
        cur = await self._c.execute(
            "SELECT COALESCE(MAX(seq), -1) + 1 AS next FROM job_logs WHERE job_id=?",
            (job_id,),
        )
        row = await cur.fetchone()
        seq = row["next"]
        await self._c.execute(
            "INSERT INTO job_logs (job_id, seq, stream, data, ts) VALUES (?, ?, ?, ?, ?)",
            (job_id, seq, stream, data, time.time()),
        )
        await self._c.commit()
        return seq

    async def read_logs(self, job_id: str, from_seq: int) -> list[aiosqlite.Row]:
        cur = await self._c.execute(
            """SELECT seq, stream, data, ts FROM job_logs
               WHERE job_id=? AND seq >= ? ORDER BY seq ASC""",
            (job_id, from_seq),
        )
        return await cur.fetchall()

    # --- workers ----------------------------------------------------------

    async def upsert_worker(
        self,
        worker_id: str,
        tier: int,
        state: str,
        max_slots: int,
        warm_envs: list[str],
        live_sessions: list[str],
    ) -> None:
        await self._c.execute(
            """INSERT INTO workers
               (id, tier, state, max_slots, warm_envs, live_sessions, last_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 tier=excluded.tier, state=excluded.state,
                 max_slots=excluded.max_slots, warm_envs=excluded.warm_envs,
                 live_sessions=excluded.live_sessions, last_seen=excluded.last_seen""",
            (
                worker_id,
                tier,
                state,
                max_slots,
                json.dumps(warm_envs),
                json.dumps(live_sessions),
                time.time(),
            ),
        )
        await self._c.commit()

    async def mark_worker_dead(self, worker_id: str) -> None:
        await self._c.execute(
            "UPDATE workers SET state='dead', last_seen=? WHERE id=?",
            (time.time(), worker_id),
        )
        await self._c.commit()

    # --- startup recovery -------------------------------------------------

    async def recover_on_startup(self) -> list[str]:
        """Expire all stale leases → requeue (§5 restart note). Any job left
        assigned/running from a previous process has no live lease holder, so we
        force-expire immediately."""
        now = time.time()
        await self._c.execute(
            """UPDATE jobs SET lease_expires=? WHERE state IN ('assigned', 'running')""",
            (now - 1,),
        )
        await self._c.commit()
        return await self.requeue_expired(now)
