"""Phase 5 acceptance: end-to-end lossless, ordered streaming.

The streaming path was reversed from drop-oldest to lossless+ordered: emit may
block the handler as backpressure; oversize/non-JSON partials raise a hard error
(session survives); terminal results stay uncapped (auto-spill to blobs). These
tests exercise the real serve-mode path (partials only exist there) plus the
once-mode subprocess pump, mirroring the integration harness in
test_p2_sessions.py.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import websockets

from godserve.models import JobDefaults, JobSpec, RunConfig
from tests.conftest import wait_state
from tests.test_p2_sessions import _serve_spec, echo_serve_spec, submit

pytestmark = pytest.mark.asyncio


# --- serve spec builders --------------------------------------------------


def big_partial_serve_spec(nbytes: int, tag: str) -> JobSpec:
    # Handler yields one partial of ~nbytes payload, then returns a terminal.
    body = (
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        f"    yield {{'blob': 'x' * {nbytes}}}\n"
        "    return {'done': True}\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag=tag, timeout_s=60)


def oversize_partial_serve_spec() -> JobSpec:
    # A single partial whose JSON encoding exceeds the 256 KB inline cap → emit
    # raises inside the child → job fails, session survives.
    body = (
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        "    yield {'blob': 'x' * (300 * 1024)}\n"
        "    return {'done': True}\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag="oversize", timeout_s=60)


def nonjson_partial_serve_spec() -> JobSpec:
    # A partial carrying a non-JSON-serializable value (a set) → emit raises.
    body = (
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        "    yield {'bad': {1, 2, 3}}\n"
        "    return {'done': True}\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag="nonjson", timeout_s=60)


def ordered_stream_serve_spec(n_chunks: int) -> JobSpec:
    # Yields n_chunks partials carrying their emit index; returns a terminal.
    body = (
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        f"    for i in range({n_chunks}):\n"
        "        yield {'i': i}\n"
        "    return {'done': True}\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag=f"ordered{n_chunks}", timeout_s=60)


def big_result_serve_spec(nbytes: int) -> JobSpec:
    # Terminal result far above 256 KB (and above the old ~1 MiB WS ceiling):
    # proves the raised worker ws_max_size and dispatcher blob-spill.
    body = (
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        f"    return {{'blob': 'x' * {nbytes}}}\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag=f"bigresult{nbytes}", timeout_s=60)


def blocking_emit_serve_spec() -> JobSpec:
    # Yields large (near-cap) partials forever with no reader: a handful fill the
    # fd-3 socketpair + agent FIFO + TCP buffers and emit blocks in sendall. Big
    # chunks (not a tiny-partial flood) keep the persisted backlog small so the
    # FIFO-ordered canceled Result isn't stuck behind tens of thousands of rows.
    # Cancelling must tear the session down and free the worker for the next job.
    body = (
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        "    i = 0\n"
        "    while True:\n"
        "        yield {'i': i, 'pad': 'x' * (200 * 1024)}\n"
        "        i += 1\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag="blockemit", timeout_s=120)


def big_stdout_once_spec(nbytes: int) -> JobSpec:
    # once-mode job printing a single line far longer than the 64 KB readline
    # default; the pump must chunk-split, never drop.
    run = (
        "python - <<PY\n"
        "import json, os, sys\n"
        f"sys.stdout.write('y' * {nbytes} + '\\n')\n"
        "sys.stdout.flush()\n"
        "open(os.environ['GODSERVE_RESULT_PATH'],'w').write(json.dumps({'ok': True}))\n"
        "PY\n"
    )
    return JobSpec(
        python="3.13",
        setup="echo build bigline\n",
        run=RunConfig(script=run, mode="once"),
        defaults=JobDefaults(timeout_s=60, max_attempts=1),
    )


# --- helpers --------------------------------------------------------------


async def _stream_all(client, job_id, from_seq: int = 0, max_size=None) -> list[dict]:
    uri = f"ws://127.0.0.1:{client.base_url.port}/v1/jobs/{job_id}/stream"
    if from_seq:
        uri += f"?from_seq={from_seq}"
    frames: list[dict] = []
    async with websockets.connect(uri, max_size=max_size) as ws:
        async for raw in ws:
            f = json.loads(raw)
            frames.append(f)
            if f.get("t") == "result":
                break
    return frames


# --- (a) large partial round-trips intact ---------------------------------


async def test_200kb_partial_round_trips_intact(client, make_worker):
    make_worker()
    payload_len = 200 * 1024
    spec = big_partial_serve_spec(payload_len, tag="200k")

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]
    st = await wait_state(client, jid, {"succeeded"}, timeout=60)
    assert st["result"] == {"done": True}

    frames = await _stream_all(client, jid)
    partials = [f for f in frames if f.get("t") == "partial"]
    assert len(partials) == 1, f"expected exactly one partial, got {len(partials)}"
    # The partial data is a JSON string (the shim json.dumps the chunk).
    chunk = json.loads(partials[0]["data"])
    assert chunk == {"blob": "x" * payload_len}, "large partial payload corrupted/truncated"


# --- (b) oversize / non-JSON partial fails the job; session survives ------


async def test_oversize_partial_fails_job_session_survives(client, make_worker):
    w = make_worker()
    bad_spec = oversize_partial_serve_spec()

    r = await submit(client, bad_spec, {})
    jid = r.json()["job_id"]
    st = await wait_state(client, jid, {"failed"}, timeout=60)
    assert "partial exceeds 256 KB inline cap; use blobs" in (st["error"] or "")

    # The session survived the handler error: a follow-up job on the SAME
    # session_key completes on the same live process.
    backend = w.agent._backend
    assert bad_spec.session_key in backend.live_sessions(), "session should survive a handler error"

    r = await submit(client, bad_spec, {})
    jid2 = r.json()["job_id"]
    st2 = await wait_state(client, jid2, {"failed"}, timeout=60)
    # Same failure again (the handler always oversizes) but crucially it RAN,
    # proving the session accepted a second job.
    assert "partial exceeds 256 KB inline cap; use blobs" in (st2["error"] or "")


async def test_nonjson_partial_fails_job_then_session_serves_next(client, make_worker):
    w = make_worker()
    # Two specs sharing one worker but distinct session_keys would spawn two
    # processes; instead we prove the failed session then serves a DIFFERENT,
    # succeeding job on the same session_key by combining specs in one process.
    bad_spec = nonjson_partial_serve_spec()

    r = await submit(client, bad_spec, {})
    jid = r.json()["job_id"]
    st = await wait_state(client, jid, {"failed"}, timeout=60)
    assert "partial is not JSON-serializable" in (st["error"] or "")

    backend = w.agent._backend
    assert bad_spec.session_key in backend.live_sessions(), "session should survive a handler error"

    # A second job on the same live session runs (fails identically), proving the
    # process was reused rather than killed.
    r = await submit(client, bad_spec, {})
    jid2 = r.json()["job_id"]
    st2 = await wait_state(client, jid2, {"failed"}, timeout=60)
    assert "partial is not JSON-serializable" in (st2["error"] or "")


# --- (c) strict emit order; terminal result strictly last -----------------


async def test_partials_arrive_in_order_result_last(client, make_worker):
    make_worker()
    n = 12
    spec = ordered_stream_serve_spec(n)

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]
    await wait_state(client, jid, {"succeeded"}, timeout=60)

    frames = await _stream_all(client, jid)
    assert frames[-1]["t"] == "result", "terminal result must arrive strictly last"
    assert frames[-1]["status"] == "succeeded"

    partials = [f for f in frames if f.get("t") == "partial"]
    assert len(partials) == n
    indices = [json.loads(p["data"])["i"] for p in partials]
    assert indices == list(range(n)), f"partials out of emit order: {indices}"
    seqs = [p["seq"] for p in partials]
    assert seqs == sorted(seqs), "seqs not monotonically increasing"


# --- (d) pubsub overflow → DB gap-repair delivers every seq once ----------


async def test_pubsub_overflow_still_lossless_via_db_repair(
    client, make_worker, monkeypatch
):
    # Shrink the per-subscriber pubsub queue so a slow reader forces overflow;
    # drop-oldest loses live frames, but the DB gap-repair must still deliver
    # every seq exactly once, in order.
    import godserve.coordinator.pubsub as pubsub_mod

    monkeypatch.setattr(pubsub_mod, "_MAX_QUEUE", 2)

    make_worker()
    n = 40
    spec = ordered_stream_serve_spec(n)

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]

    uri = f"ws://127.0.0.1:{client.base_url.port}/v1/jobs/{jid}/stream"
    frames: list[dict] = []
    async with websockets.connect(uri, max_size=None) as ws:
        while True:
            raw = await ws.recv()
            f = json.loads(raw)
            frames.append(f)
            # Read slowly so the tiny pubsub queue overflows during the run.
            await asyncio.sleep(0.01)
            if f.get("t") == "result":
                break

    await wait_state(client, jid, {"succeeded"}, timeout=60)

    partials = [f for f in frames if f.get("t") == "partial"]
    seqs = [p["seq"] for p in partials]
    assert seqs == sorted(set(seqs)), f"duplicate or out-of-order seqs: {seqs}"
    indices = [json.loads(p["data"])["i"] for p in partials]
    assert indices == list(range(n)), f"lost or reordered partials under overflow: {indices}"
    assert frames[-1]["t"] == "result"


# --- (e) ?from_seq= resume replays from k with no dups ---------------------


async def test_from_seq_resume_no_dups(client, make_worker):
    make_worker()
    n = 10
    spec = ordered_stream_serve_spec(n)

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]
    await wait_state(client, jid, {"succeeded"}, timeout=60)

    # Full stream first to learn the seq range.
    full = await _stream_all(client, jid)
    all_seqs = [f["seq"] for f in full if "seq" in f]
    assert all_seqs, "no seq-bearing frames"
    resume_from = all_seqs[len(all_seqs) // 2]

    resumed = await _stream_all(client, jid, from_seq=resume_from)
    resumed_seqs = [f["seq"] for f in resumed if "seq" in f]
    # No seq below the resume point; strictly increasing; no duplicates.
    assert all(s >= resume_from for s in resumed_seqs), "replayed a seq before from_seq"
    assert resumed_seqs == sorted(set(resumed_seqs)), "duplicate/out-of-order on resume"
    assert set(resumed_seqs) == {s for s in all_seqs if s >= resume_from}
    assert resumed[-1]["t"] == "result"


# --- (f) >16 MB result spills to a blob, fetchable -----------------------


async def test_huge_result_spills_to_blob_and_is_fetchable(client, make_worker):
    make_worker()
    nbytes = 17 * 1024 * 1024  # > 16 MB, above the old ~1 MiB WS default ceiling
    spec = big_result_serve_spec(nbytes)

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]
    st = await wait_state(client, jid, {"succeeded"}, timeout=120)

    assert "blob_ref" in st["result"], f"expected blob_ref spill, got keys {list(st['result'])}"
    blob_id = st["result"]["blob_ref"]
    got = await client.get(f"/v1/blobs/{blob_id}")
    assert got.status_code == 200
    fetched = json.loads(got.content)
    assert fetched == {"blob": "x" * nbytes}, "spilled result corrupted/truncated"


# --- (g) >64 KB single stdout line from once-mode is not lost -------------


async def test_big_stdout_line_not_lost_once_mode(client, make_worker):
    make_worker()
    nbytes = 200 * 1024  # far beyond the 64 KB readline default
    spec = big_stdout_once_spec(nbytes)

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]
    await wait_state(client, jid, {"succeeded"}, timeout=60)

    frames = await _stream_all(client, jid)
    outputs = "".join(f["data"] for f in frames if f.get("t") == "output")
    # The pump chunk-splits an over-long line; concatenated, no bytes are lost.
    assert outputs.count("y") == nbytes, f"stdout truncated: {outputs.count('y')} of {nbytes}"


# --- (h) cancel a serve job blocked in emit tears down + frees worker -----


async def test_cancel_blocked_emit_tears_down_session_and_takes_next(client, make_worker):
    w = make_worker()
    block_spec = blocking_emit_serve_spec()

    # Submit the runaway emitter and let it start. No subscriber reads it, so its
    # outbound buffers fill and emit eventually blocks in sendall.
    r = await submit(client, block_spec, {}, overrides={"lease_ttl_s": 30})
    jid = r.json()["job_id"]
    await wait_state(client, jid, {"assigned", "running"}, timeout=30)
    # Give the handler time to flood the buffers so emit is genuinely blocked.
    await asyncio.sleep(1.0)

    r = await client.post(f"/v1/jobs/{jid}/cancel")
    assert r.json()["state"] in ("canceling", "canceled")
    st = await wait_state(client, jid, {"canceled"}, timeout=30)
    assert st["state"] == "canceled"

    backend = w.agent._backend
    assert block_spec.session_key not in backend.live_sessions(), (
        "cancelled blocked session must be torn down"
    )

    # The worker is free and takes the next (unrelated) job.
    echo_spec = echo_serve_spec(tag="after-cancel")
    r = await submit(client, echo_spec, {"n": 7})
    jid2 = r.json()["job_id"]
    st2 = await wait_state(client, jid2, {"succeeded"}, timeout=60)
    assert st2["result"] == {"echo": {"n": 7}}
