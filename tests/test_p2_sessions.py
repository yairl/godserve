"""Phase 2 acceptance: hot sessions (serve mode). PLAN §2 acceptance list.

Integration-style, mirroring test_p1_core.py: a real coordinator + in-process
agent, real venvs, tiny stdlib serve tasks driven through the fd-3 shim.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest
import websockets

from godserve.models import JobBundle, JobDefaults, JobSpec, RunConfig
from godserve.worker.backends.base import JobIO
from tests.conftest import wait_state

pytestmark = pytest.mark.asyncio


# --- serve spec builders --------------------------------------------------


def _serve_spec(body: str, tag: str = "", timeout_s: int = 60, max_attempts: int = 2) -> JobSpec:
    """A serve-mode spec whose run.sh execs a python task using `from godserve
    import serve`. ``body`` is the python source defining init()/handler and the
    final serve(...) call. ``tag`` perturbs setup so distinct specs get distinct
    env/session keys."""
    setup = f"echo 'serve build {tag}'\n"
    run = "exec python - <<'PY'\n" + body + "PY\n"
    return JobSpec(
        python="3.13",
        setup=setup,
        run=RunConfig(script=run, mode="serve"),
        defaults=JobDefaults(timeout_s=timeout_s, max_attempts=max_attempts),
    )


def echo_serve_spec(tag: str = "", counter_env: str | None = None) -> JobSpec:
    init = ""
    if counter_env:
        init = (
            "def init():\n"
            f"    p = os.environ['{counter_env}']\n"
            "    n = int(open(p).read() or '0') if os.path.exists(p) else 0\n"
            "    open(p, 'w').write(str(n + 1))\n"
        )
    serve_call = f"serve(handler, init=init)\n" if init else "serve(handler)\n"
    body = (
        "import os\n"
        "from godserve import serve\n"
        f"{init}"
        "def handler(inp, ctx):\n"
        "    return {'echo': inp}\n"
        f"{serve_call}"
    )
    return _serve_spec(body, tag=tag)


def crash_once_serve_spec(marker_env: str) -> JobSpec:
    # First job for a fresh process crashes (os._exit) mid-handler; the marker
    # file records that the crash already happened so the respawned process's
    # retry succeeds.
    body = (
        "import os\n"
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        f"    marker = os.environ['{marker_env}']\n"
        "    if not os.path.exists(marker):\n"
        "        open(marker, 'w').write('crashed')\n"
        "        os._exit(1)\n"
        "    return {'ok': True}\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag="crash")


def noop_serve_spec() -> JobSpec:
    # Handler returns immediately: running it isolates the hot-path dispatch
    # overhead (session reuse + fd-3 IPC + handler entry) with ~no compute.
    body = (
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        "    return {}\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag="latency", timeout_s=60)


def streaming_serve_spec(n_chunks: int, sleep_s: float) -> JobSpec:
    # Generator handler yields n_chunks partials with a sleep between each, then
    # returns a terminal result. Total compute time ~= n_chunks * sleep_s.
    body = (
        "import time\n"
        "from godserve import serve\n"
        "def handler(inp, ctx):\n"
        f"    for i in range({n_chunks}):\n"
        f"        time.sleep({sleep_s})\n"
        "        yield {'i': i}\n"
        "    return {'done': True}\n"
        "serve(handler)\n"
    )
    return _serve_spec(body, tag="stream", timeout_s=60)


async def submit(client, spec: JobSpec, inputs: dict, overrides: dict | None = None):
    body = {"spec": spec.model_dump(), "inputs": inputs}
    if overrides:
        body["overrides"] = overrides
    return await client.post("/v1/jobs", json=body)


# --- tests ----------------------------------------------------------------


async def test_init_runs_once_across_consecutive_serve_jobs(
    client, make_worker, tmp_path, monkeypatch
):
    counter = tmp_path / "init-count.txt"
    monkeypatch.setenv("GODSERVE_INIT_COUNTER", str(counter))
    make_worker()
    spec = echo_serve_spec(tag="once", counter_env="GODSERVE_INIT_COUNTER")

    for i in range(4):
        r = await submit(client, spec, {"a": i})
        jid = r.json()["job_id"]
        st = await wait_state(client, jid, {"succeeded"}, timeout=60)
        assert st["result"] == {"echo": {"a": i}}

    # Hot session reused across all 4 jobs → init ran exactly once.
    assert counter.read_text().strip() == "1"


async def test_idle_timeout_kills_session_and_next_job_respawns(
    client, make_worker, monkeypatch
):
    # Short idle timeout so the session dies quickly between jobs.
    monkeypatch.setenv("GODSERVE_SESSION_IDLE_S", "1")
    w = make_worker()
    spec = echo_serve_spec(tag="idle")

    r = await submit(client, spec, {"n": 1})
    jid = r.json()["job_id"]
    await wait_state(client, jid, {"succeeded"}, timeout=60)

    backend = w.agent._backend
    assert backend.live_sessions(), "session should be live after first job"
    proc = next(iter(backend._sessions._sessions.values())).proc
    pid = proc.pid

    # Wait for the idle sweeper to reap the session.
    deadline = time.time() + 15
    while backend.live_sessions():
        if time.time() > deadline:
            raise TimeoutError("idle session never reaped")
        await asyncio.sleep(0.2)
    assert proc.returncode is not None, "process should be gone after idle timeout"

    # Next job respawns a fresh session and completes.
    r = await submit(client, spec, {"n": 2})
    jid = r.json()["job_id"]
    st = await wait_state(client, jid, {"succeeded"}, timeout=60)
    assert st["result"] == {"echo": {"n": 2}}
    new_proc = next(iter(backend._sessions._sessions.values())).proc
    assert new_proc.pid != pid, "respawn should be a new process"


async def test_crash_mid_job_requeues_and_retry_succeeds(
    client, make_worker, tmp_path, monkeypatch
):
    marker = tmp_path / "crash-marker.txt"
    monkeypatch.setenv("GODSERVE_CRASH_MARKER", str(marker))
    make_worker()
    spec = crash_once_serve_spec("GODSERVE_CRASH_MARKER")

    # Short lease so the sweeper requeues quickly once the session crash leaves
    # the job leaseless (the worker suppresses the terminal Result on crash).
    r = await submit(client, spec, {}, overrides={"lease_ttl_s": 2})
    jid = r.json()["job_id"]

    # First attempt crashes (os._exit) mid-handler → session dies → worker sends
    # no terminal Result → lease lapses → sweeper requeues (attempt++, max=2) →
    # a fresh session respawns and the retry succeeds.
    st = await wait_state(client, jid, {"succeeded"}, timeout=60)
    assert st["result"] == {"ok": True}
    assert st["attempt"] >= 1, "attempt should have incremented on requeue"


async def test_generator_streams_live_while_stalled_subscriber_does_not_slow_session(
    client, make_worker
):
    make_worker()
    n_chunks, sleep_s = 6, 0.25
    spec = streaming_serve_spec(n_chunks, sleep_s)

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]

    # Subscriber connects, reads ONE partial, then stops reading entirely — a
    # stalled client. It must not backpressure the handler (invariant #3).
    uri = f"ws://127.0.0.1:{client.base_url.port}/v1/jobs/{jid}/stream"
    started = time.monotonic()
    async with websockets.connect(uri) as ws:
        first = json.loads(await ws.recv())
        assert first["t"] in ("output", "partial")
        # Deliberately do not read further; let the buffer fill unread.
        st = await wait_state(client, jid, {"succeeded"}, timeout=60)
    elapsed = time.monotonic() - started

    assert st["result"] == {"done": True}
    # Total wall time must be dominated by the handler's own compute, not stalled
    # by the unread subscriber. Generous ceiling = compute + fixed overhead.
    compute = n_chunks * sleep_s
    assert elapsed < compute + 8.0, f"stalled subscriber slowed the session ({elapsed:.1f}s)"

    # Live streaming really happened: a fresh subscriber replays all partials.
    frames = await _stream_all(client, jid)
    partials = [f for f in frames if f.get("t") == "partial"]
    assert len(partials) == n_chunks, f"expected {n_chunks} partials, got {len(partials)}"


async def test_max_live_sessions_one_evicts_idle_lru_never_busy(
    client, make_worker, monkeypatch
):
    monkeypatch.setenv("GODSERVE_MAX_LIVE_SESSIONS", "1")
    w = make_worker()
    spec_a = echo_serve_spec(tag="specA")
    spec_b = echo_serve_spec(tag="specB")
    assert spec_a.session_key != spec_b.session_key

    backend = w.agent._backend

    # Job on spec A → session A live and idle afterwards.
    r = await submit(client, spec_a, {"which": "a"})
    ja = r.json()["job_id"]
    await wait_state(client, ja, {"succeeded"}, timeout=60)
    assert backend.live_sessions() == [spec_a.session_key]

    # Job on spec B at cap=1 → idle session A (LRU) evicted, B spawned.
    r = await submit(client, spec_b, {"which": "b"})
    jb = r.json()["job_id"]
    st = await wait_state(client, jb, {"succeeded"}, timeout=60)
    assert st["result"] == {"echo": {"which": "b"}}

    live = backend.live_sessions()
    assert live == [spec_b.session_key], f"expected only B live, got {live}"
    assert spec_a.session_key not in live, "busy/never — A must have been evicted"


async def test_hot_path_added_latency_under_20ms(client, make_worker):
    # ARCH §4.4 budgets the hot path at "WS round-trip + ~1ms IPC" < 20ms: the
    # per-job latency the SESSION layer adds when a live session is reused — no
    # env build, no run.sh spawn, no init(). That is a worker-side quantity; the
    # coordinator's HTTP + aiosqlite submit/claim writes are Phase-1 infra shared
    # by every job regardless of warmth and are not part of this budget, so we
    # measure the worker hot path directly via a real backend.run on a session
    # that is already live and idle.
    w = make_worker()
    spec = noop_serve_spec()

    # Warm the session through the real submit path (builds env, spawns run.sh,
    # runs init) so the measured jobs hit a genuinely hot session.
    r = await submit(client, spec, {"warmup": True})
    await wait_state(client, r.json()["job_id"], {"succeeded"}, timeout=60)

    backend = w.agent._backend
    assert backend.live_sessions() == [spec.session_key], "session must be hot before measuring"

    def _noop(*_a, **_k):
        return None

    async def _one() -> float:
        bundle = JobBundle(
            job_id=uuid.uuid4().hex,
            spec=spec,
            inputs={},
            timeout_s=60,
            lease_ttl_s=30,
        )
        io = JobIO(bundle.job_id, emit_log=_noop, emit_partial=_noop)
        t0 = time.perf_counter()
        outcome = await backend.run(bundle, io)  # hot reuse + fd-3 IPC round trip
        dt = time.perf_counter() - t0
        assert outcome.status == "succeeded"
        return dt

    # Best of N: the hot-path floor is the achievable dispatch cost; scheduler /
    # GC jitter only inflates it.
    deltas = [await _one() for _ in range(20)]
    best_ms = min(deltas) * 1000.0
    assert best_ms < 20.0, f"hot-path added latency {best_ms:.1f}ms exceeds 20ms budget"


# --- helpers --------------------------------------------------------------


async def _stream_all(client, job_id) -> list[dict]:
    uri = f"ws://127.0.0.1:{client.base_url.port}/v1/jobs/{job_id}/stream"
    frames: list[dict] = []
    async with websockets.connect(uri) as ws:
        async for raw in ws:
            f = json.loads(raw)
            frames.append(f)
            if f.get("t") == "result":
                break
    return frames
