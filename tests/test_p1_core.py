"""Phase 1 acceptance: submit → claim → execute (once) → result.

Integration-style: a real coordinator (uvicorn) + in-process agents on ephemeral
ports; jobs are tiny stdlib scripts so the suite runs in seconds.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time

import pytest
import websockets

from godserve.models import JobDefaults, JobSpec, RunConfig
from tests.conftest import wait_state

pytestmark = pytest.mark.anyio if False else pytest.mark.asyncio


# --- spec builders --------------------------------------------------------


def echo_spec(tag: str = "", build_marker: str | None = None) -> JobSpec:
    marker_line = f"echo built >> {build_marker}\n" if build_marker else ""
    setup = f"echo 'BUILD-MARKER {tag}'\n{marker_line}"
    run = (
        "python - <<'PY'\n"
        "import json, os\n"
        "inp = json.loads(os.environ.get('GODSERVE_INPUTS','{}'))\n"
        "open(os.environ['GODSERVE_RESULT_PATH'],'w').write(json.dumps({'echo': inp}))\n"
        "print('echo done')\n"
        "PY\n"
    )
    return JobSpec(
        python="3.13", setup=setup,
        run=RunConfig(script=run, mode="once"),
        defaults=JobDefaults(timeout_s=60, max_attempts=2),
    )


def log_stream_spec() -> JobSpec:
    run = (
        "for i in 1 2 3; do echo \"line-$i\"; done\n"
        "python - <<'PY'\n"
        "import json, os\n"
        "open(os.environ['GODSERVE_RESULT_PATH'],'w').write(json.dumps({'n': 3}))\n"
        "PY\n"
    )
    return JobSpec(
        python="3.13", setup="echo build\n",
        run=RunConfig(script=run, mode="once"),
        defaults=JobDefaults(timeout_s=60, max_attempts=2),
    )


def sleep_spec(seconds: float, max_attempts: int = 2) -> JobSpec:
    run = (
        f"sleep {seconds}\n"
        "python - <<'PY'\n"
        "import json, os\n"
        "open(os.environ['GODSERVE_RESULT_PATH'],'w').write(json.dumps({'slept': True}))\n"
        "PY\n"
    )
    return JobSpec(
        python="3.13", setup="echo build\n",
        run=RunConfig(script=run, mode="once"),
        defaults=JobDefaults(timeout_s=60, max_attempts=max_attempts),
    )


def big_result_spec(nbytes: int) -> JobSpec:
    run = (
        "python - <<PY\n"
        "import json, os\n"
        f"open(os.environ['GODSERVE_RESULT_PATH'],'w').write(json.dumps({{'blob':'x'*{nbytes}}}))\n"
        "PY\n"
    )
    return JobSpec(
        python="3.13", setup="echo build\n",
        run=RunConfig(script=run, mode="once"),
        defaults=JobDefaults(timeout_s=60),
    )


async def submit(client, spec: JobSpec, inputs: dict, overrides: dict | None = None):
    body = {"spec": spec.model_dump(), "inputs": inputs}
    if overrides:
        body["overrides"] = overrides
    r = await client.post("/v1/jobs", json=body)
    return r


# --- tests ----------------------------------------------------------------


async def test_env_built_once_no_rebuild_on_reuse(client, make_worker, tmp_path):
    make_worker()
    # setup.sh appends a line to this file each time it runs; a warm env skips
    # setup entirely, so the file must have exactly one line after two jobs.
    marker = tmp_path / "build-count.txt"
    spec = echo_spec("reuse", build_marker=str(marker))

    r = await submit(client, spec, {"a": 1})
    job1 = r.json()["job_id"]
    st1 = await wait_state(client, job1, {"succeeded"})
    assert st1["result"] == {"echo": {"a": 1}}
    assert marker.read_text().count("built") == 1

    # Second job, same spec → env warm → setup.sh must NOT run again.
    r = await submit(client, spec, {"a": 2})
    job2 = r.json()["job_id"]
    st2 = await wait_state(client, job2, {"succeeded"})
    assert st2["result"] == {"echo": {"a": 2}}
    assert marker.read_text().count("built") == 1, "env rebuilt on reuse"


async def test_fetched_file_cached_once_across_env_keys(client, make_worker, tmp_path, monkeypatch):
    # Shared machine-wide content cache dir for the worker.
    cache_dir = tmp_path / "shared-cache"
    monkeypatch.setenv("GODSERVE_CACHE_DIR", str(cache_dir))
    make_worker()

    # A tiny artifact served by the coordinator's own blob store.
    payload = b"weights-bytes-v3"
    sha = hashlib.sha256(payload).hexdigest()
    blob = (await client.post("/v1/blobs", content=payload)).json()
    url = str(client.base_url) + blob["url"]

    def spec_with_fetch(tag: str) -> JobSpec:
        setup = (
            f"echo 'variant {tag}'\n"
            f"godserve-fetch {url} model.bin --sha256 {sha}\n"
        )
        run = (
            "python - <<'PY'\n"
            "import json, os\n"
            "open(os.environ['GODSERVE_RESULT_PATH'],'w').write(json.dumps({'ok': True}))\n"
            "PY\n"
        )
        return JobSpec(
            python="3.13", setup=setup,
            run=RunConfig(script=run, mode="once"),
            defaults=JobDefaults(timeout_s=60),
        )

    s1 = spec_with_fetch("one")
    s2 = spec_with_fetch("two")
    assert s1.env_key != s2.env_key

    for s in (s1, s2):
        r = await submit(client, s, {})
        jid = r.json()["job_id"]
        await wait_state(client, jid, {"succeeded"})

    # Content-addressed cache holds exactly one copy of the artifact.
    cached = list((cache_dir / "content").glob(sha))
    assert cached, "artifact not cached"
    content_files = [p for p in (cache_dir / "content").iterdir() if not p.name.startswith(".")]
    assert content_files == [cache_dir / "content" / sha]


async def test_log_stream_live_and_replay_and_result(client, make_worker):
    make_worker()
    spec = log_stream_spec()
    r = await submit(client, spec, {})
    job_id = r.json()["job_id"]

    # Late subscriber: connect after completion → full replay + result frame.
    await wait_state(client, job_id, {"succeeded"})
    frames = await _stream_all(client, job_id)
    outputs = [f["data"] for f in frames if f.get("t") == "output"]
    joined = "".join(outputs)
    for i in (1, 2, 3):
        assert f"line-{i}" in joined
    terminal = frames[-1]
    assert terminal["t"] == "result"
    assert terminal["status"] == "succeeded"
    assert terminal["result"] == {"n": 3}


async def test_kill_worker_mid_job_requeues_and_completes(client, make_worker):
    # Short lease so the sweeper requeues quickly after the hard kill.
    w1 = make_worker()
    spec = sleep_spec(2.0, max_attempts=2)
    r = await submit(client, spec, {}, overrides={"lease_ttl_s": 2})
    job_id = r.json()["job_id"]

    st = await wait_state(client, job_id, {"assigned", "running"})
    # Hard kill: drop the WS without goodbye (SIGKILL analogue).
    w1.kill()

    # Lease expires → sweeper requeues (attempt++).
    st = await wait_state(client, job_id, {"queued", "assigned", "running"}, timeout=15)

    # New worker picks it up and completes.
    make_worker()
    st = await wait_state(client, job_id, {"succeeded"}, timeout=30)
    assert st["attempt"] >= 1, "attempt not incremented on requeue"


async def test_exceeds_max_attempts_fails(client, make_worker):
    # max_attempts=1: a single lease loss → failed (no requeue).
    spec = sleep_spec(30.0, max_attempts=1)
    r = await submit(client, spec, {}, overrides={"lease_ttl_s": 2})
    job_id = r.json()["job_id"]

    w1 = make_worker()
    await wait_state(client, job_id, {"assigned", "running"})
    w1.kill()

    st = await wait_state(client, job_id, {"failed"}, timeout=15)
    assert st["state"] == "failed"


async def test_oversized_inline_input_rejected_then_blob_accepted(client, make_worker):
    make_worker()
    spec = echo_spec("blob")

    big = "x" * (300 * 1024)
    r = await submit(client, spec, {"data": big})
    assert r.status_code == 413

    # Same payload via blob + blob_ref is accepted.
    payload = json.dumps({"data": big}).encode()
    blob = (await client.post("/v1/blobs", content=payload)).json()
    r = await submit(client, spec, {"blob_ref": blob["blob_id"], "sha256": blob["blob_id"]})
    assert r.status_code == 200
    job_id = r.json()["job_id"]
    await wait_state(client, job_id, {"succeeded"})


async def test_oversized_result_returned_as_blob_ref(client, make_worker):
    make_worker()
    spec = big_result_spec(300 * 1024)
    r = await submit(client, spec, {})
    job_id = r.json()["job_id"]
    st = await wait_state(client, job_id, {"succeeded"})
    assert "blob_ref" in st["result"], f"expected blob_ref, got keys {list(st['result'])}"
    blob_id = st["result"]["blob_ref"]
    got = await client.get(f"/v1/blobs/{blob_id}")
    assert got.status_code == 200
    assert json.loads(got.content)["blob"].startswith("x")


async def test_single_job_into_empty_queue_with_idle_worker_completes(
    client, make_worker, server
):
    # Regression: lost-wakeup between _select_job's empty SELECT and conn.idle.
    # A worker connects to an empty queue and parks idle; a single job submitted
    # afterward must be dispatched by poke and complete (never hang queued).
    make_worker()

    # Let the worker connect and settle idle against the empty queue.
    registry = server._config.app.state.registry
    deadline = time.time() + 10
    while not registry.idle_workers():
        if time.time() > deadline:
            raise TimeoutError("worker never went idle")
        await asyncio.sleep(0.02)

    spec = echo_spec("empty-queue")
    r = await submit(client, spec, {"a": 7})
    job_id = r.json()["job_id"]
    st = await wait_state(client, job_id, {"succeeded"}, timeout=15)
    assert st["result"] == {"echo": {"a": 7}}


async def test_cancel_queued_immediate(client, make_worker):
    # No worker → job stays queued; cancel is immediate.
    spec = echo_spec("cancelq")
    r = await submit(client, spec, {})
    job_id = r.json()["job_id"]
    await wait_state(client, job_id, {"queued"})

    r = await client.post(f"/v1/jobs/{job_id}/cancel")
    assert r.json()["state"] == "canceled"
    st = await client.get(f"/v1/jobs/{job_id}")
    assert st.json()["state"] == "canceled"


async def test_cancel_running_kills_process(client, make_worker):
    make_worker()
    spec = sleep_spec(30.0)
    r = await submit(client, spec, {}, overrides={"lease_ttl_s": 30})
    job_id = r.json()["job_id"]

    await wait_state(client, job_id, {"assigned", "running"})
    await client.post(f"/v1/jobs/{job_id}/cancel")
    st = await wait_state(client, job_id, {"canceled"}, timeout=15)
    assert st["state"] == "canceled"


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


async def _collect_logs(client, job_id) -> list[str]:
    frames = await _stream_all(client, job_id)
    return [f.get("data", "") for f in frames if f.get("t") == "output"]
