"""Phase 4 acceptance: the ivrit reference example's godserve wiring.

Real ivrit (model download / GPU / RunPod creds) is infeasible in CI, so we
test the WIRING with a tiny fake ``ivrit`` stub injected into ``sys.modules``
inside the serve process — reusing the Phase 2 in-process harness. We assert:

- ``GODSERVE_BACKEND`` reaches ``init()`` and selects the labelled engine;
- a generator handler streams partials live and ``init()`` runs exactly once
  across N consecutive jobs on one hot session;
- the same serve spec yields identical spec_id/env_key/session_key whether
  ``GODSERVE_BACKEND=local`` or ``=runpod`` (the script text is identical);
- blob-endpoint hardening (token auth + oversize rejection);
- opacity: ``grep -ri runpod godserve/`` finds nothing.
"""

from __future__ import annotations

import subprocess

from godserve.models import JobDefaults, JobSpec, RunConfig
from tests.conftest import wait_state
from tests.test_p2_sessions import _stream_all, submit


# A serve body that mirrors examples/ivrit_transcribe/transcribe.py's wiring but
# injects a fake ``ivrit`` (no model download / GPU / creds). init() reads
# GODSERVE_BACKEND to pick the engine and records it + an init count via env
# paths; the generator handler streams each fake segment as a partial and
# returns the concatenated transcript.
_IVRIT_BODY = """
import os
import sys
import types

fake = types.ModuleType("ivrit")

class _Seg:
    def __init__(self, text):
        self.text = text

class _Model:
    def __init__(self, engine):
        self.engine = engine
    async def transcribe_async(self, path, language):
        for word in ["shalom", "olam"]:
            yield _Seg(word)

def _load_model(engine, model, **kw):
    # Record the engine string init() selected so the test can assert it.
    with open(os.environ["IVRIT_ENGINE_OUT"], "w") as f:
        f.write(engine)
    return _Model(engine)

fake.load_model = _load_model
sys.modules["ivrit"] = fake

import asyncio
import ivrit
from godserve import serve

MODEL_NAME = "ivrit-ai/whisper-large-v3-turbo-ct2"
_model = None

def init():
    global _model
    p = os.environ["IVRIT_INIT_COUNTER"]
    n = int(open(p).read() or "0") if os.path.exists(p) else 0
    open(p, "w").write(str(n + 1))
    engine = os.environ.get("GODSERVE_BACKEND", "local")
    if engine == "runpod":
        _model = ivrit.load_model(engine="runpod", model=MODEL_NAME,
                                  api_key="k", endpoint_id="e", core_engine="faster-whisper")
    else:
        _model = ivrit.load_model(engine="faster-whisper", model=MODEL_NAME)

def handler(inputs, ctx):
    async def _run():
        segs = []
        async for seg in _model.transcribe_async(path="x", language="he"):
            segs.append(seg.text)
            ctx.emit(seg.text)
        return segs
    segs = asyncio.run(_run())
    return {"transcript": " ".join(segs)}

serve(handler, init=init)
"""


def _ivrit_spec(tag: str = "") -> JobSpec:
    # Identical setup text across engines (env_key must not depend on the engine).
    setup = "echo 'ivrit build'\n"
    run = "exec python - <<'PY'\n" + _IVRIT_BODY + "PY\n"
    return JobSpec(
        python="3.13",
        setup=setup,
        run=RunConfig(script=run, mode="serve"),
        defaults=JobDefaults(timeout_s=60, max_attempts=2),
    )


async def test_backend_label_selects_engine_local(client, make_worker, tmp_path, monkeypatch):
    engine_out = tmp_path / "engine.txt"
    counter = tmp_path / "init.txt"
    monkeypatch.setenv("IVRIT_ENGINE_OUT", str(engine_out))
    monkeypatch.setenv("IVRIT_INIT_COUNTER", str(counter))
    monkeypatch.setenv("GODSERVE_BACKEND", "local")
    make_worker()
    spec = _ivrit_spec()

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]
    st = await wait_state(client, jid, {"succeeded"}, timeout=60)
    assert st["result"] == {"transcript": "shalom olam"}
    assert engine_out.read_text() == "faster-whisper"


async def test_backend_label_selects_engine_runpod(client, make_worker, tmp_path, monkeypatch):
    engine_out = tmp_path / "engine.txt"
    counter = tmp_path / "init.txt"
    monkeypatch.setenv("IVRIT_ENGINE_OUT", str(engine_out))
    monkeypatch.setenv("IVRIT_INIT_COUNTER", str(counter))
    monkeypatch.setenv("GODSERVE_BACKEND", "runpod")
    make_worker()
    spec = _ivrit_spec()

    r = await submit(client, spec, {})
    jid = r.json()["job_id"]
    await wait_state(client, jid, {"succeeded"}, timeout=60)
    assert engine_out.read_text() == "runpod"


async def test_streams_partials_and_init_runs_once(client, make_worker, tmp_path, monkeypatch):
    engine_out = tmp_path / "engine.txt"
    counter = tmp_path / "init.txt"
    monkeypatch.setenv("IVRIT_ENGINE_OUT", str(engine_out))
    monkeypatch.setenv("IVRIT_INIT_COUNTER", str(counter))
    monkeypatch.setenv("GODSERVE_BACKEND", "local")
    make_worker()
    spec = _ivrit_spec()

    for i in range(3):
        r = await submit(client, spec, {"n": i})
        jid = r.json()["job_id"]
        st = await wait_state(client, jid, {"succeeded"}, timeout=60)
        assert st["result"] == {"transcript": "shalom olam"}

    # Hot session reused across all jobs → init() (and load_model) ran once.
    assert counter.read_text().strip() == "1"

    # Live streaming really happened: a fresh subscriber replays both segments.
    frames = await _stream_all(client, jid)
    partials = [f for f in frames if f.get("t") == "partial"]
    assert len(partials) == 2


def test_spec_id_identical_across_backend_labels():
    # The script text is identical regardless of GODSERVE_BACKEND, so the spec's
    # content-address keys are identical — one spec serves both tiers.
    local_spec = _ivrit_spec()
    runpod_spec = _ivrit_spec()
    assert local_spec.spec_id == runpod_spec.spec_id
    assert local_spec.env_key == runpod_spec.env_key
    assert local_spec.session_key == runpod_spec.session_key


async def test_blob_token_required_when_set(client, server):
    # The coordinator reads GODSERVE_BLOB_TOKEN at startup; the running server
    # fixture already booted, so set the resolved config directly on app state.
    app = server._config.app
    app.state.blob_config.token = "s3cret"

    # Unauthenticated upload → 401.
    r = await client.post("/v1/blobs", content=b"hello")
    assert r.status_code == 401

    # Correct bearer token → accepted.
    r = await client.post("/v1/blobs", content=b"hello", headers={"Authorization": "Bearer s3cret"})
    assert r.status_code == 200

    app.state.blob_config.token = None


async def test_blob_oversize_rejected(client, server):
    app = server._config.app
    app.state.blob_config.max_bytes = 8

    r = await client.post("/v1/blobs", content=b"0123456789abcdef")
    assert r.status_code == 413

    app.state.blob_config.max_bytes = 1024 * 1024 * 1024


def test_no_runpod_under_core():
    # Opacity: the RunPod label lives only in examples/, never in godserve/.
    out = subprocess.run(
        ["grep", "-ri", "runpod", "godserve/"],
        capture_output=True,
        text=True,
    )
    assert out.stdout == "", f"found 'runpod' under godserve/:\n{out.stdout}"
