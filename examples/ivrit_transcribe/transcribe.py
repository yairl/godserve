"""serve-mode ivrit transcription task.

This module runs inside the godserve *session subprocess* — a process the worker
spawns once per hot session and keeps alive across consecutive jobs. The worker
agent itself never imports or runs this code; it only relays fd-3 frames and the
WebSocket. So the CPU-bound / blocking work below (model inference, blob fetch)
cannot stall the worker's event loop — process isolation is the boundary.

One spec, two engines, chosen by GODSERVE_BACKEND (the opaque label godserve
inherits into this process):

  local  -> ivrit.load_model(engine='faster-whisper', ...) — model in GPU
  runpod -> ivrit.load_model(engine='runpod', ...)         — remote endpoint

init() loads the model once per session (hot across consecutive jobs); the
handler streams each transcribed segment as a partial via ctx.emit and returns
the full concatenated transcript. Audio arrives as a blob_ref in inputs and is
fetched to a local path via the on-PATH `godserve-fetch` helper.
"""

import asyncio
import os
import shutil
import subprocess
import tempfile

import ivrit

from godserve import serve

MODEL_NAME = "ivrit-ai/whisper-large-v3-turbo-ct2"

# init() takes no args and its return value is discarded by the serve shim, so
# the loaded model lives here and is shared by every job on this hot session.
_model = None


def init():
    global _model
    engine = os.environ.get("GODSERVE_BACKEND", "local")
    if engine == "runpod":
        _model = ivrit.load_model(
            engine="runpod",
            model=MODEL_NAME,
            api_key=os.environ["RUNPOD_API_KEY"],
            endpoint_id=os.environ["RUNPOD_ENDPOINT_ID"],
            core_engine="faster-whisper",
        )
    else:
        _model = ivrit.load_model(engine="faster-whisper", model=MODEL_NAME)


def _fetch_audio(inputs) -> str:
    """Fetch the audio blob to a fresh temp dir. Inputs carry a blob_ref (a URL
    or blob_id) and its sha256, mirroring godserve's blob convention (§4.6)."""
    try:
        ref = inputs["blob_ref"]
        sha256 = inputs["sha256"]
    except KeyError as exc:
        raise ValueError(f"inputs missing required key {exc}") from exc
    dest = os.path.join(tempfile.mkdtemp(prefix="ivrit-"), "audio.bin")
    subprocess.run(
        ["godserve-fetch", ref, dest, "--sha256", sha256],
        check=True,
    )
    return dest


async def _transcribe(audio_path: str, ctx) -> list[str]:
    segments = []
    async for seg in _model.transcribe_async(path=audio_path, language="he"):
        segments.append(seg.text)
        ctx.emit(seg.text)
    return segments


def handler(inputs, ctx):
    audio_path = _fetch_audio(inputs)
    try:
        segments = asyncio.run(_transcribe(audio_path, ctx))
    finally:
        shutil.rmtree(os.path.dirname(audio_path), ignore_errors=True)
    return {"transcript": "".join(segments)}


serve(handler, init=init)
