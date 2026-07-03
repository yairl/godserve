"""serve-mode ivrit transcription task.

One spec, two engines, chosen by GODSERVE_BACKEND (the opaque label godserve
inherits into this process):

  local  -> ivrit.load_model(engine='faster-whisper', ...) — model in GPU
  runpod -> ivrit.load_model(engine='runpod', ...)         — remote endpoint

init() loads the model once per session (hot across consecutive jobs); the
generator handler streams each transcribed segment as a partial and returns the
full concatenated transcript. Audio arrives as a blob_ref in inputs and is
fetched to a local path via the on-PATH `godserve-fetch` helper.
"""

import asyncio
import os
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
    """Fetch the audio blob to a local path. Inputs carry a blob_ref (a URL or
    blob_id) and its sha256, mirroring godserve's blob convention (§4.6)."""
    ref = inputs["blob_ref"]
    sha256 = inputs["sha256"]
    dest = os.path.join(tempfile.mkdtemp(), "audio.bin")
    subprocess.run(
        ["godserve-fetch", ref, dest, "--sha256", sha256],
        check=True,
    )
    return dest


def handler(inputs, ctx):
    audio_path = _fetch_audio(inputs)

    async def _transcribe():
        segments = []
        async for seg in _model.transcribe_async(path=audio_path, language="he"):
            segments.append(seg.text)
            ctx.emit(seg.text)
        return segments

    segments = asyncio.run(_transcribe())
    return {"transcript": "".join(segments)}


serve(handler, init=init)
