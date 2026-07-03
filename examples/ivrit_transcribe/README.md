# ivrit transcription — one spec, two tiers

A single serve-mode godserve spec that transcribes Hebrew audio, running
unchanged on a local-GPU tier-0 worker and a RunPod-backed tier-1 worker. The
coordinator sees only tiers; remoteness lives entirely inside the job's own
library (ivrit's `runpod` engine), never in godserve core.

The engine is selected by `GODSERVE_BACKEND` — the same opaque label godserve
inherits into the setup.sh build and the serve process. godserve does not
interpret it; this example reads it to pick its own engine.

## Files

- `godserve.yaml` — serve-mode spec. Pins `python: "3.12"` (ivrit 0.2.5 /
  faster-whisper have no 3.13 wheels at the time of writing) and a generous
  `timeout_s`.
- `setup.sh` — installs the full inference stack for `local`, or just the ivrit
  client for `runpod`.
- `run.sh` — `exec python transcribe.py` (serve mode).
- `transcribe.py` — `init()` loads the model once per session; the generator
  handler streams each segment as a partial and returns the full transcript.

## Running the local leg (tier 0)

```bash
export GODSERVE_BACKEND=local
godserve worker --url ws://COORDINATOR/v1/worker   # GODSERVE_TIER defaults to 0
```

Submit a job with the audio uploaded as a blob:

```bash
BLOB=$(curl -s --data-binary @message.ogg http://COORDINATOR/v1/blobs)
# BLOB -> {"blob_id": "...", "url": "/v1/blobs/..."}
godserve submit -f examples/ivrit_transcribe/godserve.yaml \
    -i '{"blob_ref": "http://COORDINATOR/v1/blobs/<blob_id>", "sha256": "<blob_id>"}' \
    --follow
```

Segment texts stream live as partials; the final result is the full transcript.

## Running the RunPod leg (tier 1)

```bash
export GODSERVE_BACKEND=runpod
export RUNPOD_API_KEY=...
export RUNPOD_ENDPOINT_ID=...
export GODSERVE_TIER=1
godserve worker --url ws://COORDINATOR/v1/worker
```

The **same unmodified spec** (identical `spec_id`, `env_key`, and
`session_key`) serves both tiers — that identity is exactly what lets one spec
be hot on both. The script text is byte-for-byte the same; only the inherited
`GODSERVE_BACKEND` differs, which the handler reads at runtime.

## Caveat: one engine per machine

`env_key` hashes the **text** of `setup.sh`, not the environment it runs in.
Because both engines share the same `setup.sh` text, they compute the same
`env_key` and would share (and collide in) the same `envs/{env_key}/`
directory. Fix `GODSERVE_BACKEND` per worker deployment — run **one engine per
machine**. Do not point a `local` worker and a `runpod` worker at the same
`GODSERVE_CACHE_DIR` / work root.
