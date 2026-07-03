#!/usr/bin/env bash
# One-time env build for the ivrit transcription example.
#
# Branches on GODSERVE_BACKEND — the opaque label godserve inherits into this
# build subprocess. This is the engine selector for the example:
#   local  -> full on-host inference stack (faster-whisper + ctranslate2)
#   runpod -> ivrit alone; heavy inference runs on the remote endpoint
#
# NOTE: env_key hashes THIS SCRIPT'S TEXT, not GODSERVE_BACKEND. Fix
# GODSERVE_BACKEND per worker deployment (one engine per machine) — see README.
set -euo pipefail

ENGINE="${GODSERVE_BACKEND:-local}"

if [ "$ENGINE" = "runpod" ]; then
    echo "ivrit-transcribe: installing runpod client deps"
    uv pip install "ivrit==0.2.5"
else
    echo "ivrit-transcribe: installing local inference stack"
    uv pip install "ivrit==0.2.5" "faster-whisper"
fi
