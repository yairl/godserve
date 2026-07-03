#!/usr/bin/env bash
# serve-mode job: launched ONCE. transcribe.py starts the long-lived session
# process via `from godserve import serve` and speaks the fd-3 IPC protocol.
# The serve shim is injected on PYTHONPATH by the session manager.
set -euo pipefail
exec python transcribe.py
