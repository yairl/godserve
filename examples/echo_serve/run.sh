#!/usr/bin/env bash
# serve-mode job: launched ONCE. task.py starts the long-lived session process
# via `from godserve import serve` and speaks the fd-3 IPC protocol.
set -euo pipefail
exec python task.py
