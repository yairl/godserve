#!/usr/bin/env bash
# One-time env build. Nothing extra to install for a stdlib serve echo — the
# godserve serve shim is injected on PYTHONPATH by the agent, not pip-installed.
set -euo pipefail
echo "echo-serve env built"
