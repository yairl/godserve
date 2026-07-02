#!/usr/bin/env bash
# One-time env build. The venv is already created and activated by godserve;
# this script is self-contained. Nothing extra to install for a stdlib echo.
set -euo pipefail
echo "echo-once env built"
