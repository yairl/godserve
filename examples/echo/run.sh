#!/usr/bin/env bash
# once-mode job: read $GODSERVE_INPUTS (JSON), write JSON to $GODSERVE_RESULT_PATH.
set -euo pipefail
echo "running echo job"
python task.py
