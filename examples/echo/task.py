"""once-mode task: echo the inputs back as the structured result."""

import json
import os

inputs = json.loads(os.environ.get("GODSERVE_INPUTS", "{}"))
result = {"echo": inputs}

with open(os.environ["GODSERVE_RESULT_PATH"], "w") as f:
    json.dump(result, f)

print("echo job done")
