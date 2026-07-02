"""serve-mode task: init() runs once (increments a counter file, doubling as the
init-once fixture); handler echoes inputs back per job."""

import os

from godserve import serve


def init():
    # Prove init runs exactly once across N consecutive serve jobs: increment a
    # persistent counter. A hot session reuses this process, so the file stays 1.
    path = os.environ.get("ECHO_SERVE_INIT_COUNTER")
    if path:
        prev = 0
        if os.path.exists(path):
            prev = int(open(path).read() or "0")
        with open(path, "w") as f:
            f.write(str(prev + 1))


def handler(inputs, ctx):
    return {"echo": inputs}


serve(handler, init=init)
