"""CLI: godserve coordinator|worker|submit|status (PLAN §1.6).

`--dir` sugar packs a directory into a blob and appends a synthesized
`godserve-fetch <url> . --extract` line to the spec's setup script (§3.1).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path

import yaml

from .client.sdk import Client
from .models import JobSpec, RunConfig, JobDefaults


def _load_spec(spec_file: str) -> JobSpec:
    """Load a godserve.yaml, inlining setup/run script file paths (§3.1)."""
    path = Path(spec_file)
    doc = yaml.safe_load(path.read_text())
    base = path.parent

    setup = _inline(base, doc["setup"])
    run = doc["run"]
    run_script = _inline(base, run["script"])
    defaults = JobDefaults(**(doc.get("defaults") or {}))
    return JobSpec(
        name=doc.get("name"),
        python=str(doc["python"]),
        setup=setup,
        run=RunConfig(script=run_script, mode=run["mode"]),
        defaults=defaults,
    )


def _inline(base: Path, value: str) -> str:
    """A path relative to the spec is read as file contents; else literal."""
    candidate = base / value
    if candidate.exists() and candidate.is_file():
        return candidate.read_text()
    return value


def _pack_dir(dir_path: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(dir_path, arcname=".")
    return buf.getvalue()


async def _submit(args) -> int:
    client = Client(args.url)
    spec = _load_spec(args.file)

    if args.dir:
        data = _pack_dir(args.dir)
        sha = hashlib.sha256(data).hexdigest()
        blob = await client.upload_blob(data)
        url = args.url.rstrip("/") + blob["url"]
        fetch_line = f"godserve-fetch {url} . --sha256 {sha} --extract\n"
        spec = spec.model_copy(update={"setup": spec.setup + "\n" + fetch_line})

    inputs = json.loads(args.inputs) if args.inputs else {}
    job_id = await client.submit(spec, inputs)
    print(job_id)

    if args.follow:
        async for frame in client.stream(job_id):
            if frame.get("t") in ("output", "partial"):
                sys.stdout.write(frame.get("data", ""))
                sys.stdout.flush()
            elif frame.get("t") == "result":
                print(json.dumps({"status": frame.get("status"), "result": frame.get("result"), "error": frame.get("error")}))
    return 0


async def _status(args) -> int:
    client = Client(args.url)
    st = await client.status(args.job_id)
    print(json.dumps(st, indent=2))
    return 0


def _coordinator(args) -> int:
    import uvicorn

    from .coordinator.app import create_app

    cfg = {}
    if args.config and Path(args.config).exists():
        cfg = yaml.safe_load(Path(args.config).read_text()) or {}
    db_path = cfg.get("db_path", args.db or "godserve.sqlite3")
    blob_root = cfg.get("blob_root", args.blob_root or "godserve-data")
    app = create_app(db_path, blob_root)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


def _worker(args) -> int:
    from .worker.agent import run_agent

    asyncio.run(run_agent(args.url, args.work_root, max_slots=args.slots))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="godserve")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_coord = sub.add_parser("coordinator")
    p_coord.add_argument("--config", default=None)
    p_coord.add_argument("--host", default="127.0.0.1")
    p_coord.add_argument("--port", type=int, default=8000)
    p_coord.add_argument("--db", default=None)
    p_coord.add_argument("--blob-root", dest="blob_root", default=None)

    p_worker = sub.add_parser("worker")
    p_worker.add_argument("--url", required=True, help="ws://host:port/v1/worker")
    p_worker.add_argument("--work-root", dest="work_root", default="godserve-work")
    p_worker.add_argument("--slots", type=int, default=1)

    p_submit = sub.add_parser("submit")
    p_submit.add_argument("--url", default="http://127.0.0.1:8000")
    p_submit.add_argument("-f", "--file", required=True)
    p_submit.add_argument("-i", "--inputs", default=None)
    p_submit.add_argument("--dir", default=None)
    p_submit.add_argument("--follow", action="store_true")

    p_status = sub.add_parser("status")
    p_status.add_argument("--url", default="http://127.0.0.1:8000")
    p_status.add_argument("job_id")

    args = parser.parse_args(argv)

    if args.cmd == "coordinator":
        return _coordinator(args)
    if args.cmd == "worker":
        return _worker(args)
    if args.cmd == "submit":
        return asyncio.run(_submit(args))
    if args.cmd == "status":
        return asyncio.run(_status(args))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
