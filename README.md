# godserve

N-tier job servicing with warm envs, hot sessions, and load-based tiering.

Clients submit jobs into a **single FIFO queue**. Workers of different cost
tiers pull and run them. Tier 0 is never gated; higher (more expensive) overflow
tiers activate only under **sustained** queue depth and clear once the backlog
drains. The coordinator has no concept of local vs remote — **only tiering**.

Two mechanisms keep per-job overhead low:

- **Warm envs** — an env (built from `python` + `setup.sh`) is content-addressed
  by `env_key` and reused across jobs; `uv` builds it once.
- **Hot sessions** — a `serve`-mode job runs `init()` once (e.g. a model loaded
  into GPU) and stays resident across consecutive short jobs, keyed by
  `session_key`.

Every worker runs the same built-in local executor. `GODSERVE_BACKEND` is an
**opaque, job-visible label** inherited by job subprocesses — the job's own code
may read it to choose its remoting (see the ivrit example), but godserve core
never interprets it and never selects an execution backend from it.

## Install

Requires Python 3.13 and [`uv`](https://docs.astral.sh/uv/) (used for env builds).

```bash
uv sync              # or: pip install -e .
```

This installs two CLI entrypoints: `godserve` and `godserve-fetch`.

## Quickstart

**1. Start a coordinator** (single-node SQLite queue + blob store):

```bash
godserve coordinator --host 127.0.0.1 --port 8000
```

Optional `--config config.yaml` sets `db_path`, `blob_root`, and overflow
`levels`:

```yaml
db_path: godserve.sqlite3
blob_root: godserve-data
levels:
  - {tier: 1, depth: 20, sustain_s: 10, clear_below: 5, max_inflight: 4}
```

**2. Start a worker** (tier 0 by default; `--slots` = concurrent jobs):

```bash
GODSERVE_TIER=0 godserve worker --url ws://127.0.0.1:8000/v1/worker --slots 2
```

**3. Submit a job** from a spec directory:

```bash
godserve submit -f examples/echo/godserve.yaml -i '{"msg": "hi"}' --follow
```

- `-i/--inputs` — inline JSON inputs (kept under the 256 KB cap).
- `--dir PATH` — pack a directory into a blob and append a fetch+extract step to
  the spec's setup (for shipping code/assets with the job).
- `--follow` — stream partials/logs until the job finishes.

Check status later:

```bash
godserve status <job_id> --url http://127.0.0.1:8000
```

## Job spec (`godserve.yaml`)

A spec is self-contained; `setup`/`run.script` may point at sibling files, whose
contents are inlined at registration.

```yaml
name: my-task
python: "3.13"          # interpreter for the env
setup: setup.sh         # one-time env build (uv pip install ...)
run:
  script: run.sh        # process entrypoint
  mode: serve           # "serve" (hot session) or "once" (per-job process)
defaults:
  timeout_s: 300
  max_attempts: 2
  max_tier: null        # cap the highest tier this job may run on
```

Content-address keys (never bypass these):

| Key          | Derivation                              |
|--------------|-----------------------------------------|
| `env_key`    | `H(python + setup.sh)`                   |
| `session_key`| `H(env_key + run.sh + mode)`             |
| `spec_id`    | `H(canonical spec JSON)`                 |

Because the engine branch lives *inside* `setup.sh`, one spec yields identical
keys across differently-labelled workers — the same spec runs on any tier and
affinity works uniformly.

### Serve mode

A `serve` handler uses the shim:

```python
from godserve import serve

def init():                 # runs once per hot session; return value discarded
    ...

def handler(inputs, ctx):   # runs per job; ctx.emit(chunk) streams partials
    ...
    return {"result": ...}

serve(handler, init=init)
```

## HTTP / WebSocket API

Served by the coordinator (default `http://127.0.0.1:8000`):

| Method | Path                        | Purpose                                  |
|--------|-----------------------------|------------------------------------------|
| POST   | `/v1/specs`                 | Register a spec, returns `spec_id`       |
| POST   | `/v1/jobs`                  | Enqueue a job                            |
| GET    | `/v1/jobs/{job_id}`         | Job status/result                        |
| POST   | `/v1/jobs/{job_id}/cancel`  | Request cancellation                     |
| WS     | `/v1/jobs/{job_id}/stream`  | Live partials + logs                     |
| POST   | `/v1/blobs`                 | Upload an out-of-band payload            |
| GET    | `/v1/blobs/{blob_id}`       | Download a blob                          |
| POST   | `/v1/prewarm`               | Reserved — returns 501 (deferred)        |
| WS     | `/v1/worker`                | Worker attach (claim/lease/stream)       |

`inputs` and per-element stream chunks (partials/logs) are capped at **256 KB**
inline — an oversized or non-JSON partial raises a hard error in the handler
(the session survives), so large chunks must go through blobs. Terminal
`result`s are **uncapped** and auto-spill to a blob above the cap. Streaming is
end-to-end lossless and ordered: emission may block the handler as backpressure
(counting against `timeout_s`), and stream clients gap-repair from the DB, so no
partial is silently dropped. `POST /v1/blobs` honors optional auth
(`GODSERVE_BLOB_TOKEN`, 401), a per-request size cap (`GODSERVE_BLOB_MAX_BYTES`,
413), and a disk quota (`GODSERVE_BLOB_DISK_QUOTA_BYTES`, 507).

## Environment variables

All config uses the `GODSERVE_` prefix.

| Var                             | Read by   | Purpose                                     |
|---------------------------------|-----------|---------------------------------------------|
| `GODSERVE_TIER`                 | worker    | This worker's cost tier (0 = never gated)   |
| `GODSERVE_BACKEND`              | job       | Opaque label inherited by job subprocesses  |
| `GODSERVE_CACHE_DIR`            | `godserve-fetch` | Cache dir for fetched blobs           |
| `GODSERVE_ENV_DISK_BUDGET`      | worker    | LRU-evict envs above this byte budget       |
| `GODSERVE_MAX_LIVE_SESSIONS`    | worker    | Cap on concurrent hot sessions              |
| `GODSERVE_SESSION_IDLE_S`       | worker    | Idle timeout before a hot session is torn down |
| `GODSERVE_SESSION_INIT_S`       | worker    | Deadline for `init()` on session start      |
| `GODSERVE_INPUTS`               | handler   | Path to the job's inputs JSON               |
| `GODSERVE_RESULT_PATH`          | handler   | Path where the job writes its result        |
| `GODSERVE_BLOB_TOKEN`           | coordinator | Bearer/header token gating blob uploads   |
| `GODSERVE_BLOB_MAX_BYTES`       | coordinator | Max size of a single blob upload          |
| `GODSERVE_BLOB_DISK_QUOTA_BYTES`| coordinator | Total blob-store disk quota               |

## Examples

- `examples/echo/` — minimal `once`-mode job.
- `examples/echo_serve/` — minimal `serve`-mode hot session.
- `examples/ivrit_transcribe/` — one serve spec that transcribes Hebrew audio,
  selecting ivrit's `faster-whisper` (local GPU) or `runpod` (remote endpoint)
  engine from `GODSERVE_BACKEND`. Deploy a tier-0 worker with
  `GODSERVE_BACKEND=local` and a tier-1 worker with `GODSERVE_BACKEND=runpod`
  (+ `RUNPOD_API_KEY` / `RUNPOD_ENDPOINT_ID`); the same spec runs on both. See
  its README for the "one engine per machine" caveat.

## Layout

```
godserve/
  cli.py            # godserve / godserve-fetch entrypoints
  models.py         # JobSpec, RunConfig, keys
  coordinator/      # queue, dispatcher, blobs, HTTP/WS app
  worker/           # agent, env provider (uv), sessions, serve shim
  client/           # submit SDK
```

## Docs

- **[ARCH.md](ARCH.md)** — architecture reference (converged design).
- **[PLAN.md](PLAN.md)** — phased implementation plan and acceptance criteria.
- **[CLAUDE.md](CLAUDE.md)** — hard invariants and contributor workflow.

## Development

```bash
uv run pytest
```

Trusted execution — no sandbox in v1.
