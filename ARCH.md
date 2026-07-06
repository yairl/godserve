# godserve — Architecture

**godserve** provides N-tier servicing for jobs. Clients submit into a **single
FIFO queue**; workers of different cost pull and run them. Cheap workers serve
the queue normally; **overflow worker sets activate when the queue stays over a
given depth for a given time** — several fill levels, each enabling a costlier
set. No priority queues, no WFQ.

Two ideas drive everything:

1. **Load handling** — sustained-depth fill levels enable/disable tiers.
2. **Single worker structure** — every worker pulls a self-contained bundle and
   runs it. An env variable on the worker selects its execution backend (local
   subprocess vs pushing to a 3rd party such as RunPod). **godserve itself has
   no concept of local vs remote — only tiering.** The user writes their
   venv/spec exactly once; it runs identically under any backend.

Plus a hard performance requirement: **1–2s jobs must not re-pay prep work** —
not just "venv already built" but "**process already running with prep code
executed**" (e.g. model already loaded into the GPU). This adds a **hot
session** layer (RunPod `handler()`-style) above warm envs, and a **blob**
system so big payloads never ride the hot path.

Stack: FastAPI/uvicorn, websockets, httpx, aiosqlite, pydantic, anyio; `uv` for
env builds. Trusted execution — no sandbox in v1.

---

## 1. Core model

```
 Client ──POST /v1/jobs──►  Coordinator  ◄── persistent WS (outbound) ── Workers
        ◄─stream/result──   • ONE FIFO queue (depth-tracked)          (one structure;
                            • fill levels → enable overflow tiers      tier = cost class;
                            • dispatcher: session/env affinity,        backend chosen by
                              atomic claim + lease                     env var, opaque
                            • specs + blobs (content-addressed)        to godserve)
                            • SQLite state, log pub/sub
```

### Queue and jobs

- **Single queue**, FIFO. A job is one row:
  `queued → assigned → running → succeeded | failed | canceled`.
- **Atomic claim**: `UPDATE jobs SET state='assigned', … WHERE id=? AND
  state='queued'`; `rowcount == 1` means the claimer won. This guarantees
  single execution when multiple tiers race for the same job.
- Optional per-job `max_tier` caps which tiers may run it.

### Tiers

Tiers are **opaque cost classes ordered by price**. godserve knows nothing
about what backs a tier (a laptop, a GPU host, a RunPod pool).

- **Tier 0 is never gated** — fully work-conserving, always able to claim
  first. This is the rescue path: when cheap capacity frees up, it takes work
  away from paid tiers and stops paid spend.
- **Tier k > 0** claims only while its fill level is active (§2).

### Worker

One structure everywhere: **pull bundle → ensure env → execute → stream →
result**. Execution is always the built-in local executor (§3.4);
`GODSERVE_BACKEND` is an opaque job-visible label, not a backend selector. Tier
is registration config.

One persistent **outbound** websocket per worker (NAT-friendly):

| direction | frames |
|---|---|
| worker → coordinator | `hello`, `ready`, `output`, `partial`, `heartbeat`, `result`, `goodbye` |
| coordinator → worker | `assign`, `no_work`, `cancel`, `shutdown` (+ `prepare` — deferred, §4.5) |

### Lease

Each assignment carries a lease (TTL, renewed by any `output`/`partial`/
`heartbeat`/`result` frame). Lease lapse ⇒ job requeues with `attempt++`,
bounded by `max_attempts`.

---

## 2. Load-handling API — sustained-depth fill levels

### 2.1 Config (one level per overflow tier)

```yaml
levels:
  - tier: 1
    depth: 10          # activates when queue depth ≥ 10 …
    sustain_s: 15      # … continuously for 15s
    clear_below: 3     # deactivates when depth < 3 (hysteresis)
    max_inflight: 8    # hard cost ceiling (concurrency — a job holds cost for
                       # its whole duration, so slots are the honest unit)
  - tier: 2            # deeper, later, costlier
    depth: 50
    sustain_s: 60
    clear_below: 10
    max_inflight: 20
```

### 2.2 Mechanism: measure → decide → enforce

- **Measure** (each ~1s tick):
  `LoadSnapshot { depth, over_since[k], in_flight[k] }` — per level, the
  timestamp since depth has continuously been ≥ `depth_k`.
- **Decide** (`LoadPolicy`, pluggable; default `SustainedDepthPolicy`):

  ```
  active_k:  depth ≥ depth_k continuously for sustain_s
  cleared_k: depth < clear_below_k                      (hysteresis — no flapping)
  budget_k = active_k ? min(max_inflight_k − in_flight_k, depth) : 0
  ```

- **Enforce**: a tier-k (k>0) worker's `ready` is served only while
  `budget_k > 0`; each assignment decrements budget until the next tick;
  failed/expired assignments return budget. Tier 0 is served immediately,
  always.

Several fill levels ⇒ progressive recruitment of costlier sets; `sustain_s`
gives burst immunity; hysteresis + tier-0 rescue give self-termination of paid
spend. Smarter policies later change nothing outside `coordinator/load.py`.

*(Deferred)* Fill-level activation will also emit `prepare` hints (§4.5) so
recruited tier-k workers warm up before their first claim.

---

## 3. Job submission API & the single worker structure

### 3.1 The `JobSpec` — two self-contained scripts

Env build and launch are **plain, self-contained scripts** — no `files[]`, no
`inputs[]` DSL, no Dockerfile-like language. Each script is hashed whole:

```yaml
# godserve.yaml
name: whisper-transcribe            # optional, informational
python: "3.13"                      # base venv provided by godserve (kind: docker later)
setup: setup.sh                     # one-time env build — self-contained
run: { script: run.sh, mode: serve }    # serve = hot session | once = per job
defaults: { timeout_s: 300, max_attempts: 2, max_tier: null }
```

```bash
# setup.sh — venv already created+activated by godserve; self-contained:
# everything inline or fetched by immutable ref; env vars are just exports
uv pip install "faster-whisper==1.2.0"
godserve-fetch https://…/whisper-v3.safetensors models/ --sha256 abc…      # machine-wide cache
godserve-fetch https://…/code-4f2a91.tar.gz . --sha256 4f2a91… --extract   # job code
```

```bash
# run.sh — stays alive as the hot session (mode: serve)
export HF_HOME=./hf
exec python task.py        # task.py: from godserve import serve; serve(handler, init=init)
```

- **Content-addressing is trivial**:
  `env_key = H(python + setup.sh)`,
  `session_key = H(env_key + run.sh + mode)`.
  Any byte change ⇒ new key ⇒ fresh build/session.
- **Immutable refs only** inside scripts (sha256 / pinned tags, never branch
  names) — a mutable ref would silently reuse a stale env/session.
- **Local files survive as CLI sugar**: `godserve submit --dir ./job` packs the
  directory, uploads it as a blob (§4.6), and synthesizes the
  `godserve-fetch <blob-url> . --extract` line. The core model stays file-less.
- When a docker env kind lands later, the "single build file" becomes a
  *literal* Dockerfile — no custom DSL ever.

### 3.2 HTTP API

```
POST /v1/specs   {spec}                      → { spec_id }     # content-hash; dedup
POST /v1/jobs    {spec: {...} | spec_id, inputs, overrides?}   → { job_id }
GET  /v1/jobs/{id}                           → state + structured result
GET  /v1/jobs/{id}/stream                    → WS/SSE: logs + partial results
                                               (replay + live tail) + result frame
POST /v1/jobs/{id}/cancel
POST /v1/blobs   (bytes)                     → { blob_id, url } # large payloads (§4.6)
POST /v1/prewarm {spec | spec_id, tier?}     → prep without a job (§4.5, DEFERRED)
```

Registered specs make repeat submission `{spec_id, inputs}` — tiny, deduped.
CLI: `godserve submit -f godserve.yaml -i '{"x":1}'`.

### 3.3 The invariant: self-contained, location-independent `JobBundle`

```
JobBundle = { job_id, spec (resolved), inputs | blob_ref,
              timeout_s, lease_ttl_s, result_contract }
```

No references to coordinator-local state — the bundle runs anywhere, under any
backend.

### 3.4 Single worker structure — one execution path

Every worker runs the same loop and the same execution path: godserve ships
**one** built-in executor and always uses it. The internal `Backend` Protocol
is the seam through which both `once` and `serve` modes dispatch:

```python
class Backend(Protocol):
    async def run(self, bundle: JobBundle, io: JobIO) -> JobOutcome:
        """io.emit_log(stream, chunk) / io.emit_partial(chunk) as it goes;
        returns {status, exit_code, result|error}."""
```

There is no execution-backend selection. `GODSERVE_BACKEND` is **not** a
godserve concept beyond a label: its raw value is inherited (via
`os.environ.copy()`) by the setup.sh build subprocess and the serve process, so
a job's own handler may read it to choose *its own* remoting — e.g. the
reference example (§8) reads it to select ivrit's `faster-whisper` (local) vs
`runpod` engine. Remoteness, when a job wants it, lives inside the job's
library; godserve core always runs the built-in local executor on-host and
never resolves a "remote" backend.

Why this holds together: warm/hot **affinity is just keys**. A worker
advertises the `env_key`s / `session_key`s it can serve hot; the coordinator
sees only a worker at some tier, never how or where the work runs.

---

## 4. Execution environments — three warmth layers

A 1–2s job must not re-pay *any* prep. Three content-addressed layers, each
skippable when warm:

| layer | key | built by | reuse cost when warm |
|---|---|---|---|
| **env** (fs: venv/docker) | `env_key = H(python + setup.sh)` | `setup.sh`, once per key | fork/exec ~10ms |
| **session** (live process, init done) | `session_key = H(env_key + run.sh + mode)` | `run.sh` spawn + init, once per process | **~1ms IPC — model stays in GPU** |
| job | — | `handler(inputs)` per job | the job itself |

`env_key` hashes the *text* of `setup.sh`, not the environment it runs in. A
`setup.sh` that branches on inherited env vars (e.g. `GODSERVE_BACKEND`)
therefore produces the same `env_key` regardless of which branch runs, so two
deployments feeding different values would share and collide on
`envs/{env_key}/`. Fix such vars per deployment — one value per machine.

### 4.1 Env layer (`EnvProvider`, pluggable) + machine-wide content cache

- **VenvProvider (uv) — v1 default**: `envs/{env_key}/` built once; "toggling
  venvs" = spawning with a different `VIRTUAL_ENV`/`PATH` (zero switch cost);
  LRU-evict by disk budget; concurrent jobs share a warm env (read-only venv,
  per-job scratch cwd).
- **`godserve-fetch` helper** (on PATH inside setup.sh/run.sh):
  `godserve-fetch <url> <dest> --sha256 <hash> [--extract]` downloads through a
  **machine-wide content-addressed cache** `content/{sha256}`, hardlinking into
  the env — model weights download **once per machine**, shared across
  env_keys, verified by hash.
- **DockerProvider — later** (system deps / isolation): image per `env_key`;
  warm container start ~100–300ms. The session layer works identically inside
  it.

### 4.2 Session layer (hot process reuse — the RunPod-`handler()` model)

For `run.mode == "serve"`, the agent launches `run.sh` **once** inside the env;
whatever long-lived process it starts becomes the session by speaking a minimal
IPC protocol on a dedicated **duplex socket on fd 3**. The agent creates a
`socket.socketpair()`; the child endpoint is placed on fd 3 via `pass_fds` plus
a `preexec_fn` that `dup2`s it onto 3 and clears `CLOEXEC` (`pass_fds` alone does
not land it on a fixed number). The parent endpoint drives a single asyncio
transport (StreamReader for frames from the child, StreamWriter for jobs to it);
frames are newline-delimited JSON. stdout/stderr stay separate log pipes:

```
session → agent:  READY                       (after init/prep completes)
agent  → session: {job_id, inputs}            (one job at a time)
session → agent:  {job_id, partial}*          (zero or more streamed chunks)
session → agent:  {job_id, result | error}    (terminal frame per job)
```

**stdout/stderr remain log streams**, tagged with the current `job_id`, relayed
upstream as `output`.

- The protocol is trivially speakable from any language, but the Python SDK
  makes it invisible: `from godserve import serve; serve(handler, init=init)`.
  `init()` runs once (load model into GPU); `handler(inputs, ctx)` runs per
  job. The **serve shim is a single dependency-free module** the agent puts on
  `PYTHONPATH`, so jobs don't need godserve installed in their venv.
- **Partial-result streaming (end-to-end lossless, ordered)**: a handler may
  `ctx.emit(chunk)` — or be a generator yielding chunks — producing
  `{job_id, partial}` IPC frames relayed to `stream` subscribers alongside logs
  and persisted for replay. Emission is **lossless with backpressure**: a
  blocking `sendall` on fd 3 → session read loop → a single bounded agent FIFO →
  one WS sender task. A slow link may **block the handler** (this counts against
  `timeout_s`); frames are never silently dropped. The coordinator **persists
  each frame (commit) before publishing**, so emit order == persist order ==
  delivery order; the internal pubsub tail stays drop-oldest (a wake-up channel
  only) and stream clients gap-repair missing frames from the DB by `seq`.
  Chunks obey the 256 KB cap as a **hard error at emission** — an oversized or
  non-JSON partial raises in the handler (the session survives), big chunks go
  via blobs (§4.6). The terminal `{job_id, result}` frame is unchanged and
  **uncapped** (auto-spills to a blob above the cap; polling clients see only
  the final result).
- A live session **receives the next job iff `session_key` matches** — no
  respawn, no re-init, no model reload. Idle timeout ⇒ graceful exit (frees
  GPU). Crash mid-job ⇒ that job requeues; the session respawns on demand.
- Worker config `max_live_sessions` (default 1 on GPU hosts); at cap, evict an
  **idle** LRU session first. Session knobs (all `GODSERVE_`-prefixed):
  `GODSERVE_MAX_LIVE_SESSIONS` (default 1), `GODSERVE_SESSION_IDLE_S` (idle
  seconds before graceful shutdown, default 300), `GODSERVE_SESSION_INIT_S`
  (seconds to await `SessionReady` after spawn, default 300). A live session's
  env is pinned (acquired for the session's whole lifetime) so it is never
  LRU-evicted from under a running process; the env is released on session
  close. With cap 1 on a single-slot worker, "cap reached but all busy" cannot
  occur, so no waiter exists — this is a config constraint, not a runtime path.
- `run.mode == "once"` bypasses sessions: `run.sh` executes per job, reads
  `$GODSERVE_INPUTS`, writes JSON to `$GODSERVE_RESULT_PATH` — no SDK needed.

### 4.3 Dispatch affinity (hot > warm > cold)

Workers advertise `live_sessions` + `warm_envs` in `ready`. Dispatcher picks:

1. oldest job matching a **live session** (hot, ~ms),
2. oldest job matching a **warm env** (pays spawn + init),
3. oldest job overall (cold).

Starvation guard: affinity may skip the head job only within a bounded age
window (knob, ~30s default) — and a skipped head job raises depth signals,
recruiting capacity anyway.

### 4.4 Latency budget (hot path)

WS push-on-ready + IPC into a live session ⇒ overhead ≈ WS round-trip + ~1ms
IPC. No venv creation, no import, no model load. **Target < 20ms per job.**

### 4.5 Ahead-of-time prep — DOCUMENTED, DEFERRED (not in v1)

Prep runs **concurrently with the currently-executing job**: while a worker
runs job N, it fetches content and builds the env (and optionally spawns +
`init()`s a session) for a likely next spec, in the background, throttled so
running jobs are undisturbed. This saves generic download time regardless of
job duration.

- **Mechanism**: coordinator→worker `prepare {spec, warm: "env" | "session"}`,
  best-effort (failure just means the first job pays the cold path).
- **Triggers (design)**: (1) fill-level activation — recruited tier-k workers
  prep the env/session keys of queued jobs before their first claim; (2) queued
  jobs whose `env_key` no ready worker holds warm; (3) explicit
  `POST /v1/prewarm`.
- Kept in the protocol and API design; **excluded from the implementation
  phases**.

### 4.6 Payload limits: inline vs blob (the 256 KB rule)

The worker WS is one multiplexed connection carrying heartbeats, logs, and
assignments for all slots — a large frame head-of-line-blocks everything behind
it. **256 KB at ~100 Mbps ≈ 20 ms — exactly the hot-path budget**; a 10 MB
frame would stall ~1s and delay lease renewals. It also bounds SQLite row size
and coordinator memory. Therefore:

- `inputs` and per-element stream chunks (partials/logs) are capped at
  **256 KB inline** (configurable), enforced as a hard error at emission.
  Terminal `result`s are **uncapped** and auto-spill to a blob above the cap.
- Anything larger: `POST /v1/blobs` (coordinator-hosted, sha256-verified) or
  any external URL via `godserve-fetch` / `inputs.blob_ref`; workers download
  over HTTP into the machine-wide content cache (§4.1).
- Oversized results are stored as blobs automatically; the result JSON carries
  a `blob_ref`.

---

## 5. Data model (SQLite / aiosqlite)

```
jobs:     id PK, state, spec_id, env_key, session_key, inputs JSON|blob_ref,
          result JSON|blob_ref, exit_code, error, attempt, max_attempts,
          max_tier, timeout_s, submitted_at, assigned_to, claimed_tier,
          lease_expires, created, updated
specs:    spec_id PK (content hash), spec JSON, created
blobs:    blob_id PK (sha256), size, path (coordinator disk), created
workers:  id PK, tier, state(ready|busy|draining|dead), max_slots,
          warm_envs JSON, live_sessions JSON, last_seen
job_logs: job_id, seq, stream(stdout|stderr|partial), data, ts   (append-only)
```

Fill-level/tier config lives in the coordinator config file. Budgets,
`over_since`, and stats are in-memory, recomputed each tick. Restart: rehydrate
from SQLite, expire stale leases → requeue.

---

## 6. Reliability

- **Worker churn**: hard drop ⇒ leases expire ⇒ requeue; graceful
  `goodbye{drain}` ⇒ finish in-flight, then leave; rejoin ⇒ re-advertise warm
  envs (sessions die with the agent; respawn on demand).
- **Session crash**: fails only the in-flight job (requeue); the next matching
  job triggers respawn + re-init.
- **Backend failures**: a backend error/timeout looks like any job failure (the
  worker holds the lease); godserve needs no remote-specific handling.
- **Semantics**: at-least-once delivery + single-claim ⇒ once-per-success.
  Retried jobs must tolerate re-execution — handlers should be idempotent
  (documented contract).

---

## 7. Module layout

```
godserve/
  models.py                 # JobSpec (setup/run scripts), JobBundle, level configs
  protocol.py               # WS frames (incl. deferred `prepare`) + session IPC
  db.py                     # schema, atomic claim, lease sweeps
  coordinator/
    app.py                  # FastAPI: /v1/jobs|specs|blobs, worker WS, streams
    queue.py                # single FIFO queue, depth tracking
    load.py                 # LoadSnapshot, LoadPolicy, SustainedDepthPolicy
    dispatcher.py           # budget gating + hot/warm/cold affinity + claim + lease
    registry.py             # workers, heartbeats, lease-expiry sweeper
    blobs.py                # content-addressed blob store
    pubsub.py               # log fan-out
  worker/
    agent.py                # the single worker loop (identical everywhere)
    backends/{base,local}.py          # internal execution seam + the sole
                                      # built-in local backend (always used)
    envs/{base,venv}.py     # EnvProvider; DockerProvider later
    content.py              # machine-wide content-addressed cache (godserve-fetch)
    session.py              # session manager: spawn run.sh, READY, IPC, idle/evict
    serve_shim.py           # dependency-free `serve(handler, init=)` on PYTHONPATH
  client/sdk.py             # submit(), stream(), result()
  cli.py                    # godserve coordinator|worker|submit|status + godserve-fetch
```

*(Deferred: `coordinator/prep.py` — prepare-hint triggers, §4.5.)*

---

## 8. Deferred / later

- Ahead-of-time prep (§4.5) — protocol slot reserved.
- DockerProvider env kind.
- Artifacts / S3-compatible object storage backing for blobs.
- Smarter `LoadPolicy` implementations (all confined to `coordinator/load.py`).

See `PLAN.md` for the phased implementation plan and acceptance criteria.
