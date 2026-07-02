# godserve — Implementation Plan

Companion to `ARCH.md` (the architecture reference — read it first). This
document breaks the build into 4 phases of ordered, independently-verifiable
tasks with acceptance criteria. Section references (§) point into `ARCH.md`.

**Ground rules**

- Stack: Python 3.13, FastAPI/uvicorn, `websockets`, httpx, aiosqlite,
  pydantic, anyio; `uv` for env builds. Async throughout.
- Trusted execution — no sandboxing in v1.
- Ahead-of-time prep (§4.5) is **designed but not implemented**: keep the
  `prepare` frame in `protocol.py` and the `/v1/prewarm` route stubbed (501),
  but write no prep logic.
- The coordinator must contain **zero backend-specific branches** — this is an
  explicit acceptance check in Phase 3.

---

## Phase 1 — Core loop (submit → claim → execute → result)

Goal: end-to-end happy path with `once`-mode jobs, warm-env reuse, the content
cache, streaming logs, and churn-safe requeue. Single tier (tier 0) only.

### 1.1 `models.py` — core types (pydantic)

```python
class RunConfig(BaseModel):
    script: str                      # contents of run.sh (inlined at registration)
    mode: Literal["serve", "once"]

class JobSpec(BaseModel):
    name: str | None = None
    python: str                      # e.g. "3.13"
    setup: str                       # contents of setup.sh (inlined)
    run: RunConfig
    defaults: JobDefaults            # timeout_s=300, max_attempts=2, max_tier=None

    @property
    def env_key(self) -> str: ...      # sha256(python + "\0" + setup)
    @property
    def session_key(self) -> str: ...  # sha256(env_key + "\0" + run.script + "\0" + run.mode)
    @property
    def spec_id(self) -> str: ...      # sha256 of canonical-JSON of the whole spec

class JobBundle(BaseModel):
    job_id: str
    spec: JobSpec                    # fully resolved — no coordinator references
    inputs: dict | BlobRef
    timeout_s: int
    lease_ttl_s: int

class LevelConfig(BaseModel):        # used in Phase 3, define now
    tier: int; depth: int; sustain_s: float
    clear_below: int; max_inflight: int
```

Note: the API accepts `setup`/`run.script` as file *paths* in the CLI/YAML, but
they are **inlined into the spec** before hashing/registration — the stored
spec is self-contained (§3.1).

### 1.2 `protocol.py` — WS frames + IPC frames

Discriminated-union pydantic models over `{"t": <type>, ...}`; one
`parse_frame(bytes) -> Frame` and `frame.dump() -> bytes` pair.

- Worker→coordinator: `Hello{worker_id, tier, max_slots, warm_envs,
  live_sessions}`, `Ready{slots_free, warm_envs, live_sessions}`,
  `Output{job_id, stream, data}` (stream ∈ stdout|stderr), `Partial{job_id,
  data}`, `Heartbeat{running: [job_id]}`, `Result{job_id, status, exit_code,
  result | error}`, `Goodbye{drain: bool}`.
- Coordinator→worker: `Assign{bundle: JobBundle}`, `NoWork{}`,
  `Cancel{job_id}`, `Shutdown{}`, `Prepare{spec, warm}` (defined, unused).
- Session IPC (fd 3, newline-delimited JSON): `SessionReady{}`,
  `SessionJob{job_id, inputs}`, `SessionPartial{job_id, data}`,
  `SessionResult{job_id, result | error}`.
- Constants: `INLINE_CAP = 256 * 1024`.

### 1.3 `db.py` — schema + primitives

Schema exactly as §5. Key operations (all async, aiosqlite, WAL mode):

```python
async def submit_job(spec_id, env_key, session_key, inputs, opts) -> job_id
async def claim_job(job_id, worker_id, tier, lease_ttl_s) -> bool
    # UPDATE jobs SET state='assigned', assigned_to=?, claimed_tier=?,
    #        lease_expires=?, updated=?
    # WHERE id=? AND state='queued'      → return rowcount == 1
async def renew_lease(job_id, lease_ttl_s)
async def finish_job(job_id, status, exit_code, result_or_error)
async def requeue_expired(now) -> list[job_id]
    # state='assigned'|'running' AND lease_expires < now
    # → attempt < max_attempts ? state='queued', attempt++ : state='failed'
async def queued_depth() -> int
async def oldest_queued(match: {"session_key"|"env_key": str} | None,
                        max_tier_ge: int, skip_window_s: float) -> row | None
async def append_log(job_id, stream, data); async def read_logs(job_id, from_seq)
```

Startup recovery: expire all stale leases → requeue (§5 restart note).

### 1.4 Coordinator skeleton

- `coordinator/app.py` — FastAPI app + lifespan (open DB, start sweeper +
  load tick tasks). Routes:
  - `POST /v1/specs` → inline scripts, compute `spec_id`, upsert (dedup by
    hash), return `{spec_id}`.
  - `POST /v1/jobs` → resolve `spec | spec_id`; reject inline `inputs` >
    `INLINE_CAP` with 413 and a hint to use `/v1/blobs`; insert job; return
    `{job_id}`.
  - `GET /v1/jobs/{id}` → state + result (or `blob_ref`).
  - `POST /v1/jobs/{id}/cancel` → mark canceled; send `Cancel` if assigned.
  - `GET /v1/jobs/{id}/stream` → WS: replay `job_logs` from seq 0, then live
    tail via pubsub, terminal `result` frame, close.
  - `POST /v1/blobs` → stream to disk, sha256 as `blob_id`, return
    `{blob_id, url}`; `GET /v1/blobs/{id}` serves it.
  - `WS /v1/worker` → worker protocol handler (below).
  - `POST /v1/prewarm` → 501 (deferred).
- `coordinator/blobs.py` — content-addressed store on coordinator disk:
  write to temp, hash, rename to `blobs/{sha256}`; idempotent.
- `coordinator/pubsub.py` — per-job in-memory fan-out with bounded per-
  subscriber queues; **drop-oldest on overflow** (a slow stream client never
  backpressures ingestion — same non-blocking rule as §4.2).
- `coordinator/registry.py` — worker table (in-memory mirror + DB):
  register on `hello`, update on `ready`/`heartbeat`, mark `dead` on WS drop
  or heartbeat silence; background **sweeper task** (~1s) calls
  `requeue_expired`.
- `coordinator/queue.py` — thin depth-tracking facade over `db.py`
  (`depth()`, `oldest_queued(...)`), the single place dispatch reads jobs from.
- `coordinator/dispatcher.py` — v1 (no tiers yet): on worker `ready`, pick
  hot > warm > cold via `oldest_queued` with the ~30s skip window (§4.3),
  atomic-claim, send `Assign{bundle}` or `NoWork`. On `Output`/`Partial`:
  `append_log` + pubsub publish + `renew_lease`. On `Result`: `finish_job`
  (store result as blob if > `INLINE_CAP`, §4.6), publish terminal frame.

### 1.5 Worker: agent + local backend + envs + content cache

- `worker/content.py` — machine-wide cache: `download(url, sha256) ->
  cached_path` (httpx stream → temp → verify hash → rename into
  `content/{sha256}`; concurrent-safe via `O_EXCL`/lockfile);
  `materialize(cached_path, dest, extract: bool)` — hardlink (copy fallback),
  tar/zip extract. `godserve-fetch` CLI entrypoint wraps this (env var
  `GODSERVE_CACHE_DIR` points at the cache root).
- `worker/envs/base.py` — `class EnvProvider(Protocol): async def
  ensure(spec) -> EnvHandle` (`EnvHandle{env_key, python_bin, env_vars,
  activate()}`).
- `worker/envs/venv.py` — `VenvProvider`: if `envs/{env_key}/.ok` exists →
  warm. Else: `uv venv --python {python}`, run `setup.sh` with the venv
  activated and `godserve-fetch` on PATH, write `.ok` marker on success
  (failed builds are deleted, never half-cached). Per-key asyncio lock so
  concurrent jobs don't double-build. LRU eviction by disk budget
  (`GODSERVE_ENV_DISK_BUDGET`), never evicting envs with running jobs.
- `worker/backends/base.py` — the `Backend` protocol (§3.4) + `JobIO`
  (`emit_log(stream, data)`, `emit_partial(data)`) + `JobOutcome`.
- `worker/backends/local.py` — v1, `once` mode only: `ensure_env` →
  per-job scratch cwd → run `run.sh` with `GODSERVE_INPUTS` (JSON; if
  `blob_ref`, download via content cache first and pass a path) and
  `GODSERVE_RESULT_PATH`; stream stdout/stderr via `io.emit_log`; enforce
  `timeout_s` (kill process group); read result JSON → `JobOutcome`.
- `worker/agent.py` — the single loop (identical for every backend):
  1. connect WS (retry w/ backoff), send `hello` (id, tier from
     `GODSERVE_TIER`, `max_slots`, warm envs from disk scan);
  2. send `ready` whenever a slot is free; on `Assign` → spawn slot task:
     `backend.run(bundle, io)` where io frames go up the WS; send `Result`;
  3. heartbeat task (interval `lease_ttl / 3`);
  4. `Cancel` → kill slot task; `Shutdown`/SIGTERM → send `goodbye{drain}`,
     finish in-flight, exit.
  Backend chosen once at startup: `GODSERVE_BACKEND` env var → registry dict
  `{"local": LocalBackend, "runpod": RunpodBackend}`.

### 1.6 CLI + SDK

- `cli.py` (entrypoints via `pyproject.toml`):
  `godserve coordinator --config config.yaml`, `godserve worker --url …`,
  `godserve submit -f godserve.yaml -i '{...}' [--dir ./job] [--follow]`,
  `godserve status [job_id]`, plus the separate `godserve-fetch` entrypoint.
  `--dir` sugar: tar the directory → `POST /v1/blobs` → append the synthesized
  `godserve-fetch <blob-url> . --extract` line to `setup` (§3.1).
- `client/sdk.py` — `submit(spec|spec_id, inputs) -> job_id`,
  `result(job_id, wait=True)`, `stream(job_id) -> async iterator of frames`.

### Phase 1 acceptance (from §10-P1)

- [ ] Submit a venv job → env built once; second job with same spec → **no
  rebuild** (assert via build-log marker / timing).
- [ ] A `godserve-fetch`ed file appears **once** in the machine cache across
  two different env_keys (two specs fetching the same URL+hash).
- [ ] Logs stream live over `GET /v1/jobs/{id}/stream`; late subscriber gets
  full replay; structured result returned.
- [ ] Kill the worker mid-job (SIGKILL) → lease expires → job requeues →
  completes on worker restart; `attempt` incremented; exceeds `max_attempts`
  → `failed`.
- [ ] >256 KB inline input rejected (413); same payload accepted via
  `POST /v1/blobs` + `blob_ref`; oversized result comes back as `blob_ref`.
- [ ] Cancel: queued job → immediate; running job → process killed, state
  `canceled`.

---

## Phase 2 — Hot sessions (`serve` mode)

Goal: `init()` runs once across N consecutive jobs; hot-path overhead < 20ms.

### 2.1 `worker/serve_shim.py`

Single dependency-free module (stdlib only), importable as `godserve` via a
`PYTHONPATH` shim directory the agent injects:

```python
def serve(handler, *, init=None):
    # open fd 3; ctx = Ctx(emit=..., job_id=..., scratch_dir=..., logger=...)
    # init() once → write SessionReady
    # loop: read SessionJob → out = handler(inputs, ctx)
    #   generator handler → each yielded chunk = SessionPartial (fire-and-forget
    #   through a bounded queue + writer thread; drop-oldest when full — never
    #   blocks handler); final/returned value = SessionResult
    #   exception → SessionResult{error} (session survives; traceback to stderr)
```

`ctx.emit(chunk)` goes through the same bounded non-blocking buffer (§4.2).

### 2.2 `worker/session.py` — session manager

```python
class Session:      # one live process
    session_key: str; proc: Process; state: idle|busy; last_used: float
    async def run_job(self, job_id, inputs, io) -> JobOutcome
        # write SessionJob on fd3; relay stdout/stderr → io.emit_log(job_id-tagged),
        # SessionPartial → io.emit_partial; await SessionResult; enforce timeout_s
        # (timeout ⇒ kill session, outcome=failed)

class SessionManager:
    async def acquire(self, spec) -> Session
        # match by session_key → hot; else ensure env, spawn run.sh with fd3
        # pipe + shim on PYTHONPATH, await SessionReady (init timeout knob)
    # max_live_sessions cap (default 1): evict idle-LRU first; never evict busy
    # idle timeout task: graceful shutdown (close fd3 → process exits) — frees GPU
    # crash while busy ⇒ JobOutcome failed (coordinator requeues); respawn on demand
```

### 2.3 Integrate

- `local.py` backend: `run.mode == "serve"` → `SessionManager.run_job`;
  `"once"` → Phase-1 path unchanged.
- Agent `ready`/`heartbeat` now advertise `live_sessions` (keys of idle+busy
  sessions) alongside `warm_envs`.
- Dispatcher: full hot > warm > cold affinity (§4.3) with the skip-window knob
  (already in `oldest_queued`; now exercised by real `session_key` matches).
- `Partial` frames flow: session fd3 → agent WS → dispatcher → `job_logs`
  (stream=`partial`) + pubsub → `/stream` subscribers (§4.2).

### Phase 2 acceptance (from §10-P2)

- [ ] N consecutive serve-mode jobs, same spec → `init()` exactly **once**
  (init writes a counter to a file; assert == 1).
- [ ] Hot-path added latency < 20ms on a 1s job (measure submit→running delta
  hot vs the job's own duration).
- [ ] Idle timeout kills the session (process gone); next job respawns it.
- [ ] Session crash mid-job (handler calls `os._exit`) → job requeues, session
  respawns, retry succeeds.
- [ ] Generator handler streams partials live **while continuing to compute**;
  a stalled `/stream` client does not slow the session (assert total job time
  unaffected by an unread subscriber).
- [ ] `max_live_sessions=1` with two different specs → idle LRU evicted, no
  busy eviction.

---

## Phase 3 — Tiers + load-handling API

Goal: sustained-depth spill, hysteresis, tier-0 rescue, backend opacity proven.

### 3.1 `coordinator/load.py`

```python
@dataclass
class LoadSnapshot: depth: int; over_since: dict[int, float|None]; in_flight: dict[int, int]

class LoadPolicy(Protocol):
    def budgets(self, snap: LoadSnapshot, now: float) -> dict[int, int]   # tier→budget

class SustainedDepthPolicy(LoadPolicy):
    # per LevelConfig (§2.1):
    # active_k:  over_since[k] is not None and now - over_since[k] >= sustain_s
    #            (sticky until cleared)
    # cleared_k: depth < clear_below
    # budget_k = active_k ? min(max_inflight - in_flight[k], depth) : 0
```

Coordinator tick task (~1s): update `over_since` (set when depth first ≥
`depth_k`, clear when it drops below), recompute budgets, expose to dispatcher.
In-memory only; recomputed after restart (§5).

### 3.2 Dispatcher gating

- Tier 0 `ready` → serve immediately (never gated).
- Tier k>0 `ready` → serve only if `budget_k > 0`; decrement per assignment
  until the next tick; failed/lease-expired assignments return budget.
  Respect per-job `max_tier` in `oldest_queued`.

### 3.3 Opacity proof

Run a tier-1 worker whose backend targets a **simulated remote** — e.g.
`GODSERVE_BACKEND=local` on a second process posing as "remote", or a trivial
`http_relay` backend hitting localhost. The point: coordinator config only says
`tier: 1`; nothing coordinator-side knows what backs it.

### Phase 3 acceptance (from §10-P3)

- [ ] Hold depth ≥ `depth₁` for `sustain_s` → tier 1 starts claiming, never
  exceeding `max_inflight` concurrent.
- [ ] Burst shorter than `sustain_s` → **no** spill.
- [ ] Free a tier-0 slot while tier 1 is active → tier 0 rescues the head job;
  no double-run (atomic claim).
- [ ] Drain below `clear_below` → tier 1 stops claiming (hysteresis: no
  flapping around `depth₁`).
- [ ] Two levels: tier 2 activates only at its deeper/longer threshold.
- [ ] **Opacity check**: `grep -ri runpod godserve/coordinator/` (and any
  backend name) → no matches; coordinator code has no backend branches.

---

## Phase 4 — RunPod backend

Goal: the same suite passing with real remote capacity.

- `worker/backends/runpod.py` — v1 = **persistent pods with idle-timeout**
  (best for 1–2s model-serving; hourly billing amortized; pods keep hot
  sessions across jobs because they run the same agent/env/session code).
  Serverless variant optional, later.
  - Pod lifecycle: lease/spawn pod on first job (RunPod API via httpx), ship
    the bundle, relay logs/partials/result back through the worker's WS;
    idle-timeout scale-down.
  - Simplest composition: the pod runs the standard agent code with
    `GODSERVE_BACKEND=local`, and the runpod backend is a thin transport that
    forwards the bundle and relays frames — reusing §4 wholesale.
- Auth for the backend's remote side (shared token, backend config).
- Blob endpoint hardening: auth token on `/v1/blobs`, size limits, disk quota.

### Phase 4 acceptance (from §10-P4)

- [ ] Phase 1–3 verification suite passes with a tier-1 RunPod worker.
- [ ] Pod idle-timeout scale-down observed (pod terminated after idle window).
- [ ] Hot session survives across two jobs routed to the same pod.

---

## Explicitly deferred (design retained, do not build)

- `coordinator/prep.py` + `prepare` handling (§4.5) — frame stays in
  `protocol.py`; `/v1/prewarm` stays 501.
- `DockerProvider` (§4.1).
- S3-compatible blob backing; artifacts.
- Additional `LoadPolicy` implementations.

## Suggested repo scaffolding (before Phase 1)

- `pyproject.toml` (uv-managed): deps fastapi, uvicorn, websockets, httpx,
  aiosqlite, pydantic, pyyaml, anyio; dev deps pytest, pytest-asyncio;
  entrypoints `godserve` + `godserve-fetch`.
- `tests/` mirroring phases: `test_p1_core.py`, `test_p2_sessions.py`,
  `test_p3_load.py` — integration-style, spinning coordinator + worker(s)
  in-process on ephemeral ports; jobs are tiny scripts (sleep/echo/counter)
  so the suite runs in seconds.
