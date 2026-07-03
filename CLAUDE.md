# godserve

## What this project is

**godserve** provides N-tier job servicing: clients submit into a single FIFO
queue; workers of different cost tiers pull and run jobs. Overflow tiers
activate only under sustained queue depth; tier 0 is never gated. One worker
structure everywhere — workers always run the built-in local executor;
`GODSERVE_BACKEND` is an opaque job-visible label (inherited by job
subprocesses), not an execution selector. The coordinator has **no concept of
local vs remote, only tiering**. Hot sessions keep prep work (e.g. models in
GPU) alive across consecutive 1–2s jobs.

## Authoritative documents

- **`ARCH.md`** — the architecture reference (design is converged; §-references
  elsewhere point here).
- **`PLAN.md`** — the phased implementation plan with interfaces, per-module
  responsibilities, and per-phase acceptance criteria.

Work follows PLAN.md's phases. Do not reopen settled design decisions (single
queue, no WFQ, two self-contained scripts, backend opacity) without explicit
user direction — route such proposals through the `visionary` agent.

Stack: Python 3.13, FastAPI/uvicorn, websockets, httpx, aiosqlite, pydantic,
anyio; `uv` for env builds. Async throughout. Trusted execution — no sandbox
in v1.

## Hard invariants (enforced by reviewer)

1. **Backend opacity**: no backend-specific names or branches anywhere under
   `godserve/coordinator/` (`grep -ri runpod godserve/coordinator/` must be empty).
2. **256 KB inline cap** on `inputs`/`result`/stream chunks; larger payloads go
   through blobs (`ARCH.md` §4.6).
3. **Non-blocking streaming**: partial/log emission must never backpressure a
   running handler or session (bounded buffers, drop-oldest).
4. **Atomic claim**: job state transitions from `queued` happen only via the
   single `claim_job` UPDATE; never bypass it.
5. **Deferred features stay deferred**: ahead-of-time prep (`prepare`,
   `/v1/prewarm`), DockerProvider, S3 blobs — protocol slots exist, no logic.

## Agent-Based Workflow

This project uses five specialized subagents in `.claude/agents/`. They are the
canonical source of truth for how work is done here.

| Agent       | Purpose                                                            |
|-------------|--------------------------------------------------------------------|
| `visionary` | Strategic alignment for direction-changing decisions               |
| `architect` | Technical design when components/boundaries/dependencies change   |
| `coder`     | Implementation following project conventions                       |
| `reviewer`  | Final quality gate — NO HACK, NO OVER-ENGINEERING                 |
| `manager`   | Completion confirmation and credit attribution                     |

### Workflow

For any non-trivial change, dispatch the subagents in this order, skipping any
whose "When to Invoke" criteria don't apply:

1. **visionary** — only for proposals that change product direction or reopen
   settled design decisions. Most PLAN.md-phase work skips this.
2. **architect** — when adding components, changing boundaries/interfaces, or
   introducing dependencies beyond the declared stack. Implementing a module
   exactly as specified in PLAN.md does **not** require architect.
3. **coder** — for all implementation work.
4. **reviewer** — mandatory before declaring any change done. Must return
   ✅ APPROVED.
5. **manager** — only after reviewer approves. Produces the completion summary.

If reviewer requests changes, push back to architect or coder as indicated and
re-run the affected phases.

### Ensuring the right agent runs

- **Trust the descriptions.** Each subagent's frontmatter includes
  `PROACTIVELY` / `MUST BE USED` triggers so Claude Code auto-dispatches them.
  Don't paraphrase their work inline — delegate.
- **reviewer is non-optional.** No change is complete without a `REVIEW` block
  returning ✅ APPROVED. If you're about to report "done" without one, stop and
  invoke reviewer.
- **manager closes every workflow.** After reviewer approves, invoke manager to
  emit the completion block.
- **One agent at a time.** Wait for each subagent's output block before
  dispatching the next; later agents depend on the previous block as input.
- **When unsure which agent applies**, default to the more senior one
  (visionary > architect > coder) and let its decision framework decide whether
  to proceed or downscope.
- **Manual override.** You can always force a specific agent with phrasing like
  "use the reviewer subagent on this change".

Run `/agents` to confirm all five subagents are registered.

## Naming discipline

- Package/module: `godserve`. CLI entrypoints: `godserve` and `godserve-fetch`
  (defined in `pyproject.toml`) — no other executables.
- Env vars use the `GODSERVE_` prefix exclusively (`GODSERVE_BACKEND`,
  `GODSERVE_TIER`, `GODSERVE_CACHE_DIR`, `GODSERVE_ENV_DISK_BUDGET`,
  `GODSERVE_INPUTS`, `GODSERVE_RESULT_PATH`).
- Content-address keys are named exactly `env_key`, `session_key`, `spec_id`,
  `blob_id` everywhere (code, docs, DB) — never synonyms like `env_hash`.
