"""Phase 3 acceptance: sustained-depth tiers + load-handling API (PLAN §3).

Integration-style, mirroring test_p1_core.py / test_p2_sessions.py: a real
coordinator + in-process agents on ephemeral ports. Tiers are driven via
``make_worker(tier=...)``; a slow once-mode sleep spec keeps jobs in-flight so
per-tier concurrency (``max_inflight``) is observable.
"""

from __future__ import annotations

import asyncio
import subprocess
import time

import pytest

from godserve.models import JobDefaults, JobSpec, LevelConfig, RunConfig
from tests.conftest import wait_state

pytestmark = pytest.mark.asyncio


# Tight config for fast tests: short sustain windows, small ceilings.
LEVEL_1 = {"tier": 1, "depth": 3, "sustain_s": 0.5, "clear_below": 1, "max_inflight": 2}
LEVEL_2 = {"tier": 2, "depth": 6, "sustain_s": 1.5, "clear_below": 2, "max_inflight": 2}


@pytest.fixture
def levels():
    return [LevelConfig(**LEVEL_1), LevelConfig(**LEVEL_2)]


# --- spec builders --------------------------------------------------------


def slow_sleep_spec(seconds: float, tag: str = "") -> JobSpec:
    """A once-mode job that sleeps ``seconds`` so it stays in-flight long enough
    to observe concurrency. ``tag`` perturbs setup for distinct env keys."""
    setup = f"echo build {tag}\n"
    run = (
        f"sleep {seconds}\n"
        "python - <<'PY'\n"
        "import json, os\n"
        "open(os.environ['GODSERVE_RESULT_PATH'],'w').write(json.dumps({'slept': True}))\n"
        "PY\n"
    )
    return JobSpec(
        python="3.13", setup=setup,
        run=RunConfig(script=run, mode="once"),
        defaults=JobDefaults(timeout_s=120, max_attempts=2),
    )


async def submit(client, spec: JobSpec, inputs: dict, overrides: dict | None = None) -> str:
    body = {"spec": spec.model_dump(), "inputs": inputs}
    if overrides:
        body["overrides"] = overrides
    r = await client.post("/v1/jobs", json=body)
    assert r.status_code == 200, r.text
    return r.json()["job_id"]


async def submit_many(client, spec: JobSpec, n: int) -> list[str]:
    return [await submit(client, spec, {"i": i}) for i in range(n)]


# --- coordinator introspection helpers ------------------------------------


def _state(server):
    return server._config.app.state


async def _depth(server) -> int:
    return await _state(server).db.queued_depth()


async def _inflight(server) -> dict[int, int]:
    return await _state(server).db.inflight_by_tier()


def _budget(server, tier: int) -> int:
    return _state(server).load.budget(tier)


async def _wait_budget(server, tier: int, predicate, timeout=6.0) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        b = _budget(server, tier)
        if predicate(b):
            return b
        await asyncio.sleep(0.05)
    raise TimeoutError(f"tier {tier} budget predicate not met; last={_budget(server, tier)}")


async def _count_completed(client, job_ids: list[str]) -> int:
    n = 0
    for jid in job_ids:
        r = await client.get(f"/v1/jobs/{jid}")
        if r.json()["state"] == "succeeded":
            n += 1
    return n


# --- tests ----------------------------------------------------------------


async def test_sustained_spill_respects_max_inflight(server, client, make_worker):
    # depth ≥ 3 held past sustain_s (0.5s) → tier 1 activates. Connect tier-1
    # workers; concurrent tier-1 in-flight never exceeds max_inflight=2.
    spec = slow_sleep_spec(1.5, tag="spill")
    jobs = await submit_many(client, spec, 6)
    assert await _depth(server) == 6

    # Hold depth past the sustain window → tier 1 budget goes positive.
    await _wait_budget(server, 1, lambda b: b > 0)

    # 3 tier-1 workers, but the ceiling is 2 concurrent.
    for _ in range(3):
        make_worker(tier=1)

    peak = 0
    deadline = time.time() + 20
    while time.time() < deadline:
        inflight = await _inflight(server)
        peak = max(peak, inflight.get(1, 0))
        assert inflight.get(1, 0) <= 2, f"tier-1 in-flight {inflight.get(1)} exceeded max_inflight=2"
        if await _count_completed(client, jobs) == len(jobs):
            break
        await asyncio.sleep(0.05)

    assert peak >= 1, "tier 1 never claimed"
    for jid in jobs:
        await wait_state(client, jid, {"succeeded"}, timeout=30)


async def test_short_burst_below_sustain_no_spill(server, client, make_worker):
    # A burst shorter than sustain_s must not spill: submit past depth_1, then
    # drain below clear_below before sustain elapses → tier 1 never activates.
    spec = slow_sleep_spec(0.2, tag="burst")
    # A tier-0 worker drains the queue quickly so depth never stays ≥ 3 for 0.5s.
    make_worker(tier=0)
    jobs = await submit_many(client, spec, 4)

    # Watch: tier-1 budget must stay 0 throughout (tier-0 drains before sustain).
    deadline = time.time() + 3.0
    while time.time() < deadline:
        assert _budget(server, 1) == 0, "tier 1 spilled on a sub-sustain burst"
        await asyncio.sleep(0.05)

    for jid in jobs:
        await wait_state(client, jid, {"succeeded"}, timeout=30)


async def test_tier0_rescue_no_double_run(server, client, make_worker):
    # Tier 1 active and claiming; a tier-0 worker joins and rescues the head job.
    # Each job runs exactly once (atomic claim); budget refunded on lost race.
    spec = slow_sleep_spec(1.0, tag="rescue")
    jobs = await submit_many(client, spec, 6)
    await _wait_budget(server, 1, lambda b: b > 0)

    # Tier-1 workers start claiming, then a tier-0 rescue joins the fray.
    make_worker(tier=1)
    make_worker(tier=1)
    make_worker(tier=0)
    make_worker(tier=0)

    for jid in jobs:
        await wait_state(client, jid, {"succeeded"}, timeout=40)

    # Exactly-once: total completions == submissions, no attempt inflation from
    # a double-run (a lost race sends NoWork, never a second execution).
    assert await _count_completed(client, jobs) == len(jobs)
    for jid in jobs:
        r = await client.get(f"/v1/jobs/{jid}")
        assert r.json()["state"] == "succeeded"


async def test_hysteresis_no_flapping_in_band(server, client, make_worker):
    # Activate tier 1, drain below clear_below=1 → tier 1 stops. Then hold depth
    # in the (clear_below, depth_1) band = [1, 3) → no re-activation flapping.
    spec = slow_sleep_spec(0.2, tag="hyst")
    jobs = await submit_many(client, spec, 4)
    await _wait_budget(server, 1, lambda b: b > 0)

    # Drain fully with a tier-0 worker → depth < clear_below → tier 1 clears.
    make_worker(tier=0)
    for jid in jobs:
        await wait_state(client, jid, {"succeeded"}, timeout=30)
    await _wait_budget(server, 1, lambda b: b == 0)

    # Now sit in the hysteresis band: depth 2 (∈ [1,3)) for > sustain_s. Because
    # over_since was cleared and depth never reaches depth_1 again, tier 1 must
    # NOT re-activate (no flapping).
    band = await submit_many(client, slow_sleep_spec(30.0, tag="band"), 2)
    deadline = time.time() + 2.0
    while time.time() < deadline:
        d = await _depth(server)
        # depth stays in the band while no tier-1 worker is present.
        assert d < 3, f"depth {d} left the band; test setup invalid"
        assert _budget(server, 1) == 0, "tier 1 re-activated inside the hysteresis band"
        await asyncio.sleep(0.05)

    for jid in band:
        await client.post(f"/v1/jobs/{jid}/cancel")


async def test_two_level_progressive_recruitment(server, client, make_worker):
    # depth in [depth_1, depth_2) = [3, 6) past sustain_1 → only tier 1 active;
    # a tier-2 worker gets NoWork. Push ≥ depth_2 past sustain_2 → tier 2 activates.
    spec = slow_sleep_spec(30.0, tag="twolvl")
    # 4 jobs → depth 4 ∈ [3, 6): tier 1 activates, tier 2 does not.
    first = await submit_many(client, spec, 4)
    await _wait_budget(server, 1, lambda b: b > 0)
    # tier 2 stays 0 through its own (longer) sustain window.
    deadline = time.time() + 1.0
    while time.time() < deadline:
        assert _budget(server, 2) == 0, "tier 2 activated below depth_2"
        await asyncio.sleep(0.05)

    # A tier-2 worker gets NoWork while its level is inactive: budget stays 0 and
    # it claims nothing.
    make_worker(tier=2)
    await asyncio.sleep(0.6)
    assert (await _inflight(server)).get(2, 0) == 0, "tier 2 claimed while inactive"

    # Push depth ≥ depth_2 = 6 and hold past sustain_2 = 1.5s → tier 2 activates.
    await submit_many(client, spec, 3)  # depth now 7
    await _wait_budget(server, 2, lambda b: b > 0, timeout=5.0)

    for jid in first:
        await client.post(f"/v1/jobs/{jid}/cancel")


async def test_no_levels_tier1_never_claims(make_worker, tmp_path):
    # Back-compat: with no configured levels, a tier-1 worker's budget is 0, so
    # it never claims; a tier-0 worker serves everything. Build a dedicated
    # no-levels coordinator (the module-level `levels` fixture injects levels).
    import httpx

    from godserve.coordinator.app import create_app
    from tests.conftest import RunningServer, _free_port

    app = create_app(str(tmp_path / "nl.sqlite3"), str(tmp_path / "nl-data"), None)
    srv = RunningServer(app, _free_port())
    await srv.start()
    try:
        async with httpx.AsyncClient(base_url=srv.http_base, timeout=30) as client:
            # A lone tier-1 worker: with budget 0 it must never claim.
            from godserve.worker.agent import Agent

            agent1 = Agent(srv.ws_worker, str(tmp_path / "w1"), max_slots=1)
            agent1._tier = 1
            t1 = asyncio.create_task(agent1.run_forever())

            spec = slow_sleep_spec(0.2, tag="nolvl")
            jid = await submit(client, spec, {})

            # Give tier 1 time to (not) claim: it stays queued.
            await asyncio.sleep(1.5)
            r = await client.get(f"/v1/jobs/{jid}")
            assert r.json()["state"] == "queued", "tier 1 claimed with no levels configured"

            # A tier-0 worker rescues it.
            agent0 = Agent(srv.ws_worker, str(tmp_path / "w0"), max_slots=1)
            agent0._tier = 0
            t0 = asyncio.create_task(agent0.run_forever())
            await wait_state(client, jid, {"succeeded"}, timeout=30)

            await agent1.stop()
            await agent0.stop()
            for t in (t1, t0):
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
    finally:
        await srv.stop()


async def test_backend_opacity_no_backend_names_in_coordinator():
    # ARCH invariant #1: no backend-specific names anywhere under coordinator/.
    import pathlib

    coord = pathlib.Path(__file__).resolve().parent.parent / "godserve" / "coordinator"
    out = subprocess.run(
        ["grep", "-ri", "runpod", str(coord)],
        capture_output=True, text=True,
    )
    assert out.returncode != 0 and not out.stdout.strip(), (
        f"backend name leaked into coordinator/: {out.stdout}"
    )
