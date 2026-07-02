"""Shared fixtures: an in-process coordinator + worker on ephemeral ports."""

from __future__ import annotations

import asyncio
import socket
import time
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import uvicorn

from godserve.coordinator.app import create_app
from godserve.worker.agent import Agent


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class RunningServer:
    def __init__(self, app, port: int):
        self._config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        self._server = uvicorn.Server(self._config)
        self.port = port
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._server.serve())
        deadline = time.time() + 10
        while not self._server.started:
            if time.time() > deadline:
                raise RuntimeError("server did not start")
            await asyncio.sleep(0.02)

    async def stop(self) -> None:
        self._server.should_exit = True
        if self._task is not None:
            await self._task

    @property
    def http_base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def ws_worker(self) -> str:
        return f"ws://127.0.0.1:{self.port}/v1/worker"


@pytest.fixture
def levels():
    """Load-handling fill levels for the coordinator. P1/P2 override nothing and
    get None (no gating); P3 overrides this fixture to inject LevelConfigs."""
    return None


@pytest_asyncio.fixture
async def server(tmp_path, levels):
    db_path = str(tmp_path / "state.sqlite3")
    blob_root = str(tmp_path / "coord-data")
    app = create_app(db_path, blob_root, levels)
    srv = RunningServer(app, _free_port())
    await srv.start()
    try:
        yield srv
    finally:
        await srv.stop()


class WorkerHandle:
    def __init__(self, agent: Agent):
        self.agent = agent
        self.task: asyncio.Task | None = None

    def start(self) -> None:
        self.task = asyncio.create_task(self.agent.run_forever())

    async def stop(self) -> None:
        await self.agent.stop()
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    def kill(self) -> None:
        """Simulate a hard SIGKILL: drop the WS without goodbye."""
        if self.task is not None:
            self.task.cancel()


@pytest_asyncio.fixture
async def make_worker(server, tmp_path):
    handles: list[WorkerHandle] = []
    counter = {"n": 0}

    def _make(work_root: str | None = None, max_slots: int = 1, tier: int = 0):
        counter["n"] += 1
        root = work_root or str(tmp_path / f"worker-{counter['n']}")
        agent = Agent(server.ws_worker, root, max_slots=max_slots)
        agent._tier = tier
        h = WorkerHandle(agent)
        h.start()
        handles.append(h)
        return h

    yield _make

    for h in handles:
        await h.stop()


@pytest_asyncio.fixture
async def client(server):
    async with httpx.AsyncClient(base_url=server.http_base, timeout=30) as c:
        yield c


async def wait_state(client, job_id, states, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = await client.get(f"/v1/jobs/{job_id}")
        st = r.json()
        if st["state"] in states:
            return st
        await asyncio.sleep(0.1)
    raise TimeoutError(f"job {job_id} not in {states}; last={st}")
