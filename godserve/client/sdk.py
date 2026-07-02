"""Client SDK: submit / result / stream (PLAN §1.6)."""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

import httpx
import websockets

from ..models import JobSpec


class Client:
    def __init__(self, base_url: str):
        self._base = base_url.rstrip("/")

    def _ws_base(self) -> str:
        if self._base.startswith("https://"):
            return "wss://" + self._base[len("https://"):]
        if self._base.startswith("http://"):
            return "ws://" + self._base[len("http://"):]
        return self._base

    async def register_spec(self, spec: JobSpec) -> str:
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{self._base}/v1/specs", json={"spec": spec.model_dump()})
            r.raise_for_status()
            return r.json()["spec_id"]

    async def submit(self, spec: JobSpec | str, inputs: dict, overrides: dict | None = None) -> str:
        body: dict = {"inputs": inputs}
        if isinstance(spec, str):
            body["spec_id"] = spec
        else:
            body["spec"] = spec.model_dump()
        if overrides:
            body["overrides"] = overrides
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{self._base}/v1/jobs", json=body)
            r.raise_for_status()
            return r.json()["job_id"]

    async def upload_blob(self, data: bytes) -> dict:
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{self._base}/v1/blobs", content=data)
            r.raise_for_status()
            return r.json()

    async def status(self, job_id: str) -> dict:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{self._base}/v1/jobs/{job_id}")
            r.raise_for_status()
            return r.json()

    async def result(self, job_id: str, wait: bool = True, poll_s: float = 0.1, timeout_s: float = 60) -> dict:
        if not wait:
            return await self.status(job_id)
        deadline = asyncio.get_event_loop().time() + timeout_s
        while True:
            st = await self.status(job_id)
            if st["state"] in ("succeeded", "failed", "canceled"):
                return st
            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(f"job {job_id} did not finish within {timeout_s}s")
            await asyncio.sleep(poll_s)

    async def stream(self, job_id: str) -> AsyncIterator[dict]:
        import json

        uri = f"{self._ws_base()}/v1/jobs/{job_id}/stream"
        async with websockets.connect(uri) as ws:
            async for raw in ws:
                frame = json.loads(raw)
                yield frame
                if frame.get("t") == "result":
                    return
