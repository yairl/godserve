"""A trivial zero-arg backend used to prove import-path resolution in tests."""

from __future__ import annotations

from godserve.models import JobBundle
from godserve.worker.backends.base import JobIO, JobOutcome


class DummyBackend:
    async def run(self, bundle: JobBundle, io: JobIO) -> JobOutcome:
        return JobOutcome(status="succeeded", exit_code=0, result={})
