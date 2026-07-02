"""Backend protocol + JobIO + JobOutcome (§3.4, PLAN §1.5)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, Literal, Protocol

from ...models import JobBundle


@dataclass
class JobOutcome:
    status: Literal["succeeded", "failed", "canceled"]
    exit_code: int | None = None
    result: dict | None = None
    error: str | None = None


class JobIO:
    """Streaming sink handed to a backend; frames flow up the worker WS.

    Emission is fire-and-forget — a slow reader upstream must never backpressure
    the running handler (§4.2). The callbacks here schedule sends and return.
    """

    def __init__(
        self,
        job_id: str,
        emit_log: Callable[[str, str, str], None],
        emit_partial: Callable[[str, str], None],
    ):
        self.job_id = job_id
        self._emit_log = emit_log
        self._emit_partial = emit_partial

    def emit_log(self, stream: str, data: str) -> None:
        self._emit_log(self.job_id, stream, data)

    def emit_partial(self, data: str) -> None:
        self._emit_partial(self.job_id, data)


class Backend(Protocol):
    async def run(self, bundle: JobBundle, io: JobIO) -> JobOutcome:
        ...
