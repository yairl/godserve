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
    # A session crash mid-job is a partial worker failure, not a clean job
    # failure: the agent suppresses the terminal Result so the lease lapses and
    # the coordinator's sweeper requeues the job (ARCH §4.2). Never set for a
    # handler exception (that is a real, terminal failure with the session alive).
    requeue: bool = False


class JobIO:
    """Streaming sink handed to a backend; frames flow up the worker WS.

    Emission is lossless and ordered — it awaits a bounded FIFO queue and may
    block the running handler (backpressure) rather than dropping (§4.2). The
    callbacks here enqueue sends and return once accepted.
    """

    def __init__(
        self,
        job_id: str,
        emit_log: Callable[[str, str, str], Awaitable[None]],
        emit_partial: Callable[[str, str], Awaitable[None]],
    ):
        self.job_id = job_id
        self._emit_log = emit_log
        self._emit_partial = emit_partial

    async def emit_log(self, stream: str, data: str) -> None:
        await self._emit_log(self.job_id, stream, data)

    async def emit_partial(self, data: str) -> None:
        await self._emit_partial(self.job_id, data)


class Backend(Protocol):
    async def run(self, bundle: JobBundle, io: JobIO) -> JobOutcome:
        ...

    def live_sessions(self) -> list[str]:
        """session_keys of live (idle+busy) sessions; ``[]`` if none.

        Parallels the env layer's ``warm_keys()`` pull."""
        ...

    async def shutdown(self) -> None:
        """Gracefully tear down any live sessions on worker stop/drain (frees
        GPUs; leaves no child process into interpreter shutdown)."""
        ...
