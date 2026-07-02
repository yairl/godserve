"""WS frames + session IPC frames (PLAN §1.2, §4.2).

All frames are a discriminated union over the ``t`` tag. ``parse_frame(bytes)``
decodes; ``frame.dump()`` encodes to bytes for the wire.
"""

from __future__ import annotations

import json
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter

from .models import JobBundle, JobSpec

INLINE_CAP = 256 * 1024  # §4.6 — inline payload cap in bytes


class _Frame(BaseModel):
    def dump(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


# --- worker → coordinator -------------------------------------------------


class Hello(_Frame):
    t: Literal["hello"] = "hello"
    worker_id: str
    tier: int
    max_slots: int
    warm_envs: list[str] = Field(default_factory=list)
    live_sessions: list[str] = Field(default_factory=list)


class Ready(_Frame):
    t: Literal["ready"] = "ready"
    slots_free: int
    warm_envs: list[str] = Field(default_factory=list)
    live_sessions: list[str] = Field(default_factory=list)


class Output(_Frame):
    t: Literal["output"] = "output"
    job_id: str
    stream: Literal["stdout", "stderr"]
    data: str


class Partial(_Frame):
    t: Literal["partial"] = "partial"
    job_id: str
    data: str


class Heartbeat(_Frame):
    t: Literal["heartbeat"] = "heartbeat"
    running: list[str] = Field(default_factory=list)


class Result(_Frame):
    t: Literal["result"] = "result"
    job_id: str
    status: Literal["succeeded", "failed", "canceled"]
    exit_code: int | None = None
    result: dict | None = None
    error: str | None = None


class Goodbye(_Frame):
    t: Literal["goodbye"] = "goodbye"
    drain: bool = False


# --- coordinator → worker -------------------------------------------------


class Assign(_Frame):
    t: Literal["assign"] = "assign"
    bundle: JobBundle


class NoWork(_Frame):
    t: Literal["no_work"] = "no_work"


class Cancel(_Frame):
    t: Literal["cancel"] = "cancel"
    job_id: str


class Shutdown(_Frame):
    t: Literal["shutdown"] = "shutdown"


class Prepare(_Frame):
    """Ahead-of-time prep hint (§4.5). DEFINED BUT UNUSED — deferred."""

    t: Literal["prepare"] = "prepare"
    spec: JobSpec
    warm: Literal["env", "session"]


# --- session IPC (fd 3, newline-delimited JSON, §4.2) ---------------------


class SessionReady(_Frame):
    t: Literal["session_ready"] = "session_ready"


class SessionJob(_Frame):
    t: Literal["session_job"] = "session_job"
    job_id: str
    inputs: dict


class SessionPartial(_Frame):
    t: Literal["session_partial"] = "session_partial"
    job_id: str
    data: str


class SessionResult(_Frame):
    t: Literal["session_result"] = "session_result"
    job_id: str
    result: dict | None = None
    error: str | None = None


Frame = Annotated[
    Union[
        Hello,
        Ready,
        Output,
        Partial,
        Heartbeat,
        Result,
        Goodbye,
        Assign,
        NoWork,
        Cancel,
        Shutdown,
        Prepare,
        SessionReady,
        SessionJob,
        SessionPartial,
        SessionResult,
    ],
    Field(discriminator="t"),
]

_ADAPTER: TypeAdapter[Frame] = TypeAdapter(Frame)


def parse_frame(raw: bytes | str | dict) -> Frame:
    if isinstance(raw, (bytes, str)):
        raw = json.loads(raw)
    return _ADAPTER.validate_python(raw)
