"""Core pydantic types (§3.1, PLAN §1.1).

Content-address keys are computed as properties on the spec so they are always
consistent with the stored bytes: any change to python/setup/run flips the key.
"""

from __future__ import annotations

import hashlib
from typing import Literal

from pydantic import BaseModel, Field


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class BlobRef(BaseModel):
    """Reference to an out-of-band payload (§4.6)."""

    blob_ref: str  # blob_id (sha256) or external URL
    sha256: str | None = None
    size: int | None = None


class RunConfig(BaseModel):
    script: str  # contents of run.sh (inlined at registration)
    mode: Literal["serve", "once"]


class JobDefaults(BaseModel):
    timeout_s: int = 300
    max_attempts: int = 2
    max_tier: int | None = None


class JobSpec(BaseModel):
    name: str | None = None
    python: str  # e.g. "3.13"
    setup: str  # contents of setup.sh (inlined)
    run: RunConfig
    defaults: JobDefaults = Field(default_factory=JobDefaults)

    @property
    def env_key(self) -> str:
        return _sha256(self.python + "\0" + self.setup)

    @property
    def session_key(self) -> str:
        return _sha256(self.env_key + "\0" + self.run.script + "\0" + self.run.mode)

    @property
    def spec_id(self) -> str:
        # Canonical JSON of the whole spec, key-sorted, no incidental whitespace.
        return _sha256(self.model_dump_json())


class JobBundle(BaseModel):
    """Self-contained, location-independent unit shipped to a worker (§3.3)."""

    job_id: str
    spec: JobSpec  # fully resolved — no coordinator references
    inputs: dict | BlobRef
    timeout_s: int
    lease_ttl_s: int


class LevelConfig(BaseModel):
    """One overflow tier's fill level (§2.1). Defined now, used in Phase 3."""

    tier: int
    depth: int
    sustain_s: float
    clear_below: int
    max_inflight: int
