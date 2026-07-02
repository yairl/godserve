"""EnvProvider protocol + EnvHandle (PLAN §1.5, §4.1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from ...models import JobSpec


@dataclass
class EnvHandle:
    env_key: str
    python_bin: str
    env_dir: str
    env_vars: dict[str, str] = field(default_factory=dict)


class EnvProvider(Protocol):
    async def ensure(self, spec: JobSpec) -> EnvHandle:
        """Return a warm EnvHandle for ``spec``, building it if needed."""
        ...

    def warm_keys(self) -> list[str]:
        """env_keys currently warm on disk (advertised in hello/ready)."""
        ...
