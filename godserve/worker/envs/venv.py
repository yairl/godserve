"""VenvProvider (uv) — v1 default env provider (PLAN §1.5, §4.1).

An env lives at ``envs/{env_key}/`` and is warm iff its ``.ok`` marker exists.
Building runs ``uv venv`` then ``setup.sh`` with the venv activated and
``godserve-fetch`` on PATH; a failed build is deleted (never half-cached). A
per-key asyncio lock stops concurrent jobs double-building. LRU eviction by
``GODSERVE_ENV_DISK_BUDGET`` never evicts an env with running jobs.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

from ...models import JobSpec
from .base import EnvHandle

log = logging.getLogger(__name__)


class VenvProvider:
    def __init__(self, root: str):
        self._root = Path(root)
        self._envs_dir = self._root / "envs"
        self._envs_dir.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._running: dict[str, int] = defaultdict(int)
        self._last_used: dict[str, float] = {}

    def _env_dir(self, env_key: str) -> Path:
        return self._envs_dir / env_key

    def _ok_marker(self, env_key: str) -> Path:
        return self._env_dir(env_key) / ".ok"

    def warm_keys(self) -> list[str]:
        return [
            p.name
            for p in self._envs_dir.iterdir()
            if p.is_dir() and (p / ".ok").exists()
        ]

    async def ensure(self, spec: JobSpec) -> EnvHandle:
        env_key = spec.env_key
        async with self._locks[env_key]:
            if not self._ok_marker(env_key).exists():
                await self._build(spec, env_key)
            self._last_used[env_key] = time.time()
        self._evict_if_needed(protect=env_key)
        return self._handle(env_key)

    def _handle(self, env_key: str) -> EnvHandle:
        venv = self._env_dir(env_key) / "venv"
        bin_dir = venv / ("Scripts" if os.name == "nt" else "bin")
        env_vars = {
            "VIRTUAL_ENV": str(venv),
            "PATH": f"{bin_dir}{os.pathsep}{_fetch_bin_dir()}{os.pathsep}{os.environ.get('PATH', '')}",
        }
        return EnvHandle(
            env_key=env_key,
            python_bin=str(bin_dir / "python"),
            env_dir=str(self._env_dir(env_key)),
            env_vars=env_vars,
        )

    async def _build(self, spec: JobSpec, env_key: str) -> None:
        env_dir = self._env_dir(env_key)
        if env_dir.exists():
            shutil.rmtree(env_dir, ignore_errors=True)
        env_dir.mkdir(parents=True)
        venv = env_dir / "venv"

        log.info("building env %s (python=%s)", env_key[:12], spec.python)
        await self._run(
            ["uv", "venv", "--python", spec.python, str(venv)],
            cwd=env_dir,
            env=os.environ.copy(),
        )

        bin_dir = venv / ("Scripts" if os.name == "nt" else "bin")
        build_env = os.environ.copy()
        build_env["VIRTUAL_ENV"] = str(venv)
        build_env["PATH"] = (
            f"{bin_dir}{os.pathsep}{_fetch_bin_dir()}{os.pathsep}{build_env.get('PATH', '')}"
        )

        setup_path = env_dir / "setup.sh"
        setup_path.write_text(spec.setup)
        try:
            await self._run(["bash", str(setup_path)], cwd=env_dir, env=build_env)
        except Exception:
            shutil.rmtree(env_dir, ignore_errors=True)
            raise

        self._ok_marker(env_key).write_text(str(time.time()))

    @staticmethod
    async def _run(cmd: list[str], cwd: Path, env: dict[str, str]) -> None:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(cwd),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        if proc.returncode != 0:
            text = out.decode("utf-8", "replace") if out else ""
            raise RuntimeError(f"command {cmd[0]} failed (rc={proc.returncode}): {text[-2000:]}")

    # --- running-job accounting + LRU eviction ----------------------------

    def acquire(self, env_key: str) -> None:
        self._running[env_key] += 1
        self._last_used[env_key] = time.time()

    def release(self, env_key: str) -> None:
        if self._running[env_key] > 0:
            self._running[env_key] -= 1
        self._last_used[env_key] = time.time()

    def _evict_if_needed(self, protect: str) -> None:
        budget = os.environ.get("GODSERVE_ENV_DISK_BUDGET")
        if not budget:
            return
        try:
            budget_bytes = int(budget)
        except ValueError:
            return
        while self._total_size() > budget_bytes:
            victim = self._lru_evictable(protect)
            if victim is None:
                return
            shutil.rmtree(self._env_dir(victim), ignore_errors=True)
            self._last_used.pop(victim, None)
            log.info("evicted env %s (disk budget)", victim[:12])

    def _total_size(self) -> int:
        total = 0
        for p in self._envs_dir.rglob("*"):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except FileNotFoundError:
                    pass
        return total

    def _lru_evictable(self, protect: str) -> str | None:
        candidates = [
            k
            for k in self.warm_keys()
            if k != protect and self._running.get(k, 0) == 0
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda k: self._last_used.get(k, 0.0))


def _fetch_bin_dir() -> str:
    """Directory containing the godserve-fetch executable, put on setup.sh PATH."""
    return str(Path(sys.executable).parent)
