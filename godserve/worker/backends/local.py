"""Local backend — `once` mode only for Phase 1 (PLAN §1.5, §4.2).

Ensures the env, creates a per-job scratch cwd, runs ``run.sh`` with
``GODSERVE_INPUTS`` (JSON; a blob_ref is downloaded via the content cache and
passed as a path) and ``GODSERVE_RESULT_PATH``, streams stdout/stderr through
``io.emit_log``, enforces ``timeout_s`` by killing the process GROUP, and reads
the result JSON.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import signal
import tempfile
from pathlib import Path

from ...models import BlobRef, JobBundle
from ..content import download, materialize
from ..envs.venv import VenvProvider
from ..session import SessionManager
from .base import JobIO, JobOutcome

log = logging.getLogger(__name__)


class LocalBackend:
    def __init__(self, provider: VenvProvider, scratch_root: str):
        self._provider = provider
        self._scratch_root = Path(scratch_root)
        self._scratch_root.mkdir(parents=True, exist_ok=True)
        shim_dir = self._materialize_shim(self._scratch_root.parent)
        # Persistent (per-backend = per-agent) so sessions survive run() calls.
        self._sessions = SessionManager(provider, str(self._scratch_root), shim_dir)

    @staticmethod
    def _materialize_shim(work_root: Path) -> str:
        """Copy serve_shim.py into a PYTHONPATH shim dir as ``godserve/__init__.py``
        so serve-mode venvs can ``from godserve import serve`` without godserve
        installed (§4.2). The shim is stdlib-only, so the standalone copy works."""
        shim_pkg = work_root / "shim" / "godserve"
        shim_pkg.mkdir(parents=True, exist_ok=True)
        src = Path(__file__).resolve().parent.parent / "serve_shim.py"
        shutil.copyfile(src, shim_pkg / "__init__.py")
        return str(work_root / "shim")

    def live_sessions(self) -> list[str]:
        return self._sessions.live_sessions()

    async def shutdown(self) -> None:
        await self._sessions.shutdown()

    async def run(self, bundle: JobBundle, io: JobIO) -> JobOutcome:
        if bundle.spec.run.mode == "serve":
            return await self._sessions.run_job(bundle, io)

        env_key = bundle.spec.env_key
        try:
            handle = await self._provider.ensure(bundle.spec)
        except Exception as exc:
            log.error("env build failed for job %s: %s", bundle.job_id, exc)
            return JobOutcome(status="failed", error=f"env build failed: {exc}")

        self._provider.acquire(env_key)
        try:
            return await self._run_once(bundle, io, handle)
        finally:
            self._provider.release(env_key)

    async def _run_once(self, bundle: JobBundle, io: JobIO, handle) -> JobOutcome:
        scratch = Path(tempfile.mkdtemp(dir=self._scratch_root, prefix=f"{bundle.job_id}-"))
        result_path = scratch / "result.json"

        try:
            inputs_env = await self._resolve_inputs(bundle.inputs)
        except Exception as exc:
            log.error("input resolution failed for job %s: %s", bundle.job_id, exc)
            return JobOutcome(status="failed", error=f"input error: {exc}")

        run_path = scratch / "run.sh"
        run_path.write_text(bundle.spec.run.script)

        env = os.environ.copy()
        env.update(handle.env_vars)
        env["GODSERVE_INPUTS"] = inputs_env
        env["GODSERVE_RESULT_PATH"] = str(result_path)

        proc = await asyncio.create_subprocess_exec(
            "bash",
            str(run_path),
            cwd=str(scratch),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,  # own process group → kill the whole tree
        )

        async def pump(reader, stream_name: str) -> None:
            while True:
                line = await reader.readline()
                if not line:
                    break
                io.emit_log(stream_name, line.decode("utf-8", "replace"))

        pumps = [
            asyncio.create_task(pump(proc.stdout, "stdout")),
            asyncio.create_task(pump(proc.stderr, "stderr")),
        ]
        try:
            await asyncio.wait_for(proc.wait(), timeout=bundle.timeout_s)
        except asyncio.TimeoutError:
            self._kill_group(proc)
            await asyncio.gather(*pumps, return_exceptions=True)
            return JobOutcome(status="failed", exit_code=None, error="timeout")
        except asyncio.CancelledError:
            # Cancel request: kill the process group, report canceled.
            self._kill_group(proc)
            await asyncio.gather(*pumps, return_exceptions=True)
            return JobOutcome(status="canceled", error="canceled")
        finally:
            await asyncio.gather(*pumps, return_exceptions=True)

        rc = proc.returncode
        if rc != 0:
            return JobOutcome(status="failed", exit_code=rc, error=f"exit code {rc}")

        result = self._read_result(result_path)
        return JobOutcome(status="succeeded", exit_code=rc, result=result)

    async def _resolve_inputs(self, inputs) -> str:
        """Return the value for GODSERVE_INPUTS. A blob_ref is downloaded and the
        env carries a path to the materialized file instead of inline JSON."""
        if isinstance(inputs, BlobRef):
            ref = inputs.blob_ref
            if inputs.sha256 and ref.startswith(("http://", "https://")):
                cached = await download(ref, inputs.sha256)
                dest = Path(tempfile.mkdtemp(dir=self._scratch_root)) / "inputs.bin"
                materialize(cached, str(dest), extract=False)
                return json.dumps({"blob_path": str(dest)})
            return json.dumps({"blob_ref": ref})
        return json.dumps(inputs)

    @staticmethod
    def _read_result(result_path: Path) -> dict:
        if not result_path.exists():
            return {}
        try:
            return json.loads(result_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("bad result JSON at %s: %s", result_path, exc)
            return {}

    @staticmethod
    def _kill_group(proc: asyncio.subprocess.Process) -> None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
