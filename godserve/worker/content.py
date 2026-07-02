"""Machine-wide content-addressed cache + godserve-fetch CLI (PLAN §1.5, §4.1).

``download(url, sha256)`` streams into a temp file, verifies the hash, and
atomically renames into ``content/{sha256}`` — so a given hash downloads once
per machine, shared across every env_key. Concurrency-safe via an O_EXCL
lockfile: a second concurrent fetch of the same hash waits rather than
double-downloading. ``materialize`` hardlinks (copy fallback) into a
destination, optionally tar/zip extracting.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import shutil
import sys
import tarfile
import tempfile
import time
import zipfile
from pathlib import Path

import httpx

log = logging.getLogger(__name__)


def cache_root() -> Path:
    root = os.environ.get("GODSERVE_CACHE_DIR")
    if root:
        base = Path(root)
    else:
        base = Path.home() / ".cache" / "godserve"
    (base / "content").mkdir(parents=True, exist_ok=True)
    return base


def _content_path(sha256: str) -> Path:
    return cache_root() / "content" / sha256


async def download(url: str, sha256: str) -> Path:
    """Return the cached path for ``sha256``, downloading from ``url`` if absent.

    Verified by hash. Concurrent callers of the same hash coordinate via an
    O_EXCL lockfile so the content downloads once per machine.
    """
    dest = _content_path(sha256)
    if dest.exists():
        return dest

    lock = dest.with_suffix(".lock")
    while True:
        if dest.exists():
            return dest
        try:
            fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.close(fd)
            break
        except FileExistsError:
            # Another fetch is in flight (or died leaving a stale lock).
            if time.time() - _mtime(lock) > 3600:
                _unlink(lock)
                continue
            await asyncio.sleep(0.1)

    try:
        if dest.exists():
            return dest
        h = hashlib.sha256()
        fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=".dl-")
        try:
            with os.fdopen(fd, "wb") as f:
                async with httpx.AsyncClient(follow_redirects=True, timeout=None) as client:
                    async with client.stream("GET", url) as resp:
                        resp.raise_for_status()
                        async for chunk in resp.aiter_bytes():
                            h.update(chunk)
                            f.write(chunk)
            got = h.hexdigest()
            if got != sha256:
                raise ValueError(f"hash mismatch for {url}: expected {sha256}, got {got}")
            os.replace(tmp, dest)
        except BaseException:
            _unlink(Path(tmp))
            raise
        return dest
    finally:
        _unlink(lock)


def materialize(cached_path: Path, dest: str, extract: bool) -> None:
    """Place cached content at ``dest`` — extract if archive, else hardlink."""
    dest_path = Path(dest)
    if extract:
        dest_path.mkdir(parents=True, exist_ok=True)
        if tarfile.is_tarfile(cached_path):
            with tarfile.open(cached_path) as tf:
                tf.extractall(dest_path)
        elif zipfile.is_zipfile(cached_path):
            with zipfile.ZipFile(cached_path) as zf:
                zf.extractall(dest_path)
        else:
            raise ValueError(f"{cached_path} is not a tar or zip archive")
        return

    if dest_path.is_dir():
        dest_path = dest_path / cached_path.name.lstrip(".")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        dest_path.unlink()
    try:
        os.link(cached_path, dest_path)
    except OSError:
        shutil.copy2(cached_path, dest_path)


def _mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except FileNotFoundError:
        return 0.0


def _unlink(p: Path) -> None:
    try:
        p.unlink()
    except FileNotFoundError:
        pass


async def _fetch(url: str, dest: str, sha256: str, extract: bool) -> None:
    cached = await download(url, sha256)
    materialize(cached, dest, extract)


def fetch_main(argv: list[str] | None = None) -> int:
    """`godserve-fetch <url> <dest> --sha256 <hash> [--extract]`."""
    parser = argparse.ArgumentParser(prog="godserve-fetch")
    parser.add_argument("url")
    parser.add_argument("dest")
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--extract", action="store_true")
    args = parser.parse_args(argv)
    try:
        asyncio.run(_fetch(args.url, args.dest, args.sha256, args.extract))
    except Exception as exc:
        log.error("fetch failed: %s", exc)
        print(f"godserve-fetch: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(fetch_main())
