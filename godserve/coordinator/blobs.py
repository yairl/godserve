"""Content-addressed blob store on coordinator disk (PLAN §1.4, §4.6).

Write to a temp file, sha256 the bytes, rename into ``blobs/{sha256}``.
Idempotent: the same content lands at the same path; a re-upload is a no-op.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import AsyncIterator

import anyio


class BlobTooLarge(Exception):
    """Raised mid-stream when an upload exceeds the configured max size."""

    def __init__(self, max_size: int):
        super().__init__(f"blob exceeds max size {max_size} bytes")
        self.max_size = max_size


class BlobStore:
    def __init__(self, root: str):
        self._dir = Path(root) / "blobs"
        self._dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, blob_id: str) -> Path:
        return self._dir / blob_id

    def exists(self, blob_id: str) -> bool:
        return self.path_for(blob_id).exists()

    async def store(
        self, chunks: AsyncIterator[bytes], max_size: int | None
    ) -> tuple[str, int]:
        """Stream chunks to disk, returning (blob_id, size).

        ``max_size`` (bytes) caps the upload; if exceeded mid-stream the partial
        temp file is discarded and :class:`BlobTooLarge` is raised — so an
        oversize body is rejected during streaming, never buffered whole."""
        h = hashlib.sha256()
        size = 0
        fd, tmp = tempfile.mkstemp(dir=self._dir, prefix=".tmp-")
        try:
            async with await anyio.open_file(fd, "wb") as f:
                async for chunk in chunks:
                    size += len(chunk)
                    if max_size is not None and size > max_size:
                        raise BlobTooLarge(max_size)
                    h.update(chunk)
                    await f.write(chunk)
            blob_id = h.hexdigest()
            dest = self.path_for(blob_id)
            if dest.exists():
                os.unlink(tmp)  # idempotent: already have this content
            else:
                os.replace(tmp, dest)
            return blob_id, size
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
