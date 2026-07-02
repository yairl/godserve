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


class BlobStore:
    def __init__(self, root: str):
        self._dir = Path(root) / "blobs"
        self._dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, blob_id: str) -> Path:
        return self._dir / blob_id

    def exists(self, blob_id: str) -> bool:
        return self.path_for(blob_id).exists()

    async def store(self, chunks: AsyncIterator[bytes]) -> tuple[str, int]:
        """Stream chunks to disk, returning (blob_id, size)."""
        h = hashlib.sha256()
        size = 0
        fd, tmp = tempfile.mkstemp(dir=self._dir, prefix=".tmp-")
        try:
            async with await anyio.open_file(fd, "wb") as f:
                async for chunk in chunks:
                    h.update(chunk)
                    size += len(chunk)
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
