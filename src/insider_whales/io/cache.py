"""Immutable, content-addressed store for raw downloads."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


class RawStore:
    """Raw files live at `<root>/<source>/<key>/<sha12>_<filename>` and are made read-only.

    A changed upstream file gets a new path, so earlier versions are never overwritten.
    """

    def __init__(self, root: Path):
        self.root = root

    def path_for(self, source: str, key: str, filename: str, sha256: str) -> Path:
        return self.root / source / key / f"{sha256[:12]}_{filename}"

    def scratch_path(self, source: str, key: str, filename: str) -> Path:
        p = self.root / source / key / f".incoming_{filename}"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def ingest(self, tmp: Path, source: str, key: str, filename: str, sha256: str) -> Path:
        final = self.path_for(source, key, filename, sha256)
        if final.exists():
            tmp.unlink(missing_ok=True)
        else:
            final.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp, final)
            final.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        return final

    @staticmethod
    def verify(path: Path, sha256: str) -> bool:
        return path.exists() and sha256_file(path) == sha256
