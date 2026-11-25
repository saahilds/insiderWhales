"""Append-only JSONL manifests: download history and pipeline-step records."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class Manifest:
    def __init__(self, path: Path):
        self.path = path

    def append(self, record: dict[str, Any]) -> dict[str, Any]:
        rec = {**record, "recorded_at": datetime.now(UTC).isoformat()}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, sort_keys=True, default=str) + "\n")
        return rec

    def records(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self.path.open(encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]

    def latest(self, field: str, value: Any, *, statuses: set[str] | None = None):
        """Most recent record where record[field] == value (optionally filtered by status)."""
        for rec in reversed(self.records()):
            if rec.get(field) == value and (statuses is None or rec.get("status") in statuses):
                return rec
        return None
