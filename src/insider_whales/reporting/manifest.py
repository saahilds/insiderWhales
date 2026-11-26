"""Run manifest and output digests.

`run_manifest.json` is the only place where wall-clock time is recorded. Digests of data files
hash the contents (not the Parquet bytes) so a rewrite with identical data compares equal.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from insider_whales import __version__
from insider_whales.config import REPO_ROOT, Settings

VOLATILE_KEYS = {"generated_at", "recorded_at"}


def _strip_volatile(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in obj.items() if k not in VOLATILE_KEYS}
    if isinstance(obj, list):
        return [_strip_volatile(v) for v in obj]
    return obj


def file_digest(path: Path) -> str:
    """SHA-256 of a file's content. Parquet is hashed by data, JSON without timestamps."""
    h = hashlib.sha256()
    if path.suffix == ".parquet":
        df = pl.read_parquet(path)
        h.update(json.dumps([[c, str(t)] for c, t in df.schema.items()]).encode())
        h.update(df.hash_rows(seed=0).to_numpy().tobytes())
    elif path.suffix == ".json":
        h.update(
            json.dumps(_strip_volatile(json.loads(path.read_bytes())), sort_keys=True).encode()
        )
    else:
        with path.open("rb") as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                h.update(block)
    return h.hexdigest()


def digest_files(root: Path, paths: list[Path]) -> dict[str, str]:
    """Digest of every existing file, keyed by its path relative to `root`."""
    out: dict[str, str] = {}
    for p in sorted(paths):
        if p.is_file():
            try:
                key = str(p.relative_to(root))
            except ValueError:
                key = str(p)
            out[key] = file_digest(p)
    return out


def output_files(settings: Settings) -> list[Path]:
    """Files written by the offline analysis steps (features, coverage, event study, report)."""
    data, reports = settings.data_dir, settings.reports_dir
    processed = data / "processed"
    files: list[Path] = [
        processed / "sec" / "sic.parquet",
        processed / "prices" / "event_prices.parquet",
        processed / "prices" / "series_verdicts.parquet",
        processed / "prices" / "prices_adjusted.parquet",
        processed / "prices" / "benchmarks_adjusted.parquet",
        reports / "report.html",
        REPO_ROOT / "docs" / "results" / "event-study-baseline.md",
        REPO_ROOT / "docs" / "data" / "features.md",
        REPO_ROOT / "docs" / "data" / "price-coverage.md",
    ]
    for pattern_dir, patterns in (
        (processed / "features", ["*.parquet"]),
        (processed / "event_study", ["*.parquet"]),
        (reports / "event_study", ["*.csv"]),
        (reports / "price_coverage", ["*.json", "*.md"]),
    ):
        for pattern in patterns:
            files += list(pattern_dir.glob(pattern))
    return files


def _git(*args: str) -> str | None:
    try:
        res = subprocess.run(
            ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True, timeout=20
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return res.stdout.strip()


def build_manifest(
    settings: Settings,
    steps: list[dict[str, Any]],
    started_at: datetime,
    finished_at: datetime,
    command: str,
) -> dict[str, Any]:
    dq = settings.reports_dir / "data_quality.json"
    versions = None
    if dq.exists():
        versions = json.loads(dq.read_text()).get("data_versions")
    status = _git("status", "--porcelain")
    return {
        "schema_version": 1,
        "command": command,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "package_version": __version__,
        "python": platform.python_version(),
        "polars": pl.__version__,
        "platform": platform.platform(),
        "git_commit": _git("rev-parse", "HEAD"),
        "git_dirty": bool(status) if status is not None else None,
        "seed": settings.seed,
        "config_hash": settings.config_hash(),
        "config": settings.model_dump(mode="json"),
        "labels": {
            "results": "exploratory, survivor-only",
            "development_period": [str(d) for d in settings.splits.development],
            "final_test_locked": True,
        },
        "sec_data_versions": versions,
        "steps": steps,
        "outputs": digest_files(REPO_ROOT, output_files(settings)),
    }


def write_run_manifest(
    settings: Settings,
    steps: list[dict[str, Any]],
    started_at: datetime,
    finished_at: datetime | None = None,
    command: str = "pipeline",
    path: Path | None = None,
) -> Path:
    manifest = build_manifest(
        settings, steps, started_at, finished_at or datetime.now(UTC), command
    )
    path = path or settings.reports_dir / "run_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n")
    return path
