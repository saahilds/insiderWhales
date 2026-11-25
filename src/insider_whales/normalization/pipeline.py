"""`normalize` step: stage downloaded archives, then build processed point-in-time tables."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from insider_whales.config import Settings
from insider_whales.ingestion.sec import TABLES, SecDataSets
from insider_whales.io.manifest import Manifest
from insider_whales.normalization.staging import load_staged, stage_archive
from insider_whales.normalization.transactions import normalize_tables

log = logging.getLogger(__name__)

STAGING_VERSION = "1"
NORMALIZE_VERSION = "1"


def staging_dir(settings: Settings) -> Path:
    return settings.data_dir / "staging" / "sec"


def processed_dir(settings: Settings) -> Path:
    return settings.data_dir / "processed" / "sec"


def stage_quarters(settings: Settings, quarters: list[str] | None = None) -> list[str]:
    """Stage every downloaded quarter whose archive or staging code changed."""
    ds = SecDataSets(settings, client=None)  # type: ignore[arg-type]
    steps = Manifest(settings.data_dir / "manifests" / "pipeline_steps.jsonl")
    wanted = quarters or sorted({r["quarter"] for r in ds.manifest.records()})
    staged = []
    for q in wanted:
        rec = ds.current(q)
        if rec is None:
            log.warning("%s is not downloaded; skipping", q)
            continue
        prev = steps.latest("key", f"stage:{q}")
        outputs_exist = all(
            (staging_dir(settings) / t.lower() / f"{q}.parquet").exists()
            for t in ("SUBMISSION", "NONDERIV_TRANS")
        )
        if (
            prev
            and prev.get("input_sha256") == rec["sha256"]
            and prev.get("version") == STAGING_VERSION
            and outputs_exist
        ):
            staged.append(q)
            continue
        counts = stage_archive(Path(rec["path"]), q, rec["sha256"], staging_dir(settings))
        steps.append(
            {
                "key": f"stage:{q}",
                "input_sha256": rec["sha256"],
                "version": STAGING_VERSION,
                "rows": counts,
            }
        )
        staged.append(q)
    return staged


def normalize(settings: Settings, quarters: list[str] | None = None) -> dict[str, int]:
    """Stage, then rebuild all processed SEC tables from the selected staged quarters."""
    qs = stage_quarters(settings, quarters)
    if not qs:
        raise RuntimeError("nothing to normalize; run download-sec first")
    staged = {}
    for t in TABLES:
        try:
            staged[t.lower()] = load_staged(staging_dir(settings), t, qs)
        except FileNotFoundError:
            continue
    tables = normalize_tables(staged, settings.availability)
    out = processed_dir(settings)
    out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.write_parquet(out / f"{name}.parquet")
    Manifest(settings.data_dir / "manifests" / "pipeline_steps.jsonl").append(
        {
            "key": "normalize",
            "quarters": qs,
            "version": NORMALIZE_VERSION,
            "rows": {k: v.height for k, v in tables.items()},
        }
    )
    return {k: v.height for k, v in tables.items()}


def load_processed(settings: Settings, name: str) -> pl.DataFrame:
    return pl.read_parquet(processed_dir(settings) / f"{name}.parquet")
