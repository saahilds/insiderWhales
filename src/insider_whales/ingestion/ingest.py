"""`ingest-sec`: the whole SEC pipeline for a quarter range, resumable at every step."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from insider_whales.config import REPO_ROOT, Settings
from insider_whales.ingestion.sec import (
    SchemaRegistry,
    SecDataSets,
    download_quarters,
    inspect_archive,
    render_schema_markdown,
)

log = logging.getLogger(__name__)


def record_schemas(settings: Settings, quarters: list[str]) -> list[str]:
    """Inspect every downloaded archive whose content hash is not in the schema registry."""
    ds = SecDataSets(settings, client=None)  # type: ignore[arg-type]
    registry = SchemaRegistry(settings.data_dir / "manifests" / "sec_schemas.json")
    known = registry.load()
    new = []
    for q in quarters:
        rec = ds.current(q)
        if rec is None or known.get(q, {}).get("sha256") == rec["sha256"]:
            continue
        registry.record(q, rec["sha256"], inspect_archive(Path(rec["path"])))
        new.append(q)
    return new


def run_ingest(
    settings: Settings,
    start: str,
    end: str,
    *,
    header_fallback_limit: int = 200,
    stop_after: int | None = None,
    audit: bool = True,
    write_docs: bool = True,
    say: Callable[[str], Any] = print,
) -> dict[str, Any]:
    """Download, stage, timestamp, normalize, drift-check and audit quarters `start`..`end`.

    Every step is idempotent: unchanged downloads are skipped by hash, staged and normalized
    quarters are skipped by input fingerprint, and acceptance timestamps are only fetched for
    accessions that do not have one yet. Interrupt and rerun with the same arguments to resume.
    """
    from insider_whales.evaluation.data_quality import run_audit
    from insider_whales.evaluation.schema_drift import render_drift_markdown, run_drift
    from insider_whales.normalization.pipeline import (
        build_timestamps,
        normalize,
        staging_dir,
    )

    results = asyncio.run(download_quarters(settings, start=start, end=end))
    failed = [r for r in results if r["status"] == "failed"]
    quarters = [r["quarter"] for r in results]
    say(
        f"download: {len(results)} quarters, "
        f"{sum(r['status'] == 'downloaded' for r in results)} fetched, "
        f"{sum(r['status'] == 'unchanged' for r in results)} unchanged, {len(failed)} failed"
    )
    if failed:
        raise RuntimeError(f"download failed for {[r['quarter'] for r in failed]}; rerun to resume")

    say(f"schemas: {len(record_schemas(settings, quarters))} newly inspected")
    ts = build_timestamps(settings, quarters, header_fallback_limit=header_fallback_limit)
    say(f"acceptance timestamps: {ts.height:,} accessions known")
    counts = normalize(settings, quarters, stop_after=stop_after)
    say("normalize: " + ", ".join(f"{k}={v:,}" for k, v in counts.items()))

    drift = run_drift(settings, quarters, staging_dir(settings))
    say(f"drift: {len(drift['anomalies'])} value-level anomalies flagged")
    if write_docs:
        registry = SchemaRegistry(settings.data_dir / "manifests" / "sec_schemas.json").load()
        docs = REPO_ROOT / "docs" / "data"
        docs.mkdir(parents=True, exist_ok=True)
        (docs / "sec-schema.md").write_text(render_schema_markdown(registry))
        (docs / "sec-drift.md").write_text(
            render_drift_markdown(drift["profiles"], drift["anomalies"], drift["column_changes"])
        )
    if audit:
        paths = run_audit(settings, quarters, settings.reports_dir)
        say("audit: " + ", ".join(str(p) for p in paths))
    return {"quarters": quarters, "counts": counts, "drift": drift["anomalies"]}
