"""`normalize` step: stage downloaded archives, then build processed point-in-time tables."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

import polars as pl

from insider_whales.config import Settings
from insider_whales.ingestion.sec import TABLES, SecDataSets
from insider_whales.io.cache import sha256_file
from insider_whales.io.manifest import Manifest
from insider_whales.normalization.staging import load_staged, stage_archive
from insider_whales.normalization.transactions import (
    build_global_tables,
    build_transaction_tables,
)

log = logging.getLogger(__name__)

STAGING_VERSION = "1"
NORMALIZE_VERSION = "2"


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


def _fingerprint(settings: Settings, quarter_shas: dict[str, str], ts_path: Path) -> str:
    """Identifies every input of the processed tables; a changed input rebuilds the parts."""
    payload = {
        "version": NORMALIZE_VERSION,
        "quarters": quarter_shas,
        "availability": settings.availability.model_dump(mode="json"),
        "timestamps": sha256_file(ts_path) if ts_path.exists() else None,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def normalize(
    settings: Settings, quarters: list[str] | None = None, *, stop_after: int | None = None
) -> dict[str, int]:
    """Stage, then rebuild the processed SEC tables.

    Filings, owners and insider groups are built across all quarters at once (amendments and
    groups cross quarter boundaries). Transaction lines and insider events are built one source
    quarter at a time into `_parts/`, so memory stays bounded and an interrupted run resumes
    with the quarters that are still missing. `stop_after` interrupts after that many newly
    built quarters (used to demonstrate resumption).
    """
    qs = stage_quarters(settings, quarters)
    if not qs:
        raise RuntimeError("nothing to normalize; run download-sec first")
    ds = SecDataSets(settings, client=None)  # type: ignore[arg-type]
    shas = {q: ds.current(q)["sha256"] for q in qs}  # type: ignore[index]
    out = processed_dir(settings)
    out.mkdir(parents=True, exist_ok=True)
    ts_path = out / "filing_timestamps.parquet"
    timestamps = pl.read_parquet(ts_path) if ts_path.exists() else None
    if timestamps is None:
        log.warning("no acceptance timestamps; run `acceptance-times` (using filing dates)")
    fingerprint = _fingerprint(settings, shas, ts_path)
    steps = Manifest(settings.data_dir / "manifests" / "pipeline_steps.jsonl")

    g = build_global_tables(
        load_staged(staging_dir(settings), "SUBMISSION", qs),
        load_staged(staging_dir(settings), "REPORTINGOWNER", qs),
        settings.availability,
        timestamps,
    )
    for name in ("filings", "reporting_owners", "insider_groups"):
        g[name].write_parquet(out / f"{name}.parquet")

    parts = out / "_parts"
    built = 0
    for q in qs:
        marker = steps.latest("key", f"normalize_part:{q}")
        done = all(
            (parts / t / f"{q}.parquet").exists() for t in ("transactions", "insider_events")
        )
        if marker and marker.get("fingerprint") == fingerprint and done:
            continue
        if stop_after is not None and built >= stop_after:
            raise KeyboardInterrupt(f"stopped after {built} quarters (simulated interruption)")
        staged = {}
        for t in TABLES:
            if t in ("SUBMISSION", "REPORTINGOWNER", "OWNER_SIGNATURE"):
                continue
            try:
                staged[t.lower()] = load_staged(staging_dir(settings), t, [q])
            except FileNotFoundError:
                continue
        quarter_filings = g["filings"].filter(pl.col("source_quarter") == q)
        tables = build_transaction_tables(staged, {**g, "filings": quarter_filings})
        for name, df in tables.items():
            dest = parts / name / f"{q}.parquet"
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(".tmp")
            df.write_parquet(tmp)
            tmp.replace(dest)
        steps.append(
            {
                "key": f"normalize_part:{q}",
                "fingerprint": fingerprint,
                "rows": {k: v.height for k, v in tables.items()},
            }
        )
        built += 1
        log.info("normalized %s (%d lines)", q, tables["transactions"].height)

    counts = {k: g[k].height for k in ("filings", "reporting_owners", "insider_groups")}
    for name in ("transactions", "insider_events"):
        files = [parts / name / f"{q}.parquet" for q in qs]
        tmp = out / f"{name}.parquet.tmp"
        pl.scan_parquet(files).sink_parquet(tmp)
        tmp.replace(out / f"{name}.parquet")
        counts[name] = pl.scan_parquet(out / f"{name}.parquet").select(pl.len()).collect().item()
    steps.append({"key": "normalize", "quarters": qs, "version": NORMALIZE_VERSION, "rows": counts})
    return counts


def build_timestamps(
    settings: Settings, quarters: list[str] | None = None, **kwargs
) -> pl.DataFrame:
    """Fetch acceptance timestamps for staged filings not yet resolved and store them.

    Accessions already present in `filing_timestamps.parquet` are never looked up again, so a
    rerun after an interruption only does the remaining work.
    """
    import asyncio

    from insider_whales.ingestion.edgar_meta import build_filing_timestamps

    qs = stage_quarters(settings, quarters)
    submission = load_staged(staging_dir(settings), "SUBMISSION", qs)
    owners = load_staged(staging_dir(settings), "REPORTINGOWNER", qs)
    out = processed_dir(settings)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "filing_timestamps.parquet"
    known = pl.read_parquet(path) if path.exists() else None
    if known is not None:
        submission = submission.join(
            known.select("accession_number"), on="accession_number", how="anti"
        )
        owners = owners.join(
            submission.select("accession_number"), on="accession_number", how="semi"
        )
    if submission.height:
        ts = asyncio.run(build_filing_timestamps(settings, submission, owners, **kwargs))
        if known is not None:
            ts = pl.concat([ts, known]).unique("accession_number", keep="first")
    else:
        ts = known
    ts.sort("accession_number").write_parquet(path)
    return ts


def load_processed(settings: Settings, name: str, columns: list[str] | None = None) -> pl.DataFrame:
    return pl.read_parquet(processed_dir(settings) / f"{name}.parquet", columns=columns)


def scan_processed(settings: Settings, name: str) -> pl.LazyFrame:
    return pl.scan_parquet(processed_dir(settings) / f"{name}.parquet")
