"""SEC XBRL company facts: shares outstanding from cover pages, point in time.

Every fact in the bulk `companyfacts.zip` carries the accession and the date it was filed, so a
value is known only from its filing date on. Revisions arrive as new filings and are kept as
separate versions; nothing is overwritten.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import zipfile
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from insider_whales.config import Settings, sec_user_agent
from insider_whales.io.cache import RawStore
from insider_whales.io.http import SecClient
from insider_whales.io.manifest import Manifest

log = logging.getLogger(__name__)

SHARES_SCHEMA = {
    "issuer_cik": pl.String,
    "period_end": pl.Date,
    "filed": pl.Date,
    "accession_number": pl.String,
    "form": pl.String,
    "shares": pl.Float64,
    "n_values": pl.Int32,
    "multi_class": pl.Boolean,
}
MEMBER_RE = re.compile(r"CIK(\d{10})\.json$")


async def ensure_companyfacts_bulk(
    settings: Settings, client: SecClient | None = None, max_age_days: int = 60
) -> Path:
    """The ~1.4 GB bulk archive, stored once and refreshed only when older than `max_age_days`."""
    manifest = Manifest(settings.data_dir / "manifests" / "sec_companyfacts_bulk.jsonl")
    rec = manifest.latest("source", "companyfacts_bulk", statuses={"downloaded"})
    if rec and Path(rec["path"]).exists():
        age = datetime.now(UTC) - datetime.fromisoformat(rec["recorded_at"])
        if age < timedelta(days=max_age_days):
            return Path(rec["path"])
    own = client is None
    client = client or SecClient(settings.sec, sec_user_agent(), seed=settings.seed)
    try:
        store = RawStore(settings.data_dir / "raw")
        key = datetime.now(UTC).strftime("%Y%m%d")
        tmp = store.scratch_path("sec/companyfacts", key, "companyfacts.zip")
        result = await client.download(settings.sec.companyfacts_bulk_url, tmp)
        final = store.ingest(tmp, "sec/companyfacts", key, "companyfacts.zip", result.sha256)
        manifest.append(
            {
                "source": "companyfacts_bulk",
                "status": "downloaded",
                "path": str(final),
                "sha256": result.sha256,
                "size": result.size,
                "last_modified": result.last_modified,
            }
        )
        return final
    finally:
        if own:
            await client.aclose()


def shares_from_facts(payload: dict) -> list[dict]:
    """Cover-page share counts (`dei:EntityCommonStockSharesOutstanding`) from one company file.

    Several values in one filing (one per class) are summed and flagged `multi_class`; the
    same cover date reported again in a later filing is a separate row.
    """
    facts = payload.get("facts", {}).get("dei", {}).get("EntityCommonStockSharesOutstanding")
    if not facts:
        return []
    cik = str(payload.get("cik", "")).zfill(10)
    grouped: dict[tuple[str, str], list[dict]] = {}
    for item in facts.get("units", {}).get("shares", []):
        if item.get("val") is None or not item.get("accn") or not item.get("filed"):
            continue
        grouped.setdefault((item["accn"], item["end"]), []).append(item)
    rows = []
    for (accn, end), items in grouped.items():
        distinct = {i["val"] for i in items}
        rows.append(
            {
                "issuer_cik": cik,
                "period_end": end,
                "filed": items[0]["filed"],
                "accession_number": accn,
                "form": items[0].get("form"),
                "shares": float(sum(distinct)),
                "n_values": len(distinct),
                "multi_class": len(distinct) > 1,
            }
        )
    return rows


def extract_shares(zip_path: Path, ciks: Iterable[str]) -> pl.DataFrame:
    """Shares-outstanding versions for the given issuer CIKs."""
    wanted = {str(int(c)).zfill(10) for c in ciks}
    rows: list[dict] = []
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if (m := MEMBER_RE.search(n)) and m.group(1) in wanted]
        for name in sorted(names):
            try:
                rows.extend(shares_from_facts(json.loads(zf.read(name))))
            except (json.JSONDecodeError, KeyError) as e:
                log.warning("unreadable companyfacts member %s: %s", name, e)
    schema = {**SHARES_SCHEMA, "period_end": pl.String, "filed": pl.String}
    return pl.DataFrame(rows, schema=schema).with_columns(
        pl.col("period_end").str.to_date(strict=False),
        pl.col("filed").str.to_date(strict=False),
    )


def build_shares_outstanding(settings: Settings, ciks: Iterable[str]) -> pl.DataFrame:
    zip_path = asyncio.run(ensure_companyfacts_bulk(settings))
    df = extract_shares(zip_path, ciks)
    out = settings.data_dir / "processed" / "fundamentals"
    out.mkdir(parents=True, exist_ok=True)
    df.sort("issuer_cik", "filed", "period_end").write_parquet(out / "shares_outstanding.parquet")
    return df
