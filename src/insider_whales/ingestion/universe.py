"""Universe roster: every issuer and security that has insider events, plus current listings.

The roster is built from the Form 3/4/5 data itself (so delisted issuers are included) and joined
to the SEC's file of currently listed companies. It decides which price symbols to request and
gives each missing price an explanation later.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from insider_whales.config import Settings, sec_user_agent
from insider_whales.io.cache import RawStore
from insider_whales.io.http import SecClient
from insider_whales.io.manifest import Manifest
from insider_whales.normalization.pipeline import processed_dir, scan_processed
from insider_whales.normalization.tickers import to_yahoo_symbol

log = logging.getLogger(__name__)

LISTINGS_SCHEMA = {
    "issuer_cik": pl.String,
    "name": pl.String,
    "ticker": pl.String,
    "exchange": pl.String,
}


def parse_company_tickers(payload: dict) -> pl.DataFrame:
    """SEC `company_tickers_exchange.json` (fields + data rows) to a table."""
    fields = payload["fields"]
    rows = [dict(zip(fields, r, strict=True)) for r in payload["data"]]
    df = pl.DataFrame(rows)
    if df.height == 0:
        return pl.DataFrame(schema=LISTINGS_SCHEMA)
    return df.select(
        pl.col("cik").cast(pl.String).str.zfill(10).alias("issuer_cik"),
        pl.col("name"),
        pl.col("ticker").str.to_uppercase(),
        pl.col("exchange"),
    )


async def _fetch_listings(settings: Settings, client: SecClient | None) -> pl.DataFrame:
    own = client is None
    client = client or SecClient(settings.sec, sec_user_agent(), seed=settings.seed)
    try:
        resp = await client.get(settings.sec.company_tickers_url)
        store = RawStore(settings.data_dir / "raw")
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        tmp = store.scratch_path("sec/listings", stamp, "company_tickers_exchange.json")
        tmp.write_bytes(resp.content)
        import hashlib

        sha = hashlib.sha256(resp.content).hexdigest()
        final = store.ingest(tmp, "sec/listings", stamp, "company_tickers_exchange.json", sha)
        Manifest(settings.data_dir / "manifests" / "sec_listings.jsonl").append(
            {"status": "downloaded", "path": str(final), "sha256": sha}
        )
        return parse_company_tickers(json.loads(resp.content))
    finally:
        if own:
            await client.aclose()


def current_listings(settings: Settings, refresh: bool = False) -> pl.DataFrame:
    """Companies the SEC lists today (cached; refetched only on request)."""
    path = processed_dir(settings) / "current_listings.parquet"
    if path.exists() and not refresh:
        return pl.read_parquet(path)
    df = asyncio.run(_fetch_listings(settings, None))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return df


def build_universe(settings: Settings, listings: pl.DataFrame) -> pl.DataFrame:
    """One row per security with insider events, with its listing status and symbol sets."""
    events = (
        scan_processed(settings, "insider_events")
        .group_by("security_id")
        .agg(
            pl.col("issuer_cik").first(),
            pl.len().alias("n_events"),
            pl.col("is_conservative_set").sum().alias("n_conservative_events"),
            pl.col("filing_date").min().alias("first_event"),
            pl.col("filing_date").max().alias("last_event"),
        )
        .collect()
    )
    securities = pl.read_parquet(processed_dir(settings) / "securities.parquet").select(
        "security_id", "security_type", "title_example"
    )
    ids = pl.read_parquet(processed_dir(settings) / "security_identifiers.parquet")
    hist = (
        ids.filter(pl.col("id_type") == "ticker")
        .group_by("security_id")
        .agg(pl.col("value").unique().sort().alias("historical_tickers"))
    )
    current = listings.group_by("issuer_cik").agg(
        pl.col("ticker").unique().sort().alias("current_tickers"),
        pl.col("exchange").first().alias("exchange"),
        pl.col("name").first().alias("current_name"),
    )
    return (
        events.join(securities, on="security_id", how="left")
        .join(hist, on="security_id", how="left")
        .join(current, on="issuer_cik", how="left")
        .with_columns(
            pl.col("current_tickers").is_not_null().alias("currently_listed"),
            pl.col("historical_tickers").fill_null([]),
        )
        .sort("security_id")
    )


def candidate_symbols(universe: pl.DataFrame) -> pl.DataFrame:
    """Provider symbols to request, with the number of events and securities each one serves.

    Historical tickers come from the filings; the issuer's current SEC ticker is added too, so
    a company that changed its symbol (ANTM to ELV) is found under the new one. A series is
    only attached to an event later if it overlaps the ticker window and passes the price check.
    """
    rows = []
    for r in universe.iter_rows(named=True):
        for t in r["historical_tickers"] or []:
            rows.append(
                (
                    to_yahoo_symbol(t),
                    "historical",
                    r["security_id"],
                    r["n_events"],
                    r["last_event"],
                    r["currently_listed"],
                )
            )
        for t in r["current_tickers"] or []:
            rows.append(
                (
                    to_yahoo_symbol(t),
                    "current",
                    r["security_id"],
                    r["n_events"],
                    r["last_event"],
                    r["currently_listed"],
                )
            )
    df = pl.DataFrame(
        rows,
        schema={
            "symbol": pl.String,
            "kind": pl.String,
            "security_id": pl.String,
            "n_events": pl.UInt32,
            "last_event": pl.Date,
            "currently_listed": pl.Boolean,
        },
        orient="row",
    )
    return (
        df.group_by("symbol")
        .agg(
            pl.col("kind").unique().sort().alias("kinds"),
            pl.col("security_id").n_unique().alias("n_securities"),
            pl.col("n_events").sum().alias("n_events"),
            pl.col("last_event").max().alias("last_event"),
            pl.col("currently_listed").any().alias("currently_listed"),
        )
        .sort("symbol")
    )


def write_universe(
    settings: Settings, refresh_listings: bool = False
) -> tuple[pl.DataFrame, pl.DataFrame]:
    listings = current_listings(settings, refresh_listings)
    universe = build_universe(settings, listings)
    symbols = candidate_symbols(universe)
    out = processed_dir(settings)
    universe.write_parquet(out / "universe.parquet")
    symbols.write_parquet(out / "candidate_symbols.parquet")
    return universe, symbols


def universe_dir(settings: Settings) -> Path:
    return processed_dir(settings)
