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
from datetime import UTC, date, datetime, timedelta
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

# Standard metric -> us-gaap tags in order of preference. Companies switch tags over time (e.g.
# revenue), so each metric is resolved per filing and period, taking the first tag present.
FLOW_METRICS: dict[str, tuple[str, ...]] = {
    "revenue": (
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueGoodsNet",
    ),
    "net_income": ("NetIncomeLoss", "ProfitLoss"),
    "op_cash_flow": ("NetCashProvidedByUsedInOperatingActivities",),
    "capex": ("PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets"),
}
INSTANT_METRICS: dict[str, tuple[str, ...]] = {
    "assets": ("Assets",),
    "liabilities": ("Liabilities",),
    "equity": (
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ),
    "cash": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ),
    "debt_total": ("LongTermDebt", "DebtLongtermAndShorttermCombinedAmount"),
    "debt_noncurrent": ("LongTermDebtNoncurrent",),
    "debt_current": ("DebtCurrent", "LongTermDebtCurrent"),
}
ANNUAL_FORMS = {"10-K", "10-K/A", "10-KT", "10-KT/A", "10-K405"}
ANNUAL_DAYS = (330, 400)
FACT_SCHEMA = {
    "issuer_cik": pl.String,
    "metric": pl.String,
    "tag_rank": pl.Int8,
    "period_start": pl.String,
    "period_end": pl.String,
    "filed": pl.String,
    "accession_number": pl.String,
    "form": pl.String,
    "value": pl.Float64,
}


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


def annual_facts_from_payload(payload: dict) -> list[dict]:
    """Annual (10-K) values of the standard metrics, one row per filing and period."""
    gaap = payload.get("facts", {}).get("us-gaap", {})
    cik = str(payload.get("cik", "")).zfill(10)
    rows: list[dict] = []
    for table, instant in ((FLOW_METRICS, False), (INSTANT_METRICS, True)):
        for metric, tags in table.items():
            for rank, tag in enumerate(tags):
                for item in gaap.get(tag, {}).get("units", {}).get("USD", []):
                    if item.get("form") not in ANNUAL_FORMS or item.get("val") is None:
                        continue
                    start, end = item.get("start"), item.get("end")
                    if not (end and item.get("filed") and item.get("accn")):
                        continue
                    if instant != (start is None):
                        continue
                    if not instant:
                        days = (date.fromisoformat(end) - date.fromisoformat(start)).days
                        if not ANNUAL_DAYS[0] <= days <= ANNUAL_DAYS[1]:
                            continue
                    rows.append(
                        {
                            "issuer_cik": cik,
                            "metric": metric,
                            "tag_rank": rank,
                            "period_start": start,
                            "period_end": end,
                            "filed": item["filed"],
                            "accession_number": item["accn"],
                            "form": item["form"],
                            "value": float(item["val"]),
                        }
                    )
    return rows


def extract_all(zip_path: Path, ciks: Iterable[str]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Cover-page shares and annual facts for the given CIKs, in one pass over the archive."""
    wanted = {str(int(c)).zfill(10) for c in ciks}
    shares: list[dict] = []
    facts: list[dict] = []
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if (m := MEMBER_RE.search(n)) and m.group(1) in wanted]
        for name in sorted(names):
            try:
                payload = json.loads(zf.read(name))
            except json.JSONDecodeError as e:
                log.warning("unreadable companyfacts member %s: %s", name, e)
                continue
            shares.extend(shares_from_facts(payload))
            facts.extend(annual_facts_from_payload(payload))
    s_schema = {**SHARES_SCHEMA, "period_end": pl.String, "filed": pl.String}
    shares_df = pl.DataFrame(shares, schema=s_schema).with_columns(
        pl.col("period_end").str.to_date(strict=False), pl.col("filed").str.to_date(strict=False)
    )
    facts_df = pl.DataFrame(facts, schema=FACT_SCHEMA).with_columns(
        pl.col("period_start").str.to_date(strict=False),
        pl.col("period_end").str.to_date(strict=False),
        pl.col("filed").str.to_date(strict=False),
    )
    return shares_df, facts_df


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


def build_fundamentals(
    settings: Settings, ciks: Iterable[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Write `shares_outstanding.parquet` and `annual_facts.parquet` for the given issuers."""
    zip_path = asyncio.run(ensure_companyfacts_bulk(settings))
    shares, facts = extract_all(zip_path, ciks)
    out = settings.data_dir / "processed" / "fundamentals"
    out.mkdir(parents=True, exist_ok=True)
    shares.sort("issuer_cik", "filed", "period_end").write_parquet(
        out / "shares_outstanding.parquet"
    )
    facts.sort("issuer_cik", "metric", "filed", "period_end").write_parquet(
        out / "annual_facts.parquet"
    )
    return shares, facts


def build_shares_outstanding(settings: Settings, ciks: Iterable[str]) -> pl.DataFrame:
    return build_fundamentals(settings, ciks)[0]
