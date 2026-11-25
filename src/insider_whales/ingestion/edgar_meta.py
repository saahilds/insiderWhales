"""EDGAR filing metadata: acceptance timestamps and the quarterly full-index."""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import re
import zipfile
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from insider_whales.config import Settings, sec_user_agent
from insider_whales.io.cache import RawStore
from insider_whales.io.http import SecClient, SecRequestError
from insider_whales.io.manifest import Manifest
from insider_whales.pit import UTC_DTYPE

log = logging.getLogger(__name__)

OWNERSHIP_FORMS = {"3", "3/A", "4", "4/A", "5", "5/A"}
NEW_YORK = ZoneInfo("America/New_York")
TIMESTAMP_SCHEMA = {
    "accession_number": pl.String,
    "acceptance_ts": UTC_DTYPE,
    "acceptance_source": pl.String,
}


def cik10(cik: str | int) -> str:
    return str(int(cik)).zfill(10)


# --- bulk submissions ----------------------------------------------------------------------


async def ensure_submissions_bulk(
    settings: Settings, client: SecClient, max_age_days: int = 30
) -> Path:
    """Download EDGAR's nightly `submissions.zip` unless a recent enough copy is stored."""
    manifest = Manifest(settings.data_dir / "manifests" / "edgar_submissions_bulk.jsonl")
    rec = manifest.latest("source", "submissions_bulk", statuses={"downloaded"})
    if rec and Path(rec["path"]).exists():
        fetched = datetime.fromisoformat(rec["recorded_at"])
        if datetime.now(UTC) - fetched < timedelta(days=max_age_days):
            return Path(rec["path"])
    store = RawStore(settings.data_dir / "raw")
    key = datetime.now(UTC).strftime("%Y%m%d")
    tmp = store.scratch_path("sec/submissions_bulk", key, "submissions.zip")
    result = await client.download(settings.sec.submissions_bulk_url, tmp)
    final = store.ingest(tmp, "sec/submissions_bulk", key, "submissions.zip", result.sha256)
    manifest.append(
        {
            "source": "submissions_bulk",
            "status": "downloaded",
            "path": str(final),
            "sha256": result.sha256,
            "size": result.size,
            "last_modified": result.last_modified,
        }
    )
    return final


def _rows_from_payload(payload: dict) -> Iterable[tuple[str, str, str]]:
    block = payload.get("filings", {}).get("recent", payload)
    accs = block.get("accessionNumber", [])
    forms = block.get("form", [])
    times = block.get("acceptanceDateTime", [])
    for acc, form, ts in zip(accs, forms, times, strict=False):
        if form in OWNERSHIP_FORMS and ts:
            yield acc, form, ts


def acceptance_from_bulk(zip_path: Path, ciks: Iterable[str]) -> pl.DataFrame:
    """Acceptance timestamps of ownership filings listed in the given CIKs' submission files.

    Reads only the members for those CIKs (main file plus paginated history files).
    """
    wanted = {cik10(c) for c in ciks}
    rows: dict[str, str] = {}
    with zipfile.ZipFile(zip_path) as zf:
        by_cik: dict[str, list[str]] = {}
        for name in zf.namelist():
            m = re.match(r"CIK(\d{10})(?:-submissions-\d+)?\.json$", name)
            if m and m.group(1) in wanted:
                by_cik.setdefault(m.group(1), []).append(name)
        for names in by_cik.values():
            for name in sorted(names):
                try:
                    payload = json.loads(zf.read(name))
                except (json.JSONDecodeError, KeyError) as e:
                    log.warning("unreadable member %s: %s", name, e)
                    continue
                for acc, _form, ts in _rows_from_payload(payload):
                    rows.setdefault(acc, ts)
    df = pl.DataFrame(
        {"accession_number": list(rows), "acceptance_raw": list(rows.values())},
        schema={"accession_number": pl.String, "acceptance_raw": pl.String},
    )
    return df.with_columns(
        pl.col("acceptance_raw")
        .str.strptime(pl.Datetime("us", "UTC"), "%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
        .alias("acceptance_ts"),
        pl.lit("submissions_bulk").alias("acceptance_source"),
    ).select(list(TIMESTAMP_SCHEMA))


HEADER_RE = re.compile(r"ACCEPTANCE-DATETIME>\s*(\d{14})")


def parse_index_header_acceptance(text: str) -> datetime | None:
    """`<ACCEPTANCE-DATETIME>20261123165428` (New York local time) to a UTC datetime."""
    m = HEADER_RE.search(text)
    if not m:
        return None
    local = datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(tzinfo=NEW_YORK)
    return local.astimezone(UTC)


async def acceptance_from_headers(
    settings: Settings, client: SecClient, filings: pl.DataFrame, limit: int
) -> pl.DataFrame:
    """Per-filing fallback via `-index-headers.html` for up to `limit` filings."""
    todo = filings.head(limit)

    async def one(acc: str, cik: str):
        url = (
            f"{settings.sec.archives_base}/{int(cik)}/{acc.replace('-', '')}/"
            f"{acc}-index-headers.html"
        )
        try:
            resp = await client.get(url)
        except SecRequestError as e:
            log.warning("header fallback failed for %s: %s", acc, e)
            return acc, None
        return acc, parse_index_header_acceptance(resp.text)

    results = await asyncio.gather(
        *(one(a, c) for a, c in todo.select("accession_number", "issuer_cik").iter_rows())
    )
    return pl.DataFrame(
        [(a, ts, "index_header") for a, ts in results if ts is not None],
        schema=TIMESTAMP_SCHEMA,
        orient="row",
    )


async def build_filing_timestamps(
    settings: Settings,
    submission: pl.DataFrame,
    owners: pl.DataFrame,
    *,
    header_fallback_limit: int = 200,
    max_age_days: int = 30,
    client: SecClient | None = None,
) -> pl.DataFrame:
    """Acceptance timestamps for every accession in `submission` that can be found."""
    own = client is None
    client = client or SecClient(settings.sec, sec_user_agent(), seed=settings.seed)
    try:
        bulk = await ensure_submissions_bulk(settings, client, max_age_days)
        accs = submission.select("accession_number", pl.col("issuercik").alias("issuer_cik"))
        found = acceptance_from_bulk(bulk, accs["issuer_cik"].unique().to_list())
        found = found.join(accs.select("accession_number"), on="accession_number", how="semi")
        missing = accs.join(found, on="accession_number", how="anti")
        if missing.height:
            owner_ciks = owners.join(missing, on="accession_number", how="semi")
            extra = acceptance_from_bulk(bulk, owner_ciks["rptownercik"].unique().to_list())
            extra = extra.join(missing, on="accession_number", how="semi")
            found = pl.concat(
                [
                    found,
                    extra.with_columns(pl.lit("submissions_bulk_owner").alias("acceptance_source")),
                ]
            )
            missing = missing.join(extra, on="accession_number", how="anti")
        if missing.height and header_fallback_limit > 0:
            hdr = await acceptance_from_headers(
                settings, client, missing.sort("accession_number"), header_fallback_limit
            )
            found = pl.concat([found, hdr])
        return found.unique("accession_number", keep="first").sort("accession_number")
    finally:
        if own:
            await client.aclose()


# --- full index ------------------------------------------------------------------------------


def parse_form_index(text: str) -> pl.DataFrame:
    """EDGAR `form.idx`: one row per (filing, filer) with form type, CIK, date and accession."""
    rows = []
    started = False
    for line in text.splitlines():
        if not started:
            started = line.startswith("-----")
            continue
        parts = re.split(r"\s{2,}", line.strip())
        if len(parts) < 5:
            continue
        form, cik, filed, path = parts[0], parts[-3], parts[-2], parts[-1]
        acc = path.rsplit("/", 1)[-1].removesuffix(".txt")
        rows.append((form, cik, filed, acc))
    return pl.DataFrame(
        rows, schema=["form", "cik", "date_filed", "accession_number"], orient="row"
    ).with_columns(pl.col("date_filed").str.strptime(pl.Date, "%Y-%m-%d", strict=False))


async def fetch_form_index(settings: Settings, client: SecClient, quarter: str) -> pl.DataFrame:
    """Download (or reuse) the full-index `form.gz` for a quarter such as `2019q1`."""
    year, q = int(quarter[:4]), int(quarter[-1])
    store = RawStore(settings.data_dir / "raw")
    manifest = Manifest(settings.data_dir / "manifests" / "edgar_full_index.jsonl")
    rec = manifest.latest("quarter", quarter, statuses={"downloaded"})
    complete = date(year, 3 * q, 28) + timedelta(days=10) < datetime.now(UTC).date()
    if not (rec and Path(rec["path"]).exists() and complete):
        base = f"{settings.sec.full_index_base}/{year}/QTR{q}/"
        listing = (await client.get(base + "index.json")).json()
        names = {item["name"] for item in listing["directory"]["item"]}
        if "form.gz" not in names:
            raise RuntimeError(f"form.gz not listed in {base}")
        tmp = store.scratch_path("sec/full_index", quarter, "form.gz")
        result = await client.download(base + "form.gz", tmp)
        final = store.ingest(tmp, "sec/full_index", quarter, "form.gz", result.sha256)
        rec = manifest.append(
            {
                "quarter": quarter,
                "status": "downloaded",
                "path": str(final),
                "sha256": result.sha256,
                "last_modified": result.last_modified,
            }
        )
    text = gzip.decompress(Path(rec["path"]).read_bytes()).decode("latin-1")
    return parse_form_index(text)


def reconcile_with_full_index(dataset_accessions: pl.Series, index: pl.DataFrame) -> dict:
    """Compare data set accessions with unique Form 3/4/5 accessions in EDGAR's full-index."""
    idx = set(index.filter(pl.col("form").is_in(OWNERSHIP_FORMS))["accession_number"].to_list())
    ds = set(dataset_accessions.to_list())
    only_index = sorted(idx - ds)
    only_ds = sorted(ds - idx)
    return {
        "dataset_accessions": len(ds),
        "full_index_accessions": len(idx),
        "in_both": len(ds & idx),
        "only_in_full_index": len(only_index),
        "only_in_dataset": len(only_ds),
        "only_in_full_index_sample": only_index[:20],
        "only_in_dataset_sample": only_ds[:20],
    }
