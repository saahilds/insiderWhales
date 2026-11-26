"""Sector classification from SEC SIC codes, mapped to the sector SPDR ETFs.

The SIC code is the company's current one from the SEC submissions archive (free, no history),
so a company that changed its business is classified by what it is today. The mapping is
coarse by design; it is only used for sector-relative returns and for matching controls.
"""

from __future__ import annotations

import json
import re
import zipfile
from collections.abc import Iterable
from pathlib import Path

import polars as pl

from insider_whales.config import Settings

MEMBER_RE = re.compile(r"^CIK(\d{10})\.json$")

# (prefix, ETF): the longest matching prefix of the 4-digit SIC wins; None leaves it unclassified.
_RULES: list[tuple[str, str | None]] = [
    ("6798", "XLRE"),
    ("65", "XLRE"),
    ("60", "XLF"),
    ("61", "XLF"),
    ("62", "XLF"),
    ("63", "XLF"),
    ("64", "XLF"),
    ("67", "XLF"),
    ("6770", None),
    ("12", "XLE"),
    ("13", "XLE"),
    ("29", "XLE"),
    ("46", "XLE"),
    ("5171", "XLE"),
    ("5172", "XLE"),
    ("10", "XLB"),
    ("14", "XLB"),
    ("24", "XLB"),
    ("26", "XLB"),
    ("28", "XLB"),
    ("283", "XLV"),
    ("284", "XLP"),
    ("30", "XLB"),
    ("32", "XLB"),
    ("33", "XLB"),
    ("34", "XLI"),
    ("15", "XLI"),
    ("16", "XLI"),
    ("17", "XLI"),
    ("35", "XLI"),
    ("357", "XLK"),
    ("36", "XLK"),
    ("361", "XLI"),
    ("362", "XLI"),
    ("363", "XLY"),
    ("364", "XLI"),
    ("365", "XLY"),
    ("37", "XLI"),
    ("371", "XLY"),
    ("375", "XLY"),
    ("379", "XLY"),
    ("38", "XLK"),
    ("384", "XLV"),
    ("385", "XLV"),
    ("3826", "XLV"),
    ("3841", "XLV"),
    ("3842", "XLV"),
    ("3845", "XLV"),
    ("386", "XLY"),
    ("39", "XLY"),
    ("20", "XLP"),
    ("21", "XLP"),
    ("22", "XLY"),
    ("23", "XLY"),
    ("25", "XLY"),
    ("31", "XLY"),
    ("27", "XLC"),
    ("40", "XLI"),
    ("41", "XLI"),
    ("42", "XLI"),
    ("44", "XLI"),
    ("45", "XLI"),
    ("47", "XLY"),
    ("48", "XLC"),
    ("49", "XLU"),
    ("4953", "XLI"),
    ("4955", "XLI"),
    ("4959", "XLI"),
    ("50", "XLI"),
    ("501", "XLY"),
    ("512", "XLV"),
    ("51", "XLP"),
    ("52", "XLY"),
    ("53", "XLY"),
    ("5331", "XLP"),
    ("54", "XLP"),
    ("55", "XLY"),
    ("56", "XLY"),
    ("57", "XLY"),
    ("58", "XLY"),
    ("59", "XLY"),
    ("5912", "XLP"),
    ("70", "XLY"),
    ("72", "XLY"),
    ("73", "XLI"),
    ("737", "XLK"),
    ("7311", "XLC"),
    ("75", "XLY"),
    ("78", "XLC"),
    ("79", "XLY"),
    ("80", "XLV"),
    ("81", "XLI"),
    ("82", "XLY"),
    ("83", "XLV"),
    ("87", "XLI"),
    ("8731", "XLV"),
    ("89", "XLI"),
]
_BY_LENGTH = sorted(_RULES, key=lambda r: -len(r[0]))


def sic_to_sector_etf(sic: str | int | None) -> str | None:
    """Sector ETF for a SIC code (longest matching prefix), or None when unclassified."""
    if sic is None:
        return None
    code = str(sic).strip().zfill(4)
    if not code.isdigit() or code == "0000":
        return None
    for prefix, etf in _BY_LENGTH:
        if code.startswith(prefix):
            return etf
    return None


def extract_sic(zip_path: Path, ciks: Iterable[str]) -> pl.DataFrame:
    """SIC code and description per issuer from the EDGAR submissions archive."""
    wanted = {str(int(c)).zfill(10) for c in ciks}
    rows = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            m = MEMBER_RE.match(name)
            if not m or m.group(1) not in wanted:
                continue
            payload = json.loads(zf.read(name))
            rows.append(
                {
                    "issuer_cik": m.group(1),
                    "sic": payload.get("sic") or None,
                    "sic_description": payload.get("sicDescription") or None,
                }
            )
    df = pl.DataFrame(
        rows, schema={"issuer_cik": pl.String, "sic": pl.String, "sic_description": pl.String}
    )
    return df.with_columns(
        pl.col("sic").map_elements(sic_to_sector_etf, return_dtype=pl.String).alias("sector_etf")
    )


def build_sectors(settings: Settings) -> pl.DataFrame:
    import asyncio

    from insider_whales.config import sec_user_agent
    from insider_whales.ingestion.edgar_meta import ensure_submissions_bulk
    from insider_whales.io.http import SecClient

    async def bulk() -> Path:
        client = SecClient(settings.sec, sec_user_agent(), seed=settings.seed)
        try:
            return await ensure_submissions_bulk(settings, client)
        finally:
            await client.aclose()

    universe = pl.read_parquet(settings.data_dir / "processed" / "sec" / "universe.parquet")
    df = extract_sic(asyncio.run(bulk()), universe["issuer_cik"].unique().to_list())
    df.write_parquet(settings.data_dir / "processed" / "sec" / "sic.parquet")
    return df
