"""Raw SEC archive tables to typed Parquet (no business logic)."""

from __future__ import annotations

import zipfile
from pathlib import Path

import polars as pl

from insider_whales.ingestion.sec import TABLES

DATE_COLUMNS = {
    "filing_date",
    "period_of_report",
    "date_of_orig_sub",
    "trans_date",
    "deemed_execution_date",
    "excercise_date",
    "exercise_date",
    "expiration_date",
    "ownersignaturedate",
}
NUMERIC_COLUMNS = {
    "trans_shares",
    "trans_pricepershare",
    "trans_total_value",
    "shrs_ownd_folwng_trans",
    "valu_ownd_folwng_trans",
    "conv_exercise_price",
    "undlyng_sec_shares",
    "undlyng_sec_value",
}
FLAG_COLUMNS = {
    "no_securities_owned",
    "not_subject_sec16",
    "form3_holdings_reported",
    "form4_trans_reported",
    "equity_swap_involved",
    "aff10b5one",
}
SEC_DATE_FORMAT = "%d-%b-%Y"


def parse_flag(col: str) -> pl.Expr:
    """SEC booleans arrive as 1/0/true/false/Y/N or empty; empty stays null (unknown)."""
    v = pl.col(col).str.strip_chars().str.to_lowercase()
    return (
        pl.when(v.is_in(["1", "true", "y", "yes"]))
        .then(pl.lit(True))
        .when(v.is_in(["0", "false", "n", "no"]))
        .then(pl.lit(False))
        .otherwise(pl.lit(None, dtype=pl.Boolean))
        .alias(col)
    )


def read_raw_table(zip_path: Path, table: str) -> pl.DataFrame | None:
    """One table as all-string columns, exactly as filed (tab-separated, no quoting)."""
    with zipfile.ZipFile(zip_path) as zf:
        member = next(
            (n for n in zf.namelist() if Path(n).stem.upper() == table and n.endswith(".tsv")),
            None,
        )
        if member is None:
            return None
        data = zf.read(member)
    return pl.read_csv(
        data,
        separator="\t",
        quote_char=None,
        infer_schema=False,
        encoding="utf8-lossy",
    )


def type_table(df: pl.DataFrame) -> pl.DataFrame:
    """Lower-case column names and parse dates, numbers and flags. Unparseable values become
    null; `<col>_raw` keeps the original text for date columns so parse failures are countable.
    """
    df = df.rename({c: c.lower() for c in df.columns})
    exprs = []
    for c in df.columns:
        if c in DATE_COLUMNS:
            exprs.append(pl.col(c).alias(f"{c}_raw"))
            exprs.append(
                pl.col(c).str.strip_chars().str.strptime(pl.Date, SEC_DATE_FORMAT, strict=False)
            )
        elif c in NUMERIC_COLUMNS:
            exprs.append(pl.col(c).str.strip_chars().cast(pl.Float64, strict=False))
        elif c in FLAG_COLUMNS:
            exprs.append(parse_flag(c))
        elif c.endswith("_sk"):
            exprs.append(pl.col(c).cast(pl.Int64, strict=False))
        else:
            exprs.append(pl.col(c).str.strip_chars())
    return df.select(exprs)


def stage_archive(zip_path: Path, quarter: str, source_sha256: str, out_dir: Path) -> dict:
    """Write one typed Parquet file per table: `<out_dir>/<table>/<quarter>.parquet`."""
    counts = {}
    for table in TABLES:
        raw = read_raw_table(zip_path, table)
        if raw is None:
            continue
        typed = type_table(raw).with_columns(
            pl.lit(quarter).alias("source_quarter"),
            pl.lit(source_sha256).alias("source_sha256"),
        )
        dest = out_dir / table.lower() / f"{quarter}.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        typed.write_parquet(dest)
        counts[table] = typed.height
    return counts


def load_staged(staging_dir: Path, table: str, quarters: list[str] | None = None) -> pl.DataFrame:
    files = sorted((staging_dir / table.lower()).glob("*.parquet"))
    if quarters is not None:
        files = [f for f in files if f.stem in set(quarters)]
    if not files:
        raise FileNotFoundError(f"no staged files for {table} in {staging_dir}")
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
