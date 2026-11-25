"""Security master v0: ticker windows from Form 4 history, share classes, recycled-ticker guards."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import yaml

from insider_whales.normalization.securities import (
    provisional_security_id,
    security_class_expr,
    share_class_expr,
)

PLACEHOLDER_TICKERS = ("", "NONE", "N/A", "NA", "-", "NOT APPLICABLE", "NOT LISTED")


def load_overrides(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return yaml.safe_load(path.read_text()) or []


def ticker_windows(filings: pl.DataFrame) -> pl.DataFrame:
    """Point-in-time ticker history per issuer CIK from Form 3/4/5 filings.

    Each contiguous run of the same reported ticker becomes a validity window. Placeholder
    tickers are dropped. Adjacent windows for the same ticker are merged.
    """
    rows = (
        filings.filter(
            ~pl.col("reported_ticker").fill_null("").str.to_uppercase().is_in(PLACEHOLDER_TICKERS)
        )
        .select(
            "issuer_cik",
            pl.col("reported_ticker").str.to_uppercase().alias("ticker"),
            "filing_date",
        )
        .unique()
        .sort(["issuer_cik", "filing_date", "ticker"])
    )
    if rows.height == 0:
        return pl.DataFrame(
            schema={
                "issuer_cik": pl.String,
                "ticker": pl.String,
                "valid_from": pl.Date,
                "valid_to": pl.Date,
                "source": pl.String,
                "confidence": pl.String,
            }
        )
    # Collapse consecutive identical ticker observations into one window.
    changed = (
        (pl.col("ticker") != pl.col("ticker").shift(1).over("issuer_cik"))
        | (pl.col("issuer_cik") != pl.col("issuer_cik").shift(1))
    ).fill_null(True)
    windows = (
        rows.with_columns(changed.cum_sum().alias("_run"))
        .group_by(["issuer_cik", "ticker", "_run"])
        .agg(
            pl.col("filing_date").min().alias("valid_from"),
            pl.col("filing_date").max().alias("valid_to"),
        )
        .drop("_run")
        .sort(["issuer_cik", "valid_from", "ticker"])
    )
    # Extend each window's valid_to to the day before the next window for that issuer
    # (or leave open when it is the latest).
    next_from = windows.with_columns(
        pl.col("valid_from").shift(-1).over("issuer_cik").alias("_next")
    )
    return next_from.with_columns(
        pl.when(pl.col("_next").is_not_null())
        .then(pl.col("_next") - pl.duration(days=1))
        .otherwise(pl.lit(None, dtype=pl.Date))
        .alias("valid_to"),
        pl.lit("form345").alias("source"),
        pl.lit("medium").alias("confidence"),
    ).drop("_next")


def build_securities(
    filings: pl.DataFrame,
    transactions: pl.DataFrame,
    overrides: list[dict] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build securities and security_identifiers tables from Form 4 data.

    Returns (securities, identifiers). A security_id is `<issuer_cik>:<share class or COMMON>`.
    Identifiers carry ticker validity windows so a recycled ticker cannot attach to the wrong
    company.
    """
    overrides = overrides or []
    titles = (
        transactions.filter(pl.col("table") == "nonderiv")
        .select("issuer_cik", "security_title")
        .unique()
        .with_columns(security_class_expr(), share_class_expr())
        .with_columns(provisional_security_id("issuer_cik"))
    )
    # Prefer common equity titles when an issuer has several; keep every security_id.
    securities = (
        titles.group_by("security_id")
        .agg(
            pl.col("issuer_cik").first(),
            pl.col("share_class").drop_nulls().first().alias("share_class"),
            pl.col("security_class")
            .sort_by(
                pl.col("security_class").replace_strict(
                    {
                        "common": 0,
                        "adr": 1,
                        "unit": 2,
                        "unknown": 3,
                        "preferred": 4,
                        "warrant": 5,
                        "derivative_like": 6,
                        "debt": 7,
                        "right": 8,
                    },
                    default=9,
                    return_dtype=pl.Int8,
                )
            )
            .first()
            .alias("security_type"),
            pl.col("security_title").sort().first().alias("title_example"),
        )
        .with_columns(
            pl.lit(None, dtype=pl.Date).alias("first_trade"),
            pl.lit(None, dtype=pl.Date).alias("last_trade"),
            pl.lit(None, dtype=pl.Date).alias("delist_date"),
            pl.lit(None, dtype=pl.String).alias("delist_reason"),
            pl.lit(False).alias("listed"),
        )
        .sort("security_id")
    )

    windows = ticker_windows(filings)
    # Attach each ticker window to the issuer's COMMON security by default; share-class
    # overrides (GOOG vs GOOGL) come from the overrides file.
    common = securities.filter(
        pl.col("share_class").is_null() | (pl.col("share_class") == "COMMON")
    )
    if common.height == 0:
        common = securities
    id_rows = windows.join(
        common.select("issuer_cik", "security_id"), on="issuer_cik", how="left"
    ).select(
        "security_id",
        pl.lit("ticker").alias("id_type"),
        pl.col("ticker").alias("value"),
        "valid_from",
        "valid_to",
        "source",
        "confidence",
    )

    # Apply manual overrides (ticker remaps, issuer CIK remaps, dual-class mappings).
    override_rows = []
    for o in overrides:
        override_rows.append(
            {
                "security_id": o.get("security_id")
                or f"{o['issuer_cik']}:{o.get('share_class', 'COMMON')}",
                "id_type": o.get("id_type", "ticker"),
                "value": o["value"].upper()
                if o.get("id_type", "ticker") == "ticker"
                else o["value"],
                "valid_from": date.fromisoformat(o["valid_from"]) if o.get("valid_from") else None,
                "valid_to": date.fromisoformat(o["valid_to"]) if o.get("valid_to") else None,
                "source": "override",
                "confidence": "high",
            }
        )
    if override_rows:
        id_rows = pl.concat([id_rows, pl.DataFrame(override_rows)], how="diagonal_relaxed")

    # Mark securities as listed when they have any non-placeholder ticker window.
    listed_ids = set(id_rows.filter(pl.col("security_id").is_not_null())["security_id"].to_list())
    securities = securities.with_columns(
        pl.col("security_id").is_in(list(listed_ids)).alias("listed")
    )
    return securities, id_rows.filter(pl.col("security_id").is_not_null()).sort(
        ["security_id", "valid_from", "value"]
    )


def resolve_symbol(
    identifiers: pl.DataFrame,
    symbol: str,
    as_of: date,
) -> pl.DataFrame:
    """Security identifiers whose ticker equals `symbol` and whose window covers `as_of`.

    Recycled tickers return at most one row per overlapping window; callers that get more
    than one must treat that as an error (test T11).
    """
    return identifiers.filter(
        (pl.col("id_type") == "ticker")
        & (pl.col("value") == symbol.upper())
        & (pl.col("valid_from") <= as_of)
        & (pl.col("valid_to").is_null() | (pl.col("valid_to") >= as_of))
    )


def attach_prices_to_security(
    prices: pl.DataFrame,
    identifiers: pl.DataFrame,
    security_id: str,
) -> pl.DataFrame:
    """Keep only price rows whose dates fall inside a ticker window of `security_id`.

    Rows outside every window are dropped (they belong to a different company that later
    reused the ticker). Raises if the same date would map to two windows.
    """
    windows = identifiers.filter(
        (pl.col("security_id") == security_id) & (pl.col("id_type") == "ticker")
    )
    if windows.height == 0:
        return prices.clear()
    # Join prices to every overlapping window, then reject dates that match more than one.
    joined = prices.join(windows.select("value", "valid_from", "valid_to"), how="cross").filter(
        (pl.col("symbol").str.to_uppercase() == pl.col("value"))
        & (pl.col("date") >= pl.col("valid_from"))
        & (pl.col("valid_to").is_null() | (pl.col("date") <= pl.col("valid_to")))
    )
    dupes = joined.group_by("date").len().filter(pl.col("len") > 1)
    if dupes.height:
        raise ValueError(
            f"security {security_id}: overlapping ticker windows on {dupes['date'].to_list()[:5]}"
        )
    return joined.select(prices.columns).unique().sort("date")
