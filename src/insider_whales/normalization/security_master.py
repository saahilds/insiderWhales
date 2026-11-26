"""Security master v0: ticker windows from Form 4 history, share classes, recycled-ticker guards."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import yaml

from insider_whales.normalization.securities import (
    provisional_security_id,
    security_class_expr,
    share_class_expr,
)
from insider_whales.normalization.tickers import parse_reported_tickers

PLACEHOLDER_TICKERS = ("", "NONE", "N/A", "NA", "-", "NOT APPLICABLE", "NOT LISTED")


def load_overrides(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return yaml.safe_load(path.read_text()) or []


WINDOW_GRACE_DAYS = (
    400  # price history kept after an issuer's last filing (covers 120-day horizons)
)
WINDOW_LOOKBACK_DAYS = 365  # price history kept before an issuer's first filing (feature lookback)
OPEN_WINDOW_DAYS = 365  # an issuer that filed in the last year is still considered listed
WINDOW_SCHEMA = {
    "issuer_cik": pl.String,
    "ticker": pl.String,
    "track": pl.String,
    "ticker_class": pl.String,
    "valid_from": pl.Date,
    "valid_to": pl.Date,
    "first_seen": pl.Date,
    "last_seen": pl.Date,
    "n_filings": pl.UInt32,
    "source": pl.String,
    "confidence": pl.String,
}


def _parsed_symbols(raw_values: list[str]) -> pl.DataFrame:
    """One row per ticker found in each distinct reported-symbol string."""
    rows = []
    for raw in raw_values:
        parsed = parse_reported_tickers(raw)
        for pos, p in enumerate(parsed):
            rows.append((raw, pos, len(parsed), p.ticker, p.share_class))
    return pl.DataFrame(
        rows,
        schema={
            "raw": pl.String,
            "pos": pl.Int32,
            "n_tickers": pl.Int32,
            "ticker": pl.String,
            "parsed_class": pl.String,
        },
        orient="row",
    )


def ticker_windows(filings: pl.DataFrame, data_end: date | None = None) -> pl.DataFrame:
    """Point-in-time ticker history per issuer from Form 3/4/5 filings.

    The reported symbol is cleaned (`QADA, QADB`, `NYSE: SCS`, ...) into tickers. Each issuer
    has one *track* per share class (or per position when no class is implied); a track's
    contiguous runs of the same ticker become validity windows. A window ends the day before
    the track's next ticker starts; the last one stays open while the issuer is still filing and
    otherwise ends `WINDOW_GRACE_DAYS` after its last filing, so a ticker reused by a later
    company cannot be matched to this issuer's old events.
    """
    base = filings.select(
        "issuer_cik",
        pl.col("reported_ticker").fill_null("").str.strip_chars().str.to_uppercase().alias("raw"),
        "filing_date",
    ).filter(pl.col("raw") != "")
    if base.height == 0:
        return pl.DataFrame(schema=WINDOW_SCHEMA)
    data_end = data_end or base["filing_date"].max()
    parsed = _parsed_symbols(base["raw"].unique().to_list())
    rows = base.join(parsed, on="raw")
    if rows.height == 0:
        return pl.DataFrame(schema=WINDOW_SCHEMA)

    # A ticker that was labelled with a class anywhere keeps it in single-ticker filings.
    known_class = (
        rows.filter(pl.col("parsed_class").is_not_null())
        .group_by("issuer_cik", "ticker")
        .agg(pl.col("parsed_class").first().alias("known_class"))
    )
    rows = (
        rows.join(known_class, on=["issuer_cik", "ticker"], how="left")
        .with_columns(pl.coalesce("parsed_class", "known_class").alias("ticker_class"))
        .with_columns(
            pl.coalesce(
                "ticker_class", pl.concat_str([pl.lit("#"), pl.col("pos").cast(pl.String)])
            ).alias("track")
        )
    )
    daily = (
        rows.group_by("issuer_cik", "track", "ticker", "ticker_class", "filing_date")
        .agg(pl.len().alias("n"))
        .sort(["issuer_cik", "track", "filing_date", "ticker"])
    )
    changed = (
        (pl.col("ticker") != pl.col("ticker").shift(1))
        | (pl.col("issuer_cik") != pl.col("issuer_cik").shift(1))
        | (pl.col("track") != pl.col("track").shift(1))
    ).fill_null(True)
    runs = (
        daily.with_columns(changed.cum_sum().alias("_run"))
        .group_by("_run", maintain_order=True)
        .agg(
            pl.col("issuer_cik").first(),
            pl.col("track").first(),
            pl.col("ticker").first(),
            pl.col("ticker_class").first(),
            pl.col("filing_date").min().alias("first_seen"),
            pl.col("filing_date").max().alias("last_seen"),
            pl.col("n").sum().alias("n_filings"),
        )
        .drop("_run")
        .sort(["issuer_cik", "track", "first_seen"])
    )
    nxt = pl.col("first_seen").shift(-1).over("issuer_cik", "track")
    prv = pl.col("last_seen").shift(1).over("issuer_cik", "track")
    still_filing = pl.col("last_seen") >= pl.lit(data_end - timedelta(days=OPEN_WINDOW_DAYS))
    return runs.with_columns(
        pl.when(prv.is_null())
        .then(pl.col("first_seen") - pl.duration(days=WINDOW_LOOKBACK_DAYS))
        .otherwise(pl.col("first_seen"))
        .alias("valid_from"),
        pl.when(nxt.is_not_null())
        .then(nxt - pl.duration(days=1))
        .when(still_filing)
        .then(pl.lit(None, dtype=pl.Date))
        .otherwise(pl.col("last_seen") + pl.duration(days=WINDOW_GRACE_DAYS))
        .alias("valid_to"),
        pl.lit("form345").alias("source"),
        pl.lit("medium").alias("confidence"),
    ).select(list(WINDOW_SCHEMA))


def _security_order(securities: pl.DataFrame) -> pl.DataFrame:
    return securities.with_columns(
        pl.col("security_type")
        .replace_strict({"common": 0, "adr": 1, "unknown": 2}, default=3, return_dtype=pl.Int8)
        .alias("_rank")
    ).sort(
        ["issuer_cik", "_rank", "n_lines", "security_id"], descending=[False, False, True, False]
    )


def map_tracks_to_securities(windows: pl.DataFrame, securities: pl.DataFrame) -> pl.DataFrame:
    """Attach each ticker track to the issuer's security of the same share class.

    A class label (QADA is Class A) selects the security with that share class. A position
    track `#0` is the issuer's largest common-equity security, `#1` the next one, and so on.
    Anything that matches nothing falls back to the largest security at low confidence, so a
    price series is never silently dropped.
    """
    ordered = _security_order(securities)
    by_issuer: dict[str, list[dict]] = {}
    for r in ordered.iter_rows(named=True):
        by_issuer.setdefault(r["issuer_cik"], []).append(r)
    out = []
    for w in windows.iter_rows(named=True):
        secs = by_issuer.get(w["issuer_cik"])
        if not secs:
            continue
        track, conf = w["track"], w["confidence"]
        if not track.startswith("#"):
            match = [s for s in secs if s["share_class"] == track]
            chosen = match[0] if match else secs[0]
            conf = conf if match else "low"
        else:
            k = int(track[1:])
            plain = [s for s in secs if s["share_class"] in (None, "COMMON")]
            ordered_secs = plain + [s for s in secs if s not in plain]
            chosen = ordered_secs[k] if k < len(ordered_secs) else ordered_secs[0]
            conf = conf if k < len(ordered_secs) else "low"
        out.append({**w, "security_id": chosen["security_id"], "confidence": conf})
    return pl.DataFrame(out, schema={**WINDOW_SCHEMA, "security_id": pl.String}, orient="row")


def _fill_unmapped_securities(id_rows: pl.DataFrame, securities: pl.DataFrame) -> pl.DataFrame:
    """Common-equity securities with no ticker of their own reuse the issuer's main listing.

    A filing that titles its holdings plain "Common Stock" while the issuer lists Class A and
    Class B shares cannot be told apart from the text; the issuer's largest mapped security
    stands in, at low confidence, so the event keeps a price instead of silently losing it.
    """
    mapped = set(id_rows["security_id"].unique().to_list())
    best = (
        securities.filter(pl.col("security_id").is_in(list(mapped)))
        .sort("n_lines", descending=True)
        .group_by("issuer_cik", maintain_order=True)
        .first()
        .select("issuer_cik", pl.col("security_id").alias("donor"))
    )
    orphans = securities.filter(
        ~pl.col("security_id").is_in(list(mapped))
        & pl.col("security_type").is_in(["common", "adr", "unknown"])
    ).join(best, on="issuer_cik")
    if orphans.height == 0:
        return id_rows
    copies = (
        orphans.select("security_id", "donor")
        .join(id_rows.rename({"security_id": "donor"}), on="donor")
        .drop("donor")
        .with_columns(
            pl.lit("issuer_fallback").alias("source"),
            pl.lit("low").alias("confidence"),
        )
    )
    return pl.concat([id_rows, copies.select(id_rows.columns)])


def build_securities(
    filings: pl.DataFrame,
    transactions: pl.DataFrame,
    overrides: list[dict] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build securities and security_identifiers tables from Form 3/4/5 data.

    Returns (securities, identifiers). A security_id is `<issuer_cik>:<share class or COMMON>`.
    Identifiers carry ticker validity windows so a recycled ticker cannot attach to the wrong
    company.
    """
    overrides = overrides or []
    titles = (
        transactions.filter(pl.col("table") == "nonderiv")
        .group_by("issuer_cik", "security_title")
        .agg(pl.len().alias("n_lines"))
        .with_columns(security_class_expr(), share_class_expr())
        .with_columns(provisional_security_id("issuer_cik"))
    )
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
            pl.col("n_lines").sum().alias("n_lines"),
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
    mapped = map_tracks_to_securities(windows, securities)
    id_rows = mapped.select(
        "security_id",
        pl.lit("ticker").alias("id_type"),
        pl.col("ticker").alias("value"),
        "valid_from",
        "valid_to",
        "source",
        "confidence",
        "track",
        "first_seen",
        "last_seen",
    )

    id_rows = _fill_unmapped_securities(id_rows, securities)

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
