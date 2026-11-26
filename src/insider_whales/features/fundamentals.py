"""Point-in-time fundamentals: what an investor could have known on the day of a signal.

SEC XBRL facts carry the date each filing was made. A value is usable from the day after that
date; a later restatement is a new version that only counts from its own filing date. Annual
(10-K) data only, so the latest financials can be up to a year old; older than
`MAX_AGE_DAYS` they are treated as missing instead of being used silently.
"""

from __future__ import annotations

import polars as pl

from insider_whales.ingestion.fundamentals import FLOW_METRICS, INSTANT_METRICS

MAX_AGE_DAYS = 540  # a 10-K is due about 90 days after year end, so 15 months plus slack
MAX_SHARES_AGE_DAYS = 800
LAG_DAYS = 1  # a filing dated D is usable on D + LAG_DAYS
METRICS = [*FLOW_METRICS, *INSTANT_METRICS]
LEVEL_COLUMNS = [
    "revenue",
    "net_income",
    "op_cash_flow",
    "capex",
    "assets",
    "liabilities",
    "equity",
    "cash",
    "debt",
]
NY = "America/New_York"


def signal_date(available_at: pl.Expr) -> pl.Expr:
    """New York calendar date on which a disclosure became available."""
    return available_at.dt.convert_time_zone(NY).dt.date()


def annual_snapshots(facts: pl.DataFrame) -> pl.DataFrame:
    """One row per (issuer, filing, fiscal year): each metric as that filing reported it, plus
    the prior-year value from the same filing (so growth never mixes versions).

    Rows are limited to filings' own newest fiscal year and to filings that move the issuer's
    fiscal year forward, so a late amendment of an old year cannot replace newer figures.
    """
    best = (
        facts.sort("tag_rank")
        .group_by("issuer_cik", "accession_number", "metric", "period_end", maintain_order=True)
        .agg(pl.col("value").first(), pl.col("filed").first(), pl.col("form").first())
    )
    wide = best.pivot(
        on="metric",
        index=["issuer_cik", "accession_number", "period_end", "filed", "form"],
        values="value",
        aggregate_function="first",
    ).sort("issuer_cik", "accession_number", "period_end")
    for m in METRICS:
        if m not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(m))
    wide = wide.with_columns(
        pl.when(pl.col("debt_total").is_not_null())
        .then(pl.col("debt_total"))
        .when(pl.col("debt_noncurrent").is_not_null())
        .then(pl.col("debt_noncurrent") + pl.col("debt_current").fill_null(0.0))
        .otherwise(pl.col("debt_current"))
        .alias("debt")
    )
    same_filing = ["issuer_cik", "accession_number"]
    gap = (pl.col("period_end") - pl.col("period_end").shift(1).over(same_filing)).dt.total_days()
    one_year_back = gap.is_between(340, 390)
    prior = [
        pl.when(one_year_back)
        .then(pl.col(m).shift(1).over(same_filing))
        .otherwise(None)
        .alias(f"{m}_prior")
        for m in ("revenue", "net_income", "assets", "equity")
    ]
    wide = wide.with_columns(prior)
    newest = wide.filter(pl.col("period_end") == pl.col("period_end").max().over(same_filing))
    newest = newest.sort("issuer_cik", "filed", "period_end", "accession_number")
    running_max = pl.col("period_end").cum_max().shift(1).over("issuer_cik")
    moves_forward = newest.filter(running_max.is_null() | (pl.col("period_end") >= running_max))
    # One row per filing date (a 10-K and an amendment on the same day: the later accession).
    return moves_forward.group_by("issuer_cik", "filed", maintain_order=True).last()


def usable_before(available_at: pl.Expr) -> pl.Expr:
    """The latest filing date whose content a disclosure at `available_at` may use."""
    return signal_date(available_at) - pl.duration(days=LAG_DAYS)


def fundamentals_asof(events: pl.DataFrame, snapshots: pl.DataFrame) -> pl.DataFrame:
    """Latest annual financials known before each event's disclosure (event_id + columns)."""
    left = events.select(
        "event_id", "issuer_cik", usable_before(pl.col("available_at")).alias("_known_by")
    ).sort("issuer_cik", "_known_by")
    right = snapshots.sort("issuer_cik", "filed").rename({"filed": "fundamentals_filed"})
    out = left.join_asof(
        right,
        left_on="_known_by",
        right_on="fundamentals_filed",
        by="issuer_cik",
        strategy="backward",
        check_sortedness=False,
    )
    age = (pl.col("_known_by") - pl.col("fundamentals_filed")).dt.total_days()
    stale = age > MAX_AGE_DAYS
    keep = [c for c in out.columns if c not in {"issuer_cik", "_known_by", "accession_number"}]
    return (
        out.with_columns(
            *(
                pl.when(stale).then(None).otherwise(pl.col(c)).alias(c)
                for c in keep
                if c not in {"event_id", "fundamentals_filed", "period_end", "form"}
            ),
            age.alias("fundamentals_age_days"),
        )
        .select("event_id", *[c for c in keep if c != "event_id"], "fundamentals_age_days")
        .rename({"period_end": "fundamentals_period_end", "form": "fundamentals_form"})
    )


def shares_asof(events: pl.DataFrame, shares: pl.DataFrame) -> pl.DataFrame:
    """Cover-page shares outstanding known before each disclosure (latest filing at most
    `MAX_SHARES_AGE_DAYS` old)."""
    s = (
        shares.filter(pl.col("filed").is_not_null())
        .select("issuer_cik", "filed", "shares", "multi_class")
        .sort("issuer_cik", "filed")
    )
    e = events.select(
        "event_id", "issuer_cik", usable_before(pl.col("available_at")).alias("_known_by")
    ).sort("issuer_cik", "_known_by")
    out = e.join_asof(
        s,
        left_on="_known_by",
        right_on="filed",
        by="issuer_cik",
        strategy="backward",
        tolerance=f"{MAX_SHARES_AGE_DAYS}d",
        check_sortedness=False,
    )
    return out.select("event_id", pl.col("shares").alias("shares_outstanding"), "multi_class")


def _ratio(num: pl.Expr, den: pl.Expr, den_positive: bool = True) -> pl.Expr:
    ok = den > 0 if den_positive else den != 0
    return pl.when(ok).then(num / den).otherwise(None)


def fundamental_features(
    events: pl.DataFrame, snapshots: pl.DataFrame, shares: pl.DataFrame
) -> pl.DataFrame:
    """Per-event fundamentals as known at disclosure, with ratios against the event's price."""
    f = fundamentals_asof(events, snapshots)
    sh = shares_asof(events, shares)
    price = events.select("event_id", pl.col("vwap").alias("_price"))
    out = f.join(sh, on="event_id", how="left").join(price, on="event_id", how="left")
    mcap = (pl.col("shares_outstanding") * pl.col("_price")).alias("market_cap")
    out = out.with_columns(mcap).with_columns(
        pl.coalesce("liabilities", pl.col("assets") - pl.col("equity")).alias("liabilities")
    )
    out = out.with_columns(
        _ratio(pl.col("revenue") - pl.col("revenue_prior"), pl.col("revenue_prior")).alias(
            "revenue_growth"
        ),
        _ratio(
            pl.col("net_income") - pl.col("net_income_prior"),
            pl.col("net_income_prior").abs(),
            den_positive=True,
        ).alias("earnings_growth"),
        (pl.col("op_cash_flow") - pl.col("capex")).alias("free_cash_flow"),
        _ratio(pl.col("net_income"), pl.col("revenue")).alias("net_margin"),
        _ratio(pl.col("net_income"), pl.col("equity")).alias("return_on_equity"),
        (pl.col("debt") - pl.col("cash")).alias("net_debt"),
        _ratio(pl.col("liabilities"), pl.col("equity")).alias("debt_to_equity"),
        _ratio(pl.col("market_cap"), pl.col("net_income")).alias("price_to_earnings"),
        _ratio(pl.col("market_cap"), pl.col("revenue")).alias("price_to_sales"),
        _ratio(pl.col("market_cap"), pl.col("equity")).alias("price_to_book"),
    )
    out = out.with_columns(
        _ratio(pl.col("free_cash_flow"), pl.col("market_cap")).alias("fcf_yield"),
        (
            pl.col("revenue").is_not_null()
            & pl.col("net_income").is_not_null()
            & pl.col("equity").is_not_null()
        ).alias("fundamentals_available"),
    )
    drop = [f"{m}_prior" for m in ("revenue", "net_income", "assets", "equity")] + [
        "debt_total",
        "debt_noncurrent",
        "debt_current",
        "_price",
    ]
    return out.drop([c for c in drop if c in out.columns])
