"""Insider history features: what this insider had done, and how it turned out, before a signal.

History is per insider and issuer (`insider_group_id`) and ordered by when each disclosure became
public (`available_at`). Only disclosures strictly earlier than the signal count, so a filing
that reports several transactions never feeds itself. Outcomes of past trades are used only once
their holding period has completely elapsed before the signal (spec test 13).
"""

from __future__ import annotations

import polars as pl

from insider_whales.features.fundamentals import signal_date

KEY = "insider_group_id"
LONG_WINDOW = "36500d"
ROUTINE_YEARS = 3
PROFIT_HORIZON = 60


def trailing_counts(events: pl.DataFrame) -> pl.DataFrame:
    """Counts, value statistics and recency of an insider's earlier disclosed trades."""
    e = events.select(
        "event_id",
        KEY,
        "trans_code",
        "available_at",
        "value",
    ).sort(KEY, "available_at", "event_id")
    is_p = (pl.col("trans_code") == "P").cast(pl.Int32)
    is_s = (pl.col("trans_code") == "S").cast(pl.Int32)
    ts = pl.col("available_at").dt.epoch("s").cast(pl.Float64)
    by = "available_at"

    def roll(expr: pl.Expr, window: str, fn: str = "sum") -> pl.Expr:
        method = {
            "sum": expr.rolling_sum_by,
            "max": expr.rolling_max_by,
            "median": expr.rolling_median_by,
        }[fn]
        return method(by, window, closed="left").over(KEY)

    out = e.with_columns(
        roll(is_p, "365d").alias("n_purchases_12m"),
        roll(is_s, "365d").alias("n_sales_12m"),
        roll(is_p, "1095d").alias("n_purchases_36m"),
        roll(pl.col("value"), LONG_WINDOW, "median").alias("hist_median_value"),
        roll(pl.col("value"), LONG_WINDOW, "max").alias("hist_max_value"),
        roll(is_p + is_s, LONG_WINDOW).alias("n_prior_events"),
        roll(pl.when(is_p == 1).then(ts), LONG_WINDOW, "max").alias("_last_p"),
        roll(pl.when(is_s == 1).then(ts), LONG_WINDOW, "max").alias("_last_s"),
    )
    now = ts
    return out.with_columns(
        ((now - pl.col("_last_p")) / 86400).alias("days_since_last_purchase"),
        ((now - pl.col("_last_s")) / 86400).alias("days_since_last_sale"),
    ).select(
        "event_id",
        "n_purchases_12m",
        "n_sales_12m",
        "n_purchases_36m",
        "n_prior_events",
        "hist_median_value",
        "hist_max_value",
        "days_since_last_purchase",
        "days_since_last_sale",
    )


def routine_classification(events: pl.DataFrame, years: int = ROUTINE_YEARS) -> pl.DataFrame:
    """Routine vs opportunistic trader, from calendar-month regularity.

    An insider is `routine` for a trade when they also traded in the same calendar month in each
    of the previous `years` years; `opportunistic` when they have at least `years` years of
    earlier history without that pattern; `unclassified` when there is not enough history to say.
    """
    e = events.select(
        "event_id",
        KEY,
        pl.col("trans_date").dt.year().alias("_year"),
        pl.col("trans_date").dt.month().alias("_month"),
        "available_at",
    )
    months = e.select(KEY, "_year", "_month").unique()
    out = e
    for k in range(1, years + 1):
        shifted = months.with_columns((pl.col("_year") + k).alias("_year")).with_columns(
            pl.lit(True).alias(f"_had_{k}")
        )
        out = out.join(shifted, on=[KEY, "_year", "_month"], how="left")
    first_year = e.sort(KEY, "available_at").with_columns(
        pl.col("_year")
        .rolling_min_by("available_at", LONG_WINDOW, closed="left")
        .over(KEY)
        .alias("_first_year")
    )
    out = out.join(first_year.select("event_id", "_first_year"), on="event_id", how="left")
    routine = pl.all_horizontal(
        *[pl.col(f"_had_{k}").fill_null(False) for k in range(1, years + 1)]
    )
    enough = (pl.col("_year") - pl.col("_first_year")) >= years
    return out.select(
        "event_id",
        pl.when(routine)
        .then(pl.lit("routine"))
        .when(enough)
        .then(pl.lit("opportunistic"))
        .otherwise(pl.lit("unclassified"))
        .alias("trader_type"),
    )


def mature_profitability(
    events: pl.DataFrame, outcomes: pl.DataFrame, horizon: int = PROFIT_HORIZON
) -> pl.DataFrame:
    """Average signed return of an insider's earlier trades whose outcome was already known.

    A trade counts for a later signal only if its whole evaluation horizon (entry to the exit
    `horizon` sessions later) ended before the signal's date. The signed return is the return
    after a purchase and minus the return after a sale (a sale is right when the price falls).
    Trades whose price series has no exit bar are not counted.
    """
    ret, exit_ = f"ret_{horizon}", f"exit_date_{horizon}"
    past = (
        events.select("event_id", KEY, "available_at", "trans_code")
        .join(outcomes.select("event_id", ret, exit_), on="event_id")
        .filter(pl.col(ret).is_not_null() & pl.col(exit_).is_not_null())
        .select(
            KEY,
            pl.col("available_at").alias("_past_available"),
            pl.col(exit_).alias("_past_exit"),
            pl.when(pl.col("trans_code") == "P")
            .then(pl.col(ret))
            .otherwise(-pl.col(ret))
            .alias("_signed"),
        )
    )
    now = events.select(
        "event_id", KEY, "available_at", signal_date(pl.col("available_at")).alias("_today")
    )
    pairs = now.join(past, on=KEY).filter(
        (pl.col("_past_available") < pl.col("available_at"))
        & (pl.col("_past_exit") < pl.col("_today"))
    )
    agg = pairs.group_by("event_id").agg(
        pl.len().alias("n_mature_observations"),
        pl.col("_signed").mean().alias("hist_mean_signed_return"),
        (pl.col("_signed") > 0).mean().alias("hist_win_rate"),
    )
    return (
        events.select("event_id")
        .join(agg, on="event_id", how="left")
        .with_columns(pl.col("n_mature_observations").fill_null(0))
    )
