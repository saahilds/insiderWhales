"""Market features at the moment of a disclosure: trailing returns, drawdown, volatility, volume.

Everything is computed from bars strictly before the entry session, i.e. what a trader could
have seen when the signal fired.
"""

from __future__ import annotations

import polars as pl

RETURN_WINDOWS = (5, 20, 60, 252)
VOL_WINDOW = 60
ADV_WINDOW = 20
REL_VOLUME_WINDOWS = (20, 120)
BAR_SLACK_DAYS = 10  # the last bar before entry must be recent, or the stock is not trading


def daily_market_features(adjusted: pl.DataFrame) -> pl.DataFrame:
    """One row per symbol and bar with trailing features (null until the window is full)."""
    p = adjusted.sort("symbol", "date")
    over = "symbol"
    exprs: list[pl.Expr] = []
    for w in RETURN_WINDOWS:
        exprs.append(
            (pl.col("adj_close") / pl.col("adj_close").shift(w).over(over) - 1).alias(
                f"mkt_ret_{w}"
            )
        )
    log_ret = (pl.col("adj_close") / pl.col("adj_close").shift(1).over(over)).log()
    exprs += [
        (
            pl.col("adj_close") / pl.col("adj_close").rolling_max(252, min_samples=60).over(over)
            - 1
        ).alias("mkt_drawdown_252"),
        (log_ret.rolling_std(VOL_WINDOW, min_samples=VOL_WINDOW).over(over) * (252**0.5)).alias(
            "mkt_volatility_60"
        ),
        pl.col("dollar_volume")
        .rolling_mean(ADV_WINDOW, min_samples=ADV_WINDOW)
        .over(over)
        .alias("mkt_adv_20"),
        (
            pl.col("adj_volume")
            .rolling_mean(REL_VOLUME_WINDOWS[0], min_samples=REL_VOLUME_WINDOWS[0])
            .over(over)
            / pl.col("adj_volume")
            .rolling_mean(REL_VOLUME_WINDOWS[1], min_samples=REL_VOLUME_WINDOWS[1])
            .over(over)
        ).alias("mkt_relative_volume"),
    ]
    cols = [f"mkt_ret_{w}" for w in RETURN_WINDOWS] + [
        "mkt_drawdown_252",
        "mkt_volatility_60",
        "mkt_adv_20",
        "mkt_relative_volume",
    ]
    return p.with_columns(exprs).select("symbol", "date", "adj_close", *cols)


def market_features_asof(
    events: pl.DataFrame, daily: pl.DataFrame, benchmark_daily: pl.DataFrame
) -> pl.DataFrame:
    """Attach features from the last bar before each event's entry session.

    `events` needs `event_id`, `symbol`, `entry_session` and `trans_date`. Adds the stock's
    trailing return minus SPY's (`mkt_rel_ret_*`) and the split-adjusted close on the
    transaction date (`trade_adj_close`), the base for the return from transaction to entry.
    """
    e = events.select("event_id", "symbol", "entry_session", "trans_date").filter(
        pl.col("symbol").is_not_null() & pl.col("entry_session").is_not_null()
    )
    known_by = (pl.col("entry_session") - pl.duration(days=1)).alias("_known_by")
    e = e.with_columns(known_by).sort("symbol", "_known_by")
    feats = daily.drop("adj_close").rename({"date": "market_bar_date"})
    out = e.join_asof(
        feats.sort("symbol", "market_bar_date"),
        left_on="_known_by",
        right_on="market_bar_date",
        by="symbol",
        strategy="backward",
        tolerance=f"{BAR_SLACK_DAYS}d",
        check_sortedness=False,
    )
    spy = (
        benchmark_daily.filter(pl.col("symbol") == "SPY")
        .select("date", *[c for c in daily.columns if c.startswith("mkt_ret_")])
        .rename({c: f"_spy_{c}" for c in daily.columns if c.startswith("mkt_ret_")})
        .sort("date")
    )
    out = out.sort("_known_by").join_asof(
        spy, left_on="_known_by", right_on="date", strategy="backward", check_sortedness=False
    )
    for w in RETURN_WINDOWS:
        out = out.with_columns(
            (pl.col(f"mkt_ret_{w}") - pl.col(f"_spy_mkt_ret_{w}")).alias(f"mkt_rel_ret_{w}")
        )
    close_on_trade = daily.select(
        "symbol", pl.col("date").alias("_trade_bar"), pl.col("adj_close").alias("_trade_close")
    ).sort("symbol", "_trade_bar")
    out = out.sort("symbol", "trans_date").join_asof(
        close_on_trade,
        left_on="trans_date",
        right_on="_trade_bar",
        by="symbol",
        strategy="backward",
        tolerance="5d",
        check_sortedness=False,
    )
    out = out.rename({"_trade_close": "trade_adj_close"})
    keep = [
        "event_id",
        "market_bar_date",
        *[f"mkt_ret_{w}" for w in RETURN_WINDOWS],
        *[f"mkt_rel_ret_{w}" for w in RETURN_WINDOWS],
        "mkt_drawdown_252",
        "mkt_volatility_60",
        "mkt_adv_20",
        "mkt_relative_volume",
        "trade_adj_close",
    ]
    return out.select(keep)
