"""Forward returns of events: entry at the first executable price, exit `h` sessions later.

Entry price is the open (or close) of the entry session; the exit is the close `h` sessions after
the entry session, taken from the last bar on or before that date within `EXIT_SLACK_DAYS`.
Returns include cash dividends through the total-return index; a position bought at the open
does not receive a dividend whose ex-date is the entry day.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from insider_whales.calendar import TradingCalendar

EXIT_SLACK_DAYS = 5
ENTRY_SLACK_DAYS = 7


def _finite_or_null(expr: pl.Expr) -> pl.Expr:
    return pl.when(expr.is_finite()).then(expr)


def forward_returns(
    events: pl.DataFrame,
    adjusted: pl.DataFrame,
    calendar: TradingCalendar,
    horizons: tuple[int, ...] | list[int],
    mode: str = "open",
    censor_after: date | None = None,
) -> pl.DataFrame:
    """Per event: `entry_date`, `entry_price` (split-adjusted), and for each horizon `h` the
    exit date `exit_date_{h}` and return `ret_{h}` (null when the series has no bar then).

    `events` needs `event_id`, `symbol` and `entry_session`. With `censor_after`, no price after
    that date is ever read: an exit later than it gives a null return, and so does an event
    whose entry is later (this is how the validation and final-test periods stay unseen).
    """
    bars = adjusted.select("symbol", "date", "adj_open", "adj_close", "tr_index").sort(
        "symbol", "date"
    )
    if censor_after is not None:
        bars = bars.filter(pl.col("date") <= censor_after)
    ev = events.select("event_id", "symbol", "entry_session").filter(
        pl.col("symbol").is_not_null() & pl.col("entry_session").is_not_null()
    )
    if censor_after is not None:
        ev = ev.filter(pl.col("entry_session") <= censor_after)
    ev = ev.sort("symbol", "entry_session").join_asof(
        bars.rename(
            {
                "date": "entry_date",
                "adj_open": "_e_open",
                "adj_close": "_e_close",
                "tr_index": "_e_tr",
            }
        ),
        left_on="entry_session",
        right_on="entry_date",
        by="symbol",
        strategy="forward",
        tolerance=f"{ENTRY_SLACK_DAYS}d",
        check_sortedness=False,
    )
    entry_price = pl.col("_e_open") if mode == "open" else pl.col("_e_close")
    intraday = (
        (pl.col("_e_close") / pl.col("_e_open"))
        if mode == "open"
        else pl.lit(1.0, dtype=pl.Float64)
    )
    out = ev.with_columns(entry_price.alias("entry_price"), intraday.alias("_intraday"))
    for h in horizons:
        target = calendar.shift_sessions(out["entry_session"], h).alias(f"_target_{h}")
        piece = out.select("event_id", "symbol", target)
        if censor_after is not None:
            piece = piece.filter(pl.col(f"_target_{h}") <= censor_after)
        piece = piece.sort("symbol", f"_target_{h}")
        piece = piece.join_asof(
            bars.select(
                "symbol", pl.col("date").alias(f"exit_date_{h}"), pl.col("tr_index").alias("_x_tr")
            ),
            left_on=f"_target_{h}",
            right_on=f"exit_date_{h}",
            by="symbol",
            strategy="backward",
            tolerance=f"{EXIT_SLACK_DAYS}d",
            check_sortedness=False,
        ).drop("symbol", f"_target_{h}")
        piece = piece.rename({"_x_tr": f"_x_tr_{h}"})
        out = out.join(piece, on="event_id", how="left")
    cols = [
        "event_id",
        "entry_date",
        "entry_price",
        *[f"exit_date_{h}" for h in horizons],
        *[
            _finite_or_null(pl.col(f"_x_tr_{h}") / pl.col("_e_tr") * pl.col("_intraday") - 1).alias(
                f"ret_{h}"
            )
            for h in horizons
        ],
    ]
    return out.select(*cols)
