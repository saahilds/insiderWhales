"""Transaction features: size, ownership change, role, filing delay and trading-plan status."""

from __future__ import annotations

import polars as pl

from insider_whales.calendar import TradingCalendar


def transaction_features(events: pl.DataFrame, calendar: TradingCalendar) -> pl.DataFrame:
    """Features that need only the filing itself (ratios against market data are added later)."""
    delay_sessions = calendar.sessions_between(
        events["trans_date"].alias("a"), events["filing_date"].alias("b")
    )
    return events.select(
        "event_id",
        pl.col("value").alias("total_value"),
        "shares",
        "vwap",
        pl.col("pct_holdings_change"),
        "is_new_position",
        "ownership_reliable",
        "role_norm",
        (pl.col("role_norm") == "Director").alias("is_director"),
        pl.col("role_norm").is_in(["CEO", "CFO", "OtherOfficer"]).alias("is_officer"),
        (pl.col("role_norm") == "TenPct").alias("is_ten_percent_owner"),
        (pl.col("filing_date") - pl.col("trans_date")).dt.total_days().alias("filing_delay_days"),
        delay_sessions.alias("filing_delay_sessions"),
        "plan_status",
    )
