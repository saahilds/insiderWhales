"""Assemble the point-in-time feature table, one row per insider event."""

from __future__ import annotations

import logging

import polars as pl

from insider_whales.calendar import get_calendar
from insider_whales.config import REPO_ROOT, Settings
from insider_whales.features.cluster import cluster_features
from insider_whales.features.docs import render_feature_doc
from insider_whales.features.fundamentals import annual_snapshots, fundamental_features
from insider_whales.features.history import (
    PROFIT_HORIZON,
    mature_profitability,
    routine_classification,
    trailing_counts,
)
from insider_whales.features.market import daily_market_features, market_features_asof
from insider_whales.features.outcomes import forward_returns
from insider_whales.features.transaction import transaction_features
from insider_whales.ingestion.price_runner import prices_dir
from insider_whales.normalization.adjusted_prices import build_adjusted_prices
from insider_whales.normalization.pipeline import scan_processed

log = logging.getLogger(__name__)

EVENT_COLUMNS = [
    "event_id",
    "accession_number",
    "insider_group_id",
    "issuer_cik",
    "security_id",
    "trans_date",
    "trans_code",
    "shares",
    "value",
    "vwap",
    "available_at",
    "filing_date",
    "role_norm",
    "is_conservative_set",
    "pct_holdings_change",
    "is_new_position",
    "ownership_reliable",
    "plan_status",
]


def features_dir(settings):
    path = settings.data_dir / "processed" / "features"
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_event_features(settings: Settings) -> pl.DataFrame:
    """Write `event_outcomes.parquet` and `event_features.parquet` and return the features."""
    out_dir = features_dir(settings)
    cal = get_calendar(settings.calendar.exchange)
    horizons = settings.event_study.horizons
    cutoff = settings.splits.development[1]

    events = (
        scan_processed(settings, "insider_events")
        .select(EVENT_COLUMNS)
        .filter(pl.col("filing_date") <= pl.date(2022, 12, 31))
        .collect()
    )
    prices_status = pl.read_parquet(
        prices_dir(settings) / "event_prices.parquet",
        columns=["event_id", "symbol_used", "entry_session", "price_status", "mcap_group"],
    )
    events = events.join(prices_status, on="event_id", how="left")

    log.info("adjusting prices")
    adjusted = build_adjusted_prices(settings)
    benchmarks = pl.read_parquet(prices_dir(settings) / "benchmarks_adjusted.parquet")

    priced = events.filter(pl.col("price_status") == "ok").rename({"symbol_used": "symbol"})
    log.info("forward returns")
    outcomes = forward_returns(
        priced, adjusted, cal, horizons, settings.execution.primary_mode, censor_after=cutoff
    )
    outcomes = events.select("event_id").join(outcomes, on="event_id", how="left")
    outcomes.write_parquet(out_dir / "event_outcomes.parquet")

    log.info("market features")
    daily = daily_market_features(adjusted)
    bench_daily = daily_market_features(benchmarks)
    market = market_features_asof(priced, daily, bench_daily)

    log.info("fundamentals")
    fdir = settings.data_dir / "processed" / "fundamentals"
    snapshots = annual_snapshots(pl.read_parquet(fdir / "annual_facts.parquet"))
    fundamentals = fundamental_features(
        events, snapshots, pl.read_parquet(fdir / "shares_outstanding.parquet")
    )

    log.info("history and cluster")
    history = trailing_counts(events)
    routine = routine_classification(events)
    profit = mature_profitability(events, outcomes, PROFIT_HORIZON)
    cluster = cluster_features(events)
    txn = transaction_features(events, cal)

    table = (
        txn.join(
            events.select(
                "event_id",
                "issuer_cik",
                "security_id",
                "insider_group_id",
                "trans_code",
                "trans_date",
                "filing_date",
                "available_at",
                "is_conservative_set",
                "price_status",
                "mcap_group",
            ),
            on="event_id",
        )
        .join(history, on="event_id", how="left")
        .join(routine, on="event_id", how="left")
        .join(profit, on="event_id", how="left")
        .join(cluster, on="event_id", how="left")
        .join(market, on="event_id", how="left")
        .join(fundamentals, on="event_id", how="left")
        .join(outcomes.select("event_id", "entry_date", "entry_price"), on="event_id", how="left")
    )
    table = table.with_columns(
        (pl.col("total_value") / pl.col("market_cap")).alias("value_to_market_cap"),
        (pl.col("total_value") / pl.col("mkt_adv_20")).alias("value_to_adv"),
        (pl.col("total_value") / pl.col("hist_median_value")).alias("value_to_history_median"),
        (pl.col("entry_price") / pl.col("trade_adj_close") - 1).alias("ret_since_transaction"),
    )
    table.write_parquet(out_dir / "event_features.parquet")
    doc = REPO_ROOT / "docs" / "data" / "features.md"
    doc.write_text(render_feature_doc(table))
    return table
