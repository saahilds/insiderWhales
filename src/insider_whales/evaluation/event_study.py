"""Baseline event study of insider purchases and sales (development period only).

Every returned number is measured from the first executable open after the disclosure became
public, in trading-day horizons, with returns after the end of the development period never
read. Observations without a complete horizon are dropped, never counted as zero. Results use
free survivor-only prices and are exploratory.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any

import numpy as np
import polars as pl

from insider_whales.calendar import TradingCalendar, get_calendar
from insider_whales.config import Settings
from insider_whales.evaluation.inference import (
    clustered_mean,
    newey_west_mean,
    winsorize,
)
from insider_whales.evaluation.matched_controls import FEATURES, K, match_controls, prepare_pool
from insider_whales.features.market import daily_market_features
from insider_whales.features.outcomes import forward_returns
from insider_whales.ingestion.price_runner import prices_dir

PATH_DAYS = [1, 2, 3, 5, 7, 10, 15, 20, 30, 40, 50, 60, 80, 100, 120]
MIN_PRICE = 5.0
MIN_ADV = 2e6


# --- sample ---------------------------------------------------------------------------------


def load_study_events(settings: Settings) -> pl.DataFrame:
    """Eligible events entered in the development period, with features and own returns."""
    fdir = settings.data_dir / "processed" / "features"
    feats = pl.read_parquet(fdir / "event_features.parquet")
    ep = pl.read_parquet(
        prices_dir(settings) / "event_prices.parquet",
        columns=["event_id", "symbol_used", "entry_session"],
    )
    outcomes = pl.read_parquet(fdir / "event_outcomes.parquet").drop("entry_date", "entry_price")
    sic = pl.read_parquet(settings.data_dir / "processed" / "sec" / "sic.parquet").select(
        "issuer_cik", "sector_etf"
    )
    lo, hi = settings.splits.development
    out = (
        feats.join(ep, on="event_id")
        .join(sic, on="issuer_cik", how="left")
        .join(outcomes, on="event_id", how="left")
        .filter(
            pl.col("is_conservative_set")
            & (pl.col("price_status") == "ok")
            & pl.col("trans_code").is_in(["P", "S"])
            & pl.col("entry_session").is_between(lo, hi)
        )
        .rename({"symbol_used": "symbol", "trans_code": "direction"})
    )
    return out.with_columns(
        pl.col("direction").replace_strict({"P": "purchase", "S": "sale"}),
        ((pl.col("vwap") >= MIN_PRICE) & (pl.col("mkt_adv_20") >= MIN_ADV)).alias("tradable"),
        pl.col("entry_session").dt.year().alias("entry_year"),
        (pl.col("entry_session").dt.year() * 12 + pl.col("entry_session").dt.month()).alias(
            "entry_month"
        ),
    )


def size_etf(mcap_group: pl.Expr) -> pl.Expr:
    return (
        pl.when(mcap_group.is_in(["micro", "small"]))
        .then(pl.lit("IWM"))
        .when(mcap_group == "mid")
        .then(pl.lit("MDY"))
        .when(mcap_group.is_in(["large", "mega"]))
        .then(pl.lit("SPY"))
        .otherwise(None)
    )


def benchmark_returns(
    events: pl.DataFrame,
    column: str,
    bench: pl.DataFrame,
    calendar: TradingCalendar,
    horizons: list[int],
    mode: str,
    cutoff: date,
    prefix: str,
) -> pl.DataFrame:
    """Return of the benchmark named in `column` over each event's own windows."""
    pairs = (
        events.select(pl.col(column).alias("symbol"), "entry_session")
        .drop_nulls()
        .unique()
        .with_columns(
            pl.concat_str(["symbol", pl.col("entry_session").cast(pl.String)], separator="|").alias(
                "event_id"
            )
        )
    )
    rets = forward_returns(pairs, bench, calendar, horizons, mode, censor_after=cutoff)
    rets = rets.select("event_id", *[pl.col(f"ret_{h}").alias(f"{prefix}_{h}") for h in horizons])
    keyed = events.select(
        "event_id",
        pl.concat_str(
            [pl.col(column), pl.col("entry_session").cast(pl.String)], separator="|"
        ).alias("_key"),
    )
    return keyed.join(rets.rename({"event_id": "_key"}), on="_key", how="left").drop("_key")


# --- controls -------------------------------------------------------------------------------


def control_returns(
    events: pl.DataFrame,
    adjusted: pl.DataFrame,
    calendar: TradingCalendar,
    horizons: list[int],
    mode: str,
    cutoff: date,
    all_signals: pl.DataFrame,
    symbol_sector: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Mean return of the matched controls per event, plus the control pairs themselves."""
    prior = calendar.shift_sessions(events["entry_session"], -1).alias("prior_session")
    ev = events.with_columns(prior).filter(pl.col("prior_session").is_not_null())
    daily = daily_market_features(adjusted.filter(pl.col("symbol").is_in(symbol_sector["symbol"])))
    pool = prepare_pool(
        daily, symbol_sector, all_signals, set(ev["prior_session"].unique().to_list())
    )
    treated = ev.with_columns(pl.col("mkt_adv_20").log().alias("log_adv")).select(
        "event_id", "symbol", "prior_session", "sector_etf", *FEATURES
    )
    pairs = match_controls(treated, pool, k=K)
    pair_in = (
        pairs.join(ev.select("event_id", "entry_session"), on="event_id")
        .with_columns(
            pl.concat_str(["event_id", pl.col("rank").cast(pl.String)], separator="#").alias(
                "pair_id"
            )
        )
        .select(
            "event_id",
            pl.col("pair_id"),
            pl.col("control_symbol").alias("symbol"),
            "entry_session",
        )
    )
    rets = forward_returns(
        pair_in.select(pl.col("pair_id").alias("event_id"), "symbol", "entry_session"),
        adjusted,
        calendar,
        horizons,
        mode,
        censor_after=cutoff,
    )
    joined = pair_in.select("pair_id", "event_id").join(
        rets.rename({"event_id": "pair_id"}), on="pair_id", how="left"
    )
    agg = joined.group_by("event_id").agg(
        *[pl.col(f"ret_{h}").mean().alias(f"ctrl_{h}") for h in horizons],
        pl.col("pair_id").n_unique().alias("n_controls"),
    )
    return agg, pairs


# --- statistics -----------------------------------------------------------------------------


def _codes(series: pl.Series) -> np.ndarray:
    return series.rank("dense").to_numpy().astype(np.int64) - 1


def _stats_for(d: pl.DataFrame, col: str, prefix: str) -> dict[str, Any]:
    sub = d.filter(pl.col(col).is_not_null() & pl.col(col).is_finite())
    out: dict[str, Any] = {f"{prefix}n": sub.height}
    if sub.height == 0:
        return out
    x = sub[col].to_numpy()
    est = clustered_mean(x, _codes(sub["issuer_cik"]), _codes(sub["entry_month"]))
    lo, hi = est.ci
    out.update(
        {
            f"{prefix}mean": est.mean,
            f"{prefix}ci_low": lo,
            f"{prefix}ci_high": hi,
            f"{prefix}t": est.t_stat,
            f"{prefix}median": float(np.median(x)),
        }
    )
    return out


def horizon_stats(d: pl.DataFrame, h: int) -> dict[str, Any]:
    """All reported statistics of one subset at one horizon."""
    own = d.filter(pl.col(f"ret_{h}").is_not_null())
    row: dict[str, Any] = {"horizon": h}
    row.update(_stats_for(d, f"ret_{h}", ""))
    if own.height:
        x = own[f"ret_{h}"].to_numpy()
        q = np.quantile(x, [0.05, 0.25, 0.75, 0.95])
        row.update(
            {
                "issuers": own["issuer_cik"].n_unique(),
                "insiders": own["insider_group_id"].n_unique(),
                "mean_winsorized": float(winsorize(x).mean()),
                "std": float(x.std(ddof=1)) if len(x) > 1 else float("nan"),
                "p05": float(q[0]),
                "p25": float(q[1]),
                "p75": float(q[2]),
                "p95": float(q[3]),
                "win_rate": float((x > 0).mean()),
            }
        )
    row.update(_stats_for(d, f"mkt_{h}", "mkt_"))
    mkt = d.filter(pl.col(f"mkt_{h}").is_not_null())
    if mkt.height:
        row["excess_win_rate"] = float((mkt[f"mkt_{h}"] > 0).mean())
    row.update(_stats_for(d, f"sec_{h}", "sec_"))
    row.update(_stats_for(d, f"size_{h}", "size_"))
    row.update(_stats_for(d, f"exc_{h}", "exc_"))
    return row


def summarize(
    events: pl.DataFrame, horizons: list[int], by: list[str] | None = None
) -> pl.DataFrame:
    """Statistics per direction (and per value of the `by` columns) and horizon."""
    keys = ["direction", *(by or [])]
    rows = []
    for key, d in events.partition_by(keys, as_dict=True).items():
        for h in horizons:
            r = horizon_stats(d, h)
            r.update(dict(zip(keys, key, strict=True)))
            rows.append(r)
    return pl.DataFrame(rows, infer_schema_length=None).sort([*keys, "horizon"])


def calendar_time(events: pl.DataFrame, horizons: list[int]) -> pl.DataFrame:
    """Monthly equal-weighted mean returns of events entered each month, with Newey-West errors."""
    rows = []
    for direction, d in events.partition_by("direction", as_dict=True).items():
        for h in horizons:
            lags = max(3, math.ceil(h / 21))
            row: dict[str, Any] = {"direction": direction[0], "horizon": h}
            for name, col in (("raw", f"ret_{h}"), ("mkt", f"mkt_{h}"), ("exc", f"exc_{h}")):
                monthly = (
                    d.filter(pl.col(col).is_not_null())
                    .group_by("entry_month")
                    .agg(pl.col(col).mean())
                    .sort("entry_month")[col]
                    .to_numpy()
                )
                est = newey_west_mean(monthly, lags)
                row.update(
                    {
                        f"{name}_months": est.n,
                        f"{name}_monthly_mean": est.mean,
                        f"{name}_t": est.t_stat,
                    }
                )
            rows.append(row)
    return pl.DataFrame(rows, infer_schema_length=None).sort("direction", "horizon")


# --- subgroups ------------------------------------------------------------------------------


def add_buckets(events: pl.DataFrame) -> pl.DataFrame:
    v = pl.col("total_value")
    pct = pl.col("pct_holdings_change")
    n_prior = pl.col("n_prior_events")
    prev = pl.col("mkt_ret_60")
    delay = pl.col("filing_delay_days")
    return events.with_columns(
        pl.when(v < 25_000)
        .then(pl.lit("1: <25k"))
        .when(v < 100_000)
        .then(pl.lit("2: 25k-100k"))
        .when(v < 500_000)
        .then(pl.lit("3: 100k-500k"))
        .when(v < 2_000_000)
        .then(pl.lit("4: 500k-2M"))
        .otherwise(pl.lit("5: >=2M"))
        .alias("value_bucket"),
        pl.when(pl.col("is_new_position").fill_null(False))
        .then(pl.lit("1: new position"))
        .when(pct.is_null())
        .then(pl.lit("5: unknown"))
        .when(pct < 0.05)
        .then(pl.lit("2: <5%"))
        .when(pct < 0.25)
        .then(pl.lit("3: 5-25%"))
        .otherwise(pl.lit("4: >=25%"))
        .alias("ownership_change_bucket"),
        pl.when(n_prior == 0)
        .then(pl.lit("1: first trade"))
        .when(n_prior <= 2)
        .then(pl.lit("2: 1-2 earlier"))
        .when(n_prior <= 9)
        .then(pl.lit("3: 3-9 earlier"))
        .otherwise(pl.lit("4: 10+ earlier"))
        .alias("history_bucket"),
        pl.when(prev.is_null())
        .then(pl.lit("5: unknown"))
        .when(prev < -0.2)
        .then(pl.lit("1: fell >20%"))
        .when(prev < 0.0)
        .then(pl.lit("2: fell 0-20%"))
        .when(prev < 0.2)
        .then(pl.lit("3: rose 0-20%"))
        .otherwise(pl.lit("4: rose >20%"))
        .alias("prior_performance_bucket"),
        pl.when(delay <= 2)
        .then(pl.lit("1: <=2 days"))
        .when(delay <= 7)
        .then(pl.lit("2: 3-7 days"))
        .otherwise(pl.lit("3: >7 days"))
        .alias("filing_delay_bucket"),
    )


SUBGROUPS = {
    "plan_status": "Trading plan status",
    "role_norm": "Insider role",
    "mcap_group": "Market-cap group",
    "value_bucket": "Transaction value",
    "ownership_change_bucket": "Ownership change",
    "history_bucket": "Insider trading history",
    "prior_performance_bucket": "Prior 60-day stock return",
    "filing_delay_bucket": "Filing delay",
    "entry_year": "Entry year",
    "trader_type": "Routine vs opportunistic",
}


# --- cumulative paths -----------------------------------------------------------------------


def cumulative_paths(
    events: pl.DataFrame,
    pairs: pl.DataFrame,
    adjusted: pl.DataFrame,
    benchmarks: pl.DataFrame,
    calendar: TradingCalendar,
    mode: str,
    cutoff: date,
) -> pl.DataFrame:
    """Mean cumulative return by trading day since entry for signals, controls and SPY.

    Only events whose whole 120-session path exists are used, so the sample is the same on
    every day of the plot.
    """
    sig = forward_returns(events, adjusted, calendar, PATH_DAYS, mode, censor_after=cutoff)
    complete = sig.filter(pl.all_horizontal(*[pl.col(f"ret_{k}").is_not_null() for k in PATH_DAYS]))
    keep = events.join(complete.select("event_id"), on="event_id").select(
        "event_id", "direction", "entry_session"
    )
    ctl_in = (
        pairs.join(keep, on="event_id")
        .with_columns(
            pl.concat_str(["event_id", pl.col("rank").cast(pl.String)], separator="#").alias("pid")
        )
        .select(
            pl.col("pid").alias("event_id"),
            pl.col("control_symbol").alias("symbol"),
            "entry_session",
        )
    )
    ctl = forward_returns(ctl_in, adjusted, calendar, PATH_DAYS, mode, censor_after=cutoff)
    ctl = (
        ctl.with_columns(pl.col("event_id").str.split("#").list.first().alias("event_id"))
        .group_by("event_id")
        .agg(*[pl.col(f"ret_{k}").mean() for k in PATH_DAYS])
    )
    spy_in = keep.select(pl.col("event_id"), pl.lit("SPY").alias("symbol"), "entry_session")
    spy = forward_returns(spy_in, benchmarks, calendar, PATH_DAYS, mode, censor_after=cutoff)
    rows = []
    for direction in ("purchase", "sale"):
        ids = keep.filter(pl.col("direction") == direction).select("event_id")
        s = complete.join(ids, on="event_id")
        c = ctl.join(ids, on="event_id")
        sp = spy.join(ids, on="event_id")
        for k in PATH_DAYS:
            rows.append(
                {
                    "direction": direction,
                    "day": k,
                    "events": s.height,
                    "signal": float(s[f"ret_{k}"].mean()),
                    "controls": float(c[f"ret_{k}"].mean()) if c.height else None,
                    "controls_n": c.height,
                    "spy": float(sp[f"ret_{k}"].mean()) if sp.height else None,
                }
            )
    return pl.DataFrame(rows, infer_schema_length=None)


# --- run ------------------------------------------------------------------------------------


def build_study_table(settings: Settings) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Study events with benchmark-adjusted and control-adjusted returns for every horizon."""
    horizons = list(settings.event_study.horizons)
    cal = get_calendar(settings.calendar.exchange)
    mode = settings.execution.primary_mode
    cutoff = settings.splits.development[1]
    pdir = prices_dir(settings)
    adjusted = pl.read_parquet(pdir / "prices_adjusted.parquet")
    benchmarks = pl.read_parquet(pdir / "benchmarks_adjusted.parquet")

    events = load_study_events(settings).with_columns(
        pl.lit("SPY").alias("spy_symbol"), size_etf(pl.col("mcap_group")).alias("size_symbol")
    )
    for column, prefix in (
        ("spy_symbol", "bspy"),
        ("sector_etf", "bsec"),
        ("size_symbol", "bsize"),
    ):
        events = events.join(
            benchmark_returns(events, column, benchmarks, cal, horizons, mode, cutoff, prefix),
            on="event_id",
            how="left",
        )

    ev_all = pl.read_parquet(
        pdir / "event_prices.parquet",
        columns=["event_id", "issuer_cik", "symbol_used", "available_at"],
    ).drop_nulls("symbol_used")
    signals = ev_all.select(
        pl.col("symbol_used").alias("symbol"),
        pl.col("available_at")
        .dt.convert_time_zone("America/New_York")
        .dt.date()
        .alias("signal_date"),
    )
    modal = (
        ev_all.group_by("symbol_used", "issuer_cik")
        .len()
        .sort("len", descending=True)
        .group_by("symbol_used", maintain_order=True)
        .first()
    )
    sic = pl.read_parquet(settings.data_dir / "processed" / "sec" / "sic.parquet").select(
        "issuer_cik", "sector_etf"
    )
    symbol_sector = modal.join(sic, on="issuer_cik", how="left").select(
        pl.col("symbol_used").alias("symbol"), "sector_etf"
    )
    ctrl, pairs = control_returns(
        events, adjusted, cal, horizons, mode, cutoff, signals, symbol_sector
    )
    events = events.join(ctrl, on="event_id", how="left")
    derived = []
    for h in horizons:
        r = pl.col(f"ret_{h}")
        derived += [
            (r - pl.col(f"bspy_{h}")).alias(f"mkt_{h}"),
            (r - pl.col(f"bsec_{h}")).alias(f"sec_{h}"),
            (r - pl.col(f"bsize_{h}")).alias(f"size_{h}"),
            (r - pl.col(f"ctrl_{h}")).alias(f"exc_{h}"),
        ]
    return add_buckets(events.with_columns(derived)), pairs, adjusted


def run_event_study(settings: Settings) -> dict[str, Any]:
    """Build the study table, statistics and paths; write them under `reports/event_study`."""
    horizons = list(settings.event_study.horizons)
    cal = get_calendar(settings.calendar.exchange)
    cutoff = settings.splits.development[1]
    events, pairs, adjusted = build_study_table(settings)
    benchmarks = pl.read_parquet(prices_dir(settings) / "benchmarks_adjusted.parquet")
    out = settings.reports_dir / "event_study"
    out.mkdir(parents=True, exist_ok=True)
    pdir = settings.data_dir / "processed" / "event_study"
    pdir.mkdir(parents=True, exist_ok=True)
    events.write_parquet(pdir / "event_study_events.parquet")
    pairs.write_parquet(pdir / "matched_controls.parquet")

    headline = summarize(events, horizons)
    tradable = summarize(events.filter(pl.col("tradable")), horizons)
    ctime = calendar_time(events, horizons)
    subgroups = []
    for column in SUBGROUPS:
        df = summarize(
            events.filter(pl.col(column).is_not_null()).with_columns(
                pl.col(column).cast(pl.String).alias("value")
            ),
            [20, 60],
            by=["value"],
        ).with_columns(pl.lit(column).alias("grouping"))
        subgroups.append(df)
    subgroups_df = pl.concat(subgroups, how="diagonal_relaxed")
    paths = cumulative_paths(
        events, pairs, adjusted, benchmarks, cal, settings.execution.primary_mode, cutoff
    )
    headline.write_csv(out / "headline.csv")
    tradable.write_csv(out / "headline_tradable.csv")
    ctime.write_csv(out / "calendar_time.csv")
    subgroups_df.write_csv(out / "subgroups.csv")
    paths.write_csv(out / "paths.csv")
    return {
        "events": events,
        "headline": headline,
        "tradable": tradable,
        "calendar_time": ctime,
        "subgroups": subgroups_df,
        "paths": paths,
        "pairs": pairs,
    }
