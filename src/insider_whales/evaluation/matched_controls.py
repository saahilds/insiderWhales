"""Matched controls v1: stocks without recent insider signals that looked like the signal stock.

For every treated event, candidates are other stocks of the insider universe that traded on the
last session before the entry session, in the same sector, and had no insider disclosure in the
previous `RECENT_DAYS` days. The `K` nearest by trailing 60-day return, 60-day volatility,
252-day drawdown and dollar volume are the controls. Selection uses only information from
before the entry session; whether a control later has a return is not looked at.
"""

from __future__ import annotations

import numpy as np
import polars as pl

K = 3
RECENT_DAYS = 90
FEATURES = ["mkt_ret_60", "mkt_volatility_60", "mkt_drawdown_252", "log_adv"]
MIN_POOL = 5
MIN_ADV = 1e5


def prepare_pool(
    daily: pl.DataFrame,
    symbol_sector: pl.DataFrame,
    signals: pl.DataFrame,
    dates: set,
) -> pl.DataFrame:
    """Candidate rows (symbol, date, sector, standardized features, recent-signal flag).

    `daily` has the daily market features; `symbol_sector` has `symbol`, `sector_etf`;
    `signals` has `symbol` and `signal_date` (the date any insider disclosure became public).
    """
    pool = (
        daily.filter(pl.col("date").is_in(list(dates)))
        .join(symbol_sector, on="symbol")
        .with_columns(pl.col("mkt_adv_20").log().alias("log_adv"))
        .drop_nulls(FEATURES)
        .filter(pl.all_horizontal(pl.col(f).is_finite() for f in FEATURES))
        .filter(pl.col("mkt_adv_20") >= MIN_ADV)
        .sort("symbol", "date")
    )
    last_signal = (
        signals.select("symbol", pl.col("signal_date").alias("_last_signal"))
        .unique()
        .sort("symbol", "_last_signal")
    )
    pool = pool.join_asof(
        last_signal,
        left_on="date",
        right_on="_last_signal",
        by="symbol",
        strategy="backward",
        check_sortedness=False,
    ).with_columns(
        (
            pl.col("_last_signal").is_not_null()
            & ((pl.col("date") - pl.col("_last_signal")).dt.total_days() <= RECENT_DAYS)
        ).alias("recent_signal")
    )
    return pool.filter(~pl.col("recent_signal")).drop("_last_signal", "recent_signal")


def _robust_scale(values: pl.Series) -> tuple[float, float]:
    q1, med, q3 = (float(values.quantile(q)) for q in (0.25, 0.5, 0.75))
    spread = (q3 - q1) / 1.349
    return med, spread if spread > 0 else 1.0


def match_controls(
    treated: pl.DataFrame, pool: pl.DataFrame, k: int = K, scale: dict | None = None
) -> pl.DataFrame:
    """Nearest controls per treated event: columns `event_id`, `control_symbol`, `rank`.

    `treated` needs `event_id`, `symbol`, `prior_session`, `sector_etf` and the feature columns
    (with `log_adv` computed from `mkt_adv_20`). Distances use feature scales taken from the
    pool, so no treated-event information shapes them.
    """
    scale = scale or {f: _robust_scale(pool[f]) for f in FEATURES}
    parts = {key[0]: df for key, df in pool.partition_by("date", as_dict=True).items()}
    out_event: list[str] = []
    out_symbol: list[str] = []
    out_rank: list[int] = []
    t = treated.drop_nulls(FEATURES).filter(
        pl.all_horizontal(pl.col(f).is_finite() for f in FEATURES)
    )
    for (date_key,), group in t.partition_by("prior_session", as_dict=True).items():
        cand = parts.get(date_key)
        if cand is None or cand.height == 0:
            continue
        cfeat = np.column_stack(
            [(cand[f].to_numpy() - scale[f][0]) / scale[f][1] for f in FEATURES]
        )
        csym = cand["symbol"].to_numpy()
        csec = cand["sector_etf"].to_numpy()
        for row in group.iter_rows(named=True):
            vec = np.array([(row[f] - scale[f][0]) / scale[f][1] for f in FEATURES])
            ok = csym != row["symbol"]
            if row["sector_etf"] is not None:
                ok &= csec == row["sector_etf"]
            idx = np.flatnonzero(ok)
            if len(idx) < MIN_POOL:
                continue
            dist = ((cfeat[idx] - vec) ** 2).sum(axis=1)
            take = idx[np.argsort(dist, kind="stable")[:k]]
            for rank, j in enumerate(take, start=1):
                out_event.append(row["event_id"])
                out_symbol.append(str(csym[j]))
                out_rank.append(rank)
    return pl.DataFrame(
        {"event_id": out_event, "control_symbol": out_symbol, "rank": out_rank},
        schema={"event_id": pl.String, "control_symbol": pl.String, "rank": pl.Int64},
    )
