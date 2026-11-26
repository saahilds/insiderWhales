"""Event-study engine: returns (T18), benchmarks, matched controls, inference, sectors."""

import math
from datetime import date

import numpy as np
import polars as pl
import pytest

from insider_whales.calendar import get_calendar
from insider_whales.evaluation.event_study import (
    add_buckets,
    benchmark_returns,
    calendar_time,
    horizon_stats,
    size_etf,
    summarize,
)
from insider_whales.evaluation.inference import clustered_mean, newey_west_mean
from insider_whales.evaluation.matched_controls import (
    FEATURES,
    MIN_ADV,
    RECENT_DAYS,
    match_controls,
    prepare_pool,
)
from insider_whales.ingestion.sectors import sic_to_sector_etf
from insider_whales.normalization.adjusted_prices import adjust_prices

NO_ACTIONS = pl.DataFrame(
    schema={
        "symbol": pl.String,
        "ex_date": pl.Date,
        "action": pl.String,
        "ratio": pl.Float64,
        "amount": pl.Float64,
    }
)


def bars(symbol, closes, start=date(2019, 1, 2)):
    days = get_calendar("XNYS").sessions(start, date(2019, 12, 31))[: len(closes)]
    return pl.DataFrame(
        {
            "symbol": symbol,
            "date": days,
            "open": [c - 0.5 for c in closes],
            "close": closes,
            "volume": 1000.0,
        }
    )


# --- T18: returns equal hand-calculated values --------------------------------------------


def test_t18_event_returns_and_adjusted_returns_match_manual_calculation():
    cal = get_calendar("XNYS")
    x = bars("X", [20.0, 21.0, 22.0, 24.0, 23.0, 25.0, 26.0, 27.0])
    spy = bars("SPY", [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0])
    adjusted = adjust_prices(pl.concat([x, spy]), NO_ACTIONS)
    entry = x["date"][2]
    events = pl.DataFrame(
        {"event_id": ["e1"], "symbol": ["X"], "spy_symbol": ["SPY"], "entry_session": [entry]}
    )

    bench = benchmark_returns(
        events, "spy_symbol", adjusted, cal, [1, 3], "open", date(2019, 12, 31), "bspy"
    )
    entry_open_x, entry_open_spy = 21.5, 101.5
    expected_x1 = 24.0 / entry_open_x - 1
    expected_x3 = 25.0 / entry_open_x - 1
    expected_spy1 = 103.0 / entry_open_spy - 1
    expected_spy3 = 105.0 / entry_open_spy - 1
    assert bench["bspy_1"].to_list() == pytest.approx([expected_spy1])
    assert bench["bspy_3"].to_list() == pytest.approx([expected_spy3])

    from insider_whales.features.outcomes import forward_returns

    own = forward_returns(events, adjusted, cal, [1, 3], mode="open")
    assert own["ret_1"].to_list() == pytest.approx([expected_x1])
    assert own["ret_3"].to_list() == pytest.approx([expected_x3])
    assert (own["ret_3"] - bench["bspy_3"]).to_list() == pytest.approx(
        [expected_x3 - expected_spy3]
    )


def test_benchmark_returns_ignore_prices_after_the_cutoff():
    cal = get_calendar("XNYS")
    spy = bars("SPY", [100.0 + i for i in range(10)])
    adjusted = adjust_prices(spy, NO_ACTIONS)
    events = pl.DataFrame(
        {"event_id": ["e"], "spy_symbol": ["SPY"], "entry_session": [spy["date"][1]]}
    )
    out = benchmark_returns(
        events, "spy_symbol", adjusted, cal, [2, 5], "open", spy["date"][4], "b"
    )
    assert out["b_2"][0] is not None
    assert out["b_5"][0] is None


# --- summary statistics ----------------------------------------------------------------------


def study_frame(returns, issuers, months, directions=None):
    n = len(returns)
    return pl.DataFrame(
        {
            "event_id": [f"e{i}" for i in range(n)],
            "direction": directions or ["purchase"] * n,
            "issuer_cik": issuers,
            "insider_group_id": [f"{i}:{x}" for i, x in enumerate(issuers)],
            "entry_month": months,
            "ret_5": returns,
            "mkt_5": [r - 0.01 for r in returns],
            "sec_5": [None] * n,
            "size_5": [r - 0.02 for r in returns],
            "exc_5": [r - 0.03 for r in returns],
        },
        schema_overrides={"sec_5": pl.Float64},
    )


def test_horizon_stats_report_means_and_adjusted_means():
    d = study_frame([0.10, 0.00, -0.02, 0.04], ["a", "b", "c", "d"], [1, 1, 2, 2])
    row = horizon_stats(d, 5)
    assert row["n"] == 4
    assert row["mean"] == pytest.approx(0.03)
    assert row["mkt_mean"] == pytest.approx(0.02)
    assert row["exc_mean"] == pytest.approx(0.0)
    assert row["win_rate"] == pytest.approx(0.5)
    assert row["issuers"] == 4
    assert "sec_mean" not in row


def test_summarize_splits_by_direction():
    d = study_frame(
        [0.1, 0.2, -0.1, -0.2],
        ["a", "b", "c", "d"],
        [1, 2, 1, 2],
        ["purchase", "purchase", "sale", "sale"],
    )
    out = summarize(d, [5]).sort("direction")
    assert out["direction"].to_list() == ["purchase", "sale"]
    assert out["mean"].to_list() == pytest.approx([0.15, -0.15])


def test_calendar_time_averages_months_before_testing():
    # month 1 has nine events at +1%, month 2 one event at +3%: monthly means are 1% and 3%
    returns = [0.01] * 9 + [0.03] + [0.01] * 9 + [0.03]
    months = [1] * 9 + [2] + [3] * 9 + [4]
    d = study_frame(returns, [str(i) for i in range(len(returns))], months)
    out = calendar_time(d, [5]).row(0, named=True)
    assert out["raw_months"] == 4
    assert out["raw_monthly_mean"] == pytest.approx(0.02)


def test_buckets_and_size_etf():
    d = pl.DataFrame(
        {
            "total_value": [10_000.0, 150_000.0, 5_000_000.0],
            "pct_holdings_change": [None, 0.1, 0.5],
            "is_new_position": [True, False, False],
            "n_prior_events": [0, 2, 12],
            "mkt_ret_60": [None, -0.3, 0.1],
            "filing_delay_days": [1, 5, 20],
        }
    )
    out = add_buckets(d)
    assert out["value_bucket"].to_list() == ["1: <25k", "3: 100k-500k", "5: >=2M"]
    assert out["ownership_change_bucket"].to_list() == ["1: new position", "3: 5-25%", "4: >=25%"]
    assert out["history_bucket"].to_list() == ["1: first trade", "2: 1-2 earlier", "4: 10+ earlier"]
    assert out["prior_performance_bucket"].to_list() == [
        "5: unknown",
        "1: fell >20%",
        "3: rose 0-20%",
    ]
    assert out["filing_delay_bucket"].to_list() == ["1: <=2 days", "2: 3-7 days", "3: >7 days"]
    sizes = pl.DataFrame({"g": ["micro", "mid", "mega", "unknown"]}).select(
        size_etf(pl.col("g")).alias("etf")
    )
    assert sizes["etf"].to_list() == ["IWM", "MDY", "SPY", None]


# --- inference -------------------------------------------------------------------------------


def test_clustered_mean_equals_the_plain_standard_error_when_nothing_is_clustered():
    x = np.array([0.1, -0.05, 0.02, 0.07, -0.03, 0.0])
    codes = np.arange(len(x))
    est = clustered_mean(x, codes, codes)
    assert est.mean == pytest.approx(x.mean())
    assert est.se == pytest.approx(x.std(ddof=1) / math.sqrt(len(x)))


def test_clustering_widens_the_interval_for_repeated_observations_of_one_issuer():
    x = np.array([0.1, 0.1, 0.1, -0.1, -0.1, -0.1])
    independent = clustered_mean(x, np.arange(6), np.arange(6))
    clustered = clustered_mean(x, np.array([0, 0, 0, 1, 1, 1]), np.arange(6))
    assert clustered.se > independent.se


def test_newey_west_matches_the_plain_error_without_lags_and_grows_with_autocorrelation():
    y = np.array([0.02, 0.03, 0.01, -0.01, 0.0, 0.02, 0.04, 0.03])
    plain = newey_west_mean(y, 0)
    assert plain.se == pytest.approx(y.std(ddof=0) / math.sqrt(len(y)))
    trending = np.sin(np.linspace(0, 6, 60)) * 0.05
    assert newey_west_mean(trending, 5).se > newey_west_mean(trending, 0).se


# --- sectors ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sic", "etf"),
    [
        ("1311", "XLE"),
        ("2834", "XLV"),
        ("6021", "XLF"),
        ("7372", "XLK"),
        ("4911", "XLU"),
        ("6798", "XLRE"),
        ("5411", "XLP"),
        (None, None),
        ("0", None),
    ],
)
def test_sic_codes_map_to_sector_etfs(sic, etf):
    assert sic_to_sector_etf(sic) == etf


# --- matched controls ------------------------------------------------------------------------


def daily_rows(symbols, day, **overrides):
    rows = []
    for i, s in enumerate(symbols):
        rows.append(
            {
                "symbol": s,
                "date": day,
                "mkt_ret_60": 0.01 * i,
                "mkt_volatility_60": 0.3,
                "mkt_drawdown_252": -0.1,
                "mkt_adv_20": 5e6,
            }
        )
    return pl.DataFrame(rows)


def test_controls_are_same_sector_nearest_and_without_recent_signals():
    day = date(2019, 6, 3)
    symbols = ["T", "A", "B", "C", "D", "E", "F", "G", "OTHER", "SIGNALED", "ILLIQUID"]
    daily = daily_rows(symbols, day)
    daily = daily.with_columns(
        pl.when(pl.col("symbol") == "ILLIQUID")
        .then(MIN_ADV / 10)
        .otherwise(pl.col("mkt_adv_20"))
        .alias("mkt_adv_20"),
        pl.when(pl.col("symbol").is_in(["SIGNALED", "OTHER"]))
        .then(0.0)
        .otherwise(pl.col("mkt_ret_60"))
        .alias("mkt_ret_60"),
    )
    sectors = pl.DataFrame(
        {
            "symbol": symbols,
            "sector_etf": ["XLK"] * 8 + ["XLE", "XLK", "XLK"],
        }
    )
    signals = pl.DataFrame({"symbol": ["SIGNALED"], "signal_date": [date(2019, 6, 3 - 1)]})
    pool = prepare_pool(daily, sectors, signals, {day})
    assert "SIGNALED" not in pool["symbol"].to_list()
    assert "ILLIQUID" not in pool["symbol"].to_list()

    treated = pl.DataFrame(
        {
            "event_id": ["e1"],
            "symbol": ["T"],
            "prior_session": [day],
            "sector_etf": ["XLK"],
            "mkt_ret_60": [0.0],
            "mkt_volatility_60": [0.3],
            "mkt_drawdown_252": [-0.1],
            "log_adv": [math.log(5e6)],
        }
    )
    out = match_controls(treated, pool, k=3)
    assert out["event_id"].to_list() == ["e1"] * 3
    picked = out.sort("rank")["control_symbol"].to_list()
    assert "T" not in picked and "OTHER" not in picked
    # T is index 0 with ret 0.0, so A (0.01), B (0.02), C (0.03) are the closest same-sector peers
    assert picked == ["A", "B", "C"]


def test_old_signals_do_not_exclude_a_control():
    day = date(2019, 6, 3)
    daily = daily_rows(["A", "B"], day)
    sectors = pl.DataFrame({"symbol": ["A", "B"], "sector_etf": ["XLK", "XLK"]})
    old = date(2019, 6, 3).toordinal() - RECENT_DAYS - 5
    signals = pl.DataFrame({"symbol": ["A"], "signal_date": [date.fromordinal(old)]})
    pool = prepare_pool(daily, sectors, signals, {day})
    assert sorted(pool["symbol"].to_list()) == ["A", "B"]
    assert set(FEATURES) <= set(pool.columns)


def test_events_with_too_few_candidates_get_no_controls():
    day = date(2019, 6, 3)
    daily = daily_rows(["A", "B"], day)
    sectors = pl.DataFrame({"symbol": ["A", "B"], "sector_etf": ["XLK", "XLK"]})
    pool = prepare_pool(
        daily, sectors, pl.DataFrame(schema={"symbol": pl.String, "signal_date": pl.Date}), {day}
    )
    treated = pl.DataFrame(
        {
            "event_id": ["e"],
            "symbol": ["T"],
            "prior_session": [day],
            "sector_etf": ["XLK"],
            "mkt_ret_60": [0.0],
            "mkt_volatility_60": [0.3],
            "mkt_drawdown_252": [-0.1],
            "log_adv": [math.log(5e6)],
        }
    )
    assert match_controls(treated, pool).height == 0
