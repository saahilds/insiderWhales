"""Point-in-time features: fundamentals (T12), clusters (T4), insider history (T13), prices."""

from datetime import UTC, date, datetime

import polars as pl
import pytest

from insider_whales.calendar import get_calendar
from insider_whales.features.cluster import cluster_features
from insider_whales.features.fundamentals import (
    annual_snapshots,
    fundamental_features,
    fundamentals_asof,
    shares_asof,
)
from insider_whales.features.history import (
    mature_profitability,
    routine_classification,
    trailing_counts,
)
from insider_whales.features.outcomes import forward_returns
from insider_whales.ingestion.fundamentals import (
    FACT_SCHEMA,
    annual_facts_from_payload,
)
from insider_whales.normalization.adjusted_prices import adjust_prices


def ts(y, m, d, h=15):
    return datetime(y, m, d, h, 0, tzinfo=UTC)


def events(rows):
    """rows: (event_id, insider, code, available_at, trans_date, value, issuer, role)"""
    return pl.DataFrame(
        rows,
        schema={
            "event_id": pl.String,
            "insider_group_id": pl.String,
            "trans_code": pl.String,
            "available_at": pl.Datetime("us", "UTC"),
            "trans_date": pl.Date,
            "value": pl.Float64,
            "issuer_cik": pl.String,
            "role_norm": pl.String,
        },
        orient="row",
    )


# --- T12: fundamentals cannot be used before publication ----------------------------------


def fact(accn, filed, end, metric, value, rank=0, start=None):
    return {
        "issuer_cik": "0000000001",
        "metric": metric,
        "tag_rank": rank,
        "period_start": start,
        "period_end": end,
        "filed": filed,
        "accession_number": accn,
        "form": "10-K",
        "value": value,
    }


def facts_frame(rows):
    return pl.DataFrame(rows, schema=FACT_SCHEMA).with_columns(
        pl.col("period_start").str.to_date(strict=False),
        pl.col("period_end").str.to_date(strict=False),
        pl.col("filed").str.to_date(strict=False),
    )


def two_years_of_facts():
    return facts_frame(two_years_of_fact_rows())


def two_years_of_fact_rows():
    rows = []
    # FY2018 10-K filed 2019-03-01, FY2019 10-K filed 2020-03-02 (restates FY2018 revenue).
    for accn, filed, cur, prev, rev_cur, rev_prev in (
        ("A18", "2019-03-01", "2018-12-31", "2017-12-31", 1000.0, 800.0),
        ("A19", "2020-03-02", "2019-12-31", "2018-12-31", 1500.0, 950.0),
    ):
        for end, rev in ((cur, rev_cur), (prev, rev_prev)):
            rows.append(fact(accn, filed, end, "revenue", rev))
            rows.append(fact(accn, filed, end, "net_income", rev / 10))
            rows.append(fact(accn, filed, end, "equity", rev / 2))
    return rows


def one_event(available_at, vwap=10.0):
    return pl.DataFrame(
        {
            "event_id": ["e"],
            "issuer_cik": ["0000000001"],
            "available_at": [available_at],
            "vwap": [vwap],
        },
        schema_overrides={"available_at": pl.Datetime("us", "UTC")},
    )


def test_t12_fundamentals_are_not_used_before_their_filing_date():
    snaps = annual_snapshots(two_years_of_facts())
    before = fundamentals_asof(one_event(ts(2020, 3, 1)), snaps)
    assert before["revenue"].to_list() == [1000.0]  # FY2018, the FY2019 10-K is not out yet
    same_day = fundamentals_asof(one_event(ts(2020, 3, 2)), snaps)
    assert same_day["revenue"].to_list() == [1000.0]  # filed today: not yet usable
    after = fundamentals_asof(one_event(ts(2020, 3, 3)), snaps)
    assert after["revenue"].to_list() == [1500.0]


def test_t12_restated_prior_year_comes_from_the_same_filing():
    snaps = annual_snapshots(two_years_of_facts())
    old = fundamentals_asof(one_event(ts(2019, 6, 1)), snaps)
    new = fundamentals_asof(one_event(ts(2020, 6, 1)), snaps)
    assert old["revenue_prior"].to_list() == [800.0]  # 2017 as reported in the FY2018 10-K
    assert new["revenue_prior"].to_list() == [950.0]  # 2018 as restated in the FY2019 10-K


def test_t12_missing_or_stale_fundamentals_are_null_not_filled():
    snaps = annual_snapshots(two_years_of_facts())
    none_yet = fundamentals_asof(one_event(ts(2018, 6, 1)), snaps)
    assert none_yet["revenue"].to_list() == [None]
    stale = fundamentals_asof(one_event(ts(2022, 6, 1)), snaps)
    assert stale["revenue"].to_list() == [None]


def test_an_old_amendment_cannot_replace_newer_figures():
    rows = two_years_of_fact_rows()
    amend = [
        {**fact("A18/A", "2020-09-01", "2018-12-31", "revenue", 5.0), "form": "10-K/A"},
        {**fact("A18/A", "2020-09-01", "2018-12-31", "net_income", 1.0), "form": "10-K/A"},
    ]
    snaps = annual_snapshots(facts_frame([*rows, *amend]))
    out = fundamentals_asof(one_event(ts(2020, 10, 1)), snaps)
    assert out["revenue"].to_list() == [1500.0]


def test_ratios_use_the_event_price_and_cover_page_shares():
    snaps = annual_snapshots(two_years_of_facts())
    shares = pl.DataFrame(
        {
            "issuer_cik": ["0000000001"],
            "filed": [date(2020, 2, 20)],
            "shares": [100.0],
            "multi_class": [False],
        }
    )
    out = fundamental_features(one_event(ts(2020, 6, 1), vwap=10.0), snaps, shares)
    row = out.row(0, named=True)
    assert row["market_cap"] == 1000.0
    assert row["price_to_sales"] == pytest.approx(1000.0 / 1500.0)
    assert row["revenue_growth"] == pytest.approx(1500.0 / 950.0 - 1)
    assert shares_asof(one_event(ts(2020, 2, 20)), shares)["shares_outstanding"].to_list() == [None]


def test_annual_facts_keep_only_full_year_10k_values():
    payload = {
        "cik": 1,
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {
                                "start": "2019-01-01",
                                "end": "2019-12-31",
                                "val": 10,
                                "accn": "a",
                                "form": "10-K",
                                "filed": "2020-02-01",
                            },
                            {
                                "start": "2019-01-01",
                                "end": "2019-03-31",
                                "val": 3,
                                "accn": "b",
                                "form": "10-Q",
                                "filed": "2019-05-01",
                            },
                            {
                                "start": "2019-10-01",
                                "end": "2019-12-31",
                                "val": 4,
                                "accn": "a",
                                "form": "10-K",
                                "filed": "2020-02-01",
                            },
                        ]
                    }
                },
                "Assets": {
                    "units": {
                        "USD": [
                            {
                                "end": "2019-12-31",
                                "val": 99,
                                "accn": "a",
                                "form": "10-K",
                                "filed": "2020-02-01",
                            }
                        ]
                    }
                },
            }
        },
    }
    rows = annual_facts_from_payload(payload)
    assert sorted((r["metric"], r["value"]) for r in rows) == [("assets", 99.0), ("revenue", 10.0)]


# --- T4: clusters cannot trigger before the second disclosure ------------------------------


def test_t4_cluster_is_visible_only_from_the_second_disclosure():
    ev = events(
        [
            # Trades on Mon/Tue, but the second insider's filing only becomes public on Fri.
            ("a", "i1", "P", ts(2019, 3, 4), date(2019, 3, 1), 1000.0, "c1", "CEO"),
            ("b", "i2", "P", ts(2019, 3, 8), date(2019, 3, 4), 2000.0, "c1", "Director"),
        ]
    )
    out = cluster_features(ev).sort("event_id")
    assert out["n_buyers_7d"].to_list() == [1, 2]
    assert out["n_buying_officers_7d"].to_list() == [1, 1]
    assert out["n_buying_directors_7d"].to_list() == [0, 1]
    assert out["buy_value_7d"].to_list() == [1000.0, 3000.0]


def test_repeat_purchases_by_one_insider_are_one_independent_insider():
    ev = events(
        [
            ("a", "i1", "P", ts(2019, 3, 4), date(2019, 3, 1), 1000.0, "c1", "CEO"),
            ("b", "i1", "P", ts(2019, 3, 5), date(2019, 3, 4), 500.0, "c1", "CEO"),
            ("c", "i1", "S", ts(2019, 3, 6), date(2019, 3, 5), 100.0, "c1", "CEO"),
        ]
    )
    out = cluster_features(ev).sort("event_id")
    assert out["n_buyers_7d"].to_list() == [1, 1, 1]
    assert out["n_sellers_7d"].to_list() == [0, 0, 1]
    assert out["net_value_7d"].to_list() == [1000.0, 1500.0, 1400.0]


def test_cluster_windows_and_issuers_are_separate():
    ev = events(
        [
            ("a", "i1", "P", ts(2019, 3, 1), date(2019, 2, 27), 1.0, "c1", "CEO"),
            ("b", "i2", "P", ts(2019, 3, 20), date(2019, 3, 18), 1.0, "c1", "CEO"),
            ("c", "i3", "P", ts(2019, 3, 20), date(2019, 3, 18), 1.0, "c2", "CEO"),
        ]
    )
    out = cluster_features(ev).sort("event_id")
    assert out["n_buyers_7d"].to_list() == [1, 1, 1]
    assert out["n_buyers_14d"].to_list() == [1, 1, 1]
    assert out["n_buyers_30d"].to_list() == [1, 2, 1]


# --- T13: historical win rates exclude incomplete holding periods ---------------------------


def test_t13_history_only_counts_trades_whose_holding_period_has_ended():
    ev = events(
        [
            ("old", "i1", "P", ts(2019, 1, 7), date(2019, 1, 4), 100.0, "c1", "CEO"),
            ("recent", "i1", "P", ts(2019, 5, 1), date(2019, 4, 29), 100.0, "c1", "CEO"),
            ("now", "i1", "P", ts(2019, 5, 20), date(2019, 5, 17), 100.0, "c1", "CEO"),
        ]
    )
    outcomes = pl.DataFrame(
        {
            "event_id": ["old", "recent", "now"],
            "ret_60": [0.10, -0.50, 0.0],
            "exit_date_60": [date(2019, 4, 5), date(2019, 7, 25), date(2019, 8, 15)],
        }
    )
    out = mature_profitability(ev, outcomes, horizon=60).sort("event_id")
    by_id = {r["event_id"]: r for r in out.iter_rows(named=True)}
    # "now" (May 20): the old trade finished in April, the "recent" one is still running.
    assert by_id["now"]["n_mature_observations"] == 1
    assert by_id["now"]["hist_mean_signed_return"] == pytest.approx(0.10)
    assert by_id["recent"]["n_mature_observations"] == 1
    assert by_id["old"]["n_mature_observations"] == 0
    assert by_id["old"]["hist_mean_signed_return"] is None


def test_sales_count_as_right_when_the_price_falls():
    ev = events(
        [
            ("s", "i1", "S", ts(2019, 1, 7), date(2019, 1, 4), 100.0, "c1", "CEO"),
            ("n", "i1", "P", ts(2019, 9, 1), date(2019, 8, 29), 100.0, "c1", "CEO"),
        ]
    )
    outcomes = pl.DataFrame(
        {"event_id": ["s", "n"], "ret_60": [-0.2, None], "exit_date_60": [date(2019, 4, 1), None]}
    )
    out = mature_profitability(ev, outcomes, horizon=60).filter(pl.col("event_id") == "n")
    assert out["hist_mean_signed_return"].to_list() == [pytest.approx(0.2)]
    assert out["hist_win_rate"].to_list() == [1.0]


def test_trailing_counts_ignore_the_same_disclosure_and_the_future():
    ev = events(
        [
            ("a", "i1", "P", ts(2018, 1, 4), date(2018, 1, 3), 100.0, "c1", "CEO"),
            ("b", "i1", "P", ts(2019, 1, 5), date(2019, 1, 3), 300.0, "c1", "CEO"),
            ("c", "i1", "P", ts(2019, 1, 5), date(2019, 1, 3), 500.0, "c1", "CEO"),
            ("d", "i1", "S", ts(2019, 6, 5), date(2019, 6, 3), 50.0, "c1", "CEO"),
        ]
    )
    out = trailing_counts(ev).sort("event_id")
    by_id = {r["event_id"]: r for r in out.iter_rows(named=True)}
    assert by_id["a"]["n_purchases_12m"] == 0 and by_id["a"]["n_prior_events"] == 0
    assert by_id["b"]["n_purchases_12m"] == 0  # a is 366 days old, c is simultaneous
    assert by_id["b"]["n_purchases_36m"] == 1
    assert by_id["b"]["hist_max_value"] == 100.0
    assert by_id["d"]["n_purchases_12m"] == 2
    assert by_id["d"]["hist_max_value"] == 500.0
    assert by_id["d"]["days_since_last_purchase"] == pytest.approx(151.0)
    assert by_id["d"]["days_since_last_sale"] is None


def test_routine_and_opportunistic_need_enough_history():
    rows = [
        (f"r{y}", "routine", "P", ts(y, 3, 10), date(y, 3, 8), 1.0, "c1", "CEO")
        for y in range(2014, 2019)
    ]
    rows += [
        ("o1", "opp", "P", ts(2014, 5, 10), date(2014, 5, 8), 1.0, "c1", "CEO"),
        ("o2", "opp", "P", ts(2018, 9, 10), date(2018, 9, 8), 1.0, "c1", "CEO"),
        ("n1", "new", "P", ts(2018, 9, 10), date(2018, 9, 8), 1.0, "c1", "CEO"),
    ]
    out = {
        r["event_id"]: r["trader_type"]
        for r in routine_classification(events(rows)).iter_rows(named=True)
    }
    assert out["r2017"] == "routine" and out["r2018"] == "routine"
    assert out["r2015"] == "unclassified"  # fewer than three earlier years of history
    assert out["o2"] == "opportunistic"
    assert out["n1"] == "unclassified"  # no history is not evidence of unusual behaviour


# --- prices and returns ---------------------------------------------------------------------


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


def test_split_is_neutral_and_dividend_is_credited_in_the_total_return_index():
    raw = bars("X", [100.0, 102.0, 51.0, 52.0])
    actions = pl.DataFrame(
        [
            {
                "symbol": "X",
                "ex_date": raw["date"][2],
                "action": "split",
                "ratio": 2.0,
                "amount": None,
            },
            {
                "symbol": "X",
                "ex_date": raw["date"][3],
                "action": "cash_dividend",
                "ratio": None,
                "amount": 1.0,
            },
        ],
        schema={
            "symbol": pl.String,
            "ex_date": pl.Date,
            "action": pl.String,
            "ratio": pl.Float64,
            "amount": pl.Float64,
        },
    )
    out = adjust_prices(raw, actions)
    assert out["adj_close"].to_list() == pytest.approx([50.0, 51.0, 51.0, 52.0])
    assert out["tr_index"].to_list() == pytest.approx([1.0, 1.02, 1.02, 1.02 * (52.0 + 1.0) / 51.0])


def test_forward_returns_use_the_entry_open_and_never_read_prices_after_the_cutoff():
    cal = get_calendar("XNYS")
    closes = [10.0 + i for i in range(30)]
    raw = bars("X", closes)
    adjusted = adjust_prices(
        raw,
        pl.DataFrame(
            schema={
                "symbol": pl.String,
                "ex_date": pl.Date,
                "action": pl.String,
                "ratio": pl.Float64,
                "amount": pl.Float64,
            }
        ),
    )
    ev = pl.DataFrame({"event_id": ["e"], "symbol": ["X"], "entry_session": [raw["date"][2]]})
    out = forward_returns(ev, adjusted, cal, [1, 5], mode="open").row(0, named=True)
    entry_open = closes[2] - 0.5
    assert out["entry_price"] == pytest.approx(entry_open)
    assert out["ret_1"] == pytest.approx(closes[3] / entry_open - 1)
    assert out["ret_5"] == pytest.approx(closes[7] / entry_open - 1)
    cut = forward_returns(ev, adjusted, cal, [1, 5], mode="open", censor_after=raw["date"][5]).row(
        0, named=True
    )
    assert cut["ret_1"] == pytest.approx(closes[3] / entry_open - 1)
    assert cut["ret_5"] is None and cut["exit_date_5"] is None


def test_session_arithmetic_respects_weekends_and_holidays():
    cal = get_calendar("XNYS")
    days = pl.Series("d", [date(2019, 3, 1), date(2019, 3, 2), date(2019, 12, 24)])
    assert cal.shift_sessions(days, 1).to_list() == [
        date(2019, 3, 4),
        date(2019, 3, 5),
        date(2019, 12, 26),
    ]
    assert cal.shift_sessions(days, 0).to_list() == [
        date(2019, 3, 1),
        date(2019, 3, 4),
        date(2019, 12, 24),
    ]
    start = pl.Series("a", [date(2019, 3, 1), date(2019, 12, 24)])
    end = pl.Series("b", [date(2019, 3, 5), date(2019, 12, 26)])
    assert cal.sessions_between(start, end).to_list() == [2, 1]  # Christmas Day is not a session
