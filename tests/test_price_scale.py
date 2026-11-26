"""Ticker cleaning, ticker windows, resumable price download, shares and price coverage."""

from datetime import UTC, date, datetime

import pandas as pd
import polars as pl
import pytest

from insider_whales.calendar import get_calendar
from insider_whales.config import load_settings
from insider_whales.evaluation.price_coverage import (
    classify_pairs,
    evaluate_pairs,
    explain_missing,
    market_cap_group,
    series_verdicts,
    shares_asof,
)
from insider_whales.ingestion.fundamentals import shares_from_facts
from insider_whales.ingestion.price_runner import fetch_symbols, latest_status, status_manifest
from insider_whales.ingestion.providers.yfinance_provider import (
    NoDataError,
    RateLimitedError,
    YFinanceProvider,
)
from insider_whales.normalization.security_master import ticker_windows
from insider_whales.normalization.tickers import parse_reported_tickers

# --- reported tickers -----------------------------------------------------------------------


def _pairs(raw):
    return [(p.ticker, p.share_class) for p in parse_reported_tickers(raw)]


def test_dual_class_tickers_are_split_and_labelled():
    assert _pairs("QADA, QADB") == [("QADA", "A"), ("QADB", "B")]
    assert _pairs("NRCIA/B") == [("NRCIA", "A"), ("NRCIB", "B")]


def test_decorated_and_placeholder_tickers():
    assert _pairs("NYSE: SCS") == [("SCS", None)]
    assert _pairs("(CALX)") == [("CALX", None)]
    assert _pairs("N/A") == []
    assert _pairs(None) == []


# --- ticker windows -------------------------------------------------------------------------


def _filings(rows):
    return pl.DataFrame(
        rows,
        schema={"issuer_cik": pl.String, "reported_ticker": pl.String, "filing_date": pl.Date},
        orient="row",
    )


def test_each_share_class_gets_its_own_track():
    w = ticker_windows(
        _filings(
            [
                ("0000000001", "QADA, QADB", date(2016, 3, 1)),
                ("0000000001", "QADA, QADB", date(2020, 3, 1)),
            ]
        ),
        data_end=date(2020, 12, 31),
    )
    assert set(w["ticker"]) == {"QADA", "QADB"}
    assert set(w["track"]) == {"A", "B"}


def test_renamed_ticker_window_ends_before_the_new_one_starts():
    w = ticker_windows(
        _filings(
            [
                ("0000000002", "OLD", date(2015, 1, 5)),
                ("0000000002", "OLD", date(2016, 1, 5)),
                ("0000000002", "NEW", date(2017, 6, 1)),
            ]
        ),
        data_end=date(2020, 12, 31),
    ).sort("first_seen")
    old, new = w.row(0, named=True), w.row(1, named=True)
    assert old["valid_to"] == date(2017, 5, 31)
    assert new["valid_from"] == date(2017, 6, 1)


def test_ticker_of_a_stopped_filer_stops_being_valid():
    w = ticker_windows(
        _filings(
            [
                ("0000000003", "GONE", date(2014, 1, 5)),
                ("0000000004", "LIVE", date(2022, 11, 1)),
            ]
        ),
        data_end=date(2022, 12, 31),
    )
    gone = w.filter(pl.col("ticker") == "GONE").row(0, named=True)
    live = w.filter(pl.col("ticker") == "LIVE").row(0, named=True)
    assert gone["valid_to"] is not None and gone["valid_to"] < date(2016, 1, 1)
    assert live["valid_to"] is None


# --- vectorised execution timing ------------------------------------------------------------


def test_vectorised_first_executable_sessions_match_the_scalar_method():
    cal = get_calendar("XNYS")
    stamps = [
        datetime(2019, 3, 1, 8, 0, tzinfo=UTC),  # Friday before the open
        datetime(2019, 3, 1, 20, 0, tzinfo=UTC),  # Friday after the close
        datetime(2019, 3, 2, 12, 0, tzinfo=UTC),  # Saturday
        datetime(2019, 12, 24, 19, 0, tzinfo=UTC),  # Christmas Eve after the close
        datetime(2020, 1, 1, 9, 0, tzinfo=UTC),  # holiday
    ]
    series = pl.Series("available_at", stamps, dtype=pl.Datetime("us", "UTC"))
    for mode in ("open", "close"):
        vector = cal.first_executable_sessions(series, mode, 0).to_list()
        scalar = [cal.first_executable_bar(ts, mode, 0).session for ts in stamps]
        assert vector == scalar


# --- resumable download ---------------------------------------------------------------------


def _hist(day=date(2019, 1, 2)):
    return pd.DataFrame(
        {
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.0],
            "Close": [10.5],
            "Volume": [1000.0],
            "Dividends": [0.0],
            "Stock Splits": [0.0],
        },
        index=pd.to_datetime([day]),
    )


def test_download_is_resumable_and_separates_missing_from_error(tmp_path):
    settings = load_settings(overrides={"paths": {"data_dir": str(tmp_path)}})
    calls: list[str] = []
    state = {"throttle": True}

    def fetch(symbol, start, end):
        calls.append(symbol)
        if symbol == "NONE":
            raise NoDataError("delisted")
        if symbol == "BUSY" and state["throttle"]:
            raise RateLimitedError("slow down")
        return _hist()

    def make():
        return YFinanceProvider(
            fetch=fetch, pause_seconds=0, attempts=1, rate_limit_backoff=0, raw_dir=None
        )

    symbols = ["GOOD", "NONE", "BUSY"]
    first = fetch_symbols(settings, symbols, workers=1, provider=make(), say=lambda _: None)
    status = {s: r["status"] for s, r in latest_status(status_manifest(settings)).items()}
    assert status == {"GOOD": "ok", "NONE": "missing", "BUSY": "error"}
    assert first["ok"] == 1 and first["missing"] == 1 and first["error"] == 1

    calls.clear()
    state["throttle"] = False
    second = fetch_symbols(settings, symbols, workers=1, provider=make(), say=lambda _: None)
    assert calls == ["BUSY"]  # finished symbols are not asked for again
    assert second["skipped"] == 2 and second["ok"] == 1
    assert (tmp_path / "processed" / "prices" / "by_symbol" / "GOOD.parquet").exists()


def test_download_stops_when_the_provider_keeps_throttling(tmp_path):
    settings = load_settings(overrides={"paths": {"data_dir": str(tmp_path)}})

    def fetch(symbol, start, end):
        raise RateLimitedError("slow down")

    provider = YFinanceProvider(
        fetch=fetch, pause_seconds=0, attempts=1, rate_limit_backoff=0, raw_dir=None
    )
    symbols = [f"S{i}" for i in range(30)]
    counts = fetch_symbols(
        settings,
        symbols,
        workers=1,
        provider=provider,
        max_consecutive_rate_limits=3,
        say=lambda _: None,
    )
    assert counts["aborted_by_rate_limit"] == 1
    assert counts["error"] == 3


# --- shares outstanding ---------------------------------------------------------------------


def test_cover_page_shares_are_summed_per_filing_and_flag_multi_class():
    payload = {
        "cik": 5,
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {"val": 100, "accn": "A1", "end": "2019-12-31", "filed": "2020-02-01"},
                            {"val": 40, "accn": "A2", "end": "2020-03-31", "filed": "2020-05-01"},
                            {"val": 60, "accn": "A2", "end": "2020-03-31", "filed": "2020-05-01"},
                        ]
                    }
                }
            }
        },
    }
    rows = {r["accession_number"]: r for r in shares_from_facts(payload)}
    assert rows["A1"]["shares"] == 100 and not rows["A1"]["multi_class"]
    assert rows["A2"]["shares"] == 100 and rows["A2"]["multi_class"]
    assert rows["A1"]["issuer_cik"] == "0000000005"


def test_shares_are_taken_as_of_the_filing_date():
    shares = pl.DataFrame(
        {
            "issuer_cik": ["1", "1"],
            "filed": [date(2019, 2, 1), date(2019, 5, 1)],
            "shares": [100.0, 200.0],
            "multi_class": [False, False],
        }
    )
    events = pl.DataFrame(
        {
            "event_id": ["a", "b"],
            "issuer_cik": ["1", "1"],
            "filing_date": [date(2019, 4, 1), date(2019, 6, 1)],
        }
    )
    out = shares_asof(events, shares).sort("event_id")
    assert out["shares_outstanding"].to_list() == [100.0, 200.0]


def test_market_cap_groups():
    settings = load_settings()
    df = pl.DataFrame({"m": [None, 1e8, 5e8, 5e9, 5e10, 5e11]})
    groups = df.select(market_cap_group(settings, pl.col("m")))["mcap_group"].to_list()
    assert groups == ["unknown", "micro", "small", "mid", "large", "mega"]


# --- coverage reasons -----------------------------------------------------------------------


def _bars(symbol, start, n, low=9.0, high=11.0):
    days = pd.bdate_range(start, periods=n).date
    return pl.DataFrame(
        {"symbol": symbol, "date": list(days), "low": low, "high": high},
        schema={"symbol": pl.String, "date": pl.Date, "low": pl.Float64, "high": pl.Float64},
    )


def _events(rows):
    return pl.DataFrame(
        rows,
        schema={
            "event_id": pl.String,
            "security_id": pl.String,
            "trans_date": pl.Date,
            "entry_session": pl.Date,
            "vwap": pl.Float64,
        },
        orient="row",
    )


def _classify(events, prices):
    pairs = events.select(
        "event_id", pl.lit("SYM").alias("symbol"), pl.lit("historical").alias("kind")
    )
    evaluated = evaluate_pairs(events, pairs, prices)
    return classify_pairs(evaluated, series_verdicts(evaluated))


def test_event_with_matching_bars_is_ok():
    events = _events(
        [(f"e{i}", "s1", date(2019, 3, 1 + i), date(2019, 3, 4 + i), 10.0) for i in range(4)]
    )
    out = _classify(events, _bars("SYM", date(2019, 1, 2), 80))
    assert set(out["pair_reason"]) == {"ok"}


def test_series_that_starts_after_the_event_is_reported():
    events = _events([("e1", "s1", date(2015, 3, 2), date(2015, 3, 3), 10.0)])
    out = _classify(events, _bars("SYM", date(2019, 1, 2), 80))
    assert out["pair_reason"].to_list() == ["series_starts_after_event"]


def test_recycled_ticker_with_different_prices_is_a_mismatch():
    events = _events(
        [(f"e{i}", "s1", date(2019, 3, 1 + i), date(2019, 3, 4 + i), 100.0) for i in range(4)]
    )
    out = _classify(events, _bars("SYM", date(2019, 1, 2), 80))  # bars trade around 10
    assert set(out["pair_reason"]) == {"price_mismatch"}


def test_symbol_without_history_is_provider_missing():
    events = _events([("e1", "s1", date(2019, 3, 1), date(2019, 3, 4), 10.0)])
    out = _classify(events, _bars("OTHER", date(2019, 1, 2), 10))
    assert out["pair_reason"].to_list() == ["provider_missing"]


@pytest.mark.parametrize(
    ("symbols", "fetch", "listed", "current", "last_event", "expected"),
    [
        ([], {}, False, None, date(2015, 1, 1), "no_ticker"),
        (["A"], {}, False, None, date(2015, 1, 1), "not_fetched"),
        (["A"], {"A": {"status": "error"}}, False, None, date(2015, 1, 1), "fetch_error"),
        (
            ["A"],
            {"A": {"status": "missing"}},
            True,
            "B",
            date(2022, 1, 1),
            "ticker_changed_unmatched",
        ),
        (
            ["A"],
            {"A": {"status": "missing"}},
            True,
            "A",
            date(2022, 1, 1),
            "listed_but_provider_has_none",
        ),
        (
            ["A"],
            {"A": {"status": "missing"}},
            False,
            None,
            date(2022, 6, 1),
            "went_dark_but_still_filing",
        ),
        (
            ["A"],
            {"A": {"status": "missing"}},
            False,
            None,
            date(2015, 1, 1),
            "delisted_or_acquired_likely",
        ),
    ],
)
def test_every_missing_event_gets_one_reason(symbols, fetch, listed, current, last_event, expected):
    events = pl.DataFrame({"event_id": ["e1"], "security_id": ["s1"]})
    universe = pl.DataFrame(
        {
            "security_id": ["s1"],
            "currently_listed": [listed],
            "last_event": [last_event],
            "current_tickers": [[current] if current else None],
        },
        schema={
            "security_id": pl.String,
            "currently_listed": pl.Boolean,
            "last_event": pl.Date,
            "current_tickers": pl.List(pl.String),
        },
    )
    cands = pl.DataFrame(
        {
            "event_id": ["e1"] * len(symbols),
            "symbol": symbols,
            "pair_reason": ["provider_missing"] * len(symbols),
        },
        schema={"event_id": pl.String, "symbol": pl.String, "pair_reason": pl.String},
    )
    out = explain_missing(events, universe, fetch, cands)
    assert out["missing_reason"].to_list() == [expected]


# --- later splits ---------------------------------------------------------------------------


def test_download_sees_splits_after_the_window_but_stores_nothing_after_it(tmp_path):
    from insider_whales.ingestion.price_runner import PRICE_END, SPLIT_HORIZON

    settings = load_settings(overrides={"paths": {"data_dir": str(tmp_path)}})
    requested = {}

    def fetch(symbol, start, end):
        requested["end"] = end
        hist = pd.concat(
            [_hist(date(2019, 1, 2)), _hist(date(2024, 6, 10))]
        )  # a 2024 bar and split are beyond the window
        hist.loc[hist.index[-1], "Stock Splits"] = 10.0
        return hist

    provider = YFinanceProvider(fetch=fetch, pause_seconds=0, attempts=1, raw_dir=None)
    fetch_symbols(settings, ["SPL"], workers=1, provider=provider, say=lambda _: None)
    assert requested["end"] == SPLIT_HORIZON
    stored = pl.read_parquet(tmp_path / "processed" / "prices" / "by_symbol" / "SPL.parquet")
    assert stored["date"].max() <= PRICE_END
    # The 2019 bar was adjusted for the 2024 split and is rebuilt to the price traded then.
    assert stored["close"].to_list() == pytest.approx([105.0])
    assert not (tmp_path / "processed" / "prices" / "actions_by_symbol" / "SPL.parquet").exists()


def test_symbols_fetched_without_later_splits_are_fetched_again(tmp_path):
    from insider_whales.ingestion.price_runner import is_final

    assert not is_final({"status": "ok", "split_horizon": "2022-12-31"})
    assert not is_final({"status": "ok"})
    assert not is_final({"status": "error"})
    assert is_final({"status": "missing"})
    assert not is_final({"status": "missing"}, retry_missing=True)
