"""Price provider, corporate-action math and security master (mandatory tests T9–T11)."""

from datetime import date

import pandas as pd
import polars as pl
import pytest

from insider_whales.ingestion.providers.base import NotSupportedError
from insider_whales.ingestion.providers.csv_import import CsvImportConfig, CsvImportProvider
from insider_whales.ingestion.providers.yfinance_provider import (
    CAPABILITIES,
    YFinanceProvider,
    history_to_frames,
)
from insider_whales.normalization.corporate_actions import (
    Position,
    apply_actions,
    apply_split,
    total_return_index,
    unadjust_split_adjusted,
)
from insider_whales.normalization.security_master import (
    attach_prices_to_security,
    build_securities,
    resolve_symbol,
    ticker_windows,
)

# --- T9: split-adjusted position accounting -----------------------------------------------


def test_t9_forward_and_reverse_split_position_accounting():
    # Buy 100 shares at $1,000; 10-for-1 split; then 1-for-8 reverse.
    pos = Position(shares=100, cost_basis=100_000.0)
    after_10 = apply_split(pos, 10.0, price_after=100.0)
    assert after_10.shares == 1000 and after_10.cost_basis == 100_000.0
    assert after_10.cost_per_share == pytest.approx(100.0)
    after_rev = apply_split(after_10, 0.125, price_after=800.0)
    assert after_rev.shares == 125 and after_rev.cost_basis == pytest.approx(100_000.0)
    assert after_rev.cash == 0.0


def test_t9_fractional_shares_paid_in_lieu():
    pos = Position(shares=10, cost_basis=100.0)
    # 3-for-2 split → 15 shares; with whole shares only and a leftover from a prior
    # fractional position, cash in lieu is paid at the post-split price.
    pos = Position(shares=10.5, cost_basis=105.0)
    after = apply_split(pos, 2.0, price_after=5.0, allow_fractional=False)
    assert after.shares == 21 and after.cash == pytest.approx(0.0)
    pos = Position(shares=10.25, cost_basis=102.5)
    after = apply_split(pos, 2.0, price_after=5.0, allow_fractional=False)
    assert after.shares == 20
    assert after.cash == pytest.approx(0.5 * 5.0)  # 0.5 leftover share at $5
    assert after.cost_basis == pytest.approx(102.5 * (20 / 20.5))


def test_t9_yfinance_raw_price_rebuild_and_total_return():
    # Synthetic split-adjusted series: real close was ~$1,209 before the 10:1 split.
    dates = [date(2024, 6, 7), date(2024, 6, 10), date(2024, 6, 11)]
    hist = pd.DataFrame(
        {
            "Open": [119.77, 120.37, 121.77],
            "High": [121.0, 122.0, 123.0],
            "Low": [118.0, 119.0, 120.0],
            "Close": [120.888, 121.79, 120.91],
            "Volume": [1e6, 1e6, 1e6],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 10.0, 0.0],
        },
        index=pd.to_datetime(dates),
    )
    raw, actions = history_to_frames("NVDA", hist)
    assert actions.filter(pl.col("action") == "split")["ratio"].to_list() == [10.0]
    # Pre-split day is reconstructed to ~10× the adjusted close.
    pre = raw.filter(pl.col("date") == date(2024, 6, 7))["close"][0]
    post = raw.filter(pl.col("date") == date(2024, 6, 11))["close"][0]
    assert pre == pytest.approx(1208.88)
    assert post == pytest.approx(120.91)
    # Total return across the split day is the adjusted move, not a −90% crash.
    tr = total_return_index(raw, actions)
    split_day = tr.filter(pl.col("date") == date(2024, 6, 10))["total_return"][0]
    assert abs(split_day) < 0.05  # small move, not −90%


def test_t9_dividend_cash_and_return():
    pos = apply_actions(
        Position(shares=100, cost_basis=1000.0),
        pl.DataFrame(
            {
                "ex_date": [date(2024, 1, 2)],
                "action": ["cash_dividend"],
                "ratio": [None],
                "amount": [0.50],
                "detail": [None],
                "symbol": ["X"],
            },
            schema={
                "symbol": pl.String,
                "ex_date": pl.Date,
                "action": pl.String,
                "ratio": pl.Float64,
                "amount": pl.Float64,
                "detail": pl.String,
            },
        ),
        closes={date(2024, 1, 2): 10.0},
    )
    assert pos.cash == pytest.approx(50.0)


# --- T10: delisted stocks are not silently removed ----------------------------------------


def test_t10_missing_prices_are_recorded_not_dropped():
    def empty_fetch(symbol, start, end):
        return pd.DataFrame()

    provider = YFinanceProvider(fetch=empty_fetch, pause_seconds=0)
    px = provider.ohlcv("SIVB", date(2012, 1, 1), date(2024, 1, 1))
    assert px.height == 0
    assert CAPABILITIES.survivor_only is True
    with pytest.raises(NotSupportedError):
        provider.delistings("SIVB")


def test_t10_coverage_flags_missing_delisted_symbols(tmp_path):
    from insider_whales.config import load_settings
    from insider_whales.ingestion.prices import download_prices

    settings = load_settings(overrides={"paths": {"data_dir": str(tmp_path)}})
    # Seed empty processed tables so sample_symbols isn't used; pass symbols explicitly.
    (tmp_path / "processed" / "sec").mkdir(parents=True)
    pl.DataFrame({"is_conservative_set": [], "reported_ticker": []}).write_parquet(
        tmp_path / "processed" / "sec" / "insider_events.parquet"
    )

    def fetch(symbol, start, end):
        if symbol in {"SIVB", "FRC", "TWTR"}:
            return pd.DataFrame()
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
            index=pd.to_datetime([date(2020, 1, 2)]),
        )

    # Patch the provider used by download_prices.
    import insider_whales.ingestion.prices as prices_mod

    class Stub(YFinanceProvider):
        def __init__(self, **kw):
            super().__init__(fetch=fetch, pause_seconds=0, raw_dir=tmp_path / "raw" / "prices")

    prices_mod.YFinanceProvider = Stub
    result = download_prices(
        settings,
        symbols=["NVDA", "SIVB", "FRC", "TWTR"],
        start=date(2020, 1, 1),
        end=date(2020, 1, 31),
    )
    cov = result["coverage"]
    assert cov.filter(pl.col("status") == "ok")["symbol"].to_list() == ["NVDA"]
    missing = set(cov.filter(pl.col("status") == "missing")["symbol"])
    assert missing == {"SIVB", "FRC", "TWTR"}
    assert cov["survivor_only"].all()


# --- T11: recycled tickers do not merge companies -----------------------------------------


def test_t11_recycled_ticker_windows_stay_separate():
    filings = pl.DataFrame(
        {
            "issuer_cik": ["0001326801", "0001326801", "0009999999", "0009999999"],
            "reported_ticker": ["FB", "META", "FB", "FB"],
            "filing_date": [
                date(2019, 6, 1),
                date(2022, 1, 15),
                date(2025, 7, 1),
                date(2025, 8, 1),
            ],
        }
    )
    windows = ticker_windows(filings)
    fb = windows.filter(pl.col("ticker") == "FB").sort("issuer_cik")
    assert fb.height == 2
    assert fb["issuer_cik"].to_list() == ["0001326801", "0009999999"]
    # On a date inside Meta's FB window, resolve_symbol returns Meta only.
    ids = pl.DataFrame(
        {
            "security_id": ["0001326801:COMMON", "0009999999:COMMON"],
            "id_type": ["ticker", "ticker"],
            "value": ["FB", "FB"],
            "valid_from": [date(2012, 5, 18), date(2025, 6, 1)],
            "valid_to": [date(2021, 10, 27), None],
            "source": ["form345", "form345"],
            "confidence": ["medium", "medium"],
        }
    )
    assert resolve_symbol(ids, "FB", date(2019, 1, 1))["security_id"].to_list() == [
        "0001326801:COMMON"
    ]
    assert resolve_symbol(ids, "FB", date(2025, 7, 1))["security_id"].to_list() == [
        "0009999999:COMMON"
    ]


def test_t11_price_series_spanning_two_windows_is_clipped():
    ids = pl.DataFrame(
        {
            "security_id": ["0001:COMMON", "0002:COMMON"],
            "id_type": ["ticker", "ticker"],
            "value": ["ZZ", "ZZ"],
            "valid_from": [date(2015, 1, 1), date(2022, 1, 1)],
            "valid_to": [date(2020, 12, 31), None],
            "source": ["form345", "form345"],
            "confidence": ["medium", "medium"],
        }
    )
    prices = pl.DataFrame(
        {
            "symbol": ["ZZ"] * 4,
            "date": [date(2018, 1, 2), date(2019, 6, 1), date(2022, 3, 1), date(2023, 1, 3)],
            "open": [1.0] * 4,
            "high": [1.0] * 4,
            "low": [1.0] * 4,
            "close": [1.0] * 4,
            "volume": [100.0] * 4,
        }
    )
    for_old = attach_prices_to_security(prices, ids, "0001:COMMON")
    assert for_old["date"].to_list() == [date(2018, 1, 2), date(2019, 6, 1)]
    for_new = attach_prices_to_security(prices, ids, "0002:COMMON")
    assert for_new["date"].to_list() == [date(2022, 3, 1), date(2023, 1, 3)]


def test_security_master_builds_from_filings_and_overrides():
    filings = pl.DataFrame(
        {
            "issuer_cik": ["0001326801", "0001326801", "0000000001"],
            "reported_ticker": ["FB", "META", "NONE"],
            "filing_date": [date(2019, 1, 1), date(2022, 1, 1), date(2019, 1, 1)],
        }
    )
    tx = pl.DataFrame(
        {
            "issuer_cik": ["0001326801", "0000000001"],
            "security_title": ["Class A Common Stock", "Common Stock"],
            "table": ["nonderiv", "nonderiv"],
        }
    )
    overrides = [
        {
            "issuer_cik": "0001326801",
            "value": "FB",
            "valid_from": "2012-05-18",
            "valid_to": "2021-10-27",
        },
        {"issuer_cik": "0001326801", "value": "META", "valid_from": "2021-10-28"},
    ]
    secs, ids = build_securities(filings, tx, overrides)
    assert "0001326801:A" in secs["security_id"].to_list() or any(
        s.startswith("0001326801:") for s in secs["security_id"]
    )
    assert ids.filter(pl.col("value") == "META").height >= 1
    # Placeholder ticker NEVER becomes an identifier.
    assert ids.filter(pl.col("value") == "NONE").height == 0


def test_csv_import_provider_reads_unadjusted_files(tmp_path):
    px = tmp_path / "px.csv"
    px.write_text("sym,dt,o,h,l,c,v\nAAA,2020-01-02,10,11,9,10.5,1000\n")
    ax = tmp_path / "ax.csv"
    ax.write_text("sym,dt,kind,ratio,amount\nAAA,2020-01-03,split,2.0,\n")
    provider = CsvImportProvider(
        CsvImportConfig(
            ohlcv_path=px,
            actions_path=ax,
            ohlcv_columns={
                "symbol": "sym",
                "date": "dt",
                "open": "o",
                "high": "h",
                "low": "l",
                "close": "c",
                "volume": "v",
            },
            actions_columns={
                "symbol": "sym",
                "ex_date": "dt",
                "action": "kind",
                "ratio": "ratio",
                "amount": "amount",
            },
        )
    )
    assert provider.ohlcv("AAA", date(2020, 1, 1), date(2020, 1, 31)).height == 1
    assert provider.corporate_actions("AAA", date(2020, 1, 1), date(2020, 1, 31))["ratio"][0] == 2.0
    assert provider.capabilities.has_unadjusted_prices is True


def test_unadjust_is_identity_when_no_splits():
    prices = pl.DataFrame(
        {
            "symbol": ["X"],
            "date": [date(2020, 1, 2)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [100.0],
        }
    )
    out = unadjust_split_adjusted(
        prices,
        pl.DataFrame(
            {"ex_date": [], "ratio": []}, schema={"ex_date": pl.Date, "ratio": pl.Float64}
        ),
    )
    assert out["close"].to_list() == [10.5]
