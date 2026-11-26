"""Benchmark ETF prices (market, size and sector) for market-relative features and returns."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from insider_whales.config import Settings
from insider_whales.ingestion.price_runner import PRICE_END, PRICE_START, SPLIT_HORIZON, prices_dir
from insider_whales.ingestion.providers.yfinance_provider import YFinanceProvider
from insider_whales.normalization.adjusted_prices import adjust_prices

BENCHMARKS = {
    "SPY": "S&P 500 (market)",
    "IWM": "Russell 2000 (small caps)",
    "MDY": "S&P MidCap 400",
    "XLB": "Materials",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLK": "Technology",
    "XLP": "Consumer staples",
    "XLU": "Utilities",
    "XLV": "Health care",
    "XLY": "Consumer discretionary",
    "XLC": "Communication services (from 2018)",
    "XLRE": "Real estate (from 2015)",
}


def fetch_benchmarks(
    settings: Settings,
    provider: YFinanceProvider | None = None,
    say: Callable[[str], Any] = print,
) -> pl.DataFrame:
    """Download the benchmark ETFs and write `benchmarks_adjusted.parquet`."""
    provider = provider or YFinanceProvider(
        pause_seconds=0.5, raw_dir=settings.data_dir / "raw" / "prices" / "yfinance"
    )
    prices, actions = [], []
    for symbol in BENCHMARKS:
        result = provider.fetch(symbol, PRICE_START, SPLIT_HORIZON)
        if result.status != "ok":
            say(f"{symbol}: no data ({result.reason})")
            continue
        prices.append(result.ohlcv.filter(pl.col("date") <= PRICE_END))
        actions.append(result.actions.filter(pl.col("ex_date") <= PRICE_END))
    adjusted = adjust_prices(pl.concat(prices), pl.concat(actions))
    adjusted.write_parquet(prices_dir(settings) / "benchmarks_adjusted.parquet")
    return adjusted
