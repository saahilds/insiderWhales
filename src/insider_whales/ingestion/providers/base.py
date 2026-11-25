"""Market-data provider interface and the canonical schemas every adapter returns."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Protocol, runtime_checkable

import polars as pl

# Prices are stored unadjusted ("raw"); adjustments are computed from corporate actions.
OHLCV_SCHEMA = {
    "symbol": pl.String,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}
ACTIONS_SCHEMA = {
    "symbol": pl.String,
    "ex_date": pl.Date,
    "action": pl.String,  # split | cash_dividend | delisting | merger | symbol_change
    "ratio": pl.Float64,  # new shares per old share for splits (10.0 = 10-for-1, 0.125 = 1-for-8)
    "amount": pl.Float64,  # cash per share (raw, not split-adjusted)
    "detail": pl.String,
}


class NotSupportedError(NotImplementedError):
    """The provider cannot supply this data; callers must treat it as unavailable."""


@dataclass(frozen=True)
class ProviderCapabilities:
    name: str
    has_delisted: bool
    has_unadjusted_prices: bool
    reconstructs_unadjusted: bool
    has_corporate_actions: bool
    has_historical_identifiers: bool
    has_point_in_time_shares: bool
    has_point_in_time_fundamentals: bool
    has_sector: bool
    notes: str = ""

    @property
    def survivor_only(self) -> bool:
        return not self.has_delisted

    def as_dict(self) -> dict:
        return {**asdict(self), "survivor_only": self.survivor_only}


@runtime_checkable
class MarketDataProvider(Protocol):
    capabilities: ProviderCapabilities

    def ohlcv(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """Raw (unadjusted) daily OHLCV in OHLCV_SCHEMA."""

    def corporate_actions(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """Splits, dividends and other actions in ACTIONS_SCHEMA."""

    def identifiers(self, symbol: str) -> pl.DataFrame:
        """Historical identifiers (symbol, id_type, value, valid_from, valid_to)."""

    def shares_outstanding(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """(symbol, date, shares, available_at) where available_at is when it was public."""

    def market_cap(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """(symbol, date, market_cap)."""

    def sector_industry(self, symbol: str) -> dict:
        """{'sector': ..., 'industry': ...} as of today unless the provider is point-in-time."""

    def delistings(self, symbol: str) -> pl.DataFrame:
        """(symbol, delist_date, reason, delisting_return)."""

    def fundamentals(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """Point-in-time fundamentals with publication timestamps."""


def empty(schema: dict) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)
