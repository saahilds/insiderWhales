"""Generic import of a provider's exported files (CSV or Parquet) into the canonical schemas.

This is the hook for any better data set later: map its columns, declare what it can do, and
the rest of the pipeline uses it unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import polars as pl

from insider_whales.ingestion.providers.base import (
    ACTIONS_SCHEMA,
    OHLCV_SCHEMA,
    NotSupportedError,
    ProviderCapabilities,
    empty,
)


@dataclass
class CsvImportConfig:
    ohlcv_path: Path
    actions_path: Path | None = None
    delistings_path: Path | None = None
    # canonical column -> column name in the source file
    ohlcv_columns: dict[str, str] = field(default_factory=dict)
    actions_columns: dict[str, str] = field(default_factory=dict)
    prices_are_unadjusted: bool = True
    capabilities: ProviderCapabilities | None = None


def _read(path: Path) -> pl.DataFrame:
    return (
        pl.read_parquet(path)
        if path.suffix == ".parquet"
        else pl.read_csv(path, try_parse_dates=True)
    )


def _canonical(df: pl.DataFrame, mapping: dict[str, str], schema: dict) -> pl.DataFrame:
    renamed = df.rename({v: k for k, v in mapping.items() if v in df.columns})
    missing = [c for c in schema if c not in renamed.columns]
    renamed = renamed.with_columns(pl.lit(None).alias(c) for c in missing)
    return renamed.select(list(schema)).cast(schema)


class CsvImportProvider:
    def __init__(self, cfg: CsvImportConfig):
        if not cfg.prices_are_unadjusted:
            raise ValueError("import unadjusted prices plus corporate actions")
        self.cfg = cfg
        self.capabilities = cfg.capabilities or ProviderCapabilities(
            name="csv_import",
            has_delisted=False,
            has_unadjusted_prices=True,
            reconstructs_unadjusted=False,
            has_corporate_actions=cfg.actions_path is not None,
            has_historical_identifiers=False,
            has_point_in_time_shares=False,
            has_point_in_time_fundamentals=False,
            has_sector=False,
            notes="Generic import; declare capabilities explicitly for real data sets.",
        )
        self._ohlcv = _canonical(_read(cfg.ohlcv_path), cfg.ohlcv_columns, OHLCV_SCHEMA)
        self._actions = (
            _canonical(_read(cfg.actions_path), cfg.actions_columns, ACTIONS_SCHEMA)
            if cfg.actions_path
            else empty(ACTIONS_SCHEMA)
        )

    def _slice(self, df: pl.DataFrame, symbol: str, start: date, end: date, col: str):
        return df.filter((pl.col("symbol") == symbol) & pl.col(col).is_between(start, end)).sort(
            col
        )

    def ohlcv(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        return self._slice(self._ohlcv, symbol, start, end, "date")

    def corporate_actions(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        return self._slice(self._actions, symbol, start, end, "ex_date")

    def identifiers(self, symbol: str) -> pl.DataFrame:
        raise NotSupportedError("csv_import: identifiers not configured")

    def shares_outstanding(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        raise NotSupportedError("csv_import: shares not configured")

    def market_cap(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        raise NotSupportedError("csv_import: market cap not configured")

    def sector_industry(self, symbol: str) -> dict:
        raise NotSupportedError("csv_import: sector not configured")

    def delistings(self, symbol: str) -> pl.DataFrame:
        if self.cfg.delistings_path is None:
            raise NotSupportedError("csv_import: delistings not configured")
        return _read(self.cfg.delistings_path).filter(pl.col("symbol") == symbol)

    def fundamentals(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        raise NotSupportedError("csv_import: fundamentals not configured")
