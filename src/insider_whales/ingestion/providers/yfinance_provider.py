"""Free yfinance adapter for smoke tests and exploration.

Limitations (also reported in every result through `capabilities`):
- no history for delisted or acquired companies, so results are survivor-only;
- a ticker returns whichever security holds it today (recycled tickers);
- OHLC and dividends come split-adjusted even with auto_adjust=False; raw prices are rebuilt
  from the split events;
- sector, industry and shares outstanding are current values, not point-in-time;
- unofficial scraping of Yahoo: may be rate-limited or break, personal research use only.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import polars as pl

from insider_whales.ingestion.providers.base import (
    ACTIONS_SCHEMA,
    OHLCV_SCHEMA,
    NotSupportedError,
    ProviderCapabilities,
    empty,
)
from insider_whales.normalization.corporate_actions import (
    split_factor_after,
    unadjust_split_adjusted,
)

log = logging.getLogger(__name__)

CAPABILITIES = ProviderCapabilities(
    name="yfinance",
    has_delisted=False,
    has_unadjusted_prices=False,
    reconstructs_unadjusted=True,
    has_corporate_actions=True,
    has_historical_identifiers=False,
    has_point_in_time_shares=False,
    has_point_in_time_fundamentals=False,
    has_sector=True,
    notes="Survivor-only; current ticker owner; raw prices rebuilt from split events.",
)

HistoryFetcher = Callable[[str, date, date], pd.DataFrame]


class NoDataError(Exception):
    """The provider answered, and has no price history for this symbol (final, cacheable)."""


class RateLimitedError(Exception):
    """The provider is throttling us; back off and retry later."""


class ProviderTransientError(Exception):
    """A fetch failed for a reason that may go away (network, throttling); never cached."""

    def __init__(self, message: str, rate_limited: bool = False):
        super().__init__(message)
        self.rate_limited = rate_limited


@dataclass(frozen=True)
class FetchResult:
    symbol: str
    status: str  # ok | missing
    reason: str | None
    ohlcv: pl.DataFrame
    actions: pl.DataFrame


def _yahoo_history(symbol: str, start: date, end: date) -> pd.DataFrame:
    """One Yahoo history call; Yahoo's "no data" answers become `NoDataError`."""
    import yfinance as yf
    from yfinance import exceptions as yfe

    yf.config.debug.hide_exceptions = False
    try:
        return yf.Ticker(symbol).history(
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            auto_adjust=False,
            actions=True,
        )
    except (yfe.YFPricesMissingError, yfe.YFTzMissingError, yfe.YFTickerMissingError) as e:
        raise NoDataError(str(e)) from e
    except yfe.YFRateLimitError as e:
        raise RateLimitedError(str(e)) from e


def history_to_frames(symbol: str, hist: pd.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """yfinance history (split-adjusted) -> (raw OHLCV, actions with raw dividends)."""
    if hist is None or hist.empty:
        return empty(OHLCV_SCHEMA), empty(ACTIONS_SCHEMA)
    h = hist.reset_index()
    dates = [pd.Timestamp(d).date() for d in h[h.columns[0]]]
    adj = pl.DataFrame(
        {
            "symbol": [symbol] * len(h),
            "date": dates,
            "open": h["Open"].astype(float).tolist(),
            "high": h["High"].astype(float).tolist(),
            "low": h["Low"].astype(float).tolist(),
            "close": h["Close"].astype(float).tolist(),
            "volume": h["Volume"].astype(float).tolist(),
        },
        schema=OHLCV_SCHEMA,
    )
    splits_col = h["Stock Splits"] if "Stock Splits" in h else pd.Series([0.0] * len(h))
    divs_col = h["Dividends"] if "Dividends" in h else pd.Series([0.0] * len(h))
    splits = pl.DataFrame(
        {"ex_date": dates, "ratio": splits_col.astype(float).tolist()},
        schema={"ex_date": pl.Date, "ratio": pl.Float64},
    ).filter(pl.col("ratio") > 0)
    raw = unadjust_split_adjusted(adj, splits)
    divs = pl.DataFrame(
        {"ex_date": dates, "amount": divs_col.astype(float).tolist()},
        schema={"ex_date": pl.Date, "amount": pl.Float64},
    ).filter(pl.col("amount") > 0)
    if divs.height:
        divs = divs.with_columns(pl.col("amount") * split_factor_after(divs["ex_date"], splits))
    actions = (
        pl.concat(
            [
                splits.select(
                    pl.lit(symbol).alias("symbol"),
                    "ex_date",
                    pl.lit("split").alias("action"),
                    "ratio",
                    pl.lit(None, dtype=pl.Float64).alias("amount"),
                    pl.lit(None, dtype=pl.String).alias("detail"),
                ),
                divs.select(
                    pl.lit(symbol).alias("symbol"),
                    "ex_date",
                    pl.lit("cash_dividend").alias("action"),
                    pl.lit(None, dtype=pl.Float64).alias("ratio"),
                    "amount",
                    pl.lit(None, dtype=pl.String).alias("detail"),
                ),
            ]
        )
        .cast(ACTIONS_SCHEMA)
        .sort(["ex_date", "action"])
    )
    return raw, actions


class YFinanceProvider:
    capabilities = CAPABILITIES

    def __init__(
        self,
        fetch: HistoryFetcher | None = None,
        pause_seconds: float = 0.5,
        raw_dir: Path | None = None,
        attempts: int = 3,
        rate_limit_backoff: float = 20.0,
    ):
        self._fetch = fetch or _yahoo_history
        self._pause = pause_seconds
        self._raw_dir = raw_dir
        self._attempts = attempts
        self._rate_limit_backoff = rate_limit_backoff

    def _raw_path(self, symbol: str, start: date, end: date) -> Path | None:
        if self._raw_dir is None:
            return None
        safe = symbol.replace("/", "_")
        return self._raw_dir / safe / f"{start.isoformat()}_{end.isoformat()}.parquet"

    def _sidecar(self, path: Path) -> Path:
        return path.with_suffix(".reason.json")

    def _fetch_raw(self, symbol: str, start: date, end: date) -> tuple[pd.DataFrame, str | None]:
        """Provider response exactly as received, cached once per symbol and date range.

        Returns (history, reason); an empty history carries the provider's reason. Throttling
        and network errors are retried with backoff and then raised as `ProviderTransientError`
        without writing anything to the cache, so a later run asks again.
        """
        path = self._raw_path(symbol, start, end)
        if path is not None and path.exists():
            side = self._sidecar(path)
            reason = json.loads(side.read_text())["reason"] if side.exists() else None
            return pd.read_parquet(path), reason
        hist: pd.DataFrame | None = None
        reason: str | None = None
        last_error = "no attempt made"
        rate_limited = False
        for attempt in range(self._attempts):
            try:
                hist = self._fetch(symbol, start, end)
                break
            except NoDataError as e:
                hist, reason = pd.DataFrame(), str(e)
                break
            except RateLimitedError as e:
                last_error, rate_limited = f"rate limited: {e}", True
                time.sleep(self._rate_limit_backoff * 2**attempt)
            except Exception as e:  # yfinance raises many exception types
                last_error, rate_limited = f"{type(e).__name__}: {e}", False
                log.warning("yfinance %s attempt %d failed: %s", symbol, attempt + 1, e)
                time.sleep(self._pause * 2**attempt)
        else:
            raise ProviderTransientError(last_error, rate_limited)
        if self._pause:
            time.sleep(self._pause)
        frame = hist if hist is not None else pd.DataFrame()
        if frame.empty:
            reason = reason or "provider returned no rows"
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(path)
            if reason:
                self._sidecar(path).write_text(json.dumps({"reason": reason}))
        return frame, reason

    def fetch(self, symbol: str, start: date, end: date) -> FetchResult:
        """Raw prices and actions plus an explicit status (`ok` or `missing` with a reason)."""
        hist, reason = self._fetch_raw(symbol, start, end)
        ohlcv, actions = history_to_frames(symbol, hist)
        if ohlcv.height == 0:
            reason = reason or "provider returned no rows"
            return FetchResult(symbol, "missing", reason, ohlcv, actions)
        return FetchResult(symbol, "ok", None, ohlcv, actions)

    def ohlcv(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        return self.fetch(symbol, start, end).ohlcv

    def corporate_actions(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        return self.fetch(symbol, start, end).actions

    def identifiers(self, symbol: str) -> pl.DataFrame:
        raise NotSupportedError("yfinance has no historical identifiers")

    def shares_outstanding(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        raise NotSupportedError("yfinance share counts are not point-in-time; use SEC XBRL")

    def market_cap(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        raise NotSupportedError("market cap is computed from SEC shares outstanding (Milestone 2)")

    def sector_industry(self, symbol: str) -> dict:
        import yfinance as yf

        info = yf.Ticker(symbol).info or {}
        return {"sector": info.get("sector"), "industry": info.get("industry"), "as_of": "current"}

    def delistings(self, symbol: str) -> pl.DataFrame:
        raise NotSupportedError("yfinance has no delisting data")

    def fundamentals(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        raise NotSupportedError("yfinance fundamentals are not point-in-time; use SEC XBRL")
