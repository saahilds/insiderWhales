"""Exchange trading calendar and the single rule for when a signal can first execute."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import lru_cache
from typing import Literal

import exchange_calendars as xcals
import numpy as np
import pandas as pd

ExecutionMode = Literal["open", "close"]


@dataclass(frozen=True)
class ExecutionBar:
    session: date
    timestamp: datetime  # UTC instant of the open or close auction
    mode: ExecutionMode


def _to_utc_ns(ts: datetime) -> int:
    if ts.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return int(pd.Timestamp(ts).tz_convert("UTC").value)


def _utc_ns(col: pd.Series) -> np.ndarray:
    return col.dt.tz_convert(None).to_numpy().astype("datetime64[ns]").astype("int64")


class TradingCalendar:
    """Regular-session calendar (holidays, early closes) backed by exchange_calendars."""

    def __init__(self, exchange: str = "XNYS", start: date = date(2005, 1, 1), end=None):
        end = end or date(2027, 12, 31)
        cal = xcals.get_calendar(exchange, start=start.isoformat(), end=end.isoformat())
        sched = cal.schedule
        self.exchange = exchange
        self._sessions: list[date] = [d.date() for d in sched.index]
        self._session_index = {d: i for i, d in enumerate(self._sessions)}
        self._opens = _utc_ns(sched["open"])
        self._closes = _utc_ns(sched["close"])

    @property
    def first_session(self) -> date:
        return self._sessions[0]

    @property
    def last_session(self) -> date:
        return self._sessions[-1]

    def is_session(self, d: date) -> bool:
        return d in self._session_index

    def sessions(self, start: date, end: date) -> list[date]:
        return self._sessions[
            bisect_left(self._sessions, start) : bisect_right(self._sessions, end)
        ]

    def next_session(self, d: date, n: int = 1) -> date:
        """The n-th session strictly after `d` (n >= 1)."""
        if n < 1:
            raise ValueError("n must be >= 1")
        return self._session_at(bisect_right(self._sessions, d) + n - 1)

    def session_open(self, d: date) -> datetime:
        return self._ts(self._opens[self._require(d)])

    def session_close(self, d: date) -> datetime:
        return self._ts(self._closes[self._require(d)])

    def first_executable_bar(
        self,
        available_at: datetime,
        mode: ExecutionMode = "open",
        extra_delay_sessions: int = 0,
    ) -> ExecutionBar:
        """First regular-session open (or close) at or after `available_at`.

        `available_at` must already include dissemination and processing buffers. Every
        simulated entry and exit is routed through this method.
        """
        if extra_delay_sessions < 0:
            raise ValueError("extra_delay_sessions must be >= 0")
        times = self._opens if mode == "open" else self._closes
        idx = int(np.searchsorted(times, _to_utc_ns(available_at), side="left"))
        idx += extra_delay_sessions
        session = self._session_at(idx)
        return ExecutionBar(session=session, timestamp=self._ts(times[idx]), mode=mode)

    def _session_at(self, idx: int) -> date:
        if idx < 0 or idx >= len(self._sessions):
            raise ValueError(f"outside calendar range {self.first_session}..{self.last_session}")
        return self._sessions[idx]

    def _require(self, d: date) -> int:
        try:
            return self._session_index[d]
        except KeyError:
            raise ValueError(f"{d} is not a {self.exchange} session") from None

    @staticmethod
    def _ts(ns: int) -> datetime:
        return datetime.fromtimestamp(ns / 1e9, tz=UTC)


@lru_cache(maxsize=4)
def get_calendar(exchange: str = "XNYS", start: date = date(2005, 1, 1), end=None):
    return TradingCalendar(exchange, start, end)
