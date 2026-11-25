"""Point-in-time rules: when a fact becomes knowable, and "as of" access to tables.

Research code reads time-varying tables only through `as_of` / `join_as_of`, so nothing
published after a decision time can leak into that decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import polars as pl

from insider_whales.config import AvailabilityConfig

NEW_YORK = ZoneInfo("America/New_York")
UTC_DTYPE = pl.Datetime("us", "UTC")

BASIS_ACCEPTANCE = "acceptance"
BASIS_FILING_DATE = "filing_date_end_of_day"


@dataclass(frozen=True)
class Availability:
    available_at: datetime
    basis: str


def _date_only_time(cfg: AvailabilityConfig) -> time:
    return time.fromisoformat(cfg.date_only_time)


def filing_availability(
    acceptance_ts: datetime | None, filing_date: date | None, cfg: AvailabilityConfig
) -> Availability:
    """Conservative public-availability time for one filing."""
    if acceptance_ts is not None:
        if acceptance_ts.tzinfo is None:
            raise ValueError("acceptance_ts must be timezone-aware")
        buffer = timedelta(minutes=cfg.dissemination_buffer_minutes + cfg.processing_buffer_minutes)
        return Availability(acceptance_ts + buffer, BASIS_ACCEPTANCE)
    if filing_date is None:
        raise ValueError("need an acceptance timestamp or a filing date")
    eod = datetime.combine(filing_date, _date_only_time(cfg), tzinfo=NEW_YORK)
    return Availability(eod, BASIS_FILING_DATE)


def with_available_at(
    df: pl.DataFrame | pl.LazyFrame,
    cfg: AvailabilityConfig,
    acceptance_col: str = "acceptance_ts",
    filing_date_col: str = "filing_date",
) -> pl.DataFrame | pl.LazyFrame:
    """Vectorised `filing_availability`: adds `available_at` (UTC) and `availability_basis`."""
    buffer = timedelta(minutes=cfg.dissemination_buffer_minutes + cfg.processing_buffer_minutes)
    t = _date_only_time(cfg)
    eod_offset = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    names = df.collect_schema().names()
    has_acc = acceptance_col in names
    eod = (
        (pl.col(filing_date_col).cast(pl.Datetime("us")) + eod_offset)
        .dt.replace_time_zone("America/New_York")
        .dt.convert_time_zone("UTC")
    )
    if has_acc:
        acc = pl.col(acceptance_col).dt.convert_time_zone("UTC") + buffer
        available = pl.when(pl.col(acceptance_col).is_not_null()).then(acc).otherwise(eod)
        basis = (
            pl.when(pl.col(acceptance_col).is_not_null())
            .then(pl.lit(BASIS_ACCEPTANCE))
            .otherwise(pl.lit(BASIS_FILING_DATE))
        )
    else:
        available, basis = eod, pl.lit(BASIS_FILING_DATE)
    return df.with_columns(
        available.cast(UTC_DTYPE).alias("available_at"),
        basis.alias("availability_basis"),
    )


def _as_lit(ts: datetime) -> pl.Expr:
    if ts.tzinfo is None:
        raise ValueError("decision timestamps must be timezone-aware")
    return pl.lit(ts).cast(UTC_DTYPE)


def as_of(
    df: pl.DataFrame | pl.LazyFrame,
    ts: datetime,
    *,
    time_col: str = "available_at",
    key: list[str] | None = None,
    order_cols: list[str] | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """Rows knowable at `ts`. With `key`, keep only the latest known version per key.

    Ties on `time_col` are broken by `order_cols` (e.g. accession) so results are deterministic.
    """
    out = df.filter(pl.col(time_col) <= _as_lit(ts))
    if key:
        sort_cols = [time_col, *(order_cols or [])]
        out = out.sort([*key, *sort_cols]).group_by(key, maintain_order=True).last()
    return out


def join_as_of(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    left_time: str,
    right_time: str = "available_at",
    by: list[str] | None = None,
    suffix: str = "_right",
) -> pl.DataFrame:
    """For each left row, attach the latest right row with right_time <= left_time."""
    left_sorted = left.sort([*(by or []), left_time])
    right_sorted = right.sort([*(by or []), right_time])
    return left_sorted.join_asof(
        right_sorted,
        left_on=left_time,
        right_on=right_time,
        by=by,
        strategy="backward",
        suffix=suffix,
        check_sortedness=False,
    )
