"""Mandatory tests T1–T3 (execution timing) plus availability and point-in-time rules."""

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from insider_whales.calendar import get_calendar
from insider_whales.config import load_settings
from insider_whales.pit import (
    BASIS_ACCEPTANCE,
    BASIS_FILING_DATE,
    NEW_YORK,
    as_of,
    filing_availability,
    join_as_of,
    with_available_at,
)

CAL = get_calendar("XNYS", date(2015, 1, 1), date(2026, 12, 31))
AVAIL = load_settings().availability


def ny(*args) -> datetime:
    return datetime(*args, tzinfo=NEW_YORK)


def accepted(*args):
    return filing_availability(ny(*args), None, AVAIL).available_at


# --- T1: a filing after the close cannot execute at the preceding close -----------------


def test_t1_after_close_filing_cannot_use_that_close():
    avail = accepted(2024, 3, 12, 16, 5)  # Tuesday, after the 16:00 close
    close_bar = CAL.first_executable_bar(avail, "close")
    open_bar = CAL.first_executable_bar(avail, "open")
    assert close_bar.session == date(2024, 3, 13)
    assert close_bar.timestamp == ny(2024, 3, 13, 16, 0)
    assert open_bar.session == date(2024, 3, 13)
    assert open_bar.timestamp == ny(2024, 3, 13, 9, 30)


def test_t1_intraday_filing_can_use_same_day_close_but_not_open():
    avail = accepted(2024, 3, 12, 11, 0)
    assert CAL.first_executable_bar(avail, "close").session == date(2024, 3, 12)
    assert CAL.first_executable_bar(avail, "open").session == date(2024, 3, 13)


def test_processing_buffer_decides_same_day_open():
    # 5 + 15 minutes of buffers: accepted 09:10 is available exactly at the 09:30 open.
    assert CAL.first_executable_bar(accepted(2024, 3, 12, 9, 10)).session == date(2024, 3, 12)
    assert CAL.first_executable_bar(accepted(2024, 3, 12, 9, 11)).session == date(2024, 3, 13)


# --- T2: weekend disclosures execute no earlier than the next session --------------------


def test_t2_weekend_and_friday_evening_filings_execute_monday():
    for avail in (accepted(2024, 3, 16, 10, 0), accepted(2024, 3, 15, 20, 0)):
        bar = CAL.first_executable_bar(avail, "open")
        assert bar.session == date(2024, 3, 18)
        assert bar.timestamp > avail


def test_t2_date_only_filing_executes_next_session():
    a = filing_availability(None, date(2024, 3, 15), AVAIL)  # Friday, date only
    assert a.basis == BASIS_FILING_DATE
    assert a.available_at == ny(2024, 3, 15, 23, 59, 59)
    assert CAL.first_executable_bar(a.available_at, "open").session == date(2024, 3, 18)
    assert CAL.first_executable_bar(a.available_at, "close").session == date(2024, 3, 18)


# --- T3: exchange holidays are respected ------------------------------------------------


@pytest.mark.parametrize(
    ("filed", "expected"),
    [
        ((2024, 11, 27, 18, 0), date(2024, 11, 29)),  # Thanksgiving Thursday closed
        ((2024, 3, 28, 18, 0), date(2024, 4, 1)),  # Good Friday closed
        ((2018, 12, 4, 18, 0), date(2018, 12, 6)),  # national day of mourning (G.H.W. Bush)
        ((2025, 1, 8, 18, 0), date(2025, 1, 10)),  # national day of mourning (Carter)
        ((2024, 12, 24, 18, 0), date(2024, 12, 26)),  # Christmas
    ],
)
def test_t3_holidays_skipped(filed, expected):
    assert CAL.first_executable_bar(accepted(*filed), "open").session == expected


def test_t3_early_close_is_respected():
    # Day after Thanksgiving closes at 13:00 New York time.
    assert CAL.session_close(date(2024, 11, 29)) == ny(2024, 11, 29, 13, 0)
    bar = CAL.first_executable_bar(accepted(2024, 11, 29, 13, 30), "close")
    assert bar.session == date(2024, 12, 2)


def test_extra_delay_sessions_and_calendar_helpers():
    avail = accepted(2024, 3, 12, 18, 0)
    assert CAL.first_executable_bar(avail, "open", extra_delay_sessions=2).session == date(
        2024, 3, 15
    )
    assert not CAL.is_session(date(2024, 3, 16))
    assert CAL.next_session(date(2024, 3, 15)) == date(2024, 3, 18)
    assert CAL.sessions(date(2024, 3, 15), date(2024, 3, 19)) == [
        date(2024, 3, 15),
        date(2024, 3, 18),
        date(2024, 3, 19),
    ]


def test_naive_timestamps_rejected():
    with pytest.raises(ValueError):
        CAL.first_executable_bar(datetime(2024, 3, 12, 18, 0), "open")  # noqa: DTZ001


# --- availability and point-in-time access ------------------------------------------------


def test_vectorised_availability_matches_scalar_rule():
    df = pl.DataFrame(
        {
            "acceptance_ts": [ny(2024, 3, 12, 16, 54, 28), None],
            "filing_date": [date(2024, 3, 12), date(2024, 7, 3)],
        }
    ).with_columns(pl.col("acceptance_ts").dt.convert_time_zone("UTC"))
    out = with_available_at(df, AVAIL)
    assert out["availability_basis"].to_list() == [BASIS_ACCEPTANCE, BASIS_FILING_DATE]
    assert out["available_at"][0] == ny(2024, 3, 12, 16, 54, 28) + timedelta(minutes=20)
    assert out["available_at"][1] == ny(2024, 7, 3, 23, 59, 59)  # EDT handled


def _versions():
    return pl.DataFrame(
        {
            "key": ["a", "a", "b"],
            "value": [1.0, 2.0, 5.0],
            "available_at": [
                datetime(2024, 1, 2, 22, tzinfo=UTC),
                datetime(2024, 1, 10, 22, tzinfo=UTC),
                datetime(2024, 1, 5, 22, tzinfo=UTC),
            ],
        }
    )


def test_as_of_hides_future_rows_and_keeps_latest_version():
    df = _versions()
    early = as_of(df, datetime(2024, 1, 3, tzinfo=UTC), key=["key"])
    assert early.to_dicts() == [
        {"key": "a", "value": 1.0, "available_at": datetime(2024, 1, 2, 22, tzinfo=UTC)}
    ]
    late = as_of(df, datetime(2024, 2, 1, tzinfo=UTC), key=["key"]).sort("key")
    assert late["value"].to_list() == [2.0, 5.0]


def test_join_as_of_uses_only_published_rows():
    decisions = pl.DataFrame(
        {
            "key": ["a", "a"],
            "decision_ts": [
                datetime(2024, 1, 9, tzinfo=UTC),
                datetime(2024, 1, 11, tzinfo=UTC),
            ],
        }
    )
    joined = join_as_of(decisions, _versions(), left_time="decision_ts", by=["key"])
    assert joined["value"].to_list() == [1.0, 2.0]


def test_t17_timing_is_deterministic():
    stamps = [accepted(2024, 3, d, h, 0) for d in range(11, 16) for h in (8, 12, 17)]
    run1 = [CAL.first_executable_bar(s, "open") for s in stamps]
    run2 = [
        get_calendar("XNYS", date(2015, 1, 1), date(2026, 12, 31)).first_executable_bar(s)
        for s in stamps
    ]
    assert run1 == run2
