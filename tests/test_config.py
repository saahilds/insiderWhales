from datetime import date

import pytest

from insider_whales.config import load_settings, load_watchlist, sec_user_agent


def test_base_config_loads_and_hash_is_stable():
    a = load_settings()
    b = load_settings()
    assert a.config_hash() == b.config_hash()
    assert a.sec.max_requests_per_second <= 5
    assert a.event_study.horizons == (1, 5, 10, 20, 60, 120)


def test_override_changes_hash():
    base = load_settings()
    other = load_settings(overrides={"execution": {"extra_delay_sessions": 1}})
    assert other.execution.extra_delay_sessions == 1
    assert other.config_hash() != base.config_hash()


def test_rate_limit_above_spec_cap_is_rejected():
    with pytest.raises(ValueError):
        load_settings(overrides={"sec": {"max_requests_per_second": 6}})


def test_split_lookup():
    s = load_settings().splits
    assert s.split_of(date(2013, 6, 1)) == "warmup"
    assert s.split_of(date(2018, 6, 1)) == "development"
    assert s.split_of(date(2022, 1, 3)) == "validation"
    assert s.split_of(date(2024, 1, 3)) == "final_test"


def test_watchlist_has_example_tickers():
    wl = load_watchlist()
    assert "NVDA" in wl and len(wl) == 14


def test_user_agent_required(monkeypatch):
    monkeypatch.setattr("insider_whales.config.load_dotenv", lambda *a, **k: None)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    with pytest.raises(RuntimeError):
        sec_user_agent()
    monkeypatch.setenv("SEC_USER_AGENT", "Research (Someone) someone@example.com")
    assert sec_user_agent().endswith("someone@example.com")
