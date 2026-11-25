"""Typed configuration loaded from YAML, with environment and CLI overrides."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "configs" / "base.yaml"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PathsConfig(_Strict):
    data_dir: Path = Path("data")
    reports_dir: Path = Path("reports")


class SecConfig(_Strict):
    data_sets_page: str
    full_index_base: str
    submissions_bulk_url: str
    submissions_api_base: str
    archives_base: str
    max_requests_per_second: float = Field(4.0, gt=0, le=5.0)
    max_retries: int = Field(6, ge=0)
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    timeout_seconds: float = 120.0


class CalendarConfig(_Strict):
    exchange: str = "XNYS"
    start: date = date(2005, 1, 1)
    end: date = date(2027, 12, 31)


class AvailabilityConfig(_Strict):
    dissemination_buffer_minutes: float = Field(5, ge=0)
    processing_buffer_minutes: float = Field(15, ge=0)
    date_only_time: str = "23:59:59"


class ExecutionConfig(_Strict):
    primary_mode: Literal["open", "close"] = "open"
    extra_delay_sessions: int = Field(0, ge=0)


class SplitsConfig(_Strict):
    warmup: tuple[date, date]
    development: tuple[date, date]
    validation: tuple[date, date]
    final_test: tuple[date, date]

    @field_validator("*")
    @classmethod
    def _ordered(cls, v: tuple[date, date]) -> tuple[date, date]:
        if v[0] > v[1]:
            raise ValueError(f"split start {v[0]} is after end {v[1]}")
        return v

    def split_of(self, d: date) -> str | None:
        for name in ("warmup", "development", "validation", "final_test"):
            lo, hi = getattr(self, name)
            if lo <= d <= hi:
                return name
        return None


class UniverseConfig(_Strict):
    min_price: float = 5.0
    min_market_cap: float = 300e6
    min_adv_dollars: float = 2e6
    adv_window_days: int = 20
    market_cap_groups: dict[str, tuple[float, float | None]]


class EventStudyConfig(_Strict):
    horizons: tuple[int, ...] = (1, 5, 10, 20, 60, 120)


class CostsConfig(_Strict):
    scenarios_bps_round_trip: dict[str, float]


class PortfolioConfig(_Strict):
    initial_capital: float = 100_000
    max_positions: int = 20
    target_position_weight: float = 0.05
    max_position_weight: float = 0.05
    max_sector_weight: float = 0.25
    max_adv_participation: float = 0.01
    leverage: float = 1.0
    allow_fractional_shares: bool = False
    transaction_cost_bps_round_trip: float = 25


class DelistingConfig(_Strict):
    assumed_return_performance_delist: float = -0.30


class Settings(_Strict):
    paths: PathsConfig
    sec: SecConfig
    calendar: CalendarConfig
    availability: AvailabilityConfig
    execution: ExecutionConfig
    splits: SplitsConfig
    universe: UniverseConfig
    event_study: EventStudyConfig
    costs: CostsConfig
    portfolio: PortfolioConfig
    delisting: DelistingConfig
    seed: int

    @property
    def data_dir(self) -> Path:
        p = self.paths.data_dir
        return p if p.is_absolute() else REPO_ROOT / p

    @property
    def reports_dir(self) -> Path:
        p = self.paths.reports_dir
        return p if p.is_absolute() else REPO_ROOT / p

    def config_hash(self) -> str:
        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_settings(
    path: Path | str | None = None, overrides: dict[str, Any] | None = None
) -> Settings:
    """Load settings from YAML, then apply environment and explicit overrides."""
    load_dotenv(REPO_ROOT / ".env", override=False)
    raw = yaml.safe_load(Path(path or DEFAULT_CONFIG).read_text())
    env_data_dir = os.environ.get("INSIDER_WHALES_DATA_DIR")
    if env_data_dir:
        raw = _deep_merge(raw, {"paths": {"data_dir": env_data_dir}})
    if overrides:
        raw = _deep_merge(raw, overrides)
    return Settings.model_validate(raw)


def load_watchlist(path: Path | str | None = None) -> list[str]:
    p = Path(path or REPO_ROOT / "configs" / "watchlist.yaml")
    return [t.strip().upper() for t in yaml.safe_load(p.read_text())["tickers"]]


def sec_user_agent() -> str:
    """Return the SEC User-Agent; SEC fair access requires a name and contact email."""
    load_dotenv(REPO_ROOT / ".env", override=False)
    ua = os.environ.get("SEC_USER_AGENT", "").strip()
    if not ua or "@" not in ua:
        raise RuntimeError(
            "SEC_USER_AGENT is not set (or has no contact email). Copy .env.example to .env "
            "and set it before contacting SEC."
        )
    return ua
