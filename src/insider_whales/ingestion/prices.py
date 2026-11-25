"""Download and cache market prices behind the provider interface; write coverage reports."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

import polars as pl

from insider_whales.config import REPO_ROOT, Settings, load_watchlist
from insider_whales.ingestion.providers.base import MarketDataProvider, ProviderCapabilities
from insider_whales.ingestion.providers.csv_import import CsvImportConfig, CsvImportProvider
from insider_whales.ingestion.providers.yfinance_provider import YFinanceProvider
from insider_whales.io.manifest import Manifest
from insider_whales.normalization.pipeline import load_processed, processed_dir
from insider_whales.normalization.security_master import (
    attach_prices_to_security,
    build_securities,
    load_overrides,
)

log = logging.getLogger(__name__)


def get_provider(
    settings: Settings, name: str = "yfinance", csv_config: CsvImportConfig | None = None
) -> MarketDataProvider:
    if name == "yfinance":
        return YFinanceProvider(
            pause_seconds=0.4,
            raw_dir=settings.data_dir / "raw" / "prices" / "yfinance",
        )
    if name == "csv_import":
        if csv_config is None:
            raise ValueError("csv_import needs a CsvImportConfig")
        return CsvImportProvider(csv_config)
    raise ValueError(f"unknown provider {name!r}")


def sample_symbols(settings: Settings, n_issuers: int = 50) -> list[str]:
    """Watchlist plus the most common reported tickers among conservative events."""
    wl = load_watchlist()
    events = load_processed(settings, "insider_events").filter(pl.col("is_conservative_set"))
    top = (
        events.filter(
            ~pl.col("reported_ticker").fill_null("").str.to_uppercase().is_in(["", "NONE", "N/A"])
        )
        .group_by(pl.col("reported_ticker").str.to_uppercase())
        .len()
        .sort("len", descending=True)
        .head(n_issuers)["reported_ticker"]
        .to_list()
    )
    # Known delisted / acquired names used to demonstrate free-data coverage gaps.
    probe = ["SIVB", "FRC", "TWTR", "ATVI", "XLNX", "CELG", "BBBY", "BBBYQ"]
    seen: list[str] = []
    for s in [*wl, *top, *probe]:
        if s and s not in seen:
            seen.append(s)
    return seen


def download_prices(
    settings: Settings,
    symbols: list[str] | None = None,
    provider_name: str = "yfinance",
    start: date | None = None,
    end: date | None = None,
) -> dict:
    """Fetch OHLCV and actions for each symbol; write Parquet and a coverage table."""
    start = start or date(2012, 1, 1)
    end = end or datetime.now(UTC).date()
    provider = get_provider(settings, provider_name)
    symbols = symbols or sample_symbols(settings)
    out = settings.data_dir / "processed" / "prices"
    out.mkdir(parents=True, exist_ok=True)
    price_frames: list[pl.DataFrame] = []
    action_frames: list[pl.DataFrame] = []
    coverage = []
    for sym in symbols:
        try:
            px = provider.ohlcv(sym, start, end)
            ax = provider.corporate_actions(sym, start, end)
        except Exception as e:
            log.warning("price download failed for %s: %s", sym, e)
            coverage.append(_cov(sym, "error", str(e), provider.capabilities))
            continue
        if px.height == 0:
            coverage.append(
                _cov(sym, "missing", "provider returned no rows", provider.capabilities)
            )
            continue
        price_frames.append(px.with_columns(pl.lit(provider_name).alias("provider")))
        if ax.height:
            action_frames.append(ax.with_columns(pl.lit(provider_name).alias("provider")))
        coverage.append(
            _cov(
                sym,
                "ok",
                None,
                provider.capabilities,
                first=str(px["date"].min()),
                last=str(px["date"].max()),
                rows=px.height,
                splits=ax.filter(pl.col("action") == "split").height if ax.height else 0,
            )
        )
        by_sym = out / "by_symbol"
        by_sym.mkdir(parents=True, exist_ok=True)
        px.write_parquet(by_sym / f"{sym}.parquet")
    if price_frames:
        pl.concat(price_frames).write_parquet(out / "prices_daily.parquet")
    if action_frames:
        pl.concat(action_frames).write_parquet(out / "corporate_actions.parquet")
    cov = pl.DataFrame(coverage)
    cov.write_parquet(out / "coverage.parquet")
    Manifest(settings.data_dir / "manifests" / "pipeline_steps.jsonl").append(
        {
            "key": "download-prices",
            "provider": provider_name,
            "symbols": symbols,
            "start": str(start),
            "end": str(end),
            "capabilities": provider.capabilities.as_dict(),
            "coverage": {
                "ok": int((cov["status"] == "ok").sum()),
                "missing": int((cov["status"] == "missing").sum()),
                "error": int((cov["status"] == "error").sum()),
            },
        }
    )
    return {"coverage": cov, "capabilities": provider.capabilities}


def _cov(symbol, status, reason, caps: ProviderCapabilities, **extra) -> dict:
    return {
        "symbol": symbol,
        "status": status,
        "reason": reason,
        "survivor_only": caps.survivor_only,
        "provider": caps.name,
        "first": extra.get("first"),
        "last": extra.get("last"),
        "rows": extra.get("rows"),
        "splits": extra.get("splits"),
    }


def build_and_write_security_master(settings: Settings) -> tuple[pl.DataFrame, pl.DataFrame]:
    filings = load_processed(settings, "filings")
    transactions = load_processed(settings, "transactions")
    overrides = load_overrides(REPO_ROOT / "configs" / "security_overrides.yaml")
    securities, identifiers = build_securities(filings, transactions, overrides)
    out = processed_dir(settings)
    securities.write_parquet(out / "securities.parquet")
    identifiers.write_parquet(out / "security_identifiers.parquet")
    return securities, identifiers


def coverage_report_markdown(cov: pl.DataFrame, caps: ProviderCapabilities) -> str:
    ok = cov.filter(pl.col("status") == "ok")
    missing = cov.filter(pl.col("status") == "missing")
    lines = [
        "# Price coverage (Milestone 1 sample)",
        "",
        f"Provider: **{caps.name}**. Survivor-only: **{caps.survivor_only}**.",
        "",
        caps.notes,
        "",
        f"- Symbols requested: {cov.height}",
        f"- With prices: {ok.height}",
        f"- Missing: {missing.height}",
        f"- Errors: {cov.filter(pl.col('status') == 'error').height}",
        "",
        "## Missing symbols",
        "",
        "| Symbol | Reason |",
        "|---|---|",
    ]
    for r in missing.iter_rows(named=True):
        lines.append(f"| {r['symbol']} | {r['reason'] or 'no rows'} |")
    if missing.height == 0:
        lines.append("| — | none |")
    lines += [
        "",
        "## Available symbols (sample)",
        "",
        "| Symbol | First | Last | Rows | Splits |",
        "|---|---|---|---|---|",
    ]
    for r in ok.head(30).iter_rows(named=True):
        lines.append(
            f"| {r['symbol']} | {r['first']} | {r['last']} | {r['rows']} | {r['splits']} |"
        )
    lines += [
        "",
        "## Implications",
        "",
        "Every result built on this provider is labelled **exploratory / survivor-only** and "
        "is not suitable for final profitability conclusions. Delisted and acquired names "
        "return no history; recycled tickers may return an unrelated security. Raw prices are "
        "rebuilt from split events before eligibility filters are applied.",
        "",
    ]
    return "\n".join(lines)


def map_sample_prices_to_securities(settings: Settings) -> pl.DataFrame:
    """Attach downloaded prices to security_ids via ticker windows; drop out-of-window rows."""
    identifiers = load_processed(settings, "security_identifiers")
    prices_path = settings.data_dir / "processed" / "prices" / "prices_daily.parquet"
    if not prices_path.exists():
        raise FileNotFoundError("run download-prices first")
    prices = pl.read_parquet(prices_path)
    mapped = []
    for sym in prices["symbol"].unique().to_list():
        # Every security that ever used this ticker.
        secs = (
            identifiers.filter((pl.col("id_type") == "ticker") & (pl.col("value") == sym.upper()))[
                "security_id"
            ]
            .unique()
            .to_list()
        )
        for sid in secs:
            try:
                part = attach_prices_to_security(
                    prices.filter(pl.col("symbol") == sym), identifiers, sid
                )
            except ValueError as e:
                log.warning("%s", e)
                continue
            if part.height:
                mapped.append(part.with_columns(pl.lit(sid).alias("security_id")))
    if not mapped:
        return pl.DataFrame()
    out = pl.concat(mapped)
    dest = settings.data_dir / "processed" / "prices" / "prices_by_security.parquet"
    out.write_parquet(dest)
    return out
