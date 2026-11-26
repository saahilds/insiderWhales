"""Resumable bulk price download behind the provider interface.

Each symbol is fetched once and its outcome appended to a manifest: `ok` (rows written),
`missing` (the provider has no history, with its reason) or `error` (throttling or network; to
be retried). Rerunning skips every symbol with a final outcome, so an interrupted run, or a run
on another machine that copies `data/`, continues instead of starting over.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from insider_whales.config import Settings
from insider_whales.ingestion.providers.yfinance_provider import (
    FetchResult,
    ProviderTransientError,
    YFinanceProvider,
)
from insider_whales.io.manifest import Manifest

log = logging.getLogger(__name__)

PRICE_START = date(2011, 1, 1)  # one year before the first development-era events, for lookbacks
PRICE_END = date(2022, 12, 31)
# Yahoo reports prices adjusted for every split up to today. Raw prices can only be rebuilt if
# the download also covers the splits after PRICE_END, so the request runs to this date and the
# bars (and actions) after PRICE_END are dropped before anything is stored.
SPLIT_HORIZON = date(2026, 12, 31)


def is_final(record: dict[str, Any] | None, retry_missing: bool = False) -> bool:
    """A symbol needs no further request: no history (final), or prices fetched with splits."""
    if not record:
        return False
    if record["status"] == "missing":
        return not retry_missing
    return record["status"] == "ok" and record.get("split_horizon") == str(SPLIT_HORIZON)


def prices_dir(settings: Settings) -> Path:
    return settings.data_dir / "processed" / "prices"


def status_manifest(settings: Settings, provider: str = "yfinance") -> Manifest:
    return Manifest(settings.data_dir / "manifests" / f"price_fetch_{provider}.jsonl")


def latest_status(manifest: Manifest) -> dict[str, dict[str, Any]]:
    """Most recent outcome per symbol."""
    out: dict[str, dict[str, Any]] = {}
    for rec in manifest.records():
        out[rec["symbol"]] = rec
    return out


def stratified_sample(symbols: pl.DataFrame, n: int, seed: int) -> list[str]:
    """A deterministic sample that keeps the mix of listed/delisted, event volume and era.

    Strata: currently listed or not, event-count tercile, and the year of the last event.
    Allocation is proportional with at least one symbol per stratum, so rare strata such as
    long-delisted small issuers are represented.
    """
    df = symbols.with_columns(
        pl.col("n_events").qcut([1 / 3, 2 / 3], labels=["low", "mid", "high"]).alias("_vol"),
        pl.col("last_event").dt.year().alias("_year"),
    ).with_columns(
        pl.concat_str(
            [pl.col("currently_listed").cast(pl.String), pl.col("_vol").cast(pl.String), "_year"],
            separator="|",
        ).alias("_stratum")
    )
    rng = random.Random(seed)
    total = df.height
    picked: list[str] = []
    for _, group in sorted(df.group_by("_stratum"), key=lambda kv: str(kv[0])):
        members = sorted(group["symbol"].to_list())
        k = max(1, round(n * len(members) / total))
        picked.extend(rng.sample(members, min(k, len(members))))
    rng.shuffle(picked)
    return sorted(picked[:n]) if len(picked) > n else sorted(picked)


def fetch_symbols(
    settings: Settings,
    symbols: list[str],
    *,
    start: date = PRICE_START,
    end: date = PRICE_END,
    workers: int = 3,
    pause_seconds: float = 0.4,
    retry_missing: bool = False,
    max_consecutive_rate_limits: int = 8,
    provider: YFinanceProvider | None = None,
    say: Callable[[str], Any] = print,
) -> dict[str, int]:
    """Fetch every symbol without a final outcome; stop early if Yahoo keeps throttling us."""
    provider = provider or YFinanceProvider(
        pause_seconds=pause_seconds, raw_dir=settings.data_dir / "raw" / "prices" / "yfinance"
    )
    manifest = status_manifest(settings)
    done = latest_status(manifest)
    todo = [s for s in symbols if not is_final(done.get(s), retry_missing)]
    by_symbol = prices_dir(settings) / "by_symbol"
    actions_dir = prices_dir(settings) / "actions_by_symbol"
    by_symbol.mkdir(parents=True, exist_ok=True)
    actions_dir.mkdir(parents=True, exist_ok=True)
    counts = {"ok": 0, "missing": 0, "error": 0, "skipped": len(symbols) - len(todo)}
    rate_limit_streak = 0
    aborted = False

    def work(symbol: str):
        try:
            return symbol, provider.fetch(symbol, start, SPLIT_HORIZON), None
        except ProviderTransientError as e:
            return symbol, None, e

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = iter(todo)
        futures = {pool.submit(work, s) for s in [next(pending, None) for _ in range(workers)] if s}
        finished = 0
        while futures:
            for fut in as_completed(list(futures)):
                futures.discard(fut)
                symbol, result, err = fut.result()
                finished += 1
                if err is not None:
                    counts["error"] += 1
                    rate_limit_streak = rate_limit_streak + 1 if err.rate_limited else 0
                    manifest.append(
                        {
                            "symbol": symbol,
                            "status": "error",
                            "reason": str(err),
                            "start": str(start),
                            "end": str(end),
                        }
                    )
                else:
                    rate_limit_streak = 0
                    px = result.ohlcv.filter(pl.col("date") <= end)
                    acts = result.actions.filter(pl.col("ex_date") <= end)
                    if result.status == "ok" and px.height == 0:
                        result = FetchResult(symbol, "missing", "no bars in window", px, acts)
                    counts[result.status] += 1
                    manifest.append(
                        {
                            "symbol": symbol,
                            "status": result.status,
                            "reason": result.reason,
                            "rows": px.height,
                            "first": str(px["date"].min()) if px.height else None,
                            "last": str(px["date"].max()) if px.height else None,
                            "splits": int((acts["action"] == "split").sum()) if acts.height else 0,
                            "split_horizon": str(SPLIT_HORIZON),
                            "start": str(start),
                            "end": str(end),
                            "fetched_at": datetime.now(UTC).isoformat(),
                        }
                    )
                    price_file = by_symbol / f"{_safe(symbol)}.parquet"
                    action_file = actions_dir / f"{_safe(symbol)}.parquet"
                    action_file.unlink(missing_ok=True)
                    if result.status == "ok":
                        px.write_parquet(price_file)
                        if acts.height:
                            acts.write_parquet(action_file)
                    else:
                        price_file.unlink(missing_ok=True)
                if finished % 250 == 0:
                    say(f"{finished}/{len(todo)} fetched: {counts}")
                if rate_limit_streak >= max_consecutive_rate_limits:
                    aborted = True
                if not aborted and (nxt := next(pending, None)):
                    futures.add(pool.submit(work, nxt))
            if aborted:
                break
    counts["aborted_by_rate_limit"] = int(aborted)
    return counts


def _safe(symbol: str) -> str:
    return symbol.replace("/", "_")


def consolidate_prices(settings: Settings) -> tuple[int, int]:
    """Stack the per-symbol files into `prices_daily.parquet` and `corporate_actions.parquet`."""
    d = prices_dir(settings)
    p_files = sorted((d / "by_symbol").glob("*.parquet"))
    a_files = sorted((d / "actions_by_symbol").glob("*.parquet"))
    n_p = n_a = 0
    if p_files:
        pl.scan_parquet(p_files).with_columns(pl.lit("yfinance").alias("provider")).sink_parquet(
            d / "prices_daily.parquet"
        )
        n_p = pl.scan_parquet(d / "prices_daily.parquet").select(pl.len()).collect().item()
    if a_files:
        pl.scan_parquet(a_files).with_columns(pl.lit("yfinance").alias("provider")).sink_parquet(
            d / "corporate_actions.parquet"
        )
        n_a = pl.scan_parquet(d / "corporate_actions.parquet").select(pl.len()).collect().item()
    return n_p, n_a
