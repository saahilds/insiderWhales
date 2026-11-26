"""Which insider events have a usable free price series, and why not when they do not.

For every event the symbol is chosen from the security master's ticker windows (the symbol the
issuer used at the filing date), and the issuer's current SEC ticker is tried as a second
candidate for companies that changed their symbol. A candidate series is accepted only if it has
bars around the event and its prices agree with the price in the insider's own Form 4 (a recycled
ticker belongs to a different company and fails that check). Events without a usable series get
exactly one reason.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from insider_whales.calendar import get_calendar
from insider_whales.config import Settings
from insider_whales.features.fundamentals import shares_asof
from insider_whales.ingestion.price_runner import latest_status, prices_dir, status_manifest
from insider_whales.normalization.pipeline import processed_dir, scan_processed

DATA_END = date(2022, 12, 31)
CONFIDENCE_RANK = {"high": 0, "medium": 1, "low": 2}
CONSISTENCY_BAND = (0.8, 1.25)  # Form 4 price vs that day's raw low/high, as a ratio
MIN_CHECKS = 3  # fewer comparable events than this and a series cannot be judged
MIN_CONSISTENT_SHARE = 0.5
ENTRY_SLACK_DAYS = 7  # first bar must be within this many days after the entry session
FULL_HORIZON_DAYS = 170  # about 120 sessions

REASONS = (
    "ok",
    "no_ticker",
    "delisted_or_acquired_likely",
    "ticker_changed_unmatched",
    "listed_but_provider_has_none",
    "went_dark_but_still_filing",
    "series_starts_after_event",
    "series_ends_before_event",
    "price_mismatch",
    "no_bar_near_entry",
    "fetch_error",
    "not_fetched",
)

EVENT_COLUMNS = [
    "event_id",
    "security_id",
    "issuer_cik",
    "trans_code",
    "trans_date",
    "filing_date",
    "available_at",
    "vwap",
    "is_conservative_set",
    "plan_status",
]


def market_cap_group(settings: Settings, mcap: pl.Expr) -> pl.Expr:
    """Group label from configured bounds; groups below the smallest bound are `micro`."""
    groups = settings.universe.market_cap_groups
    expr = pl.when(mcap.is_null()).then(pl.lit("unknown"))
    smallest = min(lo for lo, _ in groups.values())
    expr = expr.when(mcap < smallest).then(pl.lit("micro"))
    for name, (lo, hi) in sorted(groups.items(), key=lambda kv: kv[1][0]):
        cond = mcap >= lo if hi is None else (mcap >= lo) & (mcap < hi)
        expr = expr.when(cond).then(pl.lit(name))
    return expr.otherwise(pl.lit("unknown")).alias("mcap_group")


def load_events(settings: Settings) -> pl.DataFrame:
    return (
        scan_processed(settings, "insider_events")
        .select(EVENT_COLUMNS)
        .filter(pl.col("filing_date") <= DATA_END)
        .collect()
    )


def with_entry_sessions(events: pl.DataFrame, settings: Settings) -> pl.DataFrame:
    cal = get_calendar(settings.calendar.exchange)
    sessions = cal.first_executable_sessions(
        events["available_at"],
        settings.execution.primary_mode,
        settings.execution.extra_delay_sessions,
    )
    return events.with_columns(sessions.alias("entry_session"))


def historical_candidates(events: pl.DataFrame, identifiers: pl.DataFrame) -> pl.DataFrame:
    """One candidate symbol per event: the security's ticker window that covers the filing date."""
    ids = identifiers.filter(pl.col("id_type") == "ticker").select(
        "security_id",
        pl.col("value").alias("ticker"),
        "valid_from",
        "valid_to",
        pl.col("confidence").replace_strict(CONFIDENCE_RANK, default=3).alias("_conf"),
        pl.col("source").eq("override").cast(pl.Int8).mul(-1).alias("_override"),
    )
    joined = events.select("event_id", "security_id", "filing_date").join(ids, on="security_id")
    inside = joined.filter(
        ((pl.col("valid_from").is_null()) | (pl.col("valid_from") <= pl.col("filing_date")))
        & ((pl.col("valid_to").is_null()) | (pl.col("valid_to") >= pl.col("filing_date")))
    )
    best = (
        inside.sort(["event_id", "_override", "_conf", "ticker"])
        .group_by("event_id", maintain_order=True)
        .first()
    )
    return best.select(
        "event_id",
        pl.col("ticker").str.replace_all(".", "-", literal=True).alias("symbol"),
        pl.lit("historical").alias("kind"),
    )


def current_candidates(events: pl.DataFrame, universe: pl.DataFrame) -> pl.DataFrame:
    """The issuer's current SEC ticker, for companies whose symbol changed."""
    cur = (
        universe.filter(pl.col("current_tickers").is_not_null())
        .select("security_id", pl.col("current_tickers").list.first().alias("ticker"))
        .with_columns(pl.col("ticker").str.replace_all(".", "-", literal=True).alias("symbol"))
        .select("security_id", "symbol")
    )
    return (
        events.select("event_id", "security_id")
        .join(cur, on="security_id")
        .select("event_id", "symbol", pl.lit("current").alias("kind"))
    )


def evaluate_pairs(events: pl.DataFrame, pairs: pl.DataFrame, prices: pl.DataFrame) -> pl.DataFrame:
    """For each (event, candidate symbol): bars near the event and the Form 4 price check."""
    ev = events.select("event_id", "security_id", "trans_date", "entry_session", "vwap")
    p = pairs.join(ev, on="event_id")
    px = prices.select("symbol", "date", "low", "high").sort("symbol", "date")
    span = (
        prices.group_by("symbol")
        .agg(pl.col("date").min().alias("series_first"), pl.col("date").max().alias("series_last"))
        .sort("symbol")
    )
    p = p.join(span, on="symbol", how="left").sort("symbol", "trans_date")
    # Bar at or just before the transaction date, for the price agreement check.
    at_trade = p.join_asof(
        px.rename({"date": "trade_bar", "low": "bar_low", "high": "bar_high"}),
        left_on="trans_date",
        right_on="trade_bar",
        by="symbol",
        strategy="backward",
        check_sortedness=False,
        tolerance="4d",
    )
    lo, hi = CONSISTENCY_BAND
    at_trade = at_trade.with_columns(
        pl.when(pl.col("bar_low").is_null() | pl.col("vwap").is_null() | (pl.col("vwap") <= 0))
        .then(pl.lit(None, dtype=pl.Boolean))
        .otherwise(
            (pl.col("vwap") >= lo * pl.col("bar_low")) & (pl.col("vwap") <= hi * pl.col("bar_high"))
        )
        .alias("consistent")
    ).sort("symbol", "entry_session")
    with_entry = at_trade.join_asof(
        px.select("symbol", pl.col("date").alias("entry_bar")),
        left_on="entry_session",
        right_on="entry_bar",
        by="symbol",
        strategy="forward",
        check_sortedness=False,
        tolerance=f"{ENTRY_SLACK_DAYS}d",
    )
    return with_entry.drop("bar_low", "bar_high", "trade_bar")


def series_verdicts(pairs: pl.DataFrame) -> pl.DataFrame:
    """Does a symbol's series agree with the Form 4 prices of the security that uses it?"""
    return (
        pairs.filter(pl.col("series_first").is_not_null())
        .group_by("security_id", "symbol")
        .agg(
            pl.col("consistent").is_not_null().sum().alias("n_checked"),
            pl.col("consistent").fill_null(False).sum().alias("n_consistent"),
        )
        .with_columns(
            (
                (pl.col("n_checked") >= MIN_CHECKS)
                & (
                    pl.col("n_consistent") / pl.col("n_checked").clip(lower_bound=1)
                    < MIN_CONSISTENT_SHARE
                )
            ).alias("series_mismatch"),
            (pl.col("n_checked") < MIN_CHECKS).alias("series_unverified"),
        )
        .sort("security_id", "symbol")
    )


def classify_pairs(pairs: pl.DataFrame, verdicts: pl.DataFrame) -> pl.DataFrame:
    p = pairs.join(verdicts, on=["security_id", "symbol"], how="left")
    reason = (
        pl.when(pl.col("series_first").is_null())
        .then(pl.lit("provider_missing"))
        .when(pl.col("series_mismatch").fill_null(False))
        .then(pl.lit("price_mismatch"))
        .when(pl.col("trans_date") < pl.col("series_first"))
        .then(pl.lit("series_starts_after_event"))
        .when(pl.col("entry_bar").is_null() & (pl.col("entry_session") > pl.col("series_last")))
        .then(pl.lit("series_ends_before_event"))
        .when(pl.col("entry_bar").is_null())
        .then(pl.lit("no_bar_near_entry"))
        .otherwise(pl.lit("ok"))
    )
    rank = (
        pl.when(reason == "ok")
        .then(0)
        .when(reason == "no_bar_near_entry")
        .then(1)
        .when(reason == "series_ends_before_event")
        .then(2)
        .when(reason == "series_starts_after_event")
        .then(3)
        .when(reason == "price_mismatch")
        .then(4)
        .otherwise(5)
    )
    return p.with_columns(
        reason.alias("pair_reason"),
        rank.alias("_rank"),
        (pl.col("kind") == "current").cast(pl.Int8).alias("_kind_rank"),
    )


def explain_missing(
    events: pl.DataFrame,
    universe: pl.DataFrame,
    fetch_status: dict[str, dict[str, Any]],
    symbols_by_event: pl.DataFrame,
) -> pl.DataFrame:
    """Reason for events whose every candidate symbol returned nothing."""
    info = universe.select(
        "security_id",
        "currently_listed",
        pl.col("last_event").alias("issuer_last_event"),
        pl.col("current_tickers").list.first().alias("current_ticker"),
    )
    cand = symbols_by_event.group_by("event_id").agg(
        pl.col("symbol").alias("symbols"),
        pl.col("pair_reason").alias("pair_reasons"),
    )
    err = {s for s, r in fetch_status.items() if r["status"] == "error"}
    out = (
        events.select("event_id", "security_id")
        .join(cand, on="event_id", how="left")
        .join(info, on="security_id", how="left")
    )
    rows = []
    for r in out.iter_rows(named=True):
        syms = r["symbols"] or []
        if not syms:
            rows.append((r["event_id"], "no_ticker"))
        elif any(s in err for s in syms):
            rows.append((r["event_id"], "fetch_error"))
        elif any(s not in fetch_status for s in syms):
            rows.append((r["event_id"], "not_fetched"))
        elif r["currently_listed"] and r["current_ticker"] and r["current_ticker"] not in syms:
            rows.append((r["event_id"], "ticker_changed_unmatched"))
        elif r["currently_listed"]:
            rows.append((r["event_id"], "listed_but_provider_has_none"))
        elif r["issuer_last_event"] and r["issuer_last_event"] >= date(DATA_END.year - 1, 1, 1):
            rows.append((r["event_id"], "went_dark_but_still_filing"))
        else:
            rows.append((r["event_id"], "delisted_or_acquired_likely"))
    return pl.DataFrame(
        rows, schema={"event_id": pl.String, "missing_reason": pl.String}, orient="row"
    )


def build_event_prices(settings: Settings) -> pl.DataFrame:
    """The event-level price table (`event_prices.parquet`) and everything it needs."""
    out_dir = prices_dir(settings)
    events = with_entry_sessions(load_events(settings), settings)
    identifiers = pl.read_parquet(processed_dir(settings) / "security_identifiers.parquet")
    universe = pl.read_parquet(processed_dir(settings) / "universe.parquet")
    prices = pl.read_parquet(
        out_dir / "prices_daily.parquet", columns=["symbol", "date", "low", "high"]
    )
    fetch = latest_status(status_manifest(settings))

    pairs = pl.concat(
        [historical_candidates(events, identifiers), current_candidates(events, universe)]
    ).unique(["event_id", "symbol"], keep="first", maintain_order=True)
    evaluated = evaluate_pairs(events, pairs, prices)
    verdicts = series_verdicts(evaluated)
    classified = classify_pairs(evaluated, verdicts)

    best = (
        classified.sort(["event_id", "_rank", "_kind_rank", "symbol"])
        .group_by("event_id", maintain_order=True)
        .first()
    )
    with_series = best.filter(pl.col("pair_reason") != "provider_missing")["event_id"]
    explained = explain_missing(
        events.filter(~pl.col("event_id").is_in(with_series)), universe, fetch, classified
    )
    table = (
        events.join(
            best.select(
                "event_id",
                pl.col("symbol").alias("symbol_used"),
                pl.col("kind").alias("symbol_kind"),
                "pair_reason",
                "entry_bar",
                "series_first",
                "series_last",
                "n_checked",
                "series_unverified",
            ),
            on="event_id",
            how="left",
        )
        .join(explained, on="event_id", how="left")
        .join(
            shares_asof(
                events,
                pl.read_parquet(
                    settings.data_dir / "processed" / "fundamentals" / "shares_outstanding.parquet"
                ),
            ),
            on="event_id",
            how="left",
        )
        .with_columns((pl.col("shares_outstanding") * pl.col("vwap")).alias("market_cap_proxy"))
    )
    series_failure = pl.when(~pl.col("pair_reason").is_in(["ok", "provider_missing"])).then(
        pl.col("pair_reason")
    )
    table = table.with_columns(
        market_cap_group(settings, pl.col("market_cap_proxy")),
        pl.when(pl.col("pair_reason") == "ok")
        .then(pl.lit("ok"))
        .otherwise(pl.coalesce(series_failure, pl.col("missing_reason")))
        .alias("price_status"),
        (
            (pl.col("pair_reason") == "ok")
            & (
                pl.col("series_last")
                >= pl.col("entry_session") + pl.duration(days=FULL_HORIZON_DAYS)
            )
        ).alias("full_horizon"),
    ).drop("pair_reason", "missing_reason")
    table.write_parquet(out_dir / "event_prices.parquet")
    verdicts.write_parquet(out_dir / "series_verdicts.parquet")
    return table


# --- reporting ---------------------------------------------------------------------------------


def _share(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    return (
        df.group_by(by)
        .agg(
            pl.len().alias("events"),
            (pl.col("price_status") == "ok").sum().alias("with_prices"),
            pl.col("full_horizon").sum().alias("full_horizon"),
        )
        .with_columns(
            (pl.col("with_prices") / pl.col("events")).round(4).alias("coverage"),
            (pl.col("full_horizon") / pl.col("events")).round(4).alias("full_horizon_coverage"),
        )
        .sort(by)
    )


def coverage_summary(table: pl.DataFrame) -> dict[str, Any]:
    t = table.with_columns(pl.col("filing_date").dt.year().alias("year"))
    cons = t.filter(pl.col("is_conservative_set"))
    ps = t.with_columns(
        pl.when(pl.col("trans_code") == "P")
        .then(pl.lit("purchase"))
        .otherwise(pl.lit("sale"))
        .alias("txn_type")
    )
    return {
        "events_total": t.height,
        "events_conservative": cons.height,
        "overall": _share(t.with_columns(pl.lit("all").alias("all")), ["all"]).to_dicts(),
        "overall_conservative": _share(
            cons.with_columns(pl.lit("all").alias("all")), ["all"]
        ).to_dicts(),
        "by_year": _share(t, ["year"]).to_dicts(),
        "by_year_conservative": _share(cons, ["year"]).to_dicts(),
        "by_mcap_group": _share(t, ["mcap_group"]).to_dicts(),
        "by_mcap_group_conservative": _share(cons, ["mcap_group"]).to_dicts(),
        "by_txn_type": _share(ps, ["txn_type"]).to_dicts(),
        "by_txn_type_conservative": _share(
            ps.filter(pl.col("is_conservative_set")), ["txn_type"]
        ).to_dicts(),
        "by_year_group_type_conservative": _share(
            ps.filter(pl.col("is_conservative_set")), ["year", "mcap_group", "txn_type"]
        ).to_dicts(),
        "reasons": t.group_by("price_status").len().sort("len", descending=True).to_dicts(),
        "reasons_conservative": cons.group_by("price_status")
        .len()
        .sort("len", descending=True)
        .to_dicts(),
        "unknown_mcap_share": float(t["mcap_group"].eq("unknown").mean()),
        "unverified_series_share": float(t["series_unverified"].fill_null(False).mean()),
    }


def _pct(x: float) -> str:
    return f"{100 * x:.1f}%"


def _table(rows: list[dict], key: str, columns: list[str], header: list[str]) -> list[str]:
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for r in rows:
        cells = [str(r[key])]
        cells.append(f"{r['events']:,}")
        cells.append(f"{r['with_prices']:,}")
        cells.append(_pct(r["coverage"]))
        if "full_horizon_coverage" in columns:
            cells.append(_pct(r["full_horizon_coverage"]))
        out.append("| " + " | ".join(cells) + " |")
    return out


REASON_TEXT = {
    "no_ticker": "the filing gave no usable symbol (placeholder or unparseable)",
    "delisted_or_acquired_likely": (
        "not on the SEC's current listings and stopped filing before 2022; "
        "the provider has no history"
    ),
    "ticker_changed_unmatched": (
        "listed today under a different ticker whose series did not match this event"
    ),
    "listed_but_provider_has_none": "listed today, yet yfinance returned nothing for the symbol",
    "went_dark_but_still_filing": (
        "not currently listed but filed in 2022 (OTC, foreign or deregistered)"
    ),
    "series_starts_after_event": (
        "the symbol's history starts after the event (a later company or listing reused it)"
    ),
    "series_ends_before_event": "the symbol's history ended before the event",
    "price_mismatch": (
        "the series disagrees with the prices in the insiders' own Form 4s "
        "(recycled ticker or bad split data)"
    ),
    "no_bar_near_entry": "the series has a gap of more than 7 days at the entry session",
    "fetch_error": "the download failed for the symbol and will be retried",
    "not_fetched": "the symbol has not been downloaded yet",
}


def render_markdown(summary: dict[str, Any], symbols: dict[str, int], window: str) -> str:
    o, oc = summary["overall"][0], summary["overall_conservative"][0]
    lines = [
        "# Free price coverage of insider events (yfinance, survivor-only)",
        "",
        f"Events: filings dated {window}. Generated by `uv run insider-whales coverage`.",
        "",
        "**Exploratory / survivor-only.** yfinance has no history for most delisted or acquired "
        "companies, so events of failed or acquired issuers are systematically missing. Any "
        "result computed on covered events overstates what a real portfolio would have earned.",
        "",
        f"- Events: {summary['events_total']:,} "
        f"(conservative set: {summary['events_conservative']:,}).",
        f"- With a usable price at entry: all events {_pct(o['coverage'])}; conservative set "
        f"{_pct(oc['coverage'])}; with a full 120-session horizon "
        f"{_pct(oc['full_horizon_coverage'])} (conservative).",
        f"- Symbols requested: {symbols.get('requested', 0):,}; with prices: "
        f"{symbols.get('ok', 0):,}; none at the provider: {symbols.get('missing', 0):,}; "
        f"errors: {symbols.get('error', 0):,}.",
        f"- Market-cap group unknown (no shares outstanding in SEC XBRL): "
        f"{_pct(summary['unknown_mcap_share'])} of events.",
        "",
        "## By filing year (conservative set)",
        "",
    ]
    cols = ["full_horizon_coverage"]
    head = ["Year", "Events", "With prices", "Coverage", "Full horizon"]
    lines += _table(summary["by_year_conservative"], "year", cols, head)
    lines += [
        "",
        "## By market-cap group (conservative set)",
        "",
        "Market cap = Form 4 price x cover-page shares outstanding known at the filing date.",
        "",
    ]
    order = ["micro", "small", "mid", "large", "mega", "unknown"]
    groups = {r["mcap_group"]: r for r in summary["by_mcap_group_conservative"]}
    lines += _table(
        [groups[g] for g in order if g in groups], "mcap_group", cols, ["Group", *head[1:]]
    )
    lines += ["", "## By transaction type (conservative set)", ""]
    lines += _table(
        summary["by_txn_type_conservative"],
        "txn_type",
        [],
        ["Type", "Events", "With prices", "Coverage"],
    )
    lines += ["", "## Why events have no price (all events)", ""]
    lines += ["| Reason | Events | Share |", "|---|---|---|"]
    total = summary["events_total"]
    for r in summary["reasons"]:
        lines.append(f"| {r['price_status'] or 'none'} | {r['len']:,} | {_pct(r['len'] / total)} |")
    lines += ["", "Reason definitions:", ""]
    lines += [f"- `{k}`: {v}." for k, v in REASON_TEXT.items()]
    lines.append("")
    return "\n".join(lines)


def write_reports(
    settings: Settings, table: pl.DataFrame, out_dir: Path, docs_dir: Path
) -> list[Path]:
    fetch = latest_status(status_manifest(settings))
    counts = {"requested": len(fetch)}
    for s in ("ok", "missing", "error"):
        counts[s] = sum(1 for r in fetch.values() if r["status"] == s)
    summary = coverage_summary(table)
    window = f"{table['filing_date'].min()} to {table['filing_date'].max()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    jpath = out_dir / "price_coverage_events.json"
    jpath.write_text(json.dumps({"symbols": counts, **summary}, indent=2, default=str))
    md = render_markdown(summary, counts, window)
    mpath = out_dir / "price_coverage_events.md"
    mpath.write_text(md)
    dpath = docs_dir / "price-coverage.md"
    dpath.write_text(md)
    return [jpath, mpath, dpath]
