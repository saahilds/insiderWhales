"""Self-contained HTML research report: data quality, price coverage and the baseline event study.

The report is built only from the artefacts earlier pipeline steps wrote (JSON and CSV under
`reports/`), contains no wall-clock time and uses fixed plot ids, so the same inputs always give
the same bytes (Test 17).
"""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import polars as pl

from insider_whales.config import Settings

BANNER = (
    "Exploratory and survivor-only. Prices come from a free provider that only covers companies "
    "still trading; delisted, acquired and bankrupt issuers are missing. The event study uses "
    "the 2015-2020 development period only (2012-2014 is warm-up); the 2023+ final-test period "
    "is locked and no 2021+ returns are used. Nothing here is evidence of a tradable edge."
)

STYLE = """
body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;max-width:1100px;margin:2em auto;
padding:0 1em;color:#1b1f23;line-height:1.45}
h1{font-size:1.6em}h2{margin-top:2em;border-bottom:1px solid #d0d7de;padding-bottom:.2em}
h3{margin-top:1.4em}
.banner{background:#fff8c5;border:1px solid #d4a72c;padding:.8em 1em;border-radius:6px}
table{border-collapse:collapse;margin:.6em 0;font-size:.9em}
th,td{border:1px solid #d0d7de;padding:.25em .6em;text-align:right}
th:first-child,td:first-child{text-align:left}th{background:#f6f8fa}
.note{color:#57606a;font-size:.9em}
"""


def _pct(x: Any, digits: int = 1) -> str:
    if x is None or x != x:
        return "-"
    return f"{100 * x:.{digits}f}%"


def _num(x: Any) -> str:
    return "-" if x is None else f"{x:,}"


def _table(header: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(h)}</th>" for h in header)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in row) + "</tr>" for row in rows
    )
    return f"<table><tr>{head}</tr>{body}</table>"


def _fig(fig: go.Figure, div_id: str, first: list[bool]) -> str:
    fig.update_layout(
        template="plotly_white", height=380, margin={"l": 50, "r": 20, "t": 40, "b": 40}
    )
    include = first[0]
    first[0] = False
    return fig.to_html(full_html=False, include_plotlyjs=include, div_id=div_id)


def _read_json(path: Path) -> dict[str, Any] | None:
    return json.loads(path.read_text()) if path.exists() else None


def _read_csv(path: Path) -> pl.DataFrame | None:
    return pl.read_csv(path) if path.exists() else None


def _missing(name: str, command: str) -> str:
    return f"<p class='note'>{html.escape(name)} not available; run <code>{command}</code>.</p>"


# --- sections ------------------------------------------------------------------------------


def data_quality_section(dq: dict[str, Any] | None) -> str:
    if dq is None:
        return _missing("Data-quality report", "insider-whales audit-data")
    t, ts = dq["totals"], dq["timestamps"]
    quarters = dq.get("quarters", [])
    rows = [
        [
            "Quarters ingested",
            f"{len(quarters)} ({quarters[0]} to {quarters[-1]})" if quarters else "-",
        ],
        ["Filings (Forms 3, 4, 5 and amendments)", _num(t["filings"])],
        ["Transaction rows", _num(t["transaction_rows"])],
        ["Insider events (open-market candidates and others)", _num(t["insider_events"])],
        ["Insider groups", _num(t["insider_groups"])],
        ["Filings missing an acceptance timestamp", f"{_num(ts['missing_acceptance'])}"],
        ["Accepted after the 16:00 ET close", _pct(ts["accepted_after_1600_et"] / ts["filings"])],
        [
            "Duplicate accessions across quarters",
            _num(dq["duplicates"]["duplicate_accessions_across_quarters"]),
        ],
        [
            "Transaction dates after the filing date",
            _num(dq["date_coverage"]["transaction_dates_after_filing_date"]),
        ],
    ]
    recon = dq.get("full_index_reconciliation", {})
    if recon:
        total = sum(v["full_index_accessions"] for v in recon.values())
        both = sum(v["in_both"] for v in recon.values())
        rows.append(
            ["Accessions matched with EDGAR's full index", f"{_num(both)} of {_num(total)}"]
        )
    out = [
        "<h2>1. Data quality</h2>",
        _table(["Check", "Result"], rows),
    ]
    classes = dq.get("classification", {}).get("txn_class_ps", [])
    if classes:
        by: dict[str, dict[str, int]] = {}
        for r in classes:
            by.setdefault(r["txn_class"], {})[r["trans_code"]] = r["len"]
        out += [
            "<h3>Purchase and sale classification</h3>",
            _table(
                ["Class", "Purchases (P)", "Sales (S)"],
                [[k, _num(v.get("P", 0)), _num(v.get("S", 0))] for k, v in sorted(by.items())],
            ),
        ]
    return "\n".join(out)


def coverage_section(cov: dict[str, Any] | None, first: list[bool]) -> str:
    if cov is None:
        return _missing("Price-coverage report", "insider-whales coverage")
    sym = cov["symbols"]
    o = cov["overall_conservative"][0]
    out = [
        "<h2>2. Price coverage (free data, survivor-only)</h2>",
        f"<p>{_num(sym['ok'])} of {_num(sym['requested'])} requested symbols have price history; "
        f"{_pct(o['coverage'])} of the {_num(o['events'])} conservative-set events have a usable "
        f"price series ({_pct(o['full_horizon_coverage'])} with a full 120-session horizon).</p>",
    ]
    by_year = cov["by_year_conservative"]
    fig = go.Figure(
        go.Bar(x=[r["year"] for r in by_year], y=[r["coverage"] for r in by_year], name="coverage")
    )
    fig.update_layout(title="Share of events with prices, by filing year", yaxis_tickformat=".0%")
    out.append(_fig(fig, "coverage-year", first))
    groups = ["micro", "small", "mid", "large", "mega", "unknown"]
    by_cap = {r["mcap_group"]: r for r in cov["by_mcap_group_conservative"]}
    fig = go.Figure(
        go.Bar(
            x=[g for g in groups if g in by_cap],
            y=[by_cap[g]["coverage"] for g in groups if g in by_cap],
        )
    )
    fig.update_layout(
        title="Share of events with prices, by market-cap group", yaxis_tickformat=".0%"
    )
    out.append(_fig(fig, "coverage-cap", first))
    out.append(
        _table(
            ["Year", "Events", "With prices", "Coverage"],
            [
                [r["year"], _num(r["events"]), _num(r["with_prices"]), _pct(r["coverage"])]
                for r in by_year
            ],
        )
    )
    out.append(
        _table(
            ["Market cap", "Events", "With prices", "Coverage"],
            [
                [
                    g,
                    _num(by_cap[g]["events"]),
                    _num(by_cap[g]["with_prices"]),
                    _pct(by_cap[g]["coverage"]),
                ]
                for g in groups
                if g in by_cap
            ],
        )
    )
    reasons = cov["reasons_conservative"]
    total = sum(r["len"] for r in reasons)
    out.append("<h3>Why events have no price</h3>")
    out.append(
        _table(
            ["Status", "Events", "Share"],
            [[r["price_status"], _num(r["len"]), _pct(r["len"] / total)] for r in reasons],
        )
    )
    return "\n".join(out)


def _headline_rows(df: pl.DataFrame, direction: str) -> list[list[str]]:
    rows = []
    for r in df.filter(pl.col("direction") == direction).sort("horizon").iter_rows(named=True):
        ci = f"[{_pct(r['exc_ci_low'], 2)}, {_pct(r['exc_ci_high'], 2)}]"
        rows.append(
            [
                r["horizon"],
                _num(r["n"]),
                _pct(r["mean"], 2),
                _pct(r["median"], 2),
                _pct(r["mkt_mean"], 2),
                _pct(r["exc_mean"], 2),
                ci,
            ]
        )
    return rows


HEADLINE_HEAD = [
    "Horizon",
    "Events",
    "Mean raw",
    "Median raw",
    "Minus SPY",
    "Minus controls",
    "95% CI (controls)",
]


def event_study_section(
    headline: pl.DataFrame | None,
    tradable: pl.DataFrame | None,
    paths: pl.DataFrame | None,
    subgroups: pl.DataFrame | None,
    first: list[bool],
) -> str:
    if headline is None:
        return _missing("Event-study results", "insider-whales event-study")
    out = [
        "<h2>3. Baseline event study, 2015-2020 development period</h2>",
        "<p class='note'>Entry at the first executable open after the filing became public; "
        "exit at the close h sessions later on the total-return index. Controls are the 3 "
        "nearest same-sector stocks without recent insider signals. Confidence intervals are "
        "cluster-robust by issuer and month.</p>",
    ]
    for direction, title in (("purchase", "Purchases"), ("sale", "Sales")):
        out.append(f"<h3>{title}</h3>")
        out.append(_table(HEADLINE_HEAD, _headline_rows(headline, direction)))
        if tradable is not None:
            out.append(
                "<p class='note'>Tradable events only (price at least $5, "
                "20-day dollar volume at least $2M):</p>"
            )
            out.append(_table(HEADLINE_HEAD, _headline_rows(tradable, direction)))
        if paths is not None:
            sub = paths.filter(pl.col("direction") == direction).sort("day")
            fig = go.Figure()
            for col, name in (
                ("signal", "Insider signal"),
                ("controls", "Matched controls"),
                ("spy", "SPY"),
            ):
                fig.add_trace(
                    go.Scatter(
                        x=sub["day"].to_list(),
                        y=sub[col].to_list(),
                        name=name,
                        mode="lines+markers",
                    )
                )
            n = int(sub["events"].max()) if sub.height else 0
            fig.update_layout(
                title=(
                    f"{title}: mean cumulative return by session since entry "
                    f"({n:,} events with complete paths)"
                ),
                xaxis_title="Sessions since entry",
                yaxis_tickformat=".0%",
            )
            out.append(_fig(fig, f"paths-{direction}", first))
    if subgroups is not None:
        out.append("<h3>Subgroups at 60 sessions</h3>")
        out.append(
            "<p class='note'>Mean return minus matched controls with 95% intervals. Descriptive; "
            "no multiple-testing correction.</p>"
        )
        for direction, title in (("purchase", "Purchases"), ("sale", "Sales")):
            sub = subgroups.filter((pl.col("direction") == direction) & (pl.col("horizon") == 60))
            rows = [
                [
                    f"{r['grouping']}: {r['value']}",
                    _num(r["n"]),
                    _pct(r["mean"], 2),
                    _pct(r.get("exc_mean"), 2),
                    f"[{_pct(r.get('exc_ci_low'), 2)}, {_pct(r.get('exc_ci_high'), 2)}]",
                ]
                for r in sub.sort("grouping", "value").iter_rows(named=True)
            ]
            out.append(f"<h3>{title}</h3>")
            out.append(_table(["Group", "Events", "Mean raw", "Minus controls", "95% CI"], rows))
    return "\n".join(out)


LIMITATIONS = [
    "Survivor-only prices: delisted, acquired and bankrupt issuers are missing, and delisting "
    "returns are unavailable. This flatters long-horizon results.",
    "Events cluster in the same issuers and months; the effective sample is much smaller than the "
    "event count.",
    "Sector comes from the current SIC code and is coarse; market capitalisation is a proxy.",
    "No transaction costs, spreads or capacity limits are applied in the event study.",
    "Portfolio backtests and robustness checks are not part of this report yet.",
]


def render_report(
    settings: Settings,
    dq: dict[str, Any] | None,
    cov: dict[str, Any] | None,
    headline: pl.DataFrame | None,
    tradable: pl.DataFrame | None,
    paths: pl.DataFrame | None,
    subgroups: pl.DataFrame | None,
) -> str:
    first = [True]
    dev = settings.splits.development
    body = [
        "<h1>Insider whales: research report</h1>",
        f"<div class='banner'>{html.escape(BANNER)}</div>",
        f"<p class='note'>Development period {dev[0]} to {dev[1]}; seed {settings.seed}; "
        f"config hash {settings.config_hash()[:12]}. The run manifest "
        "(<code>run_manifest.json</code>) records versions, inputs and timings.</p>",
        data_quality_section(dq),
        coverage_section(cov, first),
        event_study_section(headline, tradable, paths, subgroups, first),
        "<h2>4. Limitations</h2><ul>"
        + "".join(f"<li>{html.escape(x)}</li>" for x in LIMITATIONS)
        + "</ul>",
    ]
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Insider whales research report</title>"
        f"<style>{STYLE}</style></head><body>{''.join(body)}</body></html>"
    )


def build_report(settings: Settings, out_path: Path | None = None) -> Path:
    reports = settings.reports_dir
    study = reports / "event_study"
    html_text = render_report(
        settings,
        _read_json(reports / "data_quality.json"),
        _read_json(reports / "price_coverage" / "price_coverage_events.json"),
        _read_csv(study / "headline.csv"),
        _read_csv(study / "headline_tradable.csv"),
        _read_csv(study / "paths.csv"),
        _read_csv(study / "subgroups.csv"),
    )
    path = out_path or reports / "report.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html_text, encoding="utf-8")
    return path
