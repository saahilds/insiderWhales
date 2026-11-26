"""Value-level drift across SEC quarters.

`ingestion.sec.schema_changes` reports columns that appear or vanish. This module profiles what
is *inside* the columns of each staged quarter (null rates, date-parse failures, category sets)
and flags quarters whose profile jumps relative to history, because format changes inside an
unchanged column are the kind of drift that silently corrupts features.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from insider_whales.config import Settings
from insider_whales.ingestion.sec import parse_quarter

CATEGORICAL = {
    "submission": ["document_type"],
    "nonderiv_trans": [
        "trans_code",
        "trans_acquired_disp_cd",
        "direct_indirect_ownership",
        "trans_timeliness",
        "trans_form_type",
    ],
    "deriv_trans": ["trans_code", "trans_acquired_disp_cd"],
}
NULL_RATE_COLUMNS = {
    "submission": ["issuertradingsymbol", "issuercik", "period_of_report", "remarks", "aff10b5one"],
    "reportingowner": ["rptownercik", "rptowner_relationship", "rptowner_title"],
    "nonderiv_trans": [
        "trans_date",
        "trans_code",
        "trans_shares",
        "trans_pricepershare",
        "trans_acquired_disp_cd",
        "shrs_ownd_folwng_trans",
        "security_title",
        "direct_indirect_ownership",
    ],
}
DATE_RAW = {
    "submission": ["filing_date", "period_of_report", "date_of_orig_sub"],
    "nonderiv_trans": ["trans_date", "deemed_execution_date"],
}
NULL_JUMP = 0.05  # absolute change in null rate vs the trailing median that gets flagged
PARSE_FAIL_LIMIT = 0.001


def _profile_table(df: pl.DataFrame, table: str) -> dict[str, Any]:
    n = df.height
    prof: dict[str, Any] = {"rows": n, "null_rate": {}, "date_parse_failure": {}, "values": {}}
    for c in NULL_RATE_COLUMNS.get(table, []):
        if c in df.columns and n:
            empty = df[c].is_null() | (df[c].cast(pl.String).str.strip_chars() == "")
            prof["null_rate"][c] = float(empty.mean())
    for c in DATE_RAW.get(table, []):
        raw = f"{c}_raw"
        if raw in df.columns and n:
            present = df[raw].is_not_null() & (df[raw].str.strip_chars() != "")
            failed = present & df[c].is_null()
            prof["date_parse_failure"][c] = float(failed.sum() / max(int(present.sum()), 1))
    for c in CATEGORICAL.get(table, []):
        if c in df.columns:
            vc = df.group_by(c).len().sort("len", descending=True)
            prof["values"][c] = {str(k): int(v) for k, v in vc.iter_rows()}
    return prof


def profile_quarter(staging_dir: Path, quarter: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for table in ("submission", "reportingowner", "nonderiv_trans", "deriv_trans"):
        f = staging_dir / table / f"{quarter}.parquet"
        if f.exists():
            out[table] = _profile_table(pl.read_parquet(f), table)
    return out


def profile_quarters(staging_dir: Path, quarters: list[str]) -> dict[str, dict[str, Any]]:
    return {q: profile_quarter(staging_dir, q) for q in sorted(quarters, key=parse_quarter)}


def _median(xs: list[float]) -> float:
    s = sorted(xs)
    return s[len(s) // 2] if s else 0.0


def find_anomalies(profiles: dict[str, dict[str, Any]], history: int = 4) -> list[dict[str, Any]]:
    """Quarters whose profile departs from the preceding quarters.

    Flags: a null rate that moves more than 5 percentage points from the same calendar quarter
    in earlier years (insider filings are strongly seasonal: Q1 carries annual Form 5 and
    director grants), any date column where more than 0.1% of present values fail to parse, and
    a non-null category value never seen in earlier quarters.
    """
    out: list[dict[str, Any]] = []
    quarters = sorted(profiles, key=parse_quarter)
    seen: dict[tuple[str, str], set[str]] = {}
    prior_nulls: dict[tuple[str, str, int], list[float]] = {}
    for i, q in enumerate(quarters):
        for table, prof in profiles[q].items():
            for col, rate in prof["null_rate"].items():
                past = prior_nulls.setdefault((table, col, parse_quarter(q)[1]), [])
                if past and abs(rate - _median(past[-history:])) > NULL_JUMP:
                    out.append(
                        {
                            "quarter": q,
                            "table": table,
                            "column": col,
                            "kind": "null_rate_jump",
                            "value": round(rate, 4),
                            "baseline": round(_median(past[-history:]), 4),
                        }
                    )
                past.append(rate)
            for col, rate in prof["date_parse_failure"].items():
                if rate > PARSE_FAIL_LIMIT:
                    out.append(
                        {
                            "quarter": q,
                            "table": table,
                            "column": col,
                            "kind": "date_parse_failure",
                            "value": round(rate, 5),
                            "baseline": 0.0,
                        }
                    )
            for col, counts in prof["values"].items():
                key = (table, col)
                current = set(counts) - {"None"}
                if i > 0 and key in seen:
                    for v in sorted(current - seen[key]):
                        out.append(
                            {
                                "quarter": q,
                                "table": table,
                                "column": col,
                                "kind": "new_category",
                                "value": v,
                                "count": counts[v],
                            }
                        )
                seen.setdefault(key, set()).update(current)
    return out


def render_drift_markdown(
    profiles: dict[str, dict[str, Any]],
    anomalies: list[dict[str, Any]],
    column_changes: list[dict[str, Any]],
) -> str:
    quarters = sorted(profiles, key=parse_quarter)
    lines = [
        "# SEC data sets: drift report",
        "",
        f"Generated by `uv run insider-whales ingest-sec`. Quarters: {quarters[0]} to "
        f"{quarters[-1]} ({len(quarters)}). Do not edit by hand.",
        "",
        "## Column changes between consecutive quarters",
        "",
    ]
    if column_changes:
        for ch in column_changes:
            lines.append(
                f"- {ch['table']} {ch['from_quarter']} → {ch['to_quarter']}: "
                f"added {ch['added'] or 'none'}, removed {ch['removed'] or 'none'}"
            )
    else:
        lines.append("None.")
    lines += ["", "## Value-level anomalies", ""]
    if not anomalies:
        lines.append("None flagged.")
    else:
        lines += [
            "| Quarter | Table | Column | Kind | Value | Baseline / count |",
            "|---|---|---|---|---|---|",
        ]
        for a in anomalies:
            extra = a.get("baseline", a.get("count", ""))
            lines.append(
                f"| {a['quarter']} | {a['table']} | `{a['column']}` | {a['kind']} | "
                f"{a['value']} | {extra} |"
            )
    lines += [
        "",
        "## Rows per quarter",
        "",
        "| Quarter | Filings | Non-derivative lines |",
        "|---|---|---|",
    ]
    for q in quarters:
        p = profiles[q]
        lines.append(
            f"| {q} | {p.get('submission', {}).get('rows', '')} | "
            f"{p.get('nonderiv_trans', {}).get('rows', '')} |"
        )
    lines.append("")
    return "\n".join(lines)


def run_drift(settings: Settings, quarters: list[str], staging_dir: Path) -> dict[str, Any]:
    from insider_whales.ingestion.sec import SchemaRegistry, schema_changes

    profiles = profile_quarters(staging_dir, quarters)
    anomalies = find_anomalies(profiles)
    registry = SchemaRegistry(settings.data_dir / "manifests" / "sec_schemas.json").load()
    changes = schema_changes({q: registry[q] for q in quarters if q in registry})
    out = settings.reports_dir / "sec_drift.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps({"anomalies": anomalies, "profiles": profiles}, indent=2, sort_keys=True)
    )
    return {"profiles": profiles, "anomalies": anomalies, "column_changes": changes}
