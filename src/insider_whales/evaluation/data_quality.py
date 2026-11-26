"""Ingestion and data-quality report (`reports/data_quality.json` plus a Markdown summary)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from insider_whales.config import Settings, sec_user_agent
from insider_whales.ingestion.edgar_meta import fetch_form_index, reconcile_with_full_index
from insider_whales.ingestion.sec import SchemaRegistry, SecDataSets, schema_changes
from insider_whales.io.http import SecClient
from insider_whales.io.manifest import Manifest
from insider_whales.normalization.pipeline import (
    load_processed,
    load_staged,
    processed_dir,
    scan_processed,
    staging_dir,
)

# Spec fields to preserve per transaction → (table, column) in the processed data.
PRESERVED_FIELDS = {
    "SEC accession number": ("transactions", "accession_number"),
    "Issuer CIK": ("transactions", "issuer_cik"),
    "Issuer name": ("filings", "issuername"),
    "Historical ticker (as reported)": ("transactions", "reported_ticker"),
    "Reporting owner CIK": ("transactions", "primary_owner_cik"),
    "Reporting owner name": ("transactions", "primary_owner_name"),
    "Officer title": ("transactions", "primary_title"),
    "Director / officer / 10% flags": ("transactions", "role_norm"),
    "Transaction date": ("transactions", "trans_date"),
    "Filing date": ("transactions", "filing_date"),
    "EDGAR acceptance timestamp": ("filings", "acceptance_ts"),
    "Security title": ("transactions", "security_title"),
    "Transaction code": ("transactions", "trans_code"),
    "Acquired/disposed": ("transactions", "acq_disp"),
    "Shares transacted": ("transactions", "shares"),
    "Transaction price": ("transactions", "price"),
    "Transaction value": ("transactions", "value"),
    "Shares owned after": ("transactions", "shares_after"),
    "Direct/indirect ownership": ("transactions", "direct_indirect"),
    "Rule 10b5-1 checkbox": ("filings", "plan_checkbox"),
    "Amendment status": ("transactions", "is_amendment"),
    "Source URL": ("filings", "source_url"),
}
PLACEHOLDER_TICKERS = ["NONE", "N/A", "NA", "", "-", "NOT APPLICABLE"]


def _null_share(df: pl.DataFrame, col: str) -> float:
    if df.height == 0:
        return 0.0
    return round(df[col].null_count() / df.height, 4)


def _counts(df: pl.DataFrame, *cols: str) -> list[dict[str, Any]]:
    return df.group_by(list(cols)).len().sort(list(cols)).to_dicts()


def field_coverage(
    tables: dict[str, pl.DataFrame], footnote_share: float | None = None
) -> dict[str, Any]:
    tx = tables["transactions"].filter(
        pl.col("trans_code").is_in(["P", "S"]) & (pl.col("table") == "nonderiv")
    )
    filings = tables["filings"]
    out: dict[str, Any] = {}
    for label, (table, col) in PRESERVED_FIELDS.items():
        df = tx if table == "transactions" else filings
        out[label] = {"column": f"{table}.{col}", "missing_share": _null_share(df, col)}
    out["Historical ticker (as reported)"]["placeholder_share"] = round(
        tx.select(
            pl.col("reported_ticker")
            .fill_null("")
            .str.to_uppercase()
            .is_in(PLACEHOLDER_TICKERS)
            .mean()
        ).item()
        or 0.0,
        4,
    )
    out["Actual dissemination timestamp"] = {
        "column": None,
        "missing_share": 1.0,
        "note": "not available in free data; acceptance time plus a buffer is used",
    }
    out["Relevant footnotes"] = {
        "column": "transactions.txn_footnote_text",
        "share_with_any_footnote": round(
            footnote_share
            if footnote_share is not None
            else tx.select((pl.col("txn_footnote_text") != "").mean()).item() or 0.0,
            4,
        ),
    }
    out["Rule 10b5-1 checkbox"]["missing_share_from_2023_04"] = _null_share(
        filings.filter(pl.col("filing_date") >= date(2023, 4, 1)), "plan_checkbox"
    )
    return out


def date_parse_failures(staged: dict[str, pl.DataFrame]) -> dict[str, int]:
    out = {}
    for table, df in staged.items():
        for c in df.columns:
            if c.endswith("_raw"):
                base = c.removesuffix("_raw")
                n = df.filter(
                    pl.col(c).is_not_null() & (pl.col(c) != "") & pl.col(base).is_null()
                ).height
                if n:
                    out[f"{table}.{base}"] = n
    return out


def duplicates(staged: dict[str, pl.DataFrame], filings: pl.DataFrame) -> dict[str, int]:
    nd = staged["nonderiv_trans"]
    content_cols = [c for c in nd.columns if c not in ("nonderiv_trans_sk", "source_sha256")]
    return {
        "duplicate_accessions_across_quarters": filings.height
        - filings["accession_number"].n_unique(),
        "duplicate_nonderiv_keys": nd.height
        - nd.select("accession_number", "nonderiv_trans_sk").n_unique(),
        "identical_nonderiv_lines_within_filing": nd.height - nd.select(content_cols).n_unique(),
        "duplicate_owner_rows": staged["reportingowner"].height
        - staged["reportingowner"].select("accession_number", "rptownercik").n_unique(),
    }


def timestamp_quality(filings: pl.DataFrame) -> dict[str, Any]:
    acc = filings.filter(pl.col("acceptance_ts").is_not_null()).with_columns(
        pl.col("acceptance_ts").dt.convert_time_zone("America/New_York").alias("_ny")
    )
    ny_date = pl.col("_ny").dt.date()
    minutes = pl.col("_ny").dt.hour().cast(pl.Int32) * 60 + pl.col("_ny").dt.minute().cast(pl.Int32)
    return {
        "filings": filings.height,
        "missing_acceptance": filings["acceptance_ts"].null_count(),
        "missing_acceptance_share": _null_share(filings, "acceptance_ts"),
        "by_source": _counts(filings, "acceptance_source"),
        "availability_basis": _counts(filings, "availability_basis"),
        "acceptance_date_after_filing_date": acc.filter(ny_date > pl.col("filing_date")).height,
        "acceptance_date_before_filing_date": acc.filter(ny_date < pl.col("filing_date")).height,
        "accepted_before_0910_et": acc.filter(minutes < 9 * 60 + 10).height,
        "accepted_0910_to_1600_et": acc.filter(
            (minutes >= 9 * 60 + 10) & (minutes < 16 * 60)
        ).height,
        "accepted_after_1600_et": acc.filter(minutes >= 16 * 60).height,
    }


def classification_summary(tables: dict[str, pl.DataFrame]) -> dict[str, Any]:
    tx = tables["transactions"]
    ps = tx.filter(pl.col("trans_code").is_in(["P", "S"]) & (pl.col("table") == "nonderiv"))
    ev = tables["insider_events"]
    lag = ps.filter(~pl.col("is_amendment"))["filing_lag_days"].drop_nulls()
    return {
        "txn_class_ps": _counts(ps, "trans_code", "txn_class"),
        "plan_status_ps": _counts(ps, "trans_code", "plan_status"),
        "research_sets": tx.group_by("trans_code")
        .agg(
            pl.col("is_conservative_set").sum().alias("conservative"),
            pl.col("is_sensitivity_set").sum().alias("sensitivity"),
        )
        .filter(pl.col("sensitivity") > 0)
        .sort("trans_code")
        .to_dicts(),
        "events": ev.group_by("trans_code")
        .agg(
            pl.len().alias("events"),
            pl.col("is_conservative_set").sum().alias("conservative"),
            pl.col("ownership_reliable").mean().round(4).alias("ownership_reliable_share"),
            pl.col("is_new_position").sum().alias("new_positions"),
        )
        .sort("trans_code")
        .to_dicts(),
        "exclusion_reasons": tx.select(pl.col("exclusion_reasons").explode())
        .drop_nulls()
        .group_by("exclusion_reasons")
        .len()
        .sort("len", descending=True)
        .to_dicts(),
        "filing_lag_days_ps": {
            "median": float(lag.median()) if lag.len() else None,
            "p90": float(lag.quantile(0.9)) if lag.len() else None,
            "share_over_2_days": round(float((lag > 2).mean()), 4) if lag.len() else None,
        },
    }


def price_coverage(settings: Settings) -> dict[str, Any]:
    path = settings.data_dir / "processed" / "prices" / "coverage.parquet"
    if not path.exists():
        return {"status": "not computed: run download-prices first"}
    cov = pl.read_parquet(path)
    return {
        "securities": cov.height,
        "by_status": _counts(cov, "status"),
        "survivor_only": True,
    }


HEAVY_TRANSACTION_COLUMNS = (
    "txn_footnote_text",
    "holding_footnote_text",
    "txn_footnote_ids",
    "holding_footnote_ids",
    "nature_of_ownership",
)


def staged_quality(
    settings: Settings, quarters: list[str]
) -> tuple[dict[str, int], dict[str, int]]:
    """Date-parse failures and duplicate counts, accumulated one quarter at a time."""
    parse: dict[str, int] = {}
    dups: dict[str, int] = {}
    for q in quarters:
        staged = {
            t: load_staged(staging_dir(settings), t, [q])
            for t in ("submission", "reportingowner", "nonderiv_trans")
        }
        for k, v in date_parse_failures(staged).items():
            parse[k] = parse.get(k, 0) + v
        d = duplicates(staged, staged["submission"])
        for k, v in d.items():
            dups[k] = dups.get(k, 0) + v
    return parse, dups


def audit(settings: Settings, quarters: list[str], index_frames: dict[str, pl.DataFrame]):
    parse_failures, dup_counts = staged_quality(settings, quarters)
    all_cols = pl.read_parquet_schema(processed_dir(settings) / "transactions.parquet").keys()
    tables = {
        "transactions": load_processed(
            settings,
            "transactions",
            [c for c in all_cols if c not in HEAVY_TRANSACTION_COLUMNS],
        ),
        **{n: load_processed(settings, n) for n in ("filings", "insider_events", "insider_groups")},
    }
    footnote_share = (
        scan_processed(settings, "transactions")
        .filter(pl.col("trans_code").is_in(["P", "S"]) & (pl.col("table") == "nonderiv"))
        .select((pl.col("txn_footnote_text") != "").mean())
        .collect()
        .item()
    )
    filings = tables["filings"]
    tx = tables["transactions"]
    dup_counts["duplicate_accessions_across_quarters"] = (
        filings.height - filings["accession_number"].n_unique()
    )
    ds = SecDataSets(settings, client=None)  # type: ignore[arg-type]
    downloads = ds.manifest.records()
    registry = SchemaRegistry(settings.data_dir / "manifests" / "sec_schemas.json").load()
    recon = {
        q: reconcile_with_full_index(
            filings.filter(pl.col("source_quarter") == q)["accession_number"], idx
        )
        for q, idx in index_frames.items()
    }
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "quarters": quarters,
        "data_versions": {q: (ds.current(q) or {}).get("sha256") for q in quarters},
        "totals": {
            "filings": filings.height,
            "filings_by_type": _counts(filings, "document_type"),
            "transaction_rows": tx.height,
            "transaction_rows_by_table": _counts(tx, "table"),
            "insider_events": tables["insider_events"].height,
            "insider_groups": tables["insider_groups"]["insider_group_id"].n_unique(),
        },
        "date_coverage": {
            "filing_date": [str(filings["filing_date"].min()), str(filings["filing_date"].max())],
            "transaction_date": [str(tx["trans_date"].min()), str(tx["trans_date"].max())],
            "transaction_dates_after_filing_date": tx.filter(pl.col("filing_lag_days") < 0).height,
        },
        "field_coverage": field_coverage(tables, footnote_share),
        "date_parse_failures": parse_failures,
        "duplicates": dup_counts,
        "timestamps": timestamp_quality(filings),
        "amendments": _counts(
            filings.filter(pl.col("is_amendment")), "document_type", "amend_link_confidence"
        ),
        "download_failures": [r for r in downloads if r.get("status") == "failed"],
        "schema_changes": schema_changes({q: v for q, v in registry.items() if q in quarters}),
        "full_index_reconciliation": recon,
        "classification": classification_summary(tables),
        "prices": price_coverage(settings),
    }


def render_markdown(report: dict[str, Any]) -> str:
    t, ts, cl = report["totals"], report["timestamps"], report["classification"]
    lines = [
        "# Data quality report",
        "",
        f"Generated {report['generated_at']} for quarters {', '.join(report['quarters'])}.",
        "",
        "## Headline numbers",
        "",
        f"- Filings: {t['filings']:,}; transaction lines: {t['transaction_rows']:,}; "
        f"insider events: {t['insider_events']:,}.",
        f"- Filing dates {report['date_coverage']['filing_date'][0]} to "
        f"{report['date_coverage']['filing_date'][1]}.",
        f"- Acceptance timestamps missing: {ts['missing_acceptance']:,} "
        f"({ts['missing_acceptance_share']:.2%}).",
        f"- Accepted before 09:10 ET: {ts['accepted_before_0910_et']:,}; during the session: "
        f"{ts['accepted_0910_to_1600_et']:,}; after 16:00 ET: {ts['accepted_after_1600_et']:,}.",
        f"- Download failures: {len(report['download_failures'])}; "
        f"schema changes between loaded quarters: {len(report['schema_changes'])}.",
        "",
        "## Full-index reconciliation",
        "",
        "| Quarter | Data set | Full-index | Both | Only full-index | Only data set |",
        "|---|---|---|---|---|---|",
    ]
    for q, r in report["full_index_reconciliation"].items():
        lines.append(
            f"| {q} | {r['dataset_accessions']:,} | {r['full_index_accessions']:,} | "
            f"{r['in_both']:,} | {r['only_in_full_index']:,} | {r['only_in_dataset']:,} |"
        )
    lines += [
        "",
        "## Field coverage (P/S non-derivative lines)",
        "",
        "| Field | Column | Missing |",
        "|---|---|---|",
    ]
    for label, v in report["field_coverage"].items():
        miss = v.get("missing_share")
        cell = "-" if miss is None else f"{miss:.2%}"
        if "share_with_any_footnote" in v:
            cell = f"{v['share_with_any_footnote']:.2%} of lines have one"
        if "placeholder_share" in v:
            cell += f" ({v['placeholder_share']:.2%} placeholders such as NONE)"
        if "missing_share_from_2023_04" in v:
            cell += f" ({v['missing_share_from_2023_04']:.2%} from Apr 2023)"
        lines.append(f"| {label} | {v.get('column') or '-'} | {cell} |")
    lines += ["", "## Duplicates", ""]
    lines += [f"- {k}: {v:,}" for k, v in report["duplicates"].items()]
    lines += ["", "## Date parse failures", ""]
    lines += [f"- {k}: {v:,}" for k, v in report["date_parse_failures"].items()] or ["- none"]
    lines += ["", "## Amendments", ""]
    lines += [
        f"- {r['document_type']} {r['amend_link_confidence']}: {r['len']:,}"
        for r in report["amendments"]
    ]
    lines += ["", "## Research sets", ""]
    lines += [
        f"- {r['trans_code']}: conservative {r['conservative']:,}, "
        f"sensitivity {r['sensitivity']:,} lines"
        for r in cl["research_sets"]
    ]
    lines += [
        f"- {r['trans_code']} events: {r['events']:,} (conservative {r['conservative']:,}, "
        f"ownership reliable {r['ownership_reliable_share']:.1%})"
        for r in cl["events"]
    ]
    lag = cl["filing_lag_days_ps"]
    lines += [
        "",
        f"Filing lag (P/S lines): median {lag['median']} days, 90th percentile "
        f"{lag['p90']} days, {lag['share_over_2_days']:.1%} filed more than 2 days after "
        "the trade.",
        "",
        "## Prices",
        "",
        f"{json.dumps(report['prices'])}",
        "",
    ]
    return "\n".join(lines)


def run_audit(settings: Settings, quarters: list[str] | None, out_dir: Path) -> list[Path]:
    last = Manifest(settings.data_dir / "manifests" / "pipeline_steps.jsonl").latest(
        "key", "normalize"
    )
    if last is None:
        raise RuntimeError("run `normalize` before `audit-data`")
    if quarters and sorted(quarters) != sorted(last["quarters"]):
        raise RuntimeError(f"processed tables cover {last['quarters']}; re-run normalize first")
    qs = last["quarters"]

    async def fetch():
        async with SecClient(settings.sec, sec_user_agent(), seed=settings.seed) as client:
            return {q: await fetch_form_index(settings, client, q) for q in qs}

    report = audit(settings, qs, asyncio.run(fetch()))
    out_dir.mkdir(parents=True, exist_ok=True)
    jpath, mpath = out_dir / "data_quality.json", out_dir / "data_quality.md"
    jpath.write_text(json.dumps(report, indent=2, default=str))
    mpath.write_text(render_markdown(report))
    return [jpath, mpath]
