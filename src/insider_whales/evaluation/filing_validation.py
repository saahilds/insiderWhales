"""Validate parsed filings against the original SEC XML, plus recorded manual reviews."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from insider_whales.config import Settings, sec_user_agent
from insider_whales.ingestion.filing_xml import (
    OwnershipDocument,
    fetch_ownership_xml,
    parse_ownership_xml,
)
from insider_whales.io.http import SecClient

# (category, how many, filter on P/S non-derivative lines joined with filing columns)
SAMPLE_PLAN: list[tuple[str, int, pl.Expr]] = [
    (
        "purchase_open_market_confirmed",
        4,
        (pl.col("trans_code") == "P")
        & (pl.col("txn_class") == "open_market_confirmed")
        & pl.col("is_conservative_set"),
    ),
    (
        "purchase_open_market_probable",
        3,
        (pl.col("trans_code") == "P")
        & (pl.col("txn_class") == "open_market_probable")
        & pl.col("is_conservative_set"),
    ),
    (
        "purchase_private_or_offering",
        2,
        (pl.col("trans_code") == "P") & (pl.col("txn_class") == "private"),
    ),
    (
        "purchase_compensation_plan",
        2,
        (pl.col("trans_code") == "P") & (pl.col("txn_class") == "compensation"),
    ),
    (
        "sale_10b5_1_footnote",
        3,
        (pl.col("trans_code") == "S") & (pl.col("plan_evidence") == "footnote"),
    ),
    (
        "sale_plan_unknown",
        2,
        (pl.col("trans_code") == "S")
        & (pl.col("plan_status") == "unknown")
        & pl.col("is_conservative_set"),
    ),
    (
        "sale_tax_or_option_related",
        2,
        (pl.col("trans_code") == "S")
        & pl.col("txn_class").is_in(["compensation", "option_related"]),
    ),
    (
        "amendment_4a",
        3,
        (pl.col("document_type") == "4/A") & pl.col("amends_accession").is_not_null(),
    ),
    ("multi_owner_filing", 3, (pl.col("n_owners") > 1) & ~pl.col("is_amendment")),
    (
        "indirect_holding",
        2,
        (pl.col("direct_indirect") == "I")
        & ~pl.col("is_amendment")
        & (pl.col("trans_code") == "P"),
    ),
    (
        "ten_pct_owner_only",
        1,
        pl.col("exclusion_reasons").list.contains("ten_pct_or_other_only")
        & ~pl.col("is_amendment"),
    ),
    ("form_5", 1, pl.col("document_type") == "5"),
    (
        "checkbox_ticked_2023",
        2,
        (pl.col("plan_evidence") == "checkbox_filing_level") & (pl.col("trans_code") == "S"),
    ),
    (
        "checkbox_unticked_2023",
        1,
        (pl.col("plan_evidence") == "checkbox_unchecked") & (pl.col("trans_code") == "P"),
    ),
    (
        "checkbox_empty_2023",
        1,
        (pl.col("filing_date") >= date(2023, 4, 1))
        & (pl.col("plan_status") == "unknown")
        & (pl.col("trans_code") == "S"),
    ),
]
NUM_TOL = 0.006  # the bulk data sets store NUMBER(16,2)


def select_sample(transactions: pl.DataFrame, seed: int) -> pl.DataFrame:
    """Deterministic stratified sample of filings, one category per filing."""
    ps = transactions.filter(
        pl.col("trans_code").is_in(["P", "S"]) & (pl.col("table") == "nonderiv")
    )
    chosen: list[tuple[str, str, str]] = []
    taken: set[str] = set()
    for i, (category, n, cond) in enumerate(SAMPLE_PLAN):
        pool = (
            ps.filter(cond & ~pl.col("accession_number").is_in(list(taken)))
            .select("accession_number", "issuer_cik")
            .unique()
            .sort("accession_number")
        )
        if pool.height == 0:
            continue
        picked = pool.sample(min(n, pool.height), seed=seed + i)
        for acc, cik in picked.iter_rows():
            chosen.append((acc, cik, category))
            taken.add(acc)
    return pl.DataFrame(chosen, schema=["accession_number", "issuer_cik", "category"], orient="row")


def _close(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= max(NUM_TOL, 1e-6 * abs(b))


def compare_filing(
    doc: OwnershipDocument, filing: dict, owners: pl.DataFrame, lines: pl.DataFrame
) -> dict[str, Any]:
    """Field-by-field comparison of parsed bulk rows with the original XML."""
    checks: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append((name, ok, detail))

    check(
        "document_type",
        doc.document_type == filing["document_type"],
        f"{doc.document_type} vs {filing['document_type']}",
    )
    check("issuer_cik", doc.issuer_cik == filing["issuer_cik"])
    check(
        "ticker",
        (doc.issuer_symbol or "") == (filing["reported_ticker"] or ""),
        f"{doc.issuer_symbol} vs {filing['reported_ticker']}",
    )
    if doc.aff10b5one is not None or filing.get("plan_checkbox") is not None:
        check(
            "aff10b5one",
            doc.aff10b5one == filing.get("plan_checkbox"),
            f"{doc.aff10b5one} vs {filing.get('plan_checkbox')}",
        )
    xml_owners = {o.cik: o for o in doc.owners}
    ours = {r["owner_cik"]: r for r in owners.iter_rows(named=True)}
    check("owner_set", set(xml_owners) == set(ours), f"{sorted(xml_owners)} vs {sorted(ours)}")
    for cik, o in xml_owners.items():
        r = ours.get(cik)
        if r is None:
            continue
        flags_ok = (o.is_director, o.is_officer, o.is_ten_pct) == (
            r["is_director"],
            r["is_officer"],
            r["is_ten_pct"],
        )
        check(f"owner_flags[{cik}]", flags_ok)
    nd = lines.sort("line_sk")
    check(
        "nonderiv_line_count", len(doc.nonderiv) == nd.height, f"{len(doc.nonderiv)} vs {nd.height}"
    )
    for i, (x, r) in enumerate(zip(doc.nonderiv, nd.iter_rows(named=True), strict=False)):
        rdate = r["trans_date"].isoformat() if r["trans_date"] else None
        diffs = []
        if (x.security_title or "") != (r["security_title"] or ""):
            diffs.append(f"title {x.security_title!r} vs {r['security_title']!r}")
        if (x.trans_date or "")[:10] != (rdate or ""):
            diffs.append(f"date {x.trans_date} vs {rdate}")
        if x.trans_code != r["trans_code"]:
            diffs.append(f"code {x.trans_code} vs {r['trans_code']}")
        for name, xv, rv in (
            ("shares", x.shares, r["shares"]),
            ("price", x.price, r["price"]),
            ("shares_after", x.shares_after, r["shares_after"]),
        ):
            if not _close(xv, rv):
                diffs.append(f"{name} {xv} vs {rv}")
        if x.acq_disp != r["acq_disp"]:
            diffs.append(f"A/D {x.acq_disp} vs {r['acq_disp']}")
        if x.direct_indirect != r["direct_indirect"]:
            diffs.append(f"D/I {x.direct_indirect} vs {r['direct_indirect']}")
        xml_fn = set(x.footnote_ids)
        our_fn = set(r["txn_footnote_ids"] or []) | set(r["holding_footnote_ids"] or [])
        if not xml_fn <= our_fn | {""}:
            diffs.append(f"footnotes {sorted(xml_fn)} vs {sorted(our_fn)}")
        check(f"line[{i}]", not diffs, "; ".join(diffs))
    passed = sum(ok for _, ok, _ in checks)
    return {
        "checks": len(checks),
        "passed": passed,
        "all_match": passed == len(checks),
        "issues": [f"{n}: {d}" for n, ok, d in checks if not ok],
    }


def pipeline_view(accession: str, tables: dict[str, pl.DataFrame]) -> dict[str, Any]:
    tx = (
        tables["transactions"]
        .filter((pl.col("accession_number") == accession) & (pl.col("table") == "nonderiv"))
        .sort("line_sk")
    )
    ev = tables["insider_events"].filter(pl.col("accession_number") == accession)
    return {
        "lines": [
            {
                "sk": r["line_sk"],
                "code": r["trans_code"],
                "txn_class": r["txn_class"],
                "plan_status": r["plan_status"],
                "conservative": r["is_conservative_set"],
                "sensitivity": r["is_sensitivity_set"],
                "exclusions": list(r["exclusion_reasons"] or []),
            }
            for r in tx.iter_rows(named=True)
        ],
        "events": ev.height,
        "insider_groups": ev["insider_group_id"].n_unique() if ev.height else 0,
        "pct_holdings_change": [
            None if v is None else round(v, 4) for v in ev.sort("event_id")["pct_holdings_change"]
        ],
    }


def compare_review(view: dict[str, Any], review: dict[str, Any]) -> list[str]:
    """Differences between the pipeline's classification and the recorded manual review."""
    issues = []
    ps = [ln for ln in view["lines"] if ln["code"] in ("P", "S")]
    for key in ("txn_class", "plan_status", "conservative"):
        if key in review:
            got = sorted({ln[key] for ln in ps}, key=str)
            want = review[key] if isinstance(review[key], list) else [review[key]]
            if got != sorted(want, key=str):
                issues.append(f"{key}: pipeline {got} vs review {want}")
    if "events" in review and review["events"] != view["events"]:
        issues.append(f"events: pipeline {view['events']} vs review {review['events']}")
    if "insider_groups" in review and review["insider_groups"] != view["insider_groups"]:
        issues.append(
            f"insider_groups: pipeline {view['insider_groups']} vs review "
            f"{review['insider_groups']}"
        )
    return issues


async def validate(
    settings: Settings,
    sample: pl.DataFrame,
    tables: dict[str, pl.DataFrame],
    reviews: dict[str, dict],
) -> list[dict[str, Any]]:
    filings = tables["filings"]
    owners = tables["reporting_owners"]
    results = []
    async with SecClient(settings.sec, sec_user_agent(), seed=settings.seed) as client:
        for acc, cik, category in sample.iter_rows():
            path, url = await fetch_ownership_xml(settings, client, cik, acc)
            doc = parse_ownership_xml(path.read_bytes())
            filing = filings.filter(pl.col("accession_number") == acc).row(0, named=True)
            lines = tables["transactions"].filter(
                (pl.col("accession_number") == acc) & (pl.col("table") == "nonderiv")
            )
            cmp = compare_filing(
                doc, filing, owners.filter(pl.col("accession_number") == acc), lines
            )
            view = pipeline_view(acc, tables)
            review = reviews.get(acc)
            results.append(
                {
                    "accession_number": acc,
                    "issuer_cik": cik,
                    "issuer": filing["issuername"],
                    "category": category,
                    "document_type": filing["document_type"],
                    "xml_url": url,
                    "parse": cmp,
                    "pipeline": view,
                    "footnotes": doc.footnotes,
                    "remarks": doc.remarks,
                    "owners": [asdict(o) for o in doc.owners],
                    "review": review,
                    "review_issues": compare_review(view, review) if review else None,
                }
            )
    return results


def render_validation_markdown(results: list[dict[str, Any]]) -> str:
    n = len(results)
    parse_ok = sum(r["parse"]["all_match"] for r in results)
    checks = sum(r["parse"]["checks"] for r in results)
    passed = sum(r["parse"]["passed"] for r in results)
    reviewed = [r for r in results if r["review"] is not None]
    agree = sum(not r["review_issues"] for r in reviewed)
    lines = [
        "# Milestone 1: filing validation against original SEC records",
        "",
        "Generated by `uv run insider-whales validate-filings`. Each sampled filing's original "
        "XML was downloaded from EDGAR and compared field by field with the rows parsed from the "
        "quarterly data set. Classification was then reviewed by reading each filing's footnotes "
        "and remarks; the review verdicts live in `docs/validation/m1-reviews.yaml`.",
        "",
        "## Summary",
        "",
        f"- Filings validated: {n} (categories below).",
        f"- Parsed fields matching the XML: {passed} of {checks} checks; {parse_ok} of {n} filings "
        "match on every check.",
        f"- Manual classification review: {agree} of {len(reviewed)} reviewed filings agree with "
        "the pipeline.",
        "",
        "## Results",
        "",
        "| # | Filing | Issuer | Category | Parse checks | Pipeline (P/S lines) | Review |",
        "|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(results, 1):
        ps = [ln for ln in r["pipeline"]["lines"] if ln["code"] in ("P", "S")]
        cls = ", ".join(
            sorted({f"{ln['code']}:{ln['txn_class']}/{ln['plan_status']}" for ln in ps})
        )
        cons = sum(ln["conservative"] for ln in ps)
        review = (
            "agrees"
            if r["review"] and not r["review_issues"]
            else "; ".join(r["review_issues"])
            if r["review"]
            else "not reviewed"
        )
        parse = f"{r['parse']['passed']}/{r['parse']['checks']}"
        if r["parse"]["issues"]:
            parse += " (" + "; ".join(r["parse"]["issues"])[:160] + ")"
        lines.append(
            f"| {i} | [{r['accession_number']}]({r['xml_url']}) {r['document_type']} | "
            f"{r['issuer']} | {r['category']} | {parse} | {cls or '-'}; "
            f"conservative {cons}/{len(ps)}; events {r['pipeline']['events']} | {review} |"
        )
    notes = [r for r in results if r["review"] and r["review"].get("note")]
    if notes:
        lines += ["", "## Reviewer notes", ""]
        for r in notes:
            lines.append(f"- **{r['accession_number']}** ({r['issuer']}): {r['review']['note']}")
    lines.append("")
    return "\n".join(lines)


def load_reviews(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text()) or {}
    return {str(k): v for k, v in data.get("reviews", {}).items()}


def run_validation(
    settings: Settings,
    tables: dict[str, pl.DataFrame],
    sample_path: Path,
    reviews_path: Path,
    out_md: Path,
    resample: bool = False,
) -> dict[str, Any]:
    if resample or not sample_path.exists():
        sample = select_sample(
            tables["transactions"].join(
                tables["filings"].select("accession_number"), on="accession_number", how="semi"
            ),
            settings.seed,
        )
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        sample.write_csv(sample_path)
    sample = pl.read_csv(sample_path, schema_overrides={"issuer_cik": pl.String})
    results = asyncio.run(validate(settings, sample, tables, load_reviews(reviews_path)))
    out_md.write_text(render_validation_markdown(results))
    return {"results": results}
