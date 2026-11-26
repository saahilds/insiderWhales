"""Chunked normalization, resumable runs and value-level drift detection (synthetic data only)."""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from factories import AVAIL, staged
from insider_whales.config import load_settings
from insider_whales.evaluation.schema_drift import find_anomalies
from insider_whales.normalization import pipeline
from insider_whales.normalization.transactions import (
    build_global_tables,
    build_transaction_tables,
    normalize_tables,
)

Q1, Q2 = "2019q1", "2019q2"


def _two_quarters() -> dict[str, pl.DataFrame]:
    """Two quarters; a 4/A filed in Q2 amends an original filed in Q1."""
    subs = [
        {"accession_number": "A1", "source_quarter": Q1, "filing_date": date(2019, 3, 1)},
        {
            "accession_number": "A2",
            "source_quarter": Q1,
            "issuercik": "0000000002",
            "issuertradingsymbol": "OTHR",
            "filing_date": date(2019, 3, 4),
        },
        {
            "accession_number": "A3",
            "source_quarter": Q2,
            "document_type": "4/A",
            "date_of_orig_sub": date(2019, 3, 1),
            "filing_date": date(2019, 4, 2),
        },
        {
            "accession_number": "A4",
            "source_quarter": Q2,
            "filing_date": date(2019, 5, 6),
            "period_of_report": date(2019, 5, 3),
        },
    ]
    owners = [
        {"accession_number": "A1"},
        {"accession_number": "A2", "rptownercik": "0000000200"},
        {"accession_number": "A3"},
        {"accession_number": "A4"},
    ]
    trans = [
        {"accession_number": "A1", "trans_pricepershare": 10.0},
        {"accession_number": "A2", "trans_code": "S", "trans_acquired_disp_cd": "D"},
        {"accession_number": "A3", "trans_pricepershare": 10.5},
        {"accession_number": "A4", "trans_date": date(2019, 5, 3), "trans_shares": 300.0},
    ]
    return staged(subs, owners, trans)


def _by_quarter(tables: dict[str, pl.DataFrame], q: str, g: dict) -> dict[str, pl.DataFrame]:
    accs = g["filings"].filter(pl.col("source_quarter") == q).select("accession_number")
    return {
        name: df.join(accs, on="accession_number", how="semi")
        for name, df in tables.items()
        if "accession_number" in df.columns and name not in ("submission", "reportingowner")
    }


def test_per_quarter_build_equals_whole_build():
    tables = _two_quarters()
    whole = normalize_tables(tables, AVAIL)
    g = build_global_tables(tables["submission"], tables["reportingowner"], AVAIL)
    parts = []
    events = []
    for q in (Q1, Q2):
        quarter = _by_quarter(tables, q, g)
        built = build_transaction_tables(
            quarter,
            {**g, "filings": g["filings"].filter(pl.col("source_quarter") == q)},
        )
        parts.append(built["transactions"])
        events.append(built["insider_events"])
    assert pl.concat(parts).sort("txn_id").equals(whole["transactions"].sort("txn_id"))
    assert pl.concat(events).sort("event_id").equals(whole["insider_events"].sort("event_id"))


def test_amendment_in_later_quarter_links_to_original_in_earlier_quarter():
    tables = _two_quarters()
    g = build_global_tables(tables["submission"], tables["reportingowner"], AVAIL)
    a3 = g["filings"].filter(pl.col("accession_number") == "A3").row(0, named=True)
    assert a3["amends_accession"] == "A1"


@pytest.fixture
def workspace(tmp_path):
    """A settings object rooted in tmp_path, with two quarters already staged."""
    settings = load_settings(overrides={"paths": {"data_dir": str(tmp_path / "data")}})
    tables = _two_quarters()
    sha = {Q1: "sha-q1", Q2: "sha-q2"}
    manifest = pipeline.Manifest(settings.data_dir / "manifests" / "sec_form345_downloads.jsonl")
    steps = pipeline.Manifest(settings.data_dir / "manifests" / "pipeline_steps.jsonl")
    for q in (Q1, Q2):
        archive = tmp_path / f"{q}.zip"
        archive.write_bytes(b"x")
        manifest.append(
            {"quarter": q, "status": "downloaded", "path": str(archive), "sha256": sha[q]}
        )
        steps.append(
            {
                "key": f"stage:{q}",
                "input_sha256": sha[q],
                "version": pipeline.STAGING_VERSION,
            }
        )
        for name, df in tables.items():
            part = (
                df.filter(pl.col("source_quarter") == q) if "source_quarter" in df.columns else None
            )
            if part is None:
                accs = tables["submission"].filter(pl.col("source_quarter") == q)
                part = df.join(accs.select("accession_number"), on="accession_number", how="semi")
            dest = pipeline.staging_dir(settings) / name / f"{q}.parquet"
            dest.parent.mkdir(parents=True, exist_ok=True)
            part.write_parquet(dest)
    return settings


def _fresh_outputs(settings):
    return {
        n: pl.read_parquet(pipeline.processed_dir(settings) / f"{n}.parquet").sort(
            "txn_id" if n == "transactions" else "event_id"
        )
        for n in ("transactions", "insider_events")
    }


def test_interrupted_normalize_resumes_and_matches_uninterrupted_run(workspace):
    settings = workspace
    with pytest.raises(KeyboardInterrupt):
        pipeline.normalize(settings, [Q1, Q2], stop_after=1)
    parts = pipeline.processed_dir(settings) / "_parts" / "transactions"
    assert [p.stem for p in parts.glob("*.parquet")] == [Q1]

    pipeline.normalize(settings, [Q1, Q2])
    resumed = _fresh_outputs(settings)
    built = [
        r
        for r in pipeline.Manifest(
            settings.data_dir / "manifests" / "pipeline_steps.jsonl"
        ).records()
        if r["key"].startswith("normalize_part:")
    ]
    assert [r["key"] for r in built] == [f"normalize_part:{Q1}", f"normalize_part:{Q2}"]

    expected = normalize_tables(_two_quarters(), AVAIL)
    assert resumed["transactions"].equals(expected["transactions"].sort("txn_id"))
    assert resumed["insider_events"].equals(expected["insider_events"].sort("event_id"))


def test_completed_quarters_are_not_rebuilt(workspace):
    pipeline.normalize(workspace, [Q1, Q2])
    steps = pipeline.Manifest(workspace.data_dir / "manifests" / "pipeline_steps.jsonl")
    before = len([r for r in steps.records() if r["key"].startswith("normalize_part:")])
    pipeline.normalize(workspace, [Q1, Q2], stop_after=0)
    after = len([r for r in steps.records() if r["key"].startswith("normalize_part:")])
    assert before == after == 2


def test_changed_timestamps_invalidate_parts(workspace):
    pipeline.normalize(workspace, [Q1, Q2])
    ts = pl.DataFrame(
        {
            "accession_number": ["A1"],
            "acceptance_ts": [None],
            "acceptance_source": ["submissions_bulk"],
        },
        schema={
            "accession_number": pl.String,
            "acceptance_ts": pl.Datetime("us", "UTC"),
            "acceptance_source": pl.String,
        },
    )
    ts.write_parquet(pipeline.processed_dir(workspace) / "filing_timestamps.parquet")
    with pytest.raises(KeyboardInterrupt):
        pipeline.normalize(workspace, [Q1, Q2], stop_after=0)


# --- drift -------------------------------------------------------------------------------------


def _profile(null_title: float, codes: dict[str, int], parse_fail: float = 0.0) -> dict:
    return {
        "reportingowner": {
            "rows": 10,
            "null_rate": {"rptowner_title": null_title},
            "date_parse_failure": {},
            "values": {},
        },
        "nonderiv_trans": {
            "rows": 10,
            "null_rate": {},
            "date_parse_failure": {"trans_date": parse_fail},
            "values": {"trans_code": codes},
        },
    }


def test_seasonal_null_rate_is_not_flagged_but_a_real_jump_is():
    p = {
        "2012q1": _profile(0.40, {"P": 1}),
        "2012q2": _profile(0.56, {"P": 1}),
        "2013q1": _profile(0.41, {"P": 1}),
        "2013q2": _profile(0.57, {"P": 1}),
        "2014q1": _profile(0.41, {"P": 1}),
        "2014q2": _profile(0.95, {"P": 1}),
    }
    flagged = find_anomalies(p)
    assert [(a["quarter"], a["kind"]) for a in flagged] == [("2014q2", "null_rate_jump")]


def test_new_category_and_parse_failures_are_flagged_and_null_category_is_not():
    p = {
        "2012q1": _profile(0.4, {"P": 5, "S": 5}),
        "2012q2": _profile(0.4, {"P": 5, "S": 5, "None": 1, "H": 2}, parse_fail=0.01),
    }
    kinds = {(a["kind"], a["value"]) for a in find_anomalies(p)}
    assert kinds == {("new_category", "H"), ("date_parse_failure", 0.01)}
    json.dumps(find_anomalies(p))
