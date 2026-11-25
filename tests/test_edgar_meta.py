"""Acceptance timestamps, full-index reconciliation and data-quality report (offline)."""

import asyncio
import io
import json
import zipfile
from datetime import UTC, date, datetime

import httpx
import polars as pl

from factories import AVAIL, normalized, one_filing, staged
from insider_whales.config import load_settings
from insider_whales.evaluation.data_quality import (
    classification_summary,
    field_coverage,
    render_markdown,
    timestamp_quality,
)
from insider_whales.ingestion.edgar_meta import (
    acceptance_from_bulk,
    build_filing_timestamps,
    parse_form_index,
    parse_index_header_acceptance,
    reconcile_with_full_index,
)
from insider_whales.io.http import SecClient
from insider_whales.normalization.transactions import normalize_tables
from insider_whales.pit import BASIS_ACCEPTANCE, BASIS_FILING_DATE, NEW_YORK

FORM_IDX = """Description:           Master Index of EDGAR Dissemination Feed by Form Type
Last Data Received:    March 31, 2019

Form Type   Company Name                                                  CIK         Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
4           EXAMPLE CORP                                                  1           2019-03-01  edgar/data/1/A1.txt
4           DOE JANE                                                      100         2019-03-01  edgar/data/100/A1.txt
4/A         EXAMPLE CORP                                                  1           2019-03-05  edgar/data/1/A2.txt
SC 13G      OTHER CO                                                      7           2019-03-05  edgar/data/7/X9.txt
4           EXAMPLE CORP                                                  1           2019-03-06  edgar/data/1/A3.txt
"""


def _bulk_zip() -> bytes:
    buf = io.BytesIO()
    main = {
        "cik": "1",
        "filings": {
            "recent": {
                "accessionNumber": ["A1", "Z1"],
                "form": ["4", "10-K"],
                "acceptanceDateTime": ["2019-03-01T21:54:28.000Z", "2019-02-01T12:00:00.000Z"],
            },
            "files": [{"name": "CIK0000000001-submissions-001.json"}],
        },
    }
    page = {
        "accessionNumber": ["A0"],
        "form": ["4/A"],
        "acceptanceDateTime": ["2018-12-31T14:00:00.000Z"],
    }
    owner = {
        "filings": {
            "recent": {
                "accessionNumber": ["A2"],
                "form": ["4"],
                "acceptanceDateTime": ["2019-03-05T13:05:00.000Z"],
            }
        }
    }
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("CIK0000000001.json", json.dumps(main))
        zf.writestr("CIK0000000001-submissions-001.json", json.dumps(page))
        zf.writestr("CIK0000000100.json", json.dumps(owner))
        zf.writestr("CIK0000000999.json", json.dumps(owner))
    return buf.getvalue()


def test_parse_form_index_and_reconcile():
    idx = parse_form_index(FORM_IDX)
    assert idx.height == 5
    assert idx.filter(pl.col("form") == "SC 13G")["accession_number"][0] == "X9"
    r = reconcile_with_full_index(pl.Series(["A1", "A2", "B7"]), idx)
    assert r["full_index_accessions"] == 3  # A1 listed twice (issuer and owner) counts once
    assert (r["in_both"], r["only_in_full_index"], r["only_in_dataset"]) == (2, 1, 1)
    assert r["only_in_full_index_sample"] == ["A3"]


def test_acceptance_from_bulk_reads_paginated_files_and_skips_other_forms(tmp_path):
    path = tmp_path / "submissions.zip"
    path.write_bytes(_bulk_zip())
    df = acceptance_from_bulk(path, ["0000000001"]).sort("accession_number")
    assert df["accession_number"].to_list() == ["A0", "A1"]
    assert df["acceptance_ts"][1] == datetime(2019, 3, 1, 21, 54, 28, tzinfo=UTC)


def test_index_header_is_new_york_time():
    ts = parse_index_header_acceptance("<ACCEPTANCE-DATETIME>20261123165428\n")
    assert ts == datetime(2026, 11, 23, 16, 54, 28, tzinfo=NEW_YORK)
    assert parse_index_header_acceptance("nothing here") is None


def test_build_timestamps_uses_issuer_then_owner_files_then_headers(tmp_path):
    settings = load_settings(overrides={"paths": {"data_dir": str(tmp_path)}})
    bulk = _bulk_zip()

    def handler(request: httpx.Request):
        if request.url.path.endswith("submissions.zip"):
            return httpx.Response(200, content=bulk)
        if request.url.path.endswith("A3-index-headers.html"):
            return httpx.Response(200, text="<ACCEPTANCE-DATETIME>20190306080000")
        return httpx.Response(404)

    async def no_sleep(_):
        return None

    client = SecClient(
        settings.sec, "T (T) t@example.com", transport=httpx.MockTransport(handler), sleep=no_sleep
    )
    sub = pl.DataFrame({"accession_number": ["A1", "A2", "A3"], "issuercik": ["0000000001"] * 3})
    owners = pl.DataFrame(
        {"accession_number": ["A1", "A2", "A3"], "rptownercik": ["0000000100"] * 3}
    )
    ts = asyncio.run(build_filing_timestamps(settings, sub, owners, client=client))
    got = dict(zip(ts["accession_number"], ts["acceptance_source"], strict=True))
    assert got == {"A1": "submissions_bulk", "A2": "submissions_bulk_owner", "A3": "index_header"}
    assert ts.filter(pl.col("accession_number") == "A3")["acceptance_ts"][0] == datetime(
        2019, 3, 6, 13, 0, tzinfo=UTC
    )


def test_normalize_uses_acceptance_time_when_available():
    subs, owners, trans = one_filing()
    subs.append({"accession_number": "A2"})
    owners.append({"accession_number": "A2"})
    trans.append({"accession_number": "A2"})
    stamps = pl.DataFrame(
        {
            "accession_number": ["A1"],
            "acceptance_ts": [datetime(2019, 3, 1, 21, 0, tzinfo=UTC)],
            "acceptance_source": ["submissions_bulk"],
        },
        schema={
            "accession_number": pl.String,
            "acceptance_ts": pl.Datetime("us", "UTC"),
            "acceptance_source": pl.String,
        },
    )
    f = normalize_tables(staged(subs, owners, trans), AVAIL, stamps)["filings"].sort(
        "accession_number"
    )
    assert f["availability_basis"].to_list() == [BASIS_ACCEPTANCE, BASIS_FILING_DATE]
    assert f["available_at"][0] == datetime(2019, 3, 1, 21, 20, tzinfo=UTC)
    assert f["available_at"][1] == datetime(2019, 3, 1, 23, 59, 59, tzinfo=NEW_YORK)


def test_quality_sections_and_markdown_render():
    tables = normalized(*one_filing(sub={"filing_date": date(2019, 3, 1)}))
    filings = tables["filings"].with_columns(
        pl.lit(datetime(2019, 3, 1, 21, 0, tzinfo=UTC)).alias("acceptance_ts"),
        pl.lit("submissions_bulk").alias("acceptance_source"),
    )
    tq = timestamp_quality(filings)
    assert tq["missing_acceptance"] == 0 and tq["accepted_after_1600_et"] == 1
    assert tq["acceptance_date_after_filing_date"] == 0
    cov = field_coverage({**tables, "filings": filings})
    assert cov["Actual dissemination timestamp"]["missing_share"] == 1.0
    report = {
        "generated_at": "now",
        "quarters": ["2019q1"],
        "totals": {"filings": 1, "transaction_rows": 1, "insider_events": 1},
        "date_coverage": {"filing_date": ["2019-03-01", "2019-03-01"]},
        "timestamps": tq,
        "download_failures": [],
        "schema_changes": [],
        "full_index_reconciliation": {
            "2019q1": reconcile_with_full_index(
                filings["accession_number"],
                pl.DataFrame({"form": ["4"], "accession_number": ["A1"]}),
            )
        },
        "field_coverage": cov,
        "duplicates": {"x": 0},
        "date_parse_failures": {},
        "amendments": [],
        "classification": classification_summary(tables),
        "prices": {"status": "not computed"},
    }
    md = render_markdown(report)
    assert "| 2019q1 | 1 | 1 | 1 | 0 | 0 |" in md
    assert "Acceptance timestamps missing: 0" in md
