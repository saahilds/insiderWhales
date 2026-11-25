"""Offline tests for the XML validation tool (synthetic XML and synthetic tables)."""

from datetime import date

import polars as pl

from factories import normalized, one_filing
from insider_whales.evaluation.filing_validation import (
    compare_filing,
    compare_review,
    pipeline_view,
    select_sample,
)
from insider_whales.ingestion.filing_xml import parse_ownership_xml

XML = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2019-02-27</periodOfReport>
  <issuer><issuerCik>0000000001</issuerCik><issuerTradingSymbol>EXMP</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0000000100</rptOwnerCik><rptOwnerName>Doe Jane</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>Chief Executive Officer</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2019-02-27</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>10.0012</value><footnoteId id="F1"/></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>5000</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <footnotes><footnote id="F1">Weighted average price.</footnote></footnotes>
</ownershipDocument>
"""


def _tables(**trans):
    return normalized(
        *one_filing(trans_pricepershare_fn="F1", **trans),
        footnotes=[
            {
                "accession_number": "A1",
                "footnote_id": "F1",
                "footnote_txt": "Weighted average price.",
            }
        ],
    )


def _compare(doc, tables):
    filing = tables["filings"].row(0, named=True)
    owners = tables["reporting_owners"]
    lines = tables["transactions"].filter(pl.col("table") == "nonderiv")
    return compare_filing(doc, filing, owners, lines)


def test_parse_ownership_xml():
    doc = parse_ownership_xml(XML)
    assert doc.issuer_cik == "0000000001" and doc.issuer_symbol == "EXMP"
    assert doc.owners[0].is_officer and not doc.owners[0].is_director
    line = doc.nonderiv[0]
    assert (line.trans_code, line.shares, line.price, line.footnote_ids) == (
        "P",
        1000.0,
        10.0012,
        ["F1"],
    )
    assert doc.footnotes == {"F1": "Weighted average price."}
    assert doc.aff10b5one is None


def test_compare_filing_matches_within_two_decimal_rounding():
    result = _compare(parse_ownership_xml(XML), _tables(trans_pricepershare=10.0))
    assert result["all_match"], result["issues"]


def test_compare_filing_reports_differences():
    result = _compare(parse_ownership_xml(XML), _tables(trans_shares=900.0, trans_code="S"))
    assert not result["all_match"]
    assert any("shares 1000.0 vs 900.0" in i and "code P vs S" in i for i in result["issues"])


def test_review_comparison():
    tables = _tables()
    view = pipeline_view("A1", tables)
    assert compare_review(view, {"txn_class": "open_market_confirmed", "events": 1}) == []
    issues = compare_review(view, {"txn_class": ["private"], "conservative": False})
    assert len(issues) == 2


def test_sample_is_deterministic_and_one_category_per_filing():
    subs, owners, trans = [], [], []
    for i in range(12):
        acc = f"A{i:02d}"
        subs.append({"accession_number": acc, "filing_date": date(2019, 3, 1)})
        owners.append({"accession_number": acc})
        trans.append(
            {
                "accession_number": acc,
                "trans_code": "P" if i % 2 else "S",
                "trans_acquired_disp_cd": "A" if i % 2 else "D",
            }
        )
    tx = normalized(subs, owners, trans)["transactions"]
    a, b = select_sample(tx, seed=7), select_sample(tx, seed=7)
    assert a.equals(b)
    assert a["accession_number"].n_unique() == a.height
