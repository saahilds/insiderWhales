"""Normalization tests, including mandatory tests T5–T8 (synthetic data only)."""

import io
import zipfile
from datetime import UTC, date, datetime

import polars as pl
import pytest

from factories import AVAIL, normalized, one_filing, staged
from insider_whales.normalization.insiders import insider_groups
from insider_whales.normalization.securities import security_class_expr, share_class_expr
from insider_whales.normalization.staging import read_raw_table, type_table
from insider_whales.normalization.transactions import normalize_tables, transactions_as_of


def ps_lines(tables):
    return tables["transactions"].sort("txn_id")


# --- staging --------------------------------------------------------------------------------


def test_staging_types_dates_numbers_and_flags(tmp_path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "SUBMISSION.tsv",
            "ACCESSION_NUMBER\tFILING_DATE\tAFF10B5ONE\tREMARKS\n"
            'a\t27-MAR-2019\t1\tsee "quoted" text\n'
            "b\t31-FEB-2019\tfalse\t\n"
            "c\t02-jan-2023\t\t\n"
            "d\t02-JAN-2023\ttrue\t\n"
            "e\t02-JAN-2023\t0\t\n",
        )
    path = tmp_path / "q.zip"
    path.write_bytes(buf.getvalue())
    df = type_table(read_raw_table(path, "SUBMISSION"))
    assert df["filing_date"].to_list() == [
        date(2019, 3, 27),
        None,
        date(2023, 1, 2),
        date(2023, 1, 2),
        date(2023, 1, 2),
    ]
    assert df["filing_date_raw"][1] == "31-FEB-2019"
    assert df["aff10b5one"].to_list() == [True, False, None, True, False]
    assert df["remarks"][0] == 'see "quoted" text'


def test_security_and_share_class_parsing():
    titles = [
        "Class A Common Stock",
        "Common Stock, par value $0.01",
        "Series B Preferred",
        "Warrants to purchase Common Stock",
        "Restricted Stock Units",
        "American Depositary Shares",
        "Ordinary Shares",
        "BRINX RESOURCES LTD.",
    ]
    out = pl.DataFrame({"security_title": titles}).select(security_class_expr(), share_class_expr())
    assert out["security_class"].to_list() == [
        "common",
        "common",
        "preferred",
        "warrant",
        "derivative_like",
        "adr",
        "common",
        "unknown",
    ]
    assert out["share_class"].to_list() == ["A", None, None, None, None, None, None, None]


def test_roles_from_relationship_title_and_remarks():
    subs = [
        {"accession_number": "A1"},
        {"accession_number": "A2", "remarks": "Chief Financial Officer"},
        {"accession_number": "A3"},
    ]
    owners = [
        {"accession_number": "A1", "rptowner_title": "President & CEO"},
        {"accession_number": "A2", "rptowner_title": "See Remarks", "rptownercik": "0000000200"},
        {
            "accession_number": "A3",
            "rptowner_relationship": "TenPercentOwner",
            "rptowner_title": None,
            "rptownername": "Big Fund LP",
            "rptownercik": "0000000300",
        },
    ]
    trans = [{"accession_number": a} for a in ("A1", "A2", "A3")]
    t = normalized(subs, owners, trans)
    roles = dict(
        zip(
            t["reporting_owners"]["accession_number"],
            t["reporting_owners"]["role_norm"],
            strict=True,
        )
    )
    assert roles == {"A1": "CEO", "A2": "CFO", "A3": "TenPct"}
    entity = t["reporting_owners"].filter(pl.col("accession_number") == "A3")["is_entity_guess"][0]
    assert entity is True
    lines = ps_lines(t)
    assert lines.filter(pl.col("accession_number") == "A3")["exclusion_reasons"][0].to_list() == [
        "ten_pct_or_other_only"
    ]


# --- T5: repeated purchases by one insider are not independent insiders ---------------------


def test_t5_related_entities_and_repeat_buys_count_as_one_insider():
    # Person 100 buys three times; once jointly with her trust (CIK 101), then via the trust alone.
    subs = [
        {"accession_number": a, "filing_date": date(2019, 3, d)}
        for a, d in (("A1", 2), ("A2", 5), ("A3", 7), ("A4", 9))
    ]
    owners = [
        {"accession_number": "A1", "rptownercik": "0000000100"},
        {"accession_number": "A2", "rptownercik": "0000000100"},
        {
            "accession_number": "A2",
            "rptownercik": "0000000101",
            "rptownername": "Doe Family Trust",
            "rptowner_relationship": "Other",
            "rptowner_title": None,
        },
        {
            "accession_number": "A3",
            "rptownercik": "0000000101",
            "rptownername": "Doe Family Trust",
            "rptowner_relationship": "Other",
            "rptowner_title": None,
        },
        {"accession_number": "A4", "rptownercik": "0000000100"},
    ]
    trans = [
        {"accession_number": a, "trans_date": date(2019, 3, d - 1)}
        for a, d in (("A1", 2), ("A2", 5), ("A3", 7), ("A4", 9))
    ]
    t = normalized(subs, owners, trans)
    events = t["insider_events"]
    assert events.height == 4
    assert events["insider_group_id"].n_unique() == 1
    # A multi-owner filing produces one event, not one per reporting owner.
    assert events.filter(pl.col("accession_number") == "A2").height == 1

    # Point in time: before the joint filing is public, the trust is a separate owner.
    fo = t["reporting_owners"].join(
        t["filings"].select("accession_number", "issuer_cik", "available_at"), on="accession_number"
    )
    before = insider_groups(fo, as_of=datetime(2019, 3, 4, tzinfo=UTC))
    assert before.height == 1
    early = insider_groups(fo.filter(pl.col("accession_number") != "A2"))
    assert early["insider_group_id"].n_unique() == 2
    after = insider_groups(fo, as_of=datetime(2019, 3, 11, tzinfo=UTC))
    assert after["insider_group_id"].n_unique() == 1


def test_two_different_insiders_stay_separate():
    subs = [{"accession_number": "A1"}, {"accession_number": "A2"}]
    owners = [
        {"accession_number": "A1", "rptownercik": "0000000100"},
        {"accession_number": "A2", "rptownercik": "0000000200", "rptownername": "Roe Rick"},
    ]
    trans = [{"accession_number": "A1"}, {"accession_number": "A2"}]
    assert normalized(subs, owners, trans)["insider_events"]["insider_group_id"].n_unique() == 2


# --- T6: amendments cannot change previously available information --------------------------


def test_t6_amendment_only_visible_after_it_is_public():
    subs = [
        {"accession_number": "A1", "filing_date": date(2019, 3, 1)},
        {
            "accession_number": "A1-AMD",
            "document_type": "4/A",
            "filing_date": date(2019, 3, 20),
            "date_of_orig_sub": date(2019, 3, 1),
        },
    ]
    owners = [{"accession_number": "A1"}, {"accession_number": "A1-AMD"}]
    trans = [
        {"accession_number": "A1", "trans_pricepershare": 10.0},
        {"accession_number": "A1-AMD", "trans_pricepershare": 12.0},
    ]
    t = normalized(subs, owners, trans)
    f = t["filings"].filter(pl.col("accession_number") == "A1-AMD")
    assert f["amends_accession"][0] == "A1" and f["amend_link_confidence"][0] == "high"

    tx = t["transactions"]
    mid = datetime(2019, 3, 10, tzinfo=UTC)
    late = datetime(2019, 4, 1, tzinfo=UTC)
    assert transactions_as_of(tx, mid, "latest")["price"].to_list() == [10.0]
    assert transactions_as_of(tx, late, "latest")["price"].to_list() == [12.0]
    assert transactions_as_of(tx, late, "original")["price"].to_list() == [10.0]
    assert transactions_as_of(tx, datetime(2019, 2, 1, tzinfo=UTC), "latest").height == 0
    # Amendment lines are never part of the research sets built from original filings.
    amd = tx.filter(pl.col("accession_number") == "A1-AMD")
    assert not amd["is_conservative_set"][0] and "amendment_version" in amd["exclusion_reasons"][0]


def test_amendment_with_wrong_original_date_matched_by_period():
    subs = [
        {
            "accession_number": "A1",
            "filing_date": date(2019, 1, 2),
            "period_of_report": date(2018, 12, 31),
        },
        {
            "accession_number": "A1-AMD",
            "document_type": "4/A",
            "filing_date": date(2019, 1, 20),
            "date_of_orig_sub": date(2019, 1, 16),
            "period_of_report": date(2018, 12, 31),
        },
    ]
    t = normalized(
        subs,
        [{"accession_number": "A1"}, {"accession_number": "A1-AMD"}],
        [{"accession_number": "A1"}, {"accession_number": "A1-AMD"}],
    )
    f = t["filings"].filter(pl.col("accession_number") == "A1-AMD")
    assert f["amends_accession"][0] == "A1" and f["amend_link_confidence"][0] == "medium"


# --- T7: option exercises and tax-related dispositions are excluded --------------------------


@pytest.mark.parametrize("code", ["M", "F", "A", "G", "D", "X", "C", "J"])
def test_t7_non_market_codes_never_in_research_sets(code):
    t = normalized(*one_filing(trans_code=code))
    line = ps_lines(t).row(0, named=True)
    assert not line["is_conservative_set"] and not line["is_sensitivity_set"]
    assert t["insider_events"].height == 0


def test_t7_derivative_lines_and_footnoted_s_p_lines_excluded():
    subs = [{"accession_number": a} for a in ("D1", "S1", "P1", "P2")]
    owners = [{"accession_number": a} for a in ("D1", "S1", "P1", "P2")]
    trans = [
        {
            "accession_number": "S1",
            "trans_code": "S",
            "trans_acquired_disp_cd": "D",
            "trans_shares_fn": "F1",
        },
        {"accession_number": "P1", "trans_pricepershare_fn": "F1"},
        {"accession_number": "P2", "trans_pricepershare_fn": "F1"},
    ]
    notes = [
        {
            "accession_number": "S1",
            "footnote_id": "F1",
            "footnote_txt": "Shares sold to cover tax withholding obligations upon vesting of RSUs.",
        },
        {
            "accession_number": "P1",
            "footnote_id": "F1",
            "footnote_txt": "Shares acquired under the Employee Stock Purchase Plan.",
        },
        {
            "accession_number": "P2",
            "footnote_id": "F1",
            "footnote_txt": "Purchased in a private placement pursuant to a securities purchase agreement.",
        },
    ]
    t = normalized(subs, owners, trans, notes, deriv=[{"accession_number": "D1"}])
    lines = {r["accession_number"]: r for r in ps_lines(t).iter_rows(named=True)}
    assert lines["D1"]["txn_class"] == "option_related" and not lines["D1"]["is_sensitivity_set"]
    assert lines["S1"]["txn_class"] == "compensation" and not lines["S1"]["is_sensitivity_set"]
    assert lines["P1"]["txn_class"] == "compensation" and not lines["P1"]["is_sensitivity_set"]
    assert lines["P2"]["txn_class"] == "private"
    assert not lines["P2"]["is_conservative_set"] and lines["P2"]["is_sensitivity_set"]


def test_weighted_average_footnote_confirms_open_market_but_holding_footnotes_do_not_taint():
    t = normalized(
        *one_filing(trans_pricepershare_fn="F1", shrs_ownd_folwng_trans_fn="F2"),
        footnotes=[
            {
                "accession_number": "A1",
                "footnote_id": "F1",
                "footnote_txt": "Weighted average price; purchased in multiple transactions from $9.90 to $10.10.",
            },
            {
                "accession_number": "A1",
                "footnote_id": "F2",
                "footnote_txt": "Includes 2,000 restricted stock units awarded under the incentive plan.",
            },
        ],
    )
    line = ps_lines(t).row(0, named=True)
    assert line["txn_class"] == "open_market_confirmed"
    assert line["is_conservative_set"]


# --- T8: missing trading-plan information is not classified as discretionary ----------------


@pytest.mark.parametrize(
    ("filing_date", "checkbox", "footnote", "expected", "evidence"),
    [
        (date(2019, 3, 1), None, None, "unknown", "none"),
        (date(2019, 3, 1), False, None, "unknown", "none"),  # before the checkbox existed
        (date(2023, 6, 1), None, None, "unknown", "none"),  # checkbox left empty
        (date(2023, 6, 1), False, None, "not_scheduled_reported", "checkbox_unchecked"),
        (date(2023, 6, 1), True, None, "scheduled", "checkbox_filing_level"),
        (
            date(2019, 3, 1),
            None,
            "Effected pursuant to a Rule 10b5-1 trading plan adopted May 1.",
            "scheduled",
            "footnote",
        ),
        (
            date(2019, 3, 1),
            None,
            "These sales were not made pursuant to a Rule 10b5-1 plan.",
            "not_scheduled_reported",
            "footnote_negative",
        ),
    ],
)
def test_t8_plan_status(filing_date, checkbox, footnote, expected, evidence):
    notes = (
        [{"accession_number": "A1", "footnote_id": "F1", "footnote_txt": footnote}]
        if footnote
        else []
    )
    t = normalized(
        *one_filing(
            sub={"filing_date": filing_date, "aff10b5one": checkbox},
            trans_date=filing_date,
            trans_code="S",
            trans_acquired_disp_cd="D",
            equity_swap_trans_cd_fn="F1" if footnote else None,
        ),
        footnotes=notes,
    )
    line = ps_lines(t).row(0, named=True)
    assert (line["plan_status"], line["plan_evidence"]) == (expected, evidence)


# --- events and ownership ----------------------------------------------------------------------


def test_events_aggregate_lines_and_rebuild_ownership():
    subs = [{"accession_number": "A1"}, {"accession_number": "A2"}]
    owners = [
        {"accession_number": "A1"},
        {"accession_number": "A2", "rptownercik": "0000000200", "rptownername": "Roe Rick"},
    ]
    trans = [
        {
            "accession_number": "A1",
            "nonderiv_trans_sk": 1,
            "trans_shares": 600.0,
            "trans_pricepershare": 10.0,
            "shrs_ownd_folwng_trans": 4600.0,
        },
        {
            "accession_number": "A1",
            "nonderiv_trans_sk": 2,
            "trans_shares": 400.0,
            "trans_pricepershare": 11.0,
            "shrs_ownd_folwng_trans": 5000.0,
        },
        {"accession_number": "A2", "trans_shares": 300.0, "shrs_ownd_folwng_trans": 300.0},
    ]
    ev = {
        r["accession_number"]: r
        for r in normalized(subs, owners, trans)["insider_events"].iter_rows(named=True)
    }
    a1 = ev["A1"]
    assert a1["n_lines"] == 2 and a1["shares"] == 1000 and a1["value"] == pytest.approx(10400)
    assert a1["vwap"] == pytest.approx(10.4)
    assert a1["shares_before"] == 4000 and a1["pct_holdings_change"] == pytest.approx(0.25)
    assert not a1["is_new_position"]
    a2 = ev["A2"]
    assert a2["is_new_position"] and a2["pct_holdings_change"] is None


def test_missing_shares_after_makes_ownership_unreliable():
    ev = normalized(*one_filing(shrs_ownd_folwng_trans=None))["insider_events"].row(0, named=True)
    assert not ev["ownership_reliable"] and ev["pct_holdings_change"] is None


def test_normalization_is_deterministic():
    args = one_filing(trans_pricepershare_fn="F1")
    a = normalize_tables(staged(*args), AVAIL)
    b = normalize_tables(staged(*args), AVAIL)
    for name in a:
        assert a[name].equals(b[name]), name
