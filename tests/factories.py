"""Builders for small synthetic staged SEC tables (shape of `normalization.staging` output).

Synthetic data only; never written into data/ or reports.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from insider_whales.config import load_settings
from insider_whales.normalization.transactions import normalize_tables

AVAIL = load_settings().availability

SUBMISSION_DEFAULTS = {
    "accession_number": None,
    "filing_date": date(2019, 3, 1),
    "period_of_report": date(2019, 2, 27),
    "date_of_orig_sub": None,
    "document_type": "4",
    "issuercik": "0000000001",
    "issuername": "Example Corp",
    "issuertradingsymbol": "EXMP",
    "remarks": None,
    "aff10b5one": None,
    "source_quarter": "2019q1",
}
OWNER_DEFAULTS = {
    "accession_number": None,
    "rptownercik": "0000000100",
    "rptownername": "Doe Jane",
    "rptowner_relationship": "Officer",
    "rptowner_title": "Chief Executive Officer",
    "rptowner_txt": None,
}
TRANS_DEFAULTS = {
    "accession_number": None,
    "nonderiv_trans_sk": 1,
    "security_title": "Common Stock",
    "trans_date": date(2019, 2, 27),
    "trans_code": "P",
    "equity_swap_involved": False,
    "trans_timeliness": None,
    "trans_shares": 1000.0,
    "trans_pricepershare": 10.0,
    "trans_acquired_disp_cd": "A",
    "shrs_ownd_folwng_trans": 5000.0,
    "direct_indirect_ownership": "D",
    "nature_of_ownership": None,
    "trans_pricepershare_fn": None,
    "trans_shares_fn": None,
    "equity_swap_trans_cd_fn": None,
    "shrs_ownd_folwng_trans_fn": None,
}
DERIV_DEFAULTS = {
    "accession_number": None,
    "deriv_trans_sk": 1,
    "security_title": "Stock Option (right to buy)",
    "trans_date": date(2019, 2, 27),
    "trans_code": "M",
    "trans_shares": 1000.0,
    "trans_pricepershare": 0.0,
    "trans_acquired_disp_cd": "D",
    "shrs_ownd_folwng_trans": 0.0,
    "direct_indirect_ownership": "D",
    "nature_of_ownership": None,
}
SCHEMAS = {
    "submission": {
        "filing_date": pl.Date,
        "period_of_report": pl.Date,
        "date_of_orig_sub": pl.Date,
        "aff10b5one": pl.Boolean,
    },
    "nonderiv_trans": {"trans_date": pl.Date, "nonderiv_trans_sk": pl.Int64},
    "deriv_trans": {"trans_date": pl.Date, "deriv_trans_sk": pl.Int64},
}


def _frame(defaults: dict, rows: list[dict], table: str) -> pl.DataFrame:
    filled = [{**defaults, **r} for r in rows]
    schema = {k: pl.String for k in defaults}
    for k in ("trans_shares", "trans_pricepershare", "shrs_ownd_folwng_trans"):
        if k in defaults:
            schema[k] = pl.Float64
    if "equity_swap_involved" in defaults:
        schema["equity_swap_involved"] = pl.Boolean
    schema.update(SCHEMAS.get(table, {}))
    return pl.DataFrame(filled, schema=schema)


def staged(
    submissions: list[dict],
    owners: list[dict],
    trans: list[dict],
    footnotes: list[dict] | None = None,
    deriv: list[dict] | None = None,
) -> dict[str, pl.DataFrame]:
    out = {
        "submission": _frame(SUBMISSION_DEFAULTS, submissions, "submission"),
        "reportingowner": _frame(OWNER_DEFAULTS, owners, "reportingowner"),
        "nonderiv_trans": _frame(TRANS_DEFAULTS, trans, "nonderiv_trans"),
        "footnotes": pl.DataFrame(
            footnotes or [],
            schema={
                "accession_number": pl.String,
                "footnote_id": pl.String,
                "footnote_txt": pl.String,
            },
        ),
    }
    if deriv:
        out["deriv_trans"] = _frame(DERIV_DEFAULTS, deriv, "deriv_trans")
    return out


def normalized(*args, **kwargs) -> dict[str, pl.DataFrame]:
    return normalize_tables(staged(*args, **kwargs), AVAIL)


def one_filing(acc: str = "A1", sub: dict | None = None, owner: dict | None = None, **trans):
    """Single-owner filing with one transaction line."""
    return (
        [{"accession_number": acc, **(sub or {})}],
        [{"accession_number": acc, **(owner or {})}],
        [{"accession_number": acc, **trans}],
    )
