"""Reporting owners: role normalisation and grouping of related owners into one insider."""

from __future__ import annotations

from datetime import datetime

import polars as pl

from insider_whales.pit import UTC_DTYPE

ROLE_RANK = {"CEO": 0, "CFO": 1, "OtherOfficer": 2, "Director": 3, "TenPct": 4, "Other": 5}
OFFICER_DIRECTOR_ROLES = ("CEO", "CFO", "OtherOfficer", "Director")

CEO_RE = r"(?i)\bc\.?e\.?o\b|chief executive|principal executive"
CFO_RE = r"(?i)\bc\.?f\.?o\b|chief financial|principal financial|principal accounting and financial"
SEE_REMARKS_RE = r"(?i)see remarks|see remark|refer to remarks"
ENTITY_RE = (
    r"(?i)\b(llc|l\.l\.c|lp|l\.p|llp|inc|corp|corporation|company|co\.|fund|funds|trust|"
    r"partners|partnership|capital|holdings|management|advisors|advisers|investments|"
    r"ventures|group|limited|ltd|plc|s\.a|n\.v|gmbh|ag|bank|foundation|associates)\b"
)


def normalize_owners(owners: pl.DataFrame, filings: pl.DataFrame | None = None) -> pl.DataFrame:
    """One row per (accession, owner) with role flags and a single normalised role.

    `filings` (with `accession_number`, `remarks`) lets "See Remarks" titles be resolved.
    """
    df = owners
    if filings is not None and "remarks" in filings.columns:
        df = df.join(
            filings.select("accession_number", "remarks"), on="accession_number", how="left"
        )
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.String).alias("remarks"))
    rel = pl.col("rptowner_relationship").fill_null("").str.to_lowercase()
    title = pl.col("rptowner_title").fill_null("")
    other_txt = pl.col("rptowner_txt").fill_null("")
    role_text = (
        pl.when(title.str.contains(SEE_REMARKS_RE))
        .then(pl.concat_str([title, other_txt, pl.col("remarks").fill_null("")], separator=" "))
        .otherwise(pl.concat_str([title, other_txt], separator=" "))
    )
    df = df.with_columns(
        rel.str.contains("director").alias("is_director"),
        rel.str.contains("officer").alias("is_officer"),
        rel.str.contains("tenpercentowner|ten percent|10%").alias("is_ten_pct"),
        rel.str.contains("other").alias("is_other"),
        role_text.str.contains(CEO_RE).alias("is_ceo"),
        role_text.str.contains(CFO_RE).alias("is_cfo"),
        pl.col("rptownername").fill_null("").str.contains(ENTITY_RE).alias("is_entity_guess"),
    )
    role = (
        pl.when(pl.col("is_ceo"))
        .then(pl.lit("CEO"))
        .when(pl.col("is_cfo"))
        .then(pl.lit("CFO"))
        .when(pl.col("is_officer"))
        .then(pl.lit("OtherOfficer"))
        .when(pl.col("is_director"))
        .then(pl.lit("Director"))
        .when(pl.col("is_ten_pct"))
        .then(pl.lit("TenPct"))
        .otherwise(pl.lit("Other"))
    )
    return df.with_columns(
        role.alias("role_norm"),
        role.replace_strict(ROLE_RANK, return_dtype=pl.Int8).alias("role_rank"),
    ).select(
        "accession_number",
        pl.col("rptownercik").alias("owner_cik"),
        pl.col("rptownername").alias("owner_name"),
        "is_director",
        "is_officer",
        "is_ten_pct",
        "is_other",
        "is_ceo",
        "is_cfo",
        "role_norm",
        "role_rank",
        pl.col("rptowner_title").alias("title_raw"),
        "is_entity_guess",
    )


class _UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            lo, hi = sorted((ra, rb))
            self.parent[hi] = lo


def insider_groups(filing_owners: pl.DataFrame, as_of: datetime | None = None) -> pl.DataFrame:
    """Group owner CIKs that have co-filed for the same issuer into one insider.

    `filing_owners` needs `issuer_cik`, `accession_number`, `owner_cik`, `available_at`.
    With `as_of`, only filings public by then create links, so grouping is point-in-time.
    The group id is `<issuer_cik>:<smallest owner CIK in the group>`.
    """
    df = filing_owners
    if as_of is not None:
        df = df.filter(pl.col("available_at") <= pl.lit(as_of).cast(UTC_DTYPE))
    out = []
    for (issuer,), part in df.sort("issuer_cik").group_by(["issuer_cik"], maintain_order=True):
        uf = _UnionFind()
        for owners in part.group_by("accession_number").agg(pl.col("owner_cik"))["owner_cik"]:
            owners = sorted(set(owners))
            for o in owners:
                uf.find(o)
            for o in owners[1:]:
                uf.union(owners[0], o)
        for o in sorted(uf.parent):
            out.append((issuer, o, f"{issuer}:{uf.find(o)}"))
    return pl.DataFrame(
        out, schema=["issuer_cik", "owner_cik", "insider_group_id"], orient="row"
    ).sort(["issuer_cik", "owner_cik"])


def filing_owner_summary(owners_norm: pl.DataFrame) -> pl.DataFrame:
    """One row per filing: owner list and the most senior role across its owners."""
    return (
        owners_norm.sort(["accession_number", "role_rank", "is_entity_guess", "owner_cik"])
        .group_by("accession_number", maintain_order=True)
        .agg(
            pl.col("owner_cik").sort().alias("owner_ciks"),
            pl.col("owner_cik").first().alias("primary_owner_cik"),
            pl.col("owner_name").first().alias("primary_owner_name"),
            pl.col("role_norm").first().alias("role_norm"),
            pl.col("title_raw").first().alias("primary_title"),
            pl.col("is_director").any().alias("any_director"),
            pl.col("is_officer").any().alias("any_officer"),
            pl.col("is_ten_pct").any().alias("any_ten_pct"),
            pl.len().alias("n_owners"),
        )
        .with_columns(
            (
                pl.col("role_norm").is_in(OFFICER_DIRECTOR_ROLES)
                | pl.col("any_director")
                | pl.col("any_officer")
            ).alias("is_officer_or_director"),
            pl.col("owner_ciks").list.join(",").alias("owner_set"),
        )
    )
