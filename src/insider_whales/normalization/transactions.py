"""Transactions: classification, plan status, amendments, research sets and insider events."""

from __future__ import annotations

from datetime import date, datetime

import polars as pl

from insider_whales.config import AvailabilityConfig
from insider_whales.normalization.insiders import (
    filing_owner_summary,
    insider_groups,
    normalize_owners,
)
from insider_whales.normalization.securities import (
    provisional_security_id,
    security_class_expr,
    share_class_expr,
)
from insider_whales.pit import UTC_DTYPE, with_available_at

CHECKBOX_EFFECTIVE = date(2023, 4, 1)  # amended Forms 4/5 with the Rule 10b5-1 checkbox

# Footnotes on these columns describe the transaction itself; footnotes on holdings columns
# (shares owned after, nature of ownership) often mention awards or plans held, not traded.
TRANSACTION_FN_COLS = (
    "security_title_fn",
    "trans_date_fn",
    "deemed_execution_date_fn",
    "equity_swap_trans_cd_fn",
    "trans_timeliness_fn",
    "trans_shares_fn",
    "trans_pricepershare_fn",
    "trans_acquired_disp_cd_fn",
)
HOLDING_FN_COLS = (
    "shrs_ownd_folwng_trans_fn",
    "valu_ownd_folwng_trans_fn",
    "direct_indirect_ownership_fn",
    "nature_of_ownership_fn",
)

PLAN_RE = r"(?i)10b5-?\s?1|10b-5-1|10b5\(1\)|rule 10b5|trading plan"
PLAN_NEGATED_RE = (
    r"(?i)\bnot\b[^.;]{0,60}(10b5-?\s?1|trading plan)|(10b5-?\s?1|trading plan)[^.;]{0,30}"
    r"\b(was|were) not\b"
)
OPTION_RE = (
    r"(?i)exercise[ds]? (of )?(an? )?(stock |vested |employee )?options?|option exercise|"
    r"sell[- ]to[- ]cover|same[- ]day sale|cashless exercise|upon (the )?exercise|"
    r"exercise and (sale|sell)"
)
COMPENSATION_RE = (
    r"(?i)employee stock purchase|\bespp\b|stock purchase plan|dividend reinvest|\bdrip\b|"
    r"401\(?k\)?|deferred (stock|compensation)|in lieu of (cash|director|fees|salary)|"
    r"director(s'|'s)? fees|retainer|vesting|withh(o|e)ld|tax (withholding|obligation|liabilit)|"
    r"savings plan|incentive (plan|award)|equity award|restricted stock award|"
    r"employee benefit plan|profit sharing"
)
PRIVATE_RE = (
    r"(?i)private(ly)? (placement|negotiated|sale|transaction|purchase|offering)|"
    r"(securities |stock |share )?purchase agreement|subscription agreement|subscription|"
    r"registered direct|underwrit|public offering|initial public offering|\bipo\b|"
    r"rights offering|directed share|concurrent (private )?placement|"
    r"(from|by|with) the (issuer|company)\b|issued by the (issuer|company)|"
    r"in connection with (the|an?) (offering|merger|acquisition|business combination)|"
    r"tender offer|exchange offer|spin-?off|distribution from|pro rata distribution"
)
OPEN_MARKET_RE = (
    r"(?i)weighted.average|multiple transactions|range of prices|prices? rang(ing|ed)|"
    r"open[- ]market|on the (nasdaq|nyse|new york stock exchange)|broker[- ]dealer"
)

OPTION_CODES = ("M", "X", "C", "O", "E", "H")
COMPENSATION_CODES = ("A", "F", "D", "I", "L")
OPEN_MARKET_CLASSES = ("open_market_confirmed", "open_market_probable")
MAX_FILING_LAG_DAYS = 730


# --- filings ---------------------------------------------------------------------------


def build_filings(submission: pl.DataFrame, cfg: AvailabilityConfig) -> pl.DataFrame:
    """One row per accession with availability time and normalised plan checkbox."""
    df = submission.rename({"issuercik": "issuer_cik"}).with_columns(
        pl.col("document_type").str.ends_with("/A").alias("is_amendment"),
        pl.col("document_type").str.replace("/A", "", literal=True).alias("base_form"),
    )
    if "aff10b5one" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Boolean).alias("aff10b5one"))
    if "acceptance_ts" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=UTC_DTYPE).alias("acceptance_ts"))
    df = with_available_at(df, cfg)
    return df.select(
        "accession_number",
        "issuer_cik",
        "issuername",
        pl.col("issuertradingsymbol").alias("reported_ticker"),
        "document_type",
        "base_form",
        "is_amendment",
        "date_of_orig_sub",
        "period_of_report",
        "filing_date",
        "acceptance_ts",
        "available_at",
        "availability_basis",
        pl.col("aff10b5one").alias("plan_checkbox"),
        "remarks",
        pl.format(
            "https://www.sec.gov/Archives/edgar/data/{}/{}-index.htm",
            pl.col("issuer_cik").str.strip_chars_start("0"),
            pl.col("accession_number"),
        ).alias("source_url"),
        "source_quarter",
    ).sort("accession_number")


def link_amendments(filings: pl.DataFrame) -> pl.DataFrame:
    """Attach `amends_accession` and `amend_link_confidence` to amendment filings.

    The data sets carry only the original filing *date* (entered by the filer, sometimes
    wrong), so originals are matched in tiers:
    high: same issuer, form, owner set and original date, unique;
    medium: as high but ambiguous and narrowed by period of report, or same issuer, form,
    owner set and period of report with the latest original filed before the amendment;
    low: same issuer, form and original date only. Anything else stays unmatched.
    """
    originals = filings.filter(~pl.col("is_amendment")).select(
        pl.col("accession_number").alias("orig"),
        "issuer_cik",
        "base_form",
        pl.col("filing_date").alias("orig_filing_date"),
        pl.col("owner_set").alias("orig_owner_set"),
        pl.col("period_of_report").alias("orig_period"),
    )
    amends = filings.filter(pl.col("is_amendment")).select(
        "accession_number",
        "issuer_cik",
        "base_form",
        "date_of_orig_sub",
        "owner_set",
        "period_of_report",
        "filing_date",
    )
    by_date = amends.join(
        originals,
        left_on=["issuer_cik", "base_form", "date_of_orig_sub"],
        right_on=["issuer_cik", "base_form", "orig_filing_date"],
    )
    owner_match = by_date.filter(pl.col("owner_set") == pl.col("orig_owner_set"))
    n_owner = owner_match.group_by("accession_number").agg(pl.len().alias("n"))
    high = owner_match.join(n_owner.filter(pl.col("n") == 1), on="accession_number")
    medium_a = (
        owner_match.join(n_owner.filter(pl.col("n") > 1), on="accession_number")
        .filter(pl.col("period_of_report") == pl.col("orig_period"))
        .sort("orig")
        .group_by("accession_number")
        .first()
    )
    medium_b = (
        amends.join(
            originals,
            left_on=["issuer_cik", "base_form", "owner_set", "period_of_report"],
            right_on=["issuer_cik", "base_form", "orig_owner_set", "orig_period"],
        )
        .filter(pl.col("orig_filing_date") <= pl.col("filing_date"))
        .sort(["orig_filing_date", "orig"])
        .group_by("accession_number")
        .last()
    )
    low = by_date.sort("orig").group_by("accession_number").first()
    tiers = [(high, "high"), (medium_a, "medium"), (medium_b, "medium"), (low, "low")]
    links = pl.concat(
        [
            df.select("accession_number", "orig").with_columns(pl.lit(c).alias("conf"))
            for df, c in tiers
        ]
    ).unique("accession_number", keep="first", maintain_order=True)
    out = filings.join(
        links.rename({"orig": "amends_accession", "conf": "amend_link_confidence"}),
        on="accession_number",
        how="left",
    )
    return out.with_columns(
        pl.when(pl.col("is_amendment") & pl.col("amends_accession").is_null())
        .then(pl.lit("unmatched"))
        .otherwise(pl.col("amend_link_confidence"))
        .alias("amend_link_confidence")
    )


# --- transactions ------------------------------------------------------------------------


def _footnote_text(trans: pl.DataFrame, footnotes: pl.DataFrame, cols: tuple[str, ...], name: str):
    present = [c for c in cols if c in trans.columns]
    ids = (
        trans.select(
            "txn_id",
            "accession_number",
            pl.concat_list([pl.col(c).fill_null("").str.split(",") for c in present]).alias("fid"),
        )
        .explode("fid")
        .with_columns(pl.col("fid").str.strip_chars())
        .filter(pl.col("fid") != "")
        .unique(["txn_id", "fid"])
    )
    joined = ids.join(
        footnotes.select("accession_number", pl.col("footnote_id").alias("fid"), "footnote_txt"),
        on=["accession_number", "fid"],
        how="left",
    )
    agg = (
        joined.sort(["txn_id", "fid"])
        .group_by("txn_id")
        .agg(
            pl.col("fid").alias(f"{name}_ids"),
            pl.col("footnote_txt").drop_nulls().str.join(" | ").alias(f"{name}_text"),
        )
    )
    return trans.join(agg, on="txn_id", how="left").with_columns(
        pl.col(f"{name}_text").fill_null("")
    )


def _txn_class() -> pl.Expr:
    code = pl.col("trans_code")
    txt = pl.col("txn_footnote_text")
    is_ps = code.is_in(["P", "S"])
    return (
        pl.when(pl.col("table") == "deriv")
        .then(pl.lit("option_related"))
        .when(code.is_in(OPTION_CODES))
        .then(pl.lit("option_related"))
        .when(code.is_in(COMPENSATION_CODES))
        .then(pl.lit("compensation"))
        .when(code == "G")
        .then(pl.lit("gift"))
        .when(~is_ps)
        .then(pl.lit("other"))
        .when(txt.str.contains(OPTION_RE))
        .then(pl.lit("option_related"))
        .when(txt.str.contains(COMPENSATION_RE))
        .then(pl.lit("compensation"))
        .when(txt.str.contains(PRIVATE_RE) | pl.col("remarks_private"))
        .then(pl.lit("private"))
        .when(pl.col("trans_pricepershare").is_null() | (pl.col("trans_pricepershare") <= 0))
        .then(pl.lit("unknown"))
        .when(txt.str.contains(OPEN_MARKET_RE))
        .then(pl.lit("open_market_confirmed"))
        .otherwise(pl.lit("open_market_probable"))
        .alias("txn_class")
    )


def _plan_status() -> pl.Expr:
    all_txt = pl.concat_str(
        [pl.col("txn_footnote_text"), pl.col("holding_footnote_text")], separator=" "
    )
    fn_mention = all_txt.str.contains(PLAN_RE)
    fn_negated = all_txt.str.contains(PLAN_NEGATED_RE)
    remarks_plan = pl.col("remarks").fill_null("").str.contains(PLAN_RE)
    remarks_neg = pl.col("remarks").fill_null("").str.contains(PLAN_NEGATED_RE)
    post_rule = pl.col("filing_date") >= CHECKBOX_EFFECTIVE
    checkbox = pl.col("plan_checkbox")
    status = (
        pl.when(fn_mention & ~fn_negated)
        .then(pl.lit("scheduled"))
        .when(post_rule & (checkbox == True))  # noqa: E712
        .then(pl.lit("scheduled"))
        .when(remarks_plan & ~remarks_neg)
        .then(pl.lit("scheduled"))
        .when(fn_negated | remarks_neg)
        .then(pl.lit("not_scheduled_reported"))
        .when(post_rule & (checkbox == False))  # noqa: E712
        .then(pl.lit("not_scheduled_reported"))
        .otherwise(pl.lit("unknown"))
    )
    evidence = (
        pl.when(fn_mention & ~fn_negated)
        .then(pl.lit("footnote"))
        .when(post_rule & (checkbox == True))  # noqa: E712
        .then(pl.lit("checkbox_filing_level"))
        .when(remarks_plan & ~remarks_neg)
        .then(pl.lit("remarks"))
        .when(fn_negated | remarks_neg)
        .then(pl.lit("footnote_negative"))
        .when(post_rule & (checkbox == False))  # noqa: E712
        .then(pl.lit("checkbox_unchecked"))
        .otherwise(pl.lit("none"))
    )
    return [status.alias("plan_status"), evidence.alias("plan_evidence")]


def build_transactions(
    nonderiv: pl.DataFrame,
    deriv: pl.DataFrame | None,
    footnotes: pl.DataFrame,
    filings: pl.DataFrame,
    filing_owners: pl.DataFrame,
    groups: pl.DataFrame,
) -> pl.DataFrame:
    """Transaction lines with classification, plan status, research-set flags."""
    frames = [
        nonderiv.with_columns(
            pl.lit("nonderiv").alias("table"), pl.col("nonderiv_trans_sk").alias("line_sk")
        )
    ]
    if deriv is not None and deriv.height:
        frames.append(
            deriv.with_columns(
                pl.lit("deriv").alias("table"), pl.col("deriv_trans_sk").alias("line_sk")
            )
        )
    t = pl.concat(frames, how="diagonal_relaxed").with_columns(
        pl.concat_str(
            [pl.col("accession_number"), pl.col("table"), pl.col("line_sk").cast(pl.String)],
            separator=":",
        ).alias("txn_id")
    )
    t = _footnote_text(t, footnotes, TRANSACTION_FN_COLS, "txn_footnote")
    t = _footnote_text(t, footnotes, HOLDING_FN_COLS, "holding_footnote")
    t = (
        t.join(
            filings.select(
                "accession_number",
                "issuer_cik",
                "reported_ticker",
                "document_type",
                "is_amendment",
                "amends_accession",
                "filing_date",
                "available_at",
                "availability_basis",
                "plan_checkbox",
                "remarks",
            ),
            on="accession_number",
            how="left",
        )
        .join(
            filing_owners.select(
                "accession_number",
                "primary_owner_cik",
                "primary_owner_name",
                "role_norm",
                "primary_title",
                "is_officer_or_director",
                "any_ten_pct",
                "n_owners",
            ),
            on="accession_number",
            how="left",
        )
        .join(
            groups.rename({"owner_cik": "primary_owner_cik"}),
            on=["issuer_cik", "primary_owner_cik"],
            how="left",
        )
    )
    t = t.with_columns(
        security_class_expr("security_title"),
        share_class_expr("security_title"),
        pl.col("remarks").fill_null("").str.contains(PRIVATE_RE).alias("remarks_private"),
        (pl.col("trans_shares") * pl.col("trans_pricepershare")).alias("trans_value"),
        (pl.col("filing_date") - pl.col("trans_date")).dt.total_days().alias("filing_lag_days"),
    ).with_columns(
        provisional_security_id("issuer_cik"),
        _txn_class(),
        *_plan_status(),
    )
    reasons = {
        "code_not_p_or_s": ~pl.col("trans_code").is_in(["P", "S"]),
        "derivative_table": pl.col("table") == "deriv",
        "not_common_equity": pl.col("security_class") != "common",
        "zero_or_missing_price": pl.col("trans_pricepershare").is_null()
        | (pl.col("trans_pricepershare") <= 0),
        "invalid_shares": pl.col("trans_shares").is_null() | (pl.col("trans_shares") <= 0),
        "bad_transaction_date": pl.col("trans_date").is_null()
        | (pl.col("filing_lag_days") < 0)
        | (pl.col("filing_lag_days") > MAX_FILING_LAG_DAYS),
        "not_open_market": ~pl.col("txn_class").is_in(OPEN_MARKET_CLASSES),
        "ten_pct_or_other_only": ~pl.col("is_officer_or_director").fill_null(False),
        "amendment_version": pl.col("is_amendment"),
        "equity_swap": pl.col("equity_swap_involved").fill_null(False),
    }
    t = t.with_columns(
        pl.concat_list(
            [
                pl.when(cond.fill_null(True)).then(pl.lit(name)).otherwise(pl.lit(None))
                for name, cond in reasons.items()
            ]
        )
        .list.drop_nulls()
        .alias("exclusion_reasons")
    )
    sensitivity_blockers = [
        "code_not_p_or_s",
        "derivative_table",
        "zero_or_missing_price",
        "invalid_shares",
        "bad_transaction_date",
        "amendment_version",
    ]
    r = pl.col("exclusion_reasons")
    t = t.with_columns(
        (r.list.len() == 0).alias("is_conservative_set"),
        (
            ~r.list.eval(pl.element().is_in(sensitivity_blockers)).list.any()
            & pl.col("security_class").is_in(["common", "adr", "unknown"])
            & ~pl.col("txn_class").is_in(["compensation", "option_related", "gift"])
        ).alias("is_sensitivity_set"),
    )
    return t.select(
        "txn_id",
        "accession_number",
        "table",
        "line_sk",
        "issuer_cik",
        "security_id",
        "security_title",
        "security_class",
        "share_class",
        "reported_ticker",
        "insider_group_id",
        "primary_owner_cik",
        "primary_owner_name",
        "role_norm",
        "primary_title",
        "is_officer_or_director",
        "any_ten_pct",
        "n_owners",
        "document_type",
        "is_amendment",
        "amends_accession",
        "filing_date",
        "available_at",
        "availability_basis",
        "trans_date",
        "filing_lag_days",
        "trans_code",
        pl.col("trans_acquired_disp_cd").alias("acq_disp"),
        pl.col("trans_shares").alias("shares"),
        pl.col("trans_pricepershare").alias("price"),
        pl.col("trans_value").alias("value"),
        pl.col("shrs_ownd_folwng_trans").alias("shares_after"),
        pl.col("direct_indirect_ownership").alias("direct_indirect"),
        "nature_of_ownership",
        "trans_timeliness",
        "equity_swap_involved",
        "txn_footnote_ids",
        "txn_footnote_text",
        "holding_footnote_ids",
        "holding_footnote_text",
        "txn_class",
        "plan_status",
        "plan_evidence",
        "plan_checkbox",
        "exclusion_reasons",
        "is_conservative_set",
        "is_sensitivity_set",
    ).sort("txn_id")


def transactions_as_of(
    transactions: pl.DataFrame, ts: datetime, view: str = "original"
) -> pl.DataFrame:
    """Transaction lines knowable at `ts`.

    view="original": only as-originally-filed lines (primary research view).
    view="latest": an amendment that is public by `ts` and has transaction lines replaces the
    lines of the filing it amends; amendments never affect anything before they are public.
    """
    known = transactions.filter(pl.col("available_at") <= pl.lit(ts).cast(UTC_DTYPE))
    originals = known.filter(~pl.col("is_amendment"))
    if view == "original":
        return originals
    if view != "latest":
        raise ValueError("view must be 'original' or 'latest'")
    amends = known.filter(pl.col("is_amendment") & pl.col("amends_accession").is_not_null())
    latest_amend = (
        amends.select("amends_accession", "accession_number", "available_at")
        .unique()
        .sort(["available_at", "accession_number"])
        .group_by("amends_accession")
        .last()
    )
    superseded = latest_amend["amends_accession"].to_list()
    keep_amend_lines = amends.join(
        latest_amend.select("accession_number"), on="accession_number", how="semi"
    )
    return pl.concat(
        [originals.filter(~pl.col("accession_number").is_in(superseded)), keep_amend_lines]
    ).sort("txn_id")


# --- insider events -----------------------------------------------------------------------


def build_insider_events(
    transactions: pl.DataFrame, holdings: pl.DataFrame | None = None
) -> pl.DataFrame:
    """Aggregate lines of one filing by insider, security, date and code into one event.

    Holdings before the event are rebuilt per ownership form (direct, each indirect entity)
    from "shares owned after" minus the signed shares traded; untouched holdings reported in
    the same filing are added. `pct_holdings_change` is null whenever that is unreliable.
    """
    lines = transactions.filter(pl.col("is_sensitivity_set"))
    signed = pl.when(pl.col("acq_disp") == "D").then(-pl.col("shares")).otherwise(pl.col("shares"))
    lines = lines.with_columns(
        signed.alias("signed_shares"),
        pl.concat_str(
            [pl.col("direct_indirect").fill_null("?"), pl.col("nature_of_ownership").fill_null("")],
            separator="|",
        ).alias("ownership_form"),
    )
    keys = [
        "accession_number",
        "insider_group_id",
        "issuer_cik",
        "security_id",
        "trans_date",
        "trans_code",
    ]
    forms = (
        lines.sort("line_sk")
        .group_by([*keys, "ownership_form"], maintain_order=True)
        .agg(
            pl.col("shares_after").last().alias("after_form"),
            pl.col("signed_shares").sum().alias("traded_form"),
        )
        .with_columns((pl.col("after_form") - pl.col("traded_form")).alias("before_form"))
    )
    ownership = forms.group_by(keys).agg(
        pl.col("after_form").sum().alias("shares_after_traded_forms"),
        pl.col("before_form").sum().alias("shares_before_traded_forms"),
        (pl.col("after_form").is_null().any() | (pl.col("before_form") < -0.5).any()).alias(
            "_unreliable"
        ),
    )
    if holdings is not None and holdings.height:
        other = (
            holdings.filter(pl.col("security_class") == "common")
            .group_by(["accession_number", "security_id"])
            .agg(pl.col("shrs_ownd_folwng_trans").sum().alias("other_holdings"))
        )
        ownership = ownership.join(other, on=["accession_number", "security_id"], how="left")
    else:
        ownership = ownership.with_columns(pl.lit(None, dtype=pl.Float64).alias("other_holdings"))
    ownership = ownership.with_columns(
        (pl.col("shares_before_traded_forms") + pl.col("other_holdings").fill_null(0)).alias(
            "shares_before"
        ),
        (pl.col("shares_after_traded_forms") + pl.col("other_holdings").fill_null(0)).alias(
            "shares_after"
        ),
    )
    plan_rank = pl.col("plan_status").replace_strict(
        {"scheduled": 0, "unknown": 1, "not_scheduled_reported": 2}, return_dtype=pl.Int8
    )
    events = (
        lines.with_columns(plan_rank.alias("_plan_rank"))
        .group_by(keys)
        .agg(
            pl.col("shares").sum().alias("shares"),
            pl.col("signed_shares").sum().alias("signed_shares"),
            pl.col("value").sum().alias("value"),
            pl.col("price").min().alias("price_min"),
            pl.col("price").max().alias("price_max"),
            pl.len().alias("n_lines"),
            pl.col("txn_id").sort().alias("txn_ids"),
            pl.col("available_at").first(),
            pl.col("filing_date").first(),
            pl.col("reported_ticker").first(),
            pl.col("role_norm").first(),
            pl.col("is_officer_or_director").first(),
            pl.col("primary_owner_cik").first(),
            pl.col("primary_owner_name").first(),
            pl.col("security_class").first(),
            pl.col("is_conservative_set").all().alias("is_conservative_set"),
            (pl.col("txn_class") == "open_market_confirmed")
            .any()
            .alias("any_open_market_confirmed"),
            pl.col("txn_class").unique().sort().alias("txn_classes"),
            pl.col("_plan_rank").min().alias("_plan_rank"),
        )
        .join(ownership, on=keys, how="left")
        .with_columns(
            (pl.col("value") / pl.col("shares")).alias("vwap"),
            pl.col("_plan_rank")
            .replace_strict(
                {0: "scheduled", 1: "unknown", 2: "not_scheduled_reported"}, return_dtype=pl.String
            )
            .alias("plan_status"),
            (~pl.col("_unreliable").fill_null(True)).alias("ownership_reliable"),
        )
    )
    reliable = pl.col("ownership_reliable")
    events = events.with_columns(
        pl.when(reliable & (pl.col("shares_before") > 0.5))
        .then(pl.col("signed_shares") / pl.col("shares_before"))
        .otherwise(pl.lit(None))
        .alias("pct_holdings_change"),
        (reliable & (pl.col("shares_before").abs() <= 0.5) & (pl.col("trans_code") == "P")).alias(
            "is_new_position"
        ),
    )
    return (
        events.with_columns(
            pl.concat_str(
                [
                    pl.col("accession_number"),
                    pl.col("insider_group_id"),
                    pl.col("security_id"),
                    pl.col("trans_date").cast(pl.String),
                    pl.col("trans_code"),
                ],
                separator="|",
            ).alias("event_id")
        )
        .drop("_plan_rank", "_unreliable")
        .sort("event_id")
    )


# --- orchestration --------------------------------------------------------------------------


def normalize_tables(
    staged: dict[str, pl.DataFrame], cfg: AvailabilityConfig
) -> dict[str, pl.DataFrame]:
    """Build all processed tables from staged tables (keys are lower-case table names)."""
    filings = build_filings(staged["submission"], cfg)
    owners = normalize_owners(staged["reportingowner"], staged["submission"])
    summary = filing_owner_summary(owners)
    filings = link_amendments(
        filings.join(
            summary.select("accession_number", "owner_set"), on="accession_number", how="left"
        )
    )
    filing_owner_rows = owners.join(
        filings.select("accession_number", "issuer_cik", "available_at"), on="accession_number"
    )
    groups = insider_groups(filing_owner_rows)
    transactions = build_transactions(
        staged["nonderiv_trans"],
        staged.get("deriv_trans"),
        staged["footnotes"],
        filings,
        summary,
        groups,
    )
    holdings = staged.get("nonderiv_holding")
    if holdings is not None:
        holdings = (
            holdings.join(
                filings.select("accession_number", "issuer_cik"), on="accession_number", how="left"
            )
            .with_columns(security_class_expr(), share_class_expr())
            .with_columns(provisional_security_id())
        )
    events = build_insider_events(transactions, holdings)
    return {
        "filings": filings,
        "reporting_owners": owners,
        "insider_groups": groups,
        "transactions": transactions,
        "insider_events": events,
    }
