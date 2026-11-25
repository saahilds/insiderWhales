"""Security-level identification from Form 4 data (security master v0 grows here in PR 7)."""

from __future__ import annotations

import polars as pl

SECURITY_CLASS_RULES: list[tuple[str, str]] = [
    ("preferred", r"(?i)preferred|\bpfd\b|preference share"),
    ("warrant", r"(?i)warrant"),
    ("debt", r"(?i)\bnotes?\b|debenture|\bbonds?\b"),
    (
        "derivative_like",
        r"(?i)\boptions?\b|restricted stock units?|\brsus?\b|phantom|stock appreciation|"
        r"\bsars?\b|performance (share|stock)? ?units?|\bpsus?\b|deferred stock units?",
    ),
    ("unit", r"(?i)\bunits?\b"),
    ("right", r"(?i)\brights?\b"),
    ("adr", r"(?i)depositary|\bads\b|\badr\b"),
    (
        "common",
        r"(?i)common|ordinary|class [a-z]\b|capital stock|shares of beneficial interest|"
        r"^\s*shares\s*$|^\s*stock\s*$|restricted stock|limited partnership units?",
    ),
]


def security_class_expr(col: str = "security_title") -> pl.Expr:
    """Classify a free-text security title; first matching rule wins, else 'unknown'."""
    expr = pl.lit("unknown")
    for name, pattern in reversed(SECURITY_CLASS_RULES):
        expr = (
            pl.when(pl.col(col).fill_null("").str.contains(pattern))
            .then(pl.lit(name))
            .otherwise(expr)
        )
    return expr.alias("security_class")


def share_class_expr(col: str = "security_title") -> pl.Expr:
    """Share-class letter ('A', 'B', ...) when the title names one, else null."""
    return (
        pl.col(col)
        .fill_null("")
        .str.extract(r"(?i)\bclass\s+([a-z])\b", 1)
        .str.to_uppercase()
        .alias("share_class")
    )


def provisional_security_id(issuer_col: str = "issuer_cik") -> pl.Expr:
    """`<issuer_cik>:<share class or COMMON>` until the security master maps listings."""
    return pl.concat_str(
        [pl.col(issuer_col), pl.col("share_class").fill_null("COMMON")], separator=":"
    ).alias("security_id")
