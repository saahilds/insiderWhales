"""Clean the free-text trading symbol of a Form 3/4/5 into real tickers.

Filers type whatever they like into the symbol box: `QADA, QADB`, `MOGA/MOGB`, `NYSE: SCS`,
`(CALX)`, a ticker wrapped in quote marks, `NRCIA/B`, `BWINA / B`. This module turns such a
value into a list of tickers, each with a share-class label when the text implies one (QADA and
QADB are Class A and Class B of QAD Inc.). Values that hold no usable ticker (`N/A`, `NONE`,
numbers, codes) give an empty list.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

PLACEHOLDERS = {"", "NONE", "N/A", "NA", "-", "--", "NOT APPLICABLE", "NOT LISTED", "N A", "NIL"}

# Words that are exchange or market names or filler, never tickers.
NOISE_TOKENS = {
    "NYSE",
    "NSYE",
    "NASDAQ",
    "NASD",
    "NMS",
    "AMEX",
    "MKT",
    "ARCA",
    "OTC",
    "OTCBB",
    "OTCQB",
    "OTCQX",
    "OTCMKTS",
    "OB",
    "PK",
    "PINK",
    "GR",
    "AX",
    "ASX",
    "TSX",
    "US",
    "CATS",
    "TICKER",
    "SYMBOL",
    "AND",
    "OR",
    "THE",
    "CLASS",
    "COMMON",
    "STOCK",
    "SHARES",
}
# Company-name fragments that appear when a filer pastes the name instead of a symbol.
NAME_FRAGMENTS = {"INC", "CORP", "LTD", "LLC", "LP", "PLC", "CO", "ENT", "II", "III"}

TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}([.\-][A-Z0-9]{1,2})?$")
SEPARATORS = re.compile(r"[,;/&|:_\s]+")
WRAPPERS = str.maketrans({c: " " for c in "()[]{}\"'`?*<>#"})


@dataclass(frozen=True)
class ParsedTicker:
    ticker: str
    share_class: str | None  # 'A', 'B', ... when the text implies it, else None


def _clean_token(tok: str) -> str:
    return tok.strip(" .-")


def _split(raw: str) -> list[str]:
    text = re.sub(r"(?<=[A-Z])'(?=[A-Z]$)", ".", raw.upper().strip())  # BF'B -> BF.B
    text = text.translate(WRAPPERS)
    text = re.sub(r"\bNYSE\s+MKT\b|\bNYSE\s+AMERICAN\b|\bNYSE\s+ARCA\b", " ", text)
    # Exchange prefixes or suffixes glued to a symbol by a dot or dash: `NTIP-NYSE`, `JAMN.OTCQB`.
    text = re.sub(r"[.\-](NYSE|NASDAQ|OTC\w*|OB|PK|AX|GR|MKT)\b", " ", text)
    return [t for t in (_clean_token(x) for x in SEPARATORS.split(text)) if t]


def parse_reported_tickers(raw: str | None) -> list[ParsedTicker]:
    """All tickers named in a reported-symbol field, in order, de-duplicated.

    `NRCIA/B` style shorthand (a lone letter after a ticker) expands to the same stem with that
    letter: NRCIA and NRCIB. Single-letter pieces of a spaced ticker (`N O G`) are rejoined.
    """
    if raw is None:
        return []
    text = raw.strip().upper()
    if text in PLACEHOLDERS or text.strip("[]()\"'? ") in PLACEHOLDERS:
        return []
    tokens = [t for t in _split(raw) if t not in NOISE_TOKENS]
    if not tokens:
        return []
    if len(tokens) > 1 and all(len(t) == 1 and t.isalpha() for t in tokens) and len(tokens) <= 5:
        tokens = ["".join(tokens)]

    out: list[str] = []
    slash = "/" in raw
    for tok in tokens:
        if len(tok) == 1 and tok.isalpha() and out:
            prev = out[-1]
            # `NRCIA/B`: a lone letter after a slash is the sibling class of the previous ticker.
            # Without a slash (`MSF A`) it is not a ticker and is dropped.
            if slash and len(prev) >= 2 and prev.isalpha() and prev[-1] != tok:
                out.append(prev[:-1] + tok)
            continue
        out.append(tok)

    if len(out) > 1:
        out = [t for t in out if t not in NAME_FRAGMENTS]
    tickers = [t.replace("'", ".") for t in out if TICKER_RE.match(t) and not t.isdigit()]
    tickers = [t for t in tickers if t not in NOISE_TOKENS]
    seen: list[str] = []
    for t in tickers:
        if t not in seen:
            seen.append(t)
    return assign_share_classes(seen)


def _explicit_class(ticker: str) -> str | None:
    m = re.search(r"[.\-]([A-Z])$", ticker)
    return m.group(1) if m else None


def assign_share_classes(tickers: list[str]) -> list[ParsedTicker]:
    """Label tickers with a share class using only what the list itself implies.

    - `HEI.A`, `BRK-B`: the suffix letter.
    - `QADA, QADB`, `ISCA, ISCB`: siblings with the same stem and different last letters.
    - `HEI, HEI.A`: the plain ticker is left unlabelled (the common/ordinary line).
    A single ticker gets no label; that is resolved later from the security titles.
    """
    labels: dict[str, str | None] = {t: _explicit_class(t) for t in tickers}
    if len(tickers) > 1:
        stems: dict[str, list[str]] = {}
        for t in tickers:
            if labels[t] is None and t[-1].isalpha() and "." not in t and "-" not in t:
                stems.setdefault(t[:-1], []).append(t)
        for group in stems.values():
            if len(group) > 1:
                for t in group:
                    labels[t] = t[-1]
    return [ParsedTicker(t, labels[t]) for t in tickers]


def to_yahoo_symbol(ticker: str) -> str:
    """Yahoo writes share classes with a dash (`BRK-B`, `HEI-A`)."""
    return ticker.replace(".", "-")
