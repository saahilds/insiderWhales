# Research notes

Decisions, data limitations and unresolved issues. Newest entries are added to the relevant section rather than appended as a log.

## Decisions

- **Free data only.** No paid data will be used. Every result built on free price data is labelled *exploratory / survivor-only*. A generic provider import interface is kept so another data set could be plugged in later.
- **Point-in-time everywhere.** Every fact carries `available_at` (UTC), the earliest moment it could have been known, and all research joins are "as of" a decision time.
- **Availability of a filing.** SEC acceptance time plus a 5-minute dissemination buffer and a 15-minute processing buffer. Without an acceptance time: 23:59:59 New York time on the filing date (Forms 3/4/5 submitted up to 22:00 ET still get that day's filing date).
- **Amendments are new versions.** A 4/A never edits the original; it becomes visible only from its own `available_at`. Primary research uses as-originally-filed data.
- **"Discretionary" is never inferred from missing data.** Primary strategy variants use "not known to be scheduled"; discretionary-only variants are reported separately for 2023 onward.
- **Final test period is locked.** Returns dated 2023-01-01 or later are not computed until strategy configurations are frozen.
- **Cutoff:** 2026Q3 SEC data.

## Mandatory tests

| # | Requirement | Status |
|---|---|---|
| T1 | Filing after the close cannot execute at the preceding close | done (`tests/test_timing.py`) |
| T2 | Weekend disclosure executes no earlier than the next session | done (`tests/test_timing.py`) |
| T3 | Exchange holidays respected | done (`tests/test_timing.py`) |
| T4 | Cluster cannot trigger before the second disclosure | planned (PR 10) |
| T5 | Repeat purchases by one insider are not independent insiders | planned (PR 4) |
| T6 | Amendments cannot change previously available information | planned (PR 4) |
| T7 | Option exercises and tax dispositions excluded | planned (PR 4) |
| T8 | Missing plan information is not "discretionary" | planned (PR 4) |
| T9 | Split-adjusted position accounting | planned (PR 7, PR 15) |
| T10 | Delisted stocks not silently removed | planned (PR 7) |
| T11 | Recycled tickers do not merge companies | planned (PR 7) |
| T12 | Fundamentals not used before publication | planned (PR 10) |
| T13 | Insider win rates exclude incomplete horizons | planned (PR 10) |
| T14 | Transaction costs reduce returns correctly | planned (PR 15) |
| T15 | Position sizing cannot exceed cash | planned (PR 15) |
| T16 | Exposure respects limits | planned (PR 15) |
| T17 | Same inputs and config give identical outputs | started (`tests/test_timing.py`); end-to-end in PR 12 |
| T18 | Event-study returns match hand calculation | planned (PR 11) |

## Known data limitations

- **Survivorship bias (free prices).** yfinance returns no history for delisted or acquired companies and can map old tickers to unrelated new securities.
- **yfinance "unadjusted" prices are split-adjusted.** Raw prices must be rebuilt from split events before price filters are applied.
- **Rule 10b5-1 checkbox** exists only from about April 2023, is filing-level rather than per transaction, and is encoded inconsistently. Earlier plan status comes only from footnotes and is mostly unknown.
- **Acceptance timestamps** are not in the SEC bulk insider data sets; they come from EDGAR submissions metadata. Actual public dissemination time is not available for free.

## Open issues

None yet.
