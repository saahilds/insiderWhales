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
| T5 | Repeat purchases by one insider are not independent insiders | done (`tests/test_normalization.py`) |
| T6 | Amendments cannot change previously available information | done (`tests/test_normalization.py`) |
| T7 | Option exercises and tax dispositions excluded | done (`tests/test_normalization.py`) |
| T8 | Missing plan information is not "discretionary" | done (`tests/test_normalization.py`) |
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

## Normalization rules (PR 4)

- **Research sets.** *Conservative*: code P or S, non-derivative, common equity, price > 0, shares > 0, plausible transaction date (0–730 days before filing), open-market class, filed by an officer or director, original filing (not an amendment), no equity swap. *Sensitivity*: P or S, non-derivative, common/ADR/unclassified title, valid price, shares and date, original filing, any owner role, any class except compensation, option-related or gift.
- **Transaction class** for P/S lines comes from footnotes attached to transaction columns (not holding columns, which often mention awards that were not traded): option-related, then compensation (ESPP, DRIP, 401(k), tax withholding, …), then private/offering, then open market *confirmed* (weighted-average or open-market wording) or *probable* (no contrary evidence).
- **Plan status** per line: `scheduled` (footnote or remarks mention a 10b5-1 plan, or the checkbox is ticked on a filing from 2023-04-01 on), `not_scheduled_reported` (explicitly negated, or checkbox present and unticked from 2023-04-01 on), otherwise `unknown`.
- **Insider groups.** Owner CIKs that co-file for the same issuer form one insider (`<issuer_cik>:<smallest CIK>`); computed point-in-time with `as_of`.
- **Amendments** are linked to originals in tiers (high/medium/low/unmatched) because the data sets only carry the filer-entered original date, which is sometimes wrong.
- **Events** aggregate the lines of one filing by insider group, security, date and code. Prior holdings are rebuilt per ownership form; `pct_holdings_change` is null when that is unreliable.

### 2019Q1 sample (development period)

67,537 filings → 150,478 transaction lines (107,842 non-derivative) → 16,027 insider events (5,581 purchases, 10,446 sales). Conservative set: 4,895 P lines and 15,476 S lines. Among non-derivative P lines: 5,690 open market confirmed, 6,472 probable, 385 compensation, 128 private, 133 unknown price. Amendments: 975 high, 82 medium and 8 low-confidence links; 656 unmatched, of which 631 amend filings from earlier quarters that are not loaded. One issuer (Hyster-Yale, family members buying under 10b5-1 plans) accounts for 6,125 of the quarter's P lines; aggregation into events and the insider-group logic keep it from dominating counts.

## Open issues

- Footnote-rule precision and recall are measured on a hand-validated sample in PR 6.
- Group links are formed only by co-filing; related owners who never co-file (e.g. a spouse filing separately) stay separate insiders.
