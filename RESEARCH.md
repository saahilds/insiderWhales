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
| T9 | Split-adjusted position accounting | done (`tests/test_prices.py`) |
| T10 | Delisted stocks not silently removed | done (`tests/test_prices.py`) |
| T11 | Recycled tickers do not merge companies | done (`tests/test_prices.py`) |
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

- **Research sets.** *Conservative*: code P or S, non-derivative, common equity, price > 0, shares > 0, plausible transaction date (0–730 days before filing), open-market class, filed by an officer or director, original filing (not an amendment), no equity swap, and a real reported ticker (not `NONE`/`N/A`). *Sensitivity*: P or S, non-derivative, common/ADR/unclassified title, valid price, shares and date, original filing, any owner role, any class except compensation, option-related or gift.
- **Transaction class** for P/S lines comes from footnotes attached to transaction columns (not holding columns, which often mention awards that were not traded): option-related, then compensation (ESPP, DRIP, 401(k), tax withholding, …), then private/offering, then open market *confirmed* (weighted-average or open-market wording, unless negated as in "not a weighted average") or *probable* (no contrary evidence).
- **Exercise-and-sale.** A sale is option-related when shares acquired by exercise or conversion (M, X, C, O) in the same filing on the same date cover at least 80% of the shares sold that day. Added after the manual review found same-day exercise-and-sell filings without any explanatory footnote.
- **Plan status** per line: `scheduled` (footnote or remarks mention a 10b5-1 plan, or the checkbox is ticked on a filing from 2023-04-01 on), `not_scheduled_reported` (explicitly negated, or checkbox present and unticked from 2023-04-01 on), otherwise `unknown`.
- **Insider groups.** Owner CIKs that co-file for the same issuer form one insider (`<issuer_cik>:<smallest CIK>`); computed point-in-time with `as_of`.
- **Amendments** are linked to originals in tiers (high/medium/low/unmatched) because the data sets only carry the filer-entered original date, which is sometimes wrong.
- **Events** aggregate the lines of one filing by insider group, security, date and code. Prior holdings are rebuilt per ownership form; `pct_holdings_change` is null when that is unreliable.

### 2019Q1 sample (development period)

67,537 filings → 150,478 transaction lines (107,842 non-derivative) → 16,027 insider events (5,581 purchases, 10,446 sales). Conservative set: 4,895 P lines and 15,476 S lines. Among non-derivative P lines: 5,690 open market confirmed, 6,472 probable, 385 compensation, 128 private, 133 unknown price. Amendments: 975 high, 82 medium and 8 low-confidence links; 656 unmatched, of which 631 amend filings from earlier quarters that are not loaded. One issuer (Hyster-Yale, family members buying under 10b5-1 plans) accounts for 6,125 of the quarter's P lines; aggregation into events and the insider-group logic keep it from dominating counts.

## Acceptance timestamps and data quality (PR 5)

- Acceptance times come from EDGAR's nightly bulk `submissions.zip` (1.58 GB, downloaded at most every 30 days): first the issuer's submission files, then the reporting owners', then up to 200 per-run `-index-headers.html` lookups. `acceptanceDateTime` in the JSON is true UTC (checked against the New York-time header).
- 2019Q1 and 2023Q3: all 104,573 filings got a timestamp from the issuers' files; no fallback was needed. In 2019Q1 the acceptance date (New York time) always equals the filing date.
- 2019Q1 timing: 1,422 filings accepted before 09:10 ET (can trade at that day's open), 16,911 during the session, 49,204 after 16:00 ET.
- Full-index reconciliation, 2019Q1: 67,537 accessions in both the data set and EDGAR's full-index, none missing on either side.
- Other 2019Q1 quality numbers: no duplicate keys; 630 identical lines within a filing (legitimate repeated lots, kept); 63 transaction dates after the filing date and a transaction date range of 2003–2029 (typos, flagged `bad_transaction_date`); officer title missing on 48% of P/S lines (directors and 10% owners have none); price missing on 0.25%; 71% of P/S lines carry a transaction footnote; median filing lag 2 days, 30% filed more than 2 days after the trade.
- `uv run insider-whales audit-data` writes `reports/data_quality.json` and `reports/data_quality.md`.

## Manual validation (PR 6)

- 31 filings drawn by a seeded, stratified sampler from 2019Q1 and 2023Q3: open-market purchases (confirmed and probable), private/offering purchases, compensation-plan purchases, 10b5-1 sales, plan-unknown sales, tax/option-related sales, 4/A amendments, multi-owner filings, indirect holdings, a 10% owner, a Form 5, and 2023 filings with the checkbox ticked and unticked. Sample: `docs/validation/m1-sample.csv`.
- **Parsing:** each filing's original XML was downloaded from EDGAR and compared with the parsed rows: 318 of 318 field checks match (document type, issuer, ticker, 10b5-1 checkbox, owner set and role flags, every non-derivative line's title, date, code, shares, price within the data set's two-decimal rounding, A/D, shares after, ownership form, footnote references).
- **Classification:** reviewed by reading footnotes and remarks (`docs/validation/m1-reviews.yaml`). The first pass disagreed on 5 of 31 filings. Three were pipeline errors and are fixed: two exercise-and-sell filings (Twilio, Inspire Medical) classed as open-market sales, and a "not a weighted average" footnote (Great Elm) read as open-market confirmation. The two remaining disagreements (Muzinich BDC, Thrivent Church Loan & Income Fund) are purchases of non-listed funds that are almost certainly direct subscriptions; nothing in the filing text says so. Both are already outside the conservative set (10% owner or other relationship; placeholder or fund ticker), and the security master in PR 7 drops issuers without an exchange listing. Final agreement: 29 of 31.
- Report: `docs/validation/m1-filing-validation.md`, regenerated by `uv run insider-whales validate-filings` (original XML is cached in the raw store).

## Prices and security master (PR 7)

- Provider interface (`MarketDataProvider`) with declared capabilities. Adapters: `yfinance` (smoke/exploration) and `csv_import` (generic hook for any better data set later). No paid adapters.
- yfinance OHLC is split-adjusted even with `auto_adjust=False`; raw prices are rebuilt from split events. Verified: NVDA close on 2024-06-07 reconstructs to ~$1,208.88 (not $120.89).
- Security master v0: `security_id = <issuer_cik>:<share class>`, ticker validity windows from Form 4 history, overrides in `configs/security_overrides.yaml` (GOOG/GOOGL, FB→META). Recycled tickers cannot attach a price series that spans two companies' windows (T11).
- Milestone 1 sample (72 symbols, 2018–2024): **52 with prices, 20 missing**. All probe delisted/acquired names are missing: SIVB, FRC, TWTR, ATVI, XLNX, CELG, BBBY, BBBYQ. Several Form 4 tickers from small/OTC issuers are also missing. Every result is labelled survivor-only.
- Report: `reports/price_coverage.md`. Mandatory tests T9 (split position accounting + raw-price rebuild), T10 (missing prices recorded, not dropped), T11 (recycled tickers stay separate).

## Full SEC ingestion 2012–2022 (PR 8)

- **Run:** `uv run insider-whales ingest-sec --start 2012q1 --end 2022q4` on the cloud VM. Downloads 44 quarterly archives (about 0.5 GB), stages them, resolves acceptance times from the cached `submissions.zip`, normalizes, writes the drift report and the audit. About 5 minutes end to end once the bulk file is cached; peak memory about 4 GB.
- **Volume:** 2,306,892 filings (2,047,720 Form 4, 169,584 Form 3, 35,025 Form 5, 54,563 amendments) → 4,841,455 transaction lines → 571,763 insider events. Conservative events: 162,278 purchases and 288,421 sales (2012–2022). Purchase events per year range from 17k to 24k, sales from 25k to 39k (2021 is the high).
- **Reconciliation:** all 44 quarters match EDGAR's full-index. Five accessions (2015Q4: 1, 2019Q3: 2, 2022Q2: 2) are in the data sets but not in the index; nothing is missing the other way.
- **Acceptance times:** 2,306,886 of 2,306,892 filings from the bulk file (2 of them through the per-filing header fallback; 4 have none and fall back to the end of the filing date, flagged by `availability_basis`). 56,699 filings were accepted before 09:10 ET, 662,619 during the session and 1,587,570 after 16:00 ET. 252 filings were accepted on a later New York date than their filing date; availability always uses the acceptance time, so this does not matter for look-ahead.
- **Schema drift:** no column was added or removed between 2012Q1 and 2022Q4 in any table (only `AFF10B5ONE` appears, in the 2023 data). The value-level check compares each quarter with the same calendar quarter of earlier years (filing mix is strongly seasonal) and flags null-rate jumps of more than 5 points, date-parse failures above 0.1% and unseen category values. It flagged only transaction codes `E` (1 line) and `H` (15) first appearing in 2012Q2; neither is P or S. No date failed to parse in 2012–2022. Details: `docs/data/sec-drift.md`; column ranges: `docs/data/sec-schema.md`.
- **Data oddities carried forward:** 1,154 transaction dates after the filing date and a raw transaction-date range of year 0012 to 2047 (typos, flagged `bad_transaction_date`); 8,644 identical lines within a filing (repeated lots, kept); officer title is missing on 42% of P/S lines (directors and 10% owners); price is missing on 0.41%; 63% of P/S lines carry a transaction footnote.
- **Memory and resumption:** Filings, owners and insider groups need all quarters at once (amendments and group links cross quarter boundaries); transaction lines and events are built one source quarter at a time into `data/processed/sec/_parts/` and stitched at the end. Each part is keyed by a fingerprint of all inputs (archive hashes, availability settings, timestamps file). Resumption was tested by killing a run with SIGKILL after 8 of 44 quarters; the rerun skipped those 8, built the other 36 and produced the same totals. Acceptance timestamps are only fetched for accessions not yet resolved. The first attempt at normalizing everything in one pass was killed by the 16 GB memory limit, which motivated the per-quarter design.

## Security master and free prices at scale (PR 9)

- **Reported tickers (QADA/QADB fix):** `normalization/tickers.py` cleans the free-text symbol box of Form 3/4/5 (`QADA, QADB`, `MOGA/MOGB`, `NRCIA/B`, `NYSE: SCS`, `(CALX)`, placeholders) into tickers with an implied share class. QADA and QADB are now Class A and Class B of QAD Inc. (`0001036188:A`, `0001036188:B`) instead of one garbled symbol.
- **Ticker windows:** each issuer has one track per share class (or per reported position when no class is implied); a track's runs of the same ticker become validity windows. A window starts 365 days before the first filing that used the ticker (first window only), ends the day before the track's next ticker, and, for an issuer that stopped filing, ends 400 days after its last filing, so a ticker later reused by another company cannot be matched to the old issuer's events. Result: 12,197 securities, 19,169 identifiers.
- **Universe:** the 9,872 securities with at least one insider event; 4,281 of them are in the SEC's current `company_tickers_exchange.json`. That file also gives the current ticker for issuers that renamed (e.g. ANTM is ELV today); it is tried as a second candidate symbol. Candidate symbols to download: 12,764.
- **Download:** `fetch-prices --all` ran on the cloud VM: all 12,764 symbols, 0 errors left, about 1.5 hours for the first pass plus 30 minutes for the re-download described below. Yahoo did not block us (no 429 streak) but the Python process slowed to timeouts after 1,500–2,000 symbols; restarting the (resumable) command fixed it each time. **Result: 5,277 symbols have prices (11.6 million daily bars, 2011-01-01 to 2022-12-31, 106,934 split and dividend actions); 7,487 have no history at Yahoo** (re-asking for a random 150 of them found none). The same command is documented in the README for a full re-run on a local machine.
- **Raw prices need splits after the window.** Yahoo's close is adjusted for every split up to today. Rebuilding the traded price only works if the download also covers the splits after 2022, so requests run to 2026-12-31 and everything dated after 2022-12-31 is dropped before storage. The first full download requested up to 2022-12-31 only and was wrong for any company that split later (WMT, ANET, NOW, PANW, ...): 8.2% of events failed the price-consistency check, which is how it was found. After the fix the same check flags 1.5%.
- **Price-consistency check:** an event's Form 4 price must lie within 0.8 x low and 1.25 x high of that day's raw bar. A series is rejected when at least 3 events were checked and fewer than half are consistent. This rejects recycled tickers and bad provider split data (e.g. RGR and SITC have bars a constant factor away from the filed prices); 266 of 5,201 series with events were rejected.
- **Market cap:** no free point-in-time market cap exists. The proxy is the Form 4 price times the cover-page shares outstanding from SEC XBRL `companyfacts` (`dei:EntityCommonStockSharesOutstanding`, latest value filed on or before the filing date, at most 800 days old). Multi-class issuers are undercounted (the facts file does not say which class a value belongs to) and 17.8% of events have no value at all (`unknown`). Groups: micro < $300M, small $300M-2B, mid $2-10B, large $10-200B, mega > $200B.
- **Event-level price status (`insider-whales coverage`):** the symbol for an event comes from the ticker window covering the filing date (highest confidence first), with the issuer's current ticker as a fallback. Entry session = first executable session after `available_at`. Every event gets `ok` or exactly one reason; table and definitions: `docs/data/price-coverage.md`. The per-event table is `data/processed/prices/event_prices.parquet`.
- **Coverage (conservative set, filing years 2012-2022, with the 2015-2020 development window at 56-74%):** 64.3% of events have a usable price, 61.1% with a full 120-session horizon. Coverage rises with market cap (micro 56%, small 57%, mid 69%, large 85%, mega 88%) and over time (49% in 2012 to 81% in 2022), and purchases are covered less than sales (61% vs 66%). Without a price: 18.6% of events are issuers that stopped filing before 2022 and are not listed today (delisted or acquired), 15.6% are issuers not listed today that still filed in 2022 (OTC, foreign or deregistered). **This is the survivor bias of free data: the missing events are concentrated in companies that failed, were acquired or moved off exchange, which is where insider purchases are most informative.** Every result built on this data is survivor-only and exploratory.

## Fundamentals and point-in-time features (PR 10)

- **Fundamentals source:** SEC `companyfacts` bulk archive (1.4 GB, free, one request). Per issuer: cover-page shares outstanding (`dei:EntityCommonStockSharesOutstanding`) and annual (10-K family) values of revenue, net income, operating cash flow, capex, assets, liabilities, equity, cash and debt, each with the accession and filing date of the filing that reported it. 8,426 of 9,452 issuers have annual financials (1.9 million facts). Quarterly figures are not used (no trailing-twelve-month logic), so the newest financials can be up to a year old; anything older than 540 days is treated as missing.
- **Availability rule:** a financial value is usable from the day after its filing date (strictly later, since EDGAR filing dates are New York dates and the acceptance time is not in the facts file). A restatement is a new filing and only counts from its own filing date. Growth rates compare the current and prior year *as reported in the same filing*, so a restated base never mixes with an old numerator; a late amendment of an older fiscal year cannot replace newer figures (the selection moves only forward in fiscal year). Test T12 covers before/same-day/after filing, restatement, staleness and the amendment case.
- **Market cap** (proxy, no free point-in-time source): Form 4 price times the latest cover-page share count known before the disclosure (at most 800 days old). Multi-class issuers are undercounted and 18% of events have no share count. The coverage report and the features use the same function.
- **Insider history** is per insider and issuer (`insider_group_id`) and ordered by when each disclosure became public; only strictly earlier disclosures count, so two lines of one filing never feed each other. Historical profitability uses the 60-session return of earlier trades and counts a trade only if its exit date is before the day of the new signal (T13). A sale counts as right when the price falls. Routine versus opportunistic follows the academic definition (same calendar month in each of the previous three years); with less than three earlier years the insider is `unclassified`, never opportunistic: 17,934 routine, 148,757 opportunistic and 405,072 unclassified events.
- **Clusters** use rolling 7/14/30-day windows ending at each event's own disclosure time and contain only disclosures public by then, so a cluster first appears at the second qualifying disclosure, not on the trade dates (T4). Related reporting entities are one insider through `insider_group_id`; repeat purchases by one insider count once.
- **Market features** come from the last bar before the entry session. Split-adjusted prices and a total-return index (cash dividends credited on the ex-date) are built for all symbols (`prices_adjusted.parquet`); market-relative momentum uses SPY (benchmark ETFs including IWM, MDY and the sector SPDRs are in `benchmarks_adjusted.parquet` for PR 11). Sector-relative momentum is not available yet: sector needs a classification (SIC codes from SEC submissions are the free option) and is planned for PR 11 with the sector-adjusted returns.
- **No returns after 2020-12-31.** `event_outcomes.parquet` holds forward returns (horizons 1, 5, 10, 20, 60, 120 sessions from the first executable open) computed with all price reads cut off at the end of the development period: an exit after 2020-12-31 is null and events entering later have no return. Events from the validation period still get features (computed from earlier prices) but no outcomes. As a result, only 37% of 2020 events have a 120-session return.
- **Coverage of features** (development events 2015-2020; `docs/data/features.md`): transaction, history and cluster features exist for all events; market features for 62% (events with a usable price series and enough history), fundamentals for 77% (70% for the growth and margin ratios), market cap for 62%.

## Baseline event study (PR 11)

Full tables: `docs/results/event-study-baseline.md`. **Exploratory and survivor-only; development period 2015-2020 only, no 2021+ returns.**

- **Sample:** 157,197 conservative-set open-market events with a price series that passed the Form 4 consistency check (54,698 purchases, 102,499 sales; 3,390 issuers), entry at the first executable open, exit at the close `h` sessions later on the total-return index. Later horizons have fewer events because exits after 2020-12-31 are null.
- **Controls v1:** the 3 nearest same-sector stocks of the insider universe by 60-day return, 60-day volatility, 252-day drawdown and 20-day dollar volume on the session before entry, with no insider disclosure in the previous 90 days and at least $100k daily dollar volume. Sector is the current SIC code mapped to a SPDR sector ETF (coarse; XLRE and XLC only exist from 2015-10 and 2018-06). Every return is also reported minus SPY, minus the sector ETF and minus a size ETF (IWM, MDY or SPY).
- **Inference:** two-way cluster-robust (issuer and entry month) confidence intervals on event means, and a calendar-time view with Newey-West errors on monthly means. Events cluster heavily, so the effective sample is far smaller than the event count.
- **Purchases (all events):** mean return minus matched controls is +0.5% at 1 session, +1.2% at 20 and +2.2% at 60 (95% CI 0.7% to 3.6%), +2.8% at 120; calendar-time t-statistics of 3.8 at 20 and 3.2 at 60 sessions. Median raw returns are much smaller than means (60 sessions: 2.3% median, 6.7% mean), so the average is driven by a minority of large winners. Most of the effect sits in small, illiquid stocks: restricted to tradable events (price at least $5, 20-day dollar volume at least $2M) the 60-session excess is +0.7% (CI -0.05% to 1.5%) and the 120-session excess is about zero.
- **Sales:** raw mean returns after sales are positive (3.6% at 60 sessions) because the stock market and survivors rose; against controls the 60-session excess is +1.3% (CI -0.1% to 2.6%), not distinguishable from zero for all events, and among tradable events it is positive (+1.2% at 60 sessions, CI 0.6% to 1.9%; +2.0% at 120). There is no evidence in this sample that insider sales predict relative underperformance; the positive sign is most likely the survivor bias (sellers are mostly holders of companies that kept rising and kept trading), not a signal.
- **Reading these numbers:** the long-horizon results are inflated by survivor-only prices, since delisted and acquired issuers (about a third of events have no price) are absent, and no delisting returns exist. No costs are applied. Subgroup tables are descriptive with no multiple-testing correction.
- **Known simplifications:** SIC is not point in time; controls are matched on dollar volume rather than market cap and do not exclude other recent insider activity beyond 90 days; the sector-relative momentum feature promised in PR 10 is not built (sector-adjusted returns are).

## Open issues

- **Two non-listed fund purchases are still classed open-market probable** (Muzinich BDC, Thrivent Church Loan & Income Fund; see Manual validation). The filing text cannot distinguish them from open-market purchases. They stay out of the conservative set (10% owner / placeholder or fund ticker) and the security master drops issuers without an exchange listing, so they never reach returns. Accepted as is.
- The validation sample (31 filings) checks the rules but is too small to measure precision and recall of each footnote rule; a larger labelled sample would be needed for that.
- Sales after a small same-day exercise (e.g. 1,299 exercised, 13,310 sold) stay open-market sales; the 80% coverage threshold is a judgement call.
- Group links are formed only by co-filing; related owners who never co-file (e.g. a spouse filing separately) stay separate insiders.
