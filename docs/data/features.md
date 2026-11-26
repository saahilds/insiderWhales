# Event features

One row per insider event in `data/processed/features/event_features.parquet`, built by `uv run insider-whales build-features`. Every value uses only information public when the disclosure became available (acceptance time plus buffers); market features use bars strictly before the entry session; fundamentals are used from the day after the filing date. Returns in `event_outcomes.parquet` are computed only inside the development period (exits after 2020-12-31 are null), so no 2021+ price enters any result.

Events: 571,763; development events (2015-2020): 308,185. The last column is the share of development events with a non-null value (the mean over the matching columns).

## Transaction

| Columns | Meaning | Non-null |
|---|---|---|
| `total_value, shares, vwap` | Dollar value, shares and volume-weighted price of the event. | 100% |
| `pct_holdings_change, is_new_position` | Change in the insider's holdings, only where holdings before the trade can be reconstructed. | 99% |
| `is_officer, is_director, is_ten_percent_owner` | Role classification (`role_norm` has the detail). | 100% |
| `filing_delay_days, filing_delay_sessions` | Days and sessions from transaction to filing. | 100% |
| `plan_status` | 10b5-1 plan status; missing information is `unknown`, never discretionary. | 100% |
| `value_to_market_cap, value_to_adv, value_to_history_median` | Size relative to market cap, 20-day dollar volume and the insider's earlier median trade. | 76% |

## Insider history (per insider and issuer, earlier public disclosures only)

| Columns | Meaning | Non-null |
|---|---|---|
| `n_purchases_12m, n_sales_12m, n_purchases_36m` | Earlier disclosed trades by this insider at this issuer. | 100% |
| `hist_median_value, hist_max_value, n_prior_events` | Statistics of earlier trades (null when there are none). | 90% |
| `days_since_last_purchase, days_since_last_sale` | Days since the last earlier purchase or sale was disclosed. | 49% |
| `trader_type` | routine (same calendar month in each of the previous 3 years), opportunistic (3+ years of history, no such pattern) or unclassified (not enough history). | 100% |
| `n_mature_observations, hist_mean_signed_return, hist_win_rate` | Earlier trades whose 60-session holding period ended before this disclosure: mean return (sales count negative) and share of winners. Null without any such trade. | 48% |

## Cluster (7, 14, 30 days; disclosures public by this event's disclosure)

| Columns | Meaning | Non-null |
|---|---|---|
| `n_buyers_Nd, n_sellers_Nd` | Distinct insiders (related reporting entities count once). | 100% |
| `buy_value_Nd, sell_value_Nd, net_value_Nd` | Dollar values over the same window. | 100% |
| `n_buying_officers_Nd, n_buying_directors_Nd` | Distinct buying officers and directors. | 100% |

## Market (last bar before the entry session)

| Columns | Meaning | Non-null |
|---|---|---|
| `mkt_ret_5, mkt_ret_20, mkt_ret_60, mkt_ret_252` | Trailing split-adjusted returns. | 62% |
| `mkt_rel_ret_*` | Trailing return minus SPY over the same window. | 62% |
| `mkt_drawdown_252, mkt_volatility_60` | Drawdown from the 252-bar high; annualized volatility of daily log returns. | 62% |
| `mkt_adv_20, mkt_relative_volume` | 20-day average dollar volume; 20-day over 120-day average volume. | 62% |
| `ret_since_transaction` | Entry price over the close on the transaction date, minus one. | 63% |

## Fundamentals (SEC XBRL 10-K, usable the day after the filing date)

| Columns | Meaning | Non-null |
|---|---|---|
| `revenue, net_income, op_cash_flow, capex, assets, equity, cash, debt` | Latest annual values known at disclosure. | 77% |
| `revenue_growth, earnings_growth, free_cash_flow, net_margin, return_on_equity, net_debt, debt_to_equity` | Derived from one filing, so growth never mixes versions; ratios need a positive denominator. | 70% |
| `market_cap, price_to_earnings, price_to_sales, price_to_book, fcf_yield` | Market cap = Form 4 price x cover-page shares (multi-class issuers undercounted). | 62% |
| `fundamentals_age_days` | Age of the financials in days; financials older than 540 days are treated as missing. | 91% |
