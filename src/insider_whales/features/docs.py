# ruff: noqa: E501
"""Feature dictionary written to `docs/data/features.md` with the share of events that have each."""

from __future__ import annotations

import re

import polars as pl

# group -> (columns shown, regex of the columns, meaning)
FEATURE_DOC: dict[str, list[tuple[str, str, str]]] = {
    "Transaction": [
        (
            "total_value, shares, vwap",
            r"^(total_value|shares|vwap)$",
            "Dollar value, shares and volume-weighted price of the event.",
        ),
        (
            "pct_holdings_change, is_new_position",
            r"^(pct_holdings_change|is_new_position)$",
            "Change in the insider's holdings, only where holdings before the trade can be reconstructed.",
        ),
        (
            "is_officer, is_director, is_ten_percent_owner",
            r"^(is_officer|is_director|is_ten_percent_owner)$",
            "Role classification (`role_norm` has the detail).",
        ),
        (
            "filing_delay_days, filing_delay_sessions",
            r"^filing_delay_",
            "Days and sessions from transaction to filing.",
        ),
        (
            "plan_status",
            r"^plan_status$",
            "10b5-1 plan status; missing information is `unknown`, never discretionary.",
        ),
        (
            "value_to_market_cap, value_to_adv, value_to_history_median",
            r"^value_to_",
            "Size relative to market cap, 20-day dollar volume and the insider's earlier median trade.",
        ),
    ],
    "Insider history (per insider and issuer, earlier public disclosures only)": [
        (
            "n_purchases_12m, n_sales_12m, n_purchases_36m",
            r"^n_(purchases|sales)_\d+m$",
            "Earlier disclosed trades by this insider at this issuer.",
        ),
        (
            "hist_median_value, hist_max_value, n_prior_events",
            r"^(hist_median_value|hist_max_value|n_prior_events)$",
            "Statistics of earlier trades (null when there are none).",
        ),
        (
            "days_since_last_purchase, days_since_last_sale",
            r"^days_since_last_",
            "Days since the last earlier purchase or sale was disclosed.",
        ),
        (
            "trader_type",
            r"^trader_type$",
            "routine (same calendar month in each of the previous 3 years), opportunistic (3+ years of history, no such pattern) or unclassified (not enough history).",
        ),
        (
            "n_mature_observations, hist_mean_signed_return, hist_win_rate",
            r"^(hist_mean_signed_return|hist_win_rate)$",
            "Earlier trades whose 60-session holding period ended before this disclosure: mean return (sales count negative) and share of winners. Null without any such trade.",
        ),
    ],
    "Cluster (7, 14, 30 days; disclosures public by this event's disclosure)": [
        (
            "n_buyers_Nd, n_sellers_Nd",
            r"^n_(buyers|sellers)_\d+d$",
            "Distinct insiders (related reporting entities count once).",
        ),
        (
            "buy_value_Nd, sell_value_Nd, net_value_Nd",
            r"^(buy|sell|net)_value_\d+d$",
            "Dollar values over the same window.",
        ),
        (
            "n_buying_officers_Nd, n_buying_directors_Nd",
            r"^n_buying_(officers|directors)_\d+d$",
            "Distinct buying officers and directors.",
        ),
    ],
    "Market (last bar before the entry session)": [
        (
            "mkt_ret_5, mkt_ret_20, mkt_ret_60, mkt_ret_252",
            r"^mkt_ret_\d+$",
            "Trailing split-adjusted returns.",
        ),
        ("mkt_rel_ret_*", r"^mkt_rel_ret_", "Trailing return minus SPY over the same window."),
        (
            "mkt_drawdown_252, mkt_volatility_60",
            r"^(mkt_drawdown_252|mkt_volatility_60)$",
            "Drawdown from the 252-bar high; annualized volatility of daily log returns.",
        ),
        (
            "mkt_adv_20, mkt_relative_volume",
            r"^(mkt_adv_20|mkt_relative_volume)$",
            "20-day average dollar volume; 20-day over 120-day average volume.",
        ),
        (
            "ret_since_transaction",
            r"^ret_since_transaction$",
            "Entry price over the close on the transaction date, minus one.",
        ),
    ],
    "Fundamentals (SEC XBRL 10-K, usable the day after the filing date)": [
        (
            "revenue, net_income, op_cash_flow, capex, assets, equity, cash, debt",
            r"^(revenue|net_income|op_cash_flow|capex|assets|equity|cash|debt)$",
            "Latest annual values known at disclosure.",
        ),
        (
            "revenue_growth, earnings_growth, free_cash_flow, net_margin, return_on_equity, net_debt, debt_to_equity",
            r"^(revenue_growth|earnings_growth|free_cash_flow|net_margin|return_on_equity|net_debt|debt_to_equity)$",
            "Derived from one filing, so growth never mixes versions; ratios need a positive denominator.",
        ),
        (
            "market_cap, price_to_earnings, price_to_sales, price_to_book, fcf_yield",
            r"^(market_cap|price_to_earnings|price_to_sales|price_to_book|fcf_yield)$",
            "Market cap = Form 4 price x cover-page shares (multi-class issuers undercounted).",
        ),
        (
            "fundamentals_age_days",
            r"^fundamentals_age_days$",
            "Age of the financials in days; financials older than 540 days are treated as missing.",
        ),
    ],
}


def render_feature_doc(table: pl.DataFrame, years: tuple[int, int] = (2015, 2020)) -> str:
    dev = table.filter(pl.col("filing_date").dt.year().is_between(*years))
    lines = [
        "# Event features",
        "",
        "One row per insider event in `data/processed/features/event_features.parquet`, built by "
        "`uv run insider-whales build-features`. Every value uses only information public when the "
        "disclosure became available (acceptance time plus buffers); market features use bars "
        "strictly before the entry session; fundamentals are used from the day after the filing "
        "date. Returns in `event_outcomes.parquet` are computed only inside the development "
        "period (exits after 2020-12-31 are null), so no 2021+ price enters any result.",
        "",
        f"Events: {table.height:,}; development events ({years[0]}-{years[1]}): {dev.height:,}. "
        "The last column is the share of development events with a non-null value (the mean over "
        "the matching columns).",
        "",
    ]
    for group, items in FEATURE_DOC.items():
        lines += [f"## {group}", "", "| Columns | Meaning | Non-null |", "|---|---|---|"]
        for shown, pattern, text in items:
            cols = [c for c in table.columns if re.search(pattern, c)]
            share = (
                "-"
                if not cols
                else f"{100 * dev.select(pl.concat_list([pl.col(c).is_not_null() for c in cols]).list.mean().mean()).item():.0f}%"
            )
            lines.append(f"| `{shown}` | {text} | {share} |")
        lines.append("")
    return "\n".join(lines)
