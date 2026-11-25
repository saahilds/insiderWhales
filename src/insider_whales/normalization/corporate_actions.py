"""Split and dividend arithmetic: raw-price reconstruction, return factors, position updates."""

from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass, replace

import polars as pl


def split_factor_after(dates: pl.Series, splits: pl.DataFrame) -> pl.Series:
    """For each date, the product of split ratios with ex_date strictly after it.

    Multiplying a split-adjusted price by this factor gives the raw price that traded then.
    """
    if splits.height == 0:
        return pl.Series("factor", [1.0] * len(dates))
    ex = splits.sort("ex_date")
    ex_dates = ex["ex_date"].to_list()
    ratios = ex["ratio"].to_list()
    suffix = [1.0] * (len(ratios) + 1)
    for i in range(len(ratios) - 1, -1, -1):
        suffix[i] = suffix[i + 1] * ratios[i]
    return pl.Series("factor", [suffix[bisect_right(ex_dates, d)] for d in dates.to_list()])


def unadjust_split_adjusted(prices: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    """Turn split-adjusted OHLCV (as yfinance returns it) into the raw prices that traded."""
    f = split_factor_after(prices["date"], splits)
    return prices.with_columns(
        *(pl.col(c) * f for c in ("open", "high", "low", "close") if c in prices.columns),
        (pl.col("volume") / f).alias("volume"),
    )


def total_return_index(prices: pl.DataFrame, actions: pl.DataFrame) -> pl.DataFrame:
    """Daily total-return index from raw closes, splits and raw cash dividends.

    r_t = (close_t * split_ratio_t + dividend_t) / close_{t-1} - 1, with the split ratio and
    dividend of actions whose ex_date is t (dividend per pre-split share on that date).
    """
    p = prices.sort("date")
    splits = (
        actions.filter(pl.col("action") == "split")
        .group_by("ex_date")
        .agg(pl.col("ratio").product().alias("split_ratio"))
    )
    divs = (
        actions.filter(pl.col("action") == "cash_dividend")
        .group_by("ex_date")
        .agg(pl.col("amount").sum().alias("dividend"))
    )
    p = (
        p.join(splits, left_on="date", right_on="ex_date", how="left")
        .join(divs, left_on="date", right_on="ex_date", how="left")
        .with_columns(pl.col("split_ratio").fill_null(1.0), pl.col("dividend").fill_null(0.0))
    )
    ret = (pl.col("close") * pl.col("split_ratio") + pl.col("dividend")) / pl.col("close").shift(
        1
    ) - 1
    return (
        p.with_columns(ret.fill_null(0.0).alias("total_return"))
        .with_columns((1 + pl.col("total_return")).cum_prod().alias("tr_index"))
        .select("date", "close", "total_return", "tr_index")
    )


@dataclass(frozen=True)
class Position:
    shares: float
    cost_basis: float  # total cost, unchanged by splits
    cash: float = 0.0

    @property
    def cost_per_share(self) -> float:
        return self.cost_basis / self.shares if self.shares else 0.0


def apply_split(
    pos: Position, ratio: float, price_after: float, allow_fractional: bool = False
) -> Position:
    """Apply a split (ratio = new shares per old share). Fractional shares are paid out as
    cash in lieu at the post-split price unless fractional holdings are allowed."""
    new_shares = pos.shares * ratio
    if allow_fractional:
        return replace(pos, shares=new_shares)
    whole = math.floor(new_shares + 1e-9)
    cash_in_lieu = (new_shares - whole) * price_after
    kept_basis = pos.cost_basis * (whole / new_shares) if new_shares else 0.0
    return Position(shares=whole, cost_basis=kept_basis, cash=pos.cash + cash_in_lieu)


def apply_dividend(pos: Position, amount_per_share: float) -> Position:
    return replace(pos, cash=pos.cash + pos.shares * amount_per_share)


def apply_actions(
    pos: Position, actions: pl.DataFrame, closes: dict, allow_fractional: bool = False
) -> Position:
    """Apply actions in ex_date order; `closes` maps date -> raw close (for cash in lieu)."""
    for row in actions.sort(["ex_date", "action"]).iter_rows(named=True):
        if row["action"] == "split":
            pos = apply_split(pos, row["ratio"], closes.get(row["ex_date"], 0.0), allow_fractional)
        elif row["action"] == "cash_dividend":
            pos = apply_dividend(pos, row["amount"])
    return pos
