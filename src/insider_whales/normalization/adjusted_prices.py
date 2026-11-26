"""Adjusted price table for returns and market features.

`prices_daily` holds raw prices as traded. Returns need split-adjusted prices (so a split is not
a loss) and a total-return index that also credits cash dividends. Both are computed here for
all symbols at once from the raw prices and the recorded corporate actions.
"""

from __future__ import annotations

import polars as pl

from insider_whales.config import Settings
from insider_whales.ingestion.price_runner import prices_dir

ADJUSTED_COLUMNS = [
    "symbol",
    "date",
    "open",
    "close",
    "volume",
    "adj_open",
    "adj_close",
    "adj_volume",
    "tr_index",
    "dollar_volume",
]


def adjust_prices(prices: pl.DataFrame, actions: pl.DataFrame) -> pl.DataFrame:
    """Add split-adjusted prices and a total-return index to raw daily bars.

    - `adj_*`: raw prices divided by the product of later split ratios (current-share basis).
    - `tr_index`: 1.0 at a symbol's first bar; each day compounds
      (close x split ratio today + cash dividend today) / previous close - 1, so a split is
      neutral and a dividend is received on its ex-date and held as cash value.
    - `dollar_volume`: raw close x raw volume.
    """
    splits = (
        actions.filter(pl.col("action") == "split")
        .group_by("symbol", "ex_date")
        .agg(pl.col("ratio").product().alias("split_ratio"))
    )
    divs = (
        actions.filter(pl.col("action") == "cash_dividend")
        .group_by("symbol", "ex_date")
        .agg(pl.col("amount").sum().alias("dividend"))
    )
    p = (
        prices.sort("symbol", "date")
        .join(splits, left_on=["symbol", "date"], right_on=["symbol", "ex_date"], how="left")
        .join(divs, left_on=["symbol", "date"], right_on=["symbol", "ex_date"], how="left")
        .with_columns(pl.col("split_ratio").fill_null(1.0), pl.col("dividend").fill_null(0.0))
    )
    p = p.with_columns(
        pl.col("split_ratio").cum_prod().over("symbol").alias("_cum"),
        pl.col("split_ratio").product().over("symbol").alias("_total"),
    ).with_columns((pl.col("_cum") / pl.col("_total")).alias("_to_current"))
    ret = (pl.col("close") * pl.col("split_ratio") + pl.col("dividend")) / pl.col("close").shift(
        1
    ).over("symbol") - 1
    return (
        p.with_columns(
            (pl.col("open") * pl.col("_to_current")).alias("adj_open"),
            (pl.col("close") * pl.col("_to_current")).alias("adj_close"),
            (pl.col("volume") / pl.col("_to_current")).alias("adj_volume"),
            (pl.col("close") * pl.col("volume")).alias("dollar_volume"),
            ret.fill_null(0.0).alias("_ret"),
        )
        .with_columns((1 + pl.col("_ret")).cum_prod().over("symbol").alias("tr_index"))
        .select(ADJUSTED_COLUMNS)
    )


def build_adjusted_prices(settings: Settings) -> pl.DataFrame:
    d = prices_dir(settings)
    prices = pl.read_parquet(d / "prices_daily.parquet").drop("provider", strict=False)
    actions = pl.read_parquet(d / "corporate_actions.parquet")
    out = adjust_prices(prices, actions)
    out.write_parquet(d / "prices_adjusted.parquet")
    return out
