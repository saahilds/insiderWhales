"""Cluster features: how many independent insiders traded the same company recently.

A window ends at the disclosure time of the event it describes and only contains disclosures
that were public by then, so a cluster is first visible at the second qualifying disclosure.
Related reporting entities are one insider (`insider_group_id`).
"""

from __future__ import annotations

import numpy as np
import polars as pl

WINDOWS = (7, 14, 30)
OFFICER_ROLES = ("CEO", "CFO", "OtherOfficer")


def _window_aggs(days: int) -> list[pl.Expr]:
    inside = pl.col("_t_other") > (pl.col("_t") - pl.duration(days=days))
    buy = pl.col("_code") == "P"
    sell = pl.col("_code") == "S"
    officer = pl.col("_role").is_in(OFFICER_ROLES)
    director = pl.col("_role") == "Director"
    return [
        pl.col("_insider").filter(inside & buy).n_unique().alias(f"n_buyers_{days}d"),
        pl.col("_insider").filter(inside & sell).n_unique().alias(f"n_sellers_{days}d"),
        pl.col("_value").filter(inside & buy).sum().alias(f"buy_value_{days}d"),
        pl.col("_value").filter(inside & sell).sum().alias(f"sell_value_{days}d"),
        pl.col("_insider")
        .filter(inside & buy & officer)
        .n_unique()
        .alias(f"n_buying_officers_{days}d"),
        pl.col("_insider")
        .filter(inside & buy & director)
        .n_unique()
        .alias(f"n_buying_directors_{days}d"),
    ]


def _window_pairs(events: pl.DataFrame, days: int) -> tuple[np.ndarray, np.ndarray]:
    """Index pairs (event, earlier-or-simultaneous event of the same issuer within `days`).

    `events` must be sorted by issuer and `available_at`; ranges come from a binary search over
    a combined (issuer, time) key, so only events inside the window are ever paired.
    """
    issuer = events["issuer_cik"].rank("dense").to_numpy().astype("int64")
    seconds = events["available_at"].dt.epoch("s").to_numpy().astype("int64")
    key = (issuer << 34) + seconds
    end = np.searchsorted(key, key, side="right")
    start = np.searchsorted(key, key - days * 86400, side="right")
    counts = end - start
    left = np.repeat(np.arange(len(key)), counts)
    offsets = np.arange(counts.sum()) - np.repeat(np.cumsum(counts) - counts, counts)
    right = np.repeat(start, counts) + offsets
    return left, right


def cluster_features(events: pl.DataFrame) -> pl.DataFrame:
    """Rolling 7/14/30-day cluster statistics per event, from public disclosures only."""
    ev = events.select(
        "event_id",
        "issuer_cik",
        "available_at",
        "insider_group_id",
        "trans_code",
        "role_norm",
        pl.col("value").fill_null(0.0).alias("value"),
    ).sort("issuer_cik", "available_at", "event_id")
    chunk = 400_000
    parts = []
    longest = max(WINDOWS)
    left_all, right_all = _window_pairs(ev, longest)
    for i in range(0, ev.height, chunk):
        sel = (left_all >= i) & (left_all < i + chunk)
        left, right = left_all[sel], right_all[sel]
        pairs = pl.DataFrame(
            {
                "event_id": ev["event_id"].gather(left),
                "_t": ev["available_at"].gather(left),
                "_t_other": ev["available_at"].gather(right),
                "_insider": ev["insider_group_id"].gather(right),
                "_code": ev["trans_code"].gather(right),
                "_role": ev["role_norm"].gather(right),
                "_value": ev["value"].gather(right),
            }
        )
        parts.append(pairs.group_by("event_id").agg(*[a for w in WINDOWS for a in _window_aggs(w)]))
    agg = pl.concat(parts)
    out = events.select("event_id").join(agg, on="event_id", how="left")
    for w in WINDOWS:
        out = out.with_columns(
            (pl.col(f"buy_value_{w}d") - pl.col(f"sell_value_{w}d")).alias(f"net_value_{w}d")
        )
    return out
