"""Standard errors that respect repeated issuers and shared calendar time.

Insider events are not independent: one company produces many events, and events entered in the
same month share the market's moves. The mean's standard error is computed with cluster-robust
sums by issuer and by entry month and combined (Cameron, Gelbach and Miller 2011); the
calendar-time series of monthly means gets a Newey-West error to cover overlapping horizons.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

Z95 = 1.959964


@dataclass(frozen=True)
class MeanEstimate:
    mean: float
    se: float
    n: int
    n_clusters: int

    @property
    def ci(self) -> tuple[float, float]:
        return (self.mean - Z95 * self.se, self.mean + Z95 * self.se)

    @property
    def t_stat(self) -> float:
        return self.mean / self.se if self.se > 0 else float("nan")


def _cluster_sum_sq(resid: np.ndarray, codes: np.ndarray) -> tuple[float, int]:
    sums = np.bincount(codes, weights=resid)
    g = int((np.bincount(codes) > 0).sum())
    meat = float((sums**2).sum())
    return (meat * g / (g - 1) if g > 1 else meat), g


def clustered_mean(
    values: np.ndarray, issuer_codes: np.ndarray, month_codes: np.ndarray
) -> MeanEstimate:
    """Mean with a two-way (issuer, entry month) cluster-robust standard error."""
    x = np.asarray(values, dtype=float)
    n = len(x)
    if n == 0:
        return MeanEstimate(float("nan"), float("nan"), 0, 0)
    mean = float(x.mean())
    if n == 1:
        return MeanEstimate(mean, float("nan"), 1, 1)
    resid = x - mean
    v_issuer, g1 = _cluster_sum_sq(resid, issuer_codes)
    v_month, g2 = _cluster_sum_sq(resid, month_codes)
    both = issuer_codes.astype(np.int64) * (int(month_codes.max()) + 1) + month_codes
    _, both_codes = np.unique(both, return_inverse=True)
    v_both, _ = _cluster_sum_sq(resid, both_codes)
    var = v_issuer + v_month - v_both
    if not var > 0:
        var = max(v_issuer, v_month)
    return MeanEstimate(mean, math.sqrt(var) / n, n, min(g1, g2))


def newey_west_mean(series: np.ndarray, lags: int) -> MeanEstimate:
    """Mean of a time series with a Newey-West (Bartlett kernel) standard error."""
    y = np.asarray(series, dtype=float)
    y = y[~np.isnan(y)]
    t = len(y)
    if t < 3:
        return MeanEstimate(float(y.mean()) if t else float("nan"), float("nan"), t, t)
    mean = float(y.mean())
    e = y - mean
    lags = min(lags, t - 1)
    gamma0 = float(e @ e) / t
    var = gamma0
    for lag in range(1, lags + 1):
        w = 1 - lag / (lags + 1)
        var += 2 * w * float(e[lag:] @ e[:-lag]) / t
    var = max(var, 0.0)
    return MeanEstimate(mean, math.sqrt(var / t), t, t)


def winsorize(values: np.ndarray, lower: float = 0.01, upper: float = 0.99) -> np.ndarray:
    lo, hi = np.quantile(values, [lower, upper])
    return np.clip(values, lo, hi)
