# insiderWhales

A reproducible Python research framework that tests whether publicly disclosed US corporate insider purchases and sales (SEC Forms 3, 4 and 5) can produce statistically significant, risk-adjusted excess returns after realistic costs and delays.

This is a research tool only. It does not trade, and negative findings are valid outcomes.

## Status

Milestone 1 (data feasibility) is in progress. See [PROGRESS.md](PROGRESS.md) for the checklist and [RESEARCH.md](RESEARCH.md) for decisions, data limitations and open issues.

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12.

```bash
uv sync
cp .env.example .env      # then set SEC_USER_AGENT to your name and email
uv run insider-whales --help
uv run pytest             # offline test suite
```

SEC requests are identified with `SEC_USER_AGENT` and limited to at most 5 requests per second, as EDGAR's fair-access policy requires.

## Layout

```
configs/                 YAML configuration (base, strategies, watchlist)
data/                    raw/, staging/, processed/, manifests/ (git-ignored)
src/insider_whales/      package: ingestion, normalization, features, strategies,
                         backtesting, evaluation, reporting, io, cli
tests/                   offline unit and integration tests on synthetic data
docs/                    schema notes and validation records
reports/                 generated reports (git-ignored)
```

## Data limitations

Price data is free (yfinance), which has no history for delisted or acquired companies. Every result built on it is labelled **exploratory / survivor-only** and is not suitable for final profitability conclusions. Details are in [RESEARCH.md](RESEARCH.md).
