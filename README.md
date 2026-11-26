# insiderWhales

A reproducible Python research framework that tests whether publicly disclosed US corporate insider purchases and sales (SEC Forms 3, 4 and 5) can produce statistically significant, risk-adjusted excess returns after realistic costs and delays.

This is a research tool only. It does not trade, and negative findings are valid outcomes.

## Status

Milestone 1 (data feasibility) is complete; Milestone 2 (end-to-end prototype on 2012–2022) is in progress. See [PROGRESS.md](PROGRESS.md) for the checklist and [RESEARCH.md](RESEARCH.md) for decisions, data limitations and open issues.

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12.

```bash
uv sync
cp .env.example .env      # then set SEC_USER_AGENT to your name and email
uv run insider-whales --help
uv run pytest             # offline test suite
```

SEC requests are identified with `SEC_USER_AGENT` and limited to at most 5 requests per second, as EDGAR's fair-access policy requires.

## Running the SEC ingest

```bash
uv run insider-whales ingest-sec --start 2012q1 --end 2022q4
```

This downloads, stages, timestamps, normalizes and audits every quarter in the range. It is safe to interrupt (Ctrl-C or kill) and rerun with the same arguments: unchanged downloads are skipped by hash, finished quarters by input fingerprint, and acceptance times are only fetched for filings that do not have one yet. Needs roughly 3 GB of disk and 4 GB of memory.

## Building the security master and downloading prices

```bash
uv run insider-whales build-universe          # security master, universe, candidate symbols
uv run insider-whales fetch-prices --sample 300   # representative subset (deterministic)
uv run insider-whales fetch-prices --all      # every candidate symbol, 2011-01-01 to 2022-12-31
uv run insider-whales coverage                # event-level price status and coverage report
```

`fetch-prices` is resumable: rerun the same command to continue. Symbols with a final outcome (prices written, or Yahoo has no history) are skipped, throttled or timed-out ones are retried, and it stops with exit code 3 if Yahoo keeps throttling. Add `--retry-missing` to ask again for symbols with no history. Progress is in `data/manifests/price_fetch_yfinance.jsonl`; raw responses are cached in `data/raw/prices/yfinance/`. Expect roughly 12,800 symbols and about 1.5 hours at 3 workers on a home connection. If a run slows to a crawl (timeouts of 10 s with no bytes), stop it and rerun; a fresh process recovers.

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
