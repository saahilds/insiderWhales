# Progress

Checklist by milestone. Each item lands in its own PR; tests named "T#" refer to the 18 mandatory tests in RESEARCH.md.

## Milestone 1: Data feasibility

- [x] PR 1: scaffold (uv project, Ruff, pytest, Typer CLI, configs, CI)
- [x] PR 2: trading calendar, availability timestamps, execution timing, point-in-time joins (T1, T2, T3, T17 start)
- [x] PR 3: SEC ingestion: index discovery, rate-limited client, cache and manifests, one development quarter, schema docs
- [x] PR 4: staging and normalization: owners, insider groups, classification, plan status, amendments, events (T5, T6, T7, T8)
- [x] PR 5: acceptance timestamps, full-index reconciliation, data-quality report v1
- [x] PR 6: manual validation of at least 30 filings against the original SEC XML
- [x] PR 7: price provider interface, yfinance adapter, security master v0, sample price coverage (T9 unit, T10, T11)

## Milestone 2: End-to-end prototype (2012–2022)

- [ ] PR 8: full SEC ingestion 2012–2022
- [ ] PR 9: security master and prices at scale, coverage report
- [ ] PR 10: fundamentals (SEC XBRL) and features (T4, T12, T13)
- [ ] PR 11: event study engine and baseline results on development data (T18)
- [ ] PR 12: reporting v1, run manifest, end-to-end pipeline (T17)

## Milestone 3: Strategy research

- [ ] PR 13: strategy interface, S0–S3
- [ ] PR 14: S4–S7
- [ ] PR 15: portfolio simulator (T9 integration, T14, T15, T16)
- [ ] PR 16: benchmarks, factor regressions, placebo tests
- [ ] PR 17: sensitivity grid, experiment registry, FDR
- [ ] PR 18: validation run and freeze (check-in with Saahil before freezing)

## Milestone 4: Out-of-sample evaluation

- [ ] PR 19: ingest 2023–2026Q3, run frozen strategies once, final report (exploratory / survivor-only)

## Milestone 5: Recommendation

- [ ] PR 20: S8 combined signal, PASS/PROMISING/INCONCLUSIVE/FAIL classification, viability write-up
