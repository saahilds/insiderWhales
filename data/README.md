# data/

Everything in this folder except this README is ignored by Git.

- `raw/`: downloads exactly as received (SEC archives, price pulls). Never modified; a changed upstream file is stored as a new version.
- `staging/`: typed Parquet copies of raw tables, one partition per source file version.
- `processed/`: normalized point-in-time research tables.
- `manifests/`: download and pipeline-step records used for caching and resuming.

Rebuild with the CLI (`uv run insider-whales --help`).
