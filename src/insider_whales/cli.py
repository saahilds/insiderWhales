"""Command-line interface: `uv run insider-whales <command>`."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Annotated

import typer

from insider_whales import __version__
from insider_whales.config import REPO_ROOT, load_settings

app = typer.Typer(
    name="insider-whales",
    help="Insider-trading research pipeline (research only; no live trading).",
    no_args_is_help=True,
    add_completion=False,
)

ConfigOpt = Annotated[
    Path | None, typer.Option("--config", "-c", help="Path to a base YAML config.")
]


QuarterOpt = Annotated[
    list[str] | None, typer.Option("--quarter", "-q", help="Quarter such as 2019q1 (repeatable).")
]
StartOpt = Annotated[str | None, typer.Option(help="First quarter, e.g. 2012q1.")]
EndOpt = Annotated[str | None, typer.Option(help="Last quarter, e.g. 2022q4.")]


@app.callback()
def _main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _not_yet(command: str, milestone: str) -> None:
    typer.secho(f"`{command}` is not implemented yet (planned for {milestone}).", fg="yellow")
    raise typer.Exit(code=2)


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@app.command("show-config")
def show_config(config: ConfigOpt = None) -> None:
    """Print the resolved configuration and its hash."""
    settings = load_settings(config)
    typer.echo(settings.model_dump_json(indent=2))
    typer.echo(f"config_hash: {settings.config_hash()}")


@app.command("download-sec")
def download_sec(
    quarter: QuarterOpt = None,
    start: StartOpt = None,
    end: EndOpt = None,
    config: ConfigOpt = None,
) -> None:
    """Download SEC quarterly insider data sets (cached; unchanged files are skipped)."""
    from insider_whales.ingestion.sec import download_quarters

    if not (quarter or start or end):
        raise typer.BadParameter("give --quarter, or --start/--end")
    settings = load_settings(config)
    results = asyncio.run(download_quarters(settings, quarter, start, end))
    failed = 0
    for r in results:
        typer.echo(f"{r['quarter']}: {r['status']} {r.get('path') or r.get('error', '')}")
        failed += r["status"] == "failed"
    if failed:
        raise typer.Exit(code=1)


@app.command("inspect-sec")
def inspect_sec(
    quarter: QuarterOpt = None,
    write_docs: Annotated[bool, typer.Option(help="Write docs/data/sec-schema.md")] = False,
    config: ConfigOpt = None,
) -> None:
    """Record table schemas of downloaded quarters and report changes across quarters."""
    from insider_whales.ingestion.sec import (
        SchemaRegistry,
        SecDataSets,
        inspect_archive,
        render_schema_markdown,
        schema_changes,
    )

    settings = load_settings(config)
    ds = SecDataSets(settings, client=None)  # type: ignore[arg-type]
    registry = SchemaRegistry(settings.data_dir / "manifests" / "sec_schemas.json")
    wanted = quarter or sorted({r["quarter"] for r in ds.manifest.records()})
    for q in wanted:
        rec = ds.current(q)
        if rec is None:
            typer.secho(f"{q}: not downloaded", fg="red")
            continue
        schema = inspect_archive(Path(rec["path"]))
        registry.record(q, rec["sha256"], schema)
        rows = {t: v["rows"] for t, v in schema["tables"].items()}
        typer.echo(f"{q}: {rows} missing={schema['missing_tables']}")
    data = registry.load()
    for ch in schema_changes(data):
        typer.echo(f"schema change {ch}")
    if write_docs:
        out = REPO_ROOT / "docs" / "data" / "sec-schema.md"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(render_schema_markdown(data))
        typer.echo(f"wrote {out}")


@app.command()
def normalize(quarter: QuarterOpt = None, config: ConfigOpt = None) -> None:
    """Stage downloaded SEC archives and build normalized point-in-time tables."""
    from insider_whales.normalization.pipeline import normalize as run

    counts = run(load_settings(config), quarter)
    for name, n in counts.items():
        typer.echo(f"{name}: {n} rows")


@app.command("acceptance-times")
def acceptance_times(
    quarter: QuarterOpt = None,
    header_fallback_limit: Annotated[int, typer.Option(help="Max per-filing header fetches")] = 200,
    max_age_days: Annotated[int, typer.Option(help="Reuse bulk file younger than this")] = 30,
    config: ConfigOpt = None,
) -> None:
    """Fetch EDGAR acceptance timestamps for staged filings (bulk submissions file)."""
    from insider_whales.normalization.pipeline import build_timestamps

    ts = build_timestamps(
        load_settings(config),
        quarter,
        header_fallback_limit=header_fallback_limit,
        max_age_days=max_age_days,
    )
    for src, n in ts.group_by("acceptance_source").len().sort("acceptance_source").iter_rows():
        typer.echo(f"{src}: {n}")


@app.command("audit-data")
def audit_data(
    quarter: QuarterOpt = None,
    output_dir: Annotated[Path | None, typer.Option(help="Report folder")] = None,
    config: ConfigOpt = None,
) -> None:
    """Reconcile with EDGAR's full-index and write the data-quality report."""
    from insider_whales.evaluation.data_quality import run_audit

    settings = load_settings(config)
    paths = run_audit(settings, quarter, output_dir or settings.reports_dir)
    for p in paths:
        typer.echo(f"wrote {p}")


@app.command("validate-filings")
def validate_filings(
    resample: Annotated[bool, typer.Option(help="Draw a new sample")] = False,
    config: ConfigOpt = None,
) -> None:
    """Compare sampled filings with their original SEC XML and the recorded manual reviews."""
    import json

    from insider_whales.evaluation.filing_validation import run_validation
    from insider_whales.normalization.pipeline import load_processed

    settings = load_settings(config)
    tables = {
        n: load_processed(settings, n)
        for n in ("filings", "reporting_owners", "transactions", "insider_events")
    }
    vdir = REPO_ROOT / "docs" / "validation"
    out = run_validation(
        settings,
        tables,
        vdir / "m1-sample.csv",
        vdir / "m1-reviews.yaml",
        vdir / "m1-filing-validation.md",
        resample=resample,
    )
    dump = settings.data_dir / "processed" / "validation" / "m1_results.json"
    dump.parent.mkdir(parents=True, exist_ok=True)
    dump.write_text(json.dumps(out["results"], indent=2, default=str))
    results = out["results"]
    typer.echo(
        f"{sum(r['parse']['all_match'] for r in results)}/{len(results)} filings match the "
        f"original XML on every check; details in docs/validation/m1-filing-validation.md"
    )


@app.command("download-prices")
def download_prices_cmd(
    provider: Annotated[str, typer.Option(help="yfinance | csv_import")] = "yfinance",
    symbol: Annotated[list[str] | None, typer.Option("--symbol", "-s")] = None,
    start: Annotated[str | None, typer.Option(help="YYYY-MM-DD")] = None,
    end: Annotated[str | None, typer.Option(help="YYYY-MM-DD")] = None,
    config: ConfigOpt = None,
) -> None:
    """Download sample OHLCV prices and write a coverage report (survivor-only on free data)."""
    from datetime import date as date_cls

    from insider_whales.ingestion.prices import (
        build_and_write_security_master,
        coverage_report_markdown,
        download_prices,
        map_sample_prices_to_securities,
    )

    settings = load_settings(config)
    secs, ids = build_and_write_security_master(settings)
    typer.echo(f"securities: {secs.height}; identifiers: {ids.height}")
    result = download_prices(
        settings,
        symbols=symbol,
        provider_name=provider,
        start=date_cls.fromisoformat(start) if start else None,
        end=date_cls.fromisoformat(end) if end else None,
    )
    cov = result["coverage"]
    for status, n in cov.group_by("status").len().sort("status").iter_rows():
        typer.echo(f"{status}: {n}")
    report = coverage_report_markdown(cov, result["capabilities"])
    out = settings.reports_dir / "price_coverage.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report)
    typer.echo(f"wrote {out}")
    try:
        mapped = map_sample_prices_to_securities(settings)
        typer.echo(f"mapped price rows to securities: {mapped.height}")
    except FileNotFoundError:
        pass


@app.command("build-features")
def build_features(config: ConfigOpt = None) -> None:
    """Compute point-in-time features."""
    _not_yet("build-features", "Milestone 2")


@app.command("event-study")
def event_study(config: ConfigOpt = None) -> None:
    """Run the baseline event study."""
    _not_yet("event-study", "Milestone 2")


@app.command()
def backtest(config: ConfigOpt = None) -> None:
    """Run portfolio backtests."""
    _not_yet("backtest", "Milestone 3")


@app.command()
def report(config: ConfigOpt = None) -> None:
    """Build the HTML research report."""
    _not_yet("report", "Milestone 2")


if __name__ == "__main__":
    app()
