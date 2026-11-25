"""Command-line interface: `uv run insider-whales <command>`."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from insider_whales import __version__
from insider_whales.config import load_settings

app = typer.Typer(
    name="insider-whales",
    help="Insider-trading research pipeline (research only; no live trading).",
    no_args_is_help=True,
    add_completion=False,
)

ConfigOpt = Annotated[
    Path | None, typer.Option("--config", "-c", help="Path to a base YAML config.")
]


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
