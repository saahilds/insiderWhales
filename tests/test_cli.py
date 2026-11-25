from typer.testing import CliRunner

from insider_whales import __version__
from insider_whales.cli import app

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_show_config_prints_hash():
    result = runner.invoke(app, ["show-config"])
    assert result.exit_code == 0
    assert "config_hash:" in result.stdout


def test_unimplemented_command_exits_nonzero():
    result = runner.invoke(app, ["backtest"])
    assert result.exit_code == 2
