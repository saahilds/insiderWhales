"""Report, run manifest, pipeline control flow and determinism (Test 17)."""

import json
from datetime import UTC, datetime

import polars as pl
import pytest

from insider_whales import pipeline
from insider_whales.config import load_settings
from insider_whales.reporting.html_report import build_report, render_report
from insider_whales.reporting.manifest import build_manifest, digest_files, file_digest


@pytest.fixture
def settings(tmp_path):
    return load_settings(
        overrides={
            "paths": {"data_dir": str(tmp_path / "data"), "reports_dir": str(tmp_path / "reports")}
        }
    )


def headline(direction="purchase"):
    rows = []
    for h in (1, 20, 60):
        rows.append(
            {
                "direction": direction,
                "horizon": h,
                "n": 1000 + h,
                "mean": 0.01 * h / 20,
                "median": 0.005,
                "mkt_mean": 0.004,
                "exc_mean": 0.003,
                "exc_ci_low": -0.001,
                "exc_ci_high": 0.007,
            }
        )
    return pl.DataFrame(rows)


def paths(direction="purchase"):
    return pl.DataFrame(
        {
            "direction": direction,
            "day": [1, 5, 20],
            "events": 500,
            "signal": [0.001, 0.01, 0.02],
            "controls": [0.0, 0.005, 0.01],
            "spy": [0.0, 0.004, 0.008],
        }
    )


def subgroups():
    return pl.DataFrame(
        {
            "direction": ["purchase"],
            "horizon": [60],
            "grouping": ["role_norm"],
            "value": ["CEO"],
            "n": [100],
            "mean": [0.05],
            "exc_mean": [0.02],
            "exc_ci_low": [0.0],
            "exc_ci_high": [0.04],
        }
    )


def coverage():
    row = {
        "events": 100,
        "with_prices": 60,
        "coverage": 0.6,
        "full_horizon": 50,
        "full_horizon_coverage": 0.5,
    }
    return {
        "symbols": {"requested": 10, "ok": 6, "missing": 4, "error": 0},
        "overall_conservative": [{"all": "all", **row}],
        "by_year_conservative": [{"year": 2015, **row}],
        "by_mcap_group_conservative": [{"mcap_group": "micro", **row}],
        "reasons_conservative": [
            {"price_status": "ok", "len": 60},
            {"price_status": "delisted_or_acquired_likely", "len": 40},
        ],
    }


def render(settings):
    both = pl.concat([headline("purchase"), headline("sale")])
    return render_report(
        settings,
        None,
        coverage(),
        both,
        both,
        pl.concat([paths("purchase"), paths("sale")]),
        subgroups(),
    )


def test_report_has_every_section_and_the_survivor_only_label(settings):
    text = render(settings)
    for needle in (
        "Exploratory and survivor-only",
        "2. Price coverage",
        "3. Baseline event study",
        "4. Limitations",
        "paths-purchase",
        "role_norm: CEO",
    ):
        assert needle in text
    assert "Data-quality report not available" in text


def test_t17_report_is_byte_identical_for_identical_inputs(settings):
    assert render(settings) == render(settings)


def test_report_builds_from_missing_inputs(settings):
    path = build_report(settings)
    text = path.read_text()
    assert "not available" in text and "Exploratory and survivor-only" in text


def test_t17_digests_ignore_timestamps_and_parquet_layout(tmp_path):
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    p1, p2 = tmp_path / "one.parquet", tmp_path / "two.parquet"
    df.write_parquet(p1, row_group_size=1)
    df.write_parquet(p2, compression="uncompressed")
    assert file_digest(p1) == file_digest(p2)
    df.reverse().write_parquet(p2)
    assert file_digest(p1) != file_digest(p2)

    j1, j2 = tmp_path / "a.json", tmp_path / "b.json"
    j1.write_text(
        json.dumps({"generated_at": "2020-01-01", "x": [1, {"recorded_at": "t", "y": 2}]})
    )
    j2.write_text(
        json.dumps({"generated_at": "2024-06-06", "x": [1, {"recorded_at": "u", "y": 2}]})
    )
    assert file_digest(j1) == file_digest(j2)
    j2.write_text(json.dumps({"generated_at": "2024-06-06", "x": [1, {"y": 3}]}))
    assert file_digest(j1) != file_digest(j2)
    assert set(digest_files(tmp_path, [j1, tmp_path / "missing.csv"])) == {"a.json"}


def test_manifest_records_versions_config_and_steps(settings):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    steps = [{"name": "report", "status": "ok", "seconds": 1.0}]
    m = build_manifest(settings, steps, t0, t0, "pipeline")
    assert m["config_hash"] == settings.config_hash()
    assert m["seed"] == settings.seed
    assert m["steps"] == steps
    assert m["labels"]["final_test_locked"] is True
    assert m["config"]["splits"]["final_test"]
    assert {"python", "polars", "package_version", "git_commit", "started_at"} <= set(m)


def test_step_selection():
    names = [s.name for s in pipeline.select_steps()]
    assert names[0] == "ingest-sec" and names[-1] == "report"
    offline = [s.name for s in pipeline.select_steps(offline=True)]
    assert "ingest-sec" not in offline and "fetch-prices" not in offline
    assert "event-study" in offline
    assert [s.name for s in pipeline.select_steps(start="coverage")] == [
        "coverage",
        "event-study",
        "report",
    ]
    assert [s.name for s in pipeline.select_steps(only=["report", "coverage"])] == [
        "coverage",
        "report",
    ]
    with pytest.raises(ValueError, match="unknown step"):
        pipeline.select_steps(start="nope")


def test_pipeline_stops_at_a_failed_step_and_still_writes_the_manifest(settings, monkeypatch):
    calls = []

    def fake(step, config):
        calls.append(step.name)
        ok = step.name != "coverage"
        return {
            "name": step.name,
            "status": "ok" if ok else "failed",
            "returncode": 0 if ok else 1,
            "seconds": 0.0,
            "output_tail": [],
        }

    monkeypatch.setattr(pipeline, "run_step", fake)
    records, path = pipeline.run_pipeline(
        settings, offline=True, start="build-sectors", say=lambda _: None
    )
    assert calls == ["build-sectors", "build-features", "coverage"]
    assert [r["status"] for r in records] == ["ok", "ok", "failed"]
    assert json.loads(path.read_text())["steps"][-1]["name"] == "coverage"


def test_determinism_check_reports_differing_outputs(settings, monkeypatch):
    runs = iter([{"a": "1", "b": "2"}, {"a": "1", "b": "3"}])
    monkeypatch.setattr(
        pipeline,
        "run_step",
        lambda step, config: {"status": "ok", "name": step.name, "output_tail": []},
    )
    monkeypatch.setattr(pipeline, "hash_outputs", lambda settings: next(runs))
    out = pipeline.verify_determinism(settings, start="report", say=lambda _: None)
    assert out == {"files": 2, "differing": ["b"], "identical": False}
