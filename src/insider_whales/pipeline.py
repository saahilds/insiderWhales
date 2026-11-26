"""End-to-end pipeline: ordered steps run as separate CLI processes, then the run manifest.

Network steps are resumable and skip work that is already cached. `--offline` runs only the
steps that read local data. A failed step stops the run; the manifest is still written so the
failure is on record.
"""

from __future__ import annotations

import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from insider_whales.config import REPO_ROOT, Settings
from insider_whales.reporting.manifest import digest_files, output_files, write_run_manifest


@dataclass(frozen=True)
class Step:
    name: str
    args: tuple[str, ...]
    network: bool = False


STEPS: tuple[Step, ...] = (
    Step("ingest-sec", ("ingest-sec",), network=True),
    Step("build-universe", ("build-universe",)),
    Step("fetch-prices", ("fetch-prices", "--all"), network=True),
    Step("fetch-benchmarks", ("fetch-benchmarks",), network=True),
    Step("build-fundamentals", ("build-fundamentals",)),
    Step("build-sectors", ("build-sectors",)),
    Step("build-features", ("build-features",)),
    Step("coverage", ("coverage",)),
    Step("event-study", ("event-study",)),
    Step("report", ("report",)),
)

STEP_NAMES = [s.name for s in STEPS]


def select_steps(
    offline: bool = False, start: str | None = None, only: list[str] | None = None
) -> list[Step]:
    """Steps to run, in order. `--offline` drops network steps; `start` skips earlier ones."""
    for name in [*(only or []), *([start] if start else [])]:
        if name not in STEP_NAMES:
            raise ValueError(f"unknown step {name!r}; choose from {', '.join(STEP_NAMES)}")
    steps = list(STEPS)
    if start:
        steps = steps[STEP_NAMES.index(start) :]
    if only:
        steps = [s for s in steps if s.name in only]
    if offline:
        steps = [s for s in steps if not s.network]
    return steps


def run_step(step: Step, config: Path | None) -> dict[str, Any]:
    cmd = [sys.executable, "-m", "insider_whales.cli", *step.args]
    if config:
        cmd += ["--config", str(config)]
    t0 = time.monotonic()
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    return {
        "name": step.name,
        "command": " ".join(["insider-whales", *step.args]),
        "status": "ok" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "seconds": round(time.monotonic() - t0, 1),
        "output_tail": (proc.stdout + proc.stderr).strip().splitlines()[-5:],
    }


def run_pipeline(
    settings: Settings,
    config: Path | None = None,
    offline: bool = False,
    start: str | None = None,
    only: list[str] | None = None,
    say: Callable[[str], None] = print,
) -> tuple[list[dict[str, Any]], Path]:
    started = datetime.now(UTC)
    records: list[dict[str, Any]] = []
    for step in select_steps(offline, start, only):
        say(f"[{step.name}] running")
        rec = run_step(step, config)
        records.append(rec)
        say(f"[{step.name}] {rec['status']} in {rec['seconds']}s")
        if rec["status"] != "ok":
            for line in rec["output_tail"]:
                say(f"    {line}")
            break
    path = write_run_manifest(settings, records, started, command="pipeline")
    return records, path


def hash_outputs(settings: Settings) -> dict[str, str]:
    return digest_files(REPO_ROOT, output_files(settings))


def verify_determinism(
    settings: Settings,
    config: Path | None = None,
    start: str = "build-sectors",
    say: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Run the offline analysis steps twice and compare digests of every output (Test 17)."""
    steps = select_steps(offline=True, start=start)
    digests: list[dict[str, str]] = []
    for attempt in (1, 2):
        for step in steps:
            say(f"[run {attempt}] {step.name}")
            rec = run_step(step, config)
            if rec["status"] != "ok":
                raise RuntimeError(f"step {step.name} failed: {rec['output_tail']}")
        digests.append(hash_outputs(settings))
    first, second = digests
    differing = sorted(k for k in set(first) | set(second) if first.get(k) != second.get(k))
    return {"files": len(first), "differing": differing, "identical": not differing}
