"""Offline tests for SEC discovery, polite HTTP behaviour, caching and schema inspection."""

import asyncio
import io
import itertools
import zipfile
from pathlib import Path

import httpx
import pytest

from insider_whales.config import load_settings
from insider_whales.ingestion.sec import (
    ArchiveRef,
    SecDataSets,
    inspect_archive,
    parse_archive_links,
    render_schema_markdown,
    schema_changes,
    select_quarters,
)
from insider_whales.io.http import RateLimiter, SecClient, SecRequestError

PAGE = "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"
HTML = """
<a href="/files/structureddata/data/insider-transactions-data-sets/2019q1_form345.zip">2019 Q1</a>
<a href='/files/structureddata/data/insider-transactions-data-sets/2019q2_form345.zip'>2019 Q2</a>
<a href="/files/datastandardsinnovation/data/insider-transactions-data-sets/2026q3_form345.zip">x</a>
<a href="/files/structureddata/data/insider-transactions-data-sets/2019q1_form345.zip">dup</a>
<a href="/files/insider_transactions_readme.pdf">readme</a>
<a href="/files/other_data.zip">other</a>
"""


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    async def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.t += s


def settings(tmp_path: Path, **sec):
    return load_settings(overrides={"paths": {"data_dir": str(tmp_path)}, "sec": sec})


def make_client(tmp_path, handler, **sec):
    clock = FakeClock()
    cfg = settings(tmp_path, **sec)
    client = SecClient(
        cfg.sec,
        "Test (Tester) tester@example.com",
        transport=httpx.MockTransport(handler),
        sleep=clock.sleep,
        clock=clock,
    )
    return cfg, client, clock


def test_discovery_reads_links_including_moved_paths():
    refs = parse_archive_links(HTML, PAGE)
    assert [r.quarter for r in refs] == ["2019q1", "2019q2", "2026q3"]
    assert refs[2].url == (
        "https://www.sec.gov/files/datastandardsinnovation/data/"
        "insider-transactions-data-sets/2026q3_form345.zip"
    )


def test_quarter_selection():
    refs = parse_archive_links(HTML, PAGE)
    assert [r.quarter for r in select_quarters(refs, start="2019q2")] == ["2019q2", "2026q3"]
    assert [r.quarter for r in select_quarters(refs, quarters=["2019Q1"])] == ["2019q1"]
    with pytest.raises(ValueError):
        select_quarters(refs, quarters=["2018q4"])


def test_rate_limiter_spaces_requests():
    clock = FakeClock()
    limiter = RateLimiter(4.0, clock=clock, sleep=clock.sleep)
    starts = []

    async def run():
        for _ in range(9):
            await limiter.acquire()
            starts.append(clock.t)

    asyncio.run(run())
    gaps = [b - a for a, b in itertools.pairwise(starts)]
    assert min(gaps) >= 0.25 - 1e-9
    assert starts[-1] == pytest.approx(2.0)


def test_user_agent_sent_and_429_retry_after_honoured(tmp_path):
    calls = []

    def handler(request: httpx.Request):
        calls.append(request.headers["User-Agent"])
        if len(calls) < 3:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, text="ok")

    _, client, clock = make_client(tmp_path, handler)
    resp = asyncio.run(client.get("https://www.sec.gov/x"))
    assert resp.text == "ok"
    assert calls == ["Test (Tester) tester@example.com"] * 3
    assert clock.sleeps.count(7.0) == 2


def test_backoff_gives_up_and_non_retryable_fails_fast(tmp_path):
    _, client, clock = make_client(
        tmp_path, lambda r: httpx.Response(503), max_retries=3, backoff_base_seconds=1
    )
    with pytest.raises(SecRequestError):
        asyncio.run(client.get("https://www.sec.gov/x"))
    assert client.requests_made == 4
    backoffs = [s for s in clock.sleeps if s > 0.3]
    assert len(backoffs) == 3 and backoffs[0] < backoffs[1] < backoffs[2]

    _, client, _ = make_client(tmp_path, lambda r: httpx.Response(404))
    with pytest.raises(SecRequestError):
        asyncio.run(client.get("https://www.sec.gov/missing"))
    assert client.requests_made == 1


def _zip_bytes(extra_col: bool = False) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        sub = "ACCESSION_NUMBER\tFILING_DATE" + ("\tAFF10B5ONE" if extra_col else "")
        zf.writestr("SUBMISSION.tsv", sub + "\n" + "0001-19-000001\t02-JAN-2019\n")
        zf.writestr("NONDERIV_TRANS.tsv", "ACCESSION_NUMBER\tTRANS_CODE\na\tP\nb\tS\n")
        zf.writestr("FORM_345_metadata.json", "{}")
    return buf.getvalue()


class FakeSec:
    """Mock SEC: one data-sets page, one archive whose Last-Modified can change."""

    def __init__(self):
        self.content = _zip_bytes()
        self.last_modified = "Mon, 01 Apr 2019 00:00:00 GMT"
        self.gets = 0

    def __call__(self, request: httpx.Request):
        if request.url.path.endswith("insider-transactions-data-sets"):
            return httpx.Response(200, text=HTML)
        headers = {"Last-Modified": self.last_modified, "ETag": f'"{len(self.content)}"'}
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        self.gets += 1
        return httpx.Response(200, content=self.content, headers=headers)


def test_download_is_cached_and_new_upstream_versions_are_kept(tmp_path):
    sec = FakeSec()
    cfg, client, _ = make_client(tmp_path, sec)
    ds = SecDataSets(cfg, client)
    ref = ArchiveRef("2019q1", parse_archive_links(HTML, PAGE)[0].url)

    first = asyncio.run(ds.sync(ref))
    assert first["status"] == "downloaded" and Path(first["path"]).exists()
    second = asyncio.run(ds.sync(ref))
    assert second["status"] == "unchanged" and sec.gets == 1

    sec.content = _zip_bytes(extra_col=True)
    sec.last_modified = "Tue, 22 Jul 2025 17:10:51 GMT"
    third = asyncio.run(ds.sync(ref))
    assert third["status"] == "downloaded" and third["new_version_of"] == first["sha256"]
    assert Path(first["path"]).exists() and Path(third["path"]).exists()
    assert ds.current("2019q1")["sha256"] == third["sha256"]


def test_failed_download_is_recorded(tmp_path):
    cfg, client, _ = make_client(tmp_path, lambda r: httpx.Response(500), max_retries=1)
    ds = SecDataSets(cfg, client)
    rec = asyncio.run(ds.sync(ArchiveRef("2019q1", "https://www.sec.gov/f/2019q1_form345.zip")))
    assert rec["status"] == "failed"
    assert ds.manifest.latest("quarter", "2019q1")["status"] == "failed"


def test_inspect_archive_and_schema_changes(tmp_path):
    old, new = tmp_path / "old.zip", tmp_path / "new.zip"
    old.write_bytes(_zip_bytes())
    new.write_bytes(_zip_bytes(extra_col=True))
    a, b = inspect_archive(old), inspect_archive(new)
    assert a["tables"]["NONDERIV_TRANS"]["rows"] == 2
    assert "REPORTINGOWNER" in a["missing_tables"]
    assert a["metadata_member"] == "FORM_345_metadata.json"
    registry = {"2012q1": {"sha256": "x", **a}, "2023q1": {"sha256": "y", **b}}
    changes = schema_changes(registry)
    assert changes == [
        {
            "table": "SUBMISSION",
            "from_quarter": "2012q1",
            "to_quarter": "2023q1",
            "added": ["AFF10B5ONE"],
            "removed": [],
            "reordered": False,
        }
    ]
    assert "| `AFF10B5ONE` | 2023q1 |" in render_schema_markdown(registry)
