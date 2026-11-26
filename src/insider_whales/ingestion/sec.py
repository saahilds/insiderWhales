"""SEC insider-transaction data sets: discovery, cached download and schema inspection."""

from __future__ import annotations

import asyncio
import io
import itertools
import json
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from insider_whales.config import Settings, sec_user_agent
from insider_whales.io.cache import RawStore, sha256_file
from insider_whales.io.http import SecClient, SecRequestError
from insider_whales.io.manifest import Manifest

log = logging.getLogger(__name__)

ARCHIVE_RE = re.compile(r"(?P<year>\d{4})q(?P<q>[1-4])_form345\.zip$", re.IGNORECASE)
HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)

TABLES = (
    "SUBMISSION",
    "REPORTINGOWNER",
    "NONDERIV_TRANS",
    "NONDERIV_HOLDING",
    "DERIV_TRANS",
    "DERIV_HOLDING",
    "FOOTNOTES",
    "OWNER_SIGNATURE",
)
RAW_SOURCE = "sec/form345"


@dataclass(frozen=True, order=True)
class ArchiveRef:
    quarter: str  # e.g. "2019q1"
    url: str


def parse_quarter(q: str) -> tuple[int, int]:
    m = re.fullmatch(r"(\d{4})q([1-4])", q.strip().lower())
    if not m:
        raise ValueError(f"bad quarter {q!r}; expected e.g. 2019q1")
    return int(m.group(1)), int(m.group(2))


def parse_archive_links(html: str, base_url: str) -> list[ArchiveRef]:
    """Quarterly archive links found on the data sets page (URLs are never constructed)."""
    found: dict[str, str] = {}
    for href in HREF_RE.findall(html):
        m = ARCHIVE_RE.search(href)
        if not m:
            continue
        quarter = f"{m.group('year')}q{m.group('q')}"
        url = urljoin(base_url, href)
        if quarter in found and found[quarter] != url:
            raise ValueError(f"conflicting links for {quarter}: {found[quarter]} vs {url}")
        found[quarter] = url
    return sorted(ArchiveRef(q, u) for q, u in found.items())


def select_quarters(
    refs: list[ArchiveRef],
    quarters: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[ArchiveRef]:
    if quarters:
        wanted = {f"{y}q{q}" for y, q in map(parse_quarter, quarters)}
        missing = wanted - {r.quarter for r in refs}
        if missing:
            raise ValueError(f"quarters not listed on the SEC page: {sorted(missing)}")
        return [r for r in refs if r.quarter in wanted]
    lo = parse_quarter(start) if start else (0, 0)
    hi = parse_quarter(end) if end else (9999, 4)
    return [r for r in refs if lo <= parse_quarter(r.quarter) <= hi]


class SecDataSets:
    """Downloads quarterly archives into the raw store, recording every attempt."""

    def __init__(self, settings: Settings, client: SecClient):
        self.settings = settings
        self.client = client
        self.store = RawStore(settings.data_dir / "raw")
        self.manifest = Manifest(settings.data_dir / "manifests" / "sec_form345_downloads.jsonl")

    async def discover(self) -> list[ArchiveRef]:
        page = self.settings.sec.data_sets_page
        resp = await self.client.get(page)
        refs = parse_archive_links(resp.text, page)
        if not refs:
            raise RuntimeError(f"no quarterly archives found on {page}; page layout changed?")
        snap = self.settings.data_dir / "raw" / "sec" / "index_pages"
        snap.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        (snap / f"data_sets_page_{stamp}.html").write_text(resp.text, encoding="utf-8")
        return refs

    def current(self, quarter: str) -> dict[str, Any] | None:
        """Latest successfully stored version of a quarter, if its file is intact."""
        rec = self.manifest.latest("quarter", quarter, statuses={"downloaded", "unchanged"})
        if rec and Path(rec["path"]).exists():
            return rec
        return None

    async def sync(self, ref: ArchiveRef, *, verify_hash: bool = True) -> dict[str, Any]:
        filename = ref.url.rsplit("/", 1)[-1]
        prior = self.current(ref.quarter)
        base = {"quarter": ref.quarter, "url": ref.url}
        try:
            if prior and (not verify_hash or sha256_file(Path(prior["path"])) == prior["sha256"]):
                head = await self.client.head(ref.url)
                same = (
                    head.headers.get("Last-Modified") == prior.get("last_modified")
                    and head.headers.get("ETag") == prior.get("etag")
                    and prior["url"] == ref.url
                )
                if same:
                    return self.manifest.append({**base, **_keep(prior), "status": "unchanged"})
            tmp = self.store.scratch_path(RAW_SOURCE, ref.quarter, filename)
            result = await self.client.download(ref.url, tmp)
            final = self.store.ingest(tmp, RAW_SOURCE, ref.quarter, filename, result.sha256)
            return self.manifest.append(
                {
                    **base,
                    "status": "downloaded",
                    "path": str(final),
                    "sha256": result.sha256,
                    "size": result.size,
                    "last_modified": result.last_modified,
                    "etag": result.etag,
                    "new_version_of": prior["sha256"] if prior else None,
                }
            )
        except (SecRequestError, OSError) as e:
            log.error("download failed for %s: %s", ref.quarter, e)
            return self.manifest.append({**base, "status": "failed", "error": str(e)})


def _keep(rec: dict[str, Any]) -> dict[str, Any]:
    return {k: rec.get(k) for k in ("path", "sha256", "size", "last_modified", "etag")}


async def download_quarters(
    settings: Settings,
    quarters: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    client: SecClient | None = None,
) -> list[dict[str, Any]]:
    own = client is None
    client = client or SecClient(settings.sec, sec_user_agent(), seed=settings.seed)
    try:
        ds = SecDataSets(settings, client)
        refs = select_quarters(await ds.discover(), quarters, start, end)
        results = await asyncio.gather(*(ds.sync(r) for r in refs))
        return sorted(results, key=lambda r: r["quarter"])
    finally:
        if own:
            await client.aclose()


# --- schema inspection --------------------------------------------------------------------


def _table_role(member: str) -> str | None:
    stem = Path(member).stem.upper()
    return stem if stem in TABLES else None


def inspect_archive(path: Path) -> dict[str, Any]:
    """Tables, columns and row counts of one quarterly archive."""
    out: dict[str, Any] = {"tables": {}, "other_members": []}
    with zipfile.ZipFile(path) as zf:
        for info in zf.infolist():
            role = _table_role(info.filename)
            if role is None:
                out["other_members"].append(info.filename)
                if info.filename.lower().endswith(".json"):
                    out["metadata_member"] = info.filename
                continue
            with zf.open(info) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
                header = text.readline().rstrip("\r\n").split("\t")
                rows = sum(1 for _ in text)
            out["tables"][role] = {"member": info.filename, "columns": header, "rows": rows}
    out["missing_tables"] = [t for t in TABLES if t not in out["tables"]]
    return out


class SchemaRegistry:
    """Per-quarter schemas, so column additions/removals across quarters are visible."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> dict[str, Any]:
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def record(self, quarter: str, sha256: str, schema: dict[str, Any]) -> None:
        data = self.load()
        data[quarter] = {"sha256": sha256, **schema}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, indent=2, sort_keys=True))


def schema_changes(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Column differences between consecutive quarters present in the registry."""
    changes = []
    quarters = sorted(registry, key=parse_quarter)
    for prev, cur in itertools.pairwise(quarters):
        for table in TABLES:
            a = registry[prev]["tables"].get(table, {}).get("columns")
            b = registry[cur]["tables"].get(table, {}).get("columns")
            if a is None or b is None or a == b:
                continue
            changes.append(
                {
                    "table": table,
                    "from_quarter": prev,
                    "to_quarter": cur,
                    "added": [c for c in b if c not in a],
                    "removed": [c for c in a if c not in b],
                    "reordered": set(a) == set(b),
                }
            )
    return changes


def _presence_ranges(quarters: list[str], present: list[bool]) -> str:
    runs, start = [], None
    for i, (q, here) in enumerate(zip(quarters, present, strict=True)):
        if here and start is None:
            start = q
        if start is not None and (not here or i == len(quarters) - 1):
            end = q if here else quarters[i - 1]
            runs.append(start if start == end else f"{start}–{end}")
            start = None
    return ", ".join(runs) or "never"


def render_schema_markdown(registry: dict[str, Any]) -> str:
    """Schema document. With many quarters, columns are shown as the quarter ranges they exist in
    and row counts are totalled per year, so the file stays readable at 44 quarters."""
    quarters = sorted(registry, key=parse_quarter)
    lines = [
        "# SEC insider transaction data sets: observed schemas",
        "",
        "Generated by `uv run insider-whales inspect-sec --write-docs` from the archives in the "
        "local raw store. Do not edit the generated sections by hand.",
        "",
        f"Quarters inspected ({len(quarters)}): {', '.join(quarters)}.",
        "",
        "## Data lines per table, by year (excluding header lines)",
        "",
    ]
    years = sorted({parse_quarter(q)[0] for q in quarters})
    lines += ["| Table | " + " | ".join(str(y) for y in years) + " |"]
    lines.append("|---|" + "---|" * len(years))
    for t in TABLES:
        cells = []
        for y in years:
            qs = [q for q in quarters if parse_quarter(q)[0] == y]
            vals = [registry[q]["tables"].get(t, {}).get("rows") for q in qs]
            cells.append(
                f"{sum(v for v in vals if v is not None):,}"
                if any(v is not None for v in vals)
                else "missing"
            )
        lines.append(f"| {t} | " + " | ".join(cells) + " |")
    lines += ["", "## Columns and the quarters they exist in", ""]
    for t in TABLES:
        cols_by_q = {q: registry[q]["tables"].get(t, {}).get("columns") for q in quarters}
        union: list[str] = []
        for cols in cols_by_q.values():
            for c in cols or []:
                if c not in union:
                    union.append(c)
        lines += [f"### {t}", "", "| Column | Present in |", "|---|---|"]
        for c in union:
            present = [c in (cols_by_q[q] or []) for q in quarters]
            lines.append(
                f"| `{c}` | {'all' if all(present) else _presence_ranges(quarters, present)} |"
            )
        lines.append("")
    changes = schema_changes(registry)
    lines += ["## Changes between inspected quarters", ""]
    if not changes:
        lines.append("None.")
    for ch in changes:
        lines.append(
            f"- {ch['table']} {ch['from_quarter']} → {ch['to_quarter']}: "
            f"added {ch['added'] or 'none'}, removed {ch['removed'] or 'none'}"
            + (" (column order only)" if ch["reordered"] else "")
        )
    lines.append("")
    return "\n".join(lines)
