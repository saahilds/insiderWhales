"""Original Form 3/4/5 XML documents from EDGAR (used to validate the bulk data sets)."""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

from insider_whales.config import Settings
from insider_whales.io.cache import RawStore
from insider_whales.io.http import SecClient

log = logging.getLogger(__name__)
RAW_SOURCE = "sec/filing_xml"


@dataclass
class XmlLine:
    security_title: str | None
    trans_date: str | None
    trans_code: str | None
    shares: float | None
    price: float | None
    acq_disp: str | None
    shares_after: float | None
    direct_indirect: str | None
    footnote_ids: list[str] = field(default_factory=list)


@dataclass
class XmlOwner:
    cik: str
    name: str | None
    is_director: bool
    is_officer: bool
    is_ten_pct: bool
    is_other: bool
    officer_title: str | None


@dataclass
class OwnershipDocument:
    document_type: str | None
    period_of_report: str | None
    issuer_cik: str | None
    issuer_symbol: str | None
    aff10b5one: bool | None
    owners: list[XmlOwner]
    nonderiv: list[XmlLine]
    deriv_count: int
    footnotes: dict[str, str]
    remarks: str | None


def _text(el: ET.Element | None, path: str) -> str | None:
    if el is None:
        return None
    node = el.find(path)
    if node is None or node.text is None:
        return None
    return node.text.strip() or None


def _num(s: str | None) -> float | None:
    try:
        return float(s) if s is not None else None
    except ValueError:
        return None


def _flag(s: str | None) -> bool:
    return (s or "").strip().lower() in {"1", "true"}


def parse_ownership_xml(xml: str | bytes) -> OwnershipDocument:
    root = ET.fromstring(xml)
    owners = []
    for ro in root.findall("reportingOwner"):
        rel = ro.find("reportingOwnerRelationship")
        owners.append(
            XmlOwner(
                cik=(_text(ro, "reportingOwnerId/rptOwnerCik") or "").zfill(10),
                name=_text(ro, "reportingOwnerId/rptOwnerName"),
                is_director=_flag(_text(rel, "isDirector")),
                is_officer=_flag(_text(rel, "isOfficer")),
                is_ten_pct=_flag(_text(rel, "isTenPercentOwner")),
                is_other=_flag(_text(rel, "isOther")),
                officer_title=_text(rel, "officerTitle"),
            )
        )
    lines = []
    table = root.find("nonDerivativeTable")
    for tx in table.findall("nonDerivativeTransaction") if table is not None else []:
        lines.append(
            XmlLine(
                security_title=_text(tx, "securityTitle/value"),
                trans_date=_text(tx, "transactionDate/value"),
                trans_code=_text(tx, "transactionCoding/transactionCode"),
                shares=_num(_text(tx, "transactionAmounts/transactionShares/value")),
                price=_num(_text(tx, "transactionAmounts/transactionPricePerShare/value")),
                acq_disp=_text(tx, "transactionAmounts/transactionAcquiredDisposedCode/value"),
                shares_after=_num(
                    _text(tx, "postTransactionAmounts/sharesOwnedFollowingTransaction/value")
                ),
                direct_indirect=_text(tx, "ownershipNature/directOrIndirectOwnership/value"),
                footnote_ids=sorted({f.get("id") for f in tx.iter("footnoteId") if f.get("id")}),
            )
        )
    deriv = root.find("derivativeTable")
    aff = _text(root, "aff10b5One")
    return OwnershipDocument(
        document_type=_text(root, "documentType"),
        period_of_report=_text(root, "periodOfReport"),
        issuer_cik=(_text(root, "issuer/issuerCik") or "").zfill(10),
        issuer_symbol=_text(root, "issuer/issuerTradingSymbol"),
        aff10b5one=None if aff is None else _flag(aff),
        owners=owners,
        nonderiv=lines,
        deriv_count=len(deriv.findall("derivativeTransaction")) if deriv is not None else 0,
        footnotes={f.get("id"): " ".join((f.text or "").split()) for f in root.iter("footnote")},
        remarks=_text(root, "remarks"),
    )


async def fetch_ownership_xml(
    settings: Settings, client: SecClient, issuer_cik: str, accession: str
) -> tuple[Path, str]:
    """Download (once) the primary XML of a filing; returns (path, url)."""
    store = RawStore(settings.data_dir / "raw")
    folder = store.root / RAW_SOURCE / accession
    cached = sorted(folder.glob("*.xml")) if folder.exists() else []
    if cached:
        return cached[0], (folder / "url.txt").read_text().strip()
    base = f"{settings.sec.archives_base}/{int(issuer_cik)}/{accession.replace('-', '')}/"
    listing = (await client.get(base + "index.json")).json()
    names = [i["name"] for i in listing["directory"]["item"]]
    xmls = [n for n in names if n.lower().endswith(".xml") and "summary" not in n.lower()]
    if not xmls:
        raise RuntimeError(f"no XML document in {base}")
    url = base + xmls[0]
    tmp = store.scratch_path(RAW_SOURCE, accession, xmls[0])
    result = await client.download(url, tmp)
    path = store.ingest(tmp, RAW_SOURCE, accession, xmls[0], result.sha256)
    (folder / "url.txt").write_text(url)
    return path, url
