from __future__ import annotations

from dataclasses import dataclass
from typing import Any


FILTER_KIND = "filter"
TAG_KIND = "tag"


@dataclass(frozen=True, slots=True)
class ReportTaxonomyTermDefinition:
    key: str
    label: str
    kind: str
    parent_key: str | None = None
    description: str = ""
    sort_order: int = 0


REPORT_TAXONOMY_TERMS = (
    ReportTaxonomyTermDefinition(
        key="report",
        label="Report",
        kind=FILTER_KIND,
        description="Longer-form research and analysis outputs.",
        sort_order=10,
    ),
    ReportTaxonomyTermDefinition(
        key="daily",
        label="Daily",
        kind=FILTER_KIND,
        description="Recurring synthesis and daily pipeline output.",
        sort_order=20,
    ),
    ReportTaxonomyTermDefinition(
        key="investment",
        label="Investment",
        kind=FILTER_KIND,
        description="Investment-oriented output and market analysis.",
        sort_order=30,
    ),
    ReportTaxonomyTermDefinition(
        key="semiconductor",
        label="Semiconductor",
        kind=FILTER_KIND,
        description="Chip, foundry, GPU, and semiconductor industry output.",
        sort_order=40,
    ),
    ReportTaxonomyTermDefinition(
        key="deep_research",
        label="Deep Research",
        kind=TAG_KIND,
        parent_key="report",
        description="Long-form or synthesis-heavy research output.",
        sort_order=110,
    ),
    ReportTaxonomyTermDefinition(
        key="arxiv_paper_analysis",
        label="arXiv Paper Analysis",
        kind=TAG_KIND,
        parent_key="report",
        description="Paper-specific review and analysis.",
        sort_order=120,
    ),
    ReportTaxonomyTermDefinition(
        key="comparative_search",
        label="Comparative Search",
        kind=TAG_KIND,
        parent_key="report",
        description="Comparative or benchmark-style research.",
        sort_order=130,
    ),
    ReportTaxonomyTermDefinition(
        key="synthesis",
        label="Synthesis",
        kind=TAG_KIND,
        parent_key="daily",
        description="Daily synthesis output.",
        sort_order=210,
    ),
    ReportTaxonomyTermDefinition(
        key="scrapes",
        label="Scrapes",
        kind=TAG_KIND,
        parent_key="daily",
        description="Scraped-input daily output.",
        sort_order=220,
    ),
)

FILTER_KEYS = {term.key for term in REPORT_TAXONOMY_TERMS if term.kind == FILTER_KIND}
TAG_KEYS = {term.key for term in REPORT_TAXONOMY_TERMS if term.kind == TAG_KIND}

_COMPARATIVE_KEYWORDS = (
    "compare",
    "comparison",
    "comparative",
    "competitive",
    "versus",
    " vs ",
    "benchmark",
    "alternative",
)
_INVESTMENT_KEYWORDS = (
    "investment",
    "investor",
    "earnings",
    "valuation",
    "portfolio",
    "equity",
    "stock",
    "capital allocation",
    "multiple",
)
_SEMICONDUCTOR_KEYWORDS = (
    "semiconductor",
    "chip",
    "chips",
    "gpu",
    "wafer",
    "foundry",
    "fab",
    "fabrication",
    "asml",
    "tsmc",
    "nvidia",
    "broadcom",
)


def normalize_term_keys(keys: list[str] | tuple[str, ...] | set[str] | None, *, kind: str) -> list[str]:
    normalized: list[str] = []
    allowed = FILTER_KEYS if kind == FILTER_KIND else TAG_KEYS
    for raw in keys or []:
        key = str(raw or "").strip().lower()
        if not key:
            continue
        if key not in allowed:
            continue
        if key not in normalized:
            normalized.append(key)
    return sort_term_keys(normalized, kind=kind)


def sort_term_keys(keys: list[str] | set[str], *, kind: str) -> list[str]:
    term_keys = set(keys)
    return [
        term.key
        for term in sorted(REPORT_TAXONOMY_TERMS, key=lambda item: (item.sort_order, item.key))
        if term.kind == kind and term.key in term_keys
    ]


def taxonomy_payload() -> dict[str, list[dict[str, Any]]]:
    filters: list[dict[str, Any]] = []
    tags: list[dict[str, Any]] = []
    for term in sorted(REPORT_TAXONOMY_TERMS, key=lambda item: (item.sort_order, item.key)):
        payload = {
            "key": term.key,
            "label": term.label,
            "kind": term.kind,
            "parent_key": term.parent_key,
            "description": term.description,
            "sort_order": term.sort_order,
        }
        if term.kind == FILTER_KIND:
            filters.append(payload)
        else:
            tags.append(payload)
    return {"filters": filters, "tags": tags}


def classify_report_taxonomy(
    *,
    report_type: str,
    title: str,
    summary: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    report_type_key = str(report_type or "").strip().lower()
    title_text = str(title or "").strip()
    summary_text = str(summary or "").strip()
    metadata_payload = dict(metadata or {})

    filters: set[str] = set()
    tags: set[str] = set()

    taxonomy_metadata = metadata_payload.get("taxonomy")
    if isinstance(taxonomy_metadata, dict):
        filters.update(normalize_term_keys(taxonomy_metadata.get("filters"), kind=FILTER_KIND))
        tags.update(normalize_term_keys(taxonomy_metadata.get("tags"), kind=TAG_KIND))

    if report_type_key == "daily_brief":
        filters.add("daily")
        tags.update({"synthesis", "scrapes"})
    elif report_type_key == "paper_review":
        filters.add("report")
        tags.add("arxiv_paper_analysis")
    elif report_type_key == "substack_draft":
        filters.add("report")
        tags.add("deep_research")
    elif report_type_key:
        filters.add("report")

    search_text = " ".join(
        [
            report_type_key,
            title_text.lower(),
            summary_text.lower(),
            _stringify_metadata(metadata_payload).lower(),
        ]
    )

    if any(keyword in search_text for keyword in _COMPARATIVE_KEYWORDS):
        filters.add("report")
        tags.add("comparative_search")

    if any(keyword in search_text for keyword in _INVESTMENT_KEYWORDS):
        filters.add("investment")

    if any(keyword in search_text for keyword in _SEMICONDUCTOR_KEYWORDS):
        filters.add("semiconductor")

    if not filters:
        filters.add("report")

    return {
        "filter_keys": sort_term_keys(filters, kind=FILTER_KIND),
        "tag_keys": sort_term_keys(tags, kind=TAG_KIND),
    }


def _stringify_metadata(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(f"{key} {_stringify_metadata(item)}" for key, item in value.items())
    if isinstance(value, list):
        return " ".join(_stringify_metadata(item) for item in value)
    return str(value or "")
