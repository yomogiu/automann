from __future__ import annotations

from dataclasses import dataclass
from email.utils import parsedate_to_datetime
import html
import json
import re
from datetime import datetime, timedelta, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import httpx
from libs.config import Settings
from libs.contracts.events import EventName
from libs.contracts.models import (
    AdapterResult,
    EventSuggestion,
    ObservationRecord,
    PaperReviewRequest,
    ReportRecord,
    WorkerStatus,
)
from libs.contracts.workers import (
    ArxivFeedIngestOutput,
    ArxivFeedIngestRequest,
    NewsIngestOutput,
    NewsIngestRequest,
)
from libs.db import LifeRepository, engine_for_url

from workers.codex_runner import CodexCliRequest, CodexCliRunner, load_paper_review_prompt
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text

try:  # pragma: no cover - optional dependency path reused from artifact ingest
    from workers.artifact_ingest_runner.runner import ArtifactIngestRunner
except Exception:  # pragma: no cover - fallback normalization retained below
    ArtifactIngestRunner = None


NEWS_USER_AGENT = "automann-daily-brief/0.1"
DEFAULT_NEWS_WINDOW_HOURS = 36
DEFAULT_NEWS_ITEMS_PER_SOURCE = 5
DEFAULT_NEWS_MAX_ITEMS = 12


DEFAULT_NEWS_ITEMS = [
    {
        "source": "synthetic",
        "headline": "Local AI infrastructure costs continue shifting toward storage-heavy deployments",
        "url": "https://example.invalid/news/storage-heavy-deployments",
        "topic": "infrastructure",
    },
    {
        "source": "synthetic",
        "headline": "Open-source orchestration stacks converge on Python-native workflow definitions",
        "url": "https://example.invalid/news/python-native-workflows",
        "topic": "orchestration",
    },
]


@dataclass(slots=True)
class TrackedNewsSource:
    name: str
    url: str
    feed_url: str | None = None
    topic: str | None = None
    limit: int | None = None


def _coerce_positive_int(value: Any, *, default: int | None = None) -> int | None:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    if normalized <= 0:
        return default
    return normalized


def _source_name_for_url(url: str) -> str:
    hostname = urlparse(url).netloc.strip()
    return hostname or url


def _clean_text(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "<" in text and ">" in text:
        text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _parse_timestamp(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        parsed = None
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tag_name(value: str) -> str:
    return value.rsplit("}", 1)[-1].lower()


def _first_child_text(element: ET.Element, name: str) -> str | None:
    for child in element.iter():
        if child is element:
            continue
        if _tag_name(child.tag) != name.lower():
            continue
        text = "".join(child.itertext()).strip()
        if text:
            return text
    return None


def _feed_entry_link(entry: ET.Element, *, fallback_url: str) -> str:
    for child in entry:
        if _tag_name(child.tag) != "link":
            continue
        href = str(child.attrib.get("href") or "").strip()
        if href:
            return href
        text = "".join(child.itertext()).strip()
        if text:
            return text
    return fallback_url


def _item_is_recent_enough(item: dict[str, Any], *, not_before: datetime) -> bool:
    published_at = _parse_timestamp(str(item.get("published_at") or "").strip())
    if published_at is None:
        return True
    return published_at >= not_before


def _news_sort_key(item: dict[str, Any]) -> tuple[datetime, str]:
    published_at = _parse_timestamp(str(item.get("published_at") or "").strip()) or datetime.min.replace(tzinfo=timezone.utc)
    headline = str(item.get("headline") or "")
    return (published_at, headline)

DEFAULT_ARXIV_ITEMS = [
    {
        "paper_id": "2503.00001",
        "title": "Event-Driven Research Agents for Persistent Local Knowledge Systems",
        "summary": "Introduces a pragmatic architecture for combining scheduled ingestion with retrieval-backed drafting.",
        "authors": ["Ada Researcher", "Lin Systems"],
    },
    {
        "paper_id": "2503.00002",
        "title": "Durable Human-in-the-Loop Pipelines for Long-Horizon Analysis",
        "summary": "Explores operator approvals, auditability, and persistent workflow state in mixed automation systems.",
        "authors": ["Morgan Workflow"],
    },
]


class NewsScrapeRunner:
    worker_key = "news_scrape_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: NewsIngestRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        metadata = dict(request.metadata)
        source_failures: list[dict[str, str]] = []
        if request.seed_news:
            items = [self._normalize_news_item(item, fallback_source="seed") for item in request.seed_news]
        else:
            tracked_sources = self._tracked_sources_from_metadata(metadata)
            if tracked_sources:
                items, source_failures = self._collect_news_from_sources(
                    tracked_sources=tracked_sources,
                    brief_date=request.brief_date,
                    metadata=metadata,
                )
            else:
                items = [self._normalize_news_item(item, fallback_source="synthetic") for item in DEFAULT_NEWS_ITEMS]
        output = NewsIngestOutput(
            generated_at=datetime.now(timezone.utc),
            items=items,
            count=len(items),
        )
        payload = output.model_dump(mode="json")
        artifact_path = run_dir / "news_ingest.json"
        write_json(artifact_path, payload)
        observations = [
            ObservationRecord(
                kind="news_item",
                summary=item["headline"],
                payload=item,
                confidence=0.4,
            )
            for item in items
        ]
        observations.extend(
            ObservationRecord(
                kind="news_source_error",
                summary=f"{failure['source']}: {failure['error']}",
                payload=failure,
                confidence=0.7,
            )
            for failure in source_failures
        )
        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"Ingested {len(items)} news items.",
            artifact_manifest=[
                build_file_artifact(
                    kind="news-feed",
                    path=artifact_path,
                    media_type="application/json",
                    metadata={"count": len(items)},
                )
            ],
            structured_outputs=payload,
            observations=observations,
            next_suggested_events=[
                EventSuggestion(name=EventName.NEWS_INGEST_COMPLETED, payload={"count": len(items)})
            ],
        )

    def _collect_news_from_sources(
        self,
        *,
        tracked_sources: list[TrackedNewsSource],
        brief_date,
        metadata: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        max_items = _coerce_positive_int(metadata.get("max_news_items"), default=DEFAULT_NEWS_MAX_ITEMS)
        window_hours = _coerce_positive_int(metadata.get("news_window_hours"), default=DEFAULT_NEWS_WINDOW_HOURS)
        not_before = datetime.combine(brief_date, datetime.min.time(), tzinfo=timezone.utc) - timedelta(hours=window_hours)
        collected: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        seen_keys: set[tuple[str, str]] = set()

        for tracked_source in tracked_sources:
            try:
                source_items = self._fetch_source_items(
                    tracked_source=tracked_source,
                    not_before=not_before,
                    limit=tracked_source.limit or DEFAULT_NEWS_ITEMS_PER_SOURCE,
                )
            except Exception as exc:
                failures.append({"source": tracked_source.name, "error": str(exc)})
                continue

            for item in source_items:
                dedupe_key = (
                    str(item.get("url") or "").strip().lower(),
                    str(item.get("headline") or "").strip().lower(),
                )
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                collected.append(item)

        collected.sort(key=_news_sort_key, reverse=True)
        return collected[:max_items], failures

    def _fetch_source_items(
        self,
        *,
        tracked_source: TrackedNewsSource,
        not_before: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        if tracked_source.feed_url:
            return self._fetch_feed_items(
                tracked_source=tracked_source,
                feed_url=tracked_source.feed_url,
                not_before=not_before,
                limit=limit,
            )

        response = self._get(tracked_source.url)
        content_type = str(response.headers.get("content-type") or "").lower()
        if self._looks_like_feed(content_type=content_type, body=response.text):
            return self._parse_feed_items(
                tracked_source=tracked_source,
                feed_url=str(response.url),
                body=response.text,
                not_before=not_before,
                limit=limit,
            )

        discovered_feed_url = self._discover_feed_url(page_url=str(response.url), html_body=response.text)
        if discovered_feed_url:
            return self._fetch_feed_items(
                tracked_source=tracked_source,
                feed_url=discovered_feed_url,
                not_before=not_before,
                limit=limit,
            )
        return self._parse_html_items(
            tracked_source=tracked_source,
            page_url=str(response.url),
            html_body=response.text,
            limit=limit,
        )

    def _fetch_feed_items(
        self,
        *,
        tracked_source: TrackedNewsSource,
        feed_url: str,
        not_before: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        response = self._get(feed_url)
        return self._parse_feed_items(
            tracked_source=tracked_source,
            feed_url=str(response.url),
            body=response.text,
            not_before=not_before,
            limit=limit,
        )

    def _parse_feed_items(
        self,
        *,
        tracked_source: TrackedNewsSource,
        feed_url: str,
        body: str,
        not_before: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        try:
            root = ET.fromstring(body)
        except ET.ParseError as exc:
            raise ValueError(f"could not parse feed XML: {exc}") from exc

        entries: list[dict[str, Any]] = []
        if _tag_name(root.tag) == "rss":
            channel = next((child for child in root if _tag_name(child.tag) == "channel"), None)
            if channel is None:
                return []
            for item in channel:
                if _tag_name(item.tag) != "item":
                    continue
                normalized = self._feed_item_to_news(
                    tracked_source=tracked_source,
                    entry=item,
                    fallback_url=feed_url,
                )
                if normalized is None or not _item_is_recent_enough(normalized, not_before=not_before):
                    continue
                entries.append(normalized)
        else:
            for entry in root.iter():
                if _tag_name(entry.tag) != "entry":
                    continue
                normalized = self._feed_item_to_news(
                    tracked_source=tracked_source,
                    entry=entry,
                    fallback_url=feed_url,
                )
                if normalized is None or not _item_is_recent_enough(normalized, not_before=not_before):
                    continue
                entries.append(normalized)
        entries.sort(key=_news_sort_key, reverse=True)
        return entries[:limit]

    def _feed_item_to_news(
        self,
        *,
        tracked_source: TrackedNewsSource,
        entry: ET.Element,
        fallback_url: str,
    ) -> dict[str, Any] | None:
        title = _clean_text(_first_child_text(entry, "title"))
        if not title:
            return None
        published_raw = (
            _first_child_text(entry, "pubDate")
            or _first_child_text(entry, "published")
            or _first_child_text(entry, "updated")
        )
        published_at = _parse_timestamp(published_raw)
        return self._normalize_news_item(
            {
                "source": tracked_source.name,
                "headline": title,
                "url": _feed_entry_link(entry, fallback_url=fallback_url),
                "summary": _clean_text(
                    _first_child_text(entry, "description")
                    or _first_child_text(entry, "summary")
                    or _first_child_text(entry, "content")
                ),
                "topic": tracked_source.topic,
                "published_at": published_at.isoformat() if published_at is not None else None,
                "source_url": tracked_source.url,
                "feed_url": tracked_source.feed_url or fallback_url,
            },
            fallback_source=tracked_source.name,
        )

    def _parse_html_items(
        self,
        *,
        tracked_source: TrackedNewsSource,
        page_url: str,
        html_body: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html_body, "html.parser")
        candidates: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        page_host = urlparse(page_url).netloc.lower()
        selectors = [
            "article a[href]",
            "main a[href]",
            "a[href*='/news/']",
            "a[href*='/blog/']",
            "a[href*='/posts/']",
        ]
        for selector in selectors:
            for anchor in soup.select(selector):
                href = str(anchor.get("href") or "").strip()
                if not href:
                    continue
                absolute_url = urljoin(page_url, href)
                if urlparse(absolute_url).netloc.lower() != page_host:
                    continue
                if absolute_url in seen_urls:
                    continue
                headline = _clean_text(anchor.get_text(" ", strip=True))
                if len(headline) < 12:
                    continue
                seen_urls.add(absolute_url)
                candidates.append(
                    self._normalize_news_item(
                        {
                            "source": tracked_source.name,
                            "headline": headline,
                            "url": absolute_url,
                            "topic": tracked_source.topic,
                            "source_url": tracked_source.url,
                        },
                        fallback_source=tracked_source.name,
                    )
                )
                if len(candidates) >= limit:
                    return candidates
        return candidates

    def _discover_feed_url(self, *, page_url: str, html_body: str) -> str | None:
        soup = BeautifulSoup(html_body, "html.parser")
        for link in soup.select("link[rel~='alternate'][href]"):
            mime_type = str(link.get("type") or "").lower()
            if "rss" not in mime_type and "atom" not in mime_type and "xml" not in mime_type:
                continue
            href = str(link.get("href") or "").strip()
            if href:
                return urljoin(page_url, href)
        return None

    def _tracked_sources_from_metadata(self, metadata: dict[str, Any]) -> list[TrackedNewsSource]:
        raw_sources = metadata.get("news_sources")
        if raw_sources is None:
            raw_sources = metadata.get("news_sites")
        if not isinstance(raw_sources, list):
            return []

        tracked_sources: list[TrackedNewsSource] = []
        for item in raw_sources:
            if isinstance(item, str):
                url = item.strip()
                if not url:
                    continue
                tracked_sources.append(TrackedNewsSource(name=_source_name_for_url(url), url=url))
                continue
            if not isinstance(item, dict):
                continue
            source_url = str(item.get("url") or item.get("site_url") or "").strip()
            feed_url = str(item.get("feed_url") or "").strip() or None
            if not source_url and feed_url:
                source_url = feed_url
            if not source_url:
                continue
            tracked_sources.append(
                TrackedNewsSource(
                    name=str(item.get("name") or item.get("source") or _source_name_for_url(source_url)).strip(),
                    url=source_url,
                    feed_url=feed_url,
                    topic=str(item.get("topic") or "").strip() or None,
                    limit=_coerce_positive_int(item.get("limit")),
                )
            )
        return tracked_sources

    def _normalize_news_item(self, item: dict[str, Any], *, fallback_source: str) -> dict[str, Any]:
        normalized = dict(item)
        normalized["source"] = str(normalized.get("source") or fallback_source).strip() or fallback_source
        normalized["headline"] = _clean_text(str(normalized.get("headline") or "").strip()) or "Untitled headline"
        normalized["url"] = str(normalized.get("url") or "").strip()
        normalized["summary"] = _clean_text(str(normalized.get("summary") or "").strip())
        normalized["topic"] = str(normalized.get("topic") or "").strip()
        normalized["source_url"] = str(normalized.get("source_url") or "").strip()
        normalized["feed_url"] = str(normalized.get("feed_url") or "").strip()
        published_value = normalized.get("published_at")
        if isinstance(published_value, datetime):
            normalized["published_at"] = published_value.astimezone(timezone.utc).isoformat()
        else:
            published_at = _parse_timestamp(str(published_value or "").strip())
            normalized["published_at"] = published_at.isoformat() if published_at is not None else None
        return normalized

    def _looks_like_feed(self, *, content_type: str, body: str) -> bool:
        if any(token in content_type for token in ("rss", "atom", "xml")):
            return True
        preview = body.lstrip()[:200].lower()
        return preview.startswith("<?xml") or "<rss" in preview or "<feed" in preview

    def _get(self, url: str) -> httpx.Response:
        response = httpx.get(
            url,
            follow_redirects=True,
            timeout=20.0,
            headers={"User-Agent": NEWS_USER_AGENT},
        )
        response.raise_for_status()
        return response


class ArxivReviewRunner:
    worker_key = "arxiv_review_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def ingest_feed(self, request: ArxivFeedIngestRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        items = list(request.seed_arxiv or DEFAULT_ARXIV_ITEMS)
        artifact_path = run_dir / "arxiv_ingest.json"
        output = ArxivFeedIngestOutput(
            generated_at=datetime.now(timezone.utc),
            papers=items,
            count=len(items),
        )
        payload = output.model_dump(mode="json")
        write_json(artifact_path, payload)
        observations = [
            ObservationRecord(
                kind="paper_candidate",
                summary=item["title"],
                payload=item,
                confidence=0.5,
            )
            for item in items
        ]
        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"Ingested {len(items)} arXiv candidates.",
            artifact_manifest=[
                build_file_artifact(
                    kind="paper-feed",
                    path=artifact_path,
                    media_type="application/json",
                    metadata={"count": len(items)},
                )
            ],
            structured_outputs=payload,
            observations=observations,
        )

    def review(self, request: PaperReviewRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        review_path = run_dir / f"{request.paper_id.replace('/', '_')}_review.md"
        annotation_path = run_dir / "paper-annotations.json"
        annotated_html_path = run_dir / "annotated-paper.html"

        source = self._resolve_source(request)
        source_kind = str(source["source_kind"])
        source_url = str(source["source_url"])
        blocks = list(source["blocks"])
        fallback_reason = source["fallback_reason"]
        source_origin = str(source.get("source_origin") or "unknown")
        source_document_id = source.get("source_document_id")
        source_artifact_id = source.get("source_artifact_id")
        canonical_uri = source.get("canonical_uri")

        annotation_payload: dict[str, Any] | None = None
        if blocks and fallback_reason is None:
            try:
                annotation_payload = self._generate_annotations(
                    request=request,
                    run_dir=run_dir,
                    source_url=source_url,
                    source_kind=source_kind,
                    blocks=blocks,
                    output_path=annotation_path,
                )
                write_text(annotated_html_path, self._render_annotated_html(annotation_payload))
            except Exception as exc:
                fallback_reason = f"structured_analysis_failed: {exc}"
                annotation_payload = None

        presentation_mode = "annotated_paper" if annotation_payload is not None else "raw_review"
        review_text = self._render_review(
            request=request,
            source_url=source_url,
            source_kind=source_kind,
            presentation_mode=presentation_mode,
            annotation_payload=annotation_payload,
            fallback_reason=fallback_reason,
        )
        write_text(review_path, review_text)

        artifacts = [
            build_file_artifact(
                kind="review-card",
                path=review_path,
                media_type="text/markdown",
            )
        ]
        if annotation_payload is not None:
            artifacts.append(
                build_file_artifact(
                    kind="paper-annotations",
                    path=annotation_path,
                    media_type="application/json",
                )
            )
            artifacts.append(
                build_file_artifact(
                    kind="annotated-paper",
                    path=annotated_html_path,
                    media_type="text/html",
                )
            )

        summary_text = (
            self._best_summary(annotation_payload)
            if annotation_payload is not None
            else "Structured annotation was unavailable; generated raw review summary."
        )
        review_payload = {
            "paper_id": request.paper_id,
            "title": request.title or f"Paper {request.paper_id}",
            "source_url": source_url,
            "source_kind": source_kind,
            "presentation_mode": presentation_mode,
            "fallback_reason": fallback_reason,
            "annotation_json_artifact_id": None,
            "annotated_html_artifact_id": None,
            "annotation_json_artifact_path": str(annotation_path) if annotation_payload is not None else None,
            "annotated_html_artifact_path": str(annotated_html_path) if annotation_payload is not None else None,
            "block_count": len(blocks),
            "annotation_count": len(annotation_payload.get("annotations", [])) if annotation_payload is not None else 0,
            "summary": summary_text,
            "source_origin": source_origin,
            "source_document_id": source_document_id,
            "source_artifact_id": source_artifact_id,
            "canonical_uri": canonical_uri,
        }

        observations = [
            ObservationRecord(
                kind="paper_review_mode",
                summary=f"Presentation mode: {presentation_mode}",
                payload={
                    "paper_id": request.paper_id,
                    "presentation_mode": presentation_mode,
                    "source_kind": source_kind,
                    "source_origin": source_origin,
                    "fallback_reason": fallback_reason,
                },
                confidence=0.8 if annotation_payload is not None else 0.6,
            )
        ]
        if annotation_payload is not None:
            observations.append(
                ObservationRecord(
                    kind="paper_annotation_count",
                    summary=f"Annotated {len(annotation_payload.get('annotations', []))} spans.",
                    payload={"annotation_count": len(annotation_payload.get("annotations", []))},
                    confidence=0.7,
                )
            )

        report_metadata = {
            "paper_id": request.paper_id,
            "presentation_mode": presentation_mode,
            "source_kind": source_kind,
            "source_origin": source_origin,
            "source_document_id": source_document_id,
            "source_artifact_id": source_artifact_id,
            "canonical_uri": canonical_uri,
            "fallback_reason": fallback_reason,
            "annotation_json_artifact_id": None,
            "annotated_html_artifact_id": None,
            "annotation_json_artifact_path": str(annotation_path) if annotation_payload is not None else None,
            "annotated_html_artifact_path": str(annotated_html_path) if annotation_payload is not None else None,
            "taxonomy": {
                "filters": ["report"],
                "tags": ["arxiv_paper_analysis"],
            },
        }

        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"Reviewed paper {request.paper_id} ({presentation_mode}).",
            artifact_manifest=artifacts,
            structured_outputs=review_payload,
            observations=observations,
            reports=[
                ReportRecord(
                    report_type="paper_review",
                    title=request.title or f"Paper {request.paper_id}",
                    summary=summary_text,
                    content_markdown=review_text,
                    artifact_path=str(review_path),
                    metadata=report_metadata,
                )
            ],
            next_suggested_events=[
                EventSuggestion(name=EventName.PAPER_REVIEW_COMPLETED, payload={"paper_id": request.paper_id})
            ],
        )

    def _resolve_source(self, request: PaperReviewRequest) -> dict[str, Any]:
        source_url = request.source_url or f"https://arxiv.org/abs/{request.paper_id}"
        metadata = request.metadata or {}
        raw_text = str(metadata.get("raw_text") or "").strip()
        if not raw_text:
            raw_text = self._read_optional_text_file(metadata.get("text_path"))
        if raw_text:
            return {
                "source_url": source_url,
                "source_kind": "arxiv_text",
                "blocks": self._normalize_text_blocks(raw_text),
                "fallback_reason": None,
                "source_origin": "request_text",
            }

        raw_html = str(metadata.get("raw_html") or "").strip()
        if not raw_html:
            raw_html = self._read_optional_text_file(metadata.get("html_path"))
        if raw_html:
            text = self._html_to_text(raw_html)
            blocks = self._normalize_text_blocks(text)
            return {
                "source_url": source_url,
                "source_kind": "arxiv_html",
                "blocks": blocks,
                "fallback_reason": None if blocks else "html_without_extractable_text",
                "source_origin": "request_html",
            }

        owned_source = self._load_owned_source(source_url)
        if owned_source is not None:
            return owned_source

        if self._looks_like_pdf(source_url):
            return {
                "source_url": source_url,
                "source_kind": "pdf",
                "blocks": [],
                "fallback_reason": "pdf_only_source",
                "source_origin": "unresolved_pdf_url",
            }

        return {
            "source_url": source_url,
            "source_kind": "arxiv_unavailable",
            "blocks": [],
            "fallback_reason": "no_html_or_text_payload_available",
            "source_origin": "unresolved_request",
        }

    @staticmethod
    def _looks_like_pdf(source_url: str) -> bool:
        lowered = source_url.lower()
        return lowered.endswith(".pdf") or "/pdf/" in lowered

    @staticmethod
    def _read_optional_text_file(path_like: Any) -> str:
        if not path_like:
            return ""
        try:
            path = Path(str(path_like)).expanduser()
            if path.exists() and path.is_file():
                return path.read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            return ""
        return ""

    def _load_owned_source(self, source_url: str) -> dict[str, Any] | None:
        candidates = self._canonical_uri_candidates(source_url)
        if not candidates:
            return None

        repository = LifeRepository(engine_for_url(self.settings.life_database_url))
        source_document = None
        matched_uri = ""
        for candidate in candidates:
            source_document = repository.get_source_document_by_canonical_uri(candidate)
            if source_document is not None:
                matched_uri = candidate
                break
        if source_document is None:
            return None

        artifacts = repository.list_artifacts_for_source_document(source_document.id)
        source_type = str(getattr(source_document, "source_type", "") or "").strip().lower()
        current_text_artifact_id = str(getattr(source_document, "current_text_artifact_id", "") or "").strip()
        current_text_artifact = repository.get_artifact(current_text_artifact_id) if current_text_artifact_id else None
        raw_artifact = self._find_source_artifact(artifacts, role="raw_source")

        if source_type == "html":
            raw_html = self._read_artifact_text(raw_artifact)
            if raw_html:
                blocks = self._normalize_text_blocks(self._html_to_text(raw_html))
                return {
                    "source_url": source_url,
                    "source_kind": "arxiv_html",
                    "blocks": blocks,
                    "fallback_reason": None if blocks else "html_without_extractable_text",
                    "source_origin": "owned_source_raw_html",
                    "source_document_id": source_document.id,
                    "source_artifact_id": getattr(raw_artifact, "id", None),
                    "canonical_uri": matched_uri,
                }

        normalized_text = self._read_artifact_text(current_text_artifact)
        if normalized_text:
            return {
                "source_url": source_url,
                "source_kind": "arxiv_text",
                "blocks": self._normalize_text_blocks(normalized_text),
                "fallback_reason": None,
                "source_origin": "owned_source_normalized_text",
                "source_document_id": source_document.id,
                "source_artifact_id": getattr(current_text_artifact, "id", None),
                "canonical_uri": matched_uri,
            }

        fallback_raw_text = self._read_artifact_text(raw_artifact)
        if fallback_raw_text:
            return {
                "source_url": source_url,
                "source_kind": "arxiv_text",
                "blocks": self._normalize_text_blocks(fallback_raw_text),
                "fallback_reason": None,
                "source_origin": "owned_source_raw_text",
                "source_document_id": source_document.id,
                "source_artifact_id": getattr(raw_artifact, "id", None),
                "canonical_uri": matched_uri,
            }

        return None

    @staticmethod
    def _find_source_artifact(artifacts: list[Any], *, role: str) -> Any | None:
        target = role.strip().lower()
        for artifact in artifacts:
            metadata = dict(getattr(artifact, "metadata_json", {}) or {})
            artifact_role = str(metadata.get("role") or getattr(artifact, "kind", "") or "").strip().lower()
            if artifact_role == target:
                return artifact
        return None

    @staticmethod
    def _read_artifact_text(artifact: Any) -> str:
        path = str(getattr(artifact, "path", "") or "").strip()
        if not path:
            return ""
        try:
            candidate = Path(path).expanduser()
            if candidate.exists() and candidate.is_file():
                return candidate.read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            return ""
        return ""

    @staticmethod
    def _canonical_uri_candidates(source_url: str) -> list[str]:
        text = str(source_url or "").strip()
        if not text:
            return []
        normalized = ArxivReviewRunner._normalize_source_url(text)
        candidates = [normalized]
        if normalized != text:
            candidates.append(text)
        return list(dict.fromkeys(candidate for candidate in candidates if candidate))

    @staticmethod
    def _normalize_source_url(source_url: str) -> str:
        text = str(source_url or "").strip()
        if not text:
            return ""
        if ArtifactIngestRunner is not None:
            try:
                return str(ArtifactIngestRunner._normalize_url(text))
            except Exception:
                pass
        return text

    def _generate_annotations(
        self,
        *,
        request: PaperReviewRequest,
        run_dir: Path,
        source_url: str,
        source_kind: str,
        blocks: list[dict[str, Any]],
        output_path: Path,
    ) -> dict[str, Any]:
        schema_path = Path(__file__).resolve().parents[2] / "schemas" / "paper-annotations.schema.json"
        prompt = self._render_annotation_prompt(
            request=request,
            source_url=source_url,
            source_kind=source_kind,
            blocks=blocks,
        )
        codex_runner = CodexCliRunner(self.settings)
        response = codex_runner.run(
            CodexCliRequest(
                prompt=prompt,
                cwd=str(run_dir),
                output_schema=str(schema_path),
                output_path=str(output_path),
            )
        )
        if response.status != WorkerStatus.COMPLETED:
            raise RuntimeError("codex_structured_mode_failed")
        if not output_path.exists():
            raise FileNotFoundError("missing_annotation_json")
        payload = self._jsonify_annotation_output(
            request=request,
            run_dir=run_dir,
            source_url=source_url,
            source_kind=source_kind,
            blocks=blocks,
            schema_path=schema_path,
            output_path=output_path,
        )
        self._validate_annotation_payload(payload)
        return payload

    def _jsonify_annotation_output(
        self,
        *,
        request: PaperReviewRequest,
        run_dir: Path,
        source_url: str,
        source_kind: str,
        blocks: list[dict[str, Any]],
        schema_path: Path,
        output_path: Path,
    ) -> dict[str, Any]:
        raw_text = output_path.read_text(encoding="utf-8", errors="replace")
        try:
            payload = self._parse_annotation_payload(raw_text)
        except JSONDecodeError as exc:
            payload = self._repair_invalid_annotation_output(
                request=request,
                run_dir=run_dir,
                source_url=source_url,
                source_kind=source_kind,
                blocks=blocks,
                schema_path=schema_path,
                output_path=output_path,
                raw_text=raw_text,
                parse_error=str(exc),
            )
        write_json(output_path, payload)
        return payload

    @staticmethod
    def _parse_annotation_payload(raw_text: str) -> dict[str, Any]:
        try:
            payload = json.loads(raw_text)
        except JSONDecodeError:
            extracted = ArxivReviewRunner._extract_json_document(raw_text)
            payload = json.loads(extracted)
        if not isinstance(payload, dict):
            raise ValueError("annotation_payload_must_be_object")
        return payload

    @staticmethod
    def _extract_json_document(raw_text: str) -> str:
        stripped = raw_text.strip()
        if stripped.startswith("```"):
            stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
            stripped = re.sub(r"\s*```$", "", stripped)
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return stripped
        return stripped[start : end + 1]

    def _repair_invalid_annotation_output(
        self,
        *,
        request: PaperReviewRequest,
        run_dir: Path,
        source_url: str,
        source_kind: str,
        blocks: list[dict[str, Any]],
        schema_path: Path,
        output_path: Path,
        raw_text: str,
        parse_error: str,
    ) -> dict[str, Any]:
        repair_prompt = self._render_annotation_repair_prompt(
            request=request,
            source_url=source_url,
            source_kind=source_kind,
            blocks=blocks,
            raw_text=raw_text,
            parse_error=parse_error,
        )
        codex_runner = CodexCliRunner(self.settings)
        repair_response = codex_runner.run(
            CodexCliRequest(
                prompt=repair_prompt,
                cwd=str(run_dir),
                output_schema=str(schema_path),
                output_path=str(output_path),
            )
        )
        if repair_response.status != WorkerStatus.COMPLETED:
            raise RuntimeError(f"invalid_json_unrepairable: {parse_error}")
        repaired_text = output_path.read_text(encoding="utf-8", errors="replace")
        try:
            return self._parse_annotation_payload(repaired_text)
        except JSONDecodeError as exc:
            raise RuntimeError(f"invalid_json_unrepairable: {exc}") from exc

    @staticmethod
    def _validate_annotation_payload(payload: dict[str, Any]) -> None:
        for key in ("paper", "report", "concepts", "blocks", "annotations"):
            if key not in payload:
                raise ValueError(f"missing_key:{key}")
        if not isinstance(payload["report"], dict):
            raise ValueError("invalid_report")
        if not isinstance(payload["concepts"], list):
            raise ValueError("invalid_concepts")
        if not isinstance(payload["blocks"], list):
            raise ValueError("invalid_blocks")
        if not isinstance(payload["annotations"], list):
            raise ValueError("invalid_annotations")

    def _render_annotation_prompt(
        self,
        *,
        request: PaperReviewRequest,
        source_url: str,
        source_kind: str,
        blocks: list[dict[str, Any]],
    ) -> str:
        base_prompt = load_paper_review_prompt().strip()
        payload = {
            "paper_id": request.paper_id,
            "title": request.title or f"Paper {request.paper_id}",
            "source_url": source_url,
            "source_kind": source_kind,
            "blocks": blocks,
        }
        return "\n\n".join(
            [
                base_prompt,
                "Return strict JSON only. Do not include markdown fences.",
                "Use dense paragraphs where needed and assume a serious learner audience.",
                "Separate extracted paper claims from your inferences in analysis text.",
                "The `report` object is the main deliverable. Make it complete before optimizing local annotations.",
                "Input payload:",
                json.dumps(payload, indent=2),
            ]
        )

    def _render_annotation_repair_prompt(
        self,
        *,
        request: PaperReviewRequest,
        source_url: str,
        source_kind: str,
        blocks: list[dict[str, Any]],
        raw_text: str,
        parse_error: str,
    ) -> str:
        base_prompt = load_paper_review_prompt().strip()
        repair_payload = {
            "paper_id": request.paper_id,
            "title": request.title or f"Paper {request.paper_id}",
            "source_url": source_url,
            "source_kind": source_kind,
            "blocks": blocks[:20],
        }
        return "\n\n".join(
            [
                base_prompt,
                "The prior model output was not valid JSON.",
                "Repair the malformed output into one schema-valid JSON object.",
                "Preserve the intended meaning when possible and do not add unsupported claims.",
                f"JSON parse error: {parse_error}",
                "Reference paper payload:",
                json.dumps(repair_payload, indent=2),
                "Malformed output to repair:",
                raw_text,
            ]
        )

    @staticmethod
    def _normalize_text_blocks(text: str) -> list[dict[str, Any]]:
        cleaned = re.sub(r"\r\n?", "\n", text).strip()
        if not cleaned:
            return []
        paragraphs = [item.strip() for item in re.split(r"\n\s*\n+", cleaned) if item.strip()]
        blocks: list[dict[str, Any]] = []
        section = "Body"
        order = 1
        for paragraph in paragraphs:
            compact = re.sub(r"\s+", " ", paragraph).strip()
            if not compact:
                continue
            if len(compact) <= 80 and compact.lower() in {
                "abstract",
                "introduction",
                "related work",
                "method",
                "methods",
                "results",
                "discussion",
                "conclusion",
                "appendix",
            }:
                section = compact.title()
                continue
            blocks.append(
                {
                    "block_id": f"b-{order:04d}",
                    "section_label": section,
                    "order": order,
                    "text": compact,
                }
            )
            order += 1
        return blocks

    @staticmethod
    def _html_to_text(raw_html: str) -> str:
        stripped = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", raw_html)
        stripped = re.sub(r"(?i)<br\\s*/?>", "\n", stripped)
        stripped = re.sub(r"(?i)</p\\s*>", "\n\n", stripped)
        stripped = re.sub(r"(?i)</div\\s*>", "\n", stripped)
        stripped = re.sub(r"(?s)<[^>]+>", " ", stripped)
        decoded = html.unescape(stripped)
        decoded = re.sub(r"[ \t]+", " ", decoded)
        decoded = re.sub(r"\n{3,}", "\n\n", decoded)
        return decoded.strip()

    def _render_review(
        self,
        *,
        request: PaperReviewRequest,
        source_url: str,
        source_kind: str,
        presentation_mode: str,
        annotation_payload: dict[str, Any] | None,
        fallback_reason: str | None,
    ) -> str:
        title = request.title or f"Paper {request.paper_id}"
        lines = [
            f"# {title}",
            "",
            f"- Paper ID: {request.paper_id}",
            f"- Source URL: {source_url or 'n/a'}",
            f"- Source Kind: {source_kind}",
            f"- Presentation Mode: {presentation_mode}",
        ]
        if fallback_reason:
            lines.append(f"- Fallback Reason: {fallback_reason}")

        lines.extend(["", "## Summary"])
        if annotation_payload is None:
            lines.append(
                "Structured annotation output was unavailable, so this run produced a raw review card for retrieval and operator inspection."
            )
            return "\n".join(lines)

        lines.append(self._best_summary(annotation_payload))
        lines.extend(
            [
                "",
                "## Coverage",
                f"- Normalized blocks: {len(annotation_payload.get('blocks', []))}",
                f"- Inline annotations: {len(annotation_payload.get('annotations', []))}",
                f"- Core concepts: {len(annotation_payload.get('concepts', []))}",
            ]
        )
        lines.extend(self._render_report_markdown_sections(annotation_payload.get("report")))
        return "\n".join(lines)

    @staticmethod
    def _best_summary(annotation_payload: dict[str, Any]) -> str:
        report = annotation_payload.get("report")
        if isinstance(report, dict):
            one_sentence = str(report.get("one_sentence_summary") or "").strip()
            if one_sentence:
                return one_sentence
            executive_summary = report.get("executive_summary")
            if isinstance(executive_summary, list):
                for item in executive_summary:
                    text = str(item or "").strip()
                    if text:
                        return text
        concepts = annotation_payload.get("concepts", [])
        if concepts:
            first = concepts[0]
            return str(first.get("summary") or f"Core concept: {first.get('name', 'n/a')}")
        annotations = annotation_payload.get("annotations", [])
        if annotations:
            return str(annotations[0].get("analysis") or "Annotated analysis completed.")
        return "Structured annotation completed."

    @staticmethod
    def _render_report_markdown_sections(report: Any) -> list[str]:
        if not isinstance(report, dict):
            return []
        section_specs = [
            ("Document Type", [str(report.get("document_type") or "").strip()]),
            ("Primary Focus", [str(report.get("primary_focus") or "").strip()]),
            ("Executive Summary", report.get("executive_summary")),
            ("Problem And Scope", report.get("problem_and_scope")),
            ("Method And Architecture", report.get("method_and_architecture")),
            ("Training And Data", report.get("training_and_data")),
            ("Evaluation And Evidence", report.get("evaluation_and_evidence")),
            ("Novelty And Lineage", report.get("novelty_and_lineage")),
            ("Limitations And Risks", report.get("limitations_and_risks")),
            ("Practical Takeaways", report.get("practical_takeaways")),
            ("Open Questions", report.get("open_questions")),
        ]

        lines: list[str] = []
        for title, items in section_specs:
            normalized_items = [str(item).strip() for item in (items or []) if str(item).strip()]
            if not normalized_items:
                continue
            lines.extend(["", f"## {title}"])
            for item in normalized_items:
                lines.append(f"- {item}")
        return lines

    def _render_annotated_html(self, payload: dict[str, Any]) -> str:
        paper = payload.get("paper", {})
        report = payload.get("report", {})
        concepts = payload.get("concepts", [])
        blocks = payload.get("blocks", [])
        annotations = payload.get("annotations", [])
        concepts_by_id = {str(item.get("concept_id")): item for item in concepts}

        annotations_by_block: dict[str, list[dict[str, Any]]] = {}
        for annotation in annotations:
            block_id = str(annotation.get("block_id") or "")
            annotations_by_block.setdefault(block_id, []).append(annotation)

        def render_block(block: dict[str, Any]) -> str:
            block_id = str(block.get("block_id", ""))
            block_text = str(block.get("text", ""))
            block_annotations = annotations_by_block.get(block_id, [])
            rendered = html.escape(block_text)
            for ann in block_annotations:
                anchor = str(ann.get("anchor_text") or "").strip()
                if not anchor:
                    continue
                anchor_escaped = html.escape(anchor)
                teaser = html.escape(str(ann.get("analysis") or "")[:180])
                ann_id = html.escape(str(ann.get("annotation_id") or ""))
                label = html.escape(str(ann.get("label") or "AI Analysis"))
                replacement = (
                    f'<span class="analysis-anchor" data-ann-id="{ann_id}" data-label="{label}" '
                    f'data-teaser="{teaser}">{anchor_escaped}<span class="analysis-chip">AI Analysis</span></span>'
                )
                rendered = re.sub(
                    re.escape(anchor_escaped),
                    lambda _match: replacement,
                    rendered,
                    count=1,
                    flags=re.IGNORECASE,
                )
            return (
                f'<article class="paper-block"><div class="block-meta">{html.escape(str(block.get("section_label", "Body")))}'
                f' · {html.escape(block_id)}</div><p>{rendered}</p></article>'
            )

        annotations_json = json.dumps(annotations).replace("</", "<\\/")
        concepts_json = json.dumps(concepts_by_id).replace("</", "<\\/")

        def render_report_section(title: str, items: Any) -> str:
            normalized_items = [html.escape(str(item).strip()) for item in (items or []) if str(item).strip()]
            if not normalized_items:
                return ""
            return (
                f'<section class="panel-section"><h3>{html.escape(title)}</h3><ul>'
                + "".join(f"<li>{item}</li>" for item in normalized_items)
                + "</ul></section>"
            )

        def render_report_panel(report_payload: Any) -> str:
            if not isinstance(report_payload, dict):
                return '<p class="muted">Structured report overview is unavailable.</p>'
            document_type = html.escape(str(report_payload.get("document_type") or "n/a"))
            primary_focus = html.escape(str(report_payload.get("primary_focus") or "n/a"))
            one_sentence = html.escape(str(report_payload.get("one_sentence_summary") or ""))
            sections = [
                render_report_section("Executive Summary", report_payload.get("executive_summary")),
                render_report_section("Problem And Scope", report_payload.get("problem_and_scope")),
                render_report_section("Method And Architecture", report_payload.get("method_and_architecture")),
                render_report_section("Training And Data", report_payload.get("training_and_data")),
                render_report_section("Evaluation And Evidence", report_payload.get("evaluation_and_evidence")),
                render_report_section("Novelty And Lineage", report_payload.get("novelty_and_lineage")),
                render_report_section("Limitations And Risks", report_payload.get("limitations_and_risks")),
                render_report_section("Practical Takeaways", report_payload.get("practical_takeaways")),
                render_report_section("Open Questions", report_payload.get("open_questions")),
            ]
            return (
                '<section class="panel-section">'
                f'<h3>One Sentence Summary</h3><p>{one_sentence or "Not provided."}</p>'
                f'<p class="muted">Document type: {document_type} · Primary focus: {primary_focus}</p>'
                '</section>'
                + "".join(section for section in sections if section)
            )

        report_panel_html = render_report_panel(report)

        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(str(paper.get("title", "Annotated Paper Review")))}</title>
  <style>
    body {{ margin: 0; font-family: Georgia, "Times New Roman", serif; color: #14171f; background: #f2f4f8; }}
    .wrap {{ max-width: 1380px; margin: 0 auto; padding: 24px; display: grid; grid-template-columns: minmax(0, 2fr) minmax(340px, 1.1fr); gap: 18px; }}
    .side-column {{ display: flex; flex-direction: column; gap: 18px; }}
    .card {{ background: white; border: 1px solid #d2d8e3; border-radius: 14px; padding: 18px; box-shadow: 0 8px 18px rgba(9, 17, 31, 0.05); }}
    h1 {{ margin: 0 0 8px; font-size: 1.5rem; }}
    .kicker {{ font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.06em; color: #334155; font-weight: 700; }}
    .paper-block {{ margin-bottom: 14px; border-top: 1px solid #e5eaf3; padding-top: 12px; }}
    .block-meta {{ color: #546274; font-size: 0.82rem; margin-bottom: 6px; }}
    .analysis-anchor {{ background: #fff4bf; border-bottom: 2px solid #f2b705; cursor: pointer; position: relative; }}
    .analysis-chip {{ margin-left: 6px; padding: 1px 6px; border-radius: 999px; background: #103a8d; color: #fff; font-size: 0.66rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; }}
    .panel-title {{ margin-top: 0; }}
    .panel-section h3 {{ margin-bottom: 6px; }}
    .panel-section p {{ margin-top: 0; line-height: 1.5; }}
    .panel-section ul {{ margin: 0 0 12px 18px; padding: 0; }}
    .panel-section li {{ margin-bottom: 8px; line-height: 1.45; }}
    .panel-divider {{ border-top: 1px solid #e5eaf3; margin: 16px 0; }}
    .muted {{ color: #58677a; }}
    #teaser {{ position: fixed; z-index: 20; max-width: 360px; pointer-events: none; background: #0f1f3a; color: white; padding: 10px 12px; border-radius: 10px; box-shadow: 0 8px 22px rgba(10, 20, 35, 0.28); display: none; font-size: 0.84rem; line-height: 1.35; }}
    @media (max-width: 960px) {{ .wrap {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card">
      <div class="kicker">AI Analysis</div>
      <h1>{html.escape(str(paper.get("title", "Annotated Paper Review")))}</h1>
      <p class="muted">Source URL: {html.escape(str(paper.get("source_url", "n/a")))}</p>
      {"".join(render_block(block) for block in blocks)}
    </section>
    <aside class="side-column">
      <section class="card">
        <div class="kicker">Reader Companion</div>
        <h2 class="panel-title">Full Paper Analysis</h2>
        <div id="report-content">{report_panel_html}</div>
      </section>
      <section class="card">
        <h2 class="panel-title">Pinned Annotation</h2>
        <p class="muted">Hover for a teaser, then click a highlighted span to inspect local analysis.</p>
        <div id="panel-content" class="muted">Click a highlighted span to inspect the local note, evidence, and linked concepts.</div>
      </section>
    </aside>
  </div>
  <div id="teaser"></div>
  <script>
    const annotations = {annotations_json};
    const conceptsById = {concepts_json};
    const byId = Object.fromEntries(annotations.map((ann) => [String(ann.annotation_id || ""), ann]));
    const panel = document.getElementById("panel-content");
    const teaser = document.getElementById("teaser");

    function escapeHtml(value) {{
      return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\\"", "&quot;")
        .replaceAll("'", "&#39;");
    }}

    function conceptSection(concept) {{
      return `
        <section class="panel-section">
          <h3>${{escapeHtml(concept.name || "Concept")}}</h3>
          <p>${{escapeHtml(concept.summary || "")}}</p>
        </section>
      `;
    }}

    function evidenceSection(evidence) {{
      const items = Array.isArray(evidence) ? evidence : [];
      if (!items.length) {{
        return '<p class="muted">No evidence snippets attached.</p>';
      }}
      const rows = items.map((item) => {{
        const quote = escapeHtml(item?.quote || "");
        const reason = escapeHtml(item?.reason || "");
        const sourceRef = item?.source_ref ? ` · ${{escapeHtml(item.source_ref)}}` : "";
        return `<li><strong>${{quote}}</strong><div>${{reason}}${{sourceRef}}</div></li>`;
      }}).join("");
      return `<section class="panel-section"><h3>Evidence</h3><ul>${{rows}}</ul></section>`;
    }}

    function pinAnnotation(annotationId) {{
      const ann = byId[String(annotationId)];
      if (!ann) {{
        panel.innerHTML = '<p class="muted">Annotation not found.</p>';
        return;
      }}
      const conceptIds = Array.isArray(ann.concept_ids) ? ann.concept_ids : [];
      const conceptHtml = conceptIds
        .map((id) => conceptsById[String(id)])
        .filter(Boolean)
        .map((concept) => conceptSection(concept))
        .join("");
      const evidenceHtml = evidenceSection(ann.evidence);

      panel.innerHTML = `
        <section class="panel-section">
          <h3>${{escapeHtml(ann.label || "AI Analysis")}}</h3>
          <p>${{escapeHtml(ann.analysis || "")}}</p>
          <p class="muted">Kind: ${{escapeHtml(ann.kind || "analysis")}} · Confidence: ${{escapeHtml(ann.confidence ?? "n/a")}}</p>
        </section>
        <div class="panel-divider"></div>
        ${{evidenceHtml}}
        <div class="panel-divider"></div>
        ${{conceptHtml || '<p class="muted">No linked concepts.</p>'}}
      `;
    }}

    for (const el of document.querySelectorAll(".analysis-anchor")) {{
      el.addEventListener("mouseenter", () => {{
        const label = el.dataset.label || "AI Analysis";
        const teaserText = el.dataset.teaser || "";
        teaser.innerHTML = `<strong>${{escapeHtml(label)}}</strong><div>${{escapeHtml(teaserText)}}</div>`;
        teaser.style.display = "block";
      }});
      el.addEventListener("mousemove", (event) => {{
        teaser.style.left = `${{event.clientX + 14}}px`;
        teaser.style.top = `${{event.clientY + 14}}px`;
      }});
      el.addEventListener("mouseleave", () => {{
        teaser.style.display = "none";
      }});
      el.addEventListener("click", () => {{
        pinAnnotation(el.dataset.annId);
      }});
    }}
  </script>
</body>
</html>
"""
