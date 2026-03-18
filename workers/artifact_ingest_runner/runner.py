from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
from io import BytesIO
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

from libs.config import Settings
from libs.contracts.models import AdapterResult, ObservationRecord, WorkerStatus
from libs.contracts.workers import (
    ArtifactIngestChunk,
    ArtifactIngestItemOutput,
    ArtifactIngestItemRequest,
    ArtifactIngestOutput,
    ArtifactIngestRequest,
)
from libs.retrieval import chunk_text
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text

try:  # pragma: no cover - optional dependency at runtime
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - optional dependency at runtime
    BeautifulSoup = None

try:  # pragma: no cover - optional dependency at runtime
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency at runtime
    PdfReader = None

try:  # pragma: no cover - optional dependency at runtime
    import trafilatura
except Exception:  # pragma: no cover - optional dependency at runtime
    trafilatura = None


_TRACKING_QUERY_KEYS = {"fbclid", "gclid"}
_WHITESPACE_RE = re.compile(r"\s+")
_ENTITY_RE = re.compile(r"\b(?:[A-Z][A-Za-z0-9&.-]+(?:\s+[A-Z][A-Za-z0-9&.-]+)+)\b")


@dataclass(slots=True)
class _ResolvedSource:
    raw_bytes: bytes
    raw_text: str | None
    media_type: str | None
    source_label: str


@dataclass(slots=True)
class _ParsedContent:
    source_type: str
    normalized_text: str
    title: str | None
    author: str | None
    published_at: datetime | None
    auto_tags: list[str]
    entities: list[str]
    chunk_payloads: list[dict[str, Any]]
    warning_codes: list[str]


class ArtifactIngestRunner:
    worker_key = "artifact_ingest_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: ArtifactIngestRequest | dict[str, Any]) -> AdapterResult:
        ingest_request = self._coerce_request(request)
        run_dir = ensure_worker_dir(self.settings, self.worker_key)

        item_outputs: list[ArtifactIngestItemOutput] = []
        artifacts = []
        observations = []
        success_count = 0
        failure_count = 0
        warning_count = 0

        for index, item in enumerate(ingest_request.items):
            item_dir = run_dir / f"item-{index:03d}"
            item_dir.mkdir(parents=True, exist_ok=True)
            output, item_artifacts, observation = self._process_item(index=index, item=item, item_dir=item_dir)
            item_outputs.append(output)
            artifacts.extend(item_artifacts)
            observations.append(observation)
            warning_count += len(output.warning_codes)
            if output.status == "completed":
                success_count += 1
            else:
                failure_count += 1

        status = WorkerStatus.COMPLETED if success_count > 0 else WorkerStatus.FAILED
        structured_output = ArtifactIngestOutput(
            generated_at=datetime.now(timezone.utc),
            input_count=len(ingest_request.items),
            success_count=success_count,
            failure_count=failure_count,
            warning_count=warning_count,
            items=item_outputs,
        )
        stdout = f"Ingested {success_count} of {len(ingest_request.items)} items."
        if failure_count:
            stdout += f" {failure_count} items failed or were unsupported."

        return AdapterResult(
            status=status,
            stdout=stdout,
            artifact_manifest=artifacts,
            structured_outputs=structured_output.model_dump(mode="json"),
            observations=observations,
        )

    def _coerce_request(self, request: ArtifactIngestRequest | dict[str, Any]) -> ArtifactIngestRequest:
        if isinstance(request, ArtifactIngestRequest):
            return request
        if isinstance(request, dict):
            return ArtifactIngestRequest.model_validate(
                {
                    "items": list(request.get("items") or []),
                    "metadata": dict(request.get("metadata") or {}),
                }
            )
        raise TypeError("artifact ingest request must be a dict or ArtifactIngestRequest")

    def _process_item(
        self,
        *,
        index: int,
        item: ArtifactIngestItemRequest,
        item_dir: Path,
    ) -> tuple[ArtifactIngestItemOutput, list[Any], ObservationRecord]:
        try:
            resolved = self._resolve_source(item)
            canonical_uri = self._canonical_uri(item, resolved)
            parsed = self._parse_content(item, resolved, canonical_uri=canonical_uri)
            title = item.title or parsed.title
            author = item.author or parsed.author
            published_at = item.published_at or parsed.published_at
            tags = self._merge_tags(item.tags, parsed.auto_tags)
            metadata = {
                **dict(item.metadata),
                "source_type": parsed.source_type,
                "entities": parsed.entities,
            }

            raw_path = self._write_raw_source(
                item_dir=item_dir,
                item=item,
                resolved=resolved,
                source_type=parsed.source_type,
            )
            normalized_path = item_dir / "normalized.txt"
            write_text(normalized_path, parsed.normalized_text)
            manifest_path = item_dir / "ingest_manifest.json"

            output = ArtifactIngestItemOutput(
                input_index=index,
                input_kind=item.input_kind,
                status="completed",
                canonical_uri=canonical_uri,
                source_type=parsed.source_type,
                raw_artifact_path=str(raw_path),
                normalized_text_artifact_path=str(normalized_path),
                ingest_manifest_artifact_path=str(manifest_path),
                chunk_count=len(parsed.chunk_payloads),
                warning_codes=parsed.warning_codes,
                title=title,
                author=author,
                published_at=published_at,
                tags=tags,
                metadata=metadata,
                chunks=[ArtifactIngestChunk.model_validate(chunk) for chunk in parsed.chunk_payloads],
            )
            write_json(manifest_path, output.model_dump(mode="json"))

            artifacts = [
                build_file_artifact(
                    kind="source-raw",
                    path=raw_path,
                    media_type=resolved.media_type,
                    metadata={
                        "role": "raw_source",
                        "input_index": index,
                        "canonical_uri": canonical_uri,
                        "source_type": parsed.source_type,
                    },
                ),
                build_file_artifact(
                    kind="source-text",
                    path=normalized_path,
                    media_type="text/plain",
                    metadata={
                        "role": "normalized_text",
                        "input_index": index,
                        "canonical_uri": canonical_uri,
                        "source_type": parsed.source_type,
                        "title": title,
                    },
                ),
                build_file_artifact(
                    kind="source-manifest",
                    path=manifest_path,
                    media_type="application/json",
                    metadata={
                        "role": "ingest_manifest",
                        "input_index": index,
                        "canonical_uri": canonical_uri,
                        "source_type": parsed.source_type,
                    },
                ),
            ]
            observation = ObservationRecord(
                kind="artifact_ingest",
                summary=f"Ingested {canonical_uri}",
                payload={
                    "input_index": index,
                    "canonical_uri": canonical_uri,
                    "source_type": parsed.source_type,
                    "chunk_count": len(parsed.chunk_payloads),
                    "warning_codes": parsed.warning_codes,
                },
                confidence=0.7,
            )
            return output, artifacts, observation
        except Exception as exc:
            canonical_uri = self._fallback_canonical_uri(item)
            manifest_path = item_dir / "ingest_manifest.json"
            output = ArtifactIngestItemOutput(
                input_index=index,
                input_kind=item.input_kind,
                status="unsupported" if "unsupported" in str(exc).lower() else "failed",
                canonical_uri=canonical_uri,
                warning_codes=[],
                error=str(exc),
                metadata=dict(item.metadata),
                tags=list(item.tags),
            )
            write_json(manifest_path, output.model_dump(mode="json"))
            artifacts = [
                build_file_artifact(
                    kind="source-manifest",
                    path=manifest_path,
                    media_type="application/json",
                    metadata={
                        "role": "ingest_manifest",
                        "input_index": index,
                        "canonical_uri": canonical_uri,
                    },
                )
            ]
            observation = ObservationRecord(
                kind="artifact_ingest_error",
                summary=f"Failed to ingest {canonical_uri}",
                payload={"input_index": index, "canonical_uri": canonical_uri, "error": str(exc)},
                confidence=0.4,
            )
            return output, artifacts, observation

    def _resolve_source(self, item: ArtifactIngestItemRequest) -> _ResolvedSource:
        if item.input_kind == "inline":
            content = str(item.content or "")
            media_type = item.declared_media_type or self._inline_media_type(item)
            return _ResolvedSource(
                raw_bytes=content.encode("utf-8"),
                raw_text=content,
                media_type=media_type,
                source_label="inline",
            )
        if item.input_kind == "file":
            path = Path(str(item.file_path or "")).expanduser().resolve()
            raw_bytes = path.read_bytes()
            media_type = item.declared_media_type or self._guess_media_type(path.suffix.lower())
            raw_text = None if media_type == "application/pdf" else raw_bytes.decode("utf-8", errors="replace")
            return _ResolvedSource(raw_bytes=raw_bytes, raw_text=raw_text, media_type=media_type, source_label=path.name)
        response = httpx.get(str(item.url), follow_redirects=True, timeout=20.0)
        response.raise_for_status()
        media_type = item.declared_media_type or response.headers.get("content-type", "").split(";", maxsplit=1)[0].strip() or None
        raw_bytes = response.content
        raw_text = None if media_type == "application/pdf" else response.text
        return _ResolvedSource(
            raw_bytes=raw_bytes,
            raw_text=raw_text,
            media_type=media_type,
            source_label=str(item.url or ""),
        )

    def _parse_content(
        self,
        item: ArtifactIngestItemRequest,
        resolved: _ResolvedSource,
        *,
        canonical_uri: str,
    ) -> _ParsedContent:
        media_type = (resolved.media_type or "").lower()
        if item.input_kind == "inline":
            format_name = str(item.content_format or "text")
            if format_name == "html":
                return self._parse_html(resolved.raw_text or "", canonical_uri=canonical_uri)
            if format_name == "markdown":
                return self._parse_textual("markdown", resolved.raw_text or "", canonical_uri=canonical_uri)
            return self._parse_textual("text", resolved.raw_text or "", canonical_uri=canonical_uri)

        if media_type == "application/pdf" or canonical_uri.lower().endswith(".pdf"):
            return self._parse_pdf(resolved.raw_bytes, canonical_uri=canonical_uri)
        if "html" in media_type or canonical_uri.startswith("http"):
            return self._parse_html(resolved.raw_text or resolved.raw_bytes.decode("utf-8", errors="replace"), canonical_uri=canonical_uri)
        source_type = "markdown" if canonical_uri.lower().endswith((".md", ".markdown")) else "text"
        return self._parse_textual(
            source_type,
            resolved.raw_text or resolved.raw_bytes.decode("utf-8", errors="replace"),
            canonical_uri=canonical_uri,
        )

    def _parse_textual(self, source_type: str, text: str, *, canonical_uri: str) -> _ParsedContent:
        normalized_text = self._normalize_text(text)
        if not normalized_text:
            raise RuntimeError("unsupported_empty_text")
        tags = [source_type]
        chunk_payloads = self._build_chunks(
            normalized_text,
            base_metadata={
                "canonical_uri": canonical_uri,
                "source_type": source_type,
            },
        )
        return _ParsedContent(
            source_type=source_type,
            normalized_text=normalized_text,
            title=None,
            author=None,
            published_at=None,
            auto_tags=tags,
            entities=[],
            chunk_payloads=chunk_payloads,
            warning_codes=[],
        )

    def _parse_html(self, raw_html: str, *, canonical_uri: str) -> _ParsedContent:
        warnings: list[str] = []
        extracted_text = ""
        if trafilatura is not None:
            try:
                extracted_text = str(
                    trafilatura.extract(raw_html, include_comments=False, include_links=False, output_format="txt") or ""
                ).strip()
            except Exception:
                warnings.append("html_extraction_fallback")
        if not extracted_text:
            warnings.append("html_extraction_fallback")
            extracted_text = self._html_to_text(raw_html)
        normalized_text = self._normalize_text(extracted_text)
        if not normalized_text:
            raise RuntimeError("unsupported_empty_html")

        title = self._html_title(raw_html)
        author = self._html_meta(raw_html, ("author", "article:author", "og:article:author"))
        published_at = self._parse_datetime(
            self._html_meta(raw_html, ("article:published_time", "published_time", "pubdate", "date"))
        )
        host = urlparse(canonical_uri).netloc.replace("www.", "").strip()
        auto_tags = ["html"]
        if host:
            auto_tags.append(host)
        entities = self._infer_entities(title or normalized_text[:200])
        chunk_payloads = self._build_chunks(
            normalized_text,
            base_metadata={
                "canonical_uri": canonical_uri,
                "source_type": "html",
                "title": title,
                "author": author,
                "published_at": published_at.isoformat() if published_at else None,
                "entities": entities,
                "tags": auto_tags,
            },
        )
        return _ParsedContent(
            source_type="html",
            normalized_text=normalized_text,
            title=title,
            author=author,
            published_at=published_at,
            auto_tags=auto_tags,
            entities=entities,
            chunk_payloads=chunk_payloads,
            warning_codes=list(dict.fromkeys(warnings)),
        )

    def _parse_pdf(self, raw_bytes: bytes, *, canonical_uri: str) -> _ParsedContent:
        if PdfReader is None:
            raise RuntimeError("unsupported_missing_pdf_dependency")
        reader = PdfReader(BytesIO(raw_bytes))
        page_texts: list[str] = []
        chunk_payloads: list[dict[str, Any]] = []
        ordinal = 0
        for page_number, page in enumerate(reader.pages, start=1):
            text = self._normalize_text(page.extract_text() or "")
            if not text:
                continue
            page_texts.append(f"Page {page_number}\n\n{text}")
            page_chunks = chunk_text(text)
            for entry in page_chunks:
                chunk_payloads.append(
                    {
                        "ordinal": ordinal,
                        "text": entry["text"],
                        "token_count": int(entry.get("token_count", 0)),
                        "metadata": {
                            **dict(entry.get("metadata", {})),
                            "page": page_number,
                            "canonical_uri": canonical_uri,
                            "source_type": "pdf",
                        },
                    }
                )
                ordinal += 1
        if not page_texts:
            raise RuntimeError("unsupported_pdf_no_extractable_text")
        normalized_text = "\n\n".join(page_texts)
        return _ParsedContent(
            source_type="pdf",
            normalized_text=normalized_text,
            title=None,
            author=None,
            published_at=None,
            auto_tags=["pdf"],
            entities=[],
            chunk_payloads=chunk_payloads,
            warning_codes=[],
        )

    def _build_chunks(self, text: str, *, base_metadata: dict[str, Any]) -> list[dict[str, Any]]:
        chunks = []
        for entry in chunk_text(text):
            chunks.append(
                {
                    "ordinal": int(entry["ordinal"]),
                    "text": str(entry["text"]),
                    "token_count": int(entry.get("token_count", 0)),
                    "metadata": {
                        **dict(entry.get("metadata", {})),
                        **{key: value for key, value in base_metadata.items() if value not in (None, "", [], {})},
                    },
                }
            )
        return chunks

    def _write_raw_source(
        self,
        *,
        item_dir: Path,
        item: ArtifactIngestItemRequest,
        resolved: _ResolvedSource,
        source_type: str,
    ) -> Path:
        suffix = self._raw_suffix(item=item, media_type=resolved.media_type, source_type=source_type)
        raw_path = item_dir / f"raw_source{suffix}"
        if resolved.raw_text is not None and resolved.media_type != "application/pdf":
            write_text(raw_path, resolved.raw_text)
        else:
            raw_path.write_bytes(resolved.raw_bytes)
        return raw_path

    def _canonical_uri(self, item: ArtifactIngestItemRequest, resolved: _ResolvedSource) -> str:
        if item.input_kind == "url":
            return self._normalize_url(str(item.url or ""))
        if item.input_kind == "file":
            return Path(str(item.file_path or "")).expanduser().resolve().as_uri()
        digest = hashlib.sha256(resolved.raw_bytes).hexdigest()
        return f"inline://sha256/{digest}"

    @staticmethod
    def _fallback_canonical_uri(item: ArtifactIngestItemRequest) -> str:
        if item.input_kind == "url":
            return item.url or "url://unknown"
        if item.input_kind == "file":
            return item.file_path or "file://unknown"
        content = str(item.content or "").encode("utf-8")
        digest = re.sub(r"[^a-f0-9]", "", content.hex())[:64]
        return f"inline://sha256/{digest or 'unknown'}"

    @staticmethod
    def _normalize_url(url: str) -> str:
        parsed = urlparse(url.strip())
        query = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key and not key.startswith("utm_") and key not in _TRACKING_QUERY_KEYS
        ]
        normalized = parsed._replace(fragment="", query=urlencode(query, doseq=True))
        return urlunparse(normalized)

    @staticmethod
    def _normalize_text(text: str) -> str:
        lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").splitlines()]
        normalized = "\n".join(lines).strip()
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        return normalized.strip()

    @staticmethod
    def _merge_tags(user_tags: list[str], auto_tags: list[str]) -> list[str]:
        merged: list[str] = []
        for tag in [*user_tags, *auto_tags]:
            text = str(tag).strip()
            if text and text not in merged:
                merged.append(text)
        return merged

    @staticmethod
    def _inline_media_type(item: ArtifactIngestItemRequest) -> str:
        if item.content_format == "html":
            return "text/html"
        if item.content_format == "markdown":
            return "text/markdown"
        return "text/plain"

    @staticmethod
    def _guess_media_type(suffix: str) -> str | None:
        mapping = {
            ".pdf": "application/pdf",
            ".html": "text/html",
            ".htm": "text/html",
            ".md": "text/markdown",
            ".markdown": "text/markdown",
            ".txt": "text/plain",
        }
        return mapping.get(suffix)

    @staticmethod
    def _raw_suffix(*, item: ArtifactIngestItemRequest, media_type: str | None, source_type: str) -> str:
        if item.input_kind == "file":
            suffix = Path(str(item.file_path or "")).suffix
            if suffix:
                return suffix
        if media_type == "application/pdf":
            return ".pdf"
        if source_type == "html":
            return ".html"
        if source_type == "markdown":
            return ".md"
        return ".txt"

    @staticmethod
    def _html_to_text(raw_html: str) -> str:
        if BeautifulSoup is not None:
            soup = BeautifulSoup(raw_html, "html.parser")
            for element in soup(["script", "style", "noscript"]):
                element.decompose()
            return "\n".join(part.strip() for part in soup.get_text("\n").splitlines() if part.strip())
        text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", raw_html)
        text = re.sub(r"(?s)<[^>]+>", " ", text)
        return _WHITESPACE_RE.sub(" ", text).strip()

    @staticmethod
    def _html_title(raw_html: str) -> str | None:
        if BeautifulSoup is not None:
            soup = BeautifulSoup(raw_html, "html.parser")
            for selector in ("meta[property='og:title']", "meta[name='twitter:title']", "title"):
                element = soup.select_one(selector)
                if element is None:
                    continue
                if element.name == "meta":
                    text = str(element.get("content") or "").strip()
                else:
                    text = element.get_text(strip=True)
                if text:
                    return text
        return None

    @staticmethod
    def _html_meta(raw_html: str, names: tuple[str, ...]) -> str | None:
        if BeautifulSoup is None:
            return None
        soup = BeautifulSoup(raw_html, "html.parser")
        selectors = [
            *(f"meta[name='{name}']" for name in names),
            *(f"meta[property='{name}']" for name in names),
        ]
        for selector in selectors:
            element = soup.select_one(selector)
            if element is None:
                continue
            text = str(element.get("content") or "").strip()
            if text:
                return text
        return None

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        try:
            parsed = parsedate_to_datetime(text)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _infer_entities(text: str) -> list[str]:
        entities: list[str] = []
        for match in _ENTITY_RE.finditer(text):
            value = match.group(0).strip()
            if value and value not in entities:
                entities.append(value)
            if len(entities) >= 8:
                break
        return entities
