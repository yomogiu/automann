from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Any

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

from workers.codex_runner import CodexCliRequest, CodexCliRunner, load_paper_review_prompt
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text


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
        items = list(request.seed_news or DEFAULT_NEWS_ITEMS)
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
        }

        observations = [
            ObservationRecord(
                kind="paper_review_mode",
                summary=f"Presentation mode: {presentation_mode}",
                payload={
                    "paper_id": request.paper_id,
                    "presentation_mode": presentation_mode,
                    "source_kind": source_kind,
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
        if self._looks_like_pdf(source_url):
            return {
                "source_url": source_url,
                "source_kind": "pdf",
                "blocks": [],
                "fallback_reason": "pdf_only_source",
            }

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
            }

        return {
            "source_url": source_url,
            "source_kind": "arxiv_unavailable",
            "blocks": [],
            "fallback_reason": "no_html_or_text_payload_available",
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
        for key in ("paper", "concepts", "blocks", "annotations"):
            if key not in payload:
                raise ValueError(f"missing_key:{key}")
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
            "blocks": blocks[:40],
        }
        return "\n\n".join(
            [
                base_prompt,
                "Return strict JSON only. Do not include markdown fences.",
                "Use dense paragraphs where needed and assume a serious learner audience.",
                "Separate extracted paper claims from your inferences in analysis text.",
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
        return "\n".join(lines)

    @staticmethod
    def _best_summary(annotation_payload: dict[str, Any]) -> str:
        concepts = annotation_payload.get("concepts", [])
        if concepts:
            first = concepts[0]
            return str(first.get("summary") or f"Core concept: {first.get('name', 'n/a')}")
        annotations = annotation_payload.get("annotations", [])
        if annotations:
            return str(annotations[0].get("analysis") or "Annotated analysis completed.")
        return "Structured annotation completed."

    def _render_annotated_html(self, payload: dict[str, Any]) -> str:
        paper = payload.get("paper", {})
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
                rendered = re.sub(re.escape(anchor_escaped), replacement, rendered, count=1, flags=re.IGNORECASE)
            return (
                f'<article class="paper-block"><div class="block-meta">{html.escape(str(block.get("section_label", "Body")))}'
                f' · {html.escape(block_id)}</div><p>{rendered}</p></article>'
            )

        annotations_json = json.dumps(annotations).replace("</", "<\\/")
        concepts_json = json.dumps(concepts_by_id).replace("</", "<\\/")

        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(str(paper.get("title", "Annotated Paper Review")))}</title>
  <style>
    body {{ margin: 0; font-family: Georgia, "Times New Roman", serif; color: #14171f; background: #f2f4f8; }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 24px; display: grid; grid-template-columns: 2fr 1fr; gap: 18px; }}
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
    <aside class="card">
      <h2 class="panel-title">Pinned Analysis</h2>
      <p class="muted">Hover for a teaser, then click a highlighted span to pin full analysis.</p>
      <div id="panel-content" class="muted">No annotation pinned yet.</div>
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
      const perspectives = concept.perspectives || {{}};
      return `
        <section class="panel-section">
          <h3>${{escapeHtml(concept.name || "Concept")}}</h3>
          <p>${{escapeHtml(concept.summary || "")}}</p>
          <h4>CS50 lens</h4>
          <p>${{escapeHtml(perspectives.cs50_student || "Not provided.")}}</p>
          <h4>Senior engineer lens</h4>
          <p>${{escapeHtml(perspectives.senior_engineer || "Not provided.")}}</p>
          <h4>Staff ML engineer lens</h4>
          <p>${{escapeHtml(perspectives.staff_ml_engineer || "Not provided.")}}</p>
        </section>
      `;
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

      panel.innerHTML = `
        <section class="panel-section">
          <h3>${{escapeHtml(ann.label || "AI Analysis")}}</h3>
          <p>${{escapeHtml(ann.analysis || "")}}</p>
          <p class="muted">Kind: ${{escapeHtml(ann.kind || "analysis")}} · Confidence: ${{escapeHtml(ann.confidence ?? "n/a")}}</p>
        </section>
        ${{conceptHtml || '<p class="muted">No concept perspectives linked.</p>'}}
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
