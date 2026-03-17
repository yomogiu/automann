from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from libs.config import Settings
from libs.contracts.events import EventName
from libs.contracts.models import (
    AdapterResult,
    EventSuggestion,
    ObservationRecord,
    ReportRecord,
    WorkerStatus,
)
from libs.contracts.workers import (
    DraftGenerationOutput,
    DraftGenerationRequest,
    PublicationGenerationOutput,
    PublicationGenerationRequest,
)

from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text
from workers.codex_runner import CodexCliRequest, CodexCliRunner


PUBLICATION_TRANSFORM_PROMPT_PATH = (
    Path(__file__).resolve().parents[2] / "libs" / "prompts" / "publication_transform.md"
)


def load_publication_transform_prompt() -> str:
    return PUBLICATION_TRANSFORM_PROMPT_PATH.read_text(encoding="utf-8")


class DraftWriter:
    worker_key = "draft_writer"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: DraftGenerationRequest | PublicationGenerationRequest) -> AdapterResult:
        if self._is_source_driven_request(request):
            publication_request = self._coerce_publication_request(request)
            return self._run_source_driven_publication(publication_request)
        if isinstance(request, DraftGenerationRequest):
            return self._run_legacy_draft(request)
        raise TypeError(f"Unsupported request type for DraftWriter: {type(request)!r}")

    @staticmethod
    def _is_source_driven_request(request: DraftGenerationRequest | PublicationGenerationRequest) -> bool:
        if isinstance(request, PublicationGenerationRequest):
            return True
        source_markdown = str(request.metadata.get("source_markdown") or "").strip()
        return bool(source_markdown)

    @staticmethod
    def _optional_string(value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    def _coerce_publication_request(
        self,
        request: DraftGenerationRequest | PublicationGenerationRequest,
    ) -> PublicationGenerationRequest:
        if isinstance(request, PublicationGenerationRequest):
            return request
        metadata = dict(request.metadata)
        source_markdown = str(metadata.get("source_markdown") or "").strip()
        if not source_markdown:
            raise ValueError("source_markdown is required for source-driven publication mode")
        return PublicationGenerationRequest(
            source_report_id=self._optional_string(metadata.get("source_report_id")),
            source_revision_id=self._optional_string(metadata.get("source_revision_id")),
            theme=request.theme,
            source_markdown=source_markdown,
            metadata=metadata,
        )

    def _run_legacy_draft(self, request: DraftGenerationRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        markdown_path = run_dir / "substack_draft.md"
        bundle_path = run_dir / "source_bundle.json"
        evidence_pack = list(request.evidence_pack)
        output = DraftGenerationOutput(
            theme=request.theme,
            evidence_count=len(evidence_pack),
            source_report_ids=list(request.source_report_ids),
        )

        markdown = self._render_draft(request, evidence_pack)
        write_text(markdown_path, markdown)
        write_json(
            bundle_path,
            {
                "theme": request.theme,
                "evidence_pack": evidence_pack,
                "source_report_ids": list(request.source_report_ids),
                "metadata": dict(request.metadata),
                "mode": "legacy-template",
            },
        )

        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"Drafted article for theme '{request.theme}'.",
            artifact_manifest=[
                build_file_artifact(
                    kind="draft",
                    path=markdown_path,
                    media_type="text/markdown",
                ),
                build_file_artifact(
                    kind="source-bundle",
                    path=bundle_path,
                    media_type="application/json",
                ),
            ],
            structured_outputs=output.model_dump(mode="json"),
            observations=[
                ObservationRecord(
                    kind="draft_theme",
                    summary=f"Draft centered on '{request.theme}'.",
                    payload={
                        **output.model_dump(mode="json"),
                        "mode": "legacy-template",
                    },
                    confidence=0.75,
                )
            ],
            reports=[
                ReportRecord(
                    report_type="substack_draft",
                    title=f"Draft: {request.theme}",
                    summary=f"Generated long-form draft from {len(evidence_pack)} evidence items.",
                    content_markdown=markdown,
                    artifact_path=str(markdown_path),
                    metadata={
                        "theme": request.theme,
                        "source_report_ids": list(request.source_report_ids),
                        "mode": "legacy-template",
                        "taxonomy": {
                            "filters": ["report"],
                            "tags": ["deep_research"],
                        },
                    },
                )
            ],
            next_suggested_events=[
                EventSuggestion(name=EventName.DRAFT_GENERATED, payload={"theme": request.theme})
            ],
        )

    def _run_source_driven_publication(self, request: PublicationGenerationRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        markdown_path = run_dir / "substack_draft.md"
        bundle_path = run_dir / "source_bundle.json"

        codex_runner = CodexCliRunner(self.settings)
        codex_result = codex_runner.run(
            CodexCliRequest(
                prompt=self._render_publication_prompt(request),
                cwd=str(run_dir),
            )
        )
        codex_text = codex_result.stdout.strip()
        used_codex = codex_result.status == WorkerStatus.COMPLETED and bool(codex_text)
        markdown = codex_text if used_codex else self._render_publication_fallback(request)
        write_text(markdown_path, markdown)
        mode = "codex-cli" if used_codex else "template-fallback"

        write_json(
            bundle_path,
            {
                "theme": request.theme,
                "source_report_id": request.source_report_id,
                "source_revision_id": request.source_revision_id,
                "source_markdown": request.source_markdown,
                "metadata": dict(request.metadata),
                "mode": mode,
                "codex": {
                    "status": codex_result.status.value,
                    "stderr": codex_result.stderr,
                    "structured_outputs": codex_result.structured_outputs,
                },
            },
        )

        title = self._extract_title(markdown) or request.theme or "Publication Draft"
        publication_output = PublicationGenerationOutput(
            title=title,
            source_report_id=request.source_report_id,
            source_revision_id=request.source_revision_id,
            mode=mode,
        )

        source_report_ids = [request.source_report_id] if request.source_report_id else []
        compatibility_output = {
            "theme": request.theme or title,
            "evidence_count": 0,
            "source_report_ids": source_report_ids,
            **publication_output.model_dump(mode="json"),
        }
        stdout = (
            f"Transformed source markdown into publication draft via Codex CLI ({title})."
            if mode == "codex-cli"
            else f"Transformed source markdown into publication draft with fallback template ({title})."
        )

        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=stdout,
            stderr=codex_result.stderr if mode != "codex-cli" else "",
            artifact_manifest=[
                build_file_artifact(
                    kind="draft",
                    path=markdown_path,
                    media_type="text/markdown",
                ),
                build_file_artifact(
                    kind="source-bundle",
                    path=bundle_path,
                    media_type="application/json",
                ),
            ],
            structured_outputs=compatibility_output,
            observations=[
                ObservationRecord(
                    kind="publication_transform",
                    summary=f"Generated publication draft using mode '{mode}'.",
                    payload=publication_output.model_dump(mode="json"),
                    confidence=0.85 if mode == "codex-cli" else 0.65,
                )
            ],
            reports=[
                ReportRecord(
                    report_type="substack_draft",
                    title=title,
                    summary=f"Generated publication draft from source markdown using mode '{mode}'.",
                    content_markdown=markdown,
                    artifact_path=str(markdown_path),
                    metadata={
                        "mode": mode,
                        "source_report_id": request.source_report_id,
                        "source_revision_id": request.source_revision_id,
                        "theme": request.theme,
                        "taxonomy": {
                            "filters": ["report"],
                            "tags": ["deep_research"],
                        },
                    },
                )
            ],
            next_suggested_events=[
                EventSuggestion(
                    name=EventName.DRAFT_GENERATED,
                    payload={
                        "theme": request.theme or title,
                        "mode": mode,
                        "source_report_id": request.source_report_id,
                        "source_revision_id": request.source_revision_id,
                    },
                )
            ],
        )

    def _render_publication_prompt(self, request: PublicationGenerationRequest) -> str:
        prompt = load_publication_transform_prompt().strip()
        payload = {
            "theme": request.theme,
            "source_report_id": request.source_report_id,
            "source_revision_id": request.source_revision_id,
            "metadata": request.metadata,
        }
        return "\n\n".join(
            [
                prompt,
                "Return markdown only. Do not wrap output in code fences.",
                "Input metadata:",
                json.dumps(payload, indent=2, sort_keys=True),
                "Source markdown:",
                request.source_markdown,
            ]
        )

    def _render_publication_fallback(self, request: PublicationGenerationRequest) -> str:
        title = request.theme or "Publication Draft"
        source_id = request.source_revision_id or request.source_report_id or "n/a"
        preview_lines = [line.strip() for line in request.source_markdown.splitlines() if line.strip()][:8]
        if not preview_lines:
            preview_lines = ["No source content provided."]
        lines = [
            f"# {title}",
            "",
            f"_Source reference: {source_id}_",
            "",
            "## Draft",
            "This publication draft was generated from source research material.",
            "",
            "## Source Highlights",
        ]
        for line in preview_lines:
            lines.append(f"- {line}")
        lines.extend(
            [
                "",
                "## Source Material",
                request.source_markdown,
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def _extract_title(markdown: str) -> str | None:
        for line in markdown.splitlines():
            stripped = line.strip()
            if stripped.startswith("# "):
                title = stripped[2:].strip()
                if title:
                    return title
        return None

    def _render_draft(self, request: DraftGenerationRequest, evidence_pack: list[dict[str, Any]]) -> str:
        lines = [
            f"# {request.theme}",
            "",
            "## Outline",
            "- Why this theme matters now",
            "- What changed since the previous cycle",
            "- Evidence and counterarguments",
            "- Operational implications",
            "",
            "## Draft",
            (
                "This draft argues that local research systems work better when orchestration, "
                "state, and publication are treated as separate concerns with explicit contracts."
            ),
            "",
            "## Evidence Pack",
        ]
        for item in evidence_pack:
            lines.append(f"- {item.get('text', 'Untitled evidence')}")
        return "\n".join(lines)
