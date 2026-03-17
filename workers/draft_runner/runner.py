from __future__ import annotations

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
from libs.contracts.workers import DraftGenerationOutput, DraftGenerationRequest

from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text


class DraftWriter:
    worker_key = "draft_writer"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: DraftGenerationRequest) -> AdapterResult:
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
                    payload=output.model_dump(mode="json"),
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
