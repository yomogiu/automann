from __future__ import annotations

from pathlib import Path
from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import PaperReviewRequest
from libs.db import LifeRepository, engine_for_url
from libs.retrieval import chunk_text
from workers.ingest_runner import ArxivReviewRunner

from .common import execute_adapter


@flow(name="paper-review")
def paper_review_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = LifeRepository(engine_for_url(settings.life_database_url))
    paper_request = PaperReviewRequest.model_validate(request)
    runner = ArxivReviewRunner(settings)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        review_result = execute_adapter(
            flow_name="paper_review_flow",
            worker_key=runner.worker_key,
            input_payload=paper_request.model_dump(mode="json"),
            runner=lambda: runner.review(paper_request),
            parent_run_id=run_id,
        )

        artifacts_by_kind = {
            str(item.get("kind") or ""): item
            for item in review_result.get("artifacts", [])
            if isinstance(item, dict)
        }
        annotation_artifact = artifacts_by_kind.get("paper-annotations")
        html_artifact = artifacts_by_kind.get("annotated-paper")
        if isinstance(review_result.get("structured_outputs"), dict):
            review_result["structured_outputs"]["annotation_json_artifact_id"] = (
                annotation_artifact.get("id") if annotation_artifact else None
            )
            review_result["structured_outputs"]["annotated_html_artifact_id"] = (
                html_artifact.get("id") if html_artifact else None
            )
        report_entries = review_result.get("reports", [])
        if isinstance(report_entries, list):
            for report_entry in report_entries:
                if not isinstance(report_entry, dict):
                    continue
                report_id = str(report_entry.get("id") or "")
                metadata = report_entry.get("metadata")
                if not report_id or not isinstance(metadata, dict):
                    continue
                enriched_metadata = {
                    **metadata,
                    "annotation_json_artifact_id": annotation_artifact.get("id") if annotation_artifact else None,
                    "annotated_html_artifact_id": html_artifact.get("id") if html_artifact else None,
                }
                updated_report = repository.update_report_metadata(report_id, metadata=enriched_metadata)
                report_entry["metadata"] = updated_report.metadata_json if updated_report is not None else enriched_metadata

        if review_result["artifacts"]:
            artifact_record = next(
                (item for item in review_result["artifacts"] if item["kind"] == "review-card"),
                review_result["artifacts"][0],
            )
            artifact_path = Path(artifact_record["path"])
            chunk_count = 0
            if artifact_path.exists():
                text = artifact_path.read_text(encoding="utf-8")
                chunks = chunk_text(text)
                chunk_count = repository.upsert_chunks(artifact_id=artifact_record["id"], chunks=chunks)
                review_result["chunk_count"] = chunk_count

        if run_id is not None:
            repository.update_run_status(
                run_id,
                status="completed",
                stdout=f"Completed paper review for {paper_request.paper_id}",
                structured_outputs=review_result,
                artifact_manifest=review_result.get("artifacts", []),
                observation_summary=review_result.get("observations", []),
                next_suggested_events=review_result.get("next_events", []),
            )
        return review_result
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
