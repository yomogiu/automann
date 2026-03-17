from __future__ import annotations

from pathlib import Path
from typing import Any

from prefect import flow
from pydantic import BaseModel, Field, model_validator

from libs.config import get_settings
from libs.contracts.models import AdapterResult, ObservationRecord, ReportRecord, WorkerStatus
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text

from .common import build_repository, execute_adapter, execute_child_flow
from .paper_review import paper_review_flow
from .registry import FLOW_SPECS


class PaperBatchItem(BaseModel):
    paper_id: str
    source_url: str | None = None
    title: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PaperBatchRequest(BaseModel):
    title: str | None = None
    compare_prompt: str | None = None
    papers: list[PaperBatchItem] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_non_empty(self) -> PaperBatchRequest:
        if not self.papers:
            raise ValueError("papers must not be empty")
        return self


def _render_batch_markdown(
    *,
    title: str,
    compare_prompt: str | None,
    child_runs: list[dict[str, Any]],
) -> str:
    completed = [item for item in child_runs if str(item.get("status")) == WorkerStatus.COMPLETED.value]
    failed = [item for item in child_runs if str(item.get("status")) == WorkerStatus.FAILED.value]
    lines = [
        f"# {title}",
        "",
        "## Batch Summary",
        f"- Total papers: {len(child_runs)}",
        f"- Completed reviews: {len(completed)}",
        f"- Failed reviews: {len(failed)}",
    ]
    if compare_prompt:
        lines.extend(["", "## Comparison Prompt", compare_prompt])

    lines.extend(["", "## Reviews"])
    for item in child_runs:
        result = item.get("result") if isinstance(item.get("result"), dict) else {}
        structured = result.get("structured_outputs") if isinstance(result.get("structured_outputs"), dict) else {}
        reports = result.get("reports") if isinstance(result.get("reports"), list) else []
        report_title = ""
        report_summary = ""
        if reports:
            first_report = reports[0] if isinstance(reports[0], dict) else {}
            report_title = str(first_report.get("title") or "")
            report_summary = str(first_report.get("summary") or "")
        title_line = report_title or str(structured.get("title") or structured.get("paper_id") or item.get("run_id"))
        lines.append(f"### {title_line}")
        lines.append(f"- Status: {item.get('status')}")
        if structured.get("source_url"):
            lines.append(f"- Source: {structured['source_url']}")
        if structured.get("presentation_mode"):
            lines.append(f"- Presentation mode: {structured['presentation_mode']}")
        if report_summary:
            lines.append(f"- Summary: {report_summary}")
        elif structured.get("summary"):
            lines.append(f"- Summary: {structured['summary']}")
        lines.append("")

    if failed:
        lines.extend(["## Failures", ""])
        for item in failed:
            result = item.get("result") if isinstance(item.get("result"), dict) else {}
            stderr = str(result.get("stderr") or "Unknown failure")
            lines.append(f"- {item.get('run_id')}: {stderr}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _run_paper_batch(
    request: PaperBatchRequest,
    *,
    parent_run_id: str | None,
) -> AdapterResult:
    settings = get_settings()
    run_dir = ensure_worker_dir(settings, "paper_batch_runner")
    child_runs: list[dict[str, Any]] = []

    for paper in request.papers:
        payload = {
            "paper_id": paper.paper_id,
            "source_url": paper.source_url,
            "title": paper.title,
            "metadata": dict(paper.metadata),
        }
        child_runs.append(
            execute_child_flow(
                flow_name="paper_review_flow",
                worker_key=FLOW_SPECS["paper_review_flow"].default_work_pool,
                input_payload=payload,
                runner=lambda child_run_id, child_payload=payload: paper_review_flow.fn(
                    request=child_payload,
                    run_id=child_run_id,
                ),
                parent_run_id=parent_run_id,
            )
        )

    batch_title = request.title or f"Paper Batch ({len(request.papers)} papers)"
    markdown = _render_batch_markdown(
        title=batch_title,
        compare_prompt=request.compare_prompt,
        child_runs=child_runs,
    )
    markdown_path = run_dir / "paper_batch_summary.md"
    manifest_path = run_dir / "paper_batch_manifest.json"
    write_text(markdown_path, markdown)
    manifest = {
        "title": batch_title,
        "paper_count": len(request.papers),
        "completed_count": sum(1 for item in child_runs if item.get("status") == WorkerStatus.COMPLETED.value),
        "failed_count": sum(1 for item in child_runs if item.get("status") == WorkerStatus.FAILED.value),
        "child_runs": [
            {
                "run_id": item.get("run_id"),
                "status": item.get("status"),
                "report_ids": [
                    report.get("id")
                    for report in (item.get("result") or {}).get("reports", [])
                    if isinstance(report, dict) and report.get("id")
                ],
            }
            for item in child_runs
        ],
    }
    write_json(manifest_path, manifest)

    status = WorkerStatus.COMPLETED if manifest["completed_count"] > 0 else WorkerStatus.FAILED
    observations = [
        ObservationRecord(
            kind="paper_batch_summary",
            summary=f"Completed {manifest['completed_count']} of {manifest['paper_count']} paper reviews.",
            payload=manifest,
            confidence=0.7,
        )
    ]
    reports = [
        ReportRecord(
            report_type="paper_batch",
            title=batch_title,
            summary=f"{manifest['completed_count']} completed, {manifest['failed_count']} failed.",
            content_markdown=markdown,
            artifact_path=str(markdown_path),
            metadata=manifest,
        )
    ]
    return AdapterResult(
        status=status,
        stdout=f"Processed paper batch with {manifest['paper_count']} papers.",
        artifact_manifest=[
            build_file_artifact(kind="paper-batch-summary", path=markdown_path, media_type="text/markdown"),
            build_file_artifact(kind="paper-batch-manifest", path=manifest_path, media_type="application/json"),
        ],
        structured_outputs={
            **manifest,
            "child_runs": child_runs,
        },
        observations=observations,
        reports=reports,
    )


@flow(name="paper-batch")
def paper_batch_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    repository = build_repository()
    batch_request = PaperBatchRequest.model_validate(request)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="paper_batch_flow",
            worker_key="paper_batch_runner",
            input_payload=batch_request.model_dump(mode="json"),
            runner=lambda: _run_paper_batch(batch_request, parent_run_id=run_id),
            parent_run_id=run_id,
        )
        if run_id is not None:
            child_runs = result.get("structured_outputs", {}).get("child_runs", [])
            child_artifacts = [
                artifact
                for item in child_runs
                if isinstance(item, dict)
                for artifact in item.get("artifacts", [])
                if isinstance(artifact, dict)
            ]
            child_observations = [
                observation
                for item in child_runs
                if isinstance(item, dict)
                for observation in item.get("observations", [])
                if isinstance(observation, dict)
            ]
            repository.update_run_status(
                run_id,
                status=str(result.get("status") or WorkerStatus.COMPLETED.value),
                stdout=result.get("stdout") or f"Completed paper batch with {len(batch_request.papers)} papers",
                stderr=result.get("stderr") or "",
                structured_outputs=result,
                artifact_manifest=child_artifacts + list(result.get("artifacts", [])),
                observation_summary=child_observations + list(result.get("observations", [])),
                next_suggested_events=list(result.get("next_events", [])),
            )
        return result
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
