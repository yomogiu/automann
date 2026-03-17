from __future__ import annotations

import re
from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import HumanMode, ResearchReportRequest
from libs.contracts.workers import ResearchReportWorkerRequest
from libs.db import LifeRepository, engine_for_url
from libs.retrieval import RetrievalService
from workers.research_runner import ResearchReportRunner

from .common import execute_adapter


PROMOTE_REVISION_CHECKPOINT = "promote_revision"


def _derive_report_key(theme: str, report_key: str | None) -> str:
    if report_key:
        return report_key.strip()
    normalized = re.sub(r"[^a-z0-9]+", "-", theme.lower()).strip("-")
    return normalized or "research-report"


def _checkpoint_required(request: ResearchReportRequest, run_id: str | None) -> bool:
    if run_id is None or request.human_policy.mode == HumanMode.AUTO:
        return False
    if request.human_policy.mode not in {HumanMode.CHECKPOINTED, HumanMode.LIVE}:
        return False
    checkpoints = set(request.human_policy.checkpoints or [])
    return not checkpoints or PROMOTE_REVISION_CHECKPOINT in checkpoints


@flow(name="research-report")
def research_report_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = LifeRepository(engine_for_url(settings.life_database_url))
    retrieval = RetrievalService(repository)
    research_request = ResearchReportRequest.model_validate(request)
    report_key = _derive_report_key(research_request.theme, research_request.report_key)
    current_revision = repository.current_report_revision(report_key)
    next_revision_number = (current_revision.revision_number if current_revision is not None else 0) + 1
    query_parts = [research_request.theme, *research_request.boundaries, *research_request.areas_of_interest]
    retrieval_context = retrieval.query(query=" ".join(part for part in query_parts if part), limit=12)
    worker_request = ResearchReportWorkerRequest(
        theme=research_request.theme,
        boundaries=list(research_request.boundaries),
        areas_of_interest=list(research_request.areas_of_interest),
        report_key=report_key,
        edit_mode=research_request.edit_mode.value,
        revision_number=next_revision_number,
        supersedes_report_id=current_revision.id if current_revision is not None else None,
        previous_report={
            "id": current_revision.id,
            "title": current_revision.title,
            "summary": current_revision.summary,
            "content_markdown": current_revision.content_markdown,
            "metadata": current_revision.metadata_json,
            "revision_number": current_revision.revision_number,
        }
        if current_revision is not None
        else None,
        retrieval_context=retrieval_context,
        metadata=dict(research_request.metadata),
    )
    runner = ResearchReportRunner(settings)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="research_report_flow",
            worker_key=runner.worker_key,
            input_payload=worker_request.model_dump(mode="json"),
            runner=lambda: runner.run(worker_request),
            parent_run_id=run_id,
        )

        report_entries = result.get("reports", [])
        report_id = str(report_entries[0].get("id") or "") if report_entries else ""
        promoted_report_id = current_revision.id if current_revision is not None else None
        checkpoint: dict[str, Any] | None = None

        if report_id:
            if _checkpoint_required(research_request, run_id):
                interaction = repository.create_interaction(
                    run_id=run_id,
                    title=f"Promote research revision {next_revision_number}",
                    prompt_md=(
                        f"Review the generated research revision for `{report_key}` and decide whether to promote "
                        "it as the current report revision."
                    ),
                    input_schema={
                        "type": "object",
                        "properties": {
                            "approved": {"type": "boolean"},
                            "notes": {"type": "string"},
                        },
                        "required": ["approved"],
                    },
                    default_input={"approved": False},
                    checkpoint_key=PROMOTE_REVISION_CHECKPOINT,
                    ui_hints={
                        "report_id": report_id,
                        "report_series_id": report_key,
                        "revision_number": next_revision_number,
                    },
                )
                checkpoint = {
                    "status": "waiting_input",
                    "interaction_id": interaction.id,
                    "checkpoint_key": PROMOTE_REVISION_CHECKPOINT,
                }
            else:
                promoted = repository.promote_report_revision(report_id)
                promoted_report_id = promoted.id if promoted is not None else None

        flow_result = {
            "report_key": report_key,
            "revision_number": next_revision_number,
            "current_report_id": promoted_report_id,
            "pending_report_id": report_id if checkpoint is not None else None,
            "research_run": result,
            "checkpoint": checkpoint,
        }

        if run_id is not None:
            if checkpoint is None:
                repository.update_run_status(
                    run_id,
                    status="completed",
                    stdout=f"Completed research report revision {next_revision_number} for {report_key}",
                    structured_outputs=flow_result,
                    artifact_manifest=result.get("artifacts", []),
                    observation_summary=result.get("observations", []),
                    next_suggested_events=result.get("next_events", []),
                )
            else:
                repository.update_run_status(
                    run_id,
                    status="waiting_input",
                    stdout=f"Awaiting promotion decision for research report {report_key}",
                    structured_outputs=flow_result,
                    artifact_manifest=result.get("artifacts", []),
                    observation_summary=result.get("observations", []),
                    next_suggested_events=result.get("next_events", []),
                )
        return flow_result
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
