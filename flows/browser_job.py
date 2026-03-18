from __future__ import annotations

from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import BrowserJobRequest, ObservationRecord
from libs.contracts.workers import BrowserTaskRequest
from flows.artifact_ingest import WORKER_KEY as ARTIFACT_INGEST_WORKER_KEY, artifact_ingest_flow
from workers.browser_runner import BrowserTaskRunner

from .common import build_repository, execute_adapter, execute_child_flow


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _primary_page_artifact(artifacts: list[dict[str, Any]]) -> dict[str, Any] | None:
    for artifact in artifacts:
        kind = str(artifact.get("kind") or "").strip().lower()
        metadata = _as_dict(artifact.get("metadata"))
        role = str(metadata.get("role") or "").strip().lower()
        if role == "primary_page" or kind == "browser-page":
            return artifact
    for artifact in artifacts:
        if str(artifact.get("kind") or "").strip().lower() == "browser-html":
            return artifact
    return None


def _build_browser_ingest_request(
    *,
    result: dict[str, Any],
    artifact: dict[str, Any],
) -> dict[str, Any]:
    structured = _as_dict(result.get("structured_outputs"))
    artifact_metadata = _as_dict(artifact.get("metadata"))
    final_url = str(structured.get("final_url") or result.get("target_url") or artifact_metadata.get("final_url") or "").strip()
    page_title = str(structured.get("page_title") or artifact_metadata.get("page_title") or "").strip()
    job_name = str(structured.get("job_name") or result.get("job_name") or "").strip()
    metadata = {
        "canonical_uri": final_url,
        "browser_run_id": result.get("run_id"),
        "browser_job_name": job_name,
        "target_url": structured.get("target_url") or result.get("target_url"),
        "final_url": final_url,
        "page_title": page_title,
        "browser_artifact_id": artifact.get("id"),
        "browser_artifact_kind": artifact.get("kind"),
        "browser_artifact_path": artifact.get("path"),
    }
    return {
        "items": [
            {
                "input_kind": "file",
                "file_path": artifact.get("path"),
                "canonical_uri": final_url,
                "declared_media_type": "text/html",
                "title": page_title or job_name or final_url,
                "metadata": metadata,
            }
        ],
        "metadata": {
            "browser_run_id": result.get("run_id"),
            "browser_job_name": job_name,
            "target_url": structured.get("target_url") or result.get("target_url"),
            "final_url": final_url,
            "page_title": page_title,
        },
    }


def _handoff_to_artifact_ingest(
    *,
    result: dict[str, Any],
    parent_run_id: str | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    artifacts = [artifact for artifact in result.get("artifacts", []) if isinstance(artifact, dict)]
    page_artifact = _primary_page_artifact(artifacts)
    if page_artifact is None:
        handoff = {
            "status": "skipped",
            "reason": "missing_primary_page_artifact",
            "child_run_id": None,
            "input_count": 0,
            "source_document_ids": [],
        }
        observation = ObservationRecord(
            kind="artifact_ingest_handoff_skipped",
            summary="Skipped artifact ingest handoff because no primary browser page artifact was available.",
            payload=handoff,
            confidence=0.7,
        ).model_dump(mode="json")
        return handoff, [observation]

    ingest_request = _build_browser_ingest_request(result=result, artifact=page_artifact)
    child_result = execute_child_flow(
        flow_name="artifact_ingest_flow",
        worker_key=ARTIFACT_INGEST_WORKER_KEY,
        input_payload=ingest_request,
        runner=lambda child_run_id: artifact_ingest_flow.fn(request=ingest_request, run_id=child_run_id),
        parent_run_id=parent_run_id,
    )
    child_outputs = _as_dict(child_result.get("structured_outputs"))
    child_items = child_outputs.get("items") if isinstance(child_outputs.get("items"), list) else []
    source_document_ids: list[str] = []
    for item in child_items:
        if not isinstance(item, dict):
            continue
        source_document_id = str(item.get("source_document_id") or "").strip()
        if source_document_id and source_document_id not in source_document_ids:
            source_document_ids.append(source_document_id)
    handoff = {
        "status": str(child_result.get("status") or "completed"),
        "child_run_id": child_result.get("run_id"),
        "input_count": len(ingest_request.get("items", [])),
        "source_document_ids": source_document_ids,
        "ingested_count": int(child_outputs.get("success_count") or 0),
        "failed_count": int(child_outputs.get("failure_count") or 0),
        "warning_count": int(child_outputs.get("warning_count") or 0),
        "browser_artifact_id": page_artifact.get("id"),
        "browser_artifact_path": page_artifact.get("path"),
        "canonical_uri": ingest_request["items"][0].get("canonical_uri"),
    }
    observation = ObservationRecord(
        kind="artifact_ingest_handoff_completed",
        summary=f"Created artifact ingest child run {handoff['child_run_id']} for browser job {result.get('run_id')}.",
        payload=handoff,
        confidence=0.85,
    ).model_dump(mode="json")
    return handoff, [observation]


@flow(name="browser-job")
def browser_job_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = build_repository(settings)
    command_request = BrowserJobRequest.model_validate(request)
    browser_request = BrowserTaskRequest.from_command(command_request)
    runner = BrowserTaskRunner(settings)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="browser_job_flow",
            worker_key=runner.worker_key,
            input_payload=browser_request.model_dump(mode="json"),
            runner=lambda: runner.run(browser_request),
            parent_run_id=run_id,
        )
        structured_outputs = _as_dict(result.get("structured_outputs"))
        observations = list(result.get("observations", []))
        if str(result.get("status") or "").lower() == "completed":
            try:
                ingest_handoff, handoff_observations = _handoff_to_artifact_ingest(
                    result=result,
                    parent_run_id=run_id,
                )
            except Exception as exc:
                ingest_handoff = {
                    "status": "failed",
                    "reason": "handoff_error",
                    "error": str(exc),
                    "child_run_id": None,
                    "input_count": 0,
                    "source_document_ids": [],
                }
                handoff_observations = [
                    ObservationRecord(
                        kind="artifact_ingest_handoff_failed",
                        summary="Artifact ingest handoff failed after a successful browser run.",
                        payload=ingest_handoff,
                        confidence=0.6,
                    ).model_dump(mode="json")
                ]
            structured_outputs["ingest_handoff"] = ingest_handoff
            observations.extend(handoff_observations)
            result = {
                **result,
                "structured_outputs": structured_outputs,
                "observations": observations,
            }
        if run_id is not None:
            final_status = str(result.get("status") or "completed")
            repository.update_run_status(
                run_id,
                status=final_status,
                stdout=result.get("stdout") or f"Completed browser job {browser_request.job_name}",
                stderr=result.get("stderr") or "",
                structured_outputs=result,
                artifact_manifest=result.get("artifacts", []),
                observation_summary=result.get("observations", []),
                next_suggested_events=result.get("next_events", []),
            )
        return result
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
