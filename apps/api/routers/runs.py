from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.dependencies import orchestration_dep, repository_dep
from apps.api.services import OrchestrationService
from libs.db import LifeRepository
from libs.contracts.models import RunSubmitRequest


router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("")
def list_runs(
    limit: int = 25,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return {"runs": [item.model_dump(mode="json") for item in orchestration.list_runs(limit=limit)]}


@router.get("/{run_id}")
def get_run(
    run_id: str,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    row = repository.get_run(run_id)
    codex_session = repository.get_run_codex_session(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return {
        "id": row.id,
        "task_spec_id": row.task_spec_id,
        "parent_run_id": row.parent_run_id,
        "flow_name": row.flow_name,
        "worker_key": row.worker_key,
        "prefect_flow_run_id": row.prefect_flow_run_id,
        "status": row.status,
        "schema_version": row.schema_version,
        "command_id": row.command_id,
        "session_id": row.session_id,
        "correlation_id": row.correlation_id,
        "idempotency_key": row.idempotency_key,
        "actor": row.actor,
        "codex_session": codex_session.model_dump(mode="json") if codex_session is not None else None,
        "input_payload": row.input_payload,
        "structured_outputs": row.structured_outputs,
        "artifact_manifest": row.artifact_manifest,
        "observation_summary": row.observation_summary,
        "next_suggested_events": row.next_suggested_events,
        "stdout": row.stdout,
        "stderr": row.stderr,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


@router.post("")
async def submit_run(
    request: RunSubmitRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return await orchestration.submit_flow(
        flow_name=request.flow_name,
        parameters=request.parameters,
        work_pool=request.work_pool,
    )


@router.post("/{run_id}/retry")
async def retry_run(
    run_id: str,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    try:
        return await orchestration.retry_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
