from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.dependencies import orchestration_dep
from apps.api.services import OrchestrationService
from libs.contracts.models import RunSubmitRequest


router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("")
def list_runs(
    limit: int = 25,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return {"runs": [item.model_dump(mode="json") for item in orchestration.list_runs(limit=limit)]}


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
