from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.dependencies import orchestration_dep, repository_dep
from apps.api.services import OrchestrationService
from libs.contracts.models import InteractionAnswerRequest, InteractionCreateRequest
from libs.db import LifeRepository


router = APIRouter(prefix="/interactions", tags=["interactions"])


@router.get("")
def list_interactions(
    limit: int = 25,
    status: str | None = None,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return {
        "interactions": [
            item.model_dump(mode="json")
            for item in orchestration.list_interactions(limit=limit, status=status)
        ]
    }


@router.get("/{interaction_id}")
def get_interaction(
    interaction_id: str,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    row = repository.get_interaction(interaction_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Interaction not found: {interaction_id}")
    return {
        "id": row.id,
        "run_id": row.run_id,
        "status": row.status,
        "title": row.title,
        "prompt_md": row.prompt_md,
        "input_schema": row.input_schema,
        "default_input": row.default_input,
        "response_payload": row.response_payload,
        "checkpoint_key": row.checkpoint_key,
        "prefect_flow_run_id": row.prefect_flow_run_id,
        "ui_hints": row.ui_hints,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


@router.post("")
def create_interaction(
    request: InteractionCreateRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return {"interaction": orchestration.create_interaction(request).model_dump(mode="json")}


@router.post("/{interaction_id}/answer")
async def answer_interaction(
    interaction_id: str,
    request: InteractionAnswerRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    try:
        interaction = await orchestration.answer_interaction(interaction_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Interaction not found: {interaction_id}") from exc
    return {"interaction": interaction.model_dump(mode="json")}
