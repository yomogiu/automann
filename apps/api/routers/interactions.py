from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.dependencies import orchestration_dep
from apps.api.services import OrchestrationService
from libs.contracts.models import InteractionAnswerRequest, InteractionCreateRequest


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


@router.post("")
def create_interaction(
    request: InteractionCreateRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return {"interaction": orchestration.create_interaction(request).model_dump(mode="json")}


@router.post("/{interaction_id}/answer")
def answer_interaction(
    interaction_id: str,
    request: InteractionAnswerRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    try:
        interaction = orchestration.answer_interaction(interaction_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Interaction not found: {interaction_id}") from exc
    return {"interaction": interaction.model_dump(mode="json")}
