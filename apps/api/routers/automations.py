from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.automation_service import AutomationService
from apps.api.dependencies import automation_dep
from libs.contracts.models import AutomationCreateRequest, AutomationRunRequest, AutomationStatus, AutomationUpdateRequest


router = APIRouter(prefix="/automations", tags=["automations"])


@router.get("")
async def list_automations(
    limit: int = 100,
    status: AutomationStatus | None = None,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    automations = await automation_service.list_automations(limit=limit, status=status)
    return {"automations": [item.model_dump(mode="json") for item in automations]}


@router.post("")
async def create_automation(
    request: AutomationCreateRequest,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    try:
        automation = await automation_service.create_automation(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"automation": automation.model_dump(mode="json")}


@router.get("/{task_spec_id}")
async def get_automation(
    task_spec_id: str,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    try:
        automation = await automation_service.get_automation(task_spec_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Automation not found: {task_spec_id}") from exc
    return {"automation": automation.model_dump(mode="json")}


@router.patch("/{task_spec_id}")
async def update_automation(
    task_spec_id: str,
    request: AutomationUpdateRequest,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    try:
        automation = await automation_service.update_automation(task_spec_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Automation not found: {task_spec_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"automation": automation.model_dump(mode="json")}


@router.post("/{task_spec_id}/run")
async def run_automation(
    task_spec_id: str,
    request: AutomationRunRequest,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    try:
        return await automation_service.run_automation(task_spec_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Automation not found: {task_spec_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{task_spec_id}/pause")
async def pause_automation(
    task_spec_id: str,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    try:
        automation = await automation_service.pause_automation(task_spec_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Automation not found: {task_spec_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"automation": automation.model_dump(mode="json")}


@router.post("/{task_spec_id}/resume")
async def resume_automation(
    task_spec_id: str,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    try:
        automation = await automation_service.resume_automation(task_spec_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Automation not found: {task_spec_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"automation": automation.model_dump(mode="json")}


@router.post("/{task_spec_id}/archive")
async def archive_automation(
    task_spec_id: str,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    try:
        automation = await automation_service.archive_automation(task_spec_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Automation not found: {task_spec_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"automation": automation.model_dump(mode="json")}


@router.get("/{task_spec_id}/runs")
async def automation_runs(
    task_spec_id: str,
    limit: int = 25,
    include_children: bool = False,
    automation_service: AutomationService = Depends(automation_dep),
) -> dict:
    try:
        runs = await automation_service.list_runs(task_spec_id, limit=limit, include_children=include_children)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Automation not found: {task_spec_id}") from exc
    return {"runs": [item.model_dump(mode="json") for item in runs]}
