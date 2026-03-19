from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from apps.api.codex_session_service import CodexSessionService
from apps.api.dependencies import codex_session_dep


router = APIRouter(prefix="/runs", tags=["runs"])


class RunSteerRequest(BaseModel):
    prompt: str | None = None
    message: str | None = None
    input: str | None = None
    expected_turn_id: str | None = None


@router.get("/{run_id}/stream")
async def stream_run(
    run_id: str,
    after_seq: int = Query(default=0, ge=0),
    sessions: CodexSessionService = Depends(codex_session_dep),
) -> StreamingResponse:
    async def _events():
        try:
            async for event in sessions.stream_events(run_id, after_seq=after_seq):
                yield f"data: {json.dumps(event, default=str)}\n\n"
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Run session not found: {run_id}") from exc

    return StreamingResponse(_events(), media_type="text/event-stream")


@router.post("/{run_id}/interrupt")
async def interrupt_run(
    run_id: str,
    sessions: CodexSessionService = Depends(codex_session_dep),
) -> dict:
    try:
        return await sessions.interrupt(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run session not found: {run_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/{run_id}/steer")
async def steer_run(
    run_id: str,
    request: RunSteerRequest,
    sessions: CodexSessionService = Depends(codex_session_dep),
) -> dict:
    prompt = str(request.prompt or request.message or request.input or "").strip()
    if not prompt:
        raise HTTPException(status_code=422, detail="prompt is required")
    try:
        return await sessions.steer(
            run_id,
            prompt=prompt,
            expected_turn_id=request.expected_turn_id,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run session not found: {run_id}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
