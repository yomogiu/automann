from __future__ import annotations

from fastapi import APIRouter, Depends

from apps.api.dependencies import orchestration_dep
from apps.api.services import OrchestrationService
from libs.contracts.models import (
    BrowserJobRequest,
    DailyBriefRequest,
    DraftArticleRequest,
    PaperReviewRequest,
    QueryKnowledgeRequest,
)


router = APIRouter(prefix="/commands", tags=["commands"])


@router.post("/daily-brief")
async def start_daily_brief(
    request: DailyBriefRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return await orchestration.submit_flow(
        flow_name="daily_brief_flow",
        parameters=request.model_dump(mode="json"),
    )


@router.post("/paper-review")
async def review_paper(
    request: PaperReviewRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return await orchestration.submit_flow(
        flow_name="paper_review_flow",
        parameters=request.model_dump(mode="json"),
    )


@router.post("/browser-job")
async def run_browser_job(
    request: BrowserJobRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return await orchestration.submit_flow(
        flow_name="browser_job_flow",
        parameters=request.model_dump(mode="json"),
    )


@router.post("/draft-article")
async def draft_article(
    request: DraftArticleRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return await orchestration.submit_flow(
        flow_name="substack_draft_flow",
        parameters=request.model_dump(mode="json"),
    )


@router.post("/query-knowledge")
def query_knowledge(
    request: QueryKnowledgeRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return {
        "query": request.query,
        "results": orchestration.query_knowledge(query=request.query, limit=request.limit),
    }
