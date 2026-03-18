from __future__ import annotations

from fastapi import APIRouter, Depends

from apps.api.dependencies import orchestration_dep
from apps.api.services import OrchestrationService
from libs.contracts.models import (
    ArtifactIngestRequest,
    BrowserJobRequest,
    DailyBriefRequest,
    DraftArticleRequest,
    PaperReviewRequest,
    QueryKnowledgeRequest,
    ResearchReportRequest,
    SearchReportCommandRequest,
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


@router.post("/artifact-ingest")
async def ingest_artifacts(
    request: ArtifactIngestRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return await orchestration.submit_flow(
        flow_name="artifact_ingest_flow",
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


@router.post("/research-report")
async def research_report(
    request: ResearchReportRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return await orchestration.submit_flow(
        flow_name="research_report_flow",
        parameters=request.model_dump(mode="json"),
    )


@router.post("/search-report")
async def search_report(
    request: SearchReportCommandRequest,
    orchestration: OrchestrationService = Depends(orchestration_dep),
) -> dict:
    return await orchestration.submit_flow(
        flow_name="codex_search_report_flow",
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
