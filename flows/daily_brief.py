from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import DailyBriefRequest
from libs.contracts.workers import (
    ArxivFeedIngestRequest,
    DailyBriefAnalysisRequest,
    NewsIngestRequest,
    PublishRequest,
)
from libs.db import LifeRepository, engine_for_url
from workers.analysis_runner import AnalysisRunner
from workers.ingest_runner import ArxivReviewRunner, NewsScrapeRunner
from workers.publisher import GitHubPublisher

from .common import execute_adapter


def _skipped_lane_result(*, run_id: str | None, structured_outputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": None,
        "parent_run_id": run_id,
        "task_spec_id": None,
        "status": "skipped",
        "stdout": "",
        "stderr": "",
        "structured_outputs": structured_outputs,
        "artifacts": [],
        "observations": [],
        "reports": [],
        "next_events": [],
    }


def _artifact_paths(*results: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for result in results:
        for artifact in result.get("artifacts", []):
            path = str((artifact or {}).get("path") or "").strip()
            if path:
                paths.append(path)
    return paths


@flow(name="daily-brief")
def daily_brief_flow(request: dict[str, Any] | None = None, run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = LifeRepository(engine_for_url(settings.life_database_url))
    brief_request = DailyBriefRequest.model_validate(request or {})
    brief_date = (brief_request.date or datetime.now(timezone.utc)).date()

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    news_runner = NewsScrapeRunner(settings)
    arxiv_runner = ArxivReviewRunner(settings)
    analysis_runner = AnalysisRunner(settings)
    publisher = GitHubPublisher(settings)
    executed_ingest_results: list[dict[str, Any]] = []

    try:
        if brief_request.include_news:
            news_request = NewsIngestRequest(
                brief_date=brief_date,
                seed_news=list(brief_request.metadata.get("seed_news") or []),
                metadata=dict(brief_request.metadata),
            )
            news_result = execute_adapter(
                flow_name="daily_brief_flow",
                worker_key=news_runner.worker_key,
                input_payload=news_request.model_dump(mode="json"),
                runner=lambda: news_runner.run(news_request),
                parent_run_id=run_id,
            )
            executed_ingest_results.append(news_result)
        else:
            news_result = _skipped_lane_result(
                run_id=run_id,
                structured_outputs={
                    "status": "skipped",
                    "reason": "disabled_by_request",
                    "items": [],
                    "count": 0,
                },
            )

        if brief_request.include_arxiv:
            arxiv_request = ArxivFeedIngestRequest(
                brief_date=brief_date,
                seed_arxiv=list(brief_request.metadata.get("seed_arxiv") or []),
                metadata=dict(brief_request.metadata),
            )
            arxiv_result = execute_adapter(
                flow_name="daily_brief_flow",
                worker_key=arxiv_runner.worker_key,
                input_payload=arxiv_request.model_dump(mode="json"),
                runner=lambda: arxiv_runner.ingest_feed(arxiv_request),
                parent_run_id=run_id,
            )
            executed_ingest_results.append(arxiv_result)
        else:
            arxiv_result = _skipped_lane_result(
                run_id=run_id,
                structured_outputs={
                    "status": "skipped",
                    "reason": "disabled_by_request",
                    "papers": [],
                    "count": 0,
                },
            )

        browser_reason = "disabled_by_request" if not brief_request.include_browser_jobs else "browser_deferred"
        browser_result = _skipped_lane_result(
            run_id=run_id,
            structured_outputs={
                "status": "skipped",
                "reason": browser_reason,
            },
        )

        latest_report = repository.latest_report("daily_brief")
        analysis_request = DailyBriefAnalysisRequest(
            brief_date=brief_date,
            news_items=list(news_result.get("structured_outputs", {}).get("items", [])),
            papers=list(arxiv_result.get("structured_outputs", {}).get("papers", [])),
            browser_summary=browser_result.get("structured_outputs"),
            brief_metadata=dict(brief_request.metadata),
            previous_report={
                "id": latest_report.id,
                "title": latest_report.title,
            }
            if latest_report
            else None,
        )
        analysis_result = execute_adapter(
            flow_name="daily_brief_flow",
            worker_key=analysis_runner.worker_key,
            input_payload=analysis_request.model_dump(mode="json"),
            runner=lambda: analysis_runner.compile_daily_brief(analysis_request),
            parent_run_id=run_id,
        )
        published = None
        if brief_request.publish and analysis_result["artifacts"]:
            report_path = analysis_result["artifacts"][0]["path"]
            artifact_paths = _artifact_paths(*executed_ingest_results)
            publish_request = PublishRequest(
                report_path=report_path,
                artifact_paths=artifact_paths,
                metadata={"brief_date": brief_date.isoformat()},
            )
            published = execute_adapter(
                flow_name="daily_brief_flow",
                worker_key=publisher.worker_key,
                input_payload=publish_request.model_dump(mode="json"),
                runner=lambda: publisher.run(publish_request),
                parent_run_id=run_id,
            )
        completed_results = [*executed_ingest_results, analysis_result]
        if published is not None:
            completed_results.append(published)

        flow_result = {
            "date": brief_date.isoformat(),
            "news": news_result,
            "arxiv": arxiv_result,
            "browser": browser_result,
            "analysis": analysis_result,
            "published": published,
        }

        if run_id is not None:
            repository.update_run_status(
                run_id,
                status="completed",
                stdout=f"Completed daily brief for {brief_date.isoformat()}",
                structured_outputs=flow_result,
                artifact_manifest=[artifact for item in completed_results for artifact in item.get("artifacts", [])],
                observation_summary=[obs for item in completed_results for obs in item.get("observations", [])],
                next_suggested_events=[event for item in completed_results for event in item.get("next_events", [])],
            )
        return flow_result
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
