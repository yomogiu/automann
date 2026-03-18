from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from uuid import UUID

try:
    from prefect.deployments import run_deployment
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import DeploymentScheduleCreate, DeploymentUpdate
    from prefect.schedules import Cron
except Exception:  # pragma: no cover - Prefect may not be installed during static inspection
    run_deployment = None
    get_client = None
    DeploymentScheduleCreate = None
    DeploymentUpdate = None
    Cron = None

from flows.browser_job import browser_job_flow
from flows.artifact_ingest import artifact_ingest_flow
from flows.codex_search_report import codex_search_report_flow
from flows.daily_brief import daily_brief_flow
from flows.draft_article import substack_draft_flow
from flows.paper_batch import paper_batch_flow
from flows.paper_review import paper_review_flow
from flows.research_report import research_report_flow
from flows.registry import FLOW_SPECS


FLOW_CALLABLES = {
    "daily_brief_flow": daily_brief_flow,
    "paper_review_flow": paper_review_flow,
    "paper_batch_flow": paper_batch_flow,
    "browser_job_flow": browser_job_flow,
    "artifact_ingest_flow": artifact_ingest_flow,
    "codex_search_report_flow": codex_search_report_flow,
    "substack_draft_flow": substack_draft_flow,
    "research_report_flow": research_report_flow,
}

NON_LOCAL_FLOW_NAMES = frozenset({"browser_job_flow"})

FLOW_ENTRYPOINTS = {
    "daily_brief_flow": "flows/daily_brief.py:daily_brief_flow",
    "paper_review_flow": "flows/paper_review.py:paper_review_flow",
    "paper_batch_flow": "flows/paper_batch.py:paper_batch_flow",
    "browser_job_flow": "flows/browser_job.py:browser_job_flow",
    "artifact_ingest_flow": "flows/artifact_ingest.py:artifact_ingest_flow",
    "codex_search_report_flow": "flows/codex_search_report.py:codex_search_report_flow",
    "substack_draft_flow": "flows/draft_article.py:substack_draft_flow",
    "research_report_flow": "flows/research_report.py:research_report_flow",
}


@dataclass(slots=True)
class DeploymentMetadata:
    deployment_id: str
    deployment_name: str
    deployment_path: str
    flow_name: str
    work_pool_name: str
    prefect_url: str | None = None


class PrefectOrchestrationClient:
    def __init__(self, *, prefect_ui_url: str | None = None) -> None:
        self.prefect_ui_url = prefect_ui_url

    @property
    def prefect_available(self) -> bool:
        return run_deployment is not None

    def supports_local_execution(self, flow_name: str, request: dict[str, Any] | None = None) -> bool:
        return flow_name not in NON_LOCAL_FLOW_NAMES

    async def submit(
        self,
        *,
        deployment_path: str,
        request: dict[str, Any],
        run_id: str,
    ):
        if run_deployment is None:
            return None
        return await run_deployment(
            name=deployment_path,
            parameters={"request": request, "run_id": run_id},
        )

    async def run_local(
        self,
        *,
        flow_name: str,
        request: dict[str, Any],
        run_id: str,
    ) -> dict[str, Any]:
        flow_fn = FLOW_CALLABLES[flow_name]
        return await asyncio.to_thread(flow_fn, request=request, run_id=run_id)

    async def upsert_deployment(
        self,
        *,
        flow_name: str,
        deployment_name: str,
        work_pool_name: str,
        parameters: dict[str, Any] | None = None,
        cron: str | None = None,
        timezone: str | None = None,
        description: str | None = None,
        paused: bool = False,
    ) -> DeploymentMetadata:
        if get_client is None or DeploymentUpdate is None:
            raise RuntimeError("Prefect deployment management is unavailable")

        flow_callable = FLOW_CALLABLES[flow_name]
        flow_display_name = str(getattr(flow_callable, "name", FLOW_SPECS[flow_name].slug))
        deployment_path = f"{flow_display_name}/{deployment_name}"
        entrypoint = FLOW_ENTRYPOINTS[flow_name]
        schedules = self._build_schedules(cron=cron, timezone=timezone, active=not paused)

        async with get_client() as client:
            try:
                flow_row = await client.read_flow_by_name(flow_display_name)
                flow_id = flow_row.id
            except Exception:
                flow_id = await client.create_flow_from_name(flow_display_name)

            try:
                existing = await client.read_deployment_by_name(deployment_path)
            except Exception:
                existing = None

            if existing is None:
                deployment_id = await client.create_deployment(
                    flow_id=flow_id,
                    name=deployment_name,
                    work_pool_name=work_pool_name,
                    parameters=parameters or {},
                    description=description,
                    paused=paused,
                    path=".",
                    entrypoint=entrypoint,
                    schedules=schedules,
                )
            else:
                await client.update_deployment(
                    existing.id,
                    DeploymentUpdate(
                        parameters=parameters or {},
                        description=description,
                        paused=paused,
                        work_pool_name=work_pool_name,
                        path=".",
                        entrypoint=entrypoint,
                    ),
                )
                if schedules and Cron is not None:
                    current_schedules = await client.read_deployment_schedules(existing.id)
                    if current_schedules:
                        await client.update_deployment_schedule(
                            existing.id,
                            current_schedules[0].id,
                            active=not paused,
                            schedule=schedules[0].schedule,
                        )
                    else:
                        await client.create_deployment_schedules(existing.id, schedules=schedules)
                deployment_id = existing.id

            return DeploymentMetadata(
                deployment_id=str(deployment_id),
                deployment_name=deployment_name,
                deployment_path=f"{FLOW_SPECS[flow_name].slug}/{deployment_name}",
                flow_name=flow_name,
                work_pool_name=work_pool_name,
                prefect_url=self._deployment_url(str(deployment_id)),
            )

    async def pause_deployment(self, deployment_id: str | UUID) -> None:
        if get_client is None:
            raise RuntimeError("Prefect deployment management is unavailable")
        async with get_client() as client:
            await client.pause_deployment(deployment_id)

    async def resume_deployment(self, deployment_id: str | UUID) -> None:
        if get_client is None:
            raise RuntimeError("Prefect deployment management is unavailable")
        async with get_client() as client:
            await client.resume_deployment(deployment_id)

    async def delete_deployment(self, deployment_id: str | UUID) -> None:
        if get_client is None:
            raise RuntimeError("Prefect deployment management is unavailable")
        async with get_client() as client:
            await client.delete_deployment(deployment_id)

    def _build_schedules(
        self,
        *,
        cron: str | None,
        timezone: str | None,
        active: bool,
    ) -> list[Any] | None:
        if not cron or DeploymentScheduleCreate is None or Cron is None:
            return None
        return [
            DeploymentScheduleCreate(
                schedule=Cron(cron, timezone=timezone or "UTC"),
                active=active,
            )
        ]

    def _deployment_url(self, deployment_id: str) -> str | None:
        if not self.prefect_ui_url:
            return None
        return f"{self.prefect_ui_url.rstrip('/')}/deployments/deployment/{deployment_id}"
