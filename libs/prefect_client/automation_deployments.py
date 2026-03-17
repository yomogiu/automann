from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

try:
    from prefect.client.orchestration import get_client
except Exception:  # pragma: no cover - Prefect may not be installed during static inspection
    get_client = None

from libs.config import Settings
from libs.contracts.models import AutomationPrefectStatus

from .orchestration import PrefectOrchestrationClient


class PrefectAutomationDeploymentClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.orchestration_client = PrefectOrchestrationClient(prefect_ui_url=settings.prefect_ui_url)

    @property
    def prefect_available(self) -> bool:
        return get_client is not None and self.orchestration_client.prefect_available

    async def upsert(
        self,
        *,
        task_key: str,
        task_type: str,
        flow_name: str,
        schedule_text: str | None,
        timezone: str | None,
        work_pool: str,
        parameters: dict[str, Any],
        paused: bool,
        description: str | None = None,
    ) -> AutomationPrefectStatus:
        if not self.prefect_available:
            raise RuntimeError("Prefect deployment management is unavailable in this process.")

        metadata = await self.orchestration_client.upsert_deployment(
            flow_name=flow_name,
            deployment_name=self._deployment_name(task_key),
            work_pool_name=work_pool,
            parameters={"request": parameters, "run_id": None},
            cron=schedule_text,
            timezone=timezone,
            description=description,
            paused=paused,
        )
        return await self.describe(
            deployment_id=metadata.deployment_id,
            deployment_path=metadata.deployment_path,
        )

    async def pause(self, *, deployment_id: str, deployment_path: str) -> AutomationPrefectStatus:
        if not self.prefect_available:
            raise RuntimeError("Prefect deployment management is unavailable in this process.")
        await self.orchestration_client.pause_deployment(UUID(str(deployment_id)))
        return await self.describe(
            deployment_id=deployment_id,
            deployment_path=deployment_path,
        )

    async def resume(self, *, deployment_id: str, deployment_path: str) -> AutomationPrefectStatus:
        if not self.prefect_available:
            raise RuntimeError("Prefect deployment management is unavailable in this process.")
        await self.orchestration_client.resume_deployment(UUID(str(deployment_id)))
        return await self.describe(
            deployment_id=deployment_id,
            deployment_path=deployment_path,
        )

    async def archive(self, *, deployment_id: str) -> None:
        if not self.prefect_available:
            raise RuntimeError("Prefect deployment management is unavailable in this process.")
        await self.orchestration_client.delete_deployment(UUID(str(deployment_id)))

    async def describe(
        self,
        *,
        deployment_id: str | None = None,
        deployment_path: str | None = None,
    ) -> AutomationPrefectStatus:
        if not self.prefect_available:
            return AutomationPrefectStatus(status="unavailable")

        async with get_client() as client:
            deployment = None
            if deployment_id:
                try:
                    deployment = await client.read_deployment(UUID(str(deployment_id)))
                except Exception:
                    deployment = None
            if deployment is None and deployment_path:
                deployment = await self._read_deployment_by_name(client, deployment_path)
            if deployment is None:
                return AutomationPrefectStatus(
                    deployment_id=deployment_id,
                    deployment_path=deployment_path,
                    status="missing",
                )

            paused = bool(getattr(deployment, "paused", False))
            next_run_at = await self._next_run_time(client, deployment.id)
            return AutomationPrefectStatus(
                deployment_id=str(deployment.id),
                deployment_name=str(getattr(deployment, "name", "") or ""),
                deployment_path=deployment_path or self._deployment_path_for(deployment),
                deployment_url=f"{self.settings.prefect_ui_url}/deployments/deployment/{deployment.id}",
                status="paused" if paused else "active",
                paused=paused,
                next_run_at=next_run_at,
            )

    async def _next_run_time(self, client, deployment_id) -> datetime | None:  # noqa: ANN001
        try:
            scheduled = await client.get_scheduled_flow_runs_for_deployments([deployment_id], limit=1)
        except Exception:
            return None
        if not scheduled:
            return None
        run = scheduled[0]
        return getattr(run, "expected_start_time", None) or getattr(run, "next_scheduled_start_time", None)

    async def _read_deployment_by_name(self, client, deployment_path: str):  # noqa: ANN001
        try:
            return await client.read_deployment_by_name(deployment_path)
        except Exception:
            return None

    @staticmethod
    def _deployment_name(task_key: str) -> str:
        return f"automation-{task_key}"

    @staticmethod
    def _deployment_path_for(deployment) -> str:  # noqa: ANN001
        name = str(getattr(deployment, "name", "") or "")
        flow_name = getattr(getattr(deployment, "flow", None), "name", None)
        if flow_name:
            slug = str(flow_name).replace(" ", "-").lower()
            return f"{slug}/{name}"
        return name
