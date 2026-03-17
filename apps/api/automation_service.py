from __future__ import annotations

from pathlib import Path
import re
from typing import Any
from uuid import uuid4

from libs.config import Settings
from libs.contracts.models import (
    AutomationCreateRequest,
    AutomationDetail,
    AutomationPrefectStatus,
    AutomationRunRequest,
    AutomationStatus,
    AutomationSummary,
    AutomationType,
    CommandEnvelope,
    DailyBriefAutomationPayload,
    PaperBatchAutomationPayload,
    RunSummary,
    SearchReportAutomationPayload,
)
from libs.db import LifeRepository
from libs.prefect_client.automation_deployments import PrefectAutomationDeploymentClient

from flows.registry import FLOW_SPECS

from .services import OrchestrationService


_AUTOMATION_FLOW_NAMES = {
    AutomationType.DAILY_BRIEF: "daily_brief_flow",
    AutomationType.PAPER_BATCH: "paper_batch_flow",
    AutomationType.SEARCH_REPORT: "search_report_flow",
}

_AUTOMATION_PAYLOAD_MODELS = {
    AutomationType.DAILY_BRIEF: DailyBriefAutomationPayload,
    AutomationType.PAPER_BATCH: PaperBatchAutomationPayload,
    AutomationType.SEARCH_REPORT: SearchReportAutomationPayload,
}

_TASK_KEY_RE = re.compile(r"[^a-z0-9]+")
_DEFAULT_SEARCH_PROMPT = """# Search report instructions

Use the structured query config and decide whether to search local knowledge, browser-backed web sources, or both.

Requirements:
- Prioritize concrete evidence over generic synthesis.
- Group findings by theme.
- Preserve useful citations and source links.
- Call out uncertainty or conflicting signals explicitly.
"""


class AutomationService:
    def __init__(
        self,
        settings: Settings,
        repository: LifeRepository,
        orchestration: OrchestrationService,
        deployment_client: PrefectAutomationDeploymentClient | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.orchestration = orchestration
        self.deployment_client = deployment_client or PrefectAutomationDeploymentClient(settings)

    async def list_automations(
        self,
        *,
        status: AutomationStatus | None = None,
        limit: int = 100,
    ) -> list[AutomationSummary]:
        rows = self.repository.list_task_specs(status=status.value if status is not None else None, limit=limit)
        summaries: list[AutomationSummary] = []
        for row in rows:
            if not self._is_supported_type(row.task_type):
                continue
            summaries.append(await self._summary_from_row(row))
        return summaries

    async def get_automation(self, task_spec_id: str) -> AutomationDetail:
        row = self.repository.get_task_spec(task_spec_id)
        if row is None or not self._is_supported_type(row.task_type):
            raise KeyError(task_spec_id)
        return await self._detail_from_row(row)

    async def create_automation(self, request: AutomationCreateRequest) -> AutomationDetail:
        task_key = request.task_key or self._build_task_key(request.title)
        if self.repository.get_task_spec_by_key(task_key) is not None:
            raise ValueError(f"task_key already exists: {task_key}")

        automation_type = request.automation_type
        flow_name = _AUTOMATION_FLOW_NAMES[automation_type]
        work_pool = request.work_pool or FLOW_SPECS[flow_name].default_work_pool
        payload = self._normalize_payload(automation_type, request.payload)
        prompt_path = self._normalize_prompt_path(task_key, request.prompt_path) if automation_type == AutomationType.SEARCH_REPORT else None
        prompt_body = self._normalize_prompt_body(automation_type, request.prompt_body)
        parameters = self._build_flow_parameters(
            automation_type=automation_type,
            automation_title=request.title,
            payload=payload,
            prompt_path=prompt_path,
        )
        prefect = await self.deployment_client.upsert(
            task_key=task_key,
            task_type=automation_type.value,
            flow_name=flow_name,
            schedule_text=request.schedule_text,
            timezone=request.timezone,
            work_pool=work_pool,
            parameters=parameters,
            paused=request.status == AutomationStatus.PAUSED,
            description=request.description,
        )

        if prompt_path is not None:
            self._write_prompt(prompt_path, prompt_body)

        row = self.repository.create_task_spec(
            task_key=task_key,
            task_type=automation_type.value,
            flow_name=flow_name,
            title=request.title,
            description=request.description,
            schedule_text=request.schedule_text,
            timezone=request.timezone,
            work_pool=work_pool,
            prompt_path=str(prompt_path) if prompt_path is not None else None,
            status=request.status.value,
            prefect_deployment_id=prefect.deployment_id,
            prefect_deployment_name=prefect.deployment_name,
            prefect_deployment_path=prefect.deployment_path,
            prefect_deployment_url=prefect.deployment_url,
            payload=payload,
        )
        return await self._detail_from_row(row)

    async def update_automation(self, task_spec_id: str, request) -> AutomationDetail:  # noqa: ANN001
        row = self.repository.get_task_spec(task_spec_id)
        if row is None or not self._is_supported_type(row.task_type):
            raise KeyError(task_spec_id)

        automation_type = AutomationType(row.task_type)
        flow_name = row.flow_name or _AUTOMATION_FLOW_NAMES[automation_type]
        work_pool = request.work_pool if "work_pool" in request.model_fields_set else row.work_pool
        work_pool = work_pool or FLOW_SPECS[flow_name].default_work_pool
        schedule_text = request.schedule_text if "schedule_text" in request.model_fields_set else row.schedule_text
        timezone = request.timezone if "timezone" in request.model_fields_set else row.timezone
        description = request.description if "description" in request.model_fields_set else row.description
        title = request.title if "title" in request.model_fields_set and request.title is not None else row.title
        next_status = request.status if "status" in request.model_fields_set and request.status is not None else AutomationStatus(row.status)
        if next_status == AutomationStatus.ARCHIVED:
            raise ValueError("use the archive endpoint to archive an automation")

        payload_input = request.payload if "payload" in request.model_fields_set and request.payload is not None else dict(row.payload or {})
        payload = self._normalize_payload(automation_type, payload_input)

        prompt_path = Path(row.prompt_path).resolve() if row.prompt_path else None
        if automation_type == AutomationType.SEARCH_REPORT:
            if "prompt_path" in request.model_fields_set:
                prompt_path = self._normalize_prompt_path(row.task_key, request.prompt_path)
            prompt_body = self._normalize_prompt_body(automation_type, request.prompt_body if "prompt_body" in request.model_fields_set else self._read_prompt(prompt_path))
        else:
            prompt_body = None

        parameters = self._build_flow_parameters(
            automation_type=automation_type,
            automation_title=title,
            payload=payload,
            prompt_path=prompt_path,
        )
        prefect = await self.deployment_client.upsert(
            task_key=row.task_key,
            task_type=automation_type.value,
            flow_name=flow_name,
            schedule_text=schedule_text,
            timezone=timezone,
            work_pool=work_pool,
            parameters=parameters,
            paused=next_status == AutomationStatus.PAUSED,
            description=description,
        )

        if prompt_path is not None:
            self._write_prompt(prompt_path, prompt_body)

        updated = self.repository.update_task_spec(
            row.id,
            title=title,
            description=description,
            flow_name=flow_name,
            schedule_text=schedule_text,
            timezone=timezone,
            work_pool=work_pool,
            prompt_path=str(prompt_path) if prompt_path is not None else None,
            status=next_status.value,
            prefect_deployment_id=prefect.deployment_id,
            prefect_deployment_name=prefect.deployment_name,
            prefect_deployment_path=prefect.deployment_path,
            prefect_deployment_url=prefect.deployment_url,
            payload=payload,
        )
        if updated is None:
            raise KeyError(task_spec_id)
        return await self._detail_from_row(updated)

    async def run_automation(self, task_spec_id: str, request: AutomationRunRequest) -> dict[str, Any]:
        row = self.repository.get_task_spec(task_spec_id)
        if row is None or not self._is_supported_type(row.task_type):
            raise KeyError(task_spec_id)
        if row.status == AutomationStatus.ARCHIVED.value:
            raise ValueError("archived automations cannot be run")

        automation_type = AutomationType(row.task_type)
        payload = dict(row.payload or {})
        payload.update(request.payload_overrides)
        payload = self._normalize_payload(automation_type, payload)
        parameters = self._build_flow_parameters(
            automation_type=automation_type,
            automation_title=row.title,
            payload=payload,
            prompt_path=Path(row.prompt_path).resolve() if row.prompt_path else None,
            actor="automation",
        )
        return await self.orchestration.submit_flow(
            flow_name=row.flow_name or _AUTOMATION_FLOW_NAMES[automation_type],
            parameters=parameters,
            work_pool=row.work_pool,
            task_spec_id=row.id,
            deployment_path=row.prefect_deployment_path,
        )

    async def pause_automation(self, task_spec_id: str) -> AutomationDetail:
        row = self.repository.get_task_spec(task_spec_id)
        if row is None or not self._is_supported_type(row.task_type):
            raise KeyError(task_spec_id)
        if not row.prefect_deployment_id or not row.prefect_deployment_path:
            raise ValueError("automation is missing Prefect deployment metadata")
        prefect = await self.deployment_client.pause(
            deployment_id=row.prefect_deployment_id,
            deployment_path=row.prefect_deployment_path,
        )
        updated = self.repository.update_task_spec(
            row.id,
            status=AutomationStatus.PAUSED.value,
            prefect_deployment_url=prefect.deployment_url,
        )
        if updated is None:
            raise KeyError(task_spec_id)
        return await self._detail_from_row(updated)

    async def resume_automation(self, task_spec_id: str) -> AutomationDetail:
        row = self.repository.get_task_spec(task_spec_id)
        if row is None or not self._is_supported_type(row.task_type):
            raise KeyError(task_spec_id)
        if not row.prefect_deployment_id or not row.prefect_deployment_path:
            raise ValueError("automation is missing Prefect deployment metadata")
        prefect = await self.deployment_client.resume(
            deployment_id=row.prefect_deployment_id,
            deployment_path=row.prefect_deployment_path,
        )
        updated = self.repository.update_task_spec(
            row.id,
            status=AutomationStatus.ACTIVE.value,
            prefect_deployment_url=prefect.deployment_url,
        )
        if updated is None:
            raise KeyError(task_spec_id)
        return await self._detail_from_row(updated)

    async def archive_automation(self, task_spec_id: str) -> AutomationDetail:
        row = self.repository.get_task_spec(task_spec_id)
        if row is None or not self._is_supported_type(row.task_type):
            raise KeyError(task_spec_id)
        if row.prefect_deployment_id:
            await self.deployment_client.archive(deployment_id=row.prefect_deployment_id)
        updated = self.repository.update_task_spec(
            row.id,
            status=AutomationStatus.ARCHIVED.value,
            prefect_deployment_id=None,
            prefect_deployment_name=None,
            prefect_deployment_path=None,
            prefect_deployment_url=None,
        )
        if updated is None:
            raise KeyError(task_spec_id)
        return await self._detail_from_row(updated)

    async def list_runs(self, task_spec_id: str, *, limit: int = 25, include_children: bool = False) -> list[RunSummary]:
        row = self.repository.get_task_spec(task_spec_id)
        if row is None or not self._is_supported_type(row.task_type):
            raise KeyError(task_spec_id)
        return [
            RunSummary(
                id=run.id,
                flow_name=run.flow_name,
                status=run.status,
                created_at=run.created_at,
                updated_at=run.updated_at,
                task_spec_id=run.task_spec_id,
                parent_run_id=run.parent_run_id,
                worker_key=run.worker_key,
                prefect_flow_run_id=run.prefect_flow_run_id,
                output_summary=run.structured_outputs or {},
            )
            for run in self.repository.list_runs_for_task_spec(
                task_spec_id,
                limit=limit,
                include_children=include_children,
            )
        ]

    async def _summary_from_row(self, row) -> AutomationSummary:  # noqa: ANN001
        latest_run = self.repository.list_runs_for_task_spec(row.id, limit=1)
        prefect = await self._prefect_status_for_row(row)
        return AutomationSummary(
            id=row.id,
            task_key=row.task_key,
            automation_type=AutomationType(row.task_type),
            flow_name=row.flow_name or _AUTOMATION_FLOW_NAMES[AutomationType(row.task_type)],
            title=row.title,
            description=row.description,
            schedule_text=row.schedule_text,
            timezone=row.timezone,
            work_pool=row.work_pool,
            status=AutomationStatus(row.status),
            prompt_path=row.prompt_path,
            prefect=prefect,
            latest_run=self._run_summary(latest_run[0]) if latest_run else None,
        )

    async def _detail_from_row(self, row) -> AutomationDetail:  # noqa: ANN001
        summary = await self._summary_from_row(row)
        prompt_body = self._read_prompt(Path(row.prompt_path)) if row.prompt_path else None
        return AutomationDetail(
            **summary.model_dump(mode="python"),
            payload=dict(row.payload or {}),
            prompt_body=prompt_body,
        )

    async def _prefect_status_for_row(self, row) -> AutomationPrefectStatus:  # noqa: ANN001
        if row.status == AutomationStatus.ARCHIVED.value:
            return AutomationPrefectStatus(status="archived")
        if not row.prefect_deployment_path and not row.prefect_deployment_id:
            return AutomationPrefectStatus(status="missing")
        try:
            return await self.deployment_client.describe(
                deployment_id=row.prefect_deployment_id,
                deployment_path=row.prefect_deployment_path,
            )
        except Exception:
            return AutomationPrefectStatus(
                deployment_id=row.prefect_deployment_id,
                deployment_name=row.prefect_deployment_name,
                deployment_path=row.prefect_deployment_path,
                deployment_url=row.prefect_deployment_url,
                status="error",
            )

    @staticmethod
    def _normalize_payload(automation_type: AutomationType, payload: dict[str, Any]) -> dict[str, Any]:
        model_cls = _AUTOMATION_PAYLOAD_MODELS[automation_type]
        return model_cls.model_validate(payload).model_dump(mode="json")

    @staticmethod
    def _build_task_key(title: str) -> str:
        normalized = _TASK_KEY_RE.sub("-", title.strip().lower()).strip("-")
        if not normalized:
            normalized = "automation"
        return f"{normalized}-{uuid4().hex[:8]}"

    @staticmethod
    def _is_supported_type(task_type: str) -> bool:
        return task_type in {item.value for item in AutomationType}

    @staticmethod
    def _run_summary(run) -> RunSummary:  # noqa: ANN001
        return RunSummary(
            id=run.id,
            flow_name=run.flow_name,
            status=run.status,
            created_at=run.created_at,
            updated_at=run.updated_at,
            task_spec_id=run.task_spec_id,
            parent_run_id=run.parent_run_id,
            worker_key=run.worker_key,
            prefect_flow_run_id=run.prefect_flow_run_id,
            output_summary=run.structured_outputs or {},
        )

    def _normalize_prompt_path(self, task_key: str, prompt_path: str | None) -> Path:
        prompt_root = self.settings.automation_prompt_root.resolve()
        if prompt_path:
            candidate = Path(prompt_path).expanduser()
            if not candidate.is_absolute():
                candidate = (prompt_root / candidate).resolve()
            else:
                candidate = candidate.resolve()
        else:
            candidate = (prompt_root / f"{task_key}.md").resolve()

        if prompt_root not in candidate.parents and candidate != prompt_root:
            raise ValueError("prompt_path must live inside the configured automation prompt root")
        return candidate

    @staticmethod
    def _normalize_prompt_body(automation_type: AutomationType, prompt_body: str | None) -> str | None:
        if automation_type != AutomationType.SEARCH_REPORT:
            return None
        return (prompt_body or _DEFAULT_SEARCH_PROMPT).strip() + "\n"

    @staticmethod
    def _read_prompt(prompt_path: Path | None) -> str | None:
        if prompt_path is None or not prompt_path.exists():
            return None
        return prompt_path.read_text(encoding="utf-8")

    @staticmethod
    def _write_prompt(prompt_path: Path, prompt_body: str | None) -> None:
        if prompt_body is None:
            return
        prompt_path.parent.mkdir(parents=True, exist_ok=True)
        prompt_path.write_text(prompt_body, encoding="utf-8")

    @staticmethod
    def _build_flow_parameters(
        *,
        automation_type: AutomationType,
        automation_title: str,
        payload: dict[str, Any],
        prompt_path: Path | None,
        actor: str = "user",
    ) -> dict[str, Any]:
        parameters = CommandEnvelope(actor=actor).model_dump(mode="json")
        if automation_type == AutomationType.DAILY_BRIEF:
            parameters.update(payload)
            return parameters

        if automation_type == AutomationType.PAPER_BATCH:
            parameters.update(payload)
            parameters.setdefault("title", automation_title)
            return parameters

        if automation_type == AutomationType.SEARCH_REPORT:
            max_results = int(payload.get("max_results_per_query") or 8)
            raw_queries = list(payload.get("queries") or [])
            parameters.update(
                {
                    "title": automation_title,
                    "theme": payload.get("theme"),
                    "queries": [
                        item
                        if isinstance(item, dict)
                        else {"label": None, "query": str(item), "limit": max_results}
                        for item in raw_queries
                    ],
                    "enabled_sources": list(payload.get("enabled_sources") or []),
                    "planner_enabled": bool(payload.get("planner_enabled", True)),
                    "metadata": dict(payload.get("metadata") or {}),
                }
            )
        if automation_type == AutomationType.SEARCH_REPORT and prompt_path is not None:
            parameters["prompt_path"] = str(prompt_path)
        return parameters
