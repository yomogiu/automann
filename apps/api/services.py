from __future__ import annotations

from typing import Any

from libs.config import Settings
from libs.contracts.models import (
    InteractionAnswerRequest,
    InteractionCreateRequest,
    InteractionSummary,
    RunSummary,
    SCHEMA_VERSION,
)
from libs.db import LifeRepository
from libs.prefect_client.orchestration import PrefectOrchestrationClient
from libs.retrieval import RetrievalService

from flows.registry import FLOW_SPECS


def _browser_worker_required_error(detail: str | None = None) -> str:
    base = (
        "Browser worker required: browser-backed jobs require Prefect deployment execution on the "
        "'browser-process' worker pool; local API fallback is disabled."
    )
    if not detail:
        return base
    return f"{base} {detail}"


class OrchestrationService:
    def __init__(
        self,
        settings: Settings,
        repository: LifeRepository,
        retrieval: RetrievalService,
        prefect_client: PrefectOrchestrationClient | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.retrieval = retrieval
        self.prefect_client = prefect_client or PrefectOrchestrationClient()

    def _supports_local_execution(self, flow_name: str, parameters: dict[str, Any] | None = None) -> bool:
        checker = getattr(self.prefect_client, "supports_local_execution", None)
        if callable(checker):
            try:
                return bool(checker(flow_name, request=parameters))
            except TypeError:
                return bool(checker(flow_name))
        return flow_name != "browser_job_flow"

    async def submit_flow(
        self,
        *,
        flow_name: str,
        parameters: dict[str, Any],
        work_pool: str | None = None,
        task_spec_id: str | None = None,
        deployment_path: str | None = None,
    ) -> dict[str, Any]:
        spec = FLOW_SPECS[flow_name]
        session_id = str(parameters.get("session_id") or "")
        idempotency_key = str(parameters.get("idempotency_key") or "")
        if session_id and idempotency_key:
            existing = self.repository.find_run_by_idempotency(
                session_id=session_id,
                idempotency_key=idempotency_key,
                flow_name=flow_name,
            )
            if existing is not None:
                return {
                    "run_id": existing.id,
                    "status": existing.status,
                    "mode": "idempotent-replay",
                    "prefect_flow_run_id": existing.prefect_flow_run_id,
                }

        run_record = self.repository.start_run(
            flow_name=flow_name,
            worker_key=spec.default_work_pool if work_pool is None else work_pool,
            input_payload=parameters,
            task_spec_id=task_spec_id,
            schema_version=str(parameters.get("schema_version") or SCHEMA_VERSION),
            command_id=parameters.get("command_id"),
            session_id=session_id or None,
            correlation_id=parameters.get("correlation_id"),
            idempotency_key=idempotency_key or None,
            actor=parameters.get("actor"),
            status="pending",
        )

        if self.prefect_client.prefect_available:
            try:
                flow_run = await self.prefect_client.submit(
                    deployment_path=deployment_path or spec.deployment_path,
                    request=parameters,
                    run_id=run_record.id,
                )
                self.repository.update_run_status(
                    run_record.id,
                    status="pending",
                    prefect_flow_run_id=str(getattr(flow_run, "id", "")),
                )
                return {
                    "run_id": run_record.id,
                    "status": "pending",
                    "mode": "prefect",
                    "prefect_flow_run_id": str(getattr(flow_run, "id", "")),
                }
            except Exception as exc:
                if not self._supports_local_execution(flow_name, parameters):
                    message = _browser_worker_required_error(detail=f"Prefect submit failed: {exc}")
                    self.repository.update_run_status(run_record.id, status="failed", stderr=message)
                    return {
                        "run_id": run_record.id,
                        "status": "failed",
                        "mode": "prefect-error",
                        "error": message,
                    }
                self.repository.update_run_status(run_record.id, status="pending", stderr=str(exc))

        if not self._supports_local_execution(flow_name, parameters):
            message = _browser_worker_required_error(
                detail="Prefect deployments are unavailable in this process."
            )
            self.repository.update_run_status(run_record.id, status="failed", stderr=message)
            return {
                "run_id": run_record.id,
                "status": "failed",
                "mode": "prefect-required",
                "error": message,
            }

        result = await self.prefect_client.run_local(flow_name=flow_name, request=parameters, run_id=run_record.id)
        refreshed = self.repository.get_run(run_record.id)
        return {
            "run_id": run_record.id,
            "status": refreshed.status if refreshed is not None else "completed",
            "mode": "local",
            "result": result,
        }

    def query_knowledge(
        self,
        *,
        query: str,
        limit: int = 10,
        include_semantic: bool = True,
        include_lexical: bool = True,
    ) -> dict[str, Any]:
        return self.retrieval.query_with_details(
            query=query,
            limit=limit,
            include_semantic=include_semantic,
            include_lexical=include_lexical,
        )

    def list_runs(self, *, limit: int = 25) -> list[RunSummary]:
        return [
            RunSummary(
                id=row.id,
                flow_name=row.flow_name,
                status=row.status,
                created_at=row.created_at,
                updated_at=row.updated_at,
                task_spec_id=row.task_spec_id,
                parent_run_id=row.parent_run_id,
                worker_key=row.worker_key,
                prefect_flow_run_id=row.prefect_flow_run_id,
                output_summary=row.structured_outputs or {},
            )
            for row in self.repository.list_runs(limit=limit)
        ]

    async def retry_run(self, run_id: str) -> dict[str, Any]:
        target = self.repository.get_run(run_id)
        if target is None:
            raise KeyError(run_id)
        return await self.submit_flow(
            flow_name=target.flow_name,
            parameters=target.input_payload,
            task_spec_id=target.task_spec_id,
        )

    def list_interactions(self, *, limit: int = 25, status: str | None = None) -> list[InteractionSummary]:
        return [self._interaction_summary(row) for row in self.repository.list_interactions(limit=limit, status=status)]

    def create_interaction(self, request: InteractionCreateRequest) -> InteractionSummary:
        row = self.repository.create_interaction(
            run_id=request.run_id,
            title=request.title,
            prompt_md=request.prompt_md,
            input_schema=request.input_schema,
            default_input=request.default_input,
            checkpoint_key=request.checkpoint_key,
            prefect_flow_run_id=request.prefect_flow_run_id,
            ui_hints=request.ui_hints,
        )
        return self._interaction_summary(row)

    def answer_interaction(self, interaction_id: str, request: InteractionAnswerRequest) -> InteractionSummary:
        row = self.repository.answer_interaction(interaction_id, response=request.response)
        if row is None:
            raise KeyError(interaction_id)
        if row.checkpoint_key == "promote_revision":
            report_id = str(row.ui_hints.get("report_id") or "")
            report_series_id = str(row.ui_hints.get("report_series_id") or "")
            approved = bool(request.response.get("approved"))
            run_row = self.repository.get_run(row.run_id)
            structured_outputs = dict(run_row.structured_outputs or {}) if run_row is not None else {}
            checkpoint_state = dict(structured_outputs.get("checkpoint") or {})
            checkpoint_state.update(
                {
                    "status": "answered",
                    "interaction_id": row.id,
                    "checkpoint_key": row.checkpoint_key,
                    "approved": approved,
                }
            )
            if approved and report_id:
                promoted = self.repository.promote_report_revision(report_id)
                self.repository.update_run_status(
                    row.run_id,
                    status="completed",
                    stdout=(
                        f"Promoted research report revision {promoted.revision_number}"
                        if promoted is not None
                        else "Promoted research report revision"
                    ),
                    structured_outputs={
                        **structured_outputs,
                        "current_report_id": promoted.id if promoted is not None else report_id,
                        "pending_report_id": None,
                        "checkpoint": {
                            **checkpoint_state,
                            "resolution": "approved",
                        },
                    },
                )
            else:
                current = (
                    self.repository.current_report_revision(report_series_id)
                    if report_series_id
                    else None
                )
                self.repository.update_run_status(
                    row.run_id,
                    status="skipped",
                    stdout="Research report revision promotion was rejected.",
                    structured_outputs={
                        **structured_outputs,
                        "current_report_id": current.id if current is not None else structured_outputs.get("current_report_id"),
                        "pending_report_id": None,
                        "rejected_report_id": report_id or None,
                        "checkpoint": {
                            **checkpoint_state,
                            "resolution": "rejected",
                        },
                    },
                )
        return self._interaction_summary(row)

    @staticmethod
    def _interaction_summary(row) -> InteractionSummary:  # noqa: ANN001
        return InteractionSummary(
            id=row.id,
            run_id=row.run_id,
            status=row.status,
            title=row.title,
            prompt_md=row.prompt_md,
            input_schema=row.input_schema,
            default_input=row.default_input,
            response_payload=row.response_payload,
            checkpoint_key=row.checkpoint_key,
            prefect_flow_run_id=row.prefect_flow_run_id,
            ui_hints=row.ui_hints,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
