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

    async def submit_flow(
        self,
        *,
        flow_name: str,
        parameters: dict[str, Any],
        work_pool: str | None = None,
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
                    deployment_path=spec.deployment_path,
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
                self.repository.update_run_status(run_record.id, status="pending", stderr=str(exc))

        result = await self.prefect_client.run_local(flow_name=flow_name, request=parameters, run_id=run_record.id)
        refreshed = self.repository.get_run(run_record.id)
        return {
            "run_id": run_record.id,
            "status": refreshed.status if refreshed is not None else "completed",
            "mode": "local",
            "result": result,
        }

    def query_knowledge(self, *, query: str, limit: int = 10) -> list[dict[str, Any]]:
        return self.retrieval.query(query=query, limit=limit)

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
        return await self.submit_flow(flow_name=target.flow_name, parameters=target.input_payload)

    def list_interactions(self, *, limit: int = 25, status: str | None = None) -> list[InteractionSummary]:
        return [
            InteractionSummary(
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
            for row in self.repository.list_interactions(limit=limit, status=status)
        ]

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

    def answer_interaction(self, interaction_id: str, request: InteractionAnswerRequest) -> InteractionSummary:
        row = self.repository.answer_interaction(interaction_id, response=request.response)
        if row is None:
            raise KeyError(interaction_id)
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
