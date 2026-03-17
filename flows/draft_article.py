from __future__ import annotations

from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import DraftArticleRequest
from libs.contracts.workers import DraftGenerationRequest
from libs.db import LifeRepository, engine_for_url
from libs.retrieval import RetrievalService
from workers.draft_runner import DraftWriter

from .common import execute_adapter


@flow(name="substack-draft")
def substack_draft_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = LifeRepository(engine_for_url(settings.life_database_url))
    retrieval = RetrievalService(repository)
    draft_request = DraftArticleRequest.model_validate(request)
    evidence_pack = retrieval.query(query=draft_request.theme, limit=8)
    worker_request = DraftGenerationRequest(
        theme=draft_request.theme,
        evidence_pack=evidence_pack,
        source_report_ids=list(draft_request.report_ids),
        metadata=dict(draft_request.metadata),
    )
    runner = DraftWriter(settings)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="substack_draft_flow",
            worker_key=runner.worker_key,
            input_payload=worker_request.model_dump(mode="json"),
            runner=lambda: runner.run(worker_request),
            parent_run_id=run_id,
        )
        if run_id is not None:
            repository.update_run_status(
                run_id,
                status="completed",
                stdout=f"Completed draft flow for {draft_request.theme}",
                structured_outputs=result,
                artifact_manifest=result.get("artifacts", []),
                observation_summary=result.get("observations", []),
                next_suggested_events=result.get("next_events", []),
            )
        return result
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
