from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from prefect import get_run_logger, task

try:
    from prefect.events import emit_event
except Exception:  # pragma: no cover - optional during local bootstrap
    emit_event = None

from libs.config import Settings, get_settings
from libs.contracts.models import AdapterResult
from libs.db import LifeRepository, engine_for_url


RunnerT = TypeVar("RunnerT")


def build_repository(settings: Settings | None = None) -> LifeRepository:
    config = settings or get_settings()
    return LifeRepository(engine_for_url(config.life_database_url))


def _emit_prefect_events(flow_name: str, run_id: str, result: AdapterResult) -> None:
    if emit_event is None:
        return
    resource = {
        "prefect.resource.id": f"automann.run.{run_id}",
        "prefect.resource.name": flow_name,
        "automann.run.id": run_id,
    }
    for event in result.next_suggested_events:
        try:
            emit_event(event=event.name, resource=resource, payload=event.payload)
        except Exception:
            continue


def _resolve_task_spec_id(
    repository: LifeRepository,
    *,
    task_spec_id: str | None = None,
    parent_run_id: str | None = None,
) -> str | None:
    if task_spec_id:
        return task_spec_id
    if parent_run_id is None:
        return None
    parent_run = repository.get_run(parent_run_id)
    if parent_run is None:
        return None
    return parent_run.task_spec_id


def execute_child_flow(
    *,
    flow_name: str,
    worker_key: str,
    input_payload: dict[str, Any],
    runner: Callable[[str], dict[str, Any]],
    parent_run_id: str | None = None,
    task_spec_id: str | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    repository = build_repository(settings)
    resolved_task_spec_id = _resolve_task_spec_id(
        repository,
        task_spec_id=task_spec_id,
        parent_run_id=parent_run_id,
    )
    run_record = repository.start_run(
        flow_name=flow_name,
        worker_key=worker_key,
        input_payload=input_payload,
        task_spec_id=resolved_task_spec_id,
        parent_run_id=parent_run_id,
        status="pending",
    )
    try:
        result = runner(run_record.id)
    except Exception as exc:
        repository.update_run_status(run_record.id, status="failed", stderr=str(exc))
        raise

    refreshed = repository.get_run(run_record.id)
    return {
        "run_id": run_record.id,
        "parent_run_id": parent_run_id,
        "task_spec_id": resolved_task_spec_id,
        "status": refreshed.status if refreshed is not None else str(result.get("status") or "completed"),
        "prefect_flow_run_id": refreshed.prefect_flow_run_id if refreshed is not None else None,
        "structured_outputs": refreshed.structured_outputs if refreshed is not None else dict(result),
        "artifacts": refreshed.artifact_manifest if refreshed is not None else list(result.get("artifacts", [])),
        "observations": refreshed.observation_summary if refreshed is not None else list(result.get("observations", [])),
        "reports": result.get("reports", []),
        "result": result,
    }


@task
def execute_adapter(
    *,
    flow_name: str,
    worker_key: str,
    input_payload: dict[str, Any],
    runner: Callable[[], AdapterResult],
    parent_run_id: str | None = None,
    task_spec_id: str | None = None,
) -> dict[str, Any]:
    logger = get_run_logger()
    settings = get_settings()
    repository = build_repository(settings)
    resolved_task_spec_id = _resolve_task_spec_id(
        repository,
        task_spec_id=task_spec_id,
        parent_run_id=parent_run_id,
    )
    run_record = repository.start_run(
        flow_name=flow_name,
        worker_key=worker_key,
        input_payload=input_payload,
        task_spec_id=resolved_task_spec_id,
        parent_run_id=parent_run_id,
        status="running",
    )
    logger.info("Started %s for flow %s", worker_key, flow_name)
    try:
        result = runner()
    except Exception as exc:
        repository.update_run_status(run_record.id, status="failed", stderr=str(exc))
        raise
    persisted = repository.persist_adapter_result(run_record.id, result)
    _emit_prefect_events(flow_name, run_record.id, result)

    if persisted is not None:
        artifacts = [
            {
                "id": item.id,
                "kind": item.kind,
                "path": item.path,
                "storage_uri": item.storage_uri,
                "size_bytes": item.size_bytes,
                "media_type": item.media_type,
                "sha256": item.sha256,
                "metadata": item.metadata_json,
            }
            for item in persisted.artifacts
        ]
        reports = [
            {
                "id": item.id,
                "report_type": item.report_type,
                "title": item.title,
                "summary": item.summary,
                "content_markdown": item.content_markdown,
                "source_artifact_id": item.source_artifact_id,
                "report_series_id": item.report_series_id,
                "revision_number": item.revision_number,
                "supersedes_report_id": item.supersedes_report_id,
                "is_current": item.is_current,
                "metadata": item.metadata_json,
            }
            for item in persisted.reports
        ]
    else:
        artifacts = [item.model_dump(mode="json") for item in result.artifact_manifest]
        reports = [item.model_dump(mode="json") for item in result.reports]

    return {
        "run_id": run_record.id,
        "parent_run_id": parent_run_id,
        "task_spec_id": resolved_task_spec_id,
        "status": result.status.value,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "structured_outputs": result.structured_outputs,
        "artifacts": artifacts,
        "observations": [item.model_dump(mode="json") for item in result.observations],
        "reports": reports,
        "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
    }
