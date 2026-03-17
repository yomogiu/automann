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
        "prefect.resource.id": f"life.run.{run_id}",
        "prefect.resource.name": flow_name,
        "life.run.id": run_id,
    }
    for event in result.next_suggested_events:
        try:
            emit_event(event=event.name, resource=resource, payload=event.payload)
        except Exception:
            continue


@task
def execute_adapter(
    *,
    flow_name: str,
    worker_key: str,
    input_payload: dict[str, Any],
    runner: Callable[[], AdapterResult],
    parent_run_id: str | None = None,
) -> dict[str, Any]:
    logger = get_run_logger()
    settings = get_settings()
    repository = build_repository(settings)
    run_record = repository.start_run(
        flow_name=flow_name,
        worker_key=worker_key,
        input_payload=input_payload,
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
        "status": result.status.value,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "structured_outputs": result.structured_outputs,
        "artifacts": artifacts,
        "observations": [item.model_dump(mode="json") for item in result.observations],
        "reports": reports,
        "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
    }
