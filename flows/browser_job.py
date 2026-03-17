from __future__ import annotations

from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import BrowserJobRequest
from libs.contracts.workers import BrowserTaskRequest
from workers.browser_runner import BrowserTaskRunner

from .common import build_repository, execute_adapter


@flow(name="browser-job")
def browser_job_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = build_repository(settings)
    command_request = BrowserJobRequest.model_validate(request)
    browser_request = BrowserTaskRequest.from_command(command_request)
    runner = BrowserTaskRunner(settings)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="browser_job_flow",
            worker_key=runner.worker_key,
            input_payload=browser_request.model_dump(mode="json"),
            runner=lambda: runner.run(browser_request),
            parent_run_id=run_id,
        )
        if run_id is not None:
            final_status = str(result.get("status") or "completed")
            repository.update_run_status(
                run_id,
                status=final_status,
                stdout=result.get("stdout") or f"Completed browser job {browser_request.job_name}",
                stderr=result.get("stderr") or "",
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
