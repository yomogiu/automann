from __future__ import annotations

from libs.config import Settings
from libs.contracts.models import AdapterResult, BrowserJobRequest, ObservationRecord, WorkerStatus


class BrowserTaskRunner:
    worker_key = "browser_task_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: BrowserJobRequest) -> AdapterResult:
        return AdapterResult(
            status=WorkerStatus.SKIPPED,
            stdout="Browser automation lane is scaffolded but intentionally left shallow pending Playwright design decisions.",
            structured_outputs=request.model_dump(mode="json"),
            observations=[
                ObservationRecord(
                    kind="browser_job_deferred",
                    summary=f"Deferred browser automation for {request.job_name}",
                    payload=request.model_dump(mode="json"),
                    confidence=1.0,
                )
            ],
        )
