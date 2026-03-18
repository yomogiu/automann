from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from flows.browser_job import browser_job_flow
from libs.config import get_settings
from libs.contracts.models import AdapterResult, ObservationRecord, WorkerStatus
from libs.contracts.workers import BrowserTaskRequest
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from workers.common import build_file_artifact


class BrowserJobFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.env_overrides = {
            "LIFE_ARTIFACT_ROOT": str(self.root / "artifacts"),
            "LIFE_REPORT_ROOT": str(self.root / "reports"),
            "LIFE_RUNTIME_ROOT": str(self.root / "runtime"),
            "LIFE_DATABASE_URL": f"sqlite+pysqlite:///{self.root / 'runtime' / 'life.db'}",
        }
        self.previous_env = {key: os.environ.get(key) for key in self.env_overrides}
        for key, value in self.env_overrides.items():
            os.environ[key] = value

        settings = get_settings()
        bootstrap_life_database(settings)
        self.engine = engine_for_url(settings.life_database_url)
        self.repository = LifeRepository(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def _execute_adapter_inline(
        self,
        *,
        flow_name: str,
        worker_key: str,
        input_payload: dict,
        runner,
        parent_run_id: str | None = None,
    ) -> dict:
        run_record = self.repository.start_run(
            flow_name=flow_name,
            worker_key=worker_key,
            input_payload=input_payload,
            parent_run_id=parent_run_id,
            status="running",
        )
        result = runner()
        persisted = self.repository.persist_adapter_result(run_record.id, result)
        assert persisted is not None
        return {
            "run_id": run_record.id,
            "parent_run_id": parent_run_id,
            "status": result.status.value,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "structured_outputs": result.structured_outputs,
            "artifacts": [item.model_dump(mode="json") for item in result.artifact_manifest],
            "observations": [item.model_dump(mode="json") for item in result.observations],
            "reports": [item.model_dump(mode="json") for item in result.reports],
            "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
        }

    def test_flow_translates_public_request_to_internal_browser_worker_request(self) -> None:
        parent = self.repository.start_run(
            flow_name="browser_job_flow",
            worker_key="browser-process",
            input_payload={"job_name": "x-feed", "target_url": "https://x.com/home"},
            status="pending",
        )
        captured: dict[str, object] = {}
        handoff_capture: dict[str, object] = {}
        page_path = self.root / "browser-page.html"
        page_path.write_text("<html><body><h1>Browser Page</h1></body></html>", encoding="utf-8")
        browser_page_artifact = build_file_artifact(
            kind="browser-page",
            path=page_path,
            media_type="text/html",
            metadata={"role": "primary_page", "final_url": "https://x.com/home"},
        )

        def fake_browser_run(_runner_self, request: BrowserTaskRequest) -> AdapterResult:
            captured["request"] = request
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="browser run complete",
                structured_outputs={
                    "job_name": request.job_name,
                    "status": "completed",
                    "session_mode": request.session.mode,
                    "target_url": request.target_url,
                    "final_url": "https://x.com/home",
                    "page_title": "X Home",
                },
                artifact_manifest=[browser_page_artifact],
                observations=[
                    ObservationRecord(
                        kind="browser_navigation",
                        summary="navigated",
                        payload={"target_url": request.target_url},
                        confidence=0.8,
                    )
                ],
            )

        def fake_execute_child_flow(
            *,
            flow_name: str,
            worker_key: str,
            input_payload: dict,
            runner,
            parent_run_id: str | None = None,
            task_spec_id: str | None = None,
        ) -> dict:
            handoff_capture["flow_name"] = flow_name
            handoff_capture["worker_key"] = worker_key
            handoff_capture["input_payload"] = input_payload
            handoff_capture["parent_run_id"] = parent_run_id
            handoff_capture["task_spec_id"] = task_spec_id
            return {
                "run_id": "child-run-1",
                "parent_run_id": parent_run_id,
                "task_spec_id": task_spec_id,
                "status": "completed",
                "structured_outputs": {
                    "success_count": 1,
                    "failure_count": 0,
                    "warning_count": 0,
                    "items": [
                        {
                            "source_document_id": "source-1",
                            "status": "completed",
                            "canonical_uri": "https://x.com/home",
                        }
                    ],
                },
                "artifacts": [],
                "observations": [],
                "reports": [],
                "next_events": [],
            }

        with patch("flows.browser_job.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.browser_job.BrowserTaskRunner.run", autospec=True, side_effect=fake_browser_run):
                with patch("flows.browser_job.execute_child_flow", side_effect=fake_execute_child_flow):
                    result = browser_job_flow.fn(
                        request={
                            "job_name": "x-feed",
                            "target_url": "https://x.com/home",
                            "session": {"mode": "attach", "cdp_url": "http://127.0.0.1:9222"},
                            "steps": [{"op": "wait_for", "selector": "main"}],
                            "extract": [{"name": "first_post", "selector": "article", "kind": "text"}],
                            "capture_html": False,
                            "capture_screenshots": True,
                            "capture": {"html": False, "screenshot": False, "trace": False},
                        },
                        run_id=parent.id,
                    )

        self.assertEqual(result["status"], "completed")
        self.assertIsInstance(captured["request"], BrowserTaskRequest)
        request = captured["request"]
        assert isinstance(request, BrowserTaskRequest)
        self.assertEqual(request.session.mode, "attach")
        self.assertEqual(request.session.cdp_url, "http://127.0.0.1:9222")
        self.assertEqual(request.capture.html, False)
        self.assertEqual(request.capture.screenshot, False)
        self.assertEqual(request.steps[0].op, "wait_for")
        self.assertEqual(request.extract[0].name, "first_post")
        self.assertEqual(handoff_capture["flow_name"], "artifact_ingest_flow")
        self.assertEqual(handoff_capture["worker_key"], "artifact_ingest_runner")
        self.assertEqual(handoff_capture["parent_run_id"], parent.id)
        self.assertEqual(handoff_capture["input_payload"]["items"][0]["canonical_uri"], "https://x.com/home")
        self.assertEqual(handoff_capture["input_payload"]["items"][0]["file_path"], str(page_path.resolve()))
        self.assertEqual(handoff_capture["input_payload"]["items"][0]["metadata"]["browser_artifact_kind"], "browser-page")
        self.assertEqual(handoff_capture["input_payload"]["metadata"]["final_url"], "https://x.com/home")

        stored_parent = self.repository.get_run(parent.id)
        assert stored_parent is not None
        self.assertEqual(stored_parent.status, "completed")
        self.assertEqual(stored_parent.structured_outputs["structured_outputs"]["session_mode"], "attach")
        self.assertEqual(stored_parent.structured_outputs["structured_outputs"]["ingest_handoff"]["status"], "completed")
        self.assertEqual(
            stored_parent.structured_outputs["structured_outputs"]["ingest_handoff"]["source_document_ids"],
            ["source-1"],
        )
        self.assertEqual(stored_parent.observation_summary[0]["kind"], "browser_navigation")
        self.assertEqual(stored_parent.observation_summary[-1]["kind"], "artifact_ingest_handoff_completed")

    def test_browser_handoff_failure_keeps_parent_completed(self) -> None:
        parent = self.repository.start_run(
            flow_name="browser_job_flow",
            worker_key="browser-process",
            input_payload={"job_name": "x-feed", "target_url": "https://x.com/home"},
            status="pending",
        )
        page_path = self.root / "browser-page.html"
        page_path.write_text("<html><body><h1>Browser Page</h1></body></html>", encoding="utf-8")
        browser_page_artifact = build_file_artifact(
            kind="browser-page",
            path=page_path,
            media_type="text/html",
            metadata={"role": "primary_page", "final_url": "https://x.com/home"},
        )

        def fake_browser_run(_runner_self, request: BrowserTaskRequest) -> AdapterResult:
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="browser run complete",
                structured_outputs={
                    "job_name": request.job_name,
                    "status": "completed",
                    "session_mode": request.session.mode,
                    "target_url": request.target_url,
                    "final_url": "https://x.com/home",
                    "page_title": "X Home",
                },
                artifact_manifest=[browser_page_artifact],
                observations=[],
            )

        with patch("flows.browser_job.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.browser_job.BrowserTaskRunner.run", autospec=True, side_effect=fake_browser_run):
                with patch("flows.browser_job.execute_child_flow", side_effect=RuntimeError("ingest unavailable")):
                    result = browser_job_flow.fn(
                        request={
                            "job_name": "x-feed",
                            "target_url": "https://x.com/home",
                            "session": {"mode": "attach", "cdp_url": "http://127.0.0.1:9222"},
                            "capture": {"html": False, "screenshot": False, "trace": False},
                        },
                        run_id=parent.id,
                    )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["structured_outputs"]["ingest_handoff"]["status"], "failed")
        self.assertEqual(result["structured_outputs"]["ingest_handoff"]["reason"], "handoff_error")

        stored_parent = self.repository.get_run(parent.id)
        assert stored_parent is not None
        self.assertEqual(stored_parent.status, "completed")
        self.assertEqual(stored_parent.structured_outputs["structured_outputs"]["ingest_handoff"]["status"], "failed")
        self.assertEqual(stored_parent.observation_summary[-1]["kind"], "artifact_ingest_handoff_failed")
