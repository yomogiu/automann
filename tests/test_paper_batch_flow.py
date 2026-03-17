from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import AdapterResult, WorkerStatus
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from flows.common import execute_adapter
from flows.paper_batch import paper_batch_flow


class PaperBatchFlowTests(unittest.TestCase):
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
        task_spec_id: str | None = None,
    ) -> dict:
        run_record = self.repository.start_run(
            flow_name=flow_name,
            worker_key=worker_key,
            input_payload=input_payload,
            task_spec_id=task_spec_id,
            parent_run_id=parent_run_id,
            status="running",
        )
        result = runner()
        persisted = self.repository.persist_adapter_result(run_record.id, result)
        assert persisted is not None
        return {
            "run_id": run_record.id,
            "parent_run_id": parent_run_id,
            "task_spec_id": task_spec_id,
            "status": result.status.value,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "structured_outputs": result.structured_outputs,
            "artifacts": [
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
            ],
            "observations": [item.model_dump(mode="json") for item in result.observations],
            "reports": [
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
            ],
            "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
        }

    def test_execute_adapter_inherits_task_spec_id_from_parent_run(self) -> None:
        task_spec = self.repository.create_task_spec(
            task_key="papers-nightly",
            task_type="paper_batch",
            title="Nightly papers",
        )
        parent = self.repository.start_run(
            flow_name="paper_batch_flow",
            worker_key="mini-process",
            input_payload={"papers": []},
            task_spec_id=task_spec.id,
            status="pending",
        )

        class _Logger:
            def info(self, *_args, **_kwargs) -> None:
                return None

        with patch("flows.common.get_run_logger", return_value=_Logger()):
            result = execute_adapter.fn(
                flow_name="paper_batch_flow",
                worker_key="paper_batch_runner",
                input_payload={"papers": []},
                runner=lambda: AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok"),
                parent_run_id=parent.id,
            )

        child = self.repository.get_run(result["run_id"])
        assert child is not None
        self.assertEqual(child.task_spec_id, task_spec.id)

    def test_paper_batch_flow_aggregates_child_reviews(self) -> None:
        parent = self.repository.start_run(
            flow_name="paper_batch_flow",
            worker_key="mini-process",
            input_payload={"title": "Batch"},
            status="pending",
        )
        child_results = [
            {
                "run_id": "child-1",
                "status": "completed",
                "artifacts": [{"id": "artifact-1", "kind": "review-card", "path": "/tmp/review-1.md"}],
                "observations": [{"kind": "paper", "summary": "paper 1"}],
                "result": {
                    "structured_outputs": {"paper_id": "2401.00001", "summary": "Summary 1", "source_url": "https://example.invalid/1"},
                    "reports": [{"id": "report-1", "title": "Paper 1", "summary": "Summary 1"}],
                },
            },
            {
                "run_id": "child-2",
                "status": "completed",
                "artifacts": [{"id": "artifact-2", "kind": "review-card", "path": "/tmp/review-2.md"}],
                "observations": [{"kind": "paper", "summary": "paper 2"}],
                "result": {
                    "structured_outputs": {"paper_id": "2401.00002", "summary": "Summary 2", "source_url": "https://example.invalid/2"},
                    "reports": [{"id": "report-2", "title": "Paper 2", "summary": "Summary 2"}],
                },
            },
        ]

        with patch("flows.paper_batch.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.paper_batch.execute_child_flow", side_effect=child_results):
                result = paper_batch_flow.fn(
                    request={
                        "title": "Model Serving Batch",
                        "papers": [
                            {"paper_id": "2401.00001"},
                            {"paper_id": "2401.00002"},
                        ],
                    },
                    run_id=parent.id,
                )

        self.assertEqual(result["status"], "completed")
        structured = result["structured_outputs"]
        self.assertEqual(structured["paper_count"], 2)
        self.assertEqual(len(structured["child_runs"]), 2)
        self.assertEqual(structured["completed_count"], 2)

        stored_parent = self.repository.get_run(parent.id)
        assert stored_parent is not None
        self.assertEqual(stored_parent.status, "completed")
        self.assertEqual(stored_parent.structured_outputs["structured_outputs"]["paper_count"], 2)

        report = self.repository.latest_report("paper_batch")
        self.assertIsNotNone(report)
        assert report is not None
        self.assertEqual(report.title, "Model Serving Batch")
