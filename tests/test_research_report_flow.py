from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import AdapterResult, ObservationRecord, ReportRecord, WorkerStatus
from libs.contracts.workers import ResearchReportWorkerRequest
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from flows.research_report import PROMOTE_REVISION_CHECKPOINT, research_report_flow


class ResearchReportFlowTests(unittest.TestCase):
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
                    "report_series_id": item.report_series_id,
                    "revision_number": item.revision_number,
                    "supersedes_report_id": item.supersedes_report_id,
                    "is_current": item.is_current,
                    "metadata": item.metadata_json,
                }
                for item in persisted.reports
            ],
            "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
        }

    @staticmethod
    def _worker_result(request: ResearchReportWorkerRequest) -> AdapterResult:
        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"research {request.revision_number}",
            structured_outputs={
                "report_key": request.report_key,
                "revision_number": request.revision_number,
                "edit_mode": request.edit_mode,
            },
            observations=[
                ObservationRecord(
                    kind="research_revision",
                    summary=f"revision {request.revision_number}",
                    payload={"report_key": request.report_key},
                    confidence=0.7,
                )
            ],
            reports=[
                ReportRecord(
                    report_type="research_report",
                    title=f"Research Report {request.revision_number}",
                    summary=f"summary {request.revision_number}",
                    content_markdown=f"# Revision {request.revision_number}\n",
                    report_series_id=request.report_key,
                    revision_number=request.revision_number,
                    supersedes_report_id=request.supersedes_report_id,
                    is_current=False,
                    metadata={
                        "report_key": request.report_key,
                        "edit_mode": request.edit_mode,
                        "taxonomy": {"filters": ["report"], "tags": ["deep_research"]},
                    },
                )
            ],
        )

    def test_flow_promotes_current_revision_and_increments_series(self) -> None:
        first_parent = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "Edge AI infrastructure"},
            status="pending",
        )
        second_parent = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "Edge AI infrastructure"},
            status="pending",
        )
        captured: list[ResearchReportWorkerRequest] = []
        retrieval_context = [{"id": "chunk-1", "text": "GPU supply remains tight.", "url": "https://example.invalid"}]

        def fake_run(_runner_self, request: ResearchReportWorkerRequest) -> AdapterResult:
            captured.append(request)
            return self._worker_result(request)

        with patch("flows.research_report.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.research_report.RetrievalService.query", autospec=True, return_value=retrieval_context):
                with patch("flows.research_report.ResearchReportRunner.run", autospec=True, side_effect=fake_run):
                    first_result = research_report_flow.fn(
                        request={
                            "theme": "Edge AI infrastructure",
                            "report_key": "edge-ai",
                            "areas_of_interest": ["supply chain"],
                            "boundaries": ["North America"],
                            "edit_mode": "merge",
                            "human_policy": {"mode": "auto"},
                        },
                        run_id=first_parent.id,
                    )
                    second_result = research_report_flow.fn(
                        request={
                            "theme": "Edge AI infrastructure",
                            "report_key": "edge-ai",
                            "areas_of_interest": ["power constraints"],
                            "boundaries": ["North America"],
                            "edit_mode": "append",
                            "human_policy": {"mode": "auto"},
                        },
                        run_id=second_parent.id,
                    )

        self.assertEqual(len(captured), 2)
        self.assertEqual(captured[0].revision_number, 1)
        self.assertIsNone(captured[0].supersedes_report_id)
        self.assertEqual(captured[0].retrieval_context, retrieval_context)
        self.assertEqual(captured[1].revision_number, 2)
        self.assertEqual(captured[1].edit_mode, "append")
        self.assertIsNotNone(captured[1].previous_report)

        self.assertEqual(first_result["revision_number"], 1)
        self.assertIsNotNone(first_result["current_report_id"])
        self.assertIsNone(first_result["pending_report_id"])
        self.assertIsNone(first_result["checkpoint"])

        self.assertEqual(second_result["revision_number"], 2)
        self.assertIsNotNone(second_result["current_report_id"])
        self.assertIsNone(second_result["pending_report_id"])
        self.assertIsNone(second_result["checkpoint"])

        current = self.repository.current_report_revision("edge-ai")
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.id, second_result["current_report_id"])
        self.assertEqual(current.revision_number, 2)

        revisions = self.repository.list_report_revisions(current.id)
        self.assertEqual([item.revision_number for item in revisions], [2, 1])
        self.assertTrue(revisions[0].is_current)
        self.assertFalse(revisions[1].is_current)
        self.assertEqual(captured[1].supersedes_report_id, revisions[1].id)

    def test_flow_creates_checkpoint_and_leaves_revision_pending(self) -> None:
        parent = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "Energy transition"},
            status="pending",
        )
        captured: dict[str, ResearchReportWorkerRequest] = {}

        def fake_run(_runner_self, request: ResearchReportWorkerRequest) -> AdapterResult:
            captured["request"] = request
            return self._worker_result(request)

        with patch("flows.research_report.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.research_report.RetrievalService.query", autospec=True, return_value=[]):
                with patch("flows.research_report.ResearchReportRunner.run", autospec=True, side_effect=fake_run):
                    result = research_report_flow.fn(
                        request={
                            "theme": "Energy transition",
                            "report_key": "energy-transition",
                            "areas_of_interest": ["grid capacity"],
                            "boundaries": ["public data"],
                            "human_policy": {"mode": "checkpointed", "checkpoints": [PROMOTE_REVISION_CHECKPOINT]},
                        },
                        run_id=parent.id,
                    )

        self.assertEqual(captured["request"].revision_number, 1)
        self.assertIsNotNone(result["checkpoint"])
        self.assertEqual(result["checkpoint"]["status"], "waiting_input")
        self.assertEqual(result["checkpoint"]["checkpoint_key"], PROMOTE_REVISION_CHECKPOINT)
        self.assertIsNone(result["current_report_id"])
        self.assertIsNotNone(result["pending_report_id"])

        stored_parent = self.repository.get_run(parent.id)
        assert stored_parent is not None
        self.assertEqual(stored_parent.status, "waiting_input")
        self.assertEqual(stored_parent.structured_outputs["pending_report_id"], result["pending_report_id"])

        interactions = self.repository.list_interactions(limit=10, status="open")
        self.assertEqual(len(interactions), 1)
        interaction = interactions[0]
        self.assertEqual(interaction.checkpoint_key, PROMOTE_REVISION_CHECKPOINT)
        self.assertEqual(interaction.ui_hints["report_id"], result["pending_report_id"])

        pending_report = self.repository.get_report(result["pending_report_id"])
        self.assertIsNotNone(pending_report)
        assert pending_report is not None
        self.assertFalse(pending_report.is_current)
