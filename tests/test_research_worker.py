from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import AdapterResult, WorkerStatus
from libs.contracts.workers import ResearchReportWorkerRequest
from workers.codex_runner import CodexCliRequest
from workers.research_runner import ResearchReportRunner


class ResearchReportRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.settings = get_settings().model_copy(
            update={
                "artifact_root": self.root / "artifacts",
                "report_root": self.root / "reports",
                "runtime_root": self.root / "runtime",
            }
        )
        self.settings.artifact_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _build_request() -> ResearchReportWorkerRequest:
        return ResearchReportWorkerRequest(
            theme="Edge AI infrastructure",
            boundaries=["US and Canada only", "Public sources only"],
            areas_of_interest=["capital intensity", "GPU supply"],
            report_key="edge-ai-infra",
            edit_mode="merge",
            revision_number=3,
            supersedes_report_id="report-prev",
            retrieval_context=[
                {"id": "chunk-1", "text": "GPU demand rose in 2025.", "url": "https://example.invalid/source-1"}
            ],
            metadata={"audience": "operators"},
        )

    def test_runner_uses_deterministic_placeholder_when_codex_output_is_unavailable(self) -> None:
        runner = ResearchReportRunner(self.settings)
        request = self._build_request()

        with patch(
            "workers.research_runner.runner.CodexCliRunner.run",
            return_value=AdapterResult(status=WorkerStatus.SKIPPED, stdout="codex unavailable"),
        ):
            result = runner.run(request)

        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["mode"], "placeholder")
        artifact_kinds = {item.kind for item in result.artifact_manifest}
        self.assertIn("research-report", artifact_kinds)
        self.assertIn("research-output", artifact_kinds)
        self.assertIn("research-manifest", artifact_kinds)
        self.assertIn("research-table", artifact_kinds)

        report = result.reports[0]
        self.assertEqual(report.report_type, "research_report")
        self.assertEqual(report.report_series_id, request.report_key)
        self.assertEqual(report.revision_number, request.revision_number)
        self.assertEqual(report.supersedes_report_id, request.supersedes_report_id)
        self.assertFalse(report.is_current)

        output_artifact = next(item for item in result.artifact_manifest if item.kind == "research-output")
        payload = json.loads(Path(output_artifact.path).read_text(encoding="utf-8"))
        self.assertEqual(payload["title"], "Research Report: Edge AI infrastructure")
        self.assertTrue(payload["tables"])

    def test_runner_materializes_codex_structured_output_into_markdown_json_csv(self) -> None:
        runner = ResearchReportRunner(self.settings)
        request = self._build_request()

        codex_payload = {
            "title": "Edge AI Infrastructure Revision",
            "summary": "Capacity bottlenecks are shifting from compute to grid and cooling constraints.",
            "findings": [
                {
                    "headline": "Power limits",
                    "claim": "Regional power interconnects are now the deployment bottleneck.",
                    "inference": "Capacity planning must include utility lead times.",
                    "confidence": 0.72,
                }
            ],
            "section_updates": [
                {"section": "Constraints", "action": "append", "content": "Add utility queue lead-time analysis."}
            ],
            "tables": [
                {
                    "name": "capacity_snapshot",
                    "columns": ["region", "gpu_mw", "grid_status"],
                    "rows": [["US-East", 320, "tight"], ["Canada-Central", 150, "moderate"]],
                }
            ],
            "citations": [{"label": "utility-brief", "url": "https://example.invalid/utility", "note": "Queue data"}],
        }

        def fake_codex_run(codex_request: CodexCliRequest) -> AdapterResult:
            output_path = Path(codex_request.output_path or "")
            output_path.write_text(json.dumps(codex_payload, indent=2), encoding="utf-8")
            return AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok")

        with patch("workers.research_runner.runner.CodexCliRunner.run", side_effect=fake_codex_run):
            result = runner.run(request)

        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["mode"], "codex")
        self.assertEqual(result.structured_outputs["title"], "Edge AI Infrastructure Revision")
        self.assertEqual(result.structured_outputs["summary"], codex_payload["summary"])

        csv_artifact = next(item for item in result.artifact_manifest if item.kind == "research-table")
        csv_text = Path(csv_artifact.path).read_text(encoding="utf-8")
        self.assertIn("region,gpu_mw,grid_status", csv_text)
        self.assertIn("US-East,320,tight", csv_text)
        self.assertIn("Canada-Central,150,moderate", csv_text)

        report = result.reports[0]
        self.assertEqual(report.title, "Edge AI Infrastructure Revision")
        self.assertEqual(report.metadata["generation_mode"], "codex")
        self.assertEqual(report.metadata["table_count"], 1)
