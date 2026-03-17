from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import AdapterResult, WorkerStatus
from libs.contracts.workers import PublicationGenerationRequest
from workers.draft_runner.runner import DraftGenerationRequest, DraftWriter
from workers.publisher.runner import GitHubPublisher, PublishRequest


class DraftWorkerTests(unittest.TestCase):
    def test_draft_writer_uses_generation_request_and_typed_outputs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(
                update={
                    "artifact_root": root / "artifacts",
                    "report_root": root / "reports",
                    "runtime_root": root / "runtime",
                }
            )
            runner = DraftWriter(settings)
            request = DraftGenerationRequest(
                theme="Local agent systems",
                evidence_pack=[{"text": "Evidence A"}, {"text": "Evidence B"}],
                source_report_ids=["r-1", "r-2"],
                metadata={"audience": "operators"},
            )

            result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["theme"], "Local agent systems")
            self.assertEqual(result.structured_outputs["evidence_count"], 2)
            self.assertEqual(result.structured_outputs["source_report_ids"], ["r-1", "r-2"])
            artifact_kinds = {item.kind for item in result.artifact_manifest}
            self.assertEqual(artifact_kinds, {"draft", "source-bundle"})
            self.assertEqual(result.reports[0].report_type, "substack_draft")
            self.assertEqual(
                result.reports[0].metadata["taxonomy"],
                {"filters": ["report"], "tags": ["deep_research"]},
            )
            self.assertEqual(result.reports[0].metadata["mode"], "legacy-template")

    def test_source_driven_publication_prefers_codex_cli(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(
                update={
                    "artifact_root": root / "artifacts",
                    "report_root": root / "reports",
                    "runtime_root": root / "runtime",
                }
            )
            runner = DraftWriter(settings)
            request = PublicationGenerationRequest(
                source_report_id="report-123",
                source_revision_id="rev-2",
                theme="AI Infrastructure",
                source_markdown="# Research Revision\n\nKey findings from recent work.",
                metadata={"audience": "operators"},
            )

            with patch(
                "workers.draft_runner.runner.CodexCliRunner.run",
                return_value=AdapterResult(
                    status=WorkerStatus.COMPLETED,
                    stdout="# Publication Title\n\nPolished publication body.",
                ),
            ) as codex_run:
                result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["mode"], "codex-cli")
            self.assertEqual(result.structured_outputs["title"], "Publication Title")
            self.assertEqual(result.structured_outputs["source_report_id"], "report-123")
            self.assertEqual(result.structured_outputs["source_revision_id"], "rev-2")
            self.assertEqual(result.structured_outputs["source_report_ids"], ["report-123"])
            self.assertEqual(result.reports[0].title, "Publication Title")
            self.assertEqual(result.reports[0].metadata["mode"], "codex-cli")
            self.assertEqual(result.reports[0].metadata["source_report_id"], "report-123")
            self.assertEqual(result.reports[0].metadata["source_revision_id"], "rev-2")
            codex_run.assert_called_once()

    def test_source_driven_publication_falls_back_when_codex_not_available(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(
                update={
                    "artifact_root": root / "artifacts",
                    "report_root": root / "reports",
                    "runtime_root": root / "runtime",
                }
            )
            runner = DraftWriter(settings)
            request = PublicationGenerationRequest(
                source_report_id="report-777",
                theme="Open Research Theme",
                source_markdown="# Source\n\nObservation A\n\nObservation B",
            )

            with patch(
                "workers.draft_runner.runner.CodexCliRunner.run",
                return_value=AdapterResult(
                    status=WorkerStatus.SKIPPED,
                    stdout="",
                    stderr="Codex CLI is unavailable",
                ),
            ):
                result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["mode"], "template-fallback")
            self.assertEqual(result.structured_outputs["source_report_id"], "report-777")
            self.assertEqual(result.reports[0].metadata["mode"], "template-fallback")
            draft_artifact = next(item for item in result.artifact_manifest if item.kind == "draft")
            draft_text = Path(draft_artifact.path).read_text(encoding="utf-8")
            self.assertIn("Source Material", draft_text)
            self.assertIn("Observation A", draft_text)

    def test_legacy_request_with_source_markdown_uses_source_transform_mode(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(
                update={
                    "artifact_root": root / "artifacts",
                    "report_root": root / "reports",
                    "runtime_root": root / "runtime",
                }
            )
            runner = DraftWriter(settings)
            request = DraftGenerationRequest(
                theme="Legacy theme should still pass through",
                evidence_pack=[{"text": "unused in source mode"}],
                metadata={
                    "source_markdown": "# Revision\n\nEvidence from research report.",
                    "source_report_id": "report-legacy",
                },
            )

            with patch(
                "workers.draft_runner.runner.CodexCliRunner.run",
                return_value=AdapterResult(
                    status=WorkerStatus.COMPLETED,
                    stdout="# Transformed\n\nConverted from source markdown.",
                ),
            ):
                result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["mode"], "codex-cli")
            self.assertEqual(result.structured_outputs["source_report_id"], "report-legacy")
            self.assertEqual(result.reports[0].title, "Transformed")


class PublisherWorkerTests(unittest.TestCase):
    def _build_settings(self, root: Path, *, owner: str = "", repo: str = ""):
        return get_settings().model_copy(
            update={
                "artifact_root": root / "artifacts",
                "report_root": root / "reports",
                "runtime_root": root / "runtime",
                "github_owner": owner,
                "github_repo": repo,
            }
        )

    @staticmethod
    def _write_input_files(root: Path) -> tuple[Path, Path]:
        report_path = root / "report.md"
        artifact_path = root / "artifact.json"
        report_path.write_text("# Daily brief\n", encoding="utf-8")
        artifact_path.write_text('{"ok": true}\n', encoding="utf-8")
        return report_path, artifact_path

    def test_publisher_local_bundle_output_contract(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report_path, artifact_path = self._write_input_files(root)
            settings = self._build_settings(root)
            runner = GitHubPublisher(settings)
            request = PublishRequest(report_path=str(report_path), artifact_paths=[str(artifact_path)])

            with patch("workers.publisher.runner.shutil.which", return_value=None):
                result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["publish_mode"], "local-bundle")
            self.assertTrue(result.structured_outputs["release_tag"])
            self.assertTrue(Path(result.structured_outputs["bundle_dir"]).exists())
            self.assertTrue(Path(result.structured_outputs["manifest_path"]).exists())
            self.assertEqual(result.artifact_manifest[0].kind, "publication-manifest")

    def test_publisher_github_release_success(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report_path, artifact_path = self._write_input_files(root)
            settings = self._build_settings(root, owner="acme", repo="life-system")
            runner = GitHubPublisher(settings)
            request = PublishRequest(report_path=str(report_path), artifact_paths=[str(artifact_path)])

            class Completed:
                returncode = 0
                stdout = "release-created"
                stderr = ""

            with patch("workers.publisher.runner.shutil.which", return_value="/usr/bin/gh"):
                with patch("workers.publisher.runner.subprocess.run", return_value=Completed()) as run_mock:
                    result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.stdout, "release-created")
            self.assertEqual(result.structured_outputs["publish_mode"], "github-release")
            run_mock.assert_called_once()

    def test_publisher_github_release_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report_path, artifact_path = self._write_input_files(root)
            settings = self._build_settings(root, owner="acme", repo="life-system")
            runner = GitHubPublisher(settings)
            request = PublishRequest(report_path=str(report_path), artifact_paths=[str(artifact_path)])

            class Completed:
                returncode = 1
                stdout = ""
                stderr = "release failed"

            with patch("workers.publisher.runner.shutil.which", return_value="/usr/bin/gh"):
                with patch("workers.publisher.runner.subprocess.run", return_value=Completed()):
                    result = runner.run(request)

            self.assertEqual(result.status.value, "failed")
            self.assertEqual(result.stderr, "release failed")
            self.assertEqual(result.structured_outputs["publish_mode"], "github-release")
            self.assertTrue(Path(result.structured_outputs["manifest_path"]).exists())
