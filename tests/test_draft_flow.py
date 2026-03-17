from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import AdapterResult, ObservationRecord, ReportRecord, WorkerStatus
from libs.contracts.workers import (
    DraftGenerationOutput,
    DraftGenerationRequest,
    PublicationGenerationOutput,
    PublicationGenerationRequest,
)
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from flows.draft_article import substack_draft_flow
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text


class DraftFlowTests(unittest.TestCase):
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
                    "metadata": item.metadata_json,
                }
                for item in persisted.reports
            ],
            "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
        }

    def test_flow_builds_draft_generation_request_for_worker(self) -> None:
        payload = {
            "theme": "Local research systems",
            "report_ids": ["report-1", "report-2"],
            "metadata": {"audience": "operators"},
        }
        evidence_pack = [
            {"id": "chunk-1", "text": "Evidence one", "metadata": {"kind": "note"}},
            {"id": "chunk-2", "text": "Evidence two", "metadata": {"kind": "note"}},
        ]
        parent = self.repository.start_run(
            flow_name="substack_draft_flow",
            worker_key="mini-process",
            input_payload=payload,
            status="pending",
        )
        captured: dict[str, object] = {}

        def fake_draft_run(_runner_self, request: DraftGenerationRequest) -> AdapterResult:
            captured["draft_request"] = request
            run_dir = ensure_worker_dir(get_settings(), "draft_writer_test")
            markdown_path = run_dir / "substack_draft.md"
            bundle_path = run_dir / "source_bundle.json"
            write_text(markdown_path, "# Draft\n")
            write_json(bundle_path, {"theme": request.theme, "evidence_pack": request.evidence_pack})
            output = DraftGenerationOutput(
                theme=request.theme,
                evidence_count=len(request.evidence_pack),
                source_report_ids=request.source_report_ids,
            )
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="draft ok",
                artifact_manifest=[
                    build_file_artifact(kind="draft", path=markdown_path, media_type="text/markdown"),
                    build_file_artifact(kind="source-bundle", path=bundle_path, media_type="application/json"),
                ],
                structured_outputs=output.model_dump(mode="json"),
                observations=[
                    ObservationRecord(
                        kind="draft_theme",
                        summary="theme",
                        payload={"theme": request.theme, "evidence_count": len(request.evidence_pack)},
                        confidence=0.7,
                    )
                ],
                reports=[
                    ReportRecord(
                        report_type="substack_draft",
                        title=f"Draft: {request.theme}",
                        summary="summary",
                        content_markdown="# Draft\n",
                        artifact_path=str(markdown_path),
                        metadata={
                            "theme": request.theme,
                            "taxonomy": {"filters": ["report"], "tags": ["deep_research"]},
                        },
                    )
                ],
            )

        with patch("flows.draft_article.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.draft_article.RetrievalService.query", autospec=True, return_value=evidence_pack):
                with patch("flows.draft_article.DraftWriter.run", autospec=True, side_effect=fake_draft_run):
                    result = substack_draft_flow.fn(request=payload, run_id=parent.id)

        self.assertEqual(result["status"], "completed")
        self.assertIsInstance(captured["draft_request"], DraftGenerationRequest)
        self.assertEqual(captured["draft_request"].theme, payload["theme"])
        self.assertEqual(captured["draft_request"].evidence_pack, evidence_pack)
        self.assertEqual(captured["draft_request"].source_report_ids, payload["report_ids"])
        self.assertEqual(captured["draft_request"].metadata, payload["metadata"])

        stored_parent = self.repository.get_run(parent.id)
        assert stored_parent is not None
        self.assertEqual(stored_parent.input_payload, payload)

        child_runs = [
            row
            for row in self.repository.list_runs(limit=10, top_level_only=False)
            if row.parent_run_id == parent.id
        ]
        self.assertEqual(len(child_runs), 1)
        child_payload = child_runs[0].input_payload
        self.assertEqual(child_payload["theme"], payload["theme"])
        self.assertEqual(child_payload["source_report_ids"], payload["report_ids"])
        self.assertEqual(child_payload["evidence_pack"], evidence_pack)
        self.assertNotIn("session_id", child_payload)

    def test_flow_prefers_current_revision_when_source_report_is_provided(self) -> None:
        seed_run = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="worker",
            input_payload={"theme": "Edge AI"},
            status="running",
        )
        seed_result = AdapterResult(
            status=WorkerStatus.COMPLETED,
            reports=[
                ReportRecord(
                    report_type="research_report",
                    title="Edge AI Research",
                    summary="v1",
                    content_markdown="# V1\n",
                    report_series_id="edge-ai",
                    revision_number=1,
                    is_current=False,
                    metadata={"report_key": "edge-ai"},
                ),
                ReportRecord(
                    report_type="research_report",
                    title="Edge AI Research",
                    summary="v2",
                    content_markdown="# V2\n\nCurrent report body\n",
                    report_series_id="edge-ai",
                    revision_number=2,
                    is_current=False,
                    metadata={"report_key": "edge-ai"},
                ),
            ],
        )
        persisted = self.repository.persist_adapter_result(seed_run.id, seed_result)
        assert persisted is not None
        first_revision, second_revision = persisted.reports
        self.repository.promote_report_revision(first_revision.id)
        self.repository.promote_report_revision(second_revision.id)

        parent = self.repository.start_run(
            flow_name="substack_draft_flow",
            worker_key="mini-process",
            input_payload={"source_report_id": first_revision.id},
            status="pending",
        )
        captured: dict[str, object] = {}

        def fake_publication_run(_runner_self, request: PublicationGenerationRequest) -> AdapterResult:
            captured["draft_request"] = request
            run_dir = ensure_worker_dir(get_settings(), "draft_writer_test")
            markdown_path = run_dir / "substack_draft.md"
            bundle_path = run_dir / "source_bundle.json"
            write_text(markdown_path, "# Publication Draft\n")
            write_json(bundle_path, {"source_revision_id": request.source_revision_id})
            output = PublicationGenerationOutput(
                title="Publication Draft",
                source_report_id=request.source_report_id,
                source_revision_id=request.source_revision_id,
                mode="template-fallback",
            )
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="publication ok",
                artifact_manifest=[
                    build_file_artifact(kind="draft", path=markdown_path, media_type="text/markdown"),
                    build_file_artifact(kind="source-bundle", path=bundle_path, media_type="application/json"),
                ],
                structured_outputs={
                    "theme": request.theme or output.title,
                    "evidence_count": 0,
                    "source_report_ids": [request.source_report_id] if request.source_report_id else [],
                    **output.model_dump(mode="json"),
                },
                observations=[
                    ObservationRecord(
                        kind="publication_transform",
                        summary="publication",
                        payload=output.model_dump(mode="json"),
                        confidence=0.8,
                    )
                ],
                reports=[
                    ReportRecord(
                        report_type="substack_draft",
                        title=output.title,
                        summary="source driven",
                        content_markdown="# Publication Draft\n",
                        artifact_path=str(markdown_path),
                        metadata={
                            "source_report_id": request.source_report_id,
                            "source_revision_id": request.source_revision_id,
                            "taxonomy": {"filters": ["report"], "tags": ["deep_research"]},
                        },
                    )
                ],
            )

        with patch("flows.draft_article.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.draft_article.DraftWriter.run", autospec=True, side_effect=fake_publication_run):
                result = substack_draft_flow.fn(
                    request={"source_report_id": first_revision.id},
                    run_id=parent.id,
                )

        self.assertEqual(result["status"], "completed")
        self.assertIsInstance(captured["draft_request"], PublicationGenerationRequest)
        self.assertEqual(captured["draft_request"].source_report_id, first_revision.id)
        self.assertEqual(captured["draft_request"].source_revision_id, second_revision.id)
        self.assertEqual(captured["draft_request"].source_markdown, "# V2\n\nCurrent report body\n")
