from __future__ import annotations

import json
import os
import copy
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import AdapterResult, ArtifactRecord, PaperReviewRequest, WorkerStatus
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from flows.paper_review import paper_review_flow
from workers.codex_runner import CodexCliRequest
from workers.ingest_runner import ArxivReviewRunner


def _annotation_payload() -> dict:
    return {
        "paper": {
            "paper_id": "2403.01234",
            "title": "Example Paper",
            "source_url": "https://arxiv.org/abs/2403.01234",
            "source_kind": "arxiv_text",
        },
        "report": {
            "document_type": "research paper",
            "primary_focus": "mechanistic interpretability",
            "one_sentence_summary": "The paper studies induction heads as a concrete transformer mechanism for in-context sequence continuation.",
            "executive_summary": [
                "The paper focuses on induction heads as a mechanistic circuit rather than a vague emergent behavior.",
                "Its main value is explanatory clarity around how prior-token pattern matching supports continuation."
            ],
            "problem_and_scope": [
                "The scope is understanding one concrete transformer mechanism and what it explains about in-context behavior."
            ],
            "method_and_architecture": [
                "The analysis is mechanism-focused and studies induction heads inside transformer circuits."
            ],
            "training_and_data": [
                "The provided excerpt does not supply a detailed training or data story."
            ],
            "evaluation_and_evidence": [
                "The excerpt gives direct mechanism framing in the abstract but not a full evaluation section."
            ],
            "novelty_and_lineage": [
                "The main contribution appears to be a clearer mechanistic account of an already recognizable transformer behavior."
            ],
            "limitations_and_risks": [
                "The supplied excerpt is too small to audit the full evidence base."
            ],
            "practical_takeaways": [
                "Readers should treat induction heads as a concrete reusable explanatory tool for sequence continuation behavior."
            ],
            "open_questions": [
                "How broadly does this mechanism explain real downstream behavior beyond the examples highlighted in the paper?"
            ],
        },
        "concepts": [
            {
                "concept_id": "c1",
                "name": "Induction Heads",
                "summary": "Induction heads copy sequence motifs from prior context.",
            }
        ],
        "blocks": [
            {
                "block_id": "b-0001",
                "section_label": "Abstract",
                "order": 1,
                "text": "We analyze induction heads in transformer circuits.",
            }
        ],
        "annotations": [
            {
                "annotation_id": "a1",
                "block_id": "b-0001",
                "anchor_text": "induction heads",
                "label": "AI Analysis",
                "kind": "mechanistic_explanation",
                "analysis": "The phrase points to a concrete mechanism rather than a vague capability claim.",
                "concept_ids": ["c1"],
                "confidence": 0.83,
                "evidence": [
                    {
                        "quote": "We analyze induction heads in transformer circuits.",
                        "reason": "The abstract frames induction heads as the central mechanism under analysis.",
                        "source_ref": "Abstract",
                    }
                ],
            }
        ],
    }


class PaperReviewRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.settings = get_settings().model_copy(
            update={
                "artifact_root": self.root / "artifacts",
                "report_root": self.root / "reports",
                "runtime_root": self.root / "runtime",
                "life_database_url": f"sqlite+pysqlite:///{self.root / 'runtime' / 'life.db'}",
            }
        )
        self.settings.artifact_root.mkdir(parents=True, exist_ok=True)
        bootstrap_life_database(self.settings)
        self.engine = engine_for_url(self.settings.life_database_url)
        self.repository = LifeRepository(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temp_dir.cleanup()

    @staticmethod
    def _write_structured_output(request: CodexCliRequest) -> None:
        output_path = Path(request.output_path or "")
        output_path.write_text(json.dumps(_annotation_payload(), indent=2), encoding="utf-8")

    def _store_owned_source(
        self,
        *,
        canonical_uri: str,
        source_type: str,
        normalized_text: str,
        raw_text: str,
    ) -> None:
        output_dir = self.root / "owned-source" / source_type
        output_dir.mkdir(parents=True, exist_ok=True)
        raw_suffix = ".html" if source_type == "html" else ".txt"
        raw_path = output_dir / f"raw_source{raw_suffix}"
        normalized_path = output_dir / "normalized.txt"
        raw_path.write_text(raw_text, encoding="utf-8")
        normalized_path.write_text(normalized_text, encoding="utf-8")

        run = self.repository.start_run(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload={"items": [{"input_kind": "url", "url": canonical_uri}]},
            status="running",
        )
        persisted = self.repository.persist_adapter_result(
            run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="source-raw",
                        path=str(raw_path),
                        storage_uri=raw_path.as_uri(),
                        size_bytes=raw_path.stat().st_size,
                        media_type="text/html" if source_type == "html" else "text/plain",
                        metadata={
                            "role": "raw_source",
                            "canonical_uri": canonical_uri,
                            "source_type": source_type,
                        },
                    ),
                    ArtifactRecord(
                        kind="source-text",
                        path=str(normalized_path),
                        storage_uri=normalized_path.as_uri(),
                        size_bytes=normalized_path.stat().st_size,
                        media_type="text/plain",
                        metadata={
                            "role": "normalized_text",
                            "canonical_uri": canonical_uri,
                            "source_type": source_type,
                        },
                    ),
                ],
            ),
        )
        assert persisted is not None
        raw_artifact, normalized_artifact = persisted.artifacts
        source_document = self.repository.upsert_source_document(
            canonical_uri=canonical_uri,
            source_type=source_type,
            title="Owned source",
            current_text_artifact_id=normalized_artifact.id,
            metadata_json={"source_type": source_type},
        )
        self.repository.assign_artifact_to_source(raw_artifact.id, source_document.id)
        self.repository.assign_artifact_to_source(normalized_artifact.id, source_document.id)

    def test_review_generates_annotation_and_html_artifacts(self) -> None:
        runner = ArxivReviewRunner(self.settings)
        request = PaperReviewRequest(
            paper_id="2403.01234",
            source_url="https://arxiv.org/abs/2403.01234",
            metadata={"raw_text": "Abstract\n\nWe analyze induction heads in transformer circuits."},
        )

        def fake_codex_run(codex_request: CodexCliRequest) -> AdapterResult:
            self._write_structured_output(codex_request)
            return AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok")

        with patch("workers.ingest_runner.runner.CodexCliRunner.run", side_effect=fake_codex_run):
            result = runner.review(request)

        kinds = {item.kind for item in result.artifact_manifest}
        self.assertEqual(result.structured_outputs["presentation_mode"], "annotated_paper")
        self.assertIn("review-card", kinds)
        self.assertIn("paper-annotations", kinds)
        self.assertIn("annotated-paper", kinds)
        self.assertEqual(result.reports[0].metadata["source_kind"], "arxiv_text")
        self.assertIsNone(result.reports[0].metadata["fallback_reason"])

        html_path = Path(result.structured_outputs["annotated_html_artifact_path"])
        self.assertTrue(html_path.exists())
        html_text = html_path.read_text(encoding="utf-8")
        self.assertIn("Full Paper Analysis", html_text)
        self.assertIn("Executive Summary", html_text)
        self.assertIn("Pinned Annotation", html_text)
        self.assertNotIn("CS50 lens", html_text)

    def test_review_renders_html_when_annotation_text_contains_backslashes(self) -> None:
        runner = ArxivReviewRunner(self.settings)
        request = PaperReviewRequest(
            paper_id="2403.01234",
            source_url="https://arxiv.org/abs/2403.01234",
            metadata={"raw_text": "Abstract\n\nWe analyze induction heads in transformer circuits."},
        )

        def fake_codex_run(codex_request: CodexCliRequest) -> AdapterResult:
            payload = copy.deepcopy(_annotation_payload())
            payload["annotations"][0]["analysis"] = (
                "Paper claim: the passage references a mechanism. "
                "Inference: the source uses \\parencite-style markup that should not break HTML rendering."
            )
            output_path = Path(codex_request.output_path or "")
            output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            return AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok")

        with patch("workers.ingest_runner.runner.CodexCliRunner.run", side_effect=fake_codex_run):
            result = runner.review(request)

        self.assertEqual(result.structured_outputs["presentation_mode"], "annotated_paper")
        html_path = Path(result.structured_outputs["annotated_html_artifact_path"])
        self.assertTrue(html_path.exists())
        html_text = html_path.read_text(encoding="utf-8")
        self.assertIn("\\\\parencite", html_text)
        self.assertIn("Full Paper Analysis", html_text)

    def test_review_reuses_owned_html_source_when_request_has_only_url(self) -> None:
        canonical_uri = "https://arxiv.org/html/2403.01234v1"
        self._store_owned_source(
            canonical_uri=canonical_uri,
            source_type="html",
            normalized_text="Abstract\n\nWe analyze induction heads in transformer circuits.",
            raw_text="<html><body><h1>Abstract</h1><p>We analyze induction heads in transformer circuits.</p></body></html>",
        )
        runner = ArxivReviewRunner(self.settings)
        request = PaperReviewRequest(
            paper_id="2403.01234v1",
            source_url=canonical_uri,
        )

        def fake_codex_run(codex_request: CodexCliRequest) -> AdapterResult:
            self._write_structured_output(codex_request)
            return AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok")

        with patch("workers.ingest_runner.runner.CodexCliRunner.run", side_effect=fake_codex_run):
            result = runner.review(request)

        self.assertEqual(result.structured_outputs["presentation_mode"], "annotated_paper")
        self.assertEqual(result.structured_outputs["source_origin"], "owned_source_raw_html")
        self.assertTrue(result.structured_outputs["source_document_id"])

    def test_review_uses_owned_extracted_text_for_pdf_url(self) -> None:
        canonical_uri = "https://arxiv.org/pdf/2403.01234.pdf"
        self._store_owned_source(
            canonical_uri=canonical_uri,
            source_type="pdf",
            normalized_text="Abstract\n\nWe analyze induction heads in transformer circuits.",
            raw_text="%PDF-1.4 placeholder",
        )
        runner = ArxivReviewRunner(self.settings)
        request = PaperReviewRequest(
            paper_id="2403.01234",
            source_url=canonical_uri,
        )

        def fake_codex_run(codex_request: CodexCliRequest) -> AdapterResult:
            self._write_structured_output(codex_request)
            return AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok")

        with patch("workers.ingest_runner.runner.CodexCliRunner.run", side_effect=fake_codex_run):
            result = runner.review(request)

        self.assertEqual(result.structured_outputs["presentation_mode"], "annotated_paper")
        self.assertEqual(result.structured_outputs["source_origin"], "owned_source_normalized_text")
        self.assertEqual(result.structured_outputs["source_kind"], "arxiv_text")

    def test_review_jsonifies_fenced_output_before_rendering(self) -> None:
        runner = ArxivReviewRunner(self.settings)
        request = PaperReviewRequest(
            paper_id="2403.01234",
            source_url="https://arxiv.org/abs/2403.01234",
            metadata={"raw_text": "Abstract\n\nWe analyze induction heads in transformer circuits."},
        )

        def fake_codex_run(codex_request: CodexCliRequest) -> AdapterResult:
            output_path = Path(codex_request.output_path or "")
            wrapped = "```json\n" + json.dumps(_annotation_payload(), indent=2) + "\n```"
            output_path.write_text(wrapped, encoding="utf-8")
            return AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok")

        with patch("workers.ingest_runner.runner.CodexCliRunner.run", side_effect=fake_codex_run) as run_mock:
            result = runner.review(request)

        self.assertEqual(run_mock.call_count, 1)
        self.assertEqual(result.structured_outputs["presentation_mode"], "annotated_paper")
        normalized = Path(result.structured_outputs["annotation_json_artifact_path"]).read_text(encoding="utf-8")
        self.assertFalse(normalized.lstrip().startswith("```"))
        self.assertEqual(json.loads(normalized)["paper"]["paper_id"], "2403.01234")

    def test_review_repairs_invalid_json_before_rendering(self) -> None:
        runner = ArxivReviewRunner(self.settings)
        request = PaperReviewRequest(
            paper_id="2403.01234",
            source_url="https://arxiv.org/abs/2403.01234",
            metadata={"raw_text": "Abstract\n\nWe analyze induction heads in transformer circuits."},
        )

        def fake_codex_run(codex_request: CodexCliRequest) -> AdapterResult:
            output_path = Path(codex_request.output_path or "")
            if fake_codex_run.calls == 0:
                output_path.write_text('{"paper": {"paper_id": "2403.01234",}', encoding="utf-8")
            else:
                output_path.write_text(json.dumps(_annotation_payload(), indent=2), encoding="utf-8")
            fake_codex_run.calls += 1
            return AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok")

        fake_codex_run.calls = 0
        with patch("workers.ingest_runner.runner.CodexCliRunner.run", side_effect=fake_codex_run) as run_mock:
            result = runner.review(request)

        self.assertEqual(run_mock.call_count, 2)
        self.assertEqual(result.structured_outputs["presentation_mode"], "annotated_paper")
        repaired = json.loads(Path(result.structured_outputs["annotation_json_artifact_path"]).read_text(encoding="utf-8"))
        self.assertEqual(repaired["paper"]["paper_id"], "2403.01234")

    def test_review_falls_back_for_pdf_sources(self) -> None:
        runner = ArxivReviewRunner(self.settings)
        request = PaperReviewRequest(
            paper_id="2403.01234",
            source_url="https://arxiv.org/pdf/2403.01234.pdf",
        )
        with patch("workers.ingest_runner.runner.CodexCliRunner.run") as mocked_run:
            result = runner.review(request)
        mocked_run.assert_not_called()

        kinds = {item.kind for item in result.artifact_manifest}
        self.assertEqual(kinds, {"review-card"})
        self.assertEqual(result.structured_outputs["presentation_mode"], "raw_review")
        self.assertEqual(result.structured_outputs["source_kind"], "pdf")
        self.assertEqual(result.structured_outputs["fallback_reason"], "pdf_only_source")

    def test_review_falls_back_when_structured_output_is_missing(self) -> None:
        runner = ArxivReviewRunner(self.settings)
        request = PaperReviewRequest(
            paper_id="2403.01234",
            source_url="https://arxiv.org/abs/2403.01234",
            metadata={"raw_text": "Abstract\n\nWe analyze induction heads in transformer circuits."},
        )

        with patch(
            "workers.ingest_runner.runner.CodexCliRunner.run",
            return_value=AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok"),
        ):
            result = runner.review(request)

        self.assertEqual(result.structured_outputs["presentation_mode"], "raw_review")
        self.assertIn("structured_analysis_failed", result.structured_outputs["fallback_reason"])
        self.assertEqual({item.kind for item in result.artifact_manifest}, {"review-card"})


class PaperReviewFlowTests(unittest.TestCase):
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

    @staticmethod
    def _write_structured_output(request: CodexCliRequest) -> None:
        output_path = Path(request.output_path or "")
        output_path.write_text(json.dumps(_annotation_payload(), indent=2), encoding="utf-8")

    def test_flow_chunks_review_card_when_annotation_artifacts_exist(self) -> None:
        def fake_codex_run(codex_request: CodexCliRequest) -> AdapterResult:
            self._write_structured_output(codex_request)
            return AdapterResult(status=WorkerStatus.COMPLETED, stdout="ok")

        payload = {
            "paper_id": "2403.01234",
            "source_url": "https://arxiv.org/abs/2403.01234",
            "metadata": {"raw_text": "Abstract\n\nWe analyze induction heads in transformer circuits."},
        }
        with patch("workers.ingest_runner.runner.CodexCliRunner.run", side_effect=fake_codex_run):
            with patch("flows.paper_review.execute_adapter", side_effect=self._execute_adapter_inline):
                result = paper_review_flow.fn(request=payload)

        self.assertGreater(result.get("chunk_count", 0), 0)
        self.assertEqual(result["structured_outputs"]["presentation_mode"], "annotated_paper")
        self.assertTrue(result["structured_outputs"]["annotation_json_artifact_id"])
        self.assertTrue(result["structured_outputs"]["annotated_html_artifact_id"])
        report = self.repository.list_reports(limit=1)[0]
        self.assertEqual(report.metadata_json["presentation_mode"], "annotated_paper")
        self.assertEqual(
            report.metadata_json["annotation_json_artifact_id"],
            result["structured_outputs"]["annotation_json_artifact_id"],
        )
        self.assertEqual(
            report.metadata_json["annotated_html_artifact_id"],
            result["structured_outputs"]["annotated_html_artifact_id"],
        )
