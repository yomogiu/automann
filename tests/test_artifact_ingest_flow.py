from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from flows.artifact_ingest import artifact_ingest_flow
from libs.config import get_settings
from libs.contracts.models import AdapterResult, ArtifactRecord, WorkerStatus
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url


class ArtifactIngestFlowTests(unittest.TestCase):
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

    def _build_ingest_result(
        self,
        *,
        prefix: str,
        input_kind: str,
        canonical_uri: str,
        text: str,
        source_type: str = "markdown",
        title: str | None = None,
        warning_codes: list[str] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> AdapterResult:
        output_dir = self.root / "worker-results" / prefix
        output_dir.mkdir(parents=True, exist_ok=True)

        raw_path = output_dir / "raw_source.txt"
        normalized_path = output_dir / "normalized.txt"
        manifest_path = output_dir / "ingest_manifest.json"
        raw_path.write_text(text, encoding="utf-8")
        normalized_path.write_text(text, encoding="utf-8")
        manifest_path.write_text(json.dumps({"canonical_uri": canonical_uri, "prefix": prefix}), encoding="utf-8")

        item = {
            "input_index": 0,
            "input_kind": input_kind,
            "status": "completed",
            "canonical_uri": canonical_uri,
            "source_type": source_type,
            "raw_artifact_path": str(raw_path),
            "normalized_text_artifact_path": str(normalized_path),
            "ingest_manifest_artifact_path": str(manifest_path),
            "chunk_count": 1,
            "warning_codes": warning_codes or [],
            "title": title or "Artifact ingest item",
            "tags": [source_type],
            "metadata": {
                "source_type": source_type,
                "tags": [source_type],
                **dict(metadata or {}),
            },
            "chunks": [
                {
                    "ordinal": 0,
                    "text": text,
                    "token_count": max(1, len(text.split())),
                    "metadata": {"canonical_uri": canonical_uri, "source_type": source_type},
                }
            ],
        }

        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"ingested {prefix}",
            artifact_manifest=[
                ArtifactRecord(
                    kind="source-raw",
                    path=str(raw_path),
                    storage_uri=raw_path.as_uri(),
                    size_bytes=raw_path.stat().st_size,
                    media_type="text/plain",
                    metadata={
                        "role": "raw_source",
                        "input_index": 0,
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
                        "input_index": 0,
                        "canonical_uri": canonical_uri,
                        "source_type": source_type,
                    },
                ),
                ArtifactRecord(
                    kind="source-manifest",
                    path=str(manifest_path),
                    storage_uri=manifest_path.as_uri(),
                    size_bytes=manifest_path.stat().st_size,
                    media_type="application/json",
                    metadata={
                        "role": "ingest_manifest",
                        "input_index": 0,
                        "canonical_uri": canonical_uri,
                        "source_type": source_type,
                    },
                ),
            ],
            structured_outputs={
                "generated_at": "2026-03-17T00:00:00+00:00",
                "input_count": 1,
                "success_count": 1,
                "failure_count": 0,
                "warning_count": len(warning_codes or []),
                "items": [item],
            },
        )

    def test_file_ingest_flow_links_source_document_and_chunks(self) -> None:
        input_file = self.root / "inputs" / "note.md"
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_file.write_text("# Note\n\nfile based ingest payload", encoding="utf-8")
        canonical_uri = "file:///knowledge/file-note.md"
        text = "file based ingest payload"

        with patch(
            "flows.artifact_ingest._run_artifact_ingest_worker",
            return_value=self._build_ingest_result(
                prefix="file-run",
                input_kind="file",
                canonical_uri=canonical_uri,
                text=text,
                source_type="markdown",
                title="File note",
            ),
        ):
            result = artifact_ingest_flow.fn(
                request={
                    "items": [
                        {
                            "input_kind": "file",
                            "file_path": str(input_file),
                            "title": "File note",
                            "tags": ["markdown"],
                        }
                    ]
                }
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["success_count"], 1)
        self.assertEqual(result["failure_count"], 0)
        self.assertEqual(result["items"][0]["status"], "completed")
        self.assertEqual(result["items"][0]["chunk_count"], 1)
        self.assertIsNotNone(result["items"][0]["source_document_id"])
        self.assertIsNotNone(result["items"][0]["normalized_text_artifact_id"])

        source_document = self.repository.get_source_document_by_canonical_uri(canonical_uri)
        self.assertIsNotNone(source_document)
        assert source_document is not None
        self.assertEqual(source_document.title, "File note")
        self.assertEqual(source_document.current_text_artifact_id, result["items"][0]["normalized_text_artifact_id"])
        self.assertEqual(source_document.metadata_json["source_profile"]["source_kind"], "institutional_report")
        self.assertEqual(source_document.metadata_json["source_profile"]["research_domain"], "labor")

        hits = self.repository.query_chunks(query="file based ingest", limit=5)
        self.assertEqual([item.text for item in hits], [text])

    def test_inline_ingest_flow_accepts_markdown_content(self) -> None:
        canonical_uri = "inline://sha256/inline-note"
        text = "inline content that should become chunks"

        with patch(
            "flows.artifact_ingest._run_artifact_ingest_worker",
            return_value=self._build_ingest_result(
                prefix="inline-run",
                input_kind="inline",
                canonical_uri=canonical_uri,
                text=text,
                source_type="markdown",
                title="Inline note",
            ),
        ):
            result = artifact_ingest_flow.fn(
                request={
                    "items": [
                        {
                            "input_kind": "inline",
                            "content": "# Inline note\n\ninline content that should become chunks",
                            "content_format": "markdown",
                            "title": "Inline note",
                            "tags": ["kb"],
                        }
                    ]
                }
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["items"][0]["canonical_uri"], canonical_uri)
        self.assertEqual(result["items"][0]["tags"], ["markdown"])

        source_document = self.repository.get_source_document_by_canonical_uri(canonical_uri)
        self.assertIsNotNone(source_document)
        assert source_document is not None
        self.assertEqual(source_document.title, "Inline note")

        hits = self.repository.query_chunks(query="inline content", limit=5)
        self.assertEqual([item.text for item in hits], [text])

    def test_duplicate_reingest_updates_current_source_and_filters_stale_chunks(self) -> None:
        canonical_uri = "file:///knowledge/duplicate-note.md"
        first_text = "alpha beta stale version"
        second_text = "alpha beta current version"
        first_input = self.root / "inputs" / "duplicate-v1.md"
        second_input = self.root / "inputs" / "duplicate-v2.md"
        first_input.parent.mkdir(parents=True, exist_ok=True)
        first_input.write_text(first_text, encoding="utf-8")
        second_input.write_text(second_text, encoding="utf-8")

        with patch(
            "flows.artifact_ingest._run_artifact_ingest_worker",
            side_effect=[
                self._build_ingest_result(
                    prefix="duplicate-run-1",
                    input_kind="file",
                    canonical_uri=canonical_uri,
                    text=first_text,
                    source_type="markdown",
                    title="Duplicate note",
                    metadata={
                        "source_profile": {
                            "source_kind": "expert_blog",
                            "publisher_type": "individual",
                            "document_scope": "blog_post",
                            "research_domain": "labor",
                            "authority_score": 0.65,
                            "freshness_bucket": "month",
                            "topics": ["labor"],
                            "entities": ["Duplicate Note"],
                            "signal_flags": ["file_source", "first_ingest"],
                        }
                    },
                ),
                self._build_ingest_result(
                    prefix="duplicate-run-2",
                    input_kind="file",
                    canonical_uri=canonical_uri,
                    text=second_text,
                    source_type="markdown",
                    title="Duplicate note",
                    metadata={
                        "source_profile": {
                            "source_kind": "institutional_report",
                            "publisher_type": "institutional",
                            "document_scope": "report",
                            "research_domain": "labor",
                            "authority_score": 0.8,
                            "freshness_bucket": "month",
                            "topics": ["labor"],
                            "entities": ["Duplicate Note"],
                            "signal_flags": ["file_source", "second_ingest"],
                        }
                    },
                ),
            ],
        ):
            first_result = artifact_ingest_flow.fn(
                request={
                    "items": [
                        {
                            "input_kind": "file",
                            "file_path": str(first_input),
                            "title": "Duplicate note",
                        }
                    ]
                }
            )
            second_result = artifact_ingest_flow.fn(
                request={
                    "items": [
                        {
                            "input_kind": "file",
                            "file_path": str(second_input),
                            "title": "Duplicate note",
                        }
                    ]
                }
            )

        self.assertEqual(first_result["status"], "completed")
        self.assertEqual(second_result["status"], "completed")
        self.assertEqual(first_result["items"][0]["source_document_id"], second_result["items"][0]["source_document_id"])
        self.assertNotEqual(
            first_result["items"][0]["normalized_text_artifact_id"],
            second_result["items"][0]["normalized_text_artifact_id"],
        )

        source_document = self.repository.get_source_document_by_canonical_uri(canonical_uri)
        self.assertIsNotNone(source_document)
        assert source_document is not None
        self.assertEqual(source_document.current_text_artifact_id, second_result["items"][0]["normalized_text_artifact_id"])
        self.assertEqual(source_document.metadata_json["source_profile"]["source_kind"], "institutional_report")
        self.assertIn("second_ingest", source_document.metadata_json["source_profile"]["signal_flags"])

        hits = self.repository.query_chunks(query="alpha beta", limit=10)
        self.assertEqual([item.artifact_id for item in hits], [second_result["items"][0]["normalized_text_artifact_id"]])
        self.assertEqual([item.text for item in hits], [second_text])

    def test_batch_partial_success_marks_parent_completed(self) -> None:
        success = self._build_ingest_result(
            prefix="batch-success",
            input_kind="inline",
            canonical_uri="inline://sha256/batch-success",
            text="successful chunk content",
            source_type="markdown",
            title="Batch success",
        )
        success_payload = dict(success.structured_outputs)
        success_item = dict(success_payload["items"][0])

        failed_item = {
            "input_index": 1,
            "input_kind": "inline",
            "status": "unsupported",
            "canonical_uri": "inline://sha256/batch-failure",
            "chunk_count": 0,
            "warning_codes": [],
            "error": "unsupported_pdf_no_extractable_text",
            "tags": [],
            "metadata": {},
        }

        with patch(
            "flows.artifact_ingest._run_artifact_ingest_worker",
            return_value=AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="batch ingest",
                artifact_manifest=success.artifact_manifest,
                structured_outputs={
                    **success_payload,
                    "input_count": 2,
                    "success_count": 1,
                    "failure_count": 1,
                    "items": [success_item, failed_item],
                },
            ),
        ):
            result = artifact_ingest_flow.fn(
                request={
                    "items": [
                        {
                            "input_kind": "inline",
                            "content": "# Success\n\nsuccessful chunk content",
                            "content_format": "markdown",
                            "title": "Batch success",
                        },
                        {
                            "input_kind": "inline",
                            "content": "placeholder binary pdf payload",
                            "content_format": "markdown",
                            "title": "Batch failure",
                        },
                    ]
                }
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["success_count"], 1)
        self.assertEqual(result["failure_count"], 1)
        self.assertEqual(result["items"][0]["status"], "completed")
        self.assertEqual(result["items"][1]["status"], "unsupported")
