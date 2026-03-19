from __future__ import annotations

import time
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from sqlalchemy import text

from libs.config import get_settings
from libs.contracts.models import AdapterResult, ArtifactRecord, ObservationRecord, ReportRecord, WorkerStatus
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url


class SQLiteRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.runtime_root = Path(self.temp_dir.name)
        self.database_url = f"sqlite+pysqlite:///{self.runtime_root / 'life.db'}"
        self.settings = get_settings().model_copy(update={"life_database_url": self.database_url})
        bootstrap_life_database(self.settings)
        self.engine = engine_for_url(self.database_url)
        self.repository = LifeRepository(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temp_dir.cleanup()

    def test_bootstrap_creates_fts_objects_and_enables_foreign_keys(self) -> None:
        with self.engine.connect() as connection:
            foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
            objects = set(
                connection.execute(
                    text(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE name IN (
                          'chunk_fts',
                          'chunk_fts_ai',
                          'chunk_fts_ad',
                          'chunk_fts_au',
                          'report_taxonomy_term',
                          'report_taxonomy_link'
                        )
                        """
                    )
                ).scalars()
            )

        self.assertEqual(foreign_keys, 1)
        self.assertEqual(
            objects,
            {
                "chunk_fts",
                "chunk_fts_ai",
                "chunk_fts_ad",
                "chunk_fts_au",
                "report_taxonomy_term",
                "report_taxonomy_link",
            },
        )

    def test_repository_roundtrip_persists_records_and_queries_fts(self) -> None:
        run = self.repository.start_run(
            flow_name="paper_review_flow",
            worker_key="worker",
            input_payload={"paper_id": "paper-1"},
            status="running",
        )

        result = AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout="ok",
            artifact_manifest=[
                ArtifactRecord(
                    kind="review-card",
                    path=str(self.runtime_root / "review.md"),
                    storage_uri=(self.runtime_root / "review.md").as_uri(),
                    size_bytes=32,
                    media_type="text/markdown",
                    metadata={"paper_id": "paper-1"},
                )
            ],
            structured_outputs={"ok": True},
            observations=[
                ObservationRecord(
                    kind="summary",
                    summary="alpha beta gamma",
                    payload={"paper_id": "paper-1"},
                    confidence=0.8,
                )
            ],
            reports=[
                ReportRecord(
                    report_type="paper_review",
                    title="Paper 1",
                    summary="alpha beta gamma",
                    content_markdown="# Paper 1",
                    artifact_path=str(self.runtime_root / "review.md"),
                    metadata={"paper_id": "paper-1"},
                )
            ],
        )

        persisted = self.repository.persist_adapter_result(run.id, result)
        self.assertIsNotNone(persisted)
        assert persisted is not None
        artifact = persisted.artifacts[0]

        self.assertEqual(self.repository.list_runs(limit=5)[0].status, "completed")
        self.assertEqual(len(self.repository.list_artifacts(limit=5)), 1)
        self.assertEqual(len(self.repository.list_reports(limit=5)), 1)
        taxonomy = self.repository.report_taxonomy_map([persisted.reports[0].id])[persisted.reports[0].id]
        self.assertEqual(taxonomy.filter_keys, ["report"])
        self.assertEqual(taxonomy.tag_keys, ["arxiv_paper_analysis"])

        chunk_count = self.repository.upsert_chunks(
            artifact_id=artifact.id,
            chunks=[
                {
                    "ordinal": 0,
                    "text": "Alpha beta gamma",
                    "token_count": 3,
                    "metadata": {"page": 1},
                    "embedding": [0.1, 0.2, 0.3],
                },
                {
                    "ordinal": 1,
                    "text": "Delta epsilon zeta",
                    "token_count": 3,
                    "metadata": {"page": 2},
                },
            ],
        )
        self.assertEqual(chunk_count, 2)

        hits = self.repository.query_chunks(query="alpha beta", limit=5)
        self.assertEqual([item.text for item in hits], ["Alpha beta gamma"])
        self.assertEqual(self.repository.query_chunks(query="!!!", limit=5), [])

        interaction = self.repository.create_interaction(
            run_id=run.id,
            title="Need approval",
            prompt_md="Proceed?",
            input_schema={"type": "object"},
            default_input={"approved": False},
        )
        self.assertEqual(interaction.status, "open")
        self.assertEqual(len(self.repository.list_interactions(limit=5, status="open")), 1)

        answered = self.repository.answer_interaction(interaction.id, response={"approved": True})
        self.assertIsNotNone(answered)
        assert answered is not None
        self.assertEqual(answered.status, "answered")

    def test_source_document_upsert_keeps_only_current_text_artifact_visible_in_retrieval(self) -> None:
        canonical_uri = "file:///knowledge/source-note.md"
        source_v1_path = self.runtime_root / "source-v1.md"
        source_v2_path = self.runtime_root / "source-v2.md"
        source_v1_path.write_text("alpha beta stale version\n", encoding="utf-8")
        source_v2_path.write_text("alpha beta current version\n", encoding="utf-8")

        first_run = self.repository.start_run(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload={"items": [{"input_kind": "file", "file_path": str(source_v1_path)}]},
            status="running",
        )
        first_persisted = self.repository.persist_adapter_result(
            first_run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="source-text",
                        path=str(source_v1_path),
                        storage_uri=source_v1_path.as_uri(),
                        size_bytes=source_v1_path.stat().st_size,
                        media_type="text/markdown",
                        metadata={
                            "role": "normalized_text",
                            "input_index": 0,
                            "canonical_uri": canonical_uri,
                            "source_type": "markdown",
                        },
                    )
                ],
                structured_outputs={
                    "items": [
                        {
                            "input_index": 0,
                            "input_kind": "file",
                            "status": "completed",
                            "canonical_uri": canonical_uri,
                            "source_type": "markdown",
                            "normalized_text_artifact_path": str(source_v1_path),
                            "chunk_count": 1,
                            "warning_codes": [],
                            "title": "Source note",
                            "tags": ["markdown"],
                            "metadata": {"source_type": "markdown", "tags": ["markdown"]},
                            "chunks": [
                                {
                                    "ordinal": 0,
                                    "text": "alpha beta stale version",
                                    "token_count": 4,
                                    "metadata": {"canonical_uri": canonical_uri, "source_type": "markdown"},
                                }
                            ],
                        }
                    ]
                },
            ),
        )
        self.assertIsNotNone(first_persisted)
        assert first_persisted is not None
        first_artifact = first_persisted.artifacts[0]

        source = self.repository.upsert_source_document(
            canonical_uri=canonical_uri,
            source_type="markdown",
            title="Source note v1",
            author="Analyst",
            current_text_artifact_id=first_artifact.id,
            metadata_json={"tags": ["markdown"]},
        )
        self.assertEqual(source.current_text_artifact_id, first_artifact.id)
        self.assertEqual(self.repository.assign_artifact_to_source(first_artifact.id, source.id).id, first_artifact.id)
        self.assertEqual(
            self.repository.upsert_chunks(
                artifact_id=first_artifact.id,
                chunks=[
                    {
                        "ordinal": 0,
                        "text": "alpha beta stale version",
                        "token_count": 4,
                        "metadata": {"canonical_uri": canonical_uri, "source_type": "markdown"},
                    }
                ],
            ),
            1,
        )

        time.sleep(1.1)
        second_run = self.repository.start_run(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload={"items": [{"input_kind": "file", "file_path": str(source_v2_path)}]},
            status="running",
        )
        second_persisted = self.repository.persist_adapter_result(
            second_run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="source-text",
                        path=str(source_v2_path),
                        storage_uri=source_v2_path.as_uri(),
                        size_bytes=source_v2_path.stat().st_size,
                        media_type="text/markdown",
                        metadata={
                            "role": "normalized_text",
                            "input_index": 0,
                            "canonical_uri": canonical_uri,
                            "source_type": "markdown",
                        },
                    )
                ],
                structured_outputs={
                    "items": [
                        {
                            "input_index": 0,
                            "input_kind": "file",
                            "status": "completed",
                            "canonical_uri": canonical_uri,
                            "source_type": "markdown",
                            "normalized_text_artifact_path": str(source_v2_path),
                            "chunk_count": 1,
                            "warning_codes": [],
                            "title": "Source note",
                            "tags": ["markdown"],
                            "metadata": {"source_type": "markdown", "tags": ["markdown"]},
                            "chunks": [
                                {
                                    "ordinal": 0,
                                    "text": "alpha beta current version",
                                    "token_count": 4,
                                    "metadata": {"canonical_uri": canonical_uri, "source_type": "markdown"},
                                }
                            ],
                        }
                    ]
                },
            ),
        )
        self.assertIsNotNone(second_persisted)
        assert second_persisted is not None
        second_artifact = second_persisted.artifacts[0]

        updated = self.repository.upsert_source_document(
            canonical_uri=canonical_uri,
            source_type="markdown",
            title="Source note v2",
            author="Analyst",
            current_text_artifact_id=second_artifact.id,
            metadata_json={"tags": ["markdown", "current"]},
        )
        self.assertEqual(updated.id, source.id)
        self.assertEqual(updated.current_text_artifact_id, second_artifact.id)
        self.assertEqual(self.repository.assign_artifact_to_source(second_artifact.id, source.id).id, second_artifact.id)
        self.assertEqual(
            self.repository.upsert_chunks(
                artifact_id=second_artifact.id,
                chunks=[
                    {
                        "ordinal": 0,
                        "text": "alpha beta current version",
                        "token_count": 4,
                        "metadata": {"canonical_uri": canonical_uri, "source_type": "markdown"},
                    }
                ],
            ),
            1,
        )

        source_document = self.repository.get_source_document_by_canonical_uri(canonical_uri)
        self.assertIsNotNone(source_document)
        assert source_document is not None
        self.assertEqual(source_document.current_text_artifact_id, second_artifact.id)

        hits = self.repository.query_chunks(query="alpha beta", limit=10)
        self.assertEqual([item.artifact_id for item in hits], [second_artifact.id])
        self.assertEqual([item.text for item in hits], ["alpha beta current version"])

        fetched = self.repository.get_source_document(source.id)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.current_text_artifact_id, second_artifact.id)

        by_id = self.repository.get_source_document(source.id)
        self.assertIsNotNone(by_id)
        assert by_id is not None
        self.assertEqual(by_id.canonical_uri, canonical_uri)

        artifacts = self.repository.list_artifacts_for_source_document(source.id)
        self.assertEqual([item.id for item in artifacts], [second_artifact.id, first_artifact.id])
        self.assertEqual(artifacts[0].source_document_id, source.id)

        chunks = self.repository.list_chunks_for_artifact(second_artifact.id)
        self.assertEqual([item.text for item in chunks], ["alpha beta current version"])

    def test_source_document_listing_orders_by_update_and_created_at(self) -> None:
        source_a = self.repository.upsert_source_document(
            canonical_uri="file:///knowledge/a.md",
            source_type="markdown",
            title="A",
            metadata_json={"tags": ["a"]},
        )
        time.sleep(1.1)
        source_b = self.repository.upsert_source_document(
            canonical_uri="file:///knowledge/b.md",
            source_type="markdown",
            title="B",
            metadata_json={"tags": ["b"]},
        )

        listed = self.repository.list_source_documents(limit=10)
        self.assertEqual([item.id for item in listed[:2]], [source_b.id, source_a.id])

    def test_delete_source_document_detaches_artifacts(self) -> None:
        canonical_uri = "file:///knowledge/deletable.md"
        source_path = self.runtime_root / "deletable.md"
        source_path.write_text("delete me", encoding="utf-8")

        ingest_run = self.repository.start_run(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload={"items": [{"input_kind": "file", "file_path": str(source_path)}]},
            status="running",
        )
        persisted = self.repository.persist_adapter_result(
            ingest_run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="source-text",
                        path=str(source_path),
                        storage_uri=source_path.as_uri(),
                        size_bytes=source_path.stat().st_size,
                        media_type="text/markdown",
                        metadata={
                            "role": "normalized_text",
                            "input_index": 0,
                            "canonical_uri": canonical_uri,
                            "source_type": "markdown",
                        },
                    )
                ],
                structured_outputs={},
                observations=[],
                reports=[],
            ),
        )
        self.assertIsNotNone(persisted)
        assert persisted is not None
        artifact = persisted.artifacts[0]

        source = self.repository.upsert_source_document(
            canonical_uri=canonical_uri,
            source_type="markdown",
            title="Deletable",
            current_text_artifact_id=artifact.id,
            metadata_json={"tags": ["delete"]},
        )
        self.repository.assign_artifact_to_source(artifact.id, source.id)

        self.assertTrue(self.repository.delete_source_document(source.id))
        self.assertIsNone(self.repository.get_source_document(source.id))
        self.assertIsNone(self.repository.get_artifact(artifact.id).source_document_id)

    def test_report_revision_helpers_promote_and_list_current_series(self) -> None:
        first_run = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="worker",
            input_payload={"theme": "Test"},
            status="running",
        )
        first_result = AdapterResult(
            status=WorkerStatus.COMPLETED,
            reports=[
                ReportRecord(
                    report_type="research_report",
                    title="Research Report",
                    summary="v1",
                    content_markdown="# V1",
                    report_series_id="research-key",
                    revision_number=1,
                    is_current=False,
                    metadata={"report_key": "research-key"},
                )
            ],
        )
        first_persisted = self.repository.persist_adapter_result(first_run.id, first_result)
        self.assertIsNotNone(first_persisted)
        assert first_persisted is not None
        first_report = first_persisted.reports[0]

        promoted_first = self.repository.promote_report_revision(first_report.id)
        self.assertIsNotNone(promoted_first)
        assert promoted_first is not None
        self.assertTrue(promoted_first.is_current)

        second_run = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="worker",
            input_payload={"theme": "Test"},
            status="running",
        )
        second_result = AdapterResult(
            status=WorkerStatus.COMPLETED,
            reports=[
                ReportRecord(
                    report_type="research_report",
                    title="Research Report",
                    summary="v2",
                    content_markdown="# V2",
                    report_series_id="research-key",
                    revision_number=2,
                    supersedes_report_id=first_report.id,
                    is_current=False,
                    metadata={"report_key": "research-key"},
                )
            ],
        )
        second_persisted = self.repository.persist_adapter_result(second_run.id, second_result)
        self.assertIsNotNone(second_persisted)
        assert second_persisted is not None
        second_report = second_persisted.reports[0]

        self.repository.promote_report_revision(second_report.id)

        current = self.repository.current_report_revision("research-key")
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.id, second_report.id)

        revisions = self.repository.list_report_revisions(second_report.id)
        self.assertEqual([item.revision_number for item in revisions], [2, 1])
        self.assertEqual(revisions[0].supersedes_report_id, first_report.id)

        listed = self.repository.list_reports(limit=10)
        self.assertEqual([item.id for item in listed], [second_report.id])

    def test_task_spec_roundtrip_and_run_listing(self) -> None:
        task_spec = self.repository.create_task_spec(
            task_key="daily-brief-main",
            task_type="daily_brief",
            flow_name="daily_brief_flow",
            title="Daily Brief",
            description="Morning synthesis",
            schedule_text="0 7 * * *",
            timezone="America/Toronto",
            work_pool="mini-process",
            prompt_path=str(self.runtime_root / "prompts" / "brief.md"),
            status="active",
            prefect_deployment_id="dep-1",
            prefect_deployment_name="automation-daily-brief-main",
            prefect_deployment_path="daily-brief/automation-daily-brief-main",
            prefect_deployment_url="http://127.0.0.1:4200/deployments/deployment/dep-1",
            payload={"publish": True},
        )

        fetched = self.repository.get_task_spec(task_spec.id)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.flow_name, "daily_brief_flow")
        self.assertEqual(fetched.timezone, "America/Toronto")

        updated = self.repository.update_task_spec(
            task_spec.id,
            status="paused",
            schedule_text="30 7 * * *",
            payload={"publish": False},
        )
        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertEqual(updated.status, "paused")
        self.assertEqual(updated.schedule_text, "30 7 * * *")
        self.assertEqual(updated.payload, {"publish": False})

        top_level = self.repository.start_run(
            flow_name="daily_brief_flow",
            worker_key="mini-process",
            input_payload={"publish": False},
            task_spec_id=task_spec.id,
            status="running",
        )
        child = self.repository.start_run(
            flow_name="daily_brief_flow",
            worker_key="analysis_runner",
            input_payload={"stage": "analysis"},
            task_spec_id=task_spec.id,
            parent_run_id=top_level.id,
            status="running",
        )

        top_level_runs = self.repository.list_runs_for_task_spec(task_spec.id, limit=10)
        all_runs = self.repository.list_runs_for_task_spec(task_spec.id, limit=10, include_children=True)
        self.assertEqual([item.id for item in top_level_runs], [top_level.id])
        self.assertEqual([item.id for item in all_runs], [child.id, top_level.id])
