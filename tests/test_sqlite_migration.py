from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from sqlalchemy import JSON, Column, DateTime, Float, ForeignKey, Integer, MetaData, Numeric, String, Table, Text

from libs.config import get_settings
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from scripts.migrate_life_postgres_to_sqlite import migrate_life_data, validate_migration_counts


def _build_source_schema(metadata: MetaData) -> dict[str, Table]:
    task_spec = Table(
        "task_spec",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("task_key", String(255), nullable=False, unique=True),
        Column("task_type", String(100), nullable=False),
        Column("title", String(500), nullable=False),
        Column("description", Text),
        Column("schedule_text", String(255)),
        Column("status", String(50), nullable=False),
        Column("payload", JSON, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    run = Table(
        "run",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("task_spec_id", String(36), ForeignKey("task_spec.id")),
        Column("parent_run_id", String(36), ForeignKey("run.id")),
        Column("flow_name", String(255), nullable=False),
        Column("worker_key", String(255), nullable=False),
        Column("prefect_flow_run_id", String(255), unique=True),
        Column("status", String(50), nullable=False),
        Column("schema_version", String(32), nullable=False),
        Column("command_id", String(64)),
        Column("session_id", String(64)),
        Column("correlation_id", String(64)),
        Column("idempotency_key", String(255)),
        Column("actor", String(32), nullable=False),
        Column("input_payload", JSON, nullable=False),
        Column("structured_outputs", JSON, nullable=False),
        Column("artifact_manifest", JSON, nullable=False),
        Column("observation_summary", JSON, nullable=False),
        Column("next_suggested_events", JSON, nullable=False),
        Column("stdout", Text, nullable=False),
        Column("stderr", Text, nullable=False),
        Column("started_at", DateTime(timezone=True)),
        Column("finished_at", DateTime(timezone=True)),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    artifact = Table(
        "artifact",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("task_spec_id", String(36), ForeignKey("task_spec.id")),
        Column("run_id", String(36), ForeignKey("run.id")),
        Column("kind", String(100), nullable=False),
        Column("path", String(1024)),
        Column("storage_uri", String(2048), nullable=False, unique=True),
        Column("storage_backend", String(100), nullable=False),
        Column("size_bytes", Integer, nullable=False),
        Column("media_type", String(255)),
        Column("sha256", String(64)),
        Column("metadata", JSON, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
    )
    entity = Table(
        "entity",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("entity_type", String(100), nullable=False),
        Column("canonical_name", String(255), nullable=False),
        Column("external_ref", String(255)),
        Column("metadata", JSON, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    report = Table(
        "report",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("run_id", String(36), ForeignKey("run.id")),
        Column("source_artifact_id", String(36), ForeignKey("artifact.id")),
        Column("report_type", String(100), nullable=False),
        Column("title", String(500), nullable=False),
        Column("summary", Text, nullable=False),
        Column("content_markdown", Text, nullable=False),
        Column("score", Numeric(8, 3)),
        Column("metadata", JSON, nullable=False),
        Column("published_at", DateTime(timezone=True)),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    observation = Table(
        "observation",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("run_id", String(36), ForeignKey("run.id")),
        Column("artifact_id", String(36), ForeignKey("artifact.id")),
        Column("entity_id", String(36), ForeignKey("entity.id")),
        Column("report_id", String(36), ForeignKey("report.id")),
        Column("kind", String(100), nullable=False),
        Column("summary", Text, nullable=False),
        Column("payload", JSON, nullable=False),
        Column("source_offsets", JSON, nullable=False),
        Column("confidence", Float),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    chunk = Table(
        "chunk",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("artifact_id", String(36), ForeignKey("artifact.id"), nullable=False),
        Column("ordinal", Integer, nullable=False),
        Column("text", Text, nullable=False),
        Column("token_count", Integer, nullable=False),
        Column("metadata", JSON, nullable=False),
        Column("search_vector", Text),
        Column("embedding", JSON),
        Column("created_at", DateTime(timezone=True), nullable=False),
    )
    citation_link = Table(
        "citation_link",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("report_id", String(36), ForeignKey("report.id"), nullable=False),
        Column("observation_id", String(36), ForeignKey("observation.id")),
        Column("artifact_id", String(36), ForeignKey("artifact.id")),
        Column("chunk_id", String(36), ForeignKey("chunk.id")),
        Column("anchor_text", String(500), nullable=False),
        Column("locator", String(255)),
        Column("metadata", JSON, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
    )
    interaction = Table(
        "interaction",
        metadata,
        Column("id", String(36), primary_key=True),
        Column("run_id", String(36), ForeignKey("run.id"), nullable=False),
        Column("prefect_flow_run_id", String(255)),
        Column("checkpoint_key", String(255)),
        Column("title", String(500), nullable=False),
        Column("prompt_md", Text, nullable=False),
        Column("input_schema", JSON, nullable=False),
        Column("default_input", JSON),
        Column("response_payload", JSON),
        Column("ui_hints", JSON, nullable=False),
        Column("status", String(50), nullable=False),
        Column("answered_at", DateTime(timezone=True)),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )
    return {
        "task_spec": task_spec,
        "run": run,
        "artifact": artifact,
        "entity": entity,
        "report": report,
        "observation": observation,
        "chunk": chunk,
        "citation_link": citation_link,
        "interaction": interaction,
    }


class SQLiteMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.source_url = f"sqlite+pysqlite:///{root / 'source.db'}"
        self.target_url = f"sqlite+pysqlite:///{root / 'target.db'}"
        self.source_engine = engine_for_url(self.source_url)

        metadata = MetaData()
        self.tables = _build_source_schema(metadata)
        metadata.create_all(self.source_engine)

        now = datetime.now(timezone.utc)
        with self.source_engine.begin() as connection:
            connection.execute(
                self.tables["task_spec"].insert(),
                {
                    "id": "task-1",
                    "task_key": "paper-review-1",
                    "task_type": "paper_review",
                    "title": "Paper Review",
                    "description": "Seed task",
                    "schedule_text": None,
                    "status": "active",
                    "payload": {"topic": "agents"},
                    "created_at": now,
                    "updated_at": now,
                },
            )
            connection.execute(
                self.tables["run"].insert(),
                {
                    "id": "run-1",
                    "task_spec_id": "task-1",
                    "parent_run_id": None,
                    "flow_name": "paper_review_flow",
                    "worker_key": "worker",
                    "prefect_flow_run_id": "prefect-123",
                    "status": "completed",
                    "schema_version": "2026-03-16",
                    "command_id": "command-1",
                    "session_id": "session-1",
                    "correlation_id": "corr-1",
                    "idempotency_key": "idem-1",
                    "actor": "user",
                    "input_payload": {"paper_id": "paper-1"},
                    "structured_outputs": {"ok": True},
                    "artifact_manifest": [],
                    "observation_summary": [],
                    "next_suggested_events": [],
                    "stdout": "done",
                    "stderr": "",
                    "started_at": now,
                    "finished_at": now,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            connection.execute(
                self.tables["artifact"].insert(),
                {
                    "id": "artifact-1",
                    "task_spec_id": "task-1",
                    "run_id": "run-1",
                    "kind": "review-card",
                    "path": str(root / "review.md"),
                    "storage_uri": (root / "review.md").as_uri(),
                    "storage_backend": "local",
                    "size_bytes": 128,
                    "media_type": "text/markdown",
                    "sha256": "abc123",
                    "metadata": {"paper_id": "paper-1"},
                    "created_at": now,
                },
            )
            connection.execute(
                self.tables["entity"].insert(),
                {
                    "id": "entity-1",
                    "entity_type": "paper",
                    "canonical_name": "Paper 1",
                    "external_ref": "paper-1",
                    "metadata": {"paper_id": "paper-1"},
                    "created_at": now,
                    "updated_at": now,
                },
            )
            connection.execute(
                self.tables["report"].insert(),
                {
                    "id": "report-1",
                    "run_id": "run-1",
                    "source_artifact_id": "artifact-1",
                    "report_type": "paper_review",
                    "title": "Paper 1",
                    "summary": "alpha beta gamma",
                    "content_markdown": "# Paper 1",
                    "score": None,
                    "metadata": {"paper_id": "paper-1"},
                    "published_at": now,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            connection.execute(
                self.tables["observation"].insert(),
                {
                    "id": "obs-1",
                    "run_id": "run-1",
                    "artifact_id": "artifact-1",
                    "entity_id": "entity-1",
                    "report_id": "report-1",
                    "kind": "summary",
                    "summary": "alpha beta gamma",
                    "payload": {"note": "seed"},
                    "source_offsets": {"start": 0, "end": 5},
                    "confidence": 0.7,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            connection.execute(
                self.tables["chunk"].insert(),
                {
                    "id": "chunk-1",
                    "artifact_id": "artifact-1",
                    "ordinal": 0,
                    "text": "alpha beta gamma",
                    "token_count": 3,
                    "metadata": {"page": 1},
                    "search_vector": "'alpha' 'beta' 'gamma'",
                    "embedding": [0.1, 0.2, 0.3],
                    "created_at": now,
                },
            )
            connection.execute(
                self.tables["citation_link"].insert(),
                {
                    "id": "citation-1",
                    "report_id": "report-1",
                    "observation_id": "obs-1",
                    "artifact_id": "artifact-1",
                    "chunk_id": "chunk-1",
                    "anchor_text": "alpha beta gamma",
                    "locator": "page:1",
                    "metadata": {"page": 1},
                    "created_at": now,
                },
            )
            connection.execute(
                self.tables["interaction"].insert(),
                {
                    "id": "interaction-1",
                    "run_id": "run-1",
                    "prefect_flow_run_id": "prefect-123",
                    "checkpoint_key": "check-1",
                    "title": "Review",
                    "prompt_md": "Proceed?",
                    "input_schema": {"type": "object"},
                    "default_input": {"approved": False},
                    "response_payload": {"approved": True},
                    "ui_hints": {"kind": "confirmation"},
                    "status": "answered",
                    "answered_at": now,
                    "created_at": now,
                    "updated_at": now,
                },
            )

        settings = get_settings().model_copy(update={"life_database_url": self.target_url})
        bootstrap_life_database(settings)
        self.target_engine = engine_for_url(self.target_url)

    def tearDown(self) -> None:
        self.source_engine.dispose()
        self.target_engine.dispose()
        self.temp_dir.cleanup()

    def test_migrate_life_data_preserves_rows_and_rebuilds_fts(self) -> None:
        summary = migrate_life_data(self.source_engine, self.target_engine)
        self.assertEqual(summary["task_spec"]["source_count"], 1)
        self.assertEqual(summary["interaction"]["target_count"], 1)

        repository = LifeRepository(self.target_engine)
        hits = repository.query_chunks(query="alpha beta", limit=5)
        self.assertEqual([item.id for item in hits], ["chunk-1"])

        target_metadata = MetaData()
        target_metadata.reflect(bind=self.target_engine)
        run_table = target_metadata.tables["run"]
        chunk_table = target_metadata.tables["chunk"]
        report_table = target_metadata.tables["report"]

        with self.target_engine.connect() as connection:
            run_row = connection.execute(run_table.select()).mappings().one()
            chunk_row = connection.execute(chunk_table.select()).mappings().one()
            report_row = connection.execute(report_table.select()).mappings().one()

        self.assertEqual(run_row["prefect_flow_run_id"], "prefect-123")
        self.assertEqual(run_row["input_payload"], {"paper_id": "paper-1"})
        self.assertEqual(chunk_row["embedding"], [0.1, 0.2, 0.3])
        self.assertEqual(report_row["report_series_id"], "report-1")
        self.assertEqual(report_row["revision_number"], 1)
        self.assertTrue(report_row["is_current"])

    def test_validate_migration_counts_raises_for_mismatch(self) -> None:
        with self.assertRaises(RuntimeError):
            validate_migration_counts({"task_spec": {"source_count": 1, "target_count": 0}})
