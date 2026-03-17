from __future__ import annotations

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
                        WHERE name IN ('chunk_fts', 'chunk_fts_ai', 'chunk_fts_ad', 'chunk_fts_au')
                        """
                    )
                ).scalars()
            )

        self.assertEqual(foreign_keys, 1)
        self.assertEqual(objects, {"chunk_fts", "chunk_fts_ai", "chunk_fts_ad", "chunk_fts_au"})

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
