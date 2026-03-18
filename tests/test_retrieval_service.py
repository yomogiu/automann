from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from sqlalchemy import select

from libs.config import get_settings
from libs.contracts.models import AdapterResult, ArtifactRecord, WorkerStatus
from libs.db import Chunk, LifeRepository, bootstrap_life_database, engine_for_url, session_scope
from libs.retrieval.semantic import SemanticHit
from libs.retrieval.service import RetrievalService


class _FakeSemanticAdapter:
    def __init__(self, hits: list[SemanticHit]) -> None:
        self.hits = hits
        self.calls: list[dict[str, object]] = []

    def is_available(self) -> bool:
        return True

    def sync_documents(self, documents: list[dict[str, object]]) -> list[str]:
        del documents
        return []

    def query(
        self,
        query: str,
        *,
        limit: int = 10,
        embedding: list[float] | None = None,
    ) -> tuple[list[SemanticHit], list[str]]:
        self.calls.append({"query": query, "limit": limit, "embedding": embedding})
        return self.hits[:limit], []


class RetrievalServiceTests(unittest.TestCase):
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

    def _create_source_chunk(
        self,
        *,
        canonical_uri: str,
        storage_name: str,
        text: str,
        title: str,
        source_profile: dict[str, object] | None = None,
        current: bool = True,
    ) -> tuple[str, str]:
        artifact_path = self.root / "artifacts" / storage_name
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(text, encoding="utf-8")

        run = self.repository.start_run(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload={"items": [{"input_kind": "file", "file_path": str(artifact_path)}]},
            status="running",
        )
        persisted = self.repository.persist_adapter_result(
            run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="source-text",
                        path=str(artifact_path),
                        storage_uri=artifact_path.as_uri(),
                        size_bytes=artifact_path.stat().st_size,
                        media_type="text/plain",
                        metadata={
                            "role": "normalized_text",
                            "input_index": 0,
                            "canonical_uri": canonical_uri,
                            "source_type": "markdown",
                        },
                    )
                ],
            ),
        )
        assert persisted is not None
        artifact = persisted.artifacts[0]

        source = self.repository.upsert_source_document(
            canonical_uri=canonical_uri,
            source_type="markdown",
            title=title,
            current_text_artifact_id=artifact.id if current else None,
            metadata_json={"source_profile": source_profile or {}},
        )
        self.repository.assign_artifact_to_source(artifact.id, source.id)
        if current:
            self.repository.update_source_document_current_text_artifact(source.id, current_text_artifact_id=artifact.id)
        self.repository.upsert_chunks(
            artifact_id=artifact.id,
            chunks=[
                {
                    "ordinal": 0,
                    "text": text,
                    "token_count": max(1, len(text.split())),
                    "metadata": {"canonical_uri": canonical_uri, "source_type": "markdown"},
                }
            ],
        )

        with session_scope(self.engine) as session:
            chunk = session.scalars(select(Chunk).where(Chunk.artifact_id == artifact.id)).first()
        assert chunk is not None
        return artifact.id, chunk.id

    def test_lexical_only_returns_current_text_results(self) -> None:
        _, chunk_id = self._create_source_chunk(
            canonical_uri="file:///knowledge/lexical.md",
            storage_name="lexical.md",
            text="shared ingest context for downstream research",
            title="Lexical note",
            source_profile={
                "source_kind": "general_web",
                "document_scope": "snippet",
                "authority_score": 0.1,
                "freshness_bucket": "recent",
            },
        )
        service = RetrievalService(self.repository)

        results = service.query(query="downstream research", include_semantic=False, include_lexical=True)

        self.assertEqual([item["id"] for item in results], [chunk_id])
        self.assertEqual(results[0]["text"], "shared ingest context for downstream research")

    def test_semantic_only_uses_adapter_hits_and_filters_stale_chunks(self) -> None:
        _, stale_chunk_id = self._create_source_chunk(
            canonical_uri="file:///knowledge/stale.md",
            storage_name="stale.md",
            text="stale archived analysis of automation risk",
            title="Stale note",
            source_profile={
                "source_kind": "general_web",
                "document_scope": "snippet",
                "authority_score": 0.1,
                "freshness_bucket": "stale",
            },
            current=False,
        )
        _, current_chunk_id = self._create_source_chunk(
            canonical_uri="file:///knowledge/current.md",
            storage_name="current.md",
            text="current analysis of automation risk in labor markets",
            title="Current note",
            source_profile={
                "source_kind": "paper",
                "document_scope": "full_text",
                "research_domain": "labor economics",
                "authority_score": 0.95,
                "freshness_bucket": "recent",
                "topics": ["automation", "labor markets"],
            },
        )
        fake_adapter = _FakeSemanticAdapter(
            [
                SemanticHit(chunk_id=stale_chunk_id, score=0.99),
                SemanticHit(chunk_id=current_chunk_id, score=0.75),
            ]
        )
        service = RetrievalService(self.repository, semantic_adapter=fake_adapter)

        results = service.query(query="automation risk", include_semantic=True, include_lexical=False)

        self.assertEqual([item["id"] for item in results], [current_chunk_id])
        self.assertNotIn(stale_chunk_id, [item["id"] for item in results])
        self.assertEqual(fake_adapter.calls[0]["query"], "automation risk")
        self.assertEqual(fake_adapter.calls[0]["limit"], 30)

    def test_hybrid_reranks_with_source_profile_boosts(self) -> None:
        _, lexical_chunk_id = self._create_source_chunk(
            canonical_uri="file:///knowledge/lexical-boost.md",
            storage_name="lexical-boost.md",
            text="jobs automation risk in clerical reporting work",
            title="Lexical boost note",
            source_profile={
                "source_kind": "general_web",
                "document_scope": "snippet",
                "authority_score": 0.05,
                "freshness_bucket": "recent",
            },
        )
        _, semantic_chunk_id = self._create_source_chunk(
            canonical_uri="file:///knowledge/semantic-boost.md",
            storage_name="semantic-boost.md",
            text="central bank sector note",
            title="Semantic boost note",
            source_profile={
                "source_kind": "paper",
                "document_scope": "full_text",
                "research_domain": "labor economics",
                "authority_score": 0.95,
                "freshness_bucket": "recent",
                "topics": ["jobs", "automation", "labor"],
            },
        )
        fake_adapter = _FakeSemanticAdapter([SemanticHit(chunk_id=semantic_chunk_id, score=0.88)])
        service = RetrievalService(self.repository, semantic_adapter=fake_adapter)

        results = service.query(query="jobs automation risk", include_semantic=True, include_lexical=True)

        self.assertEqual([item["id"] for item in results[:2]], [semantic_chunk_id, lexical_chunk_id])
        self.assertEqual(results[0]["text"], "central bank sector note")

    def test_semantic_fallbacks_to_lexical_when_adapter_is_unavailable(self) -> None:
        _, chunk_id = self._create_source_chunk(
            canonical_uri="file:///knowledge/fallback.md",
            storage_name="fallback.md",
            text="shared fallback retrieval context",
            title="Fallback note",
            source_profile={
                "source_kind": "expert_blog",
                "document_scope": "article",
                "authority_score": 0.4,
                "freshness_bucket": "recent",
            },
        )
        service = RetrievalService(self.repository)

        results = service.query(query="fallback retrieval", include_semantic=True, include_lexical=False)

        self.assertEqual([item["id"] for item in results], [chunk_id])
