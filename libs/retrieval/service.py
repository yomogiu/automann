from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from libs.config import get_settings
from libs.contracts.models import SourceFreshnessBucket, SourceKind
from libs.db.repository import LifeRepository

from .semantic import NullSemanticIndexAdapter, QmdSemanticIndexAdapter, SemanticIndexAdapter


_SOURCE_KIND_BOOSTS = {
    SourceKind.OFFICIAL_STATS.value: 0.24,
    SourceKind.DATASET.value: 0.2,
    SourceKind.INSTITUTIONAL_REPORT.value: 0.16,
    SourceKind.PAPER.value: 0.14,
    SourceKind.COMPANY_RESEARCH.value: 0.08,
    SourceKind.NEWSLETTER.value: 0.05,
    SourceKind.EXPERT_BLOG.value: 0.05,
    SourceKind.SOCIAL_POST.value: -0.04,
    SourceKind.GENERAL_WEB.value: 0.0,
}
_DOCUMENT_SCOPE_BOOSTS = {
    "dataset": 0.08,
    "paper": 0.06,
    "report": 0.05,
    "thread": -0.02,
    "post": -0.01,
}
_FRESHNESS_BOOSTS = {
    SourceFreshnessBucket.CURRENT.value: 0.04,
    SourceFreshnessBucket.RECENT.value: 0.02,
    SourceFreshnessBucket.EVERGREEN.value: 0.01,
    SourceFreshnessBucket.HISTORICAL.value: -0.01,
    SourceFreshnessBucket.UNDATED.value: 0.0,
}


@dataclass(slots=True)
class RetrievalDiagnostics:
    mode: str
    warnings: list[str]


class RetrievalService:
    def __init__(
        self,
        repository: LifeRepository,
        semantic_adapter: SemanticIndexAdapter | None = None,
    ):
        self.repository = repository
        self.semantic_adapter: SemanticIndexAdapter
        if semantic_adapter is not None:
            self.semantic_adapter = semantic_adapter
        else:
            settings = get_settings()
            if getattr(settings, "qmd_enabled", False):
                self.semantic_adapter = QmdSemanticIndexAdapter(settings)
            else:
                self.semantic_adapter = NullSemanticIndexAdapter()

    def query(
        self,
        *,
        query: str,
        limit: int = 10,
        embedding: list[float] | None = None,
        include_lexical: bool = True,
        include_semantic: bool = True,
    ) -> list[dict[str, Any]]:
        result = self.query_with_details(
            query=query,
            limit=limit,
            embedding=embedding,
            include_lexical=include_lexical,
            include_semantic=include_semantic,
        )
        return result["results"]

    def query_with_details(
        self,
        *,
        query: str,
        limit: int = 10,
        embedding: list[float] | None = None,
        include_lexical: bool = True,
        include_semantic: bool = True,
    ) -> dict[str, Any]:
        del embedding
        warnings: list[str] = []
        if not include_lexical and not include_semantic:
            return {"results": [], "mode": "none", "warnings": ["retrieval_disabled"]}

        semantic_available = self.semantic_adapter.is_available()
        if include_semantic and not semantic_available:
            warnings.append("semantic_index_unavailable")

        lexical_hits: list[dict[str, Any]] = []
        if include_lexical or (include_semantic and not semantic_available):
            lexical_rows = self.repository.query_chunks(query=query, limit=max(limit * 3, limit))
            lexical_hits = [self._enrich_chunk(row) for row in lexical_rows]

        semantic_hits: list[dict[str, Any]] = []
        if include_semantic and semantic_available:
            semantic_payload = self._semantic_hits(query=query, limit=max(limit * 3, limit))
            semantic_hits = semantic_payload["results"]
            warnings.extend(semantic_payload["warnings"])

        if semantic_hits and lexical_hits:
            results = self._merge_hybrid_hits(query=query, lexical_hits=lexical_hits, semantic_hits=semantic_hits, limit=limit)
            mode = "hybrid"
        elif semantic_hits:
            results = self._stamp_results(semantic_hits[:limit], mode="semantic")
            mode = "semantic"
        elif lexical_hits:
            results = self._stamp_results(lexical_hits[:limit], mode="lexical")
            mode = "lexical"
        elif include_semantic and include_lexical:
            results = []
            mode = "hybrid"
        elif include_semantic:
            results = []
            mode = "semantic"
        else:
            results = []
            mode = "lexical"

        return {
            "results": results,
            "mode": mode,
            "warnings": list(dict.fromkeys(warnings)),
        }

    def sync_semantic_index(self) -> list[str]:
        if not self.semantic_adapter.is_available():
            return []
        return list(self.semantic_adapter.sync_documents(self._current_documents()))

    def _semantic_hits(self, *, query: str, limit: int) -> dict[str, Any]:
        if not self.semantic_adapter.is_available():
            return {"results": [], "warnings": ["semantic_index_unavailable"]}

        documents = self._current_documents()
        warnings = list(self.semantic_adapter.sync_documents(documents))
        hits, query_warnings = self.semantic_adapter.query(query, limit=limit)
        warnings.extend(query_warnings)
        document_map = {str(item["id"]): item for item in documents}
        results = []
        for item in hits:
            candidate = document_map.get(item.chunk_id)
            if candidate is None:
                continue
            metadata = dict(candidate.get("metadata") or {})
            metadata["retrieval"] = {
                **dict(metadata.get("retrieval") or {}),
                "semantic_score": round(item.score, 6),
            }
            results.append({**candidate, "metadata": metadata})
        return {"results": results, "warnings": warnings}

    def _current_documents(self) -> list[dict[str, Any]]:
        documents: list[dict[str, Any]] = []
        for source in self.repository.list_source_documents(limit=5000):
            artifact_id = getattr(source, "current_text_artifact_id", None)
            if not artifact_id:
                continue
            for chunk in self.repository.list_chunks_for_artifact(artifact_id, limit=5000):
                documents.append(self._enrich_chunk(chunk))
        return documents

    def _enrich_chunk(self, row) -> dict[str, Any]:  # noqa: ANN001
        artifact = self.repository.get_artifact(row.artifact_id)
        source = None
        if artifact is not None and artifact.source_document_id:
            source = self.repository.get_source_document(artifact.source_document_id)

        metadata = dict(row.metadata_json or {})
        if artifact is not None:
            metadata.setdefault("artifact_kind", artifact.kind)
            metadata.setdefault("artifact_media_type", artifact.media_type)
        if source is not None:
            metadata.setdefault("source_document_id", source.id)
            metadata.setdefault("canonical_uri", source.canonical_uri)
            metadata.setdefault("source_type", source.source_type)
            metadata.setdefault("source_title", source.title)
            metadata.setdefault("source_author", source.author)
            metadata.setdefault(
                "source_published_at",
                source.published_at.isoformat() if getattr(source, "published_at", None) else None,
            )
            metadata.setdefault("source_metadata", dict(source.metadata_json or {}))
            profile = dict((source.metadata_json or {}).get("source_profile") or {})
            if profile:
                metadata.setdefault("source_profile", profile)

        return {
            "id": row.id,
            "artifact_id": row.artifact_id,
            "ordinal": row.ordinal,
            "text": row.text,
            "token_count": row.token_count,
            "metadata": metadata,
        }

    def _merge_hybrid_hits(
        self,
        *,
        query: str,
        lexical_hits: list[dict[str, Any]],
        semantic_hits: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for rank, item in enumerate(lexical_hits, start=1):
            candidate = merged.setdefault(item["id"], self._clone_result(item))
            metadata = dict(candidate.get("metadata") or {})
            retrieval = dict(metadata.get("retrieval") or {})
            retrieval["lexical_rank"] = rank
            retrieval["lexical_score"] = round(1.0 / rank, 6)
            metadata["retrieval"] = retrieval
            candidate["metadata"] = metadata
        for rank, item in enumerate(semantic_hits, start=1):
            candidate = merged.setdefault(item["id"], self._clone_result(item))
            metadata = dict(candidate.get("metadata") or {})
            retrieval = dict(metadata.get("retrieval") or {})
            retrieval["semantic_rank"] = rank
            retrieval["semantic_score"] = max(
                float(retrieval.get("semantic_score") or 0.0),
                round(1.0 / rank, 6),
            )
            metadata["retrieval"] = retrieval
            candidate["metadata"] = metadata

        ranked = sorted(
            merged.values(),
            key=lambda item: self._hybrid_score(query=query, item=item),
            reverse=True,
        )
        return self._stamp_results(ranked[:limit], mode="hybrid")

    def _hybrid_score(self, *, query: str, item: dict[str, Any]) -> float:
        metadata = dict(item.get("metadata") or {})
        retrieval = dict(metadata.get("retrieval") or {})
        base = float(retrieval.get("lexical_score") or 0.0) + float(retrieval.get("semantic_score") or 0.0)
        return base + self._source_profile_boost(query=query, metadata=metadata)

    def _source_profile_boost(self, *, query: str, metadata: dict[str, Any]) -> float:
        source_profile = dict(metadata.get("source_profile") or {})
        if not source_profile:
            source_profile = dict((metadata.get("source_metadata") or {}).get("source_profile") or {})
        if not source_profile:
            return 0.0

        authority_score = float(source_profile.get("authority_score") or 0.0)
        if authority_score > 1.0:
            authority_score = min(authority_score / 100.0, 1.0)
        boost = authority_score * 0.2
        boost += _SOURCE_KIND_BOOSTS.get(str(source_profile.get("source_kind") or ""), 0.0)
        boost += _DOCUMENT_SCOPE_BOOSTS.get(str(source_profile.get("document_scope") or ""), 0.0)
        boost += _FRESHNESS_BOOSTS.get(str(source_profile.get("freshness_bucket") or ""), 0.0)

        query_tokens = {token for token in str(query).lower().split() if token}
        topic_tokens = {str(item).lower() for item in source_profile.get("topics") or [] if str(item).strip()}
        entity_tokens = {str(item).lower() for item in source_profile.get("entities") or [] if str(item).strip()}
        research_domain = str(source_profile.get("research_domain") or "").lower()
        if research_domain and any(token in research_domain for token in query_tokens):
            boost += 0.04
        if query_tokens.intersection(topic_tokens):
            boost += 0.05
        if query_tokens.intersection(entity_tokens):
            boost += 0.03
        return boost

    @staticmethod
    def _stamp_results(results: list[dict[str, Any]], *, mode: str) -> list[dict[str, Any]]:
        stamped: list[dict[str, Any]] = []
        for item in results:
            candidate = RetrievalService._clone_result(item)
            metadata = dict(candidate.get("metadata") or {})
            retrieval = dict(metadata.get("retrieval") or {})
            retrieval.setdefault("mode", mode)
            metadata["retrieval"] = retrieval
            candidate["metadata"] = metadata
            stamped.append(candidate)
        return stamped

    @staticmethod
    def _clone_result(item: dict[str, Any]) -> dict[str, Any]:
        candidate = dict(item)
        candidate["metadata"] = dict(item.get("metadata") or {})
        return candidate

    @staticmethod
    def freshness_bucket(published_at: datetime | None) -> str:
        if published_at is None:
            return SourceFreshnessBucket.UNDATED.value
        age_days = max(0, (datetime.now(timezone.utc) - published_at).days)
        if age_days <= 30:
            return SourceFreshnessBucket.CURRENT.value
        if age_days <= 365:
            return SourceFreshnessBucket.RECENT.value
        if age_days <= 5 * 365:
            return SourceFreshnessBucket.EVERGREEN.value
        return SourceFreshnessBucket.HISTORICAL.value
