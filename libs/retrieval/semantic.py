from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
from typing import Any

from libs.config import Settings


_TOKEN_RE = re.compile(r"\b[a-z0-9][a-z0-9._-]{1,}\b")
_SEMANTIC_SYNONYMS = {
    "ai": {"automation", "ml", "machine", "intelligence"},
    "automation": {"ai", "automated", "robotic"},
    "employment": {"jobs", "labor", "work", "workforce"},
    "jobs": {"employment", "labor", "work", "occupation"},
    "labor": {"employment", "jobs", "work"},
    "travel": {"trip", "itinerary", "tourism"},
    "trip": {"travel", "itinerary", "journey"},
    "itinerary": {"trip", "travel", "schedule"},
    "paper": {"study", "research", "preprint"},
    "research": {"study", "analysis", "paper"},
    "dataset": {"data", "statistics", "table"},
    "statistics": {"data", "dataset", "metrics"},
}


@dataclass(slots=True)
class SemanticIndexHit:
    chunk_id: str
    score: float


SemanticHit = SemanticIndexHit


class SemanticIndexAdapter:
    def is_available(self) -> bool:
        raise NotImplementedError

    def sync_documents(self, documents: list[dict[str, Any]]) -> list[str]:
        raise NotImplementedError

    def query(self, query: str, *, limit: int) -> tuple[list[SemanticIndexHit], list[str]]:
        raise NotImplementedError

    @property
    def available(self) -> bool:
        return self.is_available()


class NullSemanticIndexAdapter(SemanticIndexAdapter):
    def is_available(self) -> bool:
        return False

    def sync_documents(self, documents: list[dict[str, Any]]) -> list[str]:
        del documents
        return []

    def query(
        self,
        query: str,
        *,
        limit: int,
        embedding: list[float] | None = None,
    ) -> tuple[list[SemanticIndexHit], list[str]]:
        del query, limit, embedding
        return [], ["semantic_index_unavailable"]


class QmdSemanticIndexAdapter(SemanticIndexAdapter):
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        index_root: Path | None = None,
        enabled: bool | None = None,
    ):
        self.settings = settings
        self._enabled = enabled if enabled is not None else self._resolve_enabled(settings)
        self.index_root = index_root or self._resolve_index_root(settings)
        self.index_path = self.index_root / "semantic-index.json"

    def is_available(self) -> bool:
        return bool(self._enabled)

    def sync_documents(self, documents: list[dict[str, Any]]) -> list[str]:
        if not self.is_available():
            return []
        self.index_root.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "chunk_count": len(documents),
            "items": [
                {
                    "chunk_id": str(item.get("chunk_id") or item.get("id") or ""),
                    "terms": _semantic_terms(str(item.get("text") or "")),
                }
                for item in documents
                if str(item.get("chunk_id") or "").strip()
            ],
        }
        self.index_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return []

    def query(
        self,
        query: str,
        *,
        limit: int,
        embedding: list[float] | None = None,
    ) -> tuple[list[SemanticIndexHit], list[str]]:
        del embedding
        if not self.is_available():
            return [], ["semantic_index_unavailable"]
        if not self.index_path.exists():
            return [], ["semantic_index_missing"]

        try:
            payload = json.loads(self.index_path.read_text(encoding="utf-8"))
        except Exception:
            return [], ["semantic_index_corrupt"]

        query_terms = Counter(_semantic_terms(query))
        if not query_terms:
            return [], []

        hits: list[SemanticIndexHit] = []
        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            chunk_id = str(item.get("chunk_id") or "").strip()
            if not chunk_id:
                continue
            doc_terms = Counter(str(term).strip() for term in item.get("terms") or [] if str(term).strip())
            score = _cosine_similarity(query_terms, doc_terms)
            if score <= 0:
                continue
            hits.append(SemanticIndexHit(chunk_id=chunk_id, score=score))

        hits.sort(key=lambda item: item.score, reverse=True)
        return hits[:limit], []

    @staticmethod
    def _resolve_enabled(settings: Settings | None) -> bool:
        if settings is not None and hasattr(settings, "qmd_enabled"):
            return bool(getattr(settings, "qmd_enabled"))
        env_value = os.environ.get("LIFE_QMD_ENABLED") or os.environ.get("QMD_ENABLED") or ""
        return env_value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _resolve_index_root(settings: Settings | None) -> Path:
        if settings is not None and hasattr(settings, "qmd_index_root"):
            index_root = getattr(settings, "qmd_index_root")
            if index_root is not None:
                return Path(index_root)
        env_root = os.environ.get("LIFE_QMD_INDEX_ROOT") or os.environ.get("QMD_INDEX_ROOT")
        if env_root:
            return Path(env_root)
        if settings is not None and hasattr(settings, "runtime_root"):
            return Path(getattr(settings, "runtime_root")) / "qmd"
        return Path.cwd() / "qmd"


def _semantic_terms(text: str) -> list[str]:
    tokens = [match.group(0).lower() for match in _TOKEN_RE.finditer(text)]
    expanded: list[str] = []
    for token in tokens:
        expanded.append(token)
        if token.endswith("s") and len(token) > 4:
            expanded.append(token[:-1])
        if token.endswith("ing") and len(token) > 5:
            expanded.append(token[:-3])
        expanded.extend(sorted(_SEMANTIC_SYNONYMS.get(token, ())))
    return expanded


def _cosine_similarity(left: Counter[str], right: Counter[str]) -> float:
    if not left or not right:
        return 0.0
    shared = set(left).intersection(right)
    numerator = sum(left[token] * right[token] for token in shared)
    if numerator <= 0:
        return 0.0
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm <= 0 or right_norm <= 0:
        return 0.0
    return numerator / (left_norm * right_norm)


def build_default_semantic_adapter(settings: Settings | None = None) -> SemanticIndexAdapter:
    adapter = QmdSemanticIndexAdapter(settings=settings)
    if adapter.is_available():
        return adapter
    return NullSemanticIndexAdapter()
