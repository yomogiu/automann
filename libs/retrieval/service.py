from __future__ import annotations

from typing import Any

from libs.db.repository import LifeRepository


class RetrievalService:
    def __init__(self, repository: LifeRepository):
        self.repository = repository

    def query(
        self,
        *,
        query: str,
        limit: int = 10,
        embedding: list[float] | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.repository.query_chunks(query=query, limit=limit, embedding=embedding)
        return [
            {
                "id": row.id,
                "artifact_id": row.artifact_id,
                "ordinal": row.ordinal,
                "text": row.text,
                "token_count": row.token_count,
                "metadata": dict(row.metadata_json),
            }
            for row in rows
        ]
