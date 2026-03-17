from __future__ import annotations

from fastapi import APIRouter, Depends

from apps.api.dependencies import repository_dep
from libs.db import LifeRepository


router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("")
def list_reports(
    limit: int = 25,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    rows = repository.list_reports(limit=limit)
    return {
        "reports": [
            {
                "id": row.id,
                "report_type": row.report_type,
                "title": row.title,
                "summary": row.summary,
                "content_markdown": row.content_markdown,
                "source_artifact_id": row.source_artifact_id,
                "metadata": row.metadata_json,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in rows
        ]
    }
