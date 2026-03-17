from __future__ import annotations

from fastapi import APIRouter, Depends

from apps.api.dependencies import repository_dep
from libs.db import LifeRepository


router = APIRouter(prefix="/artifacts", tags=["artifacts"])


@router.get("")
def list_artifacts(
    limit: int = 50,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    rows = repository.list_artifacts(limit=limit)
    return {
        "artifacts": [
            {
                "id": row.id,
                "kind": row.kind,
                "path": row.path,
                "storage_uri": row.storage_uri,
                "storage_backend": row.storage_backend,
                "size_bytes": row.size_bytes,
                "media_type": row.media_type,
                "metadata": row.metadata_json,
                "created_at": row.created_at,
            }
            for row in rows
        ]
    }
