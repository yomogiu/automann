from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.dependencies import repository_dep
from libs.db import LifeRepository
from libs.db.models import Artifact, Chunk, SourceDocument


router = APIRouter(prefix="/sources", tags=["sources"])


def _artifact_urls(artifact_id: str) -> dict[str, str]:
    return {
        "preview_url": f"/artifacts/{artifact_id}/preview",
        "download_url": f"/artifacts/{artifact_id}/download",
    }


def _serialize_chunk(row: Chunk) -> dict:
    return {
        "id": row.id,
        "artifact_id": row.artifact_id,
        "ordinal": row.ordinal,
        "text": row.text,
        "token_count": row.token_count,
        "metadata": row.metadata_json,
        "created_at": row.created_at,
    }


def _serialize_artifact(row: Artifact) -> dict:
    return {
        "id": row.id,
        "task_spec_id": row.task_spec_id,
        "run_id": row.run_id,
        "source_document_id": row.source_document_id,
        "kind": row.kind,
        "path": row.path,
        "storage_uri": row.storage_uri,
        "storage_backend": row.storage_backend,
        "size_bytes": row.size_bytes,
        "media_type": row.media_type,
        "metadata": row.metadata_json,
        "created_at": row.created_at,
        **_artifact_urls(row.id),
    }


def _serialize_source(row: SourceDocument, *, artifact_count: int) -> dict:
    return {
        "id": row.id,
        "canonical_uri": row.canonical_uri,
        "source_type": row.source_type,
        "title": row.title,
        "author": row.author,
        "published_at": row.published_at,
        "current_text_artifact_id": row.current_text_artifact_id,
        "metadata": row.metadata_json,
        "artifact_count": artifact_count,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


@router.get("")
def list_sources(
    limit: int = 100,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    rows = repository.list_source_documents(limit=limit)
    return {
        "sources": [
            _serialize_source(row, artifact_count=len(repository.list_artifacts_for_source_document(row.id)))
            for row in rows
        ]
    }


@router.get("/{source_id}")
def get_source(
    source_id: str,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    row = repository.get_source_document(source_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Source document not found: {source_id}")

    artifacts = repository.list_artifacts_for_source_document(row.id)
    current_artifact = repository.get_artifact(row.current_text_artifact_id) if row.current_text_artifact_id else None
    current_chunks = (
        repository.list_chunks_for_artifact(row.current_text_artifact_id) if row.current_text_artifact_id else []
    )
    return {
        "source": {
            **_serialize_source(row, artifact_count=len(artifacts)),
            "current_text_artifact": _serialize_artifact(current_artifact) if current_artifact else None,
            "artifacts": [_serialize_artifact(artifact) for artifact in artifacts],
            "current_chunks": [_serialize_chunk(chunk) for chunk in current_chunks],
        }
    }
