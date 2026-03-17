from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import unquote, urlparse

from fastapi import APIRouter, Depends
from fastapi import HTTPException
from fastapi.responses import FileResponse

from apps.api.dependencies import repository_dep
from libs.db import LifeRepository


router = APIRouter(prefix="/artifacts", tags=["artifacts"])
PREVIEWABLE_MEDIA_TYPES = {
    "application/json",
    "application/xml",
    "image/svg+xml",
}
MAX_PREVIEW_CHARS = 16000


def _artifact_file_path(path: str | None, storage_uri: str) -> Path | None:
    candidates: list[Path] = []
    if path:
        candidates.append(Path(path).expanduser())

    parsed = urlparse(storage_uri)
    if parsed.scheme == "file":
        candidates.append(Path(unquote(parsed.path)).expanduser())

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.exists() and resolved.is_file():
            return resolved
    return None


def _text_preview_allowed(media_type: str | None, path: Path) -> bool:
    if media_type and (media_type.startswith("text/") or media_type in PREVIEWABLE_MEDIA_TYPES):
        return True
    return path.suffix.lower() in {".csv", ".json", ".md", ".markdown", ".txt", ".log", ".html", ".xml", ".svg"}


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


@router.get("/{artifact_id}")
def get_artifact(
    artifact_id: str,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    row = repository.get_artifact(artifact_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Artifact not found: {artifact_id}")
    return {
        "id": row.id,
        "task_spec_id": row.task_spec_id,
        "run_id": row.run_id,
        "kind": row.kind,
        "path": row.path,
        "storage_uri": row.storage_uri,
        "storage_backend": row.storage_backend,
        "size_bytes": row.size_bytes,
        "media_type": row.media_type,
        "metadata": row.metadata_json,
        "created_at": row.created_at,
    }


@router.get("/{artifact_id}/preview")
def preview_artifact(
    artifact_id: str,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    row = repository.get_artifact(artifact_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Artifact not found: {artifact_id}")

    artifact_path = _artifact_file_path(row.path, row.storage_uri)
    if artifact_path is None:
        return {
            "artifact_id": row.id,
            "status": "missing",
            "media_type": row.media_type,
            "download_url": None,
        }

    if not _text_preview_allowed(row.media_type, artifact_path):
        return {
            "artifact_id": row.id,
            "status": "binary",
            "media_type": row.media_type,
            "download_url": f"/artifacts/{row.id}/download",
            "path": str(artifact_path),
        }

    content = artifact_path.read_text(encoding="utf-8", errors="replace")
    if row.media_type == "application/json" or artifact_path.suffix.lower() == ".json":
        try:
            content = json.dumps(json.loads(content), indent=2, sort_keys=True)
        except json.JSONDecodeError:
            pass

    truncated = len(content) > MAX_PREVIEW_CHARS
    return {
        "artifact_id": row.id,
        "status": "text",
        "media_type": row.media_type,
        "download_url": f"/artifacts/{row.id}/download",
        "path": str(artifact_path),
        "content": content[:MAX_PREVIEW_CHARS],
        "truncated": truncated,
    }


@router.get("/{artifact_id}/download")
def download_artifact(
    artifact_id: str,
    repository: LifeRepository = Depends(repository_dep),
) -> FileResponse:
    row = repository.get_artifact(artifact_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Artifact not found: {artifact_id}")

    artifact_path = _artifact_file_path(row.path, row.storage_uri)
    if artifact_path is None:
        raise HTTPException(status_code=404, detail=f"Artifact file not found: {artifact_id}")
    return FileResponse(artifact_path, media_type=row.media_type, filename=artifact_path.name)
