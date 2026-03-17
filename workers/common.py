from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.config import Settings


def timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_worker_dir(settings: Settings, worker_key: str) -> Path:
    path = settings.artifact_root / worker_key / timestamp_slug()
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_file_artifact(
    *,
    kind: str,
    path: Path,
    media_type: str | None = None,
    metadata: dict[str, Any] | None = None,
):
    from libs.contracts.models import ArtifactRecord

    resolved = path.resolve()
    return ArtifactRecord(
        kind=kind,
        path=str(resolved),
        storage_uri=resolved.as_uri(),
        size_bytes=resolved.stat().st_size,
        media_type=media_type,
        sha256=sha256_path(resolved),
        metadata=metadata or {},
    )
