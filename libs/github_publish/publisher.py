from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class PublicationBundle:
    bundle_dir: Path
    manifest_path: Path
    release_tag: str
    files: list[Path]


def prepare_publication_bundle(
    *,
    destination_root: Path,
    release_tag: str,
    report_path: Path,
    artifact_paths: list[Path],
    metadata: dict[str, Any] | None = None,
) -> PublicationBundle:
    bundle_dir = destination_root / release_tag
    bundle_dir.mkdir(parents=True, exist_ok=True)

    copied_files: list[Path] = []
    for source in [report_path, *artifact_paths]:
        if not source.exists():
            continue
        target = bundle_dir / source.name
        target.write_bytes(source.read_bytes())
        copied_files.append(target)

    manifest_path = bundle_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "release_tag": release_tag,
                "files": [str(path.name) for path in copied_files],
                "metadata": metadata or {},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return PublicationBundle(
        bundle_dir=bundle_dir,
        manifest_path=manifest_path,
        release_tag=release_tag,
        files=copied_files,
    )
