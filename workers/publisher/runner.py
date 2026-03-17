from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from libs.config import Settings
from libs.contracts.events import EventName
from libs.contracts.models import (
    AdapterResult,
    EventSuggestion,
    ObservationRecord,
    WorkerStatus,
)
from libs.github_publish import prepare_publication_bundle
from libs.contracts.workers import PublishOutput, PublishRequest

from workers.common import build_file_artifact


class GitHubPublisher:
    worker_key = "github_publisher"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: PublishRequest) -> AdapterResult:
        release_tag = f"{self.settings.github_release_prefix}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        publication_dir = self.settings.report_root / "generated"
        bundle = prepare_publication_bundle(
            destination_root=publication_dir,
            release_tag=release_tag,
            report_path=Path(request.report_path),
            artifact_paths=[Path(item) for item in request.artifact_paths],
            metadata=request.metadata,
        )

        stdout = f"Prepared publication bundle {bundle.release_tag}"
        publish_mode = "local-bundle"
        output = PublishOutput(
            publish_mode=publish_mode,
            release_tag=bundle.release_tag,
            bundle_dir=str(bundle.bundle_dir),
            manifest_path=str(bundle.manifest_path),
        )

        if shutil.which("gh") and self.settings.github_owner and self.settings.github_repo:
            publish_mode = "github-release"
            output = output.model_copy(update={"publish_mode": publish_mode})
            cmd = [
                "gh",
                "release",
                "create",
                bundle.release_tag,
                str(bundle.manifest_path),
                *[str(path) for path in bundle.files],
                "--repo",
                f"{self.settings.github_owner}/{self.settings.github_repo}",
                "--title",
                bundle.release_tag,
                "--notes",
                "Automated life-system publication bundle.",
            ]
            completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
            stdout = completed.stdout or stdout
            if completed.returncode:
                return AdapterResult(
                    status=WorkerStatus.FAILED,
                    stdout=stdout,
                    stderr=completed.stderr,
                    artifact_manifest=[
                        build_file_artifact(
                            kind="publication-manifest",
                            path=bundle.manifest_path,
                            media_type="application/json",
                        )
                    ],
                    structured_outputs=output.model_dump(mode="json"),
                )

        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=stdout,
            artifact_manifest=[
                build_file_artifact(
                    kind="publication-manifest",
                    path=bundle.manifest_path,
                    media_type="application/json",
                )
            ],
            structured_outputs=output.model_dump(mode="json"),
            observations=[
                ObservationRecord(
                    kind="publication_bundle",
                    summary=f"Prepared publication bundle {bundle.release_tag}.",
                    payload={"bundle_dir": str(bundle.bundle_dir), "publish_mode": publish_mode},
                    confidence=0.9,
                )
            ],
            next_suggested_events=[
                EventSuggestion(name=EventName.REPORT_PUBLISHED, payload={"release_tag": bundle.release_tag})
            ],
        )
