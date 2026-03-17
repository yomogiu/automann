from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from pydantic import BaseModel, Field

from libs.config import Settings
from libs.contracts.models import AdapterResult, ArtifactRecord, WorkerStatus

from workers.common import ensure_worker_dir, sha256_path, write_text


class CodexCliRequest(BaseModel):
    prompt: str
    cwd: str | None = None
    extra_args: list[str] = Field(default_factory=list)


class CodexCliRunner:
    worker_key = "codex_cli_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: CodexCliRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        output_path = run_dir / "codex_output.txt"

        if shutil.which("codex") is None:
            text = "Codex CLI is not available in PATH; generated placeholder result."
            write_text(output_path, text)
            return AdapterResult(
                status=WorkerStatus.SKIPPED,
                stdout=text,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="text",
                        path=str(output_path),
                        media_type="text/plain",
                        sha256=sha256_path(output_path),
                    )
                ],
                structured_outputs={"mode": "placeholder"},
            )

        cmd = ["codex", "exec", request.prompt, *request.extra_args]
        completed = subprocess.run(
            cmd,
            cwd=request.cwd or str(Path.cwd()),
            capture_output=True,
            text=True,
            check=False,
        )
        write_text(output_path, completed.stdout)
        return AdapterResult(
            status=WorkerStatus.COMPLETED if completed.returncode == 0 else WorkerStatus.FAILED,
            stdout=completed.stdout,
            stderr=completed.stderr,
            artifact_manifest=[
                ArtifactRecord(
                    kind="text",
                    path=str(output_path),
                    media_type="text/plain",
                    sha256=sha256_path(output_path),
                )
            ],
            structured_outputs={"returncode": completed.returncode},
        )
