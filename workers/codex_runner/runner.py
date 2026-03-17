from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from libs.config import Settings
from libs.contracts.models import AdapterResult, WorkerStatus

from workers.common import build_file_artifact, ensure_worker_dir, resolve_worker_output_path, write_text


PAPER_REVIEW_PROMPT_PATH = Path(__file__).resolve().parents[2] / "libs" / "prompts" / "paper_review.md"


def load_paper_review_prompt() -> str:
    return PAPER_REVIEW_PROMPT_PATH.read_text(encoding="utf-8")


class CodexCliRequest(BaseModel):
    prompt: str
    cwd: str | None = None
    extra_args: list[str] = Field(default_factory=list)
    output_schema: str | None = None
    output_path: str | None = None

    @property
    def structured_mode(self) -> bool:
        return self.output_schema is not None or self.output_path is not None

    @model_validator(mode="after")
    def validate_structured_mode(self) -> CodexCliRequest:
        if (self.output_schema is None) != (self.output_path is None):
            raise ValueError("output_schema and output_path must be provided together for structured mode")
        return self


class CodexCliRunner:
    worker_key = "codex_cli_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: CodexCliRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        stdout_path = run_dir / "codex_output.txt"
        resolved_output = resolve_worker_output_path(run_dir, request.output_path)
        if resolved_output is not None:
            request = request.model_copy(update={"output_path": str(resolved_output)})

        if shutil.which("codex") is None:
            text = "Codex CLI is not available in PATH; generated placeholder result."
            write_text(stdout_path, text)
            return AdapterResult(
                status=WorkerStatus.SKIPPED,
                stdout=text,
                artifact_manifest=[
                    build_file_artifact(kind="text", path=stdout_path, media_type="text/plain")
                ],
                structured_outputs={"mode": "placeholder"},
            )

        cmd = self._build_command(request)
        completed = subprocess.run(
            cmd,
            cwd=request.cwd or str(run_dir),
            capture_output=True,
            text=True,
            check=False,
        )
        write_text(stdout_path, completed.stdout)

        artifacts = [build_file_artifact(kind="text", path=stdout_path, media_type="text/plain")]
        if request.structured_mode:
            structured_path = Path(request.output_path or "")
            if structured_path.exists() and structured_path.is_file():
                artifacts.append(build_file_artifact(kind="json", path=structured_path, media_type="application/json"))

        return AdapterResult(
            status=WorkerStatus.COMPLETED if completed.returncode == 0 else WorkerStatus.FAILED,
            stdout=completed.stdout,
            stderr=completed.stderr,
            artifact_manifest=artifacts,
            structured_outputs={
                "returncode": completed.returncode,
                "mode": "structured-json" if request.structured_mode else "plain-text",
                "output_schema": request.output_schema,
                "output_path": request.output_path,
                "command": cmd,
            },
        )

    @staticmethod
    def _build_command(request: CodexCliRequest) -> list[str]:
        if request.structured_mode:
            return [
                "codex",
                "exec",
                "--json",
                "--output-schema",
                str(request.output_schema),
                "-o",
                str(request.output_path),
                *request.extra_args,
                request.prompt,
            ]
        return ["codex", "exec", request.prompt, *request.extra_args]
