from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from libs.config import Settings
from libs.contracts.models import AdapterResult, WorkerStatus
from libs.contracts.workers import CodexTaskOutput, CodexTaskRequest

from workers.common import build_file_artifact, ensure_worker_dir, resolve_worker_output_path, write_text


PAPER_REVIEW_PROMPT_PATH = Path(__file__).resolve().parents[2] / "libs" / "prompts" / "paper_review.md"


def load_paper_review_prompt() -> str:
    return PAPER_REVIEW_PROMPT_PATH.read_text(encoding="utf-8")


# Compatibility alias for existing imports and tests.
CodexCliRequest = CodexTaskRequest


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
            output = CodexTaskOutput(mode="placeholder")
            return AdapterResult(
                status=WorkerStatus.SKIPPED,
                stdout=text,
                artifact_manifest=[
                    build_file_artifact(kind="text", path=stdout_path, media_type="text/plain")
                ],
                structured_outputs=output.model_dump(mode="json"),
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

        output = CodexTaskOutput(
            returncode=completed.returncode,
            mode="structured-json" if request.structured_mode else "plain-text",
            output_schema=request.output_schema,
            output_path=request.output_path,
            command=cmd,
        )
        return AdapterResult(
            status=WorkerStatus.COMPLETED if completed.returncode == 0 else WorkerStatus.FAILED,
            stdout=completed.stdout,
            stderr=completed.stderr,
            artifact_manifest=artifacts,
            structured_outputs=output.model_dump(mode="json"),
        )

    @staticmethod
    def _build_command(request: CodexCliRequest) -> list[str]:
        extra_args = list(request.extra_args)
        if "--skip-git-repo-check" not in extra_args:
            extra_args = ["--skip-git-repo-check", *extra_args]
        if request.structured_mode:
            return [
                "codex",
                "exec",
                *extra_args,
                "--json",
                "--output-schema",
                str(request.output_schema),
                "-o",
                str(request.output_path),
                request.prompt,
            ]
        return ["codex", "exec", *extra_args, request.prompt]
