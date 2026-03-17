from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from libs.config import Settings
from libs.contracts.models import AdapterResult, WorkerStatus
from libs.contracts.workers import CodexSearchSessionOutput, CodexSearchSessionRequest
from workers.common import build_file_artifact, ensure_worker_dir, resolve_worker_output_path, write_json, write_text


_SESSION_INDEX_PATH = Path.home() / ".codex" / "session_index.jsonl"
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)
_SESSION_ID_KEYS = ("session_id", "conversation_id", "thread_id")
_THREAD_NAME_KEYS = ("thread_name", "conversation_title", "title")


@dataclass(slots=True)
class _SessionIndexEntry:
    session_id: str
    thread_name: str | None
    updated_at: str | None


class CodexSearchSessionRunner:
    worker_key = "codex_search_session_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: CodexSearchSessionRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        resolved_output = resolve_worker_output_path(run_dir, request.output_path) or (run_dir / "codex_last_message.json")
        events_path = run_dir / "codex_events.jsonl"
        manifest_path = run_dir / "codex_session_manifest.json"
        stderr_path = run_dir / "codex_stderr.txt"
        artifacts = []

        if shutil.which("codex") is None:
            manifest = self._build_manifest(
                mode="placeholder",
                command=[],
                output_path=resolved_output,
                events_path=events_path,
                manifest_path=manifest_path,
                stderr_path=None,
                returncode=None,
                session_id=None,
                resumed_from_session_id=request.resume_session_id,
                session_id_source="none",
                thread_name=None,
                error_reason="codex_missing",
                can_resume=False,
            )
            write_json(manifest_path, manifest)
            artifacts.append(
                build_file_artifact(
                    kind="codex-session-manifest",
                    path=manifest_path,
                    media_type="application/json",
                    metadata={"role": "session_manifest"},
                )
            )
            output = CodexSearchSessionOutput.model_validate(manifest)
            return AdapterResult(
                status=WorkerStatus.FAILED,
                stdout="",
                stderr="Codex CLI is not available in PATH.",
                artifact_manifest=artifacts,
                structured_outputs=output.model_dump(mode="json"),
            )

        if request.resume_session_id is not None and not str(request.resume_session_id).strip():
            return self._missing_session_id_result(
                request=request,
                run_dir=run_dir,
                output_path=resolved_output,
                events_path=events_path,
                manifest_path=manifest_path,
            )

        pre_sessions = self._read_session_index()
        command = self._build_command(request, resolved_output)
        completed = subprocess.run(
            command,
            cwd=request.cwd or str(run_dir),
            capture_output=True,
            text=True,
            check=False,
        )
        write_text(events_path, completed.stdout)
        if completed.stderr.strip() or completed.returncode != 0:
            write_text(stderr_path, completed.stderr)
        post_sessions = self._read_session_index()

        session_id, session_id_source, thread_name = self._resolve_session_details(
            raw_events=completed.stdout,
            pre_sessions=pre_sessions,
            post_sessions=post_sessions,
        )

        error_reason: str | None = None
        if completed.returncode != 0:
            error_reason = "resume_failed" if request.resume_session_id else None

        manifest = self._build_manifest(
            mode="resume" if request.resume_session_id else "fresh",
            command=command,
            output_path=resolved_output,
            events_path=events_path,
            manifest_path=manifest_path,
            stderr_path=stderr_path if stderr_path.exists() else None,
            returncode=completed.returncode,
            session_id=session_id,
            resumed_from_session_id=request.resume_session_id,
            session_id_source=session_id_source,
            thread_name=thread_name,
            error_reason=error_reason,
            can_resume=session_id is not None,
        )
        write_json(manifest_path, manifest)

        if resolved_output.exists():
            artifacts.append(
                build_file_artifact(
                    kind="codex-output",
                    path=resolved_output,
                    media_type="application/json",
                    metadata={
                        "role": "codex_output",
                        "codex_session_id": session_id,
                        "command_shape": "codex --search exec",
                    },
                )
            )
        artifacts.append(
            build_file_artifact(
                kind="codex-events",
                path=events_path,
                media_type="text/plain",
                metadata={
                    "role": "codex_events",
                    "codex_session_id": session_id,
                    "command_shape": "codex --search exec",
                },
            )
        )
        artifacts.append(
            build_file_artifact(
                kind="codex-session-manifest",
                path=manifest_path,
                media_type="application/json",
                metadata={
                    "role": "session_manifest",
                    "codex_session_id": session_id,
                },
            )
        )
        if stderr_path.exists():
            artifacts.append(
                build_file_artifact(
                    kind="codex-stderr",
                    path=stderr_path,
                    media_type="text/plain",
                    metadata={
                        "role": "stderr",
                        "codex_session_id": session_id,
                    },
                )
            )

        output = CodexSearchSessionOutput.model_validate(manifest)
        return AdapterResult(
            status=WorkerStatus.COMPLETED if completed.returncode == 0 else WorkerStatus.FAILED,
            stdout=(
                f"Completed Codex search session ({output.mode})."
                if completed.returncode == 0
                else ""
            ),
            stderr=completed.stderr,
            artifact_manifest=artifacts,
            structured_outputs=output.model_dump(mode="json"),
        )

    def _missing_session_id_result(
        self,
        *,
        request: CodexSearchSessionRequest,
        run_dir: Path,
        output_path: Path,
        events_path: Path,
        manifest_path: Path,
    ) -> AdapterResult:
        del run_dir
        manifest = self._build_manifest(
            mode="resume",
            command=[],
            output_path=output_path,
            events_path=events_path,
            manifest_path=manifest_path,
            stderr_path=None,
            returncode=None,
            session_id=None,
            resumed_from_session_id=request.resume_session_id,
            session_id_source="none",
            thread_name=None,
            error_reason="missing_session_id",
            can_resume=False,
        )
        write_json(manifest_path, manifest)
        output = CodexSearchSessionOutput.model_validate(manifest)
        return AdapterResult(
            status=WorkerStatus.FAILED,
            stderr="Resume requested without a valid session id.",
            artifact_manifest=[
                build_file_artifact(
                    kind="codex-session-manifest",
                    path=manifest_path,
                    media_type="application/json",
                    metadata={"role": "session_manifest"},
                )
            ],
            structured_outputs=output.model_dump(mode="json"),
        )

    @staticmethod
    def _build_command(request: CodexSearchSessionRequest, output_path: Path) -> list[str]:
        base = ["codex"]
        if request.enable_search:
            base.append("--search")
        base.extend(["exec"])
        if request.resume_session_id:
            base.extend(["resume", request.resume_session_id])
        base.extend(["--json", "-o", str(output_path), *request.extra_args, request.prompt])
        return base

    @staticmethod
    def _build_manifest(
        *,
        mode: str,
        command: list[str],
        output_path: Path,
        events_path: Path,
        manifest_path: Path,
        stderr_path: Path | None,
        returncode: int | None,
        session_id: str | None,
        resumed_from_session_id: str | None,
        session_id_source: str,
        thread_name: str | None,
        error_reason: str | None,
        can_resume: bool,
    ) -> dict[str, Any]:
        return CodexSearchSessionOutput(
            returncode=returncode,
            mode=mode,
            session_id=session_id,
            resumed_from_session_id=resumed_from_session_id,
            session_id_source=session_id_source,
            thread_name=thread_name,
            output_path=str(output_path),
            events_path=str(events_path),
            manifest_path=str(manifest_path),
            stderr_path=str(stderr_path) if stderr_path is not None else None,
            command=command,
            error_reason=error_reason,
            can_resume=can_resume,
        ).model_dump(mode="json")

    def _resolve_session_details(
        self,
        *,
        raw_events: str,
        pre_sessions: dict[str, _SessionIndexEntry],
        post_sessions: dict[str, _SessionIndexEntry],
    ) -> tuple[str | None, str, str | None]:
        session_id = self._session_id_from_events(raw_events)
        thread_name = self._thread_name_from_events(raw_events)
        if session_id is not None:
            if thread_name is None:
                thread_name = post_sessions.get(session_id).thread_name if session_id in post_sessions else None
            return session_id, "json_events", thread_name

        diff_entry = self._session_index_diff(pre_sessions, post_sessions)
        if diff_entry is not None:
            return diff_entry.session_id, "session_index", diff_entry.thread_name
        return None, "none", None

    @classmethod
    def _session_id_from_events(cls, raw_events: str) -> str | None:
        for payload in cls._iter_event_payloads(raw_events):
            session_id = cls._find_string(payload, _SESSION_ID_KEYS)
            if session_id is not None:
                return session_id
        return None

    @classmethod
    def _thread_name_from_events(cls, raw_events: str) -> str | None:
        for payload in cls._iter_event_payloads(raw_events):
            thread_name = cls._find_string(payload, _THREAD_NAME_KEYS)
            if thread_name is not None:
                return thread_name
        return None

    @staticmethod
    def _iter_event_payloads(raw_events: str) -> list[Any]:
        payloads: list[Any] = []
        for line in raw_events.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payloads.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
        return payloads

    @classmethod
    def _find_string(cls, payload: Any, keys: tuple[str, ...]) -> str | None:
        if isinstance(payload, dict):
            for key in keys:
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    candidate = value.strip()
                    if key in _THREAD_NAME_KEYS:
                        return candidate
                    if _UUID_RE.match(candidate):
                        return candidate
            for value in payload.values():
                found = cls._find_string(value, keys)
                if found is not None:
                    return found
        elif isinstance(payload, list):
            for item in payload:
                found = cls._find_string(item, keys)
                if found is not None:
                    return found
        return None

    @classmethod
    def _read_session_index(cls) -> dict[str, _SessionIndexEntry]:
        if not _SESSION_INDEX_PATH.exists():
            return {}
        entries: dict[str, _SessionIndexEntry] = {}
        for line in _SESSION_INDEX_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            session_id = str(payload.get("id") or "").strip()
            if not _UUID_RE.match(session_id):
                continue
            entries[session_id] = _SessionIndexEntry(
                session_id=session_id,
                thread_name=str(payload.get("thread_name") or "").strip() or None,
                updated_at=str(payload.get("updated_at") or "").strip() or None,
            )
        return entries

    @staticmethod
    def _session_index_diff(
        pre_sessions: dict[str, _SessionIndexEntry],
        post_sessions: dict[str, _SessionIndexEntry],
    ) -> _SessionIndexEntry | None:
        candidates = [
            post_entry
            for session_id, post_entry in post_sessions.items()
            if session_id not in pre_sessions or pre_sessions[session_id].updated_at != post_entry.updated_at
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item.updated_at or "", item.session_id), reverse=True)
        return candidates[0]
