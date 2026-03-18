from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import (
    AdapterResult,
    ObservationRecord,
    ReportRecord,
    SearchReportCommandRequest,
    WorkerStatus,
)
from libs.db import LifeRepository, engine_for_url
from libs.retrieval import RetrievalService
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text

from .common import execute_adapter


FLOW_WORKER_KEY = "codex_search_report_runner"
PROMPT_PATH = Path(__file__).resolve().parents[1] / "libs" / "prompts" / "codex_search_report.md"


def _load_prompt_template() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8").strip()


def _extract_json(raw_text: str) -> str:
    stripped = raw_text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return stripped
    return stripped[start : end + 1]


def _parse_json_file(path: Path) -> dict[str, Any]:
    raw_text = path.read_text(encoding="utf-8", errors="replace")
    payload = json.loads(_extract_json(raw_text))
    if not isinstance(payload, dict):
        raise ValueError("codex_search_report_payload_must_be_object")
    return payload


def _normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _normalize_sources(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    sources: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        sources.append(
            {
                "title": str(item.get("title") or "Source"),
                "url": str(item.get("url") or ""),
                "note": str(item.get("note") or ""),
            }
        )
    return sources


def _normalize_payload(payload: dict[str, Any], prompt: str) -> dict[str, Any]:
    title = str(payload.get("title") or f"Search Report: {prompt[:72]}")
    summary = str(payload.get("summary") or "").strip()
    report_markdown = str(payload.get("report_markdown") or "").strip()
    if not report_markdown:
        report_markdown = "\n".join(
            [
                f"# {title}",
                "",
                summary or "No summary provided.",
                "",
            ]
        )
    elif not report_markdown.lstrip().startswith("#"):
        report_markdown = f"# {title}\n\n{report_markdown}"

    completed_work = _normalize_string_list(payload.get("completed_work"))
    open_questions = _normalize_string_list(payload.get("open_questions"))
    suggested_followup_prompt = str(payload.get("suggested_followup_prompt") or "").strip()
    if not suggested_followup_prompt:
        suggested_followup_prompt = (
            "Answer the open questions and improve the report."
            if open_questions
            else "Expand the report with more detailed sourcing and refinements."
        )
    needs_user_input = bool(payload.get("needs_user_input")) or bool(open_questions)
    resume_summary = str(payload.get("resume_summary") or summary or "").strip()
    if not resume_summary:
        resume_summary = "Report generated. Continue by validating sources and refining the strongest claims."

    return {
        "title": title,
        "summary": summary or resume_summary,
        "report_markdown": report_markdown,
        "resume_summary": resume_summary,
        "completed_work": completed_work,
        "open_questions": open_questions,
        "suggested_followup_prompt": suggested_followup_prompt,
        "needs_user_input": needs_user_input,
        "sources": _normalize_sources(payload.get("sources")),
    }


def _artifact_path(
    artifacts: list[Any],
    *,
    role: str | None = None,
    kind_contains: str | None = None,
) -> str | None:
    for item in artifacts:
        if hasattr(item, "metadata"):
            metadata = getattr(item, "metadata")
            kind = getattr(item, "kind", "")
            path = getattr(item, "path", None)
        elif isinstance(item, dict):
            metadata = item.get("metadata", {})
            kind = str(item.get("kind") or "")
            path = item.get("path")
        else:
            continue
        metadata_role = str((metadata or {}).get("role") or "")
        if role and metadata_role == role:
            return str(path) if path else None
        if kind_contains and kind_contains in kind:
            return str(path) if path else None
    return None


def _load_session_runner(settings):  # noqa: ANN001
    try:
        from workers.codex_session_runner import CodexSearchSessionRequest, CodexSearchSessionRunner
    except ImportError as exc:  # pragma: no cover - Worker A owns this dependency
        raise RuntimeError("Codex session runner is unavailable in this checkout") from exc
    return CodexSearchSessionRunner(settings), CodexSearchSessionRequest


def _resume_context(
    *,
    prior_outputs: dict[str, Any],
    prior_report_markdown: str | None,
) -> dict[str, Any]:
    handoff = prior_outputs.get("handoff") if isinstance(prior_outputs, dict) else {}
    completed_work = _normalize_string_list((handoff or {}).get("completed_work"))
    open_questions = _normalize_string_list((handoff or {}).get("open_questions"))
    context = {
        "resume_summary": str((handoff or {}).get("resume_summary") or "").strip(),
        "completed_work": completed_work,
        "open_questions": open_questions,
        "suggested_followup_prompt": str((handoff or {}).get("suggested_followup_prompt") or "").strip(),
    }
    if prior_report_markdown:
        context["prior_report_markdown"] = prior_report_markdown[:8000]
    return {
        key: value
        for key, value in context.items()
        if value is not None and value != "" and value != []
    }


def _local_retrieval_context(
    repository: LifeRepository,
    *,
    prompt: str,
    limit: int = 6,
) -> list[dict[str, Any]]:
    retrieval = RetrievalService(repository)
    hits = retrieval.query(query=prompt, limit=limit)
    return [
        {
            "chunk_id": str(item.get("id") or ""),
            "artifact_id": str(item.get("artifact_id") or ""),
            "text": str(item.get("text") or "")[:800],
            "metadata": dict(item.get("metadata") or {}),
        }
        for item in hits
        if str(item.get("text") or "").strip()
    ]


def _render_prompt(
    *,
    prompt: str,
    resume_context: dict[str, Any] | None,
    local_retrieval_context: list[dict[str, Any]] | None,
) -> str:
    payload = {
        "user_prompt": prompt,
        "resume_context": resume_context or {},
        "local_retrieval_context": local_retrieval_context or [],
    }
    return "\n\n".join(
        [
            _load_prompt_template(),
            "Input payload:",
            json.dumps(payload, indent=2, sort_keys=True),
        ]
    )


def _load_prior_state(
    repository: LifeRepository,
    request: SearchReportCommandRequest,
    *,
    flow_run_id: str | None,
) -> dict[str, Any]:
    report_series_id = f"search-report:{flow_run_id or request.command_id}"
    revision_number = 1
    supersedes_report_id = None
    prior_run = None
    prior_outputs: dict[str, Any] = {}
    prior_report_markdown: str | None = None
    resume_session_id = request.codex_session_id

    if request.resume_from_run_id:
        prior_run = repository.get_run(request.resume_from_run_id)
        if prior_run is None:
            raise KeyError(request.resume_from_run_id)
        if prior_run.flow_name != "codex_search_report_flow":
            raise ValueError("resume_from_run_id must reference a codex_search_report_flow run")
        prior_outputs = dict(prior_run.structured_outputs or {})
        prior_report = prior_outputs.get("report") if isinstance(prior_outputs, dict) else {}
        report_series_id = str((prior_report or {}).get("report_series_id") or f"search-report:{prior_run.id}")
        current_revision = repository.current_report_revision(report_series_id)
        if current_revision is not None:
            revision_number = current_revision.revision_number + 1
            supersedes_report_id = current_revision.id
            prior_report_markdown = current_revision.content_markdown
        else:
            revision_number = int((prior_report or {}).get("revision_number") or 1) + 1
        codex_state = prior_outputs.get("codex") if isinstance(prior_outputs, dict) else {}
        resume_session_id = str((codex_state or {}).get("session_id") or "").strip() or None

    return {
        "report_series_id": report_series_id,
        "revision_number": revision_number,
        "supersedes_report_id": supersedes_report_id,
        "prior_run": prior_run,
        "prior_outputs": prior_outputs,
        "prior_report_markdown": prior_report_markdown,
        "resume_session_id": resume_session_id,
    }


def _run_codex_search_report(
    request: SearchReportCommandRequest,
    *,
    flow_run_id: str | None,
) -> AdapterResult:
    settings = get_settings()
    repository = LifeRepository(engine_for_url(settings.life_database_url))
    state = _load_prior_state(repository, request, flow_run_id=flow_run_id)
    resume_context = _resume_context(
        prior_outputs=state["prior_outputs"],
        prior_report_markdown=state["prior_report_markdown"],
    )
    local_retrieval_context = _local_retrieval_context(repository, prompt=request.prompt)

    try:
        session_runner, session_request_model = _load_session_runner(settings)
    except RuntimeError as exc:
        return AdapterResult(
            status=WorkerStatus.FAILED,
            stderr=str(exc),
            structured_outputs={
                "codex": {
                    "session_id": None,
                    "thread_name": None,
                    "resume_source": "fresh",
                    "resume_from_run_id": request.resume_from_run_id,
                    "cli_mode": "unavailable",
                },
                "report": {
                    "title": "",
                    "summary": "",
                    "report_series_id": state["report_series_id"],
                    "revision_number": state["revision_number"],
                    "supersedes_report_id": state["supersedes_report_id"],
                    "current_report_id": None,
                },
                "handoff": {
                    "resume_summary": "",
                    "completed_work": [],
                    "open_questions": [],
                    "suggested_followup_prompt": "",
                    "needs_user_input": True,
                    "sources": [],
                },
                "artifacts": {
                    "report_path": None,
                    "memo_path": None,
                    "manifest_path": None,
                    "events_path": None,
                },
            },
        )

    def make_session_request(prompt_text: str, *, resume_session_id: str | None = None):
        return session_request_model.model_validate(
            {
                "prompt": prompt_text,
                "cwd": str(Path.cwd()),
                "resume_session_id": resume_session_id,
                "enable_search": True,
                "output_path": "codex_last_message.json",
            }
        )

    resume_source = "fresh"
    session_result: AdapterResult | None = None
    if request.resume_from_run_id and state["resume_session_id"]:
        resume_source = "session_resume"
        session_result = session_runner.run(
            make_session_request(
                _render_prompt(
                    prompt=request.prompt,
                    resume_context=resume_context,
                    local_retrieval_context=local_retrieval_context,
                ),
                resume_session_id=state["resume_session_id"],
            )
        )
        session_outputs = dict(session_result.structured_outputs or {})
        if session_result.status != WorkerStatus.COMPLETED and request.resume_from_run_id:
            resume_source = "memo_fallback"
            session_result = session_runner.run(
                make_session_request(
                    _render_prompt(
                        prompt=request.prompt,
                        resume_context=resume_context,
                        local_retrieval_context=local_retrieval_context,
                    ),
                    resume_session_id=None,
                )
            )
    elif request.resume_from_run_id and not state["resume_session_id"]:
        resume_source = "memo_fallback"
        session_result = session_runner.run(
            make_session_request(
                _render_prompt(
                    prompt=request.prompt,
                    resume_context=resume_context,
                    local_retrieval_context=local_retrieval_context,
                ),
                resume_session_id=None,
            )
        )
    elif request.codex_session_id:
        resume_source = "session_resume"
        session_result = session_runner.run(
            make_session_request(
                _render_prompt(
                    prompt=request.prompt,
                    resume_context=resume_context,
                    local_retrieval_context=local_retrieval_context,
                ),
                resume_session_id=request.codex_session_id,
            )
        )
    else:
        session_result = session_runner.run(
            make_session_request(
                _render_prompt(
                    prompt=request.prompt,
                    resume_context=resume_context,
                    local_retrieval_context=local_retrieval_context,
                ),
                resume_session_id=None,
            )
        )

    assert session_result is not None
    session_outputs = dict(session_result.structured_outputs or {})
    session_id = str(session_outputs.get("session_id") or "").strip() or None
    thread_name = str(session_outputs.get("thread_name") or "").strip() or None
    events_path = str(session_outputs.get("events_path") or "").strip() or None
    output_path_text = str(session_outputs.get("output_path") or "").strip()
    manifest_path = _artifact_path(session_result.artifact_manifest, role="session_manifest", kind_contains="manifest")

    if session_result.status != WorkerStatus.COMPLETED or not output_path_text:
        return AdapterResult(
            status=session_result.status,
            stdout=session_result.stdout,
            stderr=session_result.stderr,
            artifact_manifest=list(session_result.artifact_manifest),
            structured_outputs={
                "codex": {
                    "session_id": session_id,
                    "thread_name": thread_name,
                    "resume_source": resume_source,
                    "resume_from_run_id": request.resume_from_run_id,
                    "cli_mode": str(session_outputs.get("mode") or ""),
                },
                "report": {
                    "title": "",
                    "summary": "",
                    "report_series_id": state["report_series_id"],
                    "revision_number": state["revision_number"],
                    "supersedes_report_id": state["supersedes_report_id"],
                    "current_report_id": None,
                },
                "handoff": {
                    "resume_summary": "",
                    "completed_work": [],
                    "open_questions": [],
                    "suggested_followup_prompt": "",
                    "needs_user_input": bool(request.resume_from_run_id),
                    "sources": [],
                },
                "artifacts": {
                    "report_path": None,
                    "memo_path": None,
                    "manifest_path": manifest_path,
                    "events_path": events_path,
                },
            },
        )

    output_path = Path(output_path_text)
    payload = _normalize_payload(_parse_json_file(output_path), request.prompt)
    run_dir = ensure_worker_dir(settings, FLOW_WORKER_KEY)
    report_path = run_dir / "search_report.md"
    memo_path = run_dir / "resume_memo.json"
    write_text(report_path, payload["report_markdown"])
    write_json(
        memo_path,
        {
            "title": payload["title"],
            "summary": payload["summary"],
            "resume_summary": payload["resume_summary"],
            "completed_work": payload["completed_work"],
            "open_questions": payload["open_questions"],
            "suggested_followup_prompt": payload["suggested_followup_prompt"],
            "needs_user_input": payload["needs_user_input"],
            "sources": payload["sources"],
            "prompt": request.prompt,
        },
    )

    artifacts = list(session_result.artifact_manifest)
    artifacts.append(
        build_file_artifact(
            kind="search-report",
            path=report_path,
            media_type="text/markdown",
            metadata={"role": "report_markdown", "codex_session_id": session_id},
        )
    )
    artifacts.append(
        build_file_artifact(
            kind="search-report-memo",
            path=memo_path,
            media_type="application/json",
            metadata={"role": "resume_memo", "codex_session_id": session_id},
        )
    )

    structured_outputs = {
        "codex": {
            "session_id": session_id,
            "thread_name": thread_name,
            "resume_source": resume_source,
            "resume_from_run_id": request.resume_from_run_id,
            "cli_mode": str(session_outputs.get("mode") or ""),
        },
        "report": {
            "title": payload["title"],
            "summary": payload["summary"],
            "report_series_id": state["report_series_id"],
            "revision_number": state["revision_number"],
            "supersedes_report_id": state["supersedes_report_id"],
            "current_report_id": None,
        },
        "handoff": {
            "resume_summary": payload["resume_summary"],
            "completed_work": payload["completed_work"],
            "open_questions": payload["open_questions"],
            "suggested_followup_prompt": payload["suggested_followup_prompt"],
            "needs_user_input": payload["needs_user_input"],
            "sources": payload["sources"],
        },
        "artifacts": {
            "report_path": str(report_path),
            "memo_path": str(memo_path),
            "manifest_path": manifest_path,
            "events_path": events_path,
        },
    }

    report = ReportRecord(
        report_type="search_report",
        title=payload["title"],
        summary=payload["summary"],
        content_markdown=payload["report_markdown"],
        artifact_path=str(report_path),
        report_series_id=state["report_series_id"],
        revision_number=state["revision_number"],
        supersedes_report_id=state["supersedes_report_id"],
        is_current=False,
        metadata={
            "codex_session_id": session_id,
            "resume_source": resume_source,
            "source_count": len(payload["sources"]),
            "open_question_count": len(payload["open_questions"]),
            "suggested_followup_prompt": payload["suggested_followup_prompt"],
            "report_series_id": state["report_series_id"],
            "revision_number": state["revision_number"],
            "taxonomy": {"filters": ["report"], "tags": ["search_report"]},
        },
    )

    observations = [
        ObservationRecord(
            kind="codex_search_report",
            summary=f"Generated search report revision {state['revision_number']} via {resume_source}.",
            payload={
                "report_series_id": state["report_series_id"],
                "revision_number": state["revision_number"],
                "resume_source": resume_source,
                "source_count": len(payload["sources"]),
            },
            confidence=0.7,
        )
    ]
    if payload["open_questions"]:
        observations.append(
            ObservationRecord(
                kind="codex_search_open_questions",
                summary=f"{len(payload['open_questions'])} open questions remain.",
                payload={"open_questions": payload["open_questions"]},
                confidence=0.65,
            )
        )

    stdout_lines = [f"Prepared search report revision {state['revision_number']}."]
    if session_result.stdout:
        stdout_lines.append(session_result.stdout.strip())

    return AdapterResult(
        status=WorkerStatus.COMPLETED,
        stdout="\n".join(line for line in stdout_lines if line),
        stderr=session_result.stderr,
        artifact_manifest=artifacts,
        structured_outputs=structured_outputs,
        observations=observations,
        reports=[report],
    )


@flow(name="search-report-manual")
def codex_search_report_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = LifeRepository(engine_for_url(settings.life_database_url))
    search_request = SearchReportCommandRequest.model_validate(request)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="codex_search_report_flow",
            worker_key=FLOW_WORKER_KEY,
            input_payload=search_request.model_dump(mode="json"),
            runner=lambda: _run_codex_search_report(search_request, flow_run_id=run_id),
            parent_run_id=run_id,
        )
        structured_outputs = dict(result.get("structured_outputs") or {})
        report_entries = result.get("reports", [])
        report_id = str(report_entries[0].get("id") or "") if report_entries else ""
        if report_id:
            promoted = repository.promote_report_revision(report_id)
            report_state = dict(structured_outputs.get("report") or {})
            report_state["current_report_id"] = promoted.id if promoted is not None else report_id
            structured_outputs["report"] = report_state

        final_status = str(result.get("status") or WorkerStatus.COMPLETED.value)
        if run_id is not None:
            repository.update_run_status(
                run_id,
                status=final_status,
                stdout=result.get("stdout") or "",
                stderr=result.get("stderr") or "",
                structured_outputs=structured_outputs,
                artifact_manifest=result.get("artifacts", []),
                observation_summary=result.get("observations", []),
                next_suggested_events=result.get("next_events", []),
            )
        return {
            "status": final_status,
            **structured_outputs,
        }
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
