from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import (
    AdapterResult,
    ArtifactIngestRequest,
    ArtifactInputKind,
    BrowserCapture,
    BrowserJobRequest,
    ObservationRecord,
    ReportRecord,
    SearchSource,
    SearchReportCommandRequest,
    WorkerStatus,
)
from libs.db import LifeRepository, engine_for_url
from libs.retrieval import RetrievalService
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text

from .common import execute_adapter, execute_child_flow


FLOW_WORKER_KEY = "codex_search_report_runner"
PROMPT_PATH = Path(__file__).resolve().parents[1] / "libs" / "prompts" / "codex_search_report.md"
DEFAULT_ENABLED_SOURCES = ["local_knowledge"]
DEFAULT_MAX_RESULTS_PER_QUERY = 8


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


def _normalize_enabled_sources(value: Any) -> list[str]:
    if not isinstance(value, list):
        return list(DEFAULT_ENABLED_SOURCES)
    sources = [str(item).strip().lower() for item in value if str(item).strip()]
    deduped = list(dict.fromkeys(sources))
    return deduped or list(DEFAULT_ENABLED_SOURCES)


def _normalize_positive_int(value: Any, *, default: int, minimum: int = 1) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, parsed)


def _is_http_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _normalize_sources(value: Any, *, limit: int) -> dict[str, Any]:
    if not isinstance(value, list):
        return {
            "sources": [],
            "source_count": 0,
            "source_limit_hit": False,
            "invalid_source_count": 0,
        }
    sources: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    invalid_source_count = 0
    total_valid_unique = 0
    for item in value:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url or not _is_http_url(url):
            invalid_source_count += 1
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        total_valid_unique += 1
        if len(sources) < limit:
            sources.append(
                {
                    "title": str(item.get("title") or "Source"),
                    "url": url,
                    "note": str(item.get("note") or ""),
                }
            )
    return {
        "sources": sources,
        "source_count": len(sources),
        "source_limit_hit": total_valid_unique > len(sources),
        "invalid_source_count": invalid_source_count,
    }


def _merge_warning_codes(*items: Any) -> list[str]:
    warning_codes: list[str] = []
    for item in items:
        values: list[Any]
        if isinstance(item, list):
            values = item
        elif isinstance(item, dict):
            raw = item.get("warning_codes")
            values = raw if isinstance(raw, list) else []
        else:
            values = [item]
        for value in values:
            text = str(value).strip()
            if text:
                warning_codes.append(text)
    return list(dict.fromkeys(warning_codes))


def _split_request_context(request: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    command_payload = {"prompt": request.get("prompt")}
    for key in (
        "resume_from_run_id",
        "codex_session_id",
        "enabled_sources",
        "planner_enabled",
        "max_results_per_query",
    ):
        if key in request and request.get(key) is not None:
            command_payload[key] = request.get(key)
    search_context = {
        "theme": str(request.get("theme") or "").strip(),
        "queries": _normalize_string_list(request.get("queries")),
        "metadata": dict(request.get("metadata") or {}) if isinstance(request.get("metadata"), dict) else {},
        "enabled_sources": _normalize_enabled_sources(request.get("enabled_sources")),
        "planner_enabled": bool(request.get("planner_enabled", True)),
        "max_results_per_query": _normalize_positive_int(
            request.get("max_results_per_query"),
            default=DEFAULT_MAX_RESULTS_PER_QUERY,
        ),
    }
    return command_payload, search_context


def _build_retrieval_query(prompt: str, search_context: dict[str, Any]) -> str:
    parts = [prompt]
    theme = str(search_context.get("theme") or "").strip()
    if theme:
        parts.append(f"Theme: {theme}")
    queries = _normalize_string_list(search_context.get("queries"))
    if queries:
        parts.append("Queries:\n" + "\n".join(f"- {query}" for query in queries))
    return "\n\n".join(part for part in parts if part)


def _local_knowledge_enabled(enabled_sources: list[str]) -> bool:
    return "local_knowledge" in {str(item).strip().lower() for item in enabled_sources}


def _status_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _run_search_ingest_handoff(
    *,
    request: SearchReportCommandRequest,
    flow_run_id: str | None,
    source_items: list[dict[str, str]],
    enabled_sources: list[str],
) -> tuple[dict[str, Any], list[ObservationRecord]]:
    handoff = {
        "status": "skipped",
        "attempted_count": len(source_items),
        "ingested_count": 0,
        "failed_count": 0,
        "browser_fallback_attempted_count": 0,
        "browser_fallback_completed_count": 0,
        "browser_fallback_failed_count": 0,
        "direct_child_run_id": None,
        "browser_child_runs": [],
        "source_document_ids": [],
        "warning_codes": [],
    }
    observations: list[ObservationRecord] = []
    if not source_items:
        return handoff, observations

    from .artifact_ingest import artifact_ingest_flow
    from .browser_job import browser_job_flow

    ingest_request = ArtifactIngestRequest.model_validate(
        {
            "actor": request.actor,
            "session_id": request.session_id,
            "correlation_id": request.correlation_id,
            "items": [
                {
                    "input_kind": ArtifactInputKind.URL.value,
                    "url": item["url"],
                    "title": item["title"],
                    "tags": ["search-report", "discovered-source"],
                    "metadata": {
                        "source_title": item["title"],
                        "source_note": item["note"],
                        "discovered_by": "search_report",
                        "parent_run_id": flow_run_id,
                    },
                }
                for item in source_items
            ],
        }
    )

    direct_result: dict[str, Any] | None = None
    try:
        direct_result = execute_child_flow(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload=ingest_request.model_dump(mode="json"),
            runner=lambda child_run_id: artifact_ingest_flow.fn(
                request=ingest_request.model_dump(mode="json"),
                run_id=child_run_id,
            ),
            parent_run_id=flow_run_id,
        )
        handoff["direct_child_run_id"] = direct_result.get("run_id")
    except Exception as exc:
        handoff["status"] = "degraded"
        handoff["failed_count"] = len(source_items)
        handoff["warning_codes"] = _merge_warning_codes(["artifact_ingest_handoff_failed"])
        observations.append(
            ObservationRecord(
                kind="search_report_ingest_handoff_degraded",
                summary="Search report source ingest handoff failed before browser fallback.",
                payload={"error": str(exc), "attempted_count": len(source_items)},
                confidence=0.6,
            )
        )
        direct_result = None

    failed_items: list[dict[str, str]] = []
    if direct_result is not None:
        structured = dict(direct_result.get("structured_outputs") or {})
        result_items = structured.get("items") if isinstance(structured.get("items"), list) else []
        source_document_ids = []
        warnings: list[str] = []
        for index, item in enumerate(result_items):
            if not isinstance(item, dict):
                continue
            if _status_text(item.get("status")) == "completed":
                source_document_id = str(item.get("source_document_id") or "").strip()
                if source_document_id:
                    source_document_ids.append(source_document_id)
            else:
                if index < len(source_items):
                    failed_items.append(source_items[index])
            warnings.extend(_merge_warning_codes(item))

        handoff["status"] = "completed" if not failed_items else "degraded"
        handoff["ingested_count"] = int(structured.get("success_count") or len(source_document_ids))
        handoff["failed_count"] = len(failed_items)
        handoff["source_document_ids"] = list(dict.fromkeys(source_document_ids))
        handoff["warning_codes"] = _merge_warning_codes(warnings)
        observations.append(
            ObservationRecord(
                kind="search_report_ingest_handoff",
                summary=f"Ingested {handoff['ingested_count']} of {len(source_items)} discovered sources.",
                payload={
                    "attempted_count": len(source_items),
                    "ingested_count": handoff["ingested_count"],
                    "failed_count": handoff["failed_count"],
                    "direct_child_run_id": handoff["direct_child_run_id"],
                },
                confidence=0.7,
            )
        )
    else:
        failed_items = list(source_items)

    browser_enabled = SearchSource.BROWSER_WEB.value in {item.lower() for item in enabled_sources}
    if not browser_enabled or not failed_items:
        return handoff, observations

    for index, item in enumerate(failed_items, start=1):
        browser_request = BrowserJobRequest.model_validate(
            {
                "actor": request.actor,
                "session_id": request.session_id,
                "correlation_id": request.correlation_id,
                "job_name": f"search-report-source-{index}",
                "target_url": item["url"],
                "capture": BrowserCapture(html=False, screenshot=False, trace=False).model_dump(mode="json"),
                "capture_html": False,
                "capture_screenshots": False,
                "metadata": {
                    "source_title": item["title"],
                    "source_note": item["note"],
                    "discovered_by": "search_report_browser_fallback",
                    "parent_run_id": flow_run_id,
                },
            }
        )
        handoff["browser_fallback_attempted_count"] += 1
        try:
            browser_result = execute_child_flow(
                flow_name="browser_job_flow",
                worker_key="browser-process",
                input_payload=browser_request.model_dump(mode="json"),
                runner=lambda child_run_id, browser_request=browser_request: browser_job_flow.fn(
                    request=browser_request.model_dump(mode="json"),
                    run_id=child_run_id,
                ),
                parent_run_id=flow_run_id,
            )
            browser_status = _status_text(browser_result.get("status"))
            browser_outputs = dict(browser_result.get("structured_outputs") or {})
            ingest_outputs = dict(browser_outputs.get("ingest_handoff") or {})
            if not ingest_outputs:
                nested_outputs = browser_outputs.get("structured_outputs")
                if isinstance(nested_outputs, dict):
                    ingest_outputs = dict(nested_outputs.get("ingest_handoff") or {})
            handoff["browser_child_runs"].append(
                {
                    "url": item["url"],
                    "run_id": browser_result.get("run_id"),
                    "status": browser_status,
                    "ingest_handoff": ingest_outputs,
                }
            )
            if browser_status == WorkerStatus.COMPLETED.value:
                handoff["browser_fallback_completed_count"] += 1
                handoff["source_document_ids"] = list(
                    dict.fromkeys(
                        list(handoff["source_document_ids"])
                        + [str(value) for value in ingest_outputs.get("source_document_ids") or [] if str(value).strip()]
                    )
                )
            else:
                handoff["browser_fallback_failed_count"] += 1
                handoff["warning_codes"] = _merge_warning_codes(
                    handoff["warning_codes"],
                    ["browser_fallback_failed"],
                    ingest_outputs,
                )
        except Exception as exc:
            handoff["browser_fallback_failed_count"] += 1
            handoff["warning_codes"] = _merge_warning_codes(
                handoff["warning_codes"],
                ["browser_fallback_failed"],
            )
            observations.append(
                ObservationRecord(
                    kind="search_report_browser_fallback_failed",
                    summary=f"Browser fallback failed for {item['url']}.",
                    payload={"url": item["url"], "error": str(exc)},
                    confidence=0.55,
                )
            )

    if handoff["browser_fallback_attempted_count"]:
        observations.append(
            ObservationRecord(
                kind="search_report_browser_fallback",
                summary=(
                    f"Attempted browser fallback for {handoff['browser_fallback_attempted_count']} discovered "
                    "sources after direct ingest failures."
                ),
                payload={
                    "attempted_count": handoff["browser_fallback_attempted_count"],
                    "completed_count": handoff["browser_fallback_completed_count"],
                    "failed_count": handoff["browser_fallback_failed_count"],
                },
                confidence=0.65,
            )
        )
    remaining_failures = max(
        0,
        int(handoff["failed_count"]) - int(handoff["browser_fallback_completed_count"]),
    )
    if remaining_failures or handoff["browser_fallback_failed_count"]:
        handoff["status"] = "degraded"
    elif handoff["attempted_count"]:
        handoff["status"] = "completed"
    return handoff, observations


def _normalize_payload(
    payload: dict[str, Any],
    prompt: str,
    *,
    source_limit: int = DEFAULT_MAX_RESULTS_PER_QUERY,
) -> dict[str, Any]:
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

    source_payload = _normalize_sources(payload.get("sources"), limit=source_limit)
    return {
        "title": title,
        "summary": summary or resume_summary,
        "report_markdown": report_markdown,
        "resume_summary": resume_summary,
        "completed_work": completed_work,
        "open_questions": open_questions,
        "suggested_followup_prompt": suggested_followup_prompt,
        "needs_user_input": needs_user_input,
        "sources": source_payload["sources"],
        "source_count": source_payload["source_count"],
        "source_limit_hit": source_payload["source_limit_hit"],
        "invalid_source_count": source_payload["invalid_source_count"],
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
    search_context: dict[str, Any] | None,
    orchestration_context: dict[str, Any] | None,
) -> str:
    payload = {
        "user_prompt": prompt,
        "resume_context": resume_context or {},
        "local_retrieval_context": local_retrieval_context or [],
        "search_context": search_context or {},
        "orchestration": orchestration_context or {},
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
    search_context: dict[str, Any],
) -> AdapterResult:
    settings = get_settings()
    repository = LifeRepository(engine_for_url(settings.life_database_url))
    state = _load_prior_state(repository, request, flow_run_id=flow_run_id)
    resume_context = _resume_context(
        prior_outputs=state["prior_outputs"],
        prior_report_markdown=state["prior_report_markdown"],
    )
    enabled_sources = _normalize_enabled_sources(search_context.get("enabled_sources"))
    planner_enabled = bool(search_context.get("planner_enabled", True))
    max_results_per_query = _normalize_positive_int(
        search_context.get("max_results_per_query"),
        default=DEFAULT_MAX_RESULTS_PER_QUERY,
    )
    search_context_payload = {
        "theme": str(search_context.get("theme") or "").strip(),
        "queries": _normalize_string_list(search_context.get("queries")),
        "metadata": dict(search_context.get("metadata") or {}),
    }
    retrieval_query = _build_retrieval_query(request.prompt, search_context_payload)
    local_knowledge_enabled = _local_knowledge_enabled(enabled_sources)
    local_retrieval_context = (
        _local_retrieval_context(repository, prompt=retrieval_query)
        if local_knowledge_enabled
        else []
    )
    orchestration_context = {
        "enabled_sources": enabled_sources,
        "planner_enabled": planner_enabled,
        "max_results_per_query": max_results_per_query,
        "local_knowledge_enabled": local_knowledge_enabled,
        "theme": search_context_payload["theme"],
        "query_count": len(search_context_payload["queries"]),
        "local_retrieval_count": len(local_retrieval_context),
    }

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
                "ingest_handoff": {
                    "status": "skipped",
                    "attempted_count": 0,
                    "ingested_count": 0,
                    "failed_count": 0,
                    "browser_fallback_attempted_count": 0,
                    "browser_fallback_completed_count": 0,
                    "browser_fallback_failed_count": 0,
                    "direct_child_run_id": None,
                    "browser_child_runs": [],
                    "source_document_ids": [],
                    "warning_codes": [],
                },
                "orchestration": orchestration_context,
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
                    search_context=search_context_payload,
                    orchestration_context=orchestration_context,
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
                        search_context=search_context_payload,
                        orchestration_context=orchestration_context,
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
                    search_context=search_context_payload,
                    orchestration_context=orchestration_context,
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
                    search_context=search_context_payload,
                    orchestration_context=orchestration_context,
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
                    search_context=search_context_payload,
                    orchestration_context=orchestration_context,
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
                "ingest_handoff": {
                    "status": "skipped",
                    "attempted_count": 0,
                    "ingested_count": 0,
                    "failed_count": 0,
                    "browser_fallback_attempted_count": 0,
                    "browser_fallback_completed_count": 0,
                    "browser_fallback_failed_count": 0,
                    "direct_child_run_id": None,
                    "browser_child_runs": [],
                    "source_document_ids": [],
                    "warning_codes": [],
                },
                "orchestration": orchestration_context,
                "artifacts": {
                    "report_path": None,
                    "memo_path": None,
                    "manifest_path": manifest_path,
                    "events_path": events_path,
                },
            },
        )

    output_path = Path(output_path_text)
    payload = _normalize_payload(
        _parse_json_file(output_path),
        request.prompt,
        source_limit=max_results_per_query,
    )
    normalized_sources = {
        "sources": payload["sources"],
        "source_count": int(payload.get("source_count") or len(payload["sources"])),
        "source_limit_hit": bool(payload.get("source_limit_hit")),
        "invalid_source_count": int(payload.get("invalid_source_count") or 0),
    }
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
            "search_context": search_context_payload,
            "orchestration": orchestration_context,
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
    ingest_handoff, handoff_observations = _run_search_ingest_handoff(
        request=request,
        flow_run_id=flow_run_id,
        source_items=payload["sources"],
        enabled_sources=enabled_sources,
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
            "metadata": {
                "codex_session_id": session_id,
                "resume_source": resume_source,
                "source_count": normalized_sources["source_count"],
                "source_limit_hit": normalized_sources["source_limit_hit"],
                "invalid_source_count": normalized_sources["invalid_source_count"],
                "open_question_count": len(payload["open_questions"]),
                "suggested_followup_prompt": payload["suggested_followup_prompt"],
                "report_series_id": state["report_series_id"],
                "revision_number": state["revision_number"],
                "orchestration": {
                    **orchestration_context,
                    "source_count": normalized_sources["source_count"],
                    "source_limit_hit": normalized_sources["source_limit_hit"],
                    "invalid_source_count": normalized_sources["invalid_source_count"],
                    "direct_handoff_attempt_count": ingest_handoff["attempted_count"],
                    "browser_fallback_count": ingest_handoff["browser_fallback_attempted_count"],
                },
                "ingest_handoff": ingest_handoff,
                "taxonomy": {"filters": ["report"], "tags": ["search_report"]},
            },
        },
        "handoff": {
            "resume_summary": payload["resume_summary"],
            "completed_work": payload["completed_work"],
            "open_questions": payload["open_questions"],
            "suggested_followup_prompt": payload["suggested_followup_prompt"],
            "needs_user_input": payload["needs_user_input"],
            "sources": payload["sources"],
        },
        "ingest_handoff": ingest_handoff,
        "orchestration": {
            **orchestration_context,
            "source_count": normalized_sources["source_count"],
            "source_limit_hit": normalized_sources["source_limit_hit"],
            "invalid_source_count": normalized_sources["invalid_source_count"],
            "direct_handoff_attempt_count": ingest_handoff["attempted_count"],
            "browser_fallback_count": ingest_handoff["browser_fallback_attempted_count"],
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
            "source_count": normalized_sources["source_count"],
            "source_limit_hit": normalized_sources["source_limit_hit"],
            "invalid_source_count": normalized_sources["invalid_source_count"],
            "open_question_count": len(payload["open_questions"]),
            "suggested_followup_prompt": payload["suggested_followup_prompt"],
            "report_series_id": state["report_series_id"],
            "revision_number": state["revision_number"],
            "orchestration": {
                **orchestration_context,
                "source_count": normalized_sources["source_count"],
                "source_limit_hit": normalized_sources["source_limit_hit"],
                "invalid_source_count": normalized_sources["invalid_source_count"],
                "direct_handoff_attempt_count": ingest_handoff["attempted_count"],
                "browser_fallback_count": ingest_handoff["browser_fallback_attempted_count"],
            },
            "ingest_handoff": ingest_handoff,
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
                "source_count": normalized_sources["source_count"],
                "source_limit_hit": normalized_sources["source_limit_hit"],
                "invalid_source_count": normalized_sources["invalid_source_count"],
            },
            confidence=0.7,
        )
    ] + handoff_observations
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
    raw_request = dict(request)
    command_payload, search_context = _split_request_context(raw_request)
    search_request = SearchReportCommandRequest.model_validate(command_payload)
    search_context["enabled_sources"] = [
        item.value if hasattr(item, "value") else str(item)
        for item in search_request.enabled_sources
    ]
    search_context["planner_enabled"] = bool(search_request.planner_enabled)
    search_context["max_results_per_query"] = int(search_request.max_results_per_query)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="codex_search_report_flow",
            worker_key=FLOW_WORKER_KEY,
            input_payload=raw_request,
            runner=lambda: _run_codex_search_report(
                search_request,
                flow_run_id=run_id,
                search_context=search_context,
            ),
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
