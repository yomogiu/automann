from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from prefect import flow
from pydantic import BaseModel, Field, model_validator

from libs.config import get_settings
from libs.contracts.models import AdapterResult, ObservationRecord, ReportRecord, WorkerStatus
from libs.retrieval import RetrievalService
from workers.codex_runner import CodexCliRequest, CodexCliRunner
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text

from .browser_job import browser_job_flow
from .common import build_repository, execute_adapter, execute_child_flow
from .registry import FLOW_SPECS


class SearchQuerySpec(BaseModel):
    label: str | None = None
    query: str
    limit: int = Field(default=5, ge=1, le=25)


class SearchPlanQuery(BaseModel):
    label: str
    query: str
    providers: list[str] = Field(default_factory=list)
    limit: int = Field(default=5, ge=1, le=25)
    rationale: str = ""


class SearchPlanPayload(BaseModel):
    summary_title: str | None = None
    summary_focus: str | None = None
    queries: list[SearchPlanQuery] = Field(default_factory=list)


class SearchReportRequestModel(BaseModel):
    title: str | None = None
    theme: str | None = None
    queries: list[SearchQuerySpec] = Field(default_factory=list)
    enabled_sources: list[str] = Field(default_factory=lambda: ["local_knowledge"])
    planner_enabled: bool = True
    prompt_path: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_theme_or_queries(self) -> SearchReportRequestModel:
        if not self.theme and not self.queries:
            raise ValueError("theme or queries must be provided")
        return self


def _resolve_prompt_text(prompt_path: str | None) -> str:
    if not prompt_path:
        return ""
    path = Path(prompt_path).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def _fallback_plan(request: SearchReportRequestModel) -> SearchPlanPayload:
    queries = list(request.queries)
    if not queries and request.theme:
        queries = [SearchQuerySpec(label="Theme", query=request.theme, limit=5)]
    enabled_sources = list(request.enabled_sources or ["local_knowledge"])
    return SearchPlanPayload(
        summary_title=request.title or request.theme or "Search Report",
        summary_focus=request.metadata.get("summary_focus") if isinstance(request.metadata, dict) else None,
        queries=[
            SearchPlanQuery(
                label=item.label or item.query[:48],
                query=item.query,
                providers=enabled_sources,
                limit=item.limit,
                rationale="Structured request fallback",
            )
            for item in queries
        ],
    )


def _build_planner_prompt(request: SearchReportRequestModel, prompt_text: str) -> str:
    payload = {
        "title": request.title,
        "theme": request.theme,
        "queries": [item.model_dump(mode="json") for item in request.queries],
        "enabled_sources": list(request.enabled_sources),
        "metadata": dict(request.metadata),
    }
    return "\n\n".join(
        [
            "You are planning a saved automation search report.",
            "Return strict JSON only.",
            "Choose providers only from the enabled_sources list.",
            "Each query item must include: label, query, providers, limit, rationale.",
            "Planner instructions:",
            prompt_text or "(none)",
            "Structured request:",
            json.dumps(payload, indent=2, sort_keys=True),
        ]
    )


def _run_search_planner(
    request: SearchReportRequestModel,
    *,
    run_dir: Path,
) -> SearchPlanPayload:
    if not request.planner_enabled:
        return _fallback_plan(request)

    settings = get_settings()
    prompt_text = _resolve_prompt_text(request.prompt_path)
    schema_path = run_dir / "search_plan.schema.json"
    output_path = run_dir / "search_plan.json"
    write_json(schema_path, SearchPlanPayload.model_json_schema())
    result = CodexCliRunner(settings).run(
        CodexCliRequest(
            prompt=_build_planner_prompt(request, prompt_text),
            cwd=str(run_dir),
            output_schema=str(schema_path),
            output_path=str(output_path),
        )
    )
    if result.status == WorkerStatus.COMPLETED and output_path.exists():
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            plan = SearchPlanPayload.model_validate(payload)
            if plan.queries:
                return plan
        except Exception:
            pass
    return _fallback_plan(request)


def _sanitize_plan(
    plan: SearchPlanPayload,
    *,
    enabled_sources: list[str],
) -> SearchPlanPayload:
    allowed = set(enabled_sources)
    queries: list[SearchPlanQuery] = []
    for item in plan.queries:
        providers = [provider for provider in item.providers if provider in allowed]
        if not providers:
            providers = list(enabled_sources)
        queries.append(
            SearchPlanQuery(
                label=item.label,
                query=item.query,
                providers=providers,
                limit=item.limit,
                rationale=item.rationale,
            )
        )
    return SearchPlanPayload(
        summary_title=plan.summary_title,
        summary_focus=plan.summary_focus,
        queries=queries,
    )


def _local_hits(query: SearchPlanQuery, retrieval: RetrievalService) -> list[dict[str, Any]]:
    rows = retrieval.query(query=query.query, limit=query.limit)
    return [
        {
            "provider": "local_knowledge",
            "query": query.query,
            "label": query.label,
            "text": item.get("text"),
            "artifact_id": item.get("artifact_id"),
            "chunk_id": item.get("id"),
            "metadata": item.get("metadata", {}),
        }
        for item in rows
    ]


def _browser_target_url(query: str, request: SearchReportRequestModel) -> str:
    template = str(request.metadata.get("browser_search_url_template") or "").strip()
    encoded = quote_plus(query)
    if "{query}" in template:
        return template.format(query=encoded)
    if template:
        return f"{template}{encoded}"
    return f"https://duckduckgo.com/?q={encoded}"


def _browser_hits(
    query: SearchPlanQuery,
    request: SearchReportRequestModel,
    *,
    parent_run_id: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    browser_request = {
        "job_name": f"search-{query.label.lower().replace(' ', '-')[:40]}",
        "target_url": _browser_target_url(query.query, request),
        "capture_html": True,
        "capture_screenshots": False,
        "metadata": {
            **dict(request.metadata),
            "search_query": query.query,
            "search_label": query.label,
        },
    }
    child = execute_child_flow(
        flow_name="browser_job_flow",
        worker_key=FLOW_SPECS["browser_job_flow"].default_work_pool,
        input_payload=browser_request,
        runner=lambda child_run_id: browser_job_flow.fn(request=browser_request, run_id=child_run_id),
        parent_run_id=parent_run_id,
    )
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    structured = result.get("structured_outputs") if isinstance(result.get("structured_outputs"), dict) else {}
    hits = [
        {
            "provider": "browser_web",
            "query": query.query,
            "label": query.label,
            "url": structured.get("final_url") or browser_request["target_url"],
            "title": structured.get("page_title") or structured.get("final_url") or browser_request["target_url"],
            "browser_run_id": child.get("run_id"),
            "artifact_kinds": structured.get("artifact_kinds", []),
        }
    ]
    return hits, [child]


def _render_search_markdown(
    *,
    title: str,
    plan: SearchPlanPayload,
    hits: list[dict[str, Any]],
) -> str:
    lines = [
        f"# {title}",
        "",
        "## Search Plan",
        f"- Queries: {len(plan.queries)}",
        f"- Summary focus: {plan.summary_focus or 'None'}",
        "",
        "## Results",
    ]
    for query in plan.queries:
        lines.append(f"### {query.label}")
        lines.append(f"- Query: {query.query}")
        lines.append(f"- Providers: {', '.join(query.providers)}")
        if query.rationale:
            lines.append(f"- Rationale: {query.rationale}")
        matched = [item for item in hits if item.get("label") == query.label]
        if not matched:
            lines.append("- No results")
        for item in matched:
            if item.get("provider") == "local_knowledge":
                text = str(item.get("text") or "").strip().replace("\n", " ")
                lines.append(f"- Local: {text[:160]}")
            else:
                lines.append(f"- Web: {item.get('title')} ({item.get('url')})")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _run_search_report(
    request: SearchReportRequestModel,
    *,
    parent_run_id: str | None,
) -> AdapterResult:
    settings = get_settings()
    repository = build_repository(settings)
    retrieval = RetrievalService(repository)
    run_dir = ensure_worker_dir(settings, "search_report_runner")

    plan = _sanitize_plan(
        _run_search_planner(request, run_dir=run_dir),
        enabled_sources=list(request.enabled_sources or ["local_knowledge"]),
    )

    collected_hits: list[dict[str, Any]] = []
    browser_runs: list[dict[str, Any]] = []
    for query in plan.queries:
        if "local_knowledge" in query.providers:
            collected_hits.extend(_local_hits(query, retrieval))
        if "browser_web" in query.providers:
            browser_hits, child_runs = _browser_hits(query, request, parent_run_id=parent_run_id)
            collected_hits.extend(browser_hits)
            browser_runs.extend(child_runs)

    title = plan.summary_title or request.title or request.theme or "Search Report"
    markdown = _render_search_markdown(title=title, plan=plan, hits=collected_hits)
    markdown_path = run_dir / "search_report.md"
    raw_results_path = run_dir / "search_results.json"
    manifest_path = run_dir / "search_manifest.json"
    write_text(markdown_path, markdown)
    write_json(raw_results_path, {"hits": collected_hits})
    manifest = {
        "title": title,
        "prompt_path": request.prompt_path,
        "planner_enabled": request.planner_enabled,
        "enabled_sources": list(request.enabled_sources),
        "queries": [item.model_dump(mode="json") for item in plan.queries],
        "browser_run_ids": [item.get("run_id") for item in browser_runs],
        "result_count": len(collected_hits),
    }
    write_json(manifest_path, manifest)

    observations = [
        ObservationRecord(
            kind="search_report_summary",
            summary=f"Collected {len(collected_hits)} search hits across {len(plan.queries)} queries.",
            payload=manifest,
            confidence=0.65,
        )
    ]
    reports = [
        ReportRecord(
            report_type="search_report",
            title=title,
            summary=f"{len(collected_hits)} search hits across {len(plan.queries)} planned queries.",
            content_markdown=markdown,
            artifact_path=str(markdown_path),
            metadata={
                **manifest,
                "taxonomy": {"filters": ["report"], "tags": ["search_report"]},
            },
        )
    ]
    return AdapterResult(
        status=WorkerStatus.COMPLETED,
        stdout=f"Generated search report with {len(collected_hits)} hits.",
        artifact_manifest=[
            build_file_artifact(kind="search-report", path=markdown_path, media_type="text/markdown"),
            build_file_artifact(kind="search-results", path=raw_results_path, media_type="application/json"),
            build_file_artifact(kind="search-manifest", path=manifest_path, media_type="application/json"),
        ],
        structured_outputs={
            **manifest,
            "plan": plan.model_dump(mode="json"),
            "hits": collected_hits,
            "browser_runs": browser_runs,
        },
        observations=observations,
        reports=reports,
    )


@flow(name="search-report")
def search_report_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    repository = build_repository()
    search_request = SearchReportRequestModel.model_validate(request)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="search_report_flow",
            worker_key="search_report_runner",
            input_payload=search_request.model_dump(mode="json"),
            runner=lambda: _run_search_report(search_request, parent_run_id=run_id),
            parent_run_id=run_id,
        )
        if run_id is not None:
            browser_runs = result.get("structured_outputs", {}).get("browser_runs", [])
            browser_artifacts = [
                artifact
                for item in browser_runs
                if isinstance(item, dict)
                for artifact in item.get("artifacts", [])
                if isinstance(artifact, dict)
            ]
            browser_observations = [
                observation
                for item in browser_runs
                if isinstance(item, dict)
                for observation in item.get("observations", [])
                if isinstance(observation, dict)
            ]
            repository.update_run_status(
                run_id,
                status=str(result.get("status") or WorkerStatus.COMPLETED.value),
                stdout=result.get("stdout") or "Completed search report",
                stderr=result.get("stderr") or "",
                structured_outputs=result,
                artifact_manifest=browser_artifacts + list(result.get("artifacts", [])),
                observation_summary=browser_observations + list(result.get("observations", [])),
                next_suggested_events=list(result.get("next_events", [])),
            )
        return result
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
