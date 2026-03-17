from __future__ import annotations

import ipaddress
from datetime import datetime, timezone
from enum import StrEnum
import re
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


SCHEMA_VERSION = "2026-03-16"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def uuid_str() -> str:
    return str(uuid4())


PROFILE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
WAIT_FOR_SELECTOR_STATES = {"attached", "detached", "hidden", "visible"}
WAIT_FOR_LOAD_STATES = {"domcontentloaded", "load", "networkidle"}


def _is_loopback_host(hostname: str | None) -> bool:
    if hostname is None:
        return False
    if hostname == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _is_loopback_endpoint(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https", "ws", "wss"}:
        return False
    return _is_loopback_host(parsed.hostname)


class StrictModel(BaseModel):
    model_config = {"extra": "forbid"}


class WorkerStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class InteractionStatus(StrEnum):
    OPEN = "open"
    ANSWERED = "answered"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class HumanMode(StrEnum):
    AUTO = "auto"
    CHECKPOINTED = "checkpointed"
    LIVE = "live"


class ResearchEditMode(StrEnum):
    MERGE = "merge"
    APPEND = "append"
    REFRESH_SECTION = "refresh_section"


class AutomationType(StrEnum):
    DAILY_BRIEF = "daily_brief"
    PAPER_BATCH = "paper_batch"
    SEARCH_REPORT = "search_report"


class AutomationStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class SearchSource(StrEnum):
    LOCAL_KNOWLEDGE = "local_knowledge"
    BROWSER_WEB = "browser_web"


class EventSuggestion(StrictModel):
    name: str
    payload: dict[str, Any] = Field(default_factory=dict)


class ArtifactRecord(StrictModel):
    kind: str
    path: str
    storage_uri: str
    size_bytes: int
    media_type: str | None = None
    sha256: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ObservationRecord(StrictModel):
    kind: str
    summary: str
    payload: dict[str, Any] = Field(default_factory=dict)
    confidence: float | None = None


class ReportRecord(StrictModel):
    report_type: str
    title: str
    summary: str = ""
    content_markdown: str = ""
    artifact_path: str | None = None
    report_series_id: str | None = None
    revision_number: int | None = None
    supersedes_report_id: str | None = None
    is_current: bool | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AdapterResult(StrictModel):
    status: WorkerStatus
    stdout: str = ""
    stderr: str = ""
    artifact_manifest: list[ArtifactRecord] = Field(default_factory=list)
    structured_outputs: dict[str, Any] = Field(default_factory=dict)
    observations: list[ObservationRecord] = Field(default_factory=list)
    reports: list[ReportRecord] = Field(default_factory=list)
    next_suggested_events: list[EventSuggestion] = Field(default_factory=list)


class HumanPolicy(StrictModel):
    mode: HumanMode = HumanMode.CHECKPOINTED
    checkpoints: list[str] = Field(default_factory=list)
    timeout_seconds: int | None = None


class BrowserCapture(StrictModel):
    html: bool = True
    screenshot: bool = True
    trace: bool = True


class BrowserSessionMode(StrEnum):
    LAUNCH = "launch"
    ATTACH = "attach"


class BrowserStepOp(StrEnum):
    WAIT_FOR = "wait_for"
    CLICK = "click"
    FILL = "fill"
    PRESS = "press"
    SCROLL = "scroll"
    SCREENSHOT = "screenshot"


class BrowserExtractKind(StrEnum):
    TEXT = "text"
    HTML = "html"
    LINKS = "links"
    ATTRIBUTE = "attribute"


class BrowserSession(StrictModel):
    mode: BrowserSessionMode = BrowserSessionMode.LAUNCH
    cdp_url: str | None = None
    profile_name: str | None = None

    @model_validator(mode="after")
    def validate_session(self) -> BrowserSession:
        if self.cdp_url is not None:
            if not _is_loopback_endpoint(self.cdp_url):
                raise ValueError("cdp_url must target a loopback browser endpoint")
            if self.mode != BrowserSessionMode.ATTACH:
                raise ValueError("cdp_url is only valid in attach mode")
        if self.profile_name is not None:
            if PROFILE_NAME_PATTERN.fullmatch(self.profile_name) is None:
                raise ValueError("profile_name must be a logical label, not a filesystem path")
            if self.mode != BrowserSessionMode.ATTACH:
                raise ValueError("profile_name is only valid in attach mode")
        return self


class BrowserStep(StrictModel):
    op: BrowserStepOp
    selector: str | None = None
    value: str | None = None
    key: str | None = None
    pixels: int = Field(default=800, ge=1)
    name: str | None = None
    path: str | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)
    state: str = "visible"
    wait_until: str | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_scroll_pixels(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        if "pixels" not in payload and "amount" in payload:
            payload["pixels"] = payload.pop("amount")
        return payload

    @model_validator(mode="after")
    def validate_step(self) -> BrowserStep:
        if self.op == BrowserStepOp.WAIT_FOR:
            if bool(self.selector) == bool(self.wait_until):
                raise ValueError("wait_for requires exactly one of selector or wait_until")
            if self.selector is not None and self.state not in WAIT_FOR_SELECTOR_STATES:
                raise ValueError("wait_for selector state must be one of attached, detached, hidden, visible")
            if self.wait_until is not None and self.wait_until not in WAIT_FOR_LOAD_STATES:
                raise ValueError("wait_for wait_until must be one of domcontentloaded, load, networkidle")
        elif self.op == BrowserStepOp.CLICK:
            if not self.selector:
                raise ValueError("click requires selector")
        elif self.op == BrowserStepOp.FILL:
            if not self.selector or self.value is None:
                raise ValueError("fill requires selector and value")
        elif self.op == BrowserStepOp.PRESS:
            if not self.key:
                raise ValueError("press requires key")
        elif self.op == BrowserStepOp.SCREENSHOT:
            if self.path is not None and (self.path.startswith("/") or self.path.startswith("~") or "\\" in self.path):
                raise ValueError("screenshot path must be relative to the run directory")
        return self


class BrowserExtract(StrictModel):
    name: str
    selector: str
    kind: BrowserExtractKind
    all: bool = False
    attribute: str | None = None

    @model_validator(mode="after")
    def validate_attribute_extract(self) -> BrowserExtract:
        if self.kind == BrowserExtractKind.ATTRIBUTE and not self.attribute:
            raise ValueError("attribute is required when kind=attribute")
        if self.kind != BrowserExtractKind.ATTRIBUTE and self.attribute is not None:
            raise ValueError("attribute is only valid when kind=attribute")
        return self


class CommandEnvelope(StrictModel):
    schema_version: str = SCHEMA_VERSION
    command_id: str = Field(default_factory=uuid_str)
    session_id: str = Field(default_factory=uuid_str)
    correlation_id: str = Field(default_factory=uuid_str)
    idempotency_key: str = Field(default_factory=uuid_str)
    actor: str = "user"
    created_at: datetime = Field(default_factory=utcnow)


class DailyBriefRequest(CommandEnvelope):
    date: datetime | None = None
    include_news: bool = True
    include_arxiv: bool = True
    include_browser_jobs: bool = True
    publish: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)


class PaperReviewRequest(CommandEnvelope):
    paper_id: str
    source_url: str | None = None
    title: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)


class BrowserJobRequest(CommandEnvelope):
    job_name: str
    target_url: str
    login_profile: str | None = None
    session: BrowserSession = Field(default_factory=BrowserSession)
    headless: bool = True
    timeout_seconds: int = Field(default=30, ge=1)
    steps: list[BrowserStep] = Field(default_factory=list)
    extract: list[BrowserExtract] = Field(default_factory=list)
    capture_html: bool = True
    capture_screenshots: bool = True
    capture: BrowserCapture = Field(default_factory=BrowserCapture)
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)

    @model_validator(mode="before")
    @classmethod
    def normalize_browser_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        payload = dict(data)

        capture_value = payload.get("capture")
        if isinstance(capture_value, BrowserCapture):
            capture_payload = capture_value.model_dump(mode="json")
        else:
            capture_payload = dict(capture_value or {})
        if "html" not in capture_payload and "capture_html" in payload:
            capture_payload["html"] = bool(payload["capture_html"])
        if "screenshot" not in capture_payload and "capture_screenshots" in payload:
            capture_payload["screenshot"] = bool(payload["capture_screenshots"])
        if capture_payload:
            payload["capture"] = capture_payload

        session_value = payload.get("session")
        if isinstance(session_value, BrowserSession):
            session_payload = session_value.model_dump(mode="json", exclude_none=True)
        else:
            session_payload = dict(session_value or {})
        if payload.get("login_profile") and "profile_name" not in session_payload:
            session_payload["profile_name"] = payload["login_profile"]
        if session_payload and "mode" not in session_payload:
            if session_payload.get("cdp_url") or session_payload.get("profile_name"):
                session_payload["mode"] = BrowserSessionMode.ATTACH.value
            else:
                session_payload["mode"] = BrowserSessionMode.LAUNCH.value
        if session_payload:
            payload["session"] = session_payload

        return payload

    @model_validator(mode="after")
    def align_legacy_login_profile(self) -> BrowserJobRequest:
        if self.login_profile and not self.session.profile_name:
            self.session = self.session.model_copy(update={"profile_name": self.login_profile})
        return self


class DraftArticleRequest(CommandEnvelope):
    theme: str | None = None
    report_ids: list[str] = Field(default_factory=list)
    source_report_id: str | None = None
    source_revision_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)

    @model_validator(mode="after")
    def validate_source_or_theme(self) -> DraftArticleRequest:
        if self.source_report_id and self.source_revision_id:
            raise ValueError("source_report_id and source_revision_id are mutually exclusive")
        if self.source_report_id or self.source_revision_id:
            return self
        if not self.theme:
            raise ValueError("theme is required when no source report is provided")
        return self


class ResearchReportRequest(CommandEnvelope):
    theme: str
    boundaries: list[str] = Field(default_factory=list)
    areas_of_interest: list[str] = Field(default_factory=list)
    report_key: str | None = None
    edit_mode: ResearchEditMode = ResearchEditMode.MERGE
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)


class SearchReportCommandRequest(CommandEnvelope):
    prompt: str
    resume_from_run_id: str | None = None
    codex_session_id: str | None = None

    @model_validator(mode="after")
    def validate_prompt(self) -> SearchReportCommandRequest:
        self.prompt = self.prompt.strip()
        if not self.prompt:
            raise ValueError("prompt is required")
        self.resume_from_run_id = str(self.resume_from_run_id or "").strip() or None
        self.codex_session_id = str(self.codex_session_id or "").strip() or None
        return self


class QueryKnowledgeRequest(CommandEnvelope):
    query: str
    limit: int = 10
    include_semantic: bool = True
    include_lexical: bool = True


class DailyBriefAutomationPayload(StrictModel):
    include_news: bool = True
    include_arxiv: bool = True
    include_browser_jobs: bool = True
    publish: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)


class PaperBatchItem(StrictModel):
    paper_id: str
    source_url: str | None = None
    title: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PaperBatchAutomationPayload(StrictModel):
    papers: list[PaperBatchItem] = Field(default_factory=list)
    compare_prompt: str = "Compare the papers, highlight agreements and disagreements, and summarize the practical implications."
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)

    @model_validator(mode="after")
    def validate_papers(self) -> PaperBatchAutomationPayload:
        if not self.papers:
            raise ValueError("papers is required")
        return self


class SearchReportAutomationPayload(StrictModel):
    theme: str
    queries: list[str] = Field(default_factory=list)
    enabled_sources: list[SearchSource] = Field(default_factory=lambda: [SearchSource.LOCAL_KNOWLEDGE])
    planner_enabled: bool = True
    max_results_per_query: int = Field(default=8, ge=1, le=50)
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)

    @model_validator(mode="after")
    def validate_search_payload(self) -> SearchReportAutomationPayload:
        if not self.queries:
            raise ValueError("queries is required")
        if not self.enabled_sources:
            raise ValueError("enabled_sources is required")
        return self


class RunSubmitRequest(StrictModel):
    flow_name: str
    work_pool: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    idempotency_key: str | None = None


class RunSummary(StrictModel):
    id: str
    flow_name: str
    status: str
    created_at: datetime
    updated_at: datetime
    task_spec_id: str | None = None
    parent_run_id: str | None = None
    worker_key: str | None = None
    prefect_flow_run_id: str | None = None
    output_summary: dict[str, Any] = Field(default_factory=dict)


class AutomationPrefectStatus(StrictModel):
    deployment_id: str | None = None
    deployment_name: str | None = None
    deployment_path: str | None = None
    deployment_url: str | None = None
    status: str = "unknown"
    paused: bool | None = None
    next_run_at: datetime | None = None


class AutomationCreateRequest(StrictModel):
    task_key: str | None = None
    automation_type: AutomationType
    title: str
    description: str | None = None
    schedule_text: str | None = None
    timezone: str | None = None
    work_pool: str | None = None
    status: AutomationStatus = AutomationStatus.ACTIVE
    payload: dict[str, Any] = Field(default_factory=dict)
    prompt_path: str | None = None
    prompt_body: str | None = None

    @model_validator(mode="after")
    def validate_status(self) -> AutomationCreateRequest:
        if self.status == AutomationStatus.ARCHIVED:
            raise ValueError("new automations cannot be created archived")
        return self


class AutomationUpdateRequest(StrictModel):
    title: str | None = None
    description: str | None = None
    schedule_text: str | None = None
    timezone: str | None = None
    work_pool: str | None = None
    status: AutomationStatus | None = None
    payload: dict[str, Any] | None = None
    prompt_path: str | None = None
    prompt_body: str | None = None


class AutomationRunRequest(StrictModel):
    payload_overrides: dict[str, Any] = Field(default_factory=dict)


class AutomationSummary(StrictModel):
    id: str
    task_key: str
    automation_type: AutomationType
    flow_name: str
    title: str
    description: str | None = None
    schedule_text: str | None = None
    timezone: str | None = None
    work_pool: str | None = None
    status: AutomationStatus
    prompt_path: str | None = None
    prefect: AutomationPrefectStatus = Field(default_factory=AutomationPrefectStatus)
    latest_run: RunSummary | None = None


class AutomationDetail(AutomationSummary):
    payload: dict[str, Any] = Field(default_factory=dict)
    prompt_body: str | None = None


class InteractionCreateRequest(CommandEnvelope):
    run_id: str
    title: str
    prompt_md: str
    input_schema: dict[str, Any]
    default_input: dict[str, Any] | None = None
    checkpoint_key: str | None = None
    prefect_flow_run_id: str | None = None
    ui_hints: dict[str, Any] = Field(default_factory=dict)


class InteractionAnswerRequest(StrictModel):
    response: dict[str, Any]


class InteractionSummary(StrictModel):
    id: str
    run_id: str
    status: InteractionStatus
    title: str
    prompt_md: str
    input_schema: dict[str, Any]
    default_input: dict[str, Any] | None = None
    response_payload: dict[str, Any] | None = None
    checkpoint_key: str | None = None
    prefect_flow_run_id: str | None = None
    ui_hints: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
