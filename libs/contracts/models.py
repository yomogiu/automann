from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


SCHEMA_VERSION = "2026-03-16"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def uuid_str() -> str:
    return str(uuid4())


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
    headless: bool = True
    capture_html: bool = True
    capture_screenshots: bool = True
    capture: BrowserCapture = Field(default_factory=BrowserCapture)
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)


class DraftArticleRequest(CommandEnvelope):
    theme: str
    report_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    human_policy: HumanPolicy = Field(default_factory=HumanPolicy)


class QueryKnowledgeRequest(CommandEnvelope):
    query: str
    limit: int = 10
    include_semantic: bool = True
    include_lexical: bool = True


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
