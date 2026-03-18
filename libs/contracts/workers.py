from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from .models import BrowserCapture, BrowserExtract, BrowserJobRequest, BrowserSession, BrowserStep


class WorkerContract(BaseModel):
    model_config = {"extra": "forbid"}


class NewsIngestRequest(WorkerContract):
    brief_date: date
    seed_news: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class NewsIngestOutput(WorkerContract):
    generated_at: datetime
    items: list[dict[str, Any]] = Field(default_factory=list)
    count: int


class ArxivFeedIngestRequest(WorkerContract):
    brief_date: date
    seed_arxiv: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArxivFeedIngestOutput(WorkerContract):
    generated_at: datetime
    papers: list[dict[str, Any]] = Field(default_factory=list)
    count: int


class ArtifactIngestChunk(WorkerContract):
    ordinal: int
    text: str
    token_count: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactIngestItemRequest(WorkerContract):
    input_kind: Literal["url", "file", "inline"]
    url: str | None = None
    file_path: str | None = None
    content: str | None = None
    content_format: Literal["text", "markdown", "html"] | None = None
    declared_media_type: str | None = None
    title: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_item(self) -> ArtifactIngestItemRequest:
        if self.input_kind == "url":
            if not self.url:
                raise ValueError("url is required when input_kind=url")
        elif self.input_kind == "file":
            if not self.file_path:
                raise ValueError("file_path is required when input_kind=file")
        elif self.input_kind == "inline":
            if self.content is None:
                raise ValueError("content is required when input_kind=inline")
            if self.content_format is None:
                raise ValueError("content_format is required when input_kind=inline")
        return self


class ArtifactIngestRequest(WorkerContract):
    items: list[ArtifactIngestItemRequest] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_items(self) -> ArtifactIngestRequest:
        if not self.items:
            raise ValueError("items is required")
        return self


class ArtifactIngestItemOutput(WorkerContract):
    input_index: int
    input_kind: Literal["url", "file", "inline"]
    status: Literal["completed", "failed", "unsupported"]
    canonical_uri: str | None = None
    source_type: str | None = None
    source_document_id: str | None = None
    raw_artifact_path: str | None = None
    normalized_text_artifact_path: str | None = None
    ingest_manifest_artifact_path: str | None = None
    chunk_count: int = 0
    warning_codes: list[str] = Field(default_factory=list)
    error: str | None = None
    title: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    chunks: list[ArtifactIngestChunk] = Field(default_factory=list)


class ArtifactIngestOutput(WorkerContract):
    generated_at: datetime
    input_count: int
    success_count: int
    failure_count: int
    warning_count: int
    items: list[ArtifactIngestItemOutput] = Field(default_factory=list)


class BrowserTaskRequest(WorkerContract):
    job_name: str
    target_url: str
    session: BrowserSession = Field(default_factory=BrowserSession)
    headless: bool = True
    capture: BrowserCapture = Field(default_factory=BrowserCapture)
    steps: list[BrowserStep] = Field(default_factory=list)
    extract: list[BrowserExtract] = Field(default_factory=list)
    timeout_seconds: int = Field(default=30, ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_command(cls, request: BrowserJobRequest) -> BrowserTaskRequest:
        return cls(
            job_name=request.job_name,
            target_url=request.target_url,
            session=request.session,
            headless=request.headless,
            capture=request.capture,
            steps=request.steps,
            extract=request.extract,
            timeout_seconds=request.timeout_seconds,
            metadata=request.metadata,
        )


class BrowserTaskOutput(WorkerContract):
    generated_at: datetime
    job_name: str
    target_url: str
    final_url: str | None = None
    page_title: str | None = None
    session_mode: str
    execution_mode: str
    status: str
    extracted_data: dict[str, Any] = Field(default_factory=dict)
    artifact_kinds: list[str] = Field(default_factory=list)
    step_count: int = 0
    extraction_count: int = 0
    profile_name: str | None = None


class DailyBriefAnalysisRequest(WorkerContract):
    brief_date: date
    news_items: list[dict[str, Any]] = Field(default_factory=list)
    papers: list[dict[str, Any]] = Field(default_factory=list)
    browser_summary: dict[str, Any] | None = None
    previous_report: dict[str, Any] | None = None


class DailyBriefAnalysisOutput(WorkerContract):
    brief_date: str
    news_count: int
    paper_count: int
    browser_status: str | None = None
    previous_report_title: str


class DraftGenerationRequest(WorkerContract):
    theme: str
    evidence_pack: list[dict[str, Any]] = Field(default_factory=list)
    source_report_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DraftGenerationOutput(WorkerContract):
    theme: str
    evidence_count: int
    source_report_ids: list[str] = Field(default_factory=list)


class ResearchReportWorkerRequest(WorkerContract):
    theme: str
    boundaries: list[str] = Field(default_factory=list)
    areas_of_interest: list[str] = Field(default_factory=list)
    report_key: str
    edit_mode: str
    revision_number: int = 1
    supersedes_report_id: str | None = None
    previous_report: dict[str, Any] | None = None
    retrieval_context: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchReportWorkerOutput(WorkerContract):
    report_key: str
    title: str
    summary: str
    findings: list[dict[str, Any]] = Field(default_factory=list)
    section_updates: list[dict[str, Any]] = Field(default_factory=list)
    tables: list[dict[str, Any]] = Field(default_factory=list)
    citations: list[dict[str, Any]] = Field(default_factory=list)


class PublicationGenerationRequest(WorkerContract):
    source_report_id: str | None = None
    source_revision_id: str | None = None
    theme: str | None = None
    source_markdown: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class PublicationGenerationOutput(WorkerContract):
    title: str
    source_report_id: str | None = None
    source_revision_id: str | None = None
    mode: str


class PublishRequest(WorkerContract):
    report_path: str
    artifact_paths: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PublishOutput(WorkerContract):
    publish_mode: str
    release_tag: str
    bundle_dir: str | None = None
    manifest_path: str | None = None


class CodexTaskRequest(WorkerContract):
    prompt: str
    cwd: str | None = None
    extra_args: list[str] = Field(default_factory=list)
    output_schema: str | None = None
    output_path: str | None = None

    @property
    def structured_mode(self) -> bool:
        return self.output_schema is not None or self.output_path is not None

    @model_validator(mode="after")
    def validate_structured_mode(self) -> CodexTaskRequest:
        if (self.output_schema is None) != (self.output_path is None):
            raise ValueError("output_schema and output_path must be provided together for structured mode")
        return self


class CodexTaskOutput(WorkerContract):
    returncode: int | None = None
    mode: str
    output_schema: str | None = None
    output_path: str | None = None
    command: list[str] = Field(default_factory=list)


class CodexSearchSessionRequest(WorkerContract):
    prompt: str
    cwd: str | None = None
    resume_session_id: str | None = None
    enable_search: bool = True
    output_path: str | None = None
    extra_args: list[str] = Field(default_factory=list)


class CodexSearchSessionOutput(WorkerContract):
    returncode: int | None = None
    mode: str
    session_id: str | None = None
    resumed_from_session_id: str | None = None
    session_id_source: str = "none"
    thread_name: str | None = None
    output_path: str | None = None
    events_path: str | None = None
    manifest_path: str | None = None
    stderr_path: str | None = None
    command: list[str] = Field(default_factory=list)
    error_reason: str | None = None
    can_resume: bool = False
