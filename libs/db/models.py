from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, Integer, Numeric, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def _uuid_pk() -> Mapped[str]:
    return mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class TaskSpec(TimestampMixin, Base):
    __tablename__ = "task_spec"

    id: Mapped[str] = _uuid_pk()
    task_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    schedule_text: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(50), default="active", nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    runs: Mapped[list["RunRecord"]] = relationship(back_populates="task_spec")
    artifacts: Mapped[list["Artifact"]] = relationship(back_populates="task_spec")


class RunRecord(TimestampMixin, Base):
    __tablename__ = "run"
    __table_args__ = (
        Index("ix_run_created_at", "created_at"),
        Index("ix_run_parent_run_id", "parent_run_id"),
        Index("ix_run_session_idempotency", "session_id", "idempotency_key"),
    )

    id: Mapped[str] = _uuid_pk()
    task_spec_id: Mapped[str | None] = mapped_column(ForeignKey("task_spec.id"))
    parent_run_id: Mapped[str | None] = mapped_column(ForeignKey("run.id"))
    flow_name: Mapped[str] = mapped_column(String(255), nullable=False)
    worker_key: Mapped[str] = mapped_column(String(255), nullable=False)
    prefect_flow_run_id: Mapped[str | None] = mapped_column(String(255), unique=True)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)
    schema_version: Mapped[str] = mapped_column(String(32), default="2026-03-16", nullable=False)
    command_id: Mapped[str | None] = mapped_column(String(64))
    session_id: Mapped[str | None] = mapped_column(String(64))
    correlation_id: Mapped[str | None] = mapped_column(String(64))
    idempotency_key: Mapped[str | None] = mapped_column(String(255))
    actor: Mapped[str] = mapped_column(String(32), default="user", nullable=False)
    input_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    structured_outputs: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    artifact_manifest: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    observation_summary: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    next_suggested_events: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    stdout: Mapped[str] = mapped_column(Text, default="", nullable=False)
    stderr: Mapped[str] = mapped_column(Text, default="", nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    task_spec: Mapped[TaskSpec | None] = relationship(back_populates="runs")
    artifacts: Mapped[list["Artifact"]] = relationship(back_populates="run")
    observations: Mapped[list["Observation"]] = relationship(back_populates="run")
    reports: Mapped[list["Report"]] = relationship(back_populates="run")


class Artifact(Base):
    __tablename__ = "artifact"
    __table_args__ = (Index("ix_artifact_created_at", "created_at"),)

    id: Mapped[str] = _uuid_pk()
    task_spec_id: Mapped[str | None] = mapped_column(ForeignKey("task_spec.id"))
    run_id: Mapped[str | None] = mapped_column(ForeignKey("run.id"))
    kind: Mapped[str] = mapped_column(String(100), nullable=False)
    path: Mapped[str | None] = mapped_column(String(1024))
    storage_uri: Mapped[str] = mapped_column(String(2048), nullable=False, unique=True)
    storage_backend: Mapped[str] = mapped_column(String(100), default="local", nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    media_type: Mapped[str | None] = mapped_column(String(255))
    sha256: Mapped[str | None] = mapped_column(String(64))
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    task_spec: Mapped[TaskSpec | None] = relationship(back_populates="artifacts")
    run: Mapped[RunRecord | None] = relationship(back_populates="artifacts")
    chunks: Mapped[list["Chunk"]] = relationship(back_populates="artifact")
    observations: Mapped[list["Observation"]] = relationship(back_populates="artifact")
    citations: Mapped[list["CitationLink"]] = relationship(back_populates="artifact")


class Chunk(Base):
    __tablename__ = "chunk"

    id: Mapped[str] = _uuid_pk()
    artifact_id: Mapped[str] = mapped_column(ForeignKey("artifact.id"), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict, nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    artifact: Mapped[Artifact] = relationship(back_populates="chunks")
    citations: Mapped[list["CitationLink"]] = relationship(back_populates="chunk")


class Entity(TimestampMixin, Base):
    __tablename__ = "entity"

    id: Mapped[str] = _uuid_pk()
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    canonical_name: Mapped[str] = mapped_column(String(255), nullable=False)
    external_ref: Mapped[str | None] = mapped_column(String(255))
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict, nullable=False)

    observations: Mapped[list["Observation"]] = relationship(back_populates="entity")


class Report(TimestampMixin, Base):
    __tablename__ = "report"

    id: Mapped[str] = _uuid_pk()
    run_id: Mapped[str | None] = mapped_column(ForeignKey("run.id"))
    source_artifact_id: Mapped[str | None] = mapped_column(ForeignKey("artifact.id"))
    report_type: Mapped[str] = mapped_column(String(100), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    summary: Mapped[str] = mapped_column(Text, default="", nullable=False)
    content_markdown: Mapped[str] = mapped_column(Text, default="", nullable=False)
    score: Mapped[float | None] = mapped_column(Numeric(8, 3))
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    run: Mapped[RunRecord | None] = relationship(back_populates="reports")
    observations: Mapped[list["Observation"]] = relationship(back_populates="report")
    citations: Mapped[list["CitationLink"]] = relationship(back_populates="report")


class Observation(TimestampMixin, Base):
    __tablename__ = "observation"

    id: Mapped[str] = _uuid_pk()
    run_id: Mapped[str | None] = mapped_column(ForeignKey("run.id"))
    artifact_id: Mapped[str | None] = mapped_column(ForeignKey("artifact.id"))
    entity_id: Mapped[str | None] = mapped_column(ForeignKey("entity.id"))
    report_id: Mapped[str | None] = mapped_column(ForeignKey("report.id"))
    kind: Mapped[str] = mapped_column(String(100), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    source_offsets: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    confidence: Mapped[float | None] = mapped_column(Float)

    run: Mapped[RunRecord | None] = relationship(back_populates="observations")
    artifact: Mapped[Artifact | None] = relationship(back_populates="observations")
    entity: Mapped[Entity | None] = relationship(back_populates="observations")
    report: Mapped[Report | None] = relationship(back_populates="observations")
    citations: Mapped[list["CitationLink"]] = relationship(back_populates="observation")


class CitationLink(Base):
    __tablename__ = "citation_link"

    id: Mapped[str] = _uuid_pk()
    report_id: Mapped[str] = mapped_column(ForeignKey("report.id"), nullable=False)
    observation_id: Mapped[str | None] = mapped_column(ForeignKey("observation.id"))
    artifact_id: Mapped[str | None] = mapped_column(ForeignKey("artifact.id"))
    chunk_id: Mapped[str | None] = mapped_column(ForeignKey("chunk.id"))
    anchor_text: Mapped[str] = mapped_column(String(500), nullable=False)
    locator: Mapped[str | None] = mapped_column(String(255))
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    report: Mapped[Report] = relationship(back_populates="citations")
    observation: Mapped[Observation | None] = relationship(back_populates="citations")
    artifact: Mapped[Artifact | None] = relationship(back_populates="citations")
    chunk: Mapped[Chunk | None] = relationship(back_populates="citations")


class Interaction(TimestampMixin, Base):
    __tablename__ = "interaction"
    __table_args__ = (
        Index("ix_interaction_status_created_at", "status", "created_at"),
        Index("ix_interaction_run_id", "run_id", "created_at"),
    )

    id: Mapped[str] = _uuid_pk()
    run_id: Mapped[str] = mapped_column(ForeignKey("run.id"), nullable=False)
    prefect_flow_run_id: Mapped[str | None] = mapped_column(String(255))
    checkpoint_key: Mapped[str | None] = mapped_column(String(255))
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    prompt_md: Mapped[str] = mapped_column(Text, nullable=False)
    input_schema: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    default_input: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    response_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    ui_hints: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="open", nullable=False)
    answered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
