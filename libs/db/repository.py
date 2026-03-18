from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Select, delete, desc, or_, select, text, update
from sqlalchemy.engine import Engine

from libs.contracts.models import SCHEMA_VERSION, AdapterResult, ArtifactRecord, ObservationRecord, ReportRecord
from libs.report_taxonomy import FILTER_KIND, TAG_KIND, classify_report_taxonomy, taxonomy_payload

from .models import (
    Artifact,
    Chunk,
    Entity,
    Interaction,
    Observation,
    Report,
    ReportTaxonomyLink,
    ReportTaxonomyTerm,
    SourceDocument,
    RunRecord,
    TaskSpec,
)
from .session import session_scope


@dataclass(slots=True)
class PersistedAdapterResult:
    run: RunRecord
    artifacts: list[Artifact]
    reports: list[Report]


@dataclass(slots=True)
class ReportTaxonomySnapshot:
    filter_keys: list[str]
    tag_keys: list[str]


_FTS_TOKEN_RE = re.compile(r"\w+")
_MISSING = object()


class LifeRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def create_task_spec(
        self,
        *,
        task_key: str,
        task_type: str,
        flow_name: str | None = None,
        title: str,
        description: str | None = None,
        schedule_text: str | None = None,
        timezone: str | None = None,
        work_pool: str | None = None,
        prompt_path: str | None = None,
        status: str = "active",
        prefect_deployment_id: str | None = None,
        prefect_deployment_name: str | None = None,
        prefect_deployment_path: str | None = None,
        prefect_deployment_url: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> TaskSpec:
        with session_scope(self.engine) as session:
            record = TaskSpec(
                task_key=task_key,
                task_type=task_type,
                flow_name=flow_name,
                title=title,
                description=description,
                schedule_text=schedule_text,
                timezone=timezone,
                work_pool=work_pool,
                prompt_path=prompt_path,
                status=status,
                prefect_deployment_id=prefect_deployment_id,
                prefect_deployment_name=prefect_deployment_name,
                prefect_deployment_path=prefect_deployment_path,
                prefect_deployment_url=prefect_deployment_url,
                payload=payload or {},
            )
            session.add(record)
            session.flush()
            session.refresh(record)
            return record

    def get_task_spec(self, task_spec_id: str) -> TaskSpec | None:
        with session_scope(self.engine) as session:
            return session.get(TaskSpec, task_spec_id)

    def get_task_spec_by_key(self, task_key: str) -> TaskSpec | None:
        with session_scope(self.engine) as session:
            stmt = select(TaskSpec).where(TaskSpec.task_key == task_key).limit(1)
            return session.scalars(stmt).first()

    def list_task_specs(self, *, status: str | None = None, limit: int = 100) -> list[TaskSpec]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[TaskSpec]] = select(TaskSpec)
            if status is not None:
                stmt = stmt.where(TaskSpec.status == status)
            stmt = stmt.order_by(desc(TaskSpec.updated_at), desc(TaskSpec.created_at)).limit(limit)
            return list(session.scalars(stmt))

    def update_task_spec(
        self,
        task_spec_id: str,
        *,
        task_type: str | object = _MISSING,
        flow_name: str | None | object = _MISSING,
        title: str | object = _MISSING,
        description: str | None | object = _MISSING,
        schedule_text: str | None | object = _MISSING,
        timezone: str | None | object = _MISSING,
        work_pool: str | None | object = _MISSING,
        prompt_path: str | None | object = _MISSING,
        status: str | object = _MISSING,
        prefect_deployment_id: str | None | object = _MISSING,
        prefect_deployment_name: str | None | object = _MISSING,
        prefect_deployment_path: str | None | object = _MISSING,
        prefect_deployment_url: str | None | object = _MISSING,
        payload: dict[str, Any] | object = _MISSING,
    ) -> TaskSpec | None:
        with session_scope(self.engine) as session:
            record = session.get(TaskSpec, task_spec_id)
            if record is None:
                return None

            updates = {
                "task_type": task_type,
                "flow_name": flow_name,
                "title": title,
                "description": description,
                "schedule_text": schedule_text,
                "timezone": timezone,
                "work_pool": work_pool,
                "prompt_path": prompt_path,
                "status": status,
                "prefect_deployment_id": prefect_deployment_id,
                "prefect_deployment_name": prefect_deployment_name,
                "prefect_deployment_path": prefect_deployment_path,
                "prefect_deployment_url": prefect_deployment_url,
                "payload": payload,
            }
            for field_name, value in updates.items():
                if value is _MISSING:
                    continue
                setattr(record, field_name, value)

            session.flush()
            session.refresh(record)
            return record

    def update_task_spec_status(self, task_spec_id: str, *, status: str) -> TaskSpec | None:
        return self.update_task_spec(task_spec_id, status=status)

    def attach_task_spec_prefect_metadata(
        self,
        task_spec_id: str,
        *,
        prefect_deployment_id: str | None = None,
        prefect_deployment_name: str | None = None,
        prefect_deployment_path: str | None = None,
        prefect_deployment_url: str | None = None,
    ) -> TaskSpec | None:
        return self.update_task_spec(
            task_spec_id,
            prefect_deployment_id=prefect_deployment_id,
            prefect_deployment_name=prefect_deployment_name,
            prefect_deployment_path=prefect_deployment_path,
            prefect_deployment_url=prefect_deployment_url,
        )

    def delete_task_spec(self, task_spec_id: str) -> bool:
        with session_scope(self.engine) as session:
            record = session.get(TaskSpec, task_spec_id)
            if record is None:
                return False
            session.delete(record)
            session.flush()
            return True

    def get_run(self, run_id: str) -> RunRecord | None:
        with session_scope(self.engine) as session:
            return session.get(RunRecord, run_id)

    def get_report(self, report_id: str) -> Report | None:
        with session_scope(self.engine) as session:
            return session.get(Report, report_id)

    def update_report_metadata(
        self,
        report_id: str,
        *,
        metadata: dict[str, Any],
    ) -> Report | None:
        with session_scope(self.engine) as session:
            record = session.get(Report, report_id)
            if record is None:
                return None
            record.metadata_json = metadata
            session.flush()
            session.refresh(record)
            return record

    def get_artifact(self, artifact_id: str) -> Artifact | None:
        with session_scope(self.engine) as session:
            return session.get(Artifact, artifact_id)

    def get_source_document(self, source_id: str) -> SourceDocument | None:
        with session_scope(self.engine) as session:
            return session.get(SourceDocument, source_id)

    def get_source_document_by_canonical_uri(self, canonical_uri: str) -> SourceDocument | None:
        with session_scope(self.engine) as session:
            stmt = select(SourceDocument).where(SourceDocument.canonical_uri == canonical_uri.strip()).limit(1)
            return session.scalars(stmt).first()

    def get_interaction(self, interaction_id: str) -> Interaction | None:
        with session_scope(self.engine) as session:
            return session.get(Interaction, interaction_id)

    def find_run_by_idempotency(
        self,
        *,
        session_id: str,
        idempotency_key: str,
        flow_name: str | None = None,
    ) -> RunRecord | None:
        with session_scope(self.engine) as session:
            stmt = select(RunRecord).where(
                RunRecord.parent_run_id.is_(None),
                RunRecord.session_id == session_id,
                RunRecord.idempotency_key == idempotency_key,
            )
            if flow_name is not None:
                stmt = stmt.where(RunRecord.flow_name == flow_name)
            stmt = stmt.order_by(desc(RunRecord.created_at)).limit(1)
            return session.scalars(stmt).first()

    def list_runs(self, *, limit: int = 25, top_level_only: bool = True) -> list[RunRecord]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[RunRecord]] = select(RunRecord)
            if top_level_only:
                stmt = stmt.where(RunRecord.parent_run_id.is_(None))
            stmt = stmt.order_by(desc(RunRecord.created_at)).limit(limit)
            return list(session.scalars(stmt))

    def list_runs_for_task_spec(
        self,
        task_spec_id: str,
        *,
        limit: int = 25,
        include_children: bool = False,
    ) -> list[RunRecord]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[RunRecord]] = select(RunRecord).where(RunRecord.task_spec_id == task_spec_id)
            if not include_children:
                stmt = stmt.where(RunRecord.parent_run_id.is_(None))
            stmt = stmt.order_by(desc(RunRecord.created_at)).limit(limit)
            return list(session.scalars(stmt))

    def list_reports(self, *, limit: int = 25, current_only: bool = True) -> list[Report]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Report]] = select(Report)
            if current_only:
                stmt = stmt.where(or_(Report.is_current.is_(True), Report.is_current.is_(None)))
            stmt = stmt.order_by(desc(Report.created_at)).limit(limit)
            return list(session.scalars(stmt))

    def list_source_documents(self, *, limit: int = 100) -> list[SourceDocument]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[SourceDocument]] = (
                select(SourceDocument)
                .order_by(desc(SourceDocument.updated_at), desc(SourceDocument.created_at))
                .limit(limit)
            )
            return list(session.scalars(stmt))

    def list_report_revisions(self, report_id: str) -> list[Report]:
        with session_scope(self.engine) as session:
            target = session.get(Report, report_id)
            if target is None:
                return []
            series_id = target.report_series_id or target.id
            stmt: Select[tuple[Report]] = (
                select(Report)
                .where(Report.report_series_id == series_id)
                .order_by(desc(Report.revision_number), desc(Report.created_at))
            )
            return list(session.scalars(stmt))

    def current_report_revision(self, report_series_id: str) -> Report | None:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Report]] = (
                select(Report)
                .where(
                    Report.report_series_id == report_series_id,
                    Report.is_current.is_(True),
                )
                .order_by(desc(Report.revision_number), desc(Report.created_at))
                .limit(1)
            )
            return session.scalars(stmt).first()

    def latest_report_revision(self, report_series_id: str) -> Report | None:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Report]] = (
                select(Report)
                .where(Report.report_series_id == report_series_id)
                .order_by(desc(Report.revision_number), desc(Report.created_at))
                .limit(1)
            )
            return session.scalars(stmt).first()

    def promote_report_revision(self, report_id: str) -> Report | None:
        with session_scope(self.engine) as session:
            target = session.get(Report, report_id)
            if target is None:
                return None
            series_id = target.report_series_id or target.id
            session.execute(
                update(Report)
                .where(Report.report_series_id == series_id)
                .values(is_current=False)
            )
            target.is_current = True
            if target.report_series_id is None:
                target.report_series_id = series_id
            session.flush()
            session.refresh(target)
            return target

    def backfill_report_revisions(self) -> int:
        updated = 0
        with session_scope(self.engine) as session:
            reports = list(session.scalars(select(Report).order_by(Report.created_at, Report.id)))
            by_series: dict[str, list[Report]] = {}
            for report in reports:
                series_id = report.report_series_id or report.id
                if report.report_series_id is None:
                    report.report_series_id = series_id
                    updated += 1
                if report.revision_number is None:
                    report.revision_number = 1
                    updated += 1
                if report.is_current is None:
                    report.is_current = True
                    updated += 1
                by_series.setdefault(series_id, []).append(report)

            for series_reports in by_series.values():
                ordered = sorted(
                    series_reports,
                    key=lambda item: ((item.revision_number or 0), item.created_at, item.id),
                )
                for index, report in enumerate(ordered, start=1):
                    if report.revision_number != index:
                        report.revision_number = index
                        updated += 1
                    should_be_current = report is ordered[-1]
                    if report.is_current != should_be_current:
                        report.is_current = should_be_current
                        updated += 1
            session.flush()
        return updated

    def list_report_taxonomy(self) -> dict[str, list[dict[str, Any]]]:
        with session_scope(self.engine) as session:
            rows = list(
                session.scalars(
                    select(ReportTaxonomyTerm)
                    .where(ReportTaxonomyTerm.active.is_(True))
                    .order_by(ReportTaxonomyTerm.sort_order, ReportTaxonomyTerm.key)
                )
            )
        if not rows:
            return taxonomy_payload()

        filters: list[dict[str, Any]] = []
        tags: list[dict[str, Any]] = []
        for row in rows:
            payload = {
                "key": row.key,
                "label": row.label,
                "kind": row.kind,
                "parent_key": row.parent_key,
                "description": row.description,
                "sort_order": row.sort_order,
            }
            if row.kind == FILTER_KIND:
                filters.append(payload)
            elif row.kind == TAG_KIND:
                tags.append(payload)
        return {"filters": filters, "tags": tags}

    def report_taxonomy_map(self, report_ids: list[str]) -> dict[str, ReportTaxonomySnapshot]:
        result = {
            report_id: ReportTaxonomySnapshot(filter_keys=[], tag_keys=[])
            for report_id in report_ids
        }
        if not report_ids:
            return result

        with session_scope(self.engine) as session:
            rows = session.execute(
                select(ReportTaxonomyLink.report_id, ReportTaxonomyTerm.key, ReportTaxonomyTerm.kind)
                .join(ReportTaxonomyTerm, ReportTaxonomyTerm.key == ReportTaxonomyLink.term_key)
                .where(ReportTaxonomyLink.report_id.in_(report_ids))
                .order_by(ReportTaxonomyTerm.sort_order, ReportTaxonomyTerm.key)
            ).all()

        for report_id, key, kind in rows:
            snapshot = result.setdefault(report_id, ReportTaxonomySnapshot(filter_keys=[], tag_keys=[]))
            if kind == FILTER_KIND:
                snapshot.filter_keys.append(key)
            elif kind == TAG_KIND:
                snapshot.tag_keys.append(key)
        return result

    def backfill_report_taxonomy(self) -> int:
        synced = 0
        with session_scope(self.engine) as session:
            reports = list(session.scalars(select(Report)))
            for report in reports:
                self._sync_report_taxonomy_links(session, report)
                synced += 1
        return synced

    def list_artifacts(self, *, limit: int = 50) -> list[Artifact]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Artifact]] = select(Artifact).order_by(desc(Artifact.created_at)).limit(limit)
            return list(session.scalars(stmt))

    def list_artifacts_for_source_document(self, source_document_id: str) -> list[Artifact]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Artifact]] = (
                select(Artifact)
                .where(Artifact.source_document_id == source_document_id)
                .order_by(desc(Artifact.created_at))
            )
            return list(session.scalars(stmt))

    def list_chunks_for_artifact(self, artifact_id: str, *, limit: int = 50) -> list[Chunk]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Chunk]] = (
                select(Chunk)
                .where(Chunk.artifact_id == artifact_id)
                .order_by(Chunk.ordinal)
                .limit(limit)
            )
            return list(session.scalars(stmt))

    def list_interactions(self, *, limit: int = 25, status: str | None = None) -> list[Interaction]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Interaction]] = select(Interaction)
            if status:
                stmt = stmt.where(Interaction.status == status)
            stmt = stmt.order_by(desc(Interaction.created_at)).limit(limit)
            return list(session.scalars(stmt))

    def start_run(
        self,
        *,
        flow_name: str,
        worker_key: str,
        input_payload: dict[str, Any],
        task_spec_id: str | None = None,
        parent_run_id: str | None = None,
        prefect_flow_run_id: str | None = None,
        schema_version: str | None = None,
        command_id: str | None = None,
        session_id: str | None = None,
        correlation_id: str | None = None,
        idempotency_key: str | None = None,
        actor: str | None = None,
        status: str = "pending",
    ) -> RunRecord:
        with session_scope(self.engine) as session:
            record = RunRecord(
                task_spec_id=task_spec_id,
                parent_run_id=parent_run_id,
                flow_name=flow_name,
                worker_key=worker_key,
                prefect_flow_run_id=prefect_flow_run_id,
                input_payload=input_payload,
                schema_version=schema_version or SCHEMA_VERSION,
                command_id=command_id,
                session_id=session_id,
                correlation_id=correlation_id,
                idempotency_key=idempotency_key,
                actor=actor or "user",
                status=status,
                started_at=datetime.now(timezone.utc) if status == "running" else None,
            )
            session.add(record)
            session.flush()
            session.refresh(record)
            return record

    def update_run_status(
        self,
        run_id: str,
        *,
        status: str,
        stdout: str | None = None,
        stderr: str | None = None,
        structured_outputs: dict[str, Any] | None = None,
        artifact_manifest: list[dict[str, Any]] | None = None,
        observation_summary: list[dict[str, Any]] | None = None,
        next_suggested_events: list[dict[str, Any]] | None = None,
        prefect_flow_run_id: str | None = None,
    ) -> RunRecord | None:
        with session_scope(self.engine) as session:
            record = session.get(RunRecord, run_id)
            if not record:
                return None
            record.status = status
            if stdout is not None:
                record.stdout = stdout
            if stderr is not None:
                record.stderr = stderr
            if structured_outputs is not None:
                record.structured_outputs = structured_outputs
            if artifact_manifest is not None:
                record.artifact_manifest = artifact_manifest
            if observation_summary is not None:
                record.observation_summary = observation_summary
            if next_suggested_events is not None:
                record.next_suggested_events = next_suggested_events
            if prefect_flow_run_id is not None:
                record.prefect_flow_run_id = prefect_flow_run_id
            if status == "running" and record.started_at is None:
                record.started_at = datetime.now(timezone.utc)
            if status in {"completed", "failed", "skipped"}:
                record.finished_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(record)
            return record

    def persist_adapter_result(self, run_id: str, result: AdapterResult) -> PersistedAdapterResult | None:
        updated = self.update_run_status(
            run_id,
            status=result.status.value,
            stdout=result.stdout,
            stderr=result.stderr,
            structured_outputs=result.structured_outputs,
            artifact_manifest=[item.model_dump(mode="json") for item in result.artifact_manifest],
            observation_summary=[item.model_dump(mode="json") for item in result.observations],
            next_suggested_events=[item.model_dump(mode="json") for item in result.next_suggested_events],
        )
        if not updated:
            return None

        with session_scope(self.engine) as session:
            db_run = session.get(RunRecord, run_id)
            if not db_run:
                return None
            stored_artifacts = self._store_artifacts(session, db_run.id, db_run.task_spec_id, result.artifact_manifest)
            artifact_index = {
                path: artifact
                for artifact in stored_artifacts
                if artifact.path
                for path in self._artifact_lookup_keys(artifact.path)
            }
            self._store_observations(session, db_run.id, result.observations)
            stored_reports = self._store_reports(session, db_run.id, result.reports, artifact_index)
            session.flush()
            session.refresh(db_run)
            return PersistedAdapterResult(run=db_run, artifacts=stored_artifacts, reports=stored_reports)

    def create_interaction(
        self,
        *,
        run_id: str,
        title: str,
        prompt_md: str,
        input_schema: dict[str, Any],
        default_input: dict[str, Any] | None = None,
        checkpoint_key: str | None = None,
        prefect_flow_run_id: str | None = None,
        ui_hints: dict[str, Any] | None = None,
    ) -> Interaction:
        with session_scope(self.engine) as session:
            record = Interaction(
                run_id=run_id,
                title=title,
                prompt_md=prompt_md,
                input_schema=input_schema,
                default_input=default_input,
                checkpoint_key=checkpoint_key,
                prefect_flow_run_id=prefect_flow_run_id,
                ui_hints=ui_hints or {},
                status="open",
            )
            session.add(record)
            run = session.get(RunRecord, run_id)
            if run is not None:
                run.status = "waiting_input"
            session.flush()
            session.refresh(record)
            return record

    def answer_interaction(self, interaction_id: str, *, response: dict[str, Any]) -> Interaction | None:
        with session_scope(self.engine) as session:
            record = session.get(Interaction, interaction_id)
            if not record:
                return None
            record.response_payload = response
            record.status = "answered"
            record.answered_at = datetime.now(timezone.utc)
            run = session.get(RunRecord, record.run_id)
            if run is not None and run.status == "waiting_input":
                run.status = "running"
            session.flush()
            session.refresh(record)
            return record

    def create_entity(self, *, entity_type: str, canonical_name: str, external_ref: str | None = None) -> Entity:
        with session_scope(self.engine) as session:
            record = Entity(
                entity_type=entity_type,
                canonical_name=canonical_name,
                external_ref=external_ref,
            )
            session.add(record)
            session.flush()
            session.refresh(record)
            return record

    def upsert_source_document(
        self,
        *,
        canonical_uri: str,
        source_type: str,
        title: str | None | object = _MISSING,
        author: str | None | object = _MISSING,
        published_at: datetime | None | object = _MISSING,
        current_text_artifact_id: str | None | object = _MISSING,
        metadata_json: dict[str, Any] | object = _MISSING,
    ) -> SourceDocument:
        canonical_uri = canonical_uri.strip()
        with session_scope(self.engine) as session:
            record = session.scalars(
                select(SourceDocument).where(SourceDocument.canonical_uri == canonical_uri).limit(1)
            ).first()
            if record is None:
                record = SourceDocument(
                    canonical_uri=canonical_uri,
                    source_type=source_type,
                    title=None if title is _MISSING else title,
                    author=None if author is _MISSING else author,
                    published_at=None if published_at is _MISSING else published_at,
                    current_text_artifact_id=(
                        None if current_text_artifact_id is _MISSING else current_text_artifact_id
                    ),
                    metadata_json={} if metadata_json is _MISSING else dict(metadata_json),
                )
                session.add(record)
            else:
                record.source_type = source_type
                if title is not _MISSING:
                    record.title = title
                if author is not _MISSING:
                    record.author = author
                if published_at is not _MISSING:
                    record.published_at = published_at
                if current_text_artifact_id is not _MISSING:
                    record.current_text_artifact_id = current_text_artifact_id
                if metadata_json is not _MISSING:
                    record.metadata_json = dict(metadata_json)
            session.flush()
            session.refresh(record)
            return record

    def assign_artifact_to_source(self, artifact_id: str, source_document_id: str) -> Artifact | None:
        with session_scope(self.engine) as session:
            artifact = session.get(Artifact, artifact_id)
            if artifact is None:
                return None
            source_document = session.get(SourceDocument, source_document_id)
            if source_document is None:
                return None
            artifact.source_document_id = source_document.id
            session.flush()
            session.refresh(artifact)
            return artifact

    def update_source_document_current_text_artifact(
        self,
        source_document_id: str,
        *,
        current_text_artifact_id: str | None,
    ) -> SourceDocument | None:
        with session_scope(self.engine) as session:
            record = session.get(SourceDocument, source_document_id)
            if record is None:
                return None
            record.current_text_artifact_id = current_text_artifact_id
            session.flush()
            session.refresh(record)
            return record

    def upsert_chunks(
        self,
        *,
        artifact_id: str,
        chunks: list[dict[str, Any]],
    ) -> int:
        with session_scope(self.engine) as session:
            for entry in chunks:
                session.add(
                    Chunk(
                        artifact_id=artifact_id,
                        ordinal=int(entry["ordinal"]),
                        text=str(entry["text"]),
                        token_count=int(entry.get("token_count", 0)),
                        metadata_json=dict(entry.get("metadata", {})),
                        embedding=entry.get("embedding"),
                    )
                )
            return len(chunks)

    def query_chunks(
        self,
        *,
        query: str,
        limit: int = 10,
        embedding: list[float] | None = None,
    ) -> list[Chunk]:
        del embedding
        match_query = self._sanitize_fts_query(query)
        if not match_query:
            return []

        with session_scope(self.engine) as session:
            chunk_ids = list(
                session.execute(
                    text(
                        """
                        SELECT chunk.id
                        FROM chunk_fts
                        JOIN chunk ON chunk.rowid = chunk_fts.rowid
                        JOIN artifact ON artifact.id = chunk.artifact_id
                        LEFT JOIN source_document ON source_document.id = artifact.source_document_id
                        WHERE chunk_fts MATCH :match_query
                          AND (
                            artifact.source_document_id IS NULL
                            OR artifact.id = source_document.current_text_artifact_id
                          )
                        ORDER BY bm25(chunk_fts), chunk.created_at DESC
                        LIMIT :limit
                        """
                    ),
                    {
                        "match_query": match_query,
                        "limit": limit,
                    },
                ).scalars()
            )
            if not chunk_ids:
                return []

            rows = list(session.scalars(select(Chunk).where(Chunk.id.in_(chunk_ids))))
            row_map = {row.id: row for row in rows}
            return [row_map[chunk_id] for chunk_id in chunk_ids if chunk_id in row_map]

    def latest_report(self, report_type: str | None = None) -> Report | None:
        with session_scope(self.engine) as session:
            stmt = select(Report)
            if report_type:
                stmt = stmt.where(Report.report_type == report_type)
            stmt = stmt.where(or_(Report.is_current.is_(True), Report.is_current.is_(None)))
            stmt = stmt.order_by(desc(Report.created_at)).limit(1)
            return session.scalars(stmt).first()

    def _store_artifacts(
        self,
        session,
        run_id: str,
        task_spec_id: str | None,
        artifacts: list[ArtifactRecord],
    ) -> list[Artifact]:
        stored: list[Artifact] = []
        for artifact in artifacts:
            record = Artifact(
                task_spec_id=task_spec_id,
                run_id=run_id,
                kind=artifact.kind,
                path=artifact.path,
                storage_uri=artifact.storage_uri,
                size_bytes=artifact.size_bytes,
                media_type=artifact.media_type,
                sha256=artifact.sha256,
                metadata_json=artifact.metadata,
            )
            session.add(record)
            session.flush()
            stored.append(record)
        return stored

    def _store_observations(
        self,
        session,
        run_id: str,
        observations: list[ObservationRecord],
    ) -> None:
        for observation in observations:
            session.add(
                Observation(
                    run_id=run_id,
                    kind=observation.kind,
                    summary=observation.summary,
                    payload=observation.payload,
                    confidence=observation.confidence,
                )
            )

    def _store_reports(
        self,
        session,
        run_id: str,
        reports: list[ReportRecord],
        artifact_index: dict[str, Artifact],
    ) -> list[Report]:
        stored: list[Report] = []
        for report in reports:
            source_artifact = None
            if report.artifact_path:
                for candidate_path in self._artifact_lookup_keys(report.artifact_path):
                    source_artifact = artifact_index.get(candidate_path)
                    if source_artifact is not None:
                        break
            record = Report(
                run_id=run_id,
                source_artifact_id=source_artifact.id if source_artifact else None,
                report_type=report.report_type,
                title=report.title,
                summary=report.summary,
                content_markdown=report.content_markdown,
                report_series_id=report.report_series_id,
                revision_number=report.revision_number or 1,
                supersedes_report_id=report.supersedes_report_id,
                is_current=True if report.is_current is None else report.is_current,
                metadata_json=report.metadata,
            )
            session.add(record)
            session.flush()
            if record.report_series_id is None:
                record.report_series_id = record.id
                session.flush()
            self._sync_report_taxonomy_links(session, record)
            stored.append(record)
        return stored

    @staticmethod
    def _artifact_lookup_keys(path: str) -> list[str]:
        keys = [path]
        try:
            keys.append(str(Path(path).resolve()))
        except OSError:
            pass
        return keys

    @staticmethod
    def _sanitize_fts_query(query: str) -> str:
        tokens = [match.group(0).lower() for match in _FTS_TOKEN_RE.finditer(query)]
        return " AND ".join(tokens)

    def _sync_report_taxonomy_links(self, session, report: Report) -> None:
        classification = classify_report_taxonomy(
            report_type=report.report_type,
            title=report.title,
            summary=report.summary,
            metadata=report.metadata_json,
        )
        desired_keys = set(classification["filter_keys"]) | set(classification["tag_keys"])
        existing_keys = set(
            session.scalars(
                select(ReportTaxonomyLink.term_key).where(ReportTaxonomyLink.report_id == report.id)
            )
        )

        stale_keys = existing_keys - desired_keys
        if stale_keys:
            session.execute(
                delete(ReportTaxonomyLink).where(
                    ReportTaxonomyLink.report_id == report.id,
                    ReportTaxonomyLink.term_key.in_(stale_keys),
                )
            )

        for key in desired_keys - existing_keys:
            session.add(ReportTaxonomyLink(report_id=report.id, term_key=key))
