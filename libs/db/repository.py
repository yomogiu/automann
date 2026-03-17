from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Select, desc, select, text
from sqlalchemy.engine import Engine

from libs.contracts.models import SCHEMA_VERSION, AdapterResult, ArtifactRecord, ObservationRecord, ReportRecord

from .models import Artifact, Chunk, Entity, Interaction, Observation, Report, RunRecord, TaskSpec
from .session import session_scope


@dataclass(slots=True)
class PersistedAdapterResult:
    run: RunRecord
    artifacts: list[Artifact]
    reports: list[Report]


_FTS_TOKEN_RE = re.compile(r"\w+")


class LifeRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def create_task_spec(
        self,
        *,
        task_key: str,
        task_type: str,
        title: str,
        description: str | None = None,
        schedule_text: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> TaskSpec:
        with session_scope(self.engine) as session:
            record = TaskSpec(
                task_key=task_key,
                task_type=task_type,
                title=title,
                description=description,
                schedule_text=schedule_text,
                payload=payload or {},
            )
            session.add(record)
            session.flush()
            session.refresh(record)
            return record

    def get_run(self, run_id: str) -> RunRecord | None:
        with session_scope(self.engine) as session:
            return session.get(RunRecord, run_id)

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

    def list_reports(self, *, limit: int = 25) -> list[Report]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Report]] = select(Report).order_by(desc(Report.created_at)).limit(limit)
            return list(session.scalars(stmt))

    def list_artifacts(self, *, limit: int = 50) -> list[Artifact]:
        with session_scope(self.engine) as session:
            stmt: Select[tuple[Artifact]] = select(Artifact).order_by(desc(Artifact.created_at)).limit(limit)
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
            artifact_index = {artifact.path: artifact for artifact in stored_artifacts if artifact.path}
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
                        WHERE chunk_fts MATCH :match_query
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
            source_artifact = artifact_index.get(report.artifact_path or "")
            record = Report(
                run_id=run_id,
                source_artifact_id=source_artifact.id if source_artifact else None,
                report_type=report.report_type,
                title=report.title,
                summary=report.summary,
                content_markdown=report.content_markdown,
                metadata_json=report.metadata,
            )
            session.add(record)
            session.flush()
            stored.append(record)
        return stored

    @staticmethod
    def _sanitize_fts_query(query: str) -> str:
        tokens = [match.group(0).lower() for match in _FTS_TOKEN_RE.finditer(query)]
        return " AND ".join(tokens)
