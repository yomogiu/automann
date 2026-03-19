from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from apps.api.dependencies import orchestration_dep, repository_dep, retrieval_dep, settings_dep
from apps.api.main import create_app
from apps.api.services import OrchestrationService
from libs.config import get_settings
from libs.contracts.models import AdapterResult, ArtifactRecord, ReportRecord, WorkerStatus
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url


class APIWebFrontendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.env_overrides = {
            "LIFE_ARTIFACT_ROOT": str(self.root / "artifacts"),
            "LIFE_REPORT_ROOT": str(self.root / "reports"),
            "LIFE_RUNTIME_ROOT": str(self.root / "runtime"),
            "LIFE_DATABASE_URL": f"sqlite+pysqlite:///{self.root / 'runtime' / 'life.db'}",
        }
        self.previous_env = {key: os.environ.get(key) for key in self.env_overrides}
        for key, value in self.env_overrides.items():
            os.environ[key] = value

        settings_dep.cache_clear()
        repository_dep.cache_clear()
        retrieval_dep.cache_clear()
        orchestration_dep.cache_clear()

        settings = get_settings()
        bootstrap_life_database(settings)
        self.engine = engine_for_url(settings.life_database_url)
        self.repository = LifeRepository(self.engine)

        artifact_path = self.root / "artifacts" / "paper-review.md"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text("# Paper Review\n\nAlpha beta gamma\n", encoding="utf-8")

        run = self.repository.start_run(
            flow_name="paper_review_flow",
            worker_key="mini-process",
            input_payload={"paper_id": "2403.01234"},
            status="running",
        )
        persisted = self.repository.persist_adapter_result(
            run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="review finished",
                artifact_manifest=[
                    ArtifactRecord(
                        kind="review-card",
                        path=str(artifact_path),
                        storage_uri=artifact_path.as_uri(),
                        size_bytes=artifact_path.stat().st_size,
                        media_type="text/markdown",
                    )
                ],
                reports=[
                    ReportRecord(
                        report_type="paper_review",
                        title="Paper 2403.01234",
                        summary="Alpha beta gamma",
                        content_markdown="# Paper Review\n\nAlpha beta gamma\n",
                        artifact_path=str(artifact_path),
                        metadata={"presentation_mode": "raw_review"},
                    )
                ],
            ),
        )
        assert persisted is not None

        self.interaction = self.repository.create_interaction(
            run_id=run.id,
            title="Need approval",
            prompt_md="Proceed with publication?",
            input_schema={
                "type": "object",
                "properties": {"approved": {"type": "boolean"}},
                "required": ["approved"],
            },
            default_input={"approved": False},
        )
        self.report = persisted.reports[0]
        self.artifact = persisted.artifacts[0]
        self.run = self.repository.get_run(run.id)
        assert self.run is not None

        research_csv_v1 = self.root / "artifacts" / "research-v1.csv"
        research_csv_v2 = self.root / "artifacts" / "research-v2.csv"
        research_csv_v1.write_text("region,value\nUS-East,120\n", encoding="utf-8")
        research_csv_v2.write_text("region,value\nUS-East,140\nCanada,90\n", encoding="utf-8")

        research_run_v1 = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "Edge AI infrastructure", "report_key": "edge-ai"},
            status="running",
        )
        research_persisted_v1 = self.repository.persist_adapter_result(
            research_run_v1.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="research-table",
                        path=str(research_csv_v1),
                        storage_uri=research_csv_v1.as_uri(),
                        size_bytes=research_csv_v1.stat().st_size,
                        media_type="text/csv",
                    )
                ],
                reports=[
                    ReportRecord(
                        report_type="research_report",
                        title="Edge AI infrastructure",
                        summary="v1",
                        content_markdown="# Edge AI infrastructure\n\nV1\n",
                        report_series_id="edge-ai",
                        revision_number=1,
                        is_current=False,
                        metadata={"report_key": "edge-ai"},
                    )
                ],
            ),
        )
        assert research_persisted_v1 is not None
        self.research_report_v1 = research_persisted_v1.reports[0]
        self.repository.promote_report_revision(self.research_report_v1.id)

        research_run_v2 = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "Edge AI infrastructure", "report_key": "edge-ai"},
            status="running",
        )
        research_persisted_v2 = self.repository.persist_adapter_result(
            research_run_v2.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="research-table",
                        path=str(research_csv_v2),
                        storage_uri=research_csv_v2.as_uri(),
                        size_bytes=research_csv_v2.stat().st_size,
                        media_type="text/csv",
                    )
                ],
                reports=[
                    ReportRecord(
                        report_type="research_report",
                        title="Edge AI infrastructure",
                        summary="v2",
                        content_markdown="# Edge AI infrastructure\n\nV2\n",
                        report_series_id="edge-ai",
                        revision_number=2,
                        supersedes_report_id=self.research_report_v1.id,
                        is_current=False,
                        metadata={"report_key": "edge-ai"},
                    )
                ],
            ),
        )
        assert research_persisted_v2 is not None
        self.research_report_v2 = research_persisted_v2.reports[0]
        self.research_csv_artifact = research_persisted_v2.artifacts[0]
        self.repository.promote_report_revision(self.research_report_v2.id)

        source_canonical_uri = "file:///knowledge/source-note.md"
        source_old_path = self.root / "artifacts" / "source-note-v1.md"
        source_current_path = self.root / "artifacts" / "source-note-v2.md"
        source_old_path.write_text("source note stale version\n", encoding="utf-8")
        source_current_path.write_text("source note current version\n", encoding="utf-8")

        source_run_v1 = self.repository.start_run(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload={"items": [{"input_kind": "file", "file_path": str(source_old_path)}]},
            status="running",
        )
        source_persisted_v1 = self.repository.persist_adapter_result(
            source_run_v1.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="source-text",
                        path=str(source_old_path),
                        storage_uri=source_old_path.as_uri(),
                        size_bytes=source_old_path.stat().st_size,
                        media_type="text/markdown",
                        metadata={
                            "role": "normalized_text",
                            "input_index": 0,
                            "canonical_uri": source_canonical_uri,
                            "source_type": "markdown",
                        },
                    )
                ],
                structured_outputs={
                    "items": [
                        {
                            "input_index": 0,
                            "input_kind": "file",
                            "status": "completed",
                            "canonical_uri": source_canonical_uri,
                            "source_type": "markdown",
                            "normalized_text_artifact_path": str(source_old_path),
                            "chunk_count": 1,
                            "warning_codes": [],
                            "title": "Source note",
                            "tags": ["markdown", "ingest"],
                            "metadata": {"source_type": "markdown", "tags": ["markdown", "ingest"]},
                            "chunks": [
                                {
                                    "ordinal": 0,
                                    "text": "source note stale version",
                                    "token_count": 4,
                                    "metadata": {"canonical_uri": source_canonical_uri, "source_type": "markdown"},
                                }
                            ],
                        }
                    ]
                },
            ),
        )
        assert source_persisted_v1 is not None
        self.source_legacy_artifact = source_persisted_v1.artifacts[0]

        self.source_document = self.repository.upsert_source_document(
            canonical_uri=source_canonical_uri,
            source_type="markdown",
            title="Source note v1",
            author="Analyst",
            current_text_artifact_id=self.source_legacy_artifact.id,
            metadata_json={
                "tags": ["markdown", "ingest"],
                "entities": ["Analyst"],
                "topics": ["source documents"],
            },
        )
        self.repository.assign_artifact_to_source(self.source_legacy_artifact.id, self.source_document.id)
        self.repository.upsert_chunks(
            artifact_id=self.source_legacy_artifact.id,
            chunks=[
                {
                    "ordinal": 0,
                    "text": "source note stale version",
                    "token_count": 4,
                    "metadata": {"canonical_uri": source_canonical_uri, "source_type": "markdown"},
                }
            ],
        )

        source_run_v2 = self.repository.start_run(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload={"items": [{"input_kind": "file", "file_path": str(source_current_path)}]},
            status="running",
        )
        source_persisted_v2 = self.repository.persist_adapter_result(
            source_run_v2.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="source-text",
                        path=str(source_current_path),
                        storage_uri=source_current_path.as_uri(),
                        size_bytes=source_current_path.stat().st_size,
                        media_type="text/markdown",
                        metadata={
                            "role": "normalized_text",
                            "input_index": 0,
                            "canonical_uri": source_canonical_uri,
                            "source_type": "markdown",
                        },
                    )
                ],
                structured_outputs={
                    "items": [
                        {
                            "input_index": 0,
                            "input_kind": "file",
                            "status": "completed",
                            "canonical_uri": source_canonical_uri,
                            "source_type": "markdown",
                            "normalized_text_artifact_path": str(source_current_path),
                            "chunk_count": 1,
                            "warning_codes": [],
                            "title": "Source note",
                            "tags": ["markdown", "ingest"],
                            "metadata": {"source_type": "markdown", "tags": ["markdown", "ingest"]},
                            "chunks": [
                                {
                                    "ordinal": 0,
                                    "text": "source note current version",
                                    "token_count": 4,
                                    "metadata": {"canonical_uri": source_canonical_uri, "source_type": "markdown"},
                                }
                            ],
                        }
                    ]
                },
            ),
        )
        assert source_persisted_v2 is not None
        self.source_current_artifact = source_persisted_v2.artifacts[0]
        self.source_document = self.repository.upsert_source_document(
            canonical_uri=source_canonical_uri,
            source_type="markdown",
            title="Source note current",
            author="Analyst",
            current_text_artifact_id=self.source_current_artifact.id,
            metadata_json={
                "tags": ["markdown", "ingest"],
                "entities": ["Analyst"],
                "topics": ["source documents"],
            },
        )
        self.repository.assign_artifact_to_source(self.source_current_artifact.id, self.source_document.id)
        self.repository.upsert_chunks(
            artifact_id=self.source_current_artifact.id,
            chunks=[
                {
                    "ordinal": 0,
                    "text": "source note current version",
                    "token_count": 4,
                    "metadata": {"canonical_uri": source_canonical_uri, "source_type": "markdown"},
                }
            ],
        )

        self.client = TestClient(create_app())

    def tearDown(self) -> None:
        self.client.close()
        self.engine.dispose()

        settings_dep.cache_clear()
        repository_dep.cache_clear()
        retrieval_dep.cache_clear()
        orchestration_dep.cache_clear()

        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def test_root_serves_brewhouse_frontend(self) -> None:
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Mnemosyne Archive", response.text)
        self.assertIn("<em>M</em>n<em>emo</em>syne", response.text)
        self.assertIn('href="/research"', response.text)
        self.assertIn('href="/sources"', response.text)
        self.assertNotIn('href="/automations"', response.text)

        static_response = self.client.get("/static/app.css")
        self.assertEqual(static_response.status_code, 200)
        self.assertIn(".doc-row", static_response.text)
        self.assertIn(".filter-bar", static_response.text)
        self.assertIn(".masthead-nav", static_response.text)

    def test_automations_route_serves_separate_frontend_shell(self) -> None:
        response = self.client.get("/automations")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Mnemosyne Automations", response.text)
        self.assertIn("<em>M</em>n<em>emo</em>syne", response.text)
        self.assertIn("/static/automations.js", response.text)
        self.assertIn("/static/automations.css", response.text)
        self.assertIn('id="newAutomationButton"', response.text)
        self.assertIn('id="saveAutomationButton"', response.text)

        script_response = self.client.get("/static/automations.js")
        self.assertEqual(script_response.status_code, 200)
        self.assertIn("templateCatalog", script_response.text)
        self.assertIn('fetchJSON("/automations?limit=100")', script_response.text)
        self.assertIn("saveCurrentAutomation", script_response.text)
        self.assertIn("applyJsonEditor", script_response.text)

        style_response = self.client.get("/static/automations.css")
        self.assertEqual(style_response.status_code, 200)
        self.assertIn(".automation-status-strip", style_response.text)
        self.assertIn(".automation-layout", style_response.text)
        self.assertIn(".editor-textarea", style_response.text)

    def test_research_route_serves_workbench_shell(self) -> None:
        response = self.client.get("/research")
        self.assertEqual(response.status_code, 200)
        self.assertIn("/static/research.js", response.text)
        self.assertIn("/static/research.css", response.text)
        self.assertIn('id="researchFlash"', response.text)
        self.assertIn('id="researchTabButton"', response.text)
        self.assertIn('id="ingestTabButton"', response.text)

        script_response = self.client.get("/static/research.js")
        self.assertEqual(script_response.status_code, 200)
        self.assertIn('getQueryParam("run_id")', script_response.text)
        self.assertIn('"/commands/"', script_response.text)
        self.assertIn('"search-report"', script_response.text)
        self.assertIn('"research-report"', script_response.text)
        self.assertIn('"/commands/artifact-ingest"', script_response.text)
        self.assertIn('"/runs?limit=25"', script_response.text)

        style_response = self.client.get("/static/research.css")
        self.assertEqual(style_response.status_code, 200)
        self.assertGreater(len(style_response.text), 0)

    def test_sources_route_and_api_surface_current_text_artifacts(self) -> None:
        response = self.client.get("/sources")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Mnemosyne Sources", response.text)
        self.assertIn("<em>M</em>n<em>emo</em>syne", response.text)
        self.assertIn('href="/sources"', response.text)
        self.assertIn('href="/research"', response.text)
        self.assertNotIn('href="/automations"', response.text)
        self.assertIn("/static/sources.js", response.text)
        self.assertIn("/static/sources.css", response.text)

        list_response = self.client.get(
            "/sources?limit=100&include_metadata=true",
            headers={"Accept": "application/json"},
        )
        self.assertEqual(list_response.status_code, 200)
        list_payload = list_response.json()
        self.assertEqual(len(list_payload["sources"]), 1)
        listed_source = list_payload["sources"][0]
        self.assertEqual(listed_source["id"], self.source_document.id)
        self.assertEqual(listed_source["artifact_count"], 2)
        self.assertEqual(listed_source["current_text_artifact_id"], self.source_current_artifact.id)
        self.assertEqual(listed_source["metadata"]["tags"], ["markdown", "ingest"])

        hidden_response = self.client.get(
            "/sources?limit=100",
            headers={"Accept": "application/json"},
        )
        self.assertEqual(hidden_response.status_code, 200)
        hidden_payload = hidden_response.json()
        hidden_source = hidden_payload["sources"][0]
        self.assertNotIn("metadata", hidden_source)
        self.assertNotIn("source_profile", hidden_source)

        detail_response = self.client.get(
            f"/sources/{self.source_document.id}",
            headers={"Accept": "application/json"},
        )
        self.assertEqual(detail_response.status_code, 200)
        detail_payload = detail_response.json()["source"]
        self.assertEqual(detail_payload["id"], self.source_document.id)
        self.assertEqual(detail_payload["artifact_count"], 2)
        self.assertEqual(detail_payload["current_text_artifact"]["id"], self.source_current_artifact.id)
        self.assertEqual(detail_payload["current_text_artifact"]["preview_url"], f"/artifacts/{self.source_current_artifact.id}/preview")
        self.assertEqual(detail_payload["current_text_artifact"]["download_url"], f"/artifacts/{self.source_current_artifact.id}/download")
        self.assertEqual(
            {item["id"] for item in detail_payload["artifacts"]},
            {self.source_current_artifact.id, self.source_legacy_artifact.id},
        )
        self.assertEqual([item["text"] for item in detail_payload["current_chunks"]], ["source note current version"])

        preview_response = self.client.get(detail_payload["current_text_artifact"]["preview_url"])
        self.assertEqual(preview_response.status_code, 200)
        self.assertEqual(preview_response.json()["status"], "text")
        self.assertIn("source note current version", preview_response.json()["content"])

        download_response = self.client.get(detail_payload["current_text_artifact"]["download_url"])
        self.assertEqual(download_response.status_code, 200)
        self.assertIn("source note current version", download_response.text)

        sources_js = self.client.get("/static/sources.js")
        self.assertEqual(sources_js.status_code, 200)
        self.assertIn('getQueryParam("source_id")', sources_js.text)
        self.assertIn("loadSourceRecord", sources_js.text)
        self.assertIn("loadSourceDetail", sources_js.text)
        self.assertIn("/sources/${encodeURIComponent(sourceId)}", sources_js.text)

        self.assertIn('data-action="delete-source"', sources_js.text)
        self.assertIn('"DELETE"', sources_js.text)

    def test_delete_source_endpoint_removes_source_from_db_and_list(self) -> None:
        delete_response = self.client.delete(f"/sources/{self.source_document.id}", headers={"Accept": "application/json"})
        self.assertEqual(delete_response.status_code, 200)
        delete_payload = delete_response.json()
        self.assertEqual(delete_payload, {"id": self.source_document.id, "deleted": True})

        missing_response = self.client.delete("/sources/not-a-source", headers={"Accept": "application/json"})
        self.assertEqual(missing_response.status_code, 404)

        list_response = self.client.get("/sources?limit=100", headers={"Accept": "application/json"})
        self.assertEqual(list_response.status_code, 200)
        list_payload = list_response.json()
        self.assertEqual(list_payload["sources"], [])

        detail_response = self.client.get(
            f"/sources/{self.source_document.id}",
            headers={"Accept": "application/json"},
        )
        self.assertEqual(detail_response.status_code, 404)

    def test_report_frontend_supports_annotated_and_raw_modes(self) -> None:
        app_js = self.client.get("/static/app.js")
        self.assertEqual(app_js.status_code, 200)
        self.assertIn("AI Analysis", app_js.text)
        self.assertIn("presentation_mode", app_js.text)
        self.assertIn("annotated_html_artifact_id", app_js.text)
        self.assertIn("Annotated paper presentation", app_js.text)
        self.assertIn("Markdown preview", app_js.text)
        self.assertIn("window.open(reportDownloadUrl(report), \"_blank\", \"noopener,noreferrer\")", app_js.text)
        self.assertIn("report.source_artifact_id", app_js.text)
        self.assertIn('return `/artifacts/${encodeURIComponent(report.source_artifact_id)}/download`;', app_js.text)
        self.assertIn('getQueryParam("report_id")', app_js.text)
        self.assertIn("loadArchivedReport", app_js.text)
        self.assertIn("/reports/${encodeURIComponent(reportId)}", app_js.text)

        app_css = self.client.get("/static/app.css")
        self.assertEqual(app_css.status_code, 200)
        self.assertIn(".flash-message", app_css.text)

    def test_detail_endpoints_back_frontend_views(self) -> None:
        run_response = self.client.get(f"/runs/{self.run.id}")
        self.assertEqual(run_response.status_code, 200)
        self.assertEqual(run_response.json()["status"], "waiting_input")

        report_response = self.client.get(f"/reports/{self.report.id}")
        self.assertEqual(report_response.status_code, 200)
        self.assertEqual(report_response.json()["title"], "Paper 2403.01234")
        self.assertEqual(report_response.json()["metadata"], {"presentation_mode": "raw_review"})
        self.assertEqual(report_response.json()["filter_keys"], ["report"])
        self.assertEqual(report_response.json()["tag_keys"], ["arxiv_paper_analysis"])

        taxonomy_response = self.client.get("/reports/taxonomy")
        self.assertEqual(taxonomy_response.status_code, 200)
        self.assertEqual(
            [item["key"] for item in taxonomy_response.json()["filters"]],
            ["report", "daily", "investment", "semiconductor"],
        )

        artifact_preview = self.client.get(f"/artifacts/{self.artifact.id}/preview")
        self.assertEqual(artifact_preview.status_code, 200)
        self.assertEqual(artifact_preview.json()["status"], "text")
        self.assertIn("Alpha beta gamma", artifact_preview.json()["content"])

        artifact_download = self.client.get(f"/artifacts/{self.artifact.id}/download")
        self.assertEqual(artifact_download.status_code, 200)
        self.assertIn("Paper Review", artifact_download.text)

        interaction_response = self.client.get(f"/interactions/{self.interaction.id}")
        self.assertEqual(interaction_response.status_code, 200)
        self.assertEqual(interaction_response.json()["status"], "open")

    def test_research_report_revisions_and_csv_preview(self) -> None:
        revisions_response = self.client.get(f"/reports/{self.research_report_v2.id}/revisions")
        self.assertEqual(revisions_response.status_code, 200)
        payload = revisions_response.json()
        self.assertEqual(payload["report_series_id"], "edge-ai")
        self.assertEqual([item["revision_number"] for item in payload["revisions"]], [2, 1])
        self.assertTrue(payload["revisions"][0]["is_current"])
        self.assertFalse(payload["revisions"][1]["is_current"])
        self.assertEqual(payload["revisions"][0]["filter_keys"], ["report"])
        self.assertEqual(payload["revisions"][0]["tag_keys"], ["deep_research"])

        csv_preview = self.client.get(f"/artifacts/{self.research_csv_artifact.id}/preview")
        self.assertEqual(csv_preview.status_code, 200)
        self.assertEqual(csv_preview.json()["status"], "text")
        self.assertIn("region,value", csv_preview.json()["content"])
        self.assertIn("US-East,140", csv_preview.json()["content"])

        csv_download = self.client.get(f"/artifacts/{self.research_csv_artifact.id}/download")
        self.assertEqual(csv_download.status_code, 200)
        self.assertIn("region,value", csv_download.text)

    def test_command_routes_accept_research_reports_and_source_driven_drafts(self) -> None:
        submit_mock = AsyncMock(
            side_effect=[
                {"run_id": "run-research", "status": "pending", "mode": "local"},
                {"run_id": "run-draft", "status": "pending", "mode": "local"},
            ]
        )
        with patch.object(OrchestrationService, "submit_flow", submit_mock):
            research_response = self.client.post(
                "/commands/research-report",
                json={
                    "theme": "Edge AI infrastructure",
                    "boundaries": ["North America"],
                    "areas_of_interest": ["power supply", "GPU lead times"],
                    "report_key": "edge-ai",
                    "edit_mode": "merge",
                    "human_policy": {"mode": "auto"},
                },
            )
            draft_response = self.client.post(
                "/commands/draft-article",
                json={"source_report_id": self.research_report_v1.id},
            )

        self.assertEqual(research_response.status_code, 200)
        self.assertEqual(draft_response.status_code, 200)
        self.assertEqual(submit_mock.await_args_list[0].kwargs["flow_name"], "research_report_flow")
        self.assertEqual(submit_mock.await_args_list[0].kwargs["parameters"]["report_key"], "edge-ai")
        self.assertEqual(submit_mock.await_args_list[1].kwargs["flow_name"], "substack_draft_flow")
        self.assertEqual(
            submit_mock.await_args_list[1].kwargs["parameters"]["source_report_id"],
            self.research_report_v1.id,
        )

    def test_command_route_accepts_search_report_requests(self) -> None:
        submit_mock = AsyncMock(return_value={"run_id": "run-search", "status": "pending", "mode": "local"})
        with patch.object(OrchestrationService, "submit_flow", submit_mock):
            response = self.client.post(
                "/commands/search-report",
                json={
                    "prompt": "Create a 10 day Tokyo itinerary focused on food and museums.",
                    "resume_from_run_id": "run-123",
                    "codex_session_id": "session-explicit",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(submit_mock.await_args.kwargs["flow_name"], "codex_search_report_flow")
        payload = submit_mock.await_args.kwargs["parameters"]
        self.assertEqual(payload["prompt"], "Create a 10 day Tokyo itinerary focused on food and museums.")
        self.assertEqual(payload["resume_from_run_id"], "run-123")
        self.assertEqual(payload["codex_session_id"], "session-explicit")

    def test_command_route_accepts_artifact_ingest_requests(self) -> None:
        submit_mock = AsyncMock(return_value={"run_id": "run-ingest", "status": "pending", "mode": "local"})
        with patch.object(OrchestrationService, "submit_flow", submit_mock):
            response = self.client.post(
                "/commands/artifact-ingest",
                json={
                    "items": [
                        {
                            "input_kind": "inline",
                            "content": "# Note\n\nA short markdown artifact.",
                            "content_format": "markdown",
                            "title": "Note",
                            "tags": ["kb", "markdown"],
                            "metadata": {"source": "unit-test"},
                        }
                    ]
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(submit_mock.await_args.kwargs["flow_name"], "artifact_ingest_flow")
        payload = submit_mock.await_args.kwargs["parameters"]
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["input_kind"], "inline")
        self.assertEqual(payload["items"][0]["content_format"], "markdown")
        self.assertEqual(payload["items"][0]["title"], "Note")
        self.assertEqual(payload["items"][0]["tags"], ["kb", "markdown"])

    def test_query_knowledge_returns_ingested_source_chunks(self) -> None:
        source_path = self.root / "artifacts" / "ingested-note.md"
        source_path.write_text("shared ingest context for downstream research", encoding="utf-8")
        canonical_uri = "file:///knowledge/ingested-note.md"

        run = self.repository.start_run(
            flow_name="artifact_ingest_flow",
            worker_key="artifact_ingest_runner",
            input_payload={"items": [{"input_kind": "file", "file_path": str(source_path)}]},
            status="running",
        )
        persisted = self.repository.persist_adapter_result(
            run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="source-text",
                        path=str(source_path),
                        storage_uri=source_path.as_uri(),
                        size_bytes=source_path.stat().st_size,
                        media_type="text/markdown",
                        metadata={
                            "role": "normalized_text",
                            "input_index": 0,
                            "canonical_uri": canonical_uri,
                            "source_type": "markdown",
                        },
                    )
                ],
            ),
        )
        assert persisted is not None
        artifact = persisted.artifacts[0]
        source = self.repository.upsert_source_document(
            canonical_uri=canonical_uri,
            source_type="markdown",
            title="Ingested note",
            current_text_artifact_id=artifact.id,
            metadata_json={"tags": ["kb"]},
        )
        self.repository.assign_artifact_to_source(artifact.id, source.id)
        self.repository.upsert_chunks(
            artifact_id=artifact.id,
            chunks=[
                {
                    "ordinal": 0,
                    "text": "shared ingest context for downstream research",
                    "token_count": 6,
                    "metadata": {"canonical_uri": canonical_uri, "source_type": "markdown"},
                }
            ],
        )

        response = self.client.post(
            "/commands/query-knowledge",
            json={"query": "downstream research", "limit": 5},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["query"], "downstream research")
        self.assertEqual(len(payload["results"]), 1)
        self.assertIn("shared ingest context", payload["results"][0]["text"])

    def test_browser_job_route_accepts_extended_payload_shape(self) -> None:
        submit_mock = AsyncMock(return_value={"run_id": "run-browser", "status": "pending", "mode": "prefect"})
        with patch.object(OrchestrationService, "submit_flow", submit_mock):
            response = self.client.post(
                "/commands/browser-job",
                json={
                    "job_name": "x-capture",
                    "target_url": "https://x.com/home",
                    "session": {"mode": "attach", "cdp_url": "http://127.0.0.1:9222", "profile_name": "main"},
                    "timeout_seconds": 90,
                    "steps": [
                        {"op": "wait_for", "selector": "main"},
                        {"op": "screenshot", "name": "timeline"},
                    ],
                    "extract": [{"name": "first_post", "selector": "article", "kind": "text"}],
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(submit_mock.await_args.kwargs["flow_name"], "browser_job_flow")
        payload = submit_mock.await_args.kwargs["parameters"]
        self.assertEqual(payload["session"]["mode"], "attach")
        self.assertEqual(payload["steps"][0]["op"], "wait_for")
        self.assertEqual(payload["extract"][0]["name"], "first_post")

    def test_search_report_artifacts_and_revisions_surface_via_api(self) -> None:
        report_path_v1 = self.root / "artifacts" / "search-report-v1.md"
        report_path_v2 = self.root / "artifacts" / "search-report-v2.md"
        memo_path_v2 = self.root / "artifacts" / "search-report-v2-memo.json"
        report_path_v1.write_text("# Tokyo Search Report\n\nVersion 1\n", encoding="utf-8")
        report_path_v2.write_text("# Tokyo Search Report\n\nVersion 2\n", encoding="utf-8")
        memo_path_v2.write_text('{"resume_summary":"done","open_questions":[]}', encoding="utf-8")

        first_run = self.repository.start_run(
            flow_name="codex_search_report_flow",
            worker_key="mini-process",
            input_payload={"prompt": "tokyo"},
            status="running",
        )
        first_persisted = self.repository.persist_adapter_result(
            first_run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="search-report",
                        path=str(report_path_v1),
                        storage_uri=report_path_v1.as_uri(),
                        size_bytes=report_path_v1.stat().st_size,
                        media_type="text/markdown",
                        metadata={"role": "report_markdown", "codex_session_id": "session-1"},
                    )
                ],
                reports=[
                    ReportRecord(
                        report_type="search_report",
                        title="Tokyo Search Report",
                        summary="v1",
                        content_markdown=report_path_v1.read_text(encoding="utf-8"),
                        artifact_path=str(report_path_v1),
                        report_series_id="search-report:test",
                        revision_number=1,
                        is_current=False,
                        metadata={
                            "codex_session_id": "session-1",
                            "resume_source": "fresh",
                            "taxonomy": {"filters": ["report"], "tags": ["search_report"]},
                        },
                    )
                ],
            ),
        )
        assert first_persisted is not None
        first_report = first_persisted.reports[0]
        self.repository.promote_report_revision(first_report.id)

        second_run = self.repository.start_run(
            flow_name="codex_search_report_flow",
            worker_key="mini-process",
            input_payload={"prompt": "tokyo refine", "resume_from_run_id": first_run.id},
            status="running",
        )
        second_persisted = self.repository.persist_adapter_result(
            second_run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                artifact_manifest=[
                    ArtifactRecord(
                        kind="search-report",
                        path=str(report_path_v2),
                        storage_uri=report_path_v2.as_uri(),
                        size_bytes=report_path_v2.stat().st_size,
                        media_type="text/markdown",
                        metadata={"role": "report_markdown", "codex_session_id": "session-2"},
                    ),
                    ArtifactRecord(
                        kind="search-report-memo",
                        path=str(memo_path_v2),
                        storage_uri=memo_path_v2.as_uri(),
                        size_bytes=memo_path_v2.stat().st_size,
                        media_type="application/json",
                        metadata={"role": "resume_memo", "codex_session_id": "session-2"},
                    ),
                ],
                reports=[
                    ReportRecord(
                        report_type="search_report",
                        title="Tokyo Search Report",
                        summary="v2",
                        content_markdown=report_path_v2.read_text(encoding="utf-8"),
                        artifact_path=str(report_path_v2),
                        report_series_id="search-report:test",
                        revision_number=2,
                        supersedes_report_id=first_report.id,
                        is_current=False,
                        metadata={
                            "codex_session_id": "session-2",
                            "resume_source": "session_resume",
                            "taxonomy": {"filters": ["report"], "tags": ["search_report"]},
                        },
                    )
                ],
            ),
        )
        assert second_persisted is not None
        second_report = second_persisted.reports[0]
        memo_artifact = second_persisted.artifacts[1]
        self.repository.promote_report_revision(second_report.id)

        reports_response = self.client.get("/reports?limit=50")
        self.assertEqual(reports_response.status_code, 200)
        stored = next(item for item in reports_response.json()["reports"] if item["id"] == second_report.id)
        self.assertEqual(stored["filter_keys"], ["report"])
        self.assertIn("search_report", stored["tag_keys"])
        self.assertIsNotNone(stored["source_artifact_id"])

        revisions_response = self.client.get(f"/reports/{second_report.id}/revisions")
        self.assertEqual(revisions_response.status_code, 200)
        self.assertEqual(
            [item["revision_number"] for item in revisions_response.json()["revisions"][:2]],
            [2, 1],
        )

        memo_preview = self.client.get(f"/artifacts/{memo_artifact.id}/preview")
        self.assertEqual(memo_preview.status_code, 200)
        self.assertEqual(memo_preview.json()["status"], "text")
        self.assertIn('"resume_summary": "done"', memo_preview.json()["content"])

    def test_answer_interaction_promotes_pending_research_revision(self) -> None:
        pending_run = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "Edge AI infrastructure", "report_key": "edge-ai"},
            status="running",
        )
        pending_persisted = self.repository.persist_adapter_result(
            pending_run.id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                reports=[
                    ReportRecord(
                        report_type="research_report",
                        title="Edge AI infrastructure",
                        summary="v3",
                        content_markdown="# Edge AI infrastructure\n\nV3\n",
                        report_series_id="edge-ai",
                        revision_number=3,
                        supersedes_report_id=self.research_report_v2.id,
                        is_current=False,
                        metadata={"report_key": "edge-ai"},
                    )
                ],
            ),
        )
        assert pending_persisted is not None
        pending_report = pending_persisted.reports[0]
        interaction = self.repository.create_interaction(
            run_id=pending_run.id,
            title="Promote research revision 3",
            prompt_md="Approve revision 3?",
            input_schema={
                "type": "object",
                "properties": {"approved": {"type": "boolean"}},
                "required": ["approved"],
            },
            default_input={"approved": False},
            checkpoint_key="promote_revision",
            ui_hints={
                "report_id": pending_report.id,
                "report_series_id": "edge-ai",
                "revision_number": 3,
            },
        )
        self.repository.update_run_status(
            pending_run.id,
            status="waiting_input",
            structured_outputs={
                "report_key": "edge-ai",
                "revision_number": 3,
                "current_report_id": self.research_report_v2.id,
                "pending_report_id": pending_report.id,
                "checkpoint": {
                    "status": "waiting_input",
                    "interaction_id": interaction.id,
                    "checkpoint_key": "promote_revision",
                },
            },
        )

        response = self.client.post(
            f"/interactions/{interaction.id}/answer",
            json={"response": {"approved": True}},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["interaction"]["status"], "answered")

        current = self.repository.current_report_revision("edge-ai")
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.id, pending_report.id)

        updated_run = self.repository.get_run(pending_run.id)
        self.assertIsNotNone(updated_run)
        assert updated_run is not None
        self.assertEqual(updated_run.status, "completed")
        self.assertEqual(updated_run.structured_outputs["current_report_id"], pending_report.id)
        self.assertIsNone(updated_run.structured_outputs["pending_report_id"])
        self.assertEqual(updated_run.structured_outputs["checkpoint"]["resolution"], "approved")
