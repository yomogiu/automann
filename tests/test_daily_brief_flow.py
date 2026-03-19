from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import (
    AdapterResult,
    EventSuggestion,
    ObservationRecord,
    ReportRecord,
    WorkerStatus,
)
from libs.contracts.workers import (
    ArxivFeedIngestOutput,
    ArxivFeedIngestRequest,
    DailyBriefAnalysisOutput,
    DailyBriefAnalysisRequest,
    NewsIngestOutput,
    NewsIngestRequest,
    PublishOutput,
    PublishRequest,
)
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from flows.daily_brief import daily_brief_flow
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text


class DailyBriefFlowTests(unittest.TestCase):
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

        settings = get_settings()
        bootstrap_life_database(settings)
        self.engine = engine_for_url(settings.life_database_url)
        self.repository = LifeRepository(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def _execute_adapter_inline(
        self,
        *,
        flow_name: str,
        worker_key: str,
        input_payload: dict,
        runner,
        parent_run_id: str | None = None,
    ) -> dict:
        run_record = self.repository.start_run(
            flow_name=flow_name,
            worker_key=worker_key,
            input_payload=input_payload,
            parent_run_id=parent_run_id,
            status="running",
        )
        result = runner()
        persisted = self.repository.persist_adapter_result(run_record.id, result)
        assert persisted is not None
        return {
            "run_id": run_record.id,
            "parent_run_id": parent_run_id,
            "status": result.status.value,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "structured_outputs": result.structured_outputs,
            "artifacts": [
                {
                    "id": item.id,
                    "kind": item.kind,
                    "path": item.path,
                    "storage_uri": item.storage_uri,
                    "size_bytes": item.size_bytes,
                    "media_type": item.media_type,
                    "sha256": item.sha256,
                    "metadata": item.metadata_json,
                }
                for item in persisted.artifacts
            ],
            "observations": [item.model_dump(mode="json") for item in result.observations],
            "reports": [
                {
                    "id": item.id,
                    "report_type": item.report_type,
                    "title": item.title,
                    "summary": item.summary,
                    "content_markdown": item.content_markdown,
                    "source_artifact_id": item.source_artifact_id,
                    "metadata": item.metadata_json,
                }
                for item in persisted.reports
            ],
            "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
        }

    def test_flow_translates_command_payloads_into_worker_requests(self) -> None:
        payload = {
            "date": "2026-03-15T12:00:00Z",
            "publish": True,
            "metadata": {
                "seed_news": [
                    {
                        "source": "seed",
                        "headline": "NY jobs market shifts toward local AI ops roles",
                        "url": "https://example.invalid/news/jobs",
                        "topic": "jobs",
                    }
                ],
                "seed_arxiv": [
                    {
                        "paper_id": "2603.00001",
                        "title": "Agent Planning at Runtime",
                        "summary": "Studies staged planning.",
                        "authors": ["Ada Example"],
                    }
                ],
            },
        }
        parent = self.repository.start_run(
            flow_name="daily_brief_flow",
            worker_key="mini-process",
            input_payload=payload,
            status="pending",
        )
        captured: dict[str, object] = {}

        def fake_news_run(_runner_self, request: NewsIngestRequest) -> AdapterResult:
            captured["news_request"] = request
            run_dir = ensure_worker_dir(get_settings(), "news_scrape_runner_test")
            output = NewsIngestOutput(
                generated_at="2026-03-15T12:00:00Z",
                items=list(request.seed_news),
                count=len(request.seed_news),
            )
            artifact_path = run_dir / "news_ingest.json"
            write_json(artifact_path, output.model_dump(mode="json"))
            captured["news_artifact_path"] = str(artifact_path)
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="news ok",
                artifact_manifest=[
                    build_file_artifact(kind="news-feed", path=artifact_path, media_type="application/json")
                ],
                structured_outputs=output.model_dump(mode="json"),
                observations=[
                    ObservationRecord(kind="news_item", summary="news", payload={"count": output.count}, confidence=0.5)
                ],
                next_suggested_events=[EventSuggestion(name="news.ingest.completed", payload={"count": output.count})],
            )

        def fake_arxiv_run(_runner_self, request: ArxivFeedIngestRequest) -> AdapterResult:
            captured["arxiv_request"] = request
            run_dir = ensure_worker_dir(get_settings(), "arxiv_review_runner_test")
            output = ArxivFeedIngestOutput(
                generated_at="2026-03-15T12:05:00Z",
                papers=list(request.seed_arxiv),
                count=len(request.seed_arxiv),
            )
            artifact_path = run_dir / "arxiv_ingest.json"
            write_json(artifact_path, output.model_dump(mode="json"))
            captured["arxiv_artifact_path"] = str(artifact_path)
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="arxiv ok",
                artifact_manifest=[
                    build_file_artifact(kind="paper-feed", path=artifact_path, media_type="application/json")
                ],
                structured_outputs=output.model_dump(mode="json"),
                observations=[
                    ObservationRecord(kind="paper_candidate", summary="paper", payload={"count": output.count}, confidence=0.5)
                ],
            )

        def fake_analysis_run(_runner_self, request: DailyBriefAnalysisRequest) -> AdapterResult:
            captured["analysis_request"] = request
            run_dir = ensure_worker_dir(get_settings(), "analysis_runner_test")
            artifact_path = run_dir / "daily_brief.md"
            write_text(artifact_path, "# Daily Brief\n")
            output = DailyBriefAnalysisOutput(
                brief_date=request.brief_date.isoformat(),
                news_count=len(request.news_items),
                paper_count=len(request.papers),
                browser_status=str((request.browser_summary or {}).get("status")),
                previous_report_title=(request.previous_report or {}).get("title", "No previous report"),
            )
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="analysis ok",
                artifact_manifest=[
                    build_file_artifact(kind="daily-brief", path=artifact_path, media_type="text/markdown")
                ],
                structured_outputs=output.model_dump(mode="json"),
                reports=[
                    ReportRecord(
                        report_type="daily_brief",
                        title="Daily Brief 2026-03-15",
                        summary="summary",
                        content_markdown="# Daily Brief\n",
                        artifact_path=str(artifact_path),
                        metadata={
                            **output.model_dump(mode="json"),
                            "taxonomy": {"filters": ["daily"], "tags": ["synthesis", "scrapes"]},
                        },
                    )
                ],
            )

        def fake_publish_run(_runner_self, request: PublishRequest) -> AdapterResult:
            captured["publish_request"] = request
            run_dir = ensure_worker_dir(get_settings(), "github_publisher_test")
            artifact_path = run_dir / "publication-manifest.json"
            output = PublishOutput(
                publish_mode="local-bundle",
                release_tag="life-20260315T120000Z",
                bundle_dir=str(run_dir),
                manifest_path=str(artifact_path),
            )
            write_json(artifact_path, output.model_dump(mode="json"))
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="publish ok",
                artifact_manifest=[
                    build_file_artifact(
                        kind="publication-manifest",
                        path=artifact_path,
                        media_type="application/json",
                    )
                ],
                structured_outputs=output.model_dump(mode="json"),
            )

        with patch("flows.daily_brief.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.daily_brief.NewsScrapeRunner.run", autospec=True, side_effect=fake_news_run):
                with patch("flows.daily_brief.ArxivReviewRunner.ingest_feed", autospec=True, side_effect=fake_arxiv_run):
                    with patch(
                        "flows.daily_brief.AnalysisRunner.compile_daily_brief",
                        autospec=True,
                        side_effect=fake_analysis_run,
                    ):
                        with patch("flows.daily_brief.GitHubPublisher.run", autospec=True, side_effect=fake_publish_run):
                            result = daily_brief_flow.fn(request=payload, run_id=parent.id)

        self.assertEqual(result["date"], "2026-03-15")
        self.assertIsNotNone(result["published"])
        self.assertIsInstance(captured["news_request"], NewsIngestRequest)
        self.assertEqual(captured["news_request"].brief_date.isoformat(), "2026-03-15")
        self.assertEqual(captured["news_request"].seed_news, payload["metadata"]["seed_news"])
        self.assertIsInstance(captured["arxiv_request"], ArxivFeedIngestRequest)
        self.assertEqual(captured["arxiv_request"].seed_arxiv, payload["metadata"]["seed_arxiv"])
        self.assertIsInstance(captured["analysis_request"], DailyBriefAnalysisRequest)
        self.assertEqual(captured["analysis_request"].news_items, payload["metadata"]["seed_news"])
        self.assertEqual(captured["analysis_request"].papers, payload["metadata"]["seed_arxiv"])
        self.assertEqual(captured["analysis_request"].browser_summary, {"status": "skipped", "reason": "browser_deferred"})
        self.assertEqual(captured["analysis_request"].brief_metadata, payload["metadata"])
        self.assertIsInstance(captured["publish_request"], PublishRequest)
        self.assertEqual(captured["publish_request"].metadata, {"brief_date": "2026-03-15"})
        self.assertEqual(
            [Path(path).resolve() for path in captured["publish_request"].artifact_paths],
            [Path(captured["news_artifact_path"]).resolve(), Path(captured["arxiv_artifact_path"]).resolve()],
        )

        stored_parent = self.repository.get_run(parent.id)
        assert stored_parent is not None
        self.assertEqual(stored_parent.input_payload, payload)

        child_runs = [
            row
            for row in self.repository.list_runs(limit=10, top_level_only=False)
            if row.parent_run_id == parent.id
        ]
        worker_payloads = {row.worker_key: row.input_payload for row in child_runs}
        self.assertEqual(worker_payloads["news_scrape_runner"]["brief_date"], "2026-03-15")
        self.assertNotIn("session_id", worker_payloads["news_scrape_runner"])
        self.assertEqual(worker_payloads["arxiv_review_runner"]["seed_arxiv"], payload["metadata"]["seed_arxiv"])
        self.assertEqual(worker_payloads["analysis_runner"]["news_items"], payload["metadata"]["seed_news"])
        self.assertEqual(worker_payloads["analysis_runner"]["brief_metadata"], payload["metadata"])
        self.assertEqual(worker_payloads["github_publisher"]["metadata"], {"brief_date": "2026-03-15"})
        self.assertNotIn("browser_task_runner", worker_payloads)

    def test_flow_honors_disabled_lane_flags_and_skips_child_runs(self) -> None:
        payload = {
            "include_news": False,
            "include_arxiv": False,
            "include_browser_jobs": False,
            "publish": True,
            "metadata": {
                "seed_news": [
                    {
                        "source": "seed",
                        "headline": "Ignored news seed",
                        "url": "https://example.invalid/news/ignored",
                        "topic": "jobs",
                    }
                ],
                "seed_arxiv": [
                    {
                        "paper_id": "2603.00002",
                        "title": "Ignored paper seed",
                        "summary": "Ignored summary.",
                        "authors": ["Ignored Author"],
                    }
                ],
            },
        }
        parent = self.repository.start_run(
            flow_name="daily_brief_flow",
            worker_key="mini-process",
            input_payload=payload,
            status="pending",
        )
        captured: dict[str, object] = {}

        def fake_analysis_run(_runner_self, request: DailyBriefAnalysisRequest) -> AdapterResult:
            captured["analysis_request"] = request
            run_dir = ensure_worker_dir(get_settings(), "analysis_runner_disabled_test")
            artifact_path = run_dir / "daily_brief.md"
            write_text(artifact_path, "# Daily Brief\n")
            output = DailyBriefAnalysisOutput(
                brief_date=request.brief_date.isoformat(),
                news_count=len(request.news_items),
                paper_count=len(request.papers),
                browser_status=str((request.browser_summary or {}).get("status")),
                previous_report_title=(request.previous_report or {}).get("title", "No previous report"),
            )
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="analysis ok",
                artifact_manifest=[
                    build_file_artifact(kind="daily-brief", path=artifact_path, media_type="text/markdown")
                ],
                structured_outputs=output.model_dump(mode="json"),
                reports=[
                    ReportRecord(
                        report_type="daily_brief",
                        title="Daily Brief 2026-03-15",
                        summary="summary",
                        content_markdown="# Daily Brief\n",
                        artifact_path=str(artifact_path),
                        metadata={
                            **output.model_dump(mode="json"),
                            "taxonomy": {"filters": ["daily"], "tags": ["synthesis", "scrapes"]},
                        },
                    )
                ],
            )

        def fake_publish_run(_runner_self, request: PublishRequest) -> AdapterResult:
            captured["publish_request"] = request
            run_dir = ensure_worker_dir(get_settings(), "github_publisher_disabled_test")
            artifact_path = run_dir / "publication-manifest.json"
            output = PublishOutput(
                publish_mode="local-bundle",
                release_tag="life-20260315T120000Z",
                bundle_dir=str(run_dir),
                manifest_path=str(artifact_path),
            )
            write_json(artifact_path, output.model_dump(mode="json"))
            return AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout="publish ok",
                artifact_manifest=[
                    build_file_artifact(
                        kind="publication-manifest",
                        path=artifact_path,
                        media_type="application/json",
                    )
                ],
                structured_outputs=output.model_dump(mode="json"),
            )

        with patch("flows.daily_brief.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.daily_brief.NewsScrapeRunner.run", autospec=True, side_effect=AssertionError("news should not run")):
                with patch("flows.daily_brief.ArxivReviewRunner.ingest_feed", autospec=True, side_effect=AssertionError("arxiv should not run")):
                    with patch(
                        "flows.daily_brief.AnalysisRunner.compile_daily_brief",
                        autospec=True,
                        side_effect=fake_analysis_run,
                    ):
                        with patch("flows.daily_brief.GitHubPublisher.run", autospec=True, side_effect=fake_publish_run):
                            result = daily_brief_flow.fn(request=payload, run_id=parent.id)

        self.assertEqual(result["news"]["status"], "skipped")
        self.assertEqual(result["news"]["structured_outputs"], {"status": "skipped", "reason": "disabled_by_request", "items": [], "count": 0})
        self.assertEqual(result["arxiv"]["status"], "skipped")
        self.assertEqual(result["arxiv"]["structured_outputs"], {"status": "skipped", "reason": "disabled_by_request", "papers": [], "count": 0})
        self.assertEqual(result["browser"]["status"], "skipped")
        self.assertEqual(result["browser"]["structured_outputs"], {"status": "skipped", "reason": "disabled_by_request"})
        self.assertIsInstance(captured["analysis_request"], DailyBriefAnalysisRequest)
        self.assertEqual(captured["analysis_request"].news_items, [])
        self.assertEqual(captured["analysis_request"].papers, [])
        self.assertEqual(captured["analysis_request"].browser_summary, {"status": "skipped", "reason": "disabled_by_request"})
        self.assertEqual(captured["analysis_request"].brief_metadata, payload["metadata"])
        self.assertIsInstance(captured["publish_request"], PublishRequest)
        self.assertEqual(captured["publish_request"].artifact_paths, [])

        child_runs = [
            row
            for row in self.repository.list_runs(limit=10, top_level_only=False)
            if row.parent_run_id == parent.id
        ]
        worker_keys = {row.worker_key for row in child_runs}
        self.assertEqual(worker_keys, {"analysis_runner", "github_publisher"})
