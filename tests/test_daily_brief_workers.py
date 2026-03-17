from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from libs.config import get_settings
from libs.contracts.workers import (
    ArxivFeedIngestRequest,
    DailyBriefAnalysisRequest,
    NewsIngestRequest,
)
from workers.analysis_runner import AnalysisRunner
from workers.ingest_runner import ArxivReviewRunner, NewsScrapeRunner


class DailyBriefWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.settings = get_settings().model_copy(
            update={
                "artifact_root": self.root / "artifacts",
                "report_root": self.root / "reports",
                "runtime_root": self.root / "runtime",
            }
        )
        self.settings.artifact_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_news_ingest_uses_worker_request_and_emits_typed_output(self) -> None:
        runner = NewsScrapeRunner(self.settings)
        request = NewsIngestRequest(
            brief_date=date(2026, 3, 16),
            seed_news=[
                {
                    "source": "wire",
                    "headline": "Chip foundry demand rises",
                    "url": "https://example.invalid/news/chip-demand",
                    "topic": "semiconductors",
                }
            ]
        )

        result = runner.run(request)

        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["count"], 1)
        self.assertEqual(result.structured_outputs["items"][0]["headline"], "Chip foundry demand rises")
        self.assertEqual(result.artifact_manifest[0].kind, "news-feed")

    def test_arxiv_ingest_uses_worker_request_and_emits_typed_output(self) -> None:
        runner = ArxivReviewRunner(self.settings)
        request = ArxivFeedIngestRequest(
            brief_date=date(2026, 3, 16),
            seed_arxiv=[
                {
                    "paper_id": "2601.12345",
                    "title": "Composable Agent Workflows",
                    "summary": "Shows typed handoffs for long-running automation.",
                    "authors": ["Test Author"],
                }
            ]
        )

        result = runner.ingest_feed(request)

        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["count"], 1)
        self.assertEqual(result.structured_outputs["papers"][0]["paper_id"], "2601.12345")
        self.assertEqual(result.artifact_manifest[0].kind, "paper-feed")

    def test_analysis_runner_accepts_single_request_and_preserves_metadata_shape(self) -> None:
        runner = AnalysisRunner(self.settings)
        request = DailyBriefAnalysisRequest(
            brief_date=date(2026, 3, 16),
            news_items=[
                {
                    "source": "wire",
                    "headline": "Local-first runtime costs stabilize",
                    "url": "https://example.invalid/news/runtime-costs",
                    "topic": "infrastructure",
                }
            ],
            papers=[
                {
                    "paper_id": "2603.00001",
                    "title": "Typed Contracts for Automation",
                    "summary": "Contract-first worker handoffs reduce hidden coupling.",
                }
            ],
            browser_summary={"status": "skipped"},
            previous_report={"title": "Daily Brief 2026-03-15"},
        )

        result = runner.compile_daily_brief(request)

        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["brief_date"], "2026-03-16")
        self.assertEqual(result.structured_outputs["news_count"], 1)
        self.assertEqual(result.structured_outputs["paper_count"], 1)
        self.assertEqual(result.structured_outputs["browser_status"], "skipped")
        self.assertEqual(result.artifact_manifest[0].kind, "daily-brief")
        self.assertEqual(result.artifact_manifest[1].kind, "analysis-manifest")
        self.assertEqual(result.reports[0].report_type, "daily_brief")
        self.assertEqual(result.reports[0].metadata["taxonomy"]["filters"], ["daily"])
        self.assertEqual(result.reports[0].metadata["taxonomy"]["tags"], ["synthesis", "scrapes"])
