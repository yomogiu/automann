from __future__ import annotations

from datetime import date
from typing import Any

from libs.config import Settings
from libs.contracts.models import AdapterResult, ObservationRecord, ReportRecord, WorkerStatus
from libs.contracts.workers import DailyBriefAnalysisOutput, DailyBriefAnalysisRequest

from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text


class AnalysisRunner:
    worker_key = "analysis_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def compile_daily_brief(self, request: DailyBriefAnalysisRequest) -> AdapterResult:
        brief_date = request.brief_date
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        markdown_path = run_dir / f"daily_brief_{brief_date.isoformat()}.md"
        manifest_path = run_dir / "analysis_manifest.json"

        news_items = list(request.news_items)
        papers = list(request.papers)
        browser_status = request.browser_summary.get("status") if request.browser_summary else "n/a"
        previous_title = request.previous_report.get("title") if request.previous_report else "No previous report"

        markdown = self._render_daily_brief(
            brief_date=brief_date,
            news_items=news_items,
            papers=papers,
            browser_status=str(browser_status),
            previous_title=previous_title,
        )
        write_text(markdown_path, markdown)
        output = DailyBriefAnalysisOutput(
            brief_date=brief_date.isoformat(),
            news_count=len(news_items),
            paper_count=len(papers),
            browser_status=browser_status,
            previous_report_title=previous_title,
        )
        manifest = output.model_dump(mode="json")
        write_json(manifest_path, manifest)

        observations = [
            ObservationRecord(
                kind="daily_delta",
                summary=f"Compared against previous report: {previous_title}",
                payload={"previous_report_title": previous_title},
                confidence=0.5,
            ),
            ObservationRecord(
                kind="coverage_summary",
                summary=f"Compiled {len(news_items)} news items and {len(papers)} papers.",
                payload=manifest,
                confidence=0.8,
            ),
        ]

        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"Compiled daily brief for {brief_date.isoformat()}",
            artifact_manifest=[
                build_file_artifact(
                    kind="daily-brief",
                    path=markdown_path,
                    media_type="text/markdown",
                ),
                build_file_artifact(
                    kind="analysis-manifest",
                    path=manifest_path,
                    media_type="application/json",
                ),
            ],
            structured_outputs=manifest,
            observations=observations,
            reports=[
                ReportRecord(
                    report_type="daily_brief",
                    title=f"Daily Brief {brief_date.isoformat()}",
                    summary=f"{len(news_items)} news items, {len(papers)} papers, browser lane {browser_status}.",
                    content_markdown=markdown,
                    artifact_path=str(markdown_path),
                    metadata={
                        **manifest,
                        "taxonomy": {
                            "filters": ["daily"],
                            "tags": ["synthesis", "scrapes"],
                        },
                    },
                )
            ],
        )

    def _render_daily_brief(
        self,
        *,
        brief_date: date,
        news_items: list[dict[str, Any]],
        papers: list[dict[str, Any]],
        browser_status: str,
        previous_title: str,
    ) -> str:
        lines = [
            f"# Daily Brief {brief_date.isoformat()}",
            "",
            "## Snapshot",
            f"- Previous report: {previous_title}",
            f"- Browser lane: {browser_status}",
            f"- News items: {len(news_items)}",
            f"- Papers: {len(papers)}",
            "",
            "## News",
        ]
        for item in news_items:
            lines.append(f"- {item['headline']} ({item['source']})")
        lines.extend(["", "## Papers"])
        for paper in papers:
            lines.append(f"- {paper['title']}: {paper['summary']}")
        lines.extend(
            [
                "",
                "## Themes",
                "- Local-first orchestration favors explicit shared persistence over shell-based handoffs.",
                "- Storage-aware infrastructure decisions matter more than a generic preference for one runtime host.",
            ]
        )
        return "\n".join(lines)
