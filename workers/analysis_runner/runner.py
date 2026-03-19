from __future__ import annotations

from collections import Counter
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
        brief_metadata = dict(request.brief_metadata)
        browser_status = request.browser_summary.get("status") if request.browser_summary else "n/a"
        previous_title = request.previous_report.get("title") if request.previous_report else "No previous report"
        tracked_news_sources = _tracked_news_source_names(brief_metadata)
        captured_news_sources = _captured_news_source_names(news_items)
        analysis_focus = _analysis_focus(brief_metadata)

        markdown = self._render_daily_brief(
            brief_date=brief_date,
            news_items=news_items,
            papers=papers,
            browser_status=str(browser_status),
            previous_title=previous_title,
            tracked_news_sources=tracked_news_sources,
            analysis_focus=analysis_focus,
        )
        write_text(markdown_path, markdown)
        output = DailyBriefAnalysisOutput(
            brief_date=brief_date.isoformat(),
            news_count=len(news_items),
            paper_count=len(papers),
            browser_status=browser_status,
            previous_report_title=previous_title,
            tracked_news_sources=tracked_news_sources,
            captured_news_sources=captured_news_sources,
            analysis_focus=analysis_focus,
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
        tracked_news_sources: list[str],
        analysis_focus: str | None,
    ) -> str:
        captured_news_sources = _captured_news_source_names(news_items)
        grouped_news = _group_news_items(news_items)
        analysis_lines = self._analysis_lines(
            news_items=news_items,
            papers=papers,
            tracked_news_sources=tracked_news_sources,
            captured_news_sources=captured_news_sources,
            analysis_focus=analysis_focus,
        )
        open_questions = self._open_questions(
            news_items=news_items,
            papers=papers,
            tracked_news_sources=tracked_news_sources,
            captured_news_sources=captured_news_sources,
            analysis_focus=analysis_focus,
        )
        lines = [
            f"# Daily Brief {brief_date.isoformat()}",
            "",
            "## Coverage",
            f"- Previous report: {previous_title}",
            f"- Browser lane: {browser_status}",
            f"- Tracked news sources: {len(tracked_news_sources)}",
            f"- Sources with items: {len(captured_news_sources)}",
            f"- News items: {len(news_items)}",
            f"- Papers: {len(papers)}",
        ]
        if analysis_focus:
            lines.append(f"- Analysis focus: {analysis_focus}")

        lines.extend(["", "## News"])
        if not grouped_news:
            lines.append("- No news items were captured for the configured window.")
        else:
            for source_name, items in grouped_news:
                lines.extend(["", f"### {source_name}"])
                for item in items:
                    lines.append(self._render_news_item(item))

        lines.extend(["", "## Papers"])
        if not papers:
            lines.append("- No papers were included in this run.")
        else:
            for paper in papers:
                lines.append(self._render_paper_item(paper))

        lines.extend(["", "## Analysis"])
        if analysis_lines:
            lines.extend(f"- {item}" for item in analysis_lines)
        else:
            lines.append("- The current brief did not surface enough signals for synthesis.")

        lines.extend(["", "## Open Questions"])
        if open_questions:
            lines.extend(f"- {item}" for item in open_questions)
        else:
            lines.append("- Which of these items should be promoted into a deeper report?")
        return "\n".join(lines)

    def _render_news_item(self, item: dict[str, Any]) -> str:
        headline = str(item.get("headline") or "Untitled headline")
        url = str(item.get("url") or "").strip()
        summary = str(item.get("summary") or "").strip()
        published_at = str(item.get("published_at") or "").strip()
        topic = str(item.get("topic") or "").strip()

        label = f"[{headline}]({url})" if url else headline
        details = [label]
        if published_at:
            details.append(f"published {published_at}")
        if topic:
            details.append(f"topic {topic}")
        if summary:
            details.append(summary)
        return "- " + " | ".join(details)

    def _render_paper_item(self, paper: dict[str, Any]) -> str:
        title = str(paper.get("title") or "Untitled paper")
        summary = str(paper.get("summary") or "").strip()
        paper_id = str(paper.get("paper_id") or "").strip()
        url = str(paper.get("url") or "").strip()
        if not url and paper_id:
            url = f"https://arxiv.org/abs/{paper_id}"
        label = f"[{title}]({url})" if url else title
        return f"- {label}" + (f" | {summary}" if summary else "")

    def _analysis_lines(
        self,
        *,
        news_items: list[dict[str, Any]],
        papers: list[dict[str, Any]],
        tracked_news_sources: list[str],
        captured_news_sources: list[str],
        analysis_focus: str | None,
    ) -> list[str]:
        lines: list[str] = []
        if analysis_focus:
            lines.append(f"Requested focus: {analysis_focus}.")
        if news_items:
            source_counts = Counter(str(item.get("source") or "unknown") for item in news_items)
            top_source, top_count = source_counts.most_common(1)[0]
            lines.append(f"Most active source in this brief is {top_source} with {top_count} captured items.")
        if tracked_news_sources:
            missing_sources = [source for source in tracked_news_sources if source not in captured_news_sources]
            if missing_sources:
                preview = ", ".join(missing_sources[:3])
                suffix = "..." if len(missing_sources) > 3 else ""
                lines.append(f"No recent items were captured for {preview}{suffix}.")
        topic_counts = Counter(str(item.get("topic") or "").strip() for item in news_items if str(item.get("topic") or "").strip())
        recurring_topics = [topic for topic, count in topic_counts.items() if count > 1]
        if recurring_topics:
            lines.append(f"Recurring topics across the brief: {', '.join(recurring_topics[:3])}.")
        elif topic_counts:
            top_topic, top_topic_count = topic_counts.most_common(1)[0]
            lines.append(f"The dominant tagged topic is {top_topic} with {top_topic_count} item(s).")
        if news_items and papers:
            lines.append("The paper lane can be used to validate or challenge the news themes above.")
        elif news_items and not papers:
            lines.append("This brief is news-heavy; add paper coverage if you want validation against research.")
        return lines

    def _open_questions(
        self,
        *,
        news_items: list[dict[str, Any]],
        papers: list[dict[str, Any]],
        tracked_news_sources: list[str],
        captured_news_sources: list[str],
        analysis_focus: str | None,
    ) -> list[str]:
        questions: list[str] = []
        if tracked_news_sources:
            missing_sources = [source for source in tracked_news_sources if source not in captured_news_sources]
            if missing_sources:
                questions.append("Do any of the missing tracked sources need an explicit RSS or Atom feed URL?")
        if analysis_focus and news_items:
            questions.append(f"Which captured item most directly advances the requested focus on {analysis_focus}?")
        elif news_items:
            questions.append("Which captured item should be promoted into a deeper research report?")
        if papers and news_items:
            questions.append("Which news claims should be cross-checked against the papers in this brief?")
        elif not papers:
            questions.append("Should the paper lane stay enabled for this brief definition?")
        return questions[:3]


def _tracked_news_source_names(metadata: dict[str, Any]) -> list[str]:
    raw_sources = metadata.get("news_sources")
    if raw_sources is None:
        raw_sources = metadata.get("news_sites")
    if not isinstance(raw_sources, list):
        return []

    names: list[str] = []
    for item in raw_sources:
        if isinstance(item, str):
            value = item.strip()
        elif isinstance(item, dict):
            value = str(item.get("name") or item.get("source") or item.get("url") or item.get("site_url") or item.get("feed_url") or "").strip()
        else:
            value = ""
        if value and value not in names:
            names.append(value)
    return names


def _captured_news_source_names(news_items: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    for item in news_items:
        value = str(item.get("source") or "").strip()
        if value and value not in names:
            names.append(value)
    return names


def _analysis_focus(metadata: dict[str, Any]) -> str | None:
    value = str(metadata.get("analysis_focus") or metadata.get("focus") or "").strip()
    return value or None


def _group_news_items(news_items: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    grouped: list[tuple[str, list[dict[str, Any]]]] = []
    index_by_source: dict[str, int] = {}
    for item in news_items:
        source = str(item.get("source") or "Unknown source").strip() or "Unknown source"
        if source not in index_by_source:
            index_by_source[source] = len(grouped)
            grouped.append((source, []))
        grouped[index_by_source[source]][1].append(item)
    return grouped
