from __future__ import annotations

from enum import StrEnum


class EventName(StrEnum):
    NEWS_INGEST_COMPLETED = "news.ingest.completed"
    PAPER_REVIEW_COMPLETED = "paper.review.completed"
    BROWSER_SCRAPE_COMPLETED = "browser.scrape.completed"
    DRAFT_GENERATED = "draft.generated"
    REPORT_PUBLISHED = "report.published"
