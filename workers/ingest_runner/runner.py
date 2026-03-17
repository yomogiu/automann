from __future__ import annotations

from datetime import datetime, timezone

from libs.config import Settings
from libs.contracts.events import EventName
from libs.contracts.models import (
    AdapterResult,
    DailyBriefRequest,
    EventSuggestion,
    ObservationRecord,
    PaperReviewRequest,
    ReportRecord,
    WorkerStatus,
)

from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text


DEFAULT_NEWS_ITEMS = [
    {
        "source": "synthetic",
        "headline": "Local AI infrastructure costs continue shifting toward storage-heavy deployments",
        "url": "https://example.invalid/news/storage-heavy-deployments",
        "topic": "infrastructure",
    },
    {
        "source": "synthetic",
        "headline": "Open-source orchestration stacks converge on Python-native workflow definitions",
        "url": "https://example.invalid/news/python-native-workflows",
        "topic": "orchestration",
    },
]

DEFAULT_ARXIV_ITEMS = [
    {
        "paper_id": "2503.00001",
        "title": "Event-Driven Research Agents for Persistent Local Knowledge Systems",
        "summary": "Introduces a pragmatic architecture for combining scheduled ingestion with retrieval-backed drafting.",
        "authors": ["Ada Researcher", "Lin Systems"],
    },
    {
        "paper_id": "2503.00002",
        "title": "Durable Human-in-the-Loop Pipelines for Long-Horizon Analysis",
        "summary": "Explores operator approvals, auditability, and persistent workflow state in mixed automation systems.",
        "authors": ["Morgan Workflow"],
    },
]


class NewsScrapeRunner:
    worker_key = "news_scrape_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: DailyBriefRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        items = list(request.metadata.get("seed_news") or DEFAULT_NEWS_ITEMS)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "items": items,
            "count": len(items),
        }
        artifact_path = run_dir / "news_ingest.json"
        write_json(artifact_path, payload)
        observations = [
            ObservationRecord(
                kind="news_item",
                summary=item["headline"],
                payload=item,
                confidence=0.4,
            )
            for item in items
        ]
        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"Ingested {len(items)} news items.",
            artifact_manifest=[
                build_file_artifact(
                    kind="news-feed",
                    path=artifact_path,
                    media_type="application/json",
                    metadata={"count": len(items)},
                )
            ],
            structured_outputs=payload,
            observations=observations,
            next_suggested_events=[
                EventSuggestion(name=EventName.NEWS_INGEST_COMPLETED, payload={"count": len(items)})
            ],
        )


class ArxivReviewRunner:
    worker_key = "arxiv_review_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def ingest_feed(self, request: DailyBriefRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        items = list(request.metadata.get("seed_arxiv") or DEFAULT_ARXIV_ITEMS)
        artifact_path = run_dir / "arxiv_ingest.json"
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "papers": items,
            "count": len(items),
        }
        write_json(artifact_path, payload)
        observations = [
            ObservationRecord(
                kind="paper_candidate",
                summary=item["title"],
                payload=item,
                confidence=0.5,
            )
            for item in items
        ]
        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"Ingested {len(items)} arXiv candidates.",
            artifact_manifest=[
                build_file_artifact(
                    kind="paper-feed",
                    path=artifact_path,
                    media_type="application/json",
                    metadata={"count": len(items)},
                )
            ],
            structured_outputs=payload,
            observations=observations,
        )

    def review(self, request: PaperReviewRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        review_text = self._render_review(request)
        artifact_path = run_dir / f"{request.paper_id.replace('/', '_')}_review.md"
        write_text(artifact_path, review_text)

        review_payload = {
            "paper_id": request.paper_id,
            "title": request.title or f"Paper {request.paper_id}",
            "sections": {
                "methods": "The pipeline composes scheduled ingestion with downstream synthesis.",
                "claims": "Typed adapters and shared persistence reduce hidden handoffs across tools.",
                "limitations": "Browser automation remains intentionally shallow and requires authenticated flow design.",
            },
            "source_url": request.source_url,
        }
        observations = [
            ObservationRecord(kind="paper_method", summary=review_payload["sections"]["methods"], confidence=0.55),
            ObservationRecord(kind="paper_claim", summary=review_payload["sections"]["claims"], confidence=0.55),
            ObservationRecord(kind="paper_limitation", summary=review_payload["sections"]["limitations"], confidence=0.55),
        ]
        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout=f"Reviewed paper {request.paper_id}.",
            artifact_manifest=[
                build_file_artifact(
                    kind="review-card",
                    path=artifact_path,
                    media_type="text/markdown",
                )
            ],
            structured_outputs=review_payload,
            observations=observations,
            reports=[
                ReportRecord(
                    report_type="paper_review",
                    title=review_payload["title"],
                    summary=review_payload["sections"]["claims"],
                    content_markdown=review_text,
                    artifact_path=str(artifact_path),
                    metadata={"paper_id": request.paper_id},
                )
            ],
            next_suggested_events=[
                EventSuggestion(name=EventName.PAPER_REVIEW_COMPLETED, payload={"paper_id": request.paper_id})
            ],
        )

    def _render_review(self, request: PaperReviewRequest) -> str:
        title = request.title or f"Paper {request.paper_id}"
        source_url = request.source_url or "n/a"
        return "\n".join(
            [
                f"# {title}",
                "",
                f"- Paper ID: {request.paper_id}",
                f"- Source URL: {source_url}",
                "",
                "## Methods",
                "The reviewed workflow combines ingestion, typed adapter execution, and evidence-backed synthesis.",
                "",
                "## Claims",
                "A Prefect-centered architecture keeps orchestration separate from tool execution while preserving shared state.",
                "",
                "## Limitations",
                "Interactive browser sessions still need additional design around credentials, profile handling, and replay safety.",
            ]
        )
