from __future__ import annotations

from pathlib import Path
from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import PaperReviewRequest
from libs.db import LifeRepository, engine_for_url
from libs.retrieval import chunk_text
from workers.ingest_runner import ArxivReviewRunner

from .common import execute_adapter


@flow(name="paper-review")
def paper_review_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = LifeRepository(engine_for_url(settings.life_database_url))
    paper_request = PaperReviewRequest.model_validate(request)
    runner = ArxivReviewRunner(settings)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        review_result = execute_adapter(
            flow_name="paper_review_flow",
            worker_key=runner.worker_key,
            input_payload=paper_request.model_dump(mode="json"),
            runner=lambda: runner.review(paper_request),
            parent_run_id=run_id,
        )

        if review_result["artifacts"]:
            artifact_record = next(
                (item for item in review_result["artifacts"] if item["kind"] == "review-card"),
                review_result["artifacts"][0],
            )
            artifact_path = Path(artifact_record["path"])
            chunk_count = 0
            if artifact_path.exists():
                text = artifact_path.read_text(encoding="utf-8")
                chunks = chunk_text(text)
                chunk_count = repository.upsert_chunks(artifact_id=artifact_record["id"], chunks=chunks)
                review_result["chunk_count"] = chunk_count

        if run_id is not None:
            repository.update_run_status(
                run_id,
                status="completed",
                stdout=f"Completed paper review for {paper_request.paper_id}",
                structured_outputs=review_result,
                artifact_manifest=review_result.get("artifacts", []),
                observation_summary=review_result.get("observations", []),
                next_suggested_events=review_result.get("next_events", []),
            )
        return review_result
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
