from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.dependencies import repository_dep
from libs.db import LifeRepository


router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("")
def list_reports(
    limit: int = 25,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    rows = repository.list_reports(limit=limit)
    taxonomy_map = repository.report_taxonomy_map([row.id for row in rows])
    return {
        "reports": [
            {
                "id": row.id,
                "report_type": row.report_type,
                "title": row.title,
                "summary": row.summary,
                "content_markdown": row.content_markdown,
                "source_artifact_id": row.source_artifact_id,
                "report_series_id": row.report_series_id,
                "revision_number": row.revision_number,
                "supersedes_report_id": row.supersedes_report_id,
                "is_current": row.is_current,
                "metadata": row.metadata_json,
                "filter_keys": taxonomy_map.get(row.id).filter_keys if taxonomy_map.get(row.id) else [],
                "tag_keys": taxonomy_map.get(row.id).tag_keys if taxonomy_map.get(row.id) else [],
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in rows
        ]
    }


@router.get("/taxonomy")
def list_report_taxonomy(
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    return repository.list_report_taxonomy()


@router.get("/{report_id}")
def get_report(
    report_id: str,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    row = repository.get_report(report_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Report not found: {report_id}")
    taxonomy = repository.report_taxonomy_map([row.id]).get(row.id)
    return {
        "id": row.id,
        "run_id": row.run_id,
        "report_type": row.report_type,
        "title": row.title,
        "summary": row.summary,
        "content_markdown": row.content_markdown,
        "source_artifact_id": row.source_artifact_id,
        "report_series_id": row.report_series_id,
        "revision_number": row.revision_number,
        "supersedes_report_id": row.supersedes_report_id,
        "is_current": row.is_current,
        "metadata": row.metadata_json,
        "filter_keys": taxonomy.filter_keys if taxonomy else [],
        "tag_keys": taxonomy.tag_keys if taxonomy else [],
        "score": row.score,
        "published_at": row.published_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


@router.get("/{report_id}/revisions")
def list_report_revisions(
    report_id: str,
    repository: LifeRepository = Depends(repository_dep),
) -> dict:
    target = repository.get_report(report_id)
    if target is None:
        raise HTTPException(status_code=404, detail=f"Report not found: {report_id}")

    revisions = repository.list_report_revisions(report_id)
    taxonomy_map = repository.report_taxonomy_map([row.id for row in revisions])
    return {
        "report_id": report_id,
        "report_series_id": target.report_series_id or target.id,
        "revisions": [
            {
                "id": row.id,
                "run_id": row.run_id,
                "report_type": row.report_type,
                "title": row.title,
                "summary": row.summary,
                "content_markdown": row.content_markdown,
                "source_artifact_id": row.source_artifact_id,
                "report_series_id": row.report_series_id,
                "revision_number": row.revision_number,
                "supersedes_report_id": row.supersedes_report_id,
                "is_current": row.is_current,
                "metadata": row.metadata_json,
                "filter_keys": taxonomy_map.get(row.id).filter_keys if taxonomy_map.get(row.id) else [],
                "tag_keys": taxonomy_map.get(row.id).tag_keys if taxonomy_map.get(row.id) else [],
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in revisions
        ],
    }
