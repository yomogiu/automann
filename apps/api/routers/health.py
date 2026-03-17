from __future__ import annotations

from fastapi import APIRouter

from apps.api.dependencies import settings_dep


router = APIRouter(tags=["health"])


@router.get("/health")
def healthcheck() -> dict[str, str]:
    settings = settings_dep()
    return {
        "status": "ok",
        "prefect_api_url": settings.prefect_api_url,
        "life_database_url": settings.life_database_url,
    }
