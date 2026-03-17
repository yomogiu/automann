from __future__ import annotations

from pathlib import Path

import uvicorn
from fastapi import FastAPI

from libs.config import get_settings

from .routers.artifacts import router as artifacts_router
from .routers.commands import router as commands_router
from .routers.health import router as health_router
from .routers.interactions import router as interactions_router
from .routers.reports import router as reports_router
from .routers.runs import router as runs_router


def create_app() -> FastAPI:
    settings = get_settings()
    for path in (settings.artifact_root, settings.report_root, settings.runtime_root):
        Path(path).mkdir(parents=True, exist_ok=True)

    app = FastAPI(title="Life System API", version="0.1.0")
    app.include_router(health_router)
    app.include_router(commands_router)
    app.include_router(runs_router)
    app.include_router(reports_router)
    app.include_router(artifacts_router)
    app.include_router(interactions_router)
    return app


app = create_app()


def run() -> None:
    settings = get_settings()
    uvicorn.run(
        "apps.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
