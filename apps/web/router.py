from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse
from fastapi.routing import APIRoute
from starlette.routing import Match


STATIC_DIR = Path(__file__).resolve().parent / "static"


class HtmlOnlyRoute(APIRoute):
    def matches(self, scope):  # type: ignore[override]
        if scope["type"] == "http":
            headers = {key.decode("latin-1"): value.decode("latin-1") for key, value in scope.get("headers", [])}
            accept = headers.get("accept", "")
            if "application/json" in accept and "text/html" not in accept:
                return Match.NONE, {}
        return super().matches(scope)


router = APIRouter(include_in_schema=False, route_class=HtmlOnlyRoute)


@router.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/automations")
def automations() -> FileResponse:
    return FileResponse(STATIC_DIR / "automations.html")


@router.get("/sources")
def sources() -> FileResponse:
    return FileResponse(STATIC_DIR / "sources.html")
