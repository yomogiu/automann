from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse


STATIC_DIR = Path(__file__).resolve().parent / "static"
router = APIRouter(include_in_schema=False)


@router.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/automations")
def automations() -> FileResponse:
    return FileResponse(STATIC_DIR / "automations.html")
