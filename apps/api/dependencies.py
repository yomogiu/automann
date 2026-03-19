from __future__ import annotations

from functools import lru_cache

from libs.config import Settings, get_settings
from libs.db import LifeRepository, engine_for_url
from libs.retrieval import RetrievalService

from .automation_service import AutomationService
from .codex_session_service import CodexSessionService
from .services import OrchestrationService


@lru_cache
def settings_dep() -> Settings:
    return get_settings()


@lru_cache
def repository_dep() -> LifeRepository:
    settings = settings_dep()
    return LifeRepository(engine_for_url(settings.life_database_url))


@lru_cache
def retrieval_dep() -> RetrievalService:
    return RetrievalService(repository_dep())


@lru_cache
def orchestration_dep() -> OrchestrationService:
    return OrchestrationService(
        settings_dep(),
        repository_dep(),
        retrieval_dep(),
        codex_session_service=codex_session_dep(),
    )


@lru_cache
def codex_session_dep() -> CodexSessionService:
    return CodexSessionService(settings_dep(), repository_dep())


@lru_cache
def automation_dep() -> AutomationService:
    return AutomationService(settings_dep(), repository_dep(), orchestration_dep())
