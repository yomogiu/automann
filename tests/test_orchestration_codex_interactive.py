from __future__ import annotations

import asyncio
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock

from apps.api.services import OrchestrationService
from libs.config import get_settings
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from libs.retrieval import RetrievalService


class _FakePrefectClient:
    prefect_available = True

    def __init__(self) -> None:
        self.submit = AsyncMock()
        self.run_local = AsyncMock()

    @staticmethod
    def supports_local_execution(flow_name: str, request: dict | None = None) -> bool:  # noqa: ARG004
        return True


class _FakeCodexSessionService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def is_interactive_flow(self, flow_name: str, parameters: dict | None = None) -> bool:
        payload = parameters or {}
        return flow_name in {"research_report_flow", "codex_search_report_flow"} or bool(payload.get("codex_interactive"))

    async def submit_run(self, *, run_id: str, flow_name: str, parameters: dict) -> dict:
        self.calls.append({"run_id": run_id, "flow_name": flow_name, "parameters": parameters})
        return {"run_id": run_id, "mode": "codex-app-server", "thread_id": "thr_123", "turn_id": "turn_123"}


class OrchestrationCodexInteractiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.env_overrides = {
            "LIFE_ARTIFACT_ROOT": str(self.root / "artifacts"),
            "LIFE_REPORT_ROOT": str(self.root / "reports"),
            "LIFE_RUNTIME_ROOT": str(self.root / "runtime"),
            "LIFE_DATABASE_URL": f"sqlite+pysqlite:///{self.root / 'runtime' / 'life.db'}",
        }
        self.previous_env = {key: os.environ.get(key) for key in self.env_overrides}
        for key, value in self.env_overrides.items():
            os.environ[key] = value
        settings = get_settings()
        bootstrap_life_database(settings)
        self.engine = engine_for_url(settings.life_database_url)
        self.repository = LifeRepository(self.engine)
        self.retrieval = RetrievalService(self.repository)

    def tearDown(self) -> None:
        self.engine.dispose()
        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def test_research_report_uses_codex_session_service(self) -> None:
        service = OrchestrationService(
            settings=get_settings(),
            repository=self.repository,
            retrieval=self.retrieval,
            prefect_client=_FakePrefectClient(),
            codex_session_service=_FakeCodexSessionService(),  # type: ignore[arg-type]
        )

        result = asyncio.run(
            service.submit_flow(
                flow_name="research_report_flow",
                parameters={"theme": "AI chip export controls"},
            )
        )

        self.assertEqual(result["mode"], "codex-app-server")
        self.assertEqual(result["status"], "running")

    def test_generic_run_submit_can_force_codex_interactive(self) -> None:
        fake_codex = _FakeCodexSessionService()
        service = OrchestrationService(
            settings=get_settings(),
            repository=self.repository,
            retrieval=self.retrieval,
            prefect_client=_FakePrefectClient(),
            codex_session_service=fake_codex,  # type: ignore[arg-type]
        )

        result = asyncio.run(
            service.submit_flow(
                flow_name="daily_brief_flow",
                parameters={"codex_interactive": True, "prompt": "Investigate market structure"},
            )
        )

        self.assertEqual(result["mode"], "codex-app-server")
        self.assertEqual(len(fake_codex.calls), 1)
        self.assertEqual(fake_codex.calls[0]["flow_name"], "daily_brief_flow")
