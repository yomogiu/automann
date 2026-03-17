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


class _FailingPrefectClient:
    prefect_available = True

    def __init__(self) -> None:
        self.run_local = AsyncMock()

    @staticmethod
    def supports_local_execution(flow_name: str) -> bool:  # noqa: ARG004
        return False

    async def submit(self, *, deployment_path: str, request: dict, run_id: str):  # noqa: ARG002
        raise RuntimeError("prefect deployment failed")


class OrchestrationBrowserFailClosedTests(unittest.TestCase):
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

    def test_browser_job_submission_fails_closed_when_prefect_submit_errors(self) -> None:
        settings = get_settings()
        prefect_client = _FailingPrefectClient()
        service = OrchestrationService(
            settings=settings,
            repository=self.repository,
            retrieval=self.retrieval,
            prefect_client=prefect_client,
        )

        result = asyncio.run(
            service.submit_flow(
                flow_name="browser_job_flow",
                parameters={
                    "job_name": "authenticated-news",
                    "target_url": "https://example.invalid/paywalled",
                    "session": {"mode": "attach", "cdp_url": "http://127.0.0.1:9222"},
                },
            )
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["mode"], "prefect-error")
        self.assertIn("browser worker required", result["error"].lower())
        prefect_client.run_local.assert_not_awaited()

        stored_run = self.repository.get_run(result["run_id"])
        assert stored_run is not None
        self.assertEqual(stored_run.status, "failed")
        self.assertIn("browser worker required", (stored_run.stderr or "").lower())
