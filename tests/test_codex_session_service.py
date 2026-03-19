from __future__ import annotations

import asyncio
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from apps.api.codex_session_service import CodexSessionService
from libs.config import get_settings
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url


class _FakeSessionManager:
    def __init__(self) -> None:
        self.started: list[dict] = []
        self.continued: list[dict] = []
        self.answered: list[dict] = []

    async def start_run(self, run_id: str, **kwargs) -> dict:
        self.started.append({"run_id": run_id, **kwargs})
        return {
            "run_id": run_id,
            "thread_id": "thr-1",
            "turn_id": "turn-1",
            "session": {
                "run_id": run_id,
                "process_id": 123,
                "thread_id": "thr-1",
                "active_turn_id": "turn-1",
                "state": "running",
                "pending_request_ids": [],
            },
        }

    async def continue_run(self, run_id: str, **kwargs) -> dict:
        self.continued.append({"run_id": run_id, **kwargs})
        return {
            "run_id": run_id,
            "thread_id": "thr-1",
            "turn_id": "turn-2",
            "session": {
                "run_id": run_id,
                "process_id": 123,
                "thread_id": "thr-1",
                "active_turn_id": "turn-2",
                "state": "running",
                "pending_request_ids": [],
            },
        }

    async def stream_events(self, run_id: str, *, after_seq: int = 0):
        del run_id, after_seq
        if False:
            yield {}

    async def snapshot(self, run_id: str) -> dict:
        return {
            "run_id": run_id,
            "process_id": 123,
            "thread_id": "thr-1",
            "active_turn_id": "turn-1",
            "state": "running",
            "pending_request_ids": [],
        }

    async def answer_pending_request(self, run_id: str, request_id: str, *, result=None, error=None) -> dict:
        self.answered.append({"run_id": run_id, "request_id": request_id, "result": result, "error": error})
        return {"run_id": run_id, "session": await self.snapshot(run_id)}


class CodexSessionServiceTests(unittest.TestCase):
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
        self.run = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "AI safety"},
            status="pending",
        )

    def tearDown(self) -> None:
        self.engine.dispose()
        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def test_submit_run_persists_codex_session_snapshot(self) -> None:
        fake_manager = _FakeSessionManager()
        service = CodexSessionService(get_settings(), self.repository, session_manager=fake_manager)  # type: ignore[arg-type]

        result = asyncio.run(
            service.submit_run(
                run_id=self.run.id,
                flow_name="research_report_flow",
                parameters={"theme": "AI safety"},
            )
        )

        self.assertEqual(result["mode"], "codex-app-server")
        codex_session = self.repository.get_run_codex_session(self.run.id)
        assert codex_session is not None
        self.assertEqual(codex_session.codex_thread_id, "thr-1")

    def test_answer_interaction_request_maps_approval_payload(self) -> None:
        fake_manager = _FakeSessionManager()
        service = CodexSessionService(get_settings(), self.repository, session_manager=fake_manager)  # type: ignore[arg-type]

        asyncio.run(
            service.answer_interaction_request(
                run_id=self.run.id,
                ui_hints={
                    "codex_request_id": "req-1",
                    "request_kind": "item/commandExecution/requestApproval",
                },
                response={"approved": True},
            )
        )

        self.assertEqual(fake_manager.answered[0]["request_id"], "req-1")
        self.assertEqual(fake_manager.answered[0]["result"]["decision"], "approved")
