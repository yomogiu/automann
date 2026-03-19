from __future__ import annotations

import asyncio
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from apps.api.services import OrchestrationService
from libs.config import get_settings
from libs.contracts.models import InteractionAnswerRequest
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from libs.retrieval import RetrievalService


class _FakePrefectClient:
    prefect_available = False


class _FakeCodexSessionService:
    def __init__(self) -> None:
        self.answers: list[dict] = []

    def is_interactive_flow(self, flow_name: str, parameters: dict | None = None) -> bool:  # noqa: ARG002
        return False

    async def answer_interaction_request(self, *, run_id: str, ui_hints: dict, response: dict) -> dict:
        self.answers.append({"run_id": run_id, "ui_hints": ui_hints, "response": response})
        return {"ok": True}


class CodexInteractionBridgeTests(unittest.TestCase):
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
        self.run = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "AI safety"},
            status="waiting_input",
        )

    def tearDown(self) -> None:
        self.engine.dispose()
        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def test_answer_interaction_forwards_codex_request(self) -> None:
        fake_codex = _FakeCodexSessionService()
        service = OrchestrationService(
            settings=get_settings(),
            repository=self.repository,
            retrieval=self.retrieval,
            prefect_client=_FakePrefectClient(),  # type: ignore[arg-type]
            codex_session_service=fake_codex,  # type: ignore[arg-type]
        )
        interaction = self.repository.create_interaction(
            run_id=self.run.id,
            title="Codex approval",
            prompt_md="Approve",
            input_schema={"type": "object"},
            default_input={"approved": True},
            ui_hints={
                "codex_request_id": "req-1",
                "request_kind": "item/commandExecution/requestApproval",
                "thread_id": "thr-1",
                "turn_id": "turn-1",
            },
        )

        result = asyncio.run(
            service.answer_interaction(
                interaction.id,
                InteractionAnswerRequest(response={"approved": True}),
            )
        )

        self.assertEqual(result.status, "answered")
        self.assertEqual(len(fake_codex.answers), 1)
        self.assertEqual(fake_codex.answers[0]["run_id"], self.run.id)
