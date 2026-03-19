from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import os
import unittest

from fastapi.testclient import TestClient

from apps.api.dependencies import codex_session_dep, orchestration_dep, repository_dep, retrieval_dep, settings_dep
from apps.api.main import create_app
from libs.config import get_settings
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url


class _FakeCodexSessionService:
    def __init__(self) -> None:
        self.interrupt_calls: list[str] = []
        self.steer_calls: list[dict] = []

    async def stream_events(self, run_id: str, *, after_seq: int = 0):
        del after_seq
        yield {
            "seq": 1,
            "type": "turn/started",
            "run_id": run_id,
            "thread_id": "thr-test",
            "turn_id": "turn-test",
            "item_id": None,
            "payload": {"ok": True},
            "ts": "2026-03-17T00:00:00+00:00",
        }

    async def interrupt(self, run_id: str) -> dict:
        self.interrupt_calls.append(run_id)
        return {"run_id": run_id, "result": {"ok": True}}

    async def steer(self, run_id: str, *, prompt: str, expected_turn_id: str | None = None) -> dict:
        self.steer_calls.append(
            {"run_id": run_id, "prompt": prompt, "expected_turn_id": expected_turn_id}
        )
        return {"run_id": run_id, "result": {"ok": True}}

    async def get_session_snapshot(self, run_id: str) -> dict:
        return {"run_id": run_id, "thread_id": "thr-test", "active_turn_id": "turn-test", "state": "running"}

    def is_interactive_flow(self, flow_name: str, parameters: dict | None = None) -> bool:  # noqa: ARG002
        return False


class CodexRunsStreamAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.env_overrides = {
            "LIFE_ARTIFACT_ROOT": str(self.root / "artifacts"),
            "LIFE_REPORT_ROOT": str(self.root / "reports"),
            "LIFE_RUNTIME_ROOT": str(self.root / "runtime"),
            "LIFE_AUTOMATION_PROMPT_ROOT": str(self.root / "automation-prompts"),
            "LIFE_DATABASE_URL": f"sqlite+pysqlite:///{self.root / 'runtime' / 'life.db'}",
        }
        self.previous_env = {key: os.environ.get(key) for key in self.env_overrides}
        for key, value in self.env_overrides.items():
            os.environ[key] = value

        settings_dep.cache_clear()
        repository_dep.cache_clear()
        retrieval_dep.cache_clear()
        orchestration_dep.cache_clear()
        codex_session_dep.cache_clear()

        settings = get_settings()
        bootstrap_life_database(settings)
        self.engine = engine_for_url(settings.life_database_url)
        self.repository = LifeRepository(self.engine)
        self.run = self.repository.start_run(
            flow_name="research_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "AI safety"},
            status="running",
        )
        self.repository.update_run_codex_session(
            self.run.id,
            codex_thread_id="thr-test",
            codex_active_turn_id="turn-test",
            codex_session_key=f"{self.run.id}:123",
            codex_state="running",
        )

        app = create_app()
        self.fake_codex = _FakeCodexSessionService()
        app.dependency_overrides[codex_session_dep] = lambda: self.fake_codex
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.client.close()
        self.engine.dispose()
        settings_dep.cache_clear()
        repository_dep.cache_clear()
        retrieval_dep.cache_clear()
        orchestration_dep.cache_clear()
        codex_session_dep.cache_clear()
        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def test_run_detail_includes_codex_session(self) -> None:
        response = self.client.get(f"/runs/{self.run.id}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["codex_session"]["codex_thread_id"], "thr-test")

    def test_stream_endpoint_emits_sse_event(self) -> None:
        with self.client.stream("GET", f"/runs/{self.run.id}/stream") as response:
            self.assertEqual(response.status_code, 200)
            body = "".join(response.iter_text())
        self.assertIn('"type": "turn/started"', body)

    def test_interrupt_and_steer_endpoints_delegate_to_codex_service(self) -> None:
        interrupt = self.client.post(f"/runs/{self.run.id}/interrupt")
        self.assertEqual(interrupt.status_code, 200)
        self.assertEqual(self.fake_codex.interrupt_calls, [self.run.id])

        steer = self.client.post(
            f"/runs/{self.run.id}/steer",
            json={"prompt": "Focus on export-control scenarios", "expected_turn_id": "turn-test"},
        )
        self.assertEqual(steer.status_code, 200)
        self.assertEqual(self.fake_codex.steer_calls[0]["run_id"], self.run.id)
