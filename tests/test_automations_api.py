from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import os
import unittest

from fastapi.testclient import TestClient

from apps.api.automation_service import AutomationService
from apps.api.dependencies import automation_dep, orchestration_dep, repository_dep, retrieval_dep, settings_dep
from apps.api.main import create_app
from libs.config import get_settings
from libs.contracts.models import AutomationPrefectStatus
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from flows.registry import FLOW_SPECS


class _FakeDeploymentClient:
    def __init__(self, ui_url: str) -> None:
        self.ui_url = ui_url
        self.deployments: dict[str, dict] = {}
        self.fail_upsert = False

    async def upsert(
        self,
        *,
        task_key: str,
        task_type: str,
        flow_name: str,
        schedule_text: str | None,
        timezone: str | None,
        work_pool: str,
        parameters: dict,
        paused: bool,
        description: str | None = None,
    ) -> AutomationPrefectStatus:
        if self.fail_upsert:
            raise RuntimeError("prefect unavailable")
        deployment_id = f"dep-{task_key}"
        deployment_name = f"automation-{task_key}"
        deployment_path = FLOW_SPECS[flow_name].deployment_path.replace("/default", f"/{deployment_name}")
        self.deployments[deployment_id] = {
            "task_key": task_key,
            "task_type": task_type,
            "flow_name": flow_name,
            "schedule_text": schedule_text,
            "timezone": timezone,
            "work_pool": work_pool,
            "parameters": parameters,
            "paused": paused,
            "description": description,
            "path": deployment_path,
        }
        return AutomationPrefectStatus(
            deployment_id=deployment_id,
            deployment_name=deployment_name,
            deployment_path=deployment_path,
            deployment_url=f"{self.ui_url}/deployments/deployment/{deployment_id}",
            status="paused" if paused else "active",
            paused=paused,
        )

    async def pause(self, *, deployment_id: str, deployment_path: str) -> AutomationPrefectStatus:
        self.deployments[deployment_id]["paused"] = True
        return AutomationPrefectStatus(
            deployment_id=deployment_id,
            deployment_name=f"automation-{self.deployments[deployment_id]['task_key']}",
            deployment_path=deployment_path,
            deployment_url=f"{self.ui_url}/deployments/deployment/{deployment_id}",
            status="paused",
            paused=True,
        )

    async def resume(self, *, deployment_id: str, deployment_path: str) -> AutomationPrefectStatus:
        self.deployments[deployment_id]["paused"] = False
        return AutomationPrefectStatus(
            deployment_id=deployment_id,
            deployment_name=f"automation-{self.deployments[deployment_id]['task_key']}",
            deployment_path=deployment_path,
            deployment_url=f"{self.ui_url}/deployments/deployment/{deployment_id}",
            status="active",
            paused=False,
        )

    async def archive(self, *, deployment_id: str) -> None:
        self.deployments.pop(deployment_id, None)

    async def describe(
        self,
        *,
        deployment_id: str | None = None,
        deployment_path: str | None = None,
    ) -> AutomationPrefectStatus:
        if deployment_id is None:
            for candidate_id, payload in self.deployments.items():
                if payload["path"] == deployment_path:
                    deployment_id = candidate_id
                    break
        if deployment_id is None or deployment_id not in self.deployments:
            return AutomationPrefectStatus(status="missing", deployment_path=deployment_path)
        payload = self.deployments[deployment_id]
        return AutomationPrefectStatus(
            deployment_id=deployment_id,
            deployment_name=f"automation-{payload['task_key']}",
            deployment_path=payload["path"],
            deployment_url=f"{self.ui_url}/deployments/deployment/{deployment_id}",
            status="paused" if payload["paused"] else "active",
            paused=payload["paused"],
        )


class _FakeOrchestrationService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def submit_flow(
        self,
        *,
        flow_name: str,
        parameters: dict,
        work_pool: str | None = None,
        task_spec_id: str | None = None,
        deployment_path: str | None = None,
    ) -> dict:
        self.calls.append(
            {
                "flow_name": flow_name,
                "parameters": parameters,
                "work_pool": work_pool,
                "task_spec_id": task_spec_id,
                "deployment_path": deployment_path,
            }
        )
        return {
            "run_id": "run-test",
            "status": "pending",
            "mode": "prefect",
            "task_spec_id": task_spec_id,
            "deployment_path": deployment_path,
        }


class AutomationsAPITests(unittest.TestCase):
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
        automation_dep.cache_clear()

        settings = get_settings()
        bootstrap_life_database(settings)
        self.engine = engine_for_url(settings.life_database_url)
        self.repository = LifeRepository(self.engine)
        self.fake_orchestration = _FakeOrchestrationService()
        self.fake_deployments = _FakeDeploymentClient(settings.prefect_ui_url)
        self.automation_service = AutomationService(
            settings=settings,
            repository=self.repository,
            orchestration=self.fake_orchestration,  # type: ignore[arg-type]
            deployment_client=self.fake_deployments,  # type: ignore[arg-type]
        )

        app = create_app()
        app.dependency_overrides[automation_dep] = lambda: self.automation_service
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.client.close()
        self.engine.dispose()

        settings_dep.cache_clear()
        repository_dep.cache_clear()
        retrieval_dep.cache_clear()
        orchestration_dep.cache_clear()
        automation_dep.cache_clear()

        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def test_create_search_report_automation_persists_prompt_file(self) -> None:
        response = self.client.post(
            "/automations",
            json={
                "automation_type": "search_report",
                "title": "Chip Search",
                "schedule_text": "0 8 * * 1-5",
                "timezone": "America/Toronto",
                "payload": {
                    "theme": "Semiconductor supply chain",
                    "queries": ["TSMC capacity 2026", "advanced packaging demand"],
                    "enabled_sources": ["local_knowledge", "browser_web"],
                    "planner_enabled": False,
                    "max_results_per_query": 3,
                },
                "prompt_body": "Use local knowledge first, then verify with web search.",
            },
        )
        self.assertEqual(response.status_code, 200)
        automation = response.json()["automation"]
        self.assertEqual(automation["automation_type"], "search_report")
        self.assertEqual(automation["flow_name"], "codex_search_report_flow")
        self.assertEqual(automation["prefect"]["status"], "active")
        prompt_path = Path(automation["prompt_path"])
        self.assertTrue(prompt_path.exists())
        self.assertIn("verify with web search", prompt_path.read_text(encoding="utf-8"))
        deployment = next(iter(self.fake_deployments.deployments.values()))
        self.assertEqual(deployment["flow_name"], "codex_search_report_flow")
        self.assertEqual(automation["prefect"]["deployment_path"], deployment["path"])
        self.assertIn("Scheduled Search Report", deployment["parameters"]["prompt"])
        self.assertIn("Title: Chip Search", deployment["parameters"]["prompt"])
        self.assertIn("1. TSMC capacity 2026", deployment["parameters"]["prompt"])
        self.assertEqual(deployment["parameters"]["enabled_sources"], ["local_knowledge", "browser_web"])
        self.assertFalse(deployment["parameters"]["planner_enabled"])
        self.assertEqual(deployment["parameters"]["max_results_per_query"], 3)
        self.assertIn("Planner enabled: no", deployment["parameters"]["prompt"])
        self.assertIn("Max results per query: 3", deployment["parameters"]["prompt"])
        self.assertIn("Important: start fresh and do not resume prior sessions.", deployment["parameters"]["prompt"])

        detail = self.client.get(f"/automations/{automation['id']}")
        self.assertEqual(detail.status_code, 200)
        self.assertIn("verify with web search", detail.json()["automation"]["prompt_body"])

    def test_relative_prompt_path_resolves_under_configured_prompt_root(self) -> None:
        response = self.client.post(
            "/automations",
            json={
                "automation_type": "search_report",
                "title": "Grid Search",
                "payload": {
                    "theme": "Grid bottlenecks",
                    "queries": ["Ontario transmission upgrades"],
                    "enabled_sources": ["local_knowledge"],
                },
                "prompt_path": "searches/grid-bottlenecks.md",
                "prompt_body": "Focus on the highest-signal transmission constraints.",
            },
        )
        self.assertEqual(response.status_code, 200)
        automation = response.json()["automation"]
        expected = (self.root / "automation-prompts" / "searches" / "grid-bottlenecks.md").resolve()
        self.assertEqual(Path(automation["prompt_path"]).resolve(), expected)
        self.assertTrue(expected.exists())
        self.assertIn("highest-signal transmission constraints", expected.read_text(encoding="utf-8"))

    def test_run_search_report_automation_submits_prompt_to_codex_flow(self) -> None:
        create = self.client.post(
            "/automations",
            json={
                "automation_type": "search_report",
                "title": "Market Search",
                "payload": {
                    "theme": "Semiconductor supply chain",
                    "queries": ["TSMC capacity 2026"],
                },
                "prompt_body": "Prefer local knowledge before broader synthesis.",
            },
        )
        self.assertEqual(create.status_code, 200)
        automation = create.json()["automation"]

        run = self.client.post(
            f"/automations/{automation['id']}/run",
            json={"payload_overrides": {"queries": ["TSMC capacity 2026", "advanced packaging demand"]}},
        )
        self.assertEqual(run.status_code, 200)
        call = self.fake_orchestration.calls[-1]
        self.assertEqual(call["flow_name"], "codex_search_report_flow")
        self.assertEqual(call["deployment_path"], automation["prefect"]["deployment_path"])
        self.assertIn("Scheduled Search Report", call["parameters"]["prompt"])
        self.assertIn("Title: Market Search", call["parameters"]["prompt"])
        self.assertIn("advanced packaging demand", call["parameters"]["prompt"])
        self.assertEqual(call["parameters"]["enabled_sources"], ["local_knowledge"])
        self.assertTrue(call["parameters"]["planner_enabled"])
        self.assertEqual(call["parameters"]["max_results_per_query"], 8)

    def test_legacy_search_automation_get_migrates_without_prefect_metadata(self) -> None:
        legacy_prompt_path = self.root / "automation-prompts" / "legacy-search.md"
        legacy_prompt_path.parent.mkdir(parents=True, exist_ok=True)
        legacy_prompt_path.write_text("Legacy instructions remain valid.\n", encoding="utf-8")
        legacy = self.repository.create_task_spec(
            task_key="legacy-search",
            task_type="search_report",
            flow_name="search_report_flow",
            title="Legacy Search",
            description="Old search automation",
            schedule_text="0 8 * * 1-5",
            timezone="America/Toronto",
            work_pool="mini-process",
            prompt_path=str(legacy_prompt_path),
            status="active",
            prefect_deployment_id="legacy-deployment",
            prefect_deployment_name="automation-legacy-search",
            prefect_deployment_path="search-report/default",
            prefect_deployment_url="https://prefect.invalid/deployments/deployment/legacy-deployment",
            payload={
                "theme": "Legacy theme",
                "queries": ["legacy query"],
                "enabled_sources": ["local_knowledge"],
                "planner_enabled": True,
                "max_results_per_query": 8,
            },
        )
        self.fake_deployments.fail_upsert = True

        response = self.client.get(f"/automations/{legacy.id}")
        self.assertEqual(response.status_code, 200)
        automation = response.json()["automation"]
        self.assertEqual(automation["flow_name"], "codex_search_report_flow")
        self.assertEqual(automation["prefect"]["status"], "missing")
        self.assertIsNone(automation["prefect"]["deployment_id"])
        self.assertIsNone(automation["prefect"]["deployment_path"])

        refreshed = self.repository.get_task_spec(legacy.id)
        assert refreshed is not None
        self.assertEqual(refreshed.flow_name, "codex_search_report_flow")
        self.assertIsNone(refreshed.prefect_deployment_id)
        self.assertIsNone(refreshed.prefect_deployment_path)

    def test_prompt_path_outside_configured_prompt_root_is_rejected(self) -> None:
        outside_path = self.root / "outside-prompts" / "bad.md"
        response = self.client.post(
            "/automations",
            json={
                "automation_type": "search_report",
                "title": "Bad Search",
                "payload": {
                    "theme": "Grid bottlenecks",
                    "queries": ["Ontario transmission upgrades"],
                    "enabled_sources": ["local_knowledge"],
                },
                "prompt_path": str(outside_path),
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("configured automation prompt root", response.json()["detail"])

    def test_pause_resume_and_archive_automation(self) -> None:
        create = self.client.post(
            "/automations",
            json={
                "automation_type": "daily_brief",
                "title": "Morning Brief",
                "payload": {
                    "include_news": True,
                    "include_arxiv": True,
                    "include_browser_jobs": False,
                    "publish": True,
                },
            },
        )
        automation_id = create.json()["automation"]["id"]

        paused = self.client.post(f"/automations/{automation_id}/pause")
        self.assertEqual(paused.status_code, 200)
        self.assertEqual(paused.json()["automation"]["status"], "paused")
        self.assertEqual(paused.json()["automation"]["prefect"]["status"], "paused")

        resumed = self.client.post(f"/automations/{automation_id}/resume")
        self.assertEqual(resumed.status_code, 200)
        self.assertEqual(resumed.json()["automation"]["status"], "active")
        self.assertEqual(resumed.json()["automation"]["prefect"]["status"], "active")

        archived = self.client.post(f"/automations/{automation_id}/archive")
        self.assertEqual(archived.status_code, 200)
        self.assertEqual(archived.json()["automation"]["status"], "archived")
        self.assertEqual(archived.json()["automation"]["prefect"]["status"], "archived")

    def test_run_automation_submits_with_task_spec_and_deployment_path(self) -> None:
        create = self.client.post(
            "/automations",
            json={
                "automation_type": "daily_brief",
                "title": "Desk Brief",
                "payload": {
                    "include_news": True,
                    "include_arxiv": False,
                    "include_browser_jobs": False,
                    "publish": False,
                },
            },
        )
        self.assertEqual(create.status_code, 200)
        automation = create.json()["automation"]

        run = self.client.post(
            f"/automations/{automation['id']}/run",
            json={"payload_overrides": {"publish": True}},
        )
        self.assertEqual(run.status_code, 200)
        call = self.fake_orchestration.calls[-1]
        self.assertEqual(call["flow_name"], "daily_brief_flow")
        self.assertEqual(call["task_spec_id"], automation["id"])
        self.assertEqual(call["deployment_path"], automation["prefect"]["deployment_path"])
        self.assertEqual(call["parameters"]["actor"], "automation")
        self.assertTrue(call["parameters"]["publish"])
