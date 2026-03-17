from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from libs.prefect_client.orchestration import PrefectOrchestrationClient
from flows.search_report import SearchPlanPayload, SearchPlanQuery, search_report_flow


class SearchReportFlowTests(unittest.TestCase):
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

    def tearDown(self) -> None:
        self.engine.dispose()
        for key, previous in self.previous_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self.temp_dir.cleanup()

    def _execute_adapter_inline(
        self,
        *,
        flow_name: str,
        worker_key: str,
        input_payload: dict,
        runner,
        parent_run_id: str | None = None,
        task_spec_id: str | None = None,
    ) -> dict:
        run_record = self.repository.start_run(
            flow_name=flow_name,
            worker_key=worker_key,
            input_payload=input_payload,
            task_spec_id=task_spec_id,
            parent_run_id=parent_run_id,
            status="running",
        )
        result = runner()
        persisted = self.repository.persist_adapter_result(run_record.id, result)
        assert persisted is not None
        return {
            "run_id": run_record.id,
            "parent_run_id": parent_run_id,
            "task_spec_id": task_spec_id,
            "status": result.status.value,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "structured_outputs": result.structured_outputs,
            "artifacts": [
                {
                    "id": item.id,
                    "kind": item.kind,
                    "path": item.path,
                    "storage_uri": item.storage_uri,
                    "size_bytes": item.size_bytes,
                    "media_type": item.media_type,
                    "sha256": item.sha256,
                    "metadata": item.metadata_json,
                }
                for item in persisted.artifacts
            ],
            "observations": [item.model_dump(mode="json") for item in result.observations],
            "reports": [
                {
                    "id": item.id,
                    "report_type": item.report_type,
                    "title": item.title,
                    "summary": item.summary,
                    "content_markdown": item.content_markdown,
                    "source_artifact_id": item.source_artifact_id,
                    "metadata": item.metadata_json,
                }
                for item in persisted.reports
            ],
            "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
        }

    def test_prefect_client_local_execution_is_request_aware_for_search(self) -> None:
        client = PrefectOrchestrationClient()
        self.assertTrue(
            client.supports_local_execution(
                "search_report_flow",
                {"enabled_sources": ["local_knowledge"]},
            )
        )
        self.assertFalse(
            client.supports_local_execution(
                "search_report_flow",
                {"enabled_sources": ["browser_web"]},
            )
        )

    def test_search_report_flow_local_only_uses_retrieval(self) -> None:
        parent = self.repository.start_run(
            flow_name="search_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "Local search"},
            status="pending",
        )
        retrieval_hits = [
            {
                "id": "chunk-1",
                "artifact_id": "artifact-1",
                "text": "Persistent local knowledge remains high-signal for saved automations.",
                "metadata": {"topic": "automation"},
            }
        ]

        with patch("flows.search_report.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.search_report.RetrievalService.query", autospec=True, return_value=retrieval_hits):
                result = search_report_flow.fn(
                    request={
                        "title": "Local Knowledge Search",
                        "theme": "saved automations",
                        "enabled_sources": ["local_knowledge"],
                        "planner_enabled": False,
                        "queries": [{"label": "Automation", "query": "saved automation lineage"}],
                    },
                    run_id=parent.id,
                )

        self.assertEqual(result["status"], "completed")
        structured = result["structured_outputs"]
        self.assertEqual(structured["result_count"], 1)
        self.assertEqual(structured["browser_run_ids"], [])
        self.assertEqual(structured["hits"][0]["provider"], "local_knowledge")

        stored_parent = self.repository.get_run(parent.id)
        assert stored_parent is not None
        self.assertEqual(stored_parent.status, "completed")

        report = self.repository.latest_report("search_report")
        self.assertIsNotNone(report)
        assert report is not None
        self.assertEqual(report.title, "Local Knowledge Search")

    def test_search_report_flow_uses_browser_children_for_web_plan(self) -> None:
        parent = self.repository.start_run(
            flow_name="search_report_flow",
            worker_key="mini-process",
            input_payload={"theme": "Web search"},
            status="pending",
        )
        plan = SearchPlanPayload(
            summary_title="Web Search Plan",
            queries=[
                SearchPlanQuery(
                    label="Markets",
                    query="semiconductor capex",
                    providers=["browser_web"],
                    limit=3,
                    rationale="needs current web pages",
                )
            ],
        )
        fake_browser_run = {
            "run_id": "browser-child-1",
            "status": "completed",
            "artifacts": [{"id": "artifact-browser", "kind": "browser-html"}],
            "observations": [{"kind": "browser_navigation_result", "summary": "navigated"}],
            "result": {
                "structured_outputs": {
                    "final_url": "https://example.invalid/search?q=semiconductor+capex",
                    "page_title": "Semiconductor Capex Search",
                    "artifact_kinds": ["browser-html"],
                }
            },
        }

        with patch("flows.search_report.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch("flows.search_report._run_search_planner", return_value=plan):
                with patch("flows.search_report.execute_child_flow", return_value=fake_browser_run):
                    result = search_report_flow.fn(
                        request={
                            "title": "Browser Search",
                            "theme": "semiconductors",
                            "enabled_sources": ["browser_web"],
                            "planner_enabled": True,
                        },
                        run_id=parent.id,
                    )

        self.assertEqual(result["status"], "completed")
        structured = result["structured_outputs"]
        self.assertEqual(structured["browser_run_ids"], ["browser-child-1"])
        self.assertEqual(structured["hits"][0]["provider"], "browser_web")
        self.assertEqual(len(structured["browser_runs"]), 1)
