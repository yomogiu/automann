from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.models import AdapterResult, WorkerStatus
from libs.db import LifeRepository, bootstrap_life_database, engine_for_url
from libs.contracts.workers import CodexSearchSessionOutput, CodexSearchSessionRequest
from flows.codex_search_report import codex_search_report_flow
from workers.common import build_file_artifact


class _FakeSessionRunner:
    def __init__(self, handler):
        self.handler = handler
        self.requests: list[CodexSearchSessionRequest] = []

    def run(self, request: CodexSearchSessionRequest) -> AdapterResult:
        self.requests.append(request)
        return self.handler(request, len(self.requests))


class CodexSearchReportFlowTests(unittest.TestCase):
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
    ) -> dict:
        run_record = self.repository.start_run(
            flow_name=flow_name,
            worker_key=worker_key,
            input_payload=input_payload,
            parent_run_id=parent_run_id,
            status="running",
        )
        result = runner()
        persisted = self.repository.persist_adapter_result(run_record.id, result)
        assert persisted is not None
        return {
            "run_id": run_record.id,
            "parent_run_id": parent_run_id,
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
                    "report_series_id": item.report_series_id,
                    "revision_number": item.revision_number,
                    "supersedes_report_id": item.supersedes_report_id,
                    "is_current": item.is_current,
                    "metadata": item.metadata_json,
                }
                for item in persisted.reports
            ],
            "next_events": [item.model_dump(mode="json") for item in result.next_suggested_events],
        }

    def _session_result(
        self,
        *,
        run_number: int,
        title: str,
        session_id: str | None,
        mode: str = "fresh",
        status: WorkerStatus = WorkerStatus.COMPLETED,
        error_reason: str | None = None,
    ) -> AdapterResult:
        run_dir = self.root / "session-runner" / f"run-{run_number}"
        run_dir.mkdir(parents=True, exist_ok=True)
        output_path = run_dir / "codex_last_message.json"
        events_path = run_dir / "codex_events.jsonl"
        manifest_path = run_dir / "codex_session_manifest.json"

        if status == WorkerStatus.COMPLETED:
            output_path.write_text(
                json.dumps(
                    {
                        "title": title,
                        "summary": f"Summary for {title}",
                        "report_markdown": f"# {title}\n\nDetailed report body.\n",
                        "resume_summary": f"Resume {title}",
                        "completed_work": [f"Completed {title}"],
                        "open_questions": [],
                        "suggested_followup_prompt": f"Improve {title}",
                        "needs_user_input": False,
                        "sources": [{"title": "Source", "url": "https://example.invalid", "note": "note"}],
                    }
                ),
                encoding="utf-8",
            )
        events_payload = {"event": "done"}
        if session_id:
            events_payload["session_id"] = session_id
            events_payload["thread_name"] = title
        events_path.write_text(json.dumps(events_payload) + "\n", encoding="utf-8")
        manifest_path.write_text(json.dumps({"mode": mode}), encoding="utf-8")

        structured = CodexSearchSessionOutput(
            returncode=0 if status == WorkerStatus.COMPLETED else 1,
            mode=mode,
            session_id=session_id,
            resumed_from_session_id=session_id if mode == "resume" else None,
            session_id_source="json_events" if session_id else "none",
            thread_name=title if session_id else None,
            output_path=str(output_path),
            events_path=str(events_path),
            manifest_path=str(manifest_path),
            stderr_path=None,
            command=["codex", "--search", "exec", "--json", "-o", str(output_path), title],
            error_reason=error_reason,
            can_resume=session_id is not None,
        ).model_dump(mode="json")

        artifacts = [
            build_file_artifact(kind="codex-output", path=output_path, media_type="application/json", metadata={"role": "codex_output"}),
            build_file_artifact(kind="codex-events", path=events_path, media_type="text/plain", metadata={"role": "codex_events"}),
            build_file_artifact(kind="codex-session-manifest", path=manifest_path, media_type="application/json", metadata={"role": "session_manifest"}),
        ]
        return AdapterResult(
            status=status,
            stdout=f"session {run_number}",
            stderr="" if status == WorkerStatus.COMPLETED else (error_reason or "failed"),
            artifact_manifest=artifacts,
            structured_outputs=structured,
        )

    def test_fresh_flow_creates_report_revision_and_promotes_it(self) -> None:
        parent = self.repository.start_run(
            flow_name="codex_search_report_flow",
            worker_key="mini-process",
            input_payload={"prompt": "tokyo itinerary"},
            status="pending",
        )
        fake_runner = _FakeSessionRunner(
            lambda request, run_number: self._session_result(
                run_number=run_number,
                title="Tokyo Itinerary",
                session_id="66666666-6666-6666-6666-666666666666",
            )
        )

        with patch("flows.codex_search_report.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch(
                "flows.codex_search_report._load_session_runner",
                return_value=(fake_runner, CodexSearchSessionRequest),
            ):
                result = codex_search_report_flow.fn(
                    request={"prompt": "Create a 10 day Tokyo itinerary"},
                    run_id=parent.id,
                )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["codex"]["session_id"], "66666666-6666-6666-6666-666666666666")
        self.assertEqual(result["codex"]["resume_source"], "fresh")
        self.assertEqual(result["report"]["revision_number"], 1)
        self.assertIsNotNone(result["report"]["current_report_id"])
        self.assertIn("report_path", result["artifacts"])
        self.assertIn("memo_path", result["artifacts"])

        current = self.repository.current_report_revision(f"search-report:{parent.id}")
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.revision_number, 1)
        self.assertTrue(current.is_current)

        stored_parent = self.repository.get_run(parent.id)
        assert stored_parent is not None
        self.assertEqual(stored_parent.status, "completed")
        self.assertEqual(stored_parent.structured_outputs["report"]["current_report_id"], current.id)

    def test_resume_prefers_prior_run_session_id_and_creates_new_revision(self) -> None:
        fake_runner = _FakeSessionRunner(
            lambda request, run_number: self._session_result(
                run_number=run_number,
                title=f"Tokyo Itinerary v{run_number}",
                session_id="77777777-7777-7777-7777-777777777777" if run_number == 1 else "88888888-8888-8888-8888-888888888888",
                mode="fresh" if run_number == 1 else "resume",
            )
        )

        with patch("flows.codex_search_report.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch(
                "flows.codex_search_report._load_session_runner",
                return_value=(fake_runner, CodexSearchSessionRequest),
            ):
                first_parent = self.repository.start_run(
                    flow_name="codex_search_report_flow",
                    worker_key="mini-process",
                    input_payload={"prompt": "tokyo itinerary"},
                    status="pending",
                )
                first_result = codex_search_report_flow.fn(
                    request={"prompt": "Create a 10 day Tokyo itinerary"},
                    run_id=first_parent.id,
                )
                second_parent = self.repository.start_run(
                    flow_name="codex_search_report_flow",
                    worker_key="mini-process",
                    input_payload={"prompt": "tokyo refine", "resume_from_run_id": first_parent.id},
                    status="pending",
                )
                second_result = codex_search_report_flow.fn(
                    request={
                        "prompt": "Refine the itinerary with quieter evenings.",
                        "resume_from_run_id": first_parent.id,
                        "codex_session_id": "99999999-9999-9999-9999-999999999999",
                    },
                    run_id=second_parent.id,
                )

        self.assertEqual(first_result["report"]["revision_number"], 1)
        self.assertEqual(second_result["report"]["revision_number"], 2)
        self.assertEqual(second_result["codex"]["resume_source"], "session_resume")
        self.assertEqual(
            fake_runner.requests[1].resume_session_id,
            "77777777-7777-7777-7777-777777777777",
        )

        current = self.repository.current_report_revision(f"search-report:{first_parent.id}")
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.revision_number, 2)
        revisions = self.repository.list_report_revisions(current.id)
        self.assertEqual([item.revision_number for item in revisions], [2, 1])

    def test_resume_without_prior_session_uses_memo_fallback(self) -> None:
        def handler(request: CodexSearchSessionRequest, run_number: int) -> AdapterResult:
            session_id = None if run_number == 1 else "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            return self._session_result(
                run_number=run_number,
                title=f"EU Jobs v{run_number}",
                session_id=session_id,
                mode="fresh",
            )

        fake_runner = _FakeSessionRunner(handler)

        with patch("flows.codex_search_report.execute_adapter", side_effect=self._execute_adapter_inline):
            with patch(
                "flows.codex_search_report._load_session_runner",
                return_value=(fake_runner, CodexSearchSessionRequest),
            ):
                first_parent = self.repository.start_run(
                    flow_name="codex_search_report_flow",
                    worker_key="mini-process",
                    input_payload={"prompt": "eu jobs"},
                    status="pending",
                )
                codex_search_report_flow.fn(
                    request={"prompt": "Identify EU jobs at risk to AI."},
                    run_id=first_parent.id,
                )
                second_parent = self.repository.start_run(
                    flow_name="codex_search_report_flow",
                    worker_key="mini-process",
                    input_payload={"prompt": "eu jobs refine", "resume_from_run_id": first_parent.id},
                    status="pending",
                )
                second_result = codex_search_report_flow.fn(
                    request={
                        "prompt": "Improve the report with more labor-market detail.",
                        "resume_from_run_id": first_parent.id,
                    },
                    run_id=second_parent.id,
                )

        self.assertIsNone(fake_runner.requests[0].resume_session_id)
        self.assertIsNone(fake_runner.requests[1].resume_session_id)
        self.assertEqual(second_result["codex"]["resume_source"], "memo_fallback")
        self.assertEqual(second_result["report"]["revision_number"], 2)
