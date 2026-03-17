from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from workers.codex_session_runner import CodexSearchSessionRequest, CodexSearchSessionRunner
from workers.codex_session_runner.runner import _SessionIndexEntry


class CodexSearchSessionRunnerTests(unittest.TestCase):
    def test_fresh_mode_builds_global_search_exec_command_and_extracts_session_id(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(update={"artifact_root": root / "artifacts"})
            runner = CodexSearchSessionRunner(settings)
            request = CodexSearchSessionRequest(prompt="research tokyo itinerary")

            def fake_run(cmd, **kwargs):  # noqa: ANN001
                output_path = Path(cmd[cmd.index("-o") + 1])
                output_path.write_text('{"title":"Tokyo"}', encoding="utf-8")

                class Result:
                    returncode = 0
                    stdout = '{"session_id":"11111111-1111-1111-1111-111111111111","thread_name":"Tokyo run"}\n'
                    stderr = ""

                return Result()

            with patch("workers.codex_session_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
                with patch("workers.codex_session_runner.runner.subprocess.run", side_effect=fake_run) as run_mock:
                    result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["mode"], "fresh")
            self.assertEqual(result.structured_outputs["session_id"], "11111111-1111-1111-1111-111111111111")
            self.assertEqual(result.structured_outputs["session_id_source"], "json_events")
            self.assertEqual(result.structured_outputs["thread_name"], "Tokyo run")
            command = run_mock.call_args[0][0]
            self.assertEqual(command[:4], ["codex", "--search", "exec", "--json"])

        # `-o <output> <prompt>` is appended after the base command.
            self.assertEqual(command[-1], "research tokyo itinerary")

    def test_resume_mode_builds_search_exec_resume_command(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(update={"artifact_root": root / "artifacts"})
            runner = CodexSearchSessionRunner(settings)
            request = CodexSearchSessionRequest(
                prompt="continue report",
                resume_session_id="22222222-2222-2222-2222-222222222222",
            )

            def fake_run(cmd, **kwargs):  # noqa: ANN001
                output_path = Path(cmd[cmd.index("-o") + 1])
                output_path.write_text('{"title":"Tokyo v2"}', encoding="utf-8")

                class Result:
                    returncode = 0
                    stdout = '{"session_id":"33333333-3333-3333-3333-333333333333"}\n'
                    stderr = ""

                return Result()

            with patch("workers.codex_session_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
                with patch("workers.codex_session_runner.runner.subprocess.run", side_effect=fake_run) as run_mock:
                    result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["mode"], "resume")
            self.assertEqual(
                result.structured_outputs["resumed_from_session_id"],
                "22222222-2222-2222-2222-222222222222",
            )
            command = run_mock.call_args[0][0]
            self.assertEqual(
                command[:6],
                ["codex", "--search", "exec", "resume", "22222222-2222-2222-2222-222222222222", "--json"],
            )

    def test_runner_falls_back_to_session_index_diff_when_events_lack_session_id(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(update={"artifact_root": root / "artifacts"})
            runner = CodexSearchSessionRunner(settings)
            request = CodexSearchSessionRequest(prompt="research eu jobs")

            def fake_run(cmd, **kwargs):  # noqa: ANN001
                output_path = Path(cmd[cmd.index("-o") + 1])
                output_path.write_text('{"title":"EU Jobs"}', encoding="utf-8")

                class Result:
                    returncode = 0
                    stdout = '{"event":"done"}\n'
                    stderr = ""

                return Result()

            pre_sessions = {}
            post_sessions = {
                "44444444-4444-4444-4444-444444444444": _SessionIndexEntry(
                    session_id="44444444-4444-4444-4444-444444444444",
                    thread_name="EU jobs",
                    updated_at="2026-03-17T16:00:00Z",
                )
            }

            with patch("workers.codex_session_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
                with patch("workers.codex_session_runner.runner.subprocess.run", side_effect=fake_run):
                    with patch.object(
                        CodexSearchSessionRunner,
                        "_read_session_index",
                        side_effect=[pre_sessions, post_sessions],
                    ):
                        result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["session_id"], "44444444-4444-4444-4444-444444444444")
            self.assertEqual(result.structured_outputs["session_id_source"], "session_index")
            self.assertEqual(result.structured_outputs["thread_name"], "EU jobs")

    def test_missing_session_id_fails_before_command_execution(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(update={"artifact_root": root / "artifacts"})
            runner = CodexSearchSessionRunner(settings)
            request = CodexSearchSessionRequest(prompt="resume report", resume_session_id="  ")

            with patch("workers.codex_session_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
                with patch("workers.codex_session_runner.runner.subprocess.run") as run_mock:
                    result = runner.run(request)

            self.assertEqual(result.status.value, "failed")
            self.assertEqual(result.structured_outputs["error_reason"], "missing_session_id")
            run_mock.assert_not_called()

    def test_resume_failures_are_typed(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(update={"artifact_root": root / "artifacts"})
            runner = CodexSearchSessionRunner(settings)
            request = CodexSearchSessionRequest(
                prompt="continue report",
                resume_session_id="55555555-5555-5555-5555-555555555555",
            )

            class Result:
                returncode = 1
                stdout = ""
                stderr = "resume failed"

            with patch("workers.codex_session_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
                with patch("workers.codex_session_runner.runner.subprocess.run", return_value=Result()):
                    result = runner.run(request)

            self.assertEqual(result.status.value, "failed")
            self.assertEqual(result.structured_outputs["error_reason"], "resume_failed")

    def test_missing_codex_cli_returns_typed_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = get_settings().model_copy(update={"artifact_root": root / "artifacts"})
            runner = CodexSearchSessionRunner(settings)
            request = CodexSearchSessionRequest(prompt="research tokyo itinerary")

            with patch("workers.codex_session_runner.runner.shutil.which", return_value=None):
                result = runner.run(request)

            self.assertEqual(result.status.value, "failed")
            self.assertEqual(result.structured_outputs["error_reason"], "codex_missing")
            self.assertEqual(result.structured_outputs["mode"], "placeholder")
