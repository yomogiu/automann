from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from libs.contracts.workers import CodexSearchSessionRequest
from workers.codex_session_runner import CodexSearchSessionRunner
from workers.codex_session_runner.runner import _SessionIndexEntry


class _CompletedProcess:
    def __init__(self, *, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class CodexSearchSessionRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.settings = get_settings().model_copy(update={"artifact_root": self.root / "artifacts"})

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fresh_run_uses_global_search_exec_command(self) -> None:
        runner = CodexSearchSessionRunner(self.settings)
        request = CodexSearchSessionRequest(
            prompt='{"title":"Report"}',
            extra_args=["--model", "gpt-5.4"],
        )

        def fake_run(cmd, **kwargs):  # noqa: ANN001
            output_path = Path(cmd[cmd.index("-o") + 1])
            output_path.write_text('{"title":"Report"}', encoding="utf-8")
            self.assertEqual(kwargs["capture_output"], True)
            self.assertEqual(kwargs["text"], True)
            return _CompletedProcess(stdout='{"session_id":"123e4567-e89b-12d3-a456-426614174000"}\n')

        with patch("workers.codex_session_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
            with patch.object(runner, "_read_session_index", side_effect=[{}, {}]):
                with patch.object(
                    runner,
                    "_resolve_session_details",
                    return_value=("123e4567-e89b-12d3-a456-426614174000", "json_events", "Tokyo"),
                ):
                    with patch("workers.codex_session_runner.runner.subprocess.run", side_effect=fake_run) as run_mock:
                        result = runner.run(request)

        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["mode"], "fresh")
        self.assertEqual(result.structured_outputs["session_id"], "123e4567-e89b-12d3-a456-426614174000")
        command = run_mock.call_args[0][0]
        self.assertEqual(command[:4], ["codex", "--search", "exec", "--json"])
        self.assertIn("--model", command)
        self.assertIn("gpt-5.4", command)

    def test_resume_run_uses_global_search_exec_resume_command(self) -> None:
        runner = CodexSearchSessionRunner(self.settings)
        request = CodexSearchSessionRequest(
            prompt='{"title":"Report"}',
            resume_session_id="123e4567-e89b-12d3-a456-426614174000",
        )

        def fake_run(cmd, **kwargs):  # noqa: ANN001
            output_path = Path(cmd[cmd.index("-o") + 1])
            output_path.write_text('{"title":"Report"}', encoding="utf-8")
            return _CompletedProcess(stdout='{"session_id":"123e4567-e89b-12d3-a456-426614174000"}\n')

        with patch("workers.codex_session_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
            with patch.object(runner, "_read_session_index", side_effect=[{}, {}]):
                with patch.object(
                    runner,
                    "_resolve_session_details",
                    return_value=("123e4567-e89b-12d3-a456-426614174000", "json_events", "Tokyo"),
                ):
                    with patch("workers.codex_session_runner.runner.subprocess.run", side_effect=fake_run) as run_mock:
                        result = runner.run(request)

        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["mode"], "resume")
        command = run_mock.call_args[0][0]
        self.assertEqual(
            command[:6],
            ["codex", "--search", "exec", "resume", "123e4567-e89b-12d3-a456-426614174000", "--json"],
        )

    def test_session_id_and_thread_name_are_extracted_from_json_events(self) -> None:
        raw_events = "\n".join(
            [
                '{"event":"step","payload":{"foo":"bar"}}',
                '{"event":"session","payload":{"session_id":"123e4567-e89b-12d3-a456-426614174000","thread_name":"Tokyo"}}',
            ]
        )

        session_id = CodexSearchSessionRunner._session_id_from_events(raw_events)
        thread_name = CodexSearchSessionRunner._thread_name_from_events(raw_events)

        self.assertEqual(session_id, "123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(thread_name, "Tokyo")

    def test_session_index_diff_uses_new_or_updated_entry(self) -> None:
        pre = {
            "00000000-0000-0000-0000-000000000001": _SessionIndexEntry(
                session_id="00000000-0000-0000-0000-000000000001",
                thread_name="Old",
                updated_at="2026-03-17T10:00:00Z",
            )
        }
        post = {
            **pre,
            "123e4567-e89b-12d3-a456-426614174000": _SessionIndexEntry(
                session_id="123e4567-e89b-12d3-a456-426614174000",
                thread_name="Tokyo",
                updated_at="2026-03-17T10:01:00Z",
            ),
        }

        diff = CodexSearchSessionRunner._session_index_diff(pre, post)

        self.assertIsNotNone(diff)
        assert diff is not None
        self.assertEqual(diff.session_id, "123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(diff.thread_name, "Tokyo")

    def test_resume_with_blank_session_id_fails_fast(self) -> None:
        runner = CodexSearchSessionRunner(self.settings)
        request = CodexSearchSessionRequest(prompt='{"title":"Report"}', resume_session_id="   ")

        result = runner.run(request)

        self.assertEqual(result.status.value, "failed")
        self.assertEqual(result.structured_outputs["error_reason"], "missing_session_id")

    def test_missing_codex_binary_returns_typed_failure(self) -> None:
        runner = CodexSearchSessionRunner(self.settings)
        request = CodexSearchSessionRequest(prompt='{"title":"Report"}')

        with patch("workers.codex_session_runner.runner.shutil.which", return_value=None):
            result = runner.run(request)

        self.assertEqual(result.status.value, "failed")
        self.assertEqual(result.structured_outputs["error_reason"], "codex_missing")
        self.assertEqual(result.structured_outputs["mode"], "placeholder")
