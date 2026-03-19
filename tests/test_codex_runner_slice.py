from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from libs.config import get_settings
from workers.codex_runner import CodexCliRequest, CodexCliRunner, CodexTaskRequest, load_paper_review_prompt


class CodexRunnerSliceTests(unittest.TestCase):
    def test_codex_cli_request_aliases_worker_contract_request(self) -> None:
        request = CodexCliRequest(prompt="hello world")
        self.assertIsInstance(request, CodexTaskRequest)

    def test_plain_text_mode_preserves_existing_command_shape(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = get_settings().model_copy(update={"artifact_root": Path(temp_dir)})
            runner = CodexCliRunner(settings)
            request = CodexCliRequest(prompt="hello world", extra_args=["--temperature", "0.2"])

            with patch("workers.codex_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
                with patch("workers.codex_runner.runner.subprocess.run") as run_mock:
                    run_mock.return_value.returncode = 0
                    run_mock.return_value.stdout = "ok"
                    run_mock.return_value.stderr = ""

                    result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["mode"], "plain-text")
            run_mock.assert_called_once()
            command = run_mock.call_args[0][0]
            self.assertEqual(
                command,
                ["codex", "exec", "--skip-git-repo-check", "--temperature", "0.2", "hello world"],
            )

    def test_structured_mode_builds_json_schema_command(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            schema_path = root / "schema.json"
            schema_path.write_text('{"type":"object"}', encoding="utf-8")
            output_path = root / "analysis.json"

            settings = get_settings().model_copy(update={"artifact_root": root / "artifacts"})
            runner = CodexCliRunner(settings)
            request = CodexCliRequest(
                prompt="analyze paper",
                output_schema=str(schema_path),
                output_path=str(output_path),
                extra_args=["--model", "gpt-5.4"],
            )

            def fake_run(cmd, **kwargs):  # noqa: ANN001
                self.assertIn("--json", cmd)
                self.assertIn("--output-schema", cmd)
                self.assertIn("-o", cmd)
                output_path.write_text('{"ok": true}', encoding="utf-8")
                class Result:
                    returncode = 0
                    stdout = "structured"
                    stderr = ""
                return Result()

            with patch("workers.codex_runner.runner.shutil.which", return_value="/usr/local/bin/codex"):
                with patch("workers.codex_runner.runner.subprocess.run", side_effect=fake_run) as run_mock:
                    result = runner.run(request)

            self.assertEqual(result.status.value, "completed")
            self.assertEqual(result.structured_outputs["mode"], "structured-json")
            command = run_mock.call_args[0][0]
            self.assertEqual(command[-1], "analyze paper")
            self.assertEqual(command[:2], ["codex", "exec"])
            self.assertIn("--skip-git-repo-check", command)
            self.assertIn("--model", command)
            self.assertIn("gpt-5.4", command)
            self.assertTrue(any(item.kind == "json" for item in result.artifact_manifest))

    def test_build_command_does_not_duplicate_skip_git_repo_check(self) -> None:
        command = CodexCliRunner._build_command(
            CodexCliRequest(
                prompt="hello world",
                extra_args=["--skip-git-repo-check", "--model", "gpt-5.4"],
            )
        )

        self.assertEqual(command.count("--skip-git-repo-check"), 1)

    def test_structured_mode_requires_schema_and_output_path_together(self) -> None:
        with self.assertRaises(ValueError):
            CodexCliRequest(prompt="x", output_schema="/tmp/schema.json")
        with self.assertRaises(ValueError):
            CodexCliRequest(prompt="x", output_path="/tmp/out.json")

    def test_load_paper_review_prompt_mentions_report_and_clarity_constraints(self) -> None:
        prompt = load_paper_review_prompt()
        self.assertIn('"report"', prompt)
        self.assertIn('"executive_summary"', prompt)
        self.assertIn("whole document", prompt.lower())
        self.assertIn("serious learner", prompt.lower())
        self.assertIn("inference", prompt.lower())
        self.assertIn("do not turn every concept", prompt.lower())
