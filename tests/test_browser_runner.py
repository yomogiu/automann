from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from libs.config import get_settings
from libs.contracts.workers import BrowserTaskRequest
from workers.browser_runner import BrowserTaskRunner


class BrowserTaskRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.settings = get_settings().model_copy(
            update={
                "artifact_root": self.root / "artifacts",
                "report_root": self.root / "reports",
                "runtime_root": self.root / "runtime",
            }
        )
        self.settings.artifact_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _playwright_stub(final_url: str = "https://example.invalid/final"):
        class _TracingStub:
            def start(self, **_kwargs) -> None:
                return None

            @staticmethod
            def stop(*, path: str) -> None:
                Path(path).write_bytes(b"trace")

        class _LocatorStub:
            def __init__(self) -> None:
                self.first = self

            def wait_for(self, **_kwargs) -> None:
                return None

            def click(self, **_kwargs) -> None:
                return None

            def fill(self, *_args, **_kwargs) -> None:
                return None

            def press(self, *_args, **_kwargs) -> None:
                return None

            def inner_text(self) -> str:
                return "Example"

            def inner_html(self) -> str:
                return "<h1>Example</h1>"

            def get_attribute(self, _name: str) -> str:
                return "https://example.invalid/author"

            def all_inner_texts(self) -> list[str]:
                return ["Example"]

            def evaluate_all(self, _script: str, *_args):
                return [
                    {"href": "https://example.invalid/a", "text": "a"},
                    {"href": "https://example.invalid/b", "text": "b"},
                ]

        class _KeyboardStub:
            def press(self, _key: str) -> None:
                return None

        class _MouseStub:
            def wheel(self, _x: int, _y: int) -> None:
                return None

        class _PageStub:
            def __init__(self) -> None:
                self.url = final_url
                self.keyboard = _KeyboardStub()
                self.mouse = _MouseStub()

            @staticmethod
            def goto(_url: str, **_kwargs) -> None:
                return None

            @staticmethod
            def wait_for_load_state(*_args, **_kwargs) -> None:
                return None

            @staticmethod
            def title() -> str:
                return "Example Title"

            @staticmethod
            def content() -> str:
                return "<html><body><h1>Example</h1></body></html>"

            @staticmethod
            def screenshot(*, path: str, full_page: bool) -> None:  # noqa: ARG004
                Path(path).write_bytes(b"png")

            @staticmethod
            def locator(_selector: str) -> _LocatorStub:
                return _LocatorStub()

            @staticmethod
            def close() -> None:
                return None

        class _ContextStub:
            def __init__(self) -> None:
                self.tracing = _TracingStub()

            @staticmethod
            def new_page() -> _PageStub:
                return _PageStub()

        class _BrowserStub:
            def __init__(self) -> None:
                self.contexts = [_ContextStub()]

            def new_context(self) -> _ContextStub:
                return self.contexts[0]

            @staticmethod
            def close() -> None:
                return None

        browser = _BrowserStub()

        chromium = MagicMock()
        chromium.launch.return_value = browser
        chromium.connect_over_cdp.return_value = browser

        playwright = MagicMock()
        playwright.chromium = chromium

        manager = MagicMock()
        manager.__enter__.return_value = playwright
        manager.__exit__.return_value = False
        return manager, chromium

    def test_launch_mode_captures_artifacts_and_emits_event(self) -> None:
        manager, _chromium = self._playwright_stub()
        runner = BrowserTaskRunner(self.settings)
        request = BrowserTaskRequest(
            job_name="launch-job",
            target_url="https://example.invalid/start",
            session={"mode": "launch"},
            capture={"html": True, "screenshot": True, "trace": True},
            extract=[{"name": "headline", "selector": "h1", "kind": "text"}],
        )

        with patch("workers.browser_runner.runner.sync_playwright", return_value=manager):
            result = runner.run(request)

        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["job_name"], "launch-job")
        self.assertEqual(result.structured_outputs["session_mode"], "launch")
        self.assertEqual(result.structured_outputs["final_url"], "https://example.invalid/final")
        self.assertIn("headline", result.structured_outputs["extracted_data"])
        artifact_kinds = {item.kind for item in result.artifact_manifest}
        self.assertIn("browser-html", artifact_kinds)
        self.assertIn("browser-screenshot", artifact_kinds)
        self.assertIn("browser-trace", artifact_kinds)
        self.assertIn("browser-metadata", artifact_kinds)
        event_names = [item.name for item in result.next_suggested_events]
        self.assertIn("browser.scrape.completed", event_names)

    def test_attach_mode_uses_cdp_url(self) -> None:
        manager, chromium = self._playwright_stub("https://x.com/home")
        runner = BrowserTaskRunner(self.settings)
        request = BrowserTaskRequest(
            job_name="attach-job",
            target_url="https://x.com",
            headless=False,
            session={"mode": "attach", "cdp_url": "http://127.0.0.1:9222"},
            capture={"html": False, "screenshot": False, "trace": False},
        )

        with patch("workers.browser_runner.runner.sync_playwright", return_value=manager):
            result = runner.run(request)

        chromium.connect_over_cdp.assert_called_once_with("http://127.0.0.1:9222")
        self.assertEqual(result.status.value, "completed")
        self.assertEqual(result.structured_outputs["session_mode"], "attach")
        self.assertEqual(result.structured_outputs["final_url"], "https://x.com/home")
