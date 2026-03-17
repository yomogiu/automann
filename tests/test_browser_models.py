from __future__ import annotations

import unittest

from pydantic import ValidationError

from libs.contracts.models import BrowserJobRequest


class BrowserJobRequestModelTests(unittest.TestCase):
    def test_legacy_payload_remains_compatible(self) -> None:
        request = BrowserJobRequest.model_validate(
            {
                "job_name": "legacy-job",
                "target_url": "https://example.invalid",
                "login_profile": "existing-profile",
                "capture_html": True,
                "capture_screenshots": False,
            }
        )

        self.assertEqual(request.job_name, "legacy-job")
        self.assertEqual(request.target_url, "https://example.invalid")
        self.assertEqual(request.login_profile, "existing-profile")
        self.assertTrue(request.capture_html)
        self.assertFalse(request.capture_screenshots)

    def test_login_profile_aliases_to_session_profile_name(self) -> None:
        request = BrowserJobRequest.model_validate(
            {
                "job_name": "alias-test",
                "target_url": "https://example.invalid",
                "login_profile": "operator-default",
            }
        )

        self.assertIn(request.session.mode, {"launch", "attach"})
        self.assertEqual(request.session.profile_name, "operator-default")

    def test_supports_session_steps_extract_and_timeout(self) -> None:
        request = BrowserJobRequest.model_validate(
            {
                "job_name": "extended-browser-job",
                "target_url": "https://example.invalid/article",
                "session": {
                    "mode": "attach",
                    "cdp_url": "http://127.0.0.1:9222",
                    "profile_name": "ops-profile",
                },
                "timeout_seconds": 75,
                "steps": [
                    {"op": "wait_for", "selector": "article"},
                    {"op": "click", "selector": "button[aria-label='Accept']"},
                    {"op": "fill", "selector": "input[name='q']", "value": "GPU"},
                    {"op": "press", "key": "Enter"},
                    {"op": "scroll", "amount": 800},
                    {"op": "screenshot", "name": "post-scroll"},
                ],
                "extract": [
                    {"name": "headline", "selector": "h1", "kind": "text"},
                    {"name": "all_links", "selector": "a", "kind": "links", "all": True},
                    {"name": "author_href", "selector": "a.author", "kind": "attribute", "attribute": "href"},
                ],
            }
        )

        self.assertEqual(request.session.mode, "attach")
        self.assertEqual(request.session.cdp_url, "http://127.0.0.1:9222")
        self.assertEqual(request.timeout_seconds, 75)
        self.assertEqual([step.op for step in request.steps], ["wait_for", "click", "fill", "press", "scroll", "screenshot"])
        self.assertEqual([item.name for item in request.extract], ["headline", "all_links", "author_href"])

    def test_rejects_non_loopback_cdp_url(self) -> None:
        with self.assertRaises(ValidationError):
            BrowserJobRequest.model_validate(
                {
                    "job_name": "bad-cdp",
                    "target_url": "https://example.invalid",
                    "session": {
                        "mode": "attach",
                        "cdp_url": "http://192.168.1.20:9222",
                    },
                }
            )

    def test_rejects_unknown_step_operation(self) -> None:
        with self.assertRaises(ValidationError):
            BrowserJobRequest.model_validate(
                {
                    "job_name": "bad-step",
                    "target_url": "https://example.invalid",
                    "steps": [{"op": "navigate", "url": "https://example.invalid"}],
                }
            )

    def test_rejects_attribute_extract_without_attribute_name(self) -> None:
        with self.assertRaises(ValidationError):
            BrowserJobRequest.model_validate(
                {
                    "job_name": "bad-extract",
                    "target_url": "https://example.invalid",
                    "extract": [{"name": "author", "selector": "a.author", "kind": "attribute"}],
                }
            )
