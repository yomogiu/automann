from __future__ import annotations

import unittest

from libs.contracts.models import DraftArticleRequest, ResearchEditMode, ResearchReportRequest


class ResearchContractTests(unittest.TestCase):
    def test_research_report_request_defaults_edit_mode(self) -> None:
        request = ResearchReportRequest(theme="Open research")
        self.assertEqual(request.edit_mode, ResearchEditMode.MERGE)
        self.assertEqual(request.boundaries, [])
        self.assertEqual(request.areas_of_interest, [])

    def test_draft_article_request_accepts_legacy_theme_mode(self) -> None:
        request = DraftArticleRequest(theme="Legacy draft")
        self.assertEqual(request.theme, "Legacy draft")
        self.assertIsNone(request.source_report_id)

    def test_draft_article_request_accepts_source_report_mode(self) -> None:
        request = DraftArticleRequest(source_report_id="report-1")
        self.assertEqual(request.source_report_id, "report-1")
        self.assertIsNone(request.theme)

    def test_draft_article_request_rejects_missing_source_and_theme(self) -> None:
        with self.assertRaises(ValueError):
            DraftArticleRequest()
