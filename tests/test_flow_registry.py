import unittest

from flows.registry import FLOW_SPECS


class FlowRegistryTests(unittest.TestCase):
    def test_flow_specs_cover_expected_lanes(self) -> None:
        self.assertEqual(FLOW_SPECS["daily_brief_flow"].deployment_path, "daily-brief/default")
        self.assertEqual(FLOW_SPECS["browser_job_flow"].default_work_pool, "browser-process")
        self.assertEqual(FLOW_SPECS["research_report_flow"].deployment_path, "research-report/default")
        self.assertEqual(FLOW_SPECS["paper_batch_flow"].deployment_path, "paper-batch/default")
        self.assertEqual(FLOW_SPECS["search_report_flow"].deployment_path, "search-report/default")
        self.assertEqual(FLOW_SPECS["codex_search_report_flow"].deployment_path, "search-report-manual/default")
