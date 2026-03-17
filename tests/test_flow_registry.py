import unittest

from flows.registry import FLOW_SPECS


class FlowRegistryTests(unittest.TestCase):
    def test_flow_specs_cover_expected_lanes(self) -> None:
        self.assertEqual(FLOW_SPECS["daily_brief_flow"].deployment_path, "daily-brief/default")
        self.assertEqual(FLOW_SPECS["browser_job_flow"].default_work_pool, "mini-docker")
