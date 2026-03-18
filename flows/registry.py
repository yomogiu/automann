from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FlowSpec:
    key: str
    slug: str
    deployment_name: str
    default_work_pool: str
    cron: str | None = None

    @property
    def deployment_path(self) -> str:
        return f"{self.slug}/{self.deployment_name}"


FLOW_SPECS: dict[str, FlowSpec] = {
    "daily_brief_flow": FlowSpec(
        key="daily_brief_flow",
        slug="daily-brief",
        deployment_name="default",
        default_work_pool="mini-process",
        cron="0 7 * * *",
    ),
    "paper_review_flow": FlowSpec(
        key="paper_review_flow",
        slug="paper-review",
        deployment_name="default",
        default_work_pool="mini-process",
    ),
    "browser_job_flow": FlowSpec(
        key="browser_job_flow",
        slug="browser-job",
        deployment_name="default",
        default_work_pool="browser-process",
    ),
    "artifact_ingest_flow": FlowSpec(
        key="artifact_ingest_flow",
        slug="artifact-ingest",
        deployment_name="default",
        default_work_pool="mini-process",
    ),
    "substack_draft_flow": FlowSpec(
        key="substack_draft_flow",
        slug="substack-draft",
        deployment_name="default",
        default_work_pool="mini-process",
    ),
    "research_report_flow": FlowSpec(
        key="research_report_flow",
        slug="research-report",
        deployment_name="default",
        default_work_pool="mini-process",
    ),
    "paper_batch_flow": FlowSpec(
        key="paper_batch_flow",
        slug="paper-batch",
        deployment_name="default",
        default_work_pool="mini-process",
    ),
    "codex_search_report_flow": FlowSpec(
        key="codex_search_report_flow",
        slug="search-report-manual",
        deployment_name="default",
        default_work_pool="mini-process",
    ),
}
