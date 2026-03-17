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
        default_work_pool="mini-docker",
    ),
    "substack_draft_flow": FlowSpec(
        key="substack_draft_flow",
        slug="substack-draft",
        deployment_name="default",
        default_work_pool="mini-process",
    ),
}
