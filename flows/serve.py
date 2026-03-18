from __future__ import annotations

from prefect import serve

from .browser_job import browser_job_flow
from .artifact_ingest import artifact_ingest_flow
from .codex_search_report import codex_search_report_flow
from .daily_brief import daily_brief_flow
from .draft_article import substack_draft_flow
from .paper_batch import paper_batch_flow
from .paper_review import paper_review_flow
from .research_report import research_report_flow
from .registry import FLOW_SPECS


def main() -> None:
    serve(
        daily_brief_flow.to_deployment(
            name=FLOW_SPECS["daily_brief_flow"].deployment_name,
            cron=FLOW_SPECS["daily_brief_flow"].cron,
            work_pool_name=FLOW_SPECS["daily_brief_flow"].default_work_pool,
        ),
        paper_review_flow.to_deployment(
            name=FLOW_SPECS["paper_review_flow"].deployment_name,
            work_pool_name=FLOW_SPECS["paper_review_flow"].default_work_pool,
        ),
        paper_batch_flow.to_deployment(
            name=FLOW_SPECS["paper_batch_flow"].deployment_name,
            work_pool_name=FLOW_SPECS["paper_batch_flow"].default_work_pool,
        ),
        browser_job_flow.to_deployment(
            name=FLOW_SPECS["browser_job_flow"].deployment_name,
            work_pool_name=FLOW_SPECS["browser_job_flow"].default_work_pool,
        ),
        artifact_ingest_flow.to_deployment(
            name=FLOW_SPECS["artifact_ingest_flow"].deployment_name,
            work_pool_name=FLOW_SPECS["artifact_ingest_flow"].default_work_pool,
        ),
        codex_search_report_flow.to_deployment(
            name=FLOW_SPECS["codex_search_report_flow"].deployment_name,
            work_pool_name=FLOW_SPECS["codex_search_report_flow"].default_work_pool,
        ),
        substack_draft_flow.to_deployment(
            name=FLOW_SPECS["substack_draft_flow"].deployment_name,
            work_pool_name=FLOW_SPECS["substack_draft_flow"].default_work_pool,
        ),
        research_report_flow.to_deployment(
            name=FLOW_SPECS["research_report_flow"].deployment_name,
            work_pool_name=FLOW_SPECS["research_report_flow"].default_work_pool,
        ),
    )


if __name__ == "__main__":
    main()
