from __future__ import annotations

import asyncio
from typing import Any

try:
    from prefect.deployments import run_deployment
except Exception:  # pragma: no cover - Prefect may not be installed during static inspection
    run_deployment = None

from flows.browser_job import browser_job_flow
from flows.daily_brief import daily_brief_flow
from flows.draft_article import substack_draft_flow
from flows.paper_review import paper_review_flow


FLOW_CALLABLES = {
    "daily_brief_flow": daily_brief_flow,
    "paper_review_flow": paper_review_flow,
    "browser_job_flow": browser_job_flow,
    "substack_draft_flow": substack_draft_flow,
}


class PrefectOrchestrationClient:
    @property
    def prefect_available(self) -> bool:
        return run_deployment is not None

    async def submit(
        self,
        *,
        deployment_path: str,
        request: dict[str, Any],
        run_id: str,
    ):
        if run_deployment is None:
            return None
        return await run_deployment(
            name=deployment_path,
            parameters={"request": request, "run_id": run_id},
        )

    async def run_local(
        self,
        *,
        flow_name: str,
        request: dict[str, Any],
        run_id: str,
    ) -> dict[str, Any]:
        flow_fn = FLOW_CALLABLES[flow_name]
        return await asyncio.to_thread(flow_fn, request=request, run_id=run_id)
