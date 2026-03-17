from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.config import Settings
from libs.contracts.events import EventName
from libs.contracts.models import (
    AdapterResult,
    BrowserExtractKind,
    BrowserSession,
    BrowserSessionMode,
    BrowserStepOp,
    EventSuggestion,
    ObservationRecord,
    WorkerStatus,
)
from libs.contracts.workers import BrowserTaskOutput, BrowserTaskRequest

from workers.common import build_file_artifact, ensure_worker_dir, resolve_worker_output_path, write_json, write_text


DEFAULT_NETWORK_IDLE_TIMEOUT_SECONDS = 3
sync_playwright = None


def _load_sync_playwright():
    if callable(sync_playwright):
        return sync_playwright
    try:
        from playwright.sync_api import sync_playwright as playwright_sync_playwright
    except Exception as exc:  # pragma: no cover - depends on runtime install
        raise RuntimeError(
            "Playwright is not installed. Install with `pip install -e \".[browser]\"` "
            "and run `playwright install chromium`."
        ) from exc
    return playwright_sync_playwright


def _unique_artifact_kinds(artifacts: list) -> list[str]:
    kinds: list[str] = []
    for artifact in artifacts:
        if artifact.kind not in kinds:
            kinds.append(artifact.kind)
    return kinds


class BrowserTaskRunner:
    worker_key = "browser_task_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: BrowserTaskRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        metadata_path = run_dir / "browser-metadata.json"
        generated_at = datetime.now(timezone.utc)
        artifacts = []
        observations = []
        execution_mode = "uninitialized"
        final_url: str | None = None
        page_title: str | None = None
        extracted_data: dict[str, Any] = {}
        page = None
        context = None
        browser = None
        owned_browser = False
        created_page = False
        tracing_started = False
        trace_path = run_dir / "trace.zip"

        try:
            sync_playwright = _load_sync_playwright()
            with sync_playwright() as playwright:
                if request.session.mode == BrowserSessionMode.ATTACH:
                    browser = playwright.chromium.connect_over_cdp(self._resolve_cdp_url(request))
                    execution_mode = "playwright.connect_over_cdp"
                    if not getattr(browser, "contexts", None):
                        raise RuntimeError("Attached browser did not expose any existing contexts")
                    context = browser.contexts[0]
                    page = context.new_page()
                    created_page = True
                else:
                    browser = playwright.chromium.launch(headless=request.headless)
                    owned_browser = True
                    context = browser.new_context()
                    page = context.new_page()
                    created_page = True
                    execution_mode = "playwright.launch"

                tracing_started = self._start_tracing(context=context, capture_trace=request.capture.trace)
                self._navigate(page=page, request=request)
                artifacts.extend(self._execute_steps(page=page, request=request, run_dir=run_dir))

                final_url = str(getattr(page, "url", "") or request.target_url)
                page_title = page.title()
                extracted_data = self._extract(page=page, request=request)

                if request.capture.html:
                    html_path = run_dir / "page.html"
                    write_text(html_path, page.content())
                    artifacts.append(
                        build_file_artifact(kind="browser-html", path=html_path, media_type="text/html")
                    )

                if request.capture.screenshot:
                    screenshot_path = run_dir / "page.png"
                    page.screenshot(path=str(screenshot_path), full_page=True)
                    if not screenshot_path.exists():
                        screenshot_path.write_bytes(b"")
                    artifacts.append(
                        build_file_artifact(
                            kind="browser-screenshot",
                            path=screenshot_path,
                            media_type="image/png",
                            metadata={"label": "final-page"},
                        )
                    )

                if request.extract:
                    extract_path = run_dir / "extract.json"
                    write_json(extract_path, extracted_data)
                    artifacts.append(
                        build_file_artifact(
                            kind="browser-extract",
                            path=extract_path,
                            media_type="application/json",
                            metadata={"field_count": len(extracted_data)},
                        )
                    )

                self._stop_tracing(context=context, tracing_started=tracing_started, trace_path=trace_path)
                tracing_started = False
                if trace_path.exists():
                    artifacts.append(
                        build_file_artifact(
                            kind="browser-trace",
                            path=trace_path,
                            media_type="application/zip",
                        )
                    )

                artifact_kinds = _unique_artifact_kinds(artifacts)
                if "browser-metadata" not in artifact_kinds:
                    artifact_kinds.append("browser-metadata")
                output = BrowserTaskOutput(
                    generated_at=generated_at,
                    job_name=request.job_name,
                    target_url=request.target_url,
                    final_url=final_url,
                    page_title=page_title,
                    session_mode=request.session.mode.value,
                    execution_mode=execution_mode,
                    status=WorkerStatus.COMPLETED.value,
                    extracted_data=extracted_data,
                    artifact_kinds=artifact_kinds,
                    step_count=len(request.steps),
                    extraction_count=len(extracted_data),
                    profile_name=request.session.profile_name,
                )
                write_json(
                    metadata_path,
                    {
                        "request": request.model_dump(mode="json"),
                        "output": output.model_dump(mode="json"),
                    },
                )
                artifacts.append(
                    build_file_artifact(
                        kind="browser-metadata",
                        path=metadata_path,
                        media_type="application/json",
                    )
                )

                observations.extend(
                    [
                        ObservationRecord(
                            kind="browser_navigation_result",
                            summary=f"Navigated to {final_url}",
                            payload={
                                "job_name": request.job_name,
                                "final_url": final_url,
                                "page_title": page_title,
                                "session_mode": request.session.mode.value,
                                "execution_mode": execution_mode,
                            },
                            confidence=0.9,
                        ),
                        ObservationRecord(
                            kind="browser_extraction_count",
                            summary=f"Collected {len(extracted_data)} extraction fields.",
                            payload={"field_count": len(extracted_data), "field_names": list(extracted_data.keys())},
                            confidence=0.8,
                        ),
                        ObservationRecord(
                            kind="browser_capture_complete",
                            summary=f"Created {len(artifacts)} browser artifacts.",
                            payload={"artifact_kinds": _unique_artifact_kinds(artifacts)},
                            confidence=0.9,
                        ),
                    ]
                )
                return AdapterResult(
                    status=WorkerStatus.COMPLETED,
                    stdout=f"Navigated to {final_url} and wrote {len(artifacts)} artifacts.",
                    artifact_manifest=artifacts,
                    structured_outputs=output.model_dump(mode="json"),
                    observations=observations,
                    next_suggested_events=[
                        EventSuggestion(
                            name=EventName.BROWSER_SCRAPE_COMPLETED,
                            payload={
                                "job_name": request.job_name,
                                "final_url": final_url,
                                "artifact_count": len(artifacts),
                            },
                        )
                    ],
                )
        except Exception as exc:
            self._stop_tracing(context=context, tracing_started=tracing_started, trace_path=trace_path)
            if trace_path.exists():
                artifacts.append(
                    build_file_artifact(
                        kind="browser-trace",
                        path=trace_path,
                        media_type="application/zip",
                    )
                )

            artifact_kinds = _unique_artifact_kinds(artifacts)
            if "browser-metadata" not in artifact_kinds:
                artifact_kinds.append("browser-metadata")
            output = BrowserTaskOutput(
                generated_at=generated_at,
                job_name=request.job_name,
                target_url=request.target_url,
                final_url=final_url,
                page_title=page_title,
                session_mode=request.session.mode.value,
                execution_mode=execution_mode,
                status=WorkerStatus.FAILED.value,
                extracted_data=extracted_data,
                artifact_kinds=artifact_kinds,
                step_count=len(request.steps),
                extraction_count=len(extracted_data),
                profile_name=request.session.profile_name,
            )
            write_json(
                metadata_path,
                {
                    "request": request.model_dump(mode="json"),
                    "output": output.model_dump(mode="json"),
                    "error": str(exc),
                },
            )
            artifacts.append(
                build_file_artifact(
                    kind="browser-metadata",
                    path=metadata_path,
                    media_type="application/json",
                )
            )
            return AdapterResult(
                status=WorkerStatus.FAILED,
                stderr=str(exc),
                artifact_manifest=artifacts,
                structured_outputs=output.model_dump(mode="json"),
                observations=[
                    ObservationRecord(
                        kind="browser_navigation_failed",
                        summary=f"Browser task failed for {request.job_name}",
                        payload={
                            "job_name": request.job_name,
                            "target_url": request.target_url,
                            "execution_mode": execution_mode,
                            "error": str(exc),
                        },
                        confidence=0.95,
                    )
                ],
            )
        finally:
            if created_page and page is not None and request.session.mode == BrowserSessionMode.ATTACH:
                try:
                    page.close()
                except Exception:
                    pass
            if owned_browser and browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass

    def _resolve_cdp_url(self, request: BrowserTaskRequest) -> str:
        cdp_url = request.session.cdp_url or str(getattr(self.settings, "browser_remote_debugging_url", "") or "")
        if not cdp_url:
            raise RuntimeError("Attach mode requires session.cdp_url or LIFE_BROWSER_REMOTE_DEBUGGING_URL")
        BrowserSession(mode=BrowserSessionMode.ATTACH, cdp_url=cdp_url)
        return cdp_url

    @staticmethod
    def _start_tracing(*, context, capture_trace: bool) -> bool:
        if not capture_trace or context is None:
            return False
        try:
            context.tracing.start(screenshots=True, snapshots=True)
            return True
        except Exception:
            return False

    @staticmethod
    def _stop_tracing(*, context, tracing_started: bool, trace_path: Path) -> None:
        if not tracing_started or context is None:
            return
        try:
            context.tracing.stop(path=str(trace_path))
            if not trace_path.exists():
                trace_path.write_bytes(b"")
        except Exception:
            return

    @staticmethod
    def _navigate(*, page, request: BrowserTaskRequest) -> None:
        timeout_ms = request.timeout_seconds * 1000
        page.goto(request.target_url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state(
                "networkidle",
                timeout=min(request.timeout_seconds, DEFAULT_NETWORK_IDLE_TIMEOUT_SECONDS) * 1000,
            )
        except Exception:
            return

    def _execute_steps(self, *, page, request: BrowserTaskRequest, run_dir: Path) -> list:
        artifacts = []
        for index, step in enumerate(request.steps, start=1):
            timeout_ms = (step.timeout_seconds or request.timeout_seconds) * 1000
            if step.op == BrowserStepOp.WAIT_FOR:
                if step.wait_until is not None:
                    page.wait_for_load_state(step.wait_until, timeout=timeout_ms)
                else:
                    page.locator(str(step.selector)).first.wait_for(state=step.state, timeout=timeout_ms)
            elif step.op == BrowserStepOp.CLICK:
                page.locator(str(step.selector)).first.click(timeout=timeout_ms)
            elif step.op == BrowserStepOp.FILL:
                page.locator(str(step.selector)).first.fill(str(step.value), timeout=timeout_ms)
            elif step.op == BrowserStepOp.PRESS:
                if step.selector:
                    page.locator(step.selector).first.press(str(step.key), timeout=timeout_ms)
                else:
                    page.keyboard.press(str(step.key))
            elif step.op == BrowserStepOp.SCROLL:
                page.mouse.wheel(0, step.pixels)
            elif step.op == BrowserStepOp.SCREENSHOT:
                screenshot_path = resolve_worker_output_path(run_dir, step.path or f"step-{index:02d}.png")
                if screenshot_path is None:
                    raise RuntimeError("Failed to resolve screenshot path")
                page.screenshot(path=str(screenshot_path), full_page=True)
                if not screenshot_path.exists():
                    screenshot_path.write_bytes(b"")
                artifacts.append(
                    build_file_artifact(
                        kind="browser-screenshot",
                        path=screenshot_path,
                        media_type="image/png",
                        metadata={"label": step.name or f"step-{index}", "step_index": index},
                    )
                )
        return artifacts

    @staticmethod
    def _extract(*, page, request: BrowserTaskRequest) -> dict[str, Any]:
        extracted: dict[str, Any] = {}
        for item in request.extract:
            if item.kind == BrowserExtractKind.TEXT:
                if item.all and hasattr(page, "eval_on_selector_all"):
                    value = page.eval_on_selector_all(item.selector, "(nodes) => nodes.map((node) => node.innerText)")
                elif hasattr(page, "inner_text"):
                    value = page.inner_text(item.selector)
                else:
                    locator = page.locator(item.selector)
                    value = locator.all_inner_texts() if item.all else locator.first.inner_text()
            elif item.kind == BrowserExtractKind.HTML:
                if item.all and hasattr(page, "eval_on_selector_all"):
                    value = page.eval_on_selector_all(item.selector, "(nodes) => nodes.map((node) => node.innerHTML)")
                else:
                    locator = page.locator(item.selector)
                    value = (
                        locator.evaluate_all("(nodes) => nodes.map((node) => node.innerHTML)")
                        if item.all
                        else locator.first.inner_html()
                    )
            elif item.kind == BrowserExtractKind.ATTRIBUTE:
                if item.all and hasattr(page, "eval_on_selector_all"):
                    value = page.eval_on_selector_all(
                        item.selector,
                        "(nodes, attribute) => nodes.map((node) => node.getAttribute(attribute))",
                        item.attribute,
                    )
                elif hasattr(page, "get_attribute"):
                    value = page.get_attribute(item.selector, str(item.attribute))
                else:
                    locator = page.locator(item.selector)
                    value = (
                        locator.evaluate_all(
                            "(nodes, attribute) => nodes.map((node) => node.getAttribute(attribute))",
                            item.attribute,
                        )
                        if item.all
                        else locator.first.get_attribute(str(item.attribute))
                    )
            else:
                if hasattr(page, "eval_on_selector_all"):
                    links = page.eval_on_selector_all(
                        item.selector,
                        """
                        (nodes) => nodes.map((node) => ({
                          "text": (node.textContent || "").trim(),
                          "href": node.href || node.getAttribute("href")
                        }))
                        """,
                    )
                else:
                    locator = page.locator(item.selector)
                    links = locator.evaluate_all(
                        """
                        (nodes) => nodes.map((node) => ({
                          "text": (node.textContent || "").trim(),
                          "href": node.href || node.getAttribute("href")
                        }))
                        """
                    )
                value = links if item.all else (links[0] if links else None)
            extracted[item.name] = value
        return extracted
