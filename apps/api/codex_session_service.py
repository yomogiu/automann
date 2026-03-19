from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping

from libs.config import Settings
from libs.contracts.models import AdapterResult, EventSuggestion, ObservationRecord, ReportRecord, WorkerStatus
from libs.db import LifeRepository
from workers.codex_app_server import CodexAppServerSessionManager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_dump(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _text_input(text: str) -> list[dict[str, str]]:
    return [{"type": "text", "text": text}]


class CodexSessionService:
    INTERACTIVE_FLOWS = frozenset({"research_report_flow", "codex_search_report_flow"})

    def __init__(
        self,
        settings: Settings,
        repository: LifeRepository,
        *,
        session_manager: CodexAppServerSessionManager | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.session_manager = session_manager or CodexAppServerSessionManager()
        self._event_tasks: dict[str, asyncio.Task[None]] = {}
        self._processed_seq: dict[str, int] = {}
        self._lock: asyncio.Lock | None = None
        self._final_answer_item_ids: dict[str, str] = {}
        self._final_answer_chunks: dict[str, list[str]] = {}

    async def _service_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def is_interactive_flow(self, flow_name: str, parameters: Mapping[str, Any] | None = None) -> bool:
        payload = parameters or {}
        return flow_name in self.INTERACTIVE_FLOWS or bool(payload.get("codex_interactive"))

    async def submit_run(self, *, run_id: str, flow_name: str, parameters: dict[str, Any]) -> dict[str, Any]:
        cwd = str(parameters.get("cwd") or self.settings.runtime_root)
        codex_session = self.repository.get_run_codex_session(run_id)
        input_items = self._build_turn_input(flow_name, parameters)
        thread_params = {"cwd": cwd}
        turn_params = {"cwd": cwd}

        if codex_session is not None and codex_session.codex_thread_id:
            payload = await self.session_manager.continue_run(
                run_id,
                cwd=cwd,
                thread_id=codex_session.codex_thread_id,
                thread_params=thread_params,
                turn_input=input_items,
                turn_params=turn_params,
            )
        else:
            payload = await self.session_manager.start_run(
                run_id,
                cwd=cwd,
                thread_params=thread_params,
                turn_input=input_items,
                turn_params=turn_params,
            )

        await self._ensure_event_task(run_id)
        self._persist_snapshot(run_id, payload.get("session", {}), last_event_at=_utcnow())
        self.repository.update_run_status(run_id, status="running")
        return {
            "run_id": run_id,
            "mode": "codex-app-server",
            "thread_id": payload.get("thread_id"),
            "turn_id": payload.get("turn_id"),
            "session": payload.get("session"),
        }

    async def stream_events(self, run_id: str, *, after_seq: int = 0) -> AsyncIterator[dict[str, Any]]:
        await self._ensure_event_task(run_id)
        async for event in self.session_manager.stream_events(run_id, after_seq=after_seq):
            yield event

    async def interrupt(self, run_id: str) -> dict[str, Any]:
        payload = await self.session_manager.interrupt(run_id)
        self._persist_snapshot(run_id, payload.get("session", {}), last_event_at=_utcnow())
        self._mark_run_interrupted(run_id, last_event_at=_utcnow())
        return payload

    async def steer(
        self,
        run_id: str,
        *,
        prompt: str,
        expected_turn_id: str | None = None,
    ) -> dict[str, Any]:
        payload = await self.session_manager.steer(
            run_id,
            input_items=_text_input(prompt),
            expected_turn_id=expected_turn_id,
        )
        self._persist_snapshot(run_id, payload.get("session", {}), last_event_at=_utcnow())
        self.repository.update_run_status(run_id, status="running")
        return payload

    async def get_session_snapshot(self, run_id: str) -> dict[str, Any] | None:
        try:
            return await self.session_manager.snapshot(run_id)
        except KeyError:
            codex_session = self.repository.get_run_codex_session(run_id)
            return codex_session.model_dump(mode="json") if codex_session is not None else None

    async def answer_interaction_request(
        self,
        *,
        run_id: str,
        ui_hints: Mapping[str, Any],
        response: dict[str, Any],
    ) -> dict[str, Any]:
        request_id = str(ui_hints.get("codex_request_id") or "").strip()
        request_kind = str(ui_hints.get("request_kind") or "").strip()
        if not request_id or not request_kind:
            raise KeyError("codex request metadata missing")
        result = self._map_interaction_response(request_kind, response)
        payload = await self.session_manager.answer_pending_request(run_id, request_id, result=result)
        self._persist_snapshot(run_id, payload.get("session", {}), last_event_at=_utcnow())
        self.repository.update_run_codex_session(
            run_id,
            codex_pending_request_id=None,
            codex_last_event_at=_utcnow(),
        )
        return payload

    async def _ensure_event_task(self, run_id: str) -> None:
        lock = await self._service_lock()
        async with lock:
            task = self._event_tasks.get(run_id)
            if task is None or task.done():
                self._event_tasks[run_id] = asyncio.create_task(self._consume_events(run_id))

    async def _consume_events(self, run_id: str) -> None:
        last_seq = self._processed_seq.get(run_id, 0)
        try:
            async for event in self.session_manager.stream_events(run_id, after_seq=last_seq):
                seq = int(event.get("seq") or 0)
                if seq <= self._processed_seq.get(run_id, 0):
                    continue
                self._processed_seq[run_id] = seq
                await self._handle_event(run_id, event)
        except KeyError:
            self._mark_run_interrupted(
                run_id,
                stderr="Codex app-server session ended unexpectedly.",
                last_event_at=_utcnow(),
            )
            return
        await self._finalize_completed_run(run_id)

    async def _handle_event(self, run_id: str, event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or "")
        last_event_at = self._parse_event_timestamp(event)
        snapshot = await self.get_session_snapshot(run_id) or {}
        self._persist_snapshot(run_id, snapshot, last_event_at=last_event_at)

        if event_type == "item/started":
            self._capture_final_answer_started(run_id, event)
        elif event_type == "item/agentMessage/delta":
            self._capture_final_answer_delta(run_id, event)
        elif event_type == "item/completed":
            self._capture_final_answer_completed(run_id, event)

        if event_type in {"turn/started", "turn/steered"}:
            self.repository.update_run_status(run_id, status="running")
            return

        if event_type == "turn/completed":
            self._persist_interactive_report(run_id)
            self.repository.update_run_status(run_id, status="completed")
            self.repository.update_run_codex_session(
                run_id,
                codex_active_turn_id=None,
                codex_pending_request_id=None,
                codex_state="completed",
                codex_last_event_at=last_event_at,
            )
            self._clear_transient_state(run_id)
            return

        if event_type == "turn/failed":
            self.repository.update_run_status(
                run_id,
                status="failed",
                stderr=self._event_error_text(event),
            )
            self.repository.update_run_codex_session(
                run_id,
                codex_active_turn_id=None,
                codex_pending_request_id=None,
                codex_state="failed",
                codex_last_event_at=last_event_at,
            )
            self._clear_transient_state(run_id)
            return

        if event_type == "turn/interrupt":
            self._mark_run_interrupted(run_id, last_event_at=last_event_at)
            return

        if event_type.startswith("request:"):
            await self._open_interaction_for_request(run_id, event)
            return

        if event_type == "serverRequest/resolved":
            self.repository.update_run_codex_session(
                run_id,
                codex_pending_request_id=None,
                codex_last_event_at=last_event_at,
            )

    async def _finalize_completed_run(self, run_id: str) -> None:
        snapshot = await self.get_session_snapshot(run_id) or {}
        active_turn_id = self._clean_string(snapshot.get("active_turn_id"))
        pending_request_ids = snapshot.get("pending_request_ids") or []
        if active_turn_id or pending_request_ids:
            return

        state = self._clean_string(snapshot.get("state")) or "completed"
        run = self.repository.get_run(run_id)
        if run is None or run.status in {"completed", "failed", "skipped"}:
            return
        if state == "failed":
            self.repository.update_run_status(run_id, status="failed")
            self.repository.update_run_codex_session(
                run_id,
                codex_state="failed",
                codex_active_turn_id=None,
                codex_pending_request_id=None,
                codex_last_event_at=_utcnow(),
            )
            return
        if state == "interrupted":
            self._mark_run_interrupted(run_id, last_event_at=_utcnow())
            return
        self._persist_interactive_report(run_id)
        self.repository.update_run_status(run_id, status="completed")
        self.repository.update_run_codex_session(
            run_id,
            codex_state="completed",
            codex_active_turn_id=None,
            codex_pending_request_id=None,
            codex_last_event_at=_utcnow(),
        )
        self._clear_transient_state(run_id)

    def _mark_run_interrupted(
        self,
        run_id: str,
        *,
        stderr: str | None = None,
        last_event_at: datetime | None = None,
    ) -> None:
        run = self.repository.get_run(run_id)
        if run is not None and run.status in {"completed", "failed", "skipped"}:
            return
        self.repository.update_run_status(
            run_id,
            status="failed",
            stderr=stderr or (run.stderr if run is not None else "") or "Codex run interrupted.",
        )
        self.repository.update_run_codex_session(
            run_id,
            codex_active_turn_id=None,
            codex_pending_request_id=None,
            codex_state="interrupted",
            codex_last_event_at=last_event_at or _utcnow(),
        )
        self._clear_transient_state(run_id)

    def _capture_final_answer_started(self, run_id: str, event: Mapping[str, Any]) -> None:
        item = dict((event.get("payload") or {}).get("item") or {})
        if item.get("type") != "agentMessage" or item.get("phase") != "final_answer":
            return
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            return
        self._final_answer_item_ids[run_id] = item_id
        self._final_answer_chunks[run_id] = []

    def _capture_final_answer_delta(self, run_id: str, event: Mapping[str, Any]) -> None:
        item_id = str(event.get("item_id") or "").strip()
        if not item_id or self._final_answer_item_ids.get(run_id) != item_id:
            return
        delta = str((event.get("payload") or {}).get("delta") or "")
        if delta:
            self._final_answer_chunks.setdefault(run_id, []).append(delta)

    def _capture_final_answer_completed(self, run_id: str, event: Mapping[str, Any]) -> None:
        item = dict((event.get("payload") or {}).get("item") or {})
        if item.get("type") != "agentMessage" or item.get("phase") != "final_answer":
            return
        item_id = str(item.get("id") or "").strip()
        if not item_id or self._final_answer_item_ids.get(run_id) != item_id:
            return
        if self._final_answer_chunks.get(run_id):
            return
        text = str(item.get("text") or "").strip()
        if text:
            self._final_answer_chunks[run_id] = [text]

    def _persist_interactive_report(self, run_id: str) -> None:
        run = self.repository.get_run(run_id)
        if run is None or run.flow_name != "paper_review_flow":
            return
        if run.structured_outputs or run.artifact_manifest:
            return

        review_text = "".join(self._final_answer_chunks.get(run_id) or []).strip()
        if not review_text:
            return

        payload = dict(run.input_payload or {})
        paper_id = str(payload.get("paper_id") or "").strip()
        source_url = str(payload.get("source_url") or "").strip()
        requested_title = str(payload.get("title") or "").strip()
        title = requested_title or (f"Paper {paper_id}" if paper_id else "Interactive Paper Review")
        summary = self._paper_review_summary(review_text)
        content_markdown = self._paper_review_markdown(
            title=title,
            paper_id=paper_id,
            source_url=source_url,
            body=review_text,
        )
        structured_outputs = {
            "paper_id": paper_id,
            "title": title,
            "source_url": source_url,
            "source_kind": "arxiv_html" if source_url else "unknown",
            "presentation_mode": "interactive_review",
            "summary": summary,
        }

        self.repository.persist_adapter_result(
            run_id,
            AdapterResult(
                status=WorkerStatus.COMPLETED,
                stdout=f"Reviewed paper {paper_id or run_id} (interactive_review).",
                structured_outputs=structured_outputs,
                observations=[
                    ObservationRecord(
                        kind="paper_review_mode",
                        summary="Presentation mode: interactive_review",
                        payload={
                            "paper_id": paper_id,
                            "presentation_mode": "interactive_review",
                            "source_kind": structured_outputs["source_kind"],
                        },
                        confidence=0.6,
                    )
                ],
                reports=[
                    ReportRecord(
                        report_type="paper_review",
                        title=title,
                        summary=summary,
                        content_markdown=content_markdown,
                        metadata={
                            "paper_id": paper_id,
                            "presentation_mode": "interactive_review",
                            "source_kind": structured_outputs["source_kind"],
                            "taxonomy": {
                                "filters": ["report"],
                                "tags": ["arxiv_paper_analysis"],
                            },
                        },
                    )
                ],
                next_suggested_events=[
                    EventSuggestion(name="paper.review.completed", payload={"paper_id": paper_id})
                ],
            ),
        )

    def _paper_review_summary(self, review_text: str) -> str:
        for marker in ("**Summary**", "## Summary", "# Summary"):
            marker_index = review_text.find(marker)
            if marker_index < 0:
                continue
            tail = review_text[marker_index + len(marker) :].strip()
            lines: list[str] = []
            for raw_line in tail.splitlines():
                line = raw_line.strip()
                if not line:
                    if lines:
                        break
                    continue
                if line.startswith("#") or (line.startswith("**") and line.endswith("**")):
                    break
                lines.append(line)
            summary = " ".join(lines).strip()
            if summary:
                return summary[:500]
        return " ".join(review_text.split())[:500]

    def _paper_review_markdown(self, *, title: str, paper_id: str, source_url: str, body: str) -> str:
        header_lines = [
            f"# {title}",
            "",
            f"- Paper ID: {paper_id or 'unknown'}",
            f"- Source URL: {source_url or 'unknown'}",
            "- Presentation Mode: interactive_review",
            "",
        ]
        normalized_body = body.strip()
        if normalized_body.startswith("# "):
            return normalized_body
        return "\n".join(header_lines) + normalized_body

    def _clear_transient_state(self, run_id: str) -> None:
        self._final_answer_item_ids.pop(run_id, None)
        self._final_answer_chunks.pop(run_id, None)

    async def _open_interaction_for_request(self, run_id: str, event: dict[str, Any]) -> None:
        payload = dict(event.get("payload") or {})
        request_id = str(payload.get("id") or "").strip()
        request_kind = str(payload.get("method") or "").strip()
        if not request_id or not request_kind:
            return

        current = self.repository.get_run_codex_session(run_id)
        if current is not None and current.codex_pending_request_id == request_id:
            return

        params = payload.get("params") or {}
        input_schema, default_input = self._build_interaction_schema(request_kind)
        prompt_md = (
            f"Codex requires user input for `{request_kind}`.\n\n"
            f"```json\n{_json_dump(params)}\n```"
        )
        self.repository.create_interaction(
            run_id=run_id,
            title=f"Codex approval: {request_kind}",
            prompt_md=prompt_md,
            input_schema=input_schema,
            default_input=default_input,
            checkpoint_key=None,
            prefect_flow_run_id=None,
            ui_hints={
                "codex_request_id": request_id,
                "request_kind": request_kind,
                "thread_id": event.get("thread_id"),
                "turn_id": event.get("turn_id"),
                "codex_managed": True,
            },
        )
        self.repository.update_run_codex_session(
            run_id,
            codex_pending_request_id=request_id,
            codex_state="waiting_input",
            codex_last_event_at=self._parse_event_timestamp(event),
        )

    def _persist_snapshot(self, run_id: str, snapshot: Mapping[str, Any], *, last_event_at: datetime) -> None:
        pending_ids = snapshot.get("pending_request_ids") or []
        pending_request_id = str(pending_ids[0]) if pending_ids else None
        process_id = snapshot.get("process_id")
        session_key = run_id if process_id in (None, "") else f"{run_id}:{process_id}"
        self.repository.update_run_codex_session(
            run_id,
            codex_thread_id=self._clean_string(snapshot.get("thread_id")),
            codex_active_turn_id=self._clean_string(snapshot.get("active_turn_id")),
            codex_session_key=session_key,
            codex_state=self._clean_string(snapshot.get("state")),
            codex_pending_request_id=pending_request_id,
            codex_last_event_at=last_event_at,
        )

    def _build_turn_input(self, flow_name: str, parameters: Mapping[str, Any]) -> list[dict[str, str]]:
        if flow_name == "research_report_flow":
            theme = str(parameters.get("theme") or "").strip()
            boundaries = [str(item).strip() for item in parameters.get("boundaries", []) if str(item).strip()]
            areas = [str(item).strip() for item in parameters.get("areas_of_interest", []) if str(item).strip()]
            parts = [
                "Create a research report.",
                f"Theme: {theme or 'unspecified'}",
            ]
            if boundaries:
                parts.append("Boundaries:\n- " + "\n- ".join(boundaries))
            if areas:
                parts.append("Areas of interest:\n- " + "\n- ".join(areas))
            metadata = dict(parameters.get("metadata") or {})
            if metadata:
                parts.append(f"Metadata:\n```json\n{_json_dump(metadata)}\n```")
            return _text_input("\n\n".join(parts))

        if flow_name == "codex_search_report_flow":
            prompt = str(parameters.get("prompt") or "").strip()
            if not prompt:
                prompt = "Research the requested topic and prepare a search report."
            parts = [prompt]
            enabled_sources = parameters.get("enabled_sources")
            if enabled_sources:
                parts.append(f"Enabled sources: {', '.join(str(item) for item in enabled_sources)}")
            if "planner_enabled" in parameters:
                parts.append(f"Planner enabled: {bool(parameters.get('planner_enabled'))}")
            if "max_results_per_query" in parameters:
                parts.append(f"Max results per query: {parameters.get('max_results_per_query')}")
            return _text_input("\n\n".join(parts))

        if flow_name == "paper_review_flow":
            paper_id = str(parameters.get("paper_id") or "").strip()
            source_url = str(parameters.get("source_url") or "").strip()
            title = str(parameters.get("title") or "").strip()
            metadata = dict(parameters.get("metadata") or {})
            parts = [
                "Review the paper directly from the provided source material.",
                "Do not submit or manage Automann runs.",
                "Do not call localhost, 127.0.0.1, the Automann API, or Prefect.",
                "Do not inspect the repository, runtime process state, or unrelated local files.",
                "Focus only on the supplied paper inputs.",
                f"Paper ID: {paper_id or 'unspecified'}",
                f"Title: {title or 'unspecified'}",
                f"Source URL: {source_url or 'unspecified'}",
                (
                    "If `metadata.html_path` is provided, use that local HTML file as the primary source. "
                    "Otherwise use `source_url`."
                ),
                (
                    "Produce a concise paper review with these sections: Summary, Main Contributions, "
                    "Strengths, Weaknesses, Questions or Risks, and Verdict."
                ),
                "If the source material is unavailable, explain the block clearly instead of attempting orchestration.",
            ]
            if metadata:
                parts.append(f"Metadata:\n```json\n{_json_dump(metadata)}\n```")
            return _text_input("\n\n".join(parts))

        prompt = str(parameters.get("prompt") or "").strip()
        if prompt:
            return _text_input(prompt)
        return _text_input(
            f"Run the interactive flow `{flow_name}` with parameters:\n\n```json\n{_json_dump(dict(parameters))}\n```"
        )

    def _build_interaction_schema(self, request_kind: str) -> tuple[dict[str, Any], dict[str, Any]]:
        if request_kind == "item/tool/requestUserInput":
            return (
                {
                    "type": "object",
                    "properties": {
                        "answers": {"type": "object", "additionalProperties": True},
                    },
                    "required": ["answers"],
                    "additionalProperties": True,
                },
                {"answers": {}},
            )

        if request_kind == "item/permissions/requestApproval":
            return (
                {
                    "type": "object",
                    "properties": {
                        "permissions": {"type": "object", "additionalProperties": True},
                        "scope": {"type": "string"},
                    },
                    "additionalProperties": True,
                },
                {"permissions": {}, "scope": "turn"},
            )

        return (
            {
                "type": "object",
                "properties": {
                    "approved": {"type": "boolean"},
                    "decision": {"type": "string"},
                },
                "additionalProperties": True,
            },
            {"approved": True},
        )

    def _map_interaction_response(self, request_kind: str, response: Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(response)
        if request_kind == "item/commandExecution/requestApproval":
            decision = str(payload.get("decision") or ("approved" if payload.get("approved", True) else "declined"))
            return {"decision": decision}
        if request_kind == "item/fileChange/requestApproval":
            decision = str(payload.get("decision") or ("accept" if payload.get("approved", True) else "decline"))
            return {"decision": decision}
        if request_kind == "item/permissions/requestApproval":
            return {
                "permissions": payload.get("permissions", {}),
                "scope": str(payload.get("scope") or "turn"),
            }
        if request_kind == "item/tool/requestUserInput":
            answers = payload.get("answers")
            if isinstance(answers, dict):
                return {"answers": answers}
            return {"answers": {}}
        return payload

    @staticmethod
    def _clean_string(value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    @staticmethod
    def _parse_event_timestamp(event: Mapping[str, Any]) -> datetime:
        raw = str(event.get("ts") or "").strip()
        if not raw:
            return _utcnow()
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return _utcnow()

    @staticmethod
    def _event_error_text(event: Mapping[str, Any]) -> str:
        payload = event.get("payload")
        if isinstance(payload, Mapping):
            for key in ("message", "error", "reason"):
                value = payload.get(key)
                if value:
                    return str(value)
        return _json_dump(payload)
