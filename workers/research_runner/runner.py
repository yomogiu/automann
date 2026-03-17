from __future__ import annotations

import csv
import json
import re
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from libs.config import Settings
from libs.contracts.models import AdapterResult, ObservationRecord, ReportRecord, WorkerStatus
from libs.contracts.workers import (
    ResearchReportWorkerOutput,
    ResearchReportWorkerRequest,
)
from workers.codex_runner import CodexCliRequest, CodexCliRunner
from workers.common import build_file_artifact, ensure_worker_dir, write_json, write_text


RESEARCH_REPORT_PROMPT_PATH = Path(__file__).resolve().parents[2] / "libs" / "prompts" / "research_report.md"
RESEARCH_REPORT_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "research-report.schema.json"


def load_research_report_prompt() -> str:
    return RESEARCH_REPORT_PROMPT_PATH.read_text(encoding="utf-8")


class ResearchReportRunner:
    worker_key = "research_report_runner"

    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: ResearchReportWorkerRequest) -> AdapterResult:
        run_dir = ensure_worker_dir(self.settings, self.worker_key)
        codex_output_path = run_dir / "research_output_raw.json"

        codex_result = CodexCliRunner(self.settings).run(
            CodexCliRequest(
                prompt=self._render_prompt(request),
                cwd=str(run_dir),
                output_schema=str(RESEARCH_REPORT_SCHEMA_PATH),
                output_path=str(codex_output_path),
            )
        )

        normalized_payload, mode = self._resolve_payload(
            request=request,
            codex_result=codex_result,
            output_path=codex_output_path,
        )

        markdown = self._render_markdown(normalized_payload, request)
        markdown_path = run_dir / "research_report.md"
        payload_path = run_dir / "research_report.json"
        write_text(markdown_path, markdown)
        write_json(payload_path, normalized_payload)

        csv_entries = self._materialize_csv_tables(run_dir=run_dir, tables=normalized_payload.get("tables", []))

        manifest = {
            "report_key": request.report_key,
            "edit_mode": request.edit_mode,
            "revision_number": request.revision_number,
            "supersedes_report_id": request.supersedes_report_id,
            "generation_mode": mode,
            "artifacts": [
                {"role": "markdown", "path": str(markdown_path)},
                {"role": "json", "path": str(payload_path)},
                *[
                    {"role": "csv_table", "path": str(item["path"]), "table_name": item["name"]}
                    for item in csv_entries
                ],
            ],
        }
        manifest_path = run_dir / "research_manifest.json"
        write_json(manifest_path, manifest)

        artifacts = [
            build_file_artifact(kind="research-report", path=markdown_path, media_type="text/markdown"),
            build_file_artifact(kind="research-output", path=payload_path, media_type="application/json"),
            build_file_artifact(kind="research-manifest", path=manifest_path, media_type="application/json"),
        ]
        for entry in csv_entries:
            artifacts.append(
                build_file_artifact(
                    kind="research-table",
                    path=entry["path"],
                    media_type="text/csv",
                    metadata={"table_name": entry["name"]},
                )
            )

        output = ResearchReportWorkerOutput(
            report_key=request.report_key,
            title=str(normalized_payload.get("title") or f"Research Report: {request.theme}"),
            summary=str(normalized_payload.get("summary") or ""),
            findings=list(normalized_payload.get("findings", [])),
            section_updates=list(normalized_payload.get("section_updates", [])),
            tables=list(normalized_payload.get("tables", [])),
            citations=list(normalized_payload.get("citations", [])),
        )

        report = ReportRecord(
            report_type="research_report",
            title=output.title,
            summary=output.summary,
            content_markdown=markdown,
            artifact_path=str(markdown_path),
            report_series_id=request.report_key,
            revision_number=request.revision_number,
            supersedes_report_id=request.supersedes_report_id,
            is_current=False,
            metadata={
                "report_key": request.report_key,
                "edit_mode": request.edit_mode,
                "generation_mode": mode,
                "table_count": len(csv_entries),
                "taxonomy": {"filters": ["report"], "tags": ["deep_research"]},
            },
        )

        stdout_lines = [f"Prepared research revision {request.revision_number} for report_key={request.report_key}."]
        if mode != "codex":
            stdout_lines.append("Codex structured output unavailable; used deterministic placeholder payload.")
        if codex_result.stdout:
            stdout_lines.append(codex_result.stdout.strip())

        return AdapterResult(
            status=WorkerStatus.COMPLETED,
            stdout="\n".join(line for line in stdout_lines if line),
            stderr=codex_result.stderr,
            artifact_manifest=artifacts,
            structured_outputs={
                **output.model_dump(mode="json"),
                "mode": mode,
                "manifest_path": str(manifest_path),
            },
            observations=[
                ObservationRecord(
                    kind="research_revision",
                    summary=f"Generated {mode} research revision {request.revision_number}.",
                    payload={
                        "report_key": request.report_key,
                        "mode": mode,
                        "table_count": len(csv_entries),
                        "finding_count": len(output.findings),
                    },
                    confidence=0.6 if mode == "placeholder" else 0.8,
                )
            ],
            reports=[report],
        )

    def _render_prompt(self, request: ResearchReportWorkerRequest) -> str:
        prompt = load_research_report_prompt().strip()
        input_payload = {
            "theme": request.theme,
            "report_key": request.report_key,
            "edit_mode": request.edit_mode,
            "boundaries": request.boundaries,
            "areas_of_interest": request.areas_of_interest,
            "revision_number": request.revision_number,
            "supersedes_report_id": request.supersedes_report_id,
            "previous_report": request.previous_report,
            "retrieval_context": request.retrieval_context[:10],
            "metadata": request.metadata,
        }
        return "\n\n".join(
            [
                prompt,
                "Return strict JSON only and conform to the supplied output schema.",
                "Input payload:",
                json.dumps(input_payload, indent=2, sort_keys=True),
            ]
        )

    def _resolve_payload(
        self,
        *,
        request: ResearchReportWorkerRequest,
        codex_result: AdapterResult,
        output_path: Path,
    ) -> tuple[dict[str, Any], str]:
        if codex_result.status == WorkerStatus.COMPLETED and output_path.exists():
            try:
                parsed = self._parse_payload(output_path.read_text(encoding="utf-8", errors="replace"))
                return self._normalize_payload(parsed, request), "codex"
            except Exception:
                pass
        return self._placeholder_payload(request), "placeholder"

    @staticmethod
    def _parse_payload(raw_text: str) -> dict[str, Any]:
        try:
            payload = json.loads(raw_text)
        except JSONDecodeError:
            payload = json.loads(ResearchReportRunner._extract_json(raw_text))
        if not isinstance(payload, dict):
            raise ValueError("research_payload_must_be_object")
        return payload

    @staticmethod
    def _extract_json(raw_text: str) -> str:
        stripped = raw_text.strip()
        if stripped.startswith("```"):
            stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
            stripped = re.sub(r"\s*```$", "", stripped)
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return stripped
        return stripped[start : end + 1]

    def _normalize_payload(
        self,
        payload: dict[str, Any],
        request: ResearchReportWorkerRequest,
    ) -> dict[str, Any]:
        base = self._placeholder_payload(request)
        merged = {
            "title": str(payload.get("title") or base["title"]),
            "summary": str(payload.get("summary") or base["summary"]),
            "findings": self._normalize_findings(payload.get("findings"), base["findings"]),
            "section_updates": self._normalize_section_updates(payload.get("section_updates"), base["section_updates"]),
            "tables": self._normalize_tables(payload.get("tables"), base["tables"]),
            "citations": self._normalize_citations(payload.get("citations"), base["citations"]),
        }
        return merged

    @staticmethod
    def _normalize_findings(raw: Any, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return fallback
        rows: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "headline": str(item.get("headline") or "Finding"),
                    "claim": str(item.get("claim") or ""),
                    "inference": str(item.get("inference") or ""),
                    "confidence": item.get("confidence"),
                }
            )
        return rows or fallback

    @staticmethod
    def _normalize_section_updates(raw: Any, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return fallback
        rows: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "section": str(item.get("section") or "Key Findings"),
                    "action": str(item.get("action") or "update"),
                    "content": str(item.get("content") or ""),
                }
            )
        return rows or fallback

    @staticmethod
    def _normalize_tables(raw: Any, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return fallback
        tables: list[dict[str, Any]] = []
        for index, table in enumerate(raw, start=1):
            if not isinstance(table, dict):
                continue
            name = str(table.get("name") or f"table_{index}")
            raw_rows = table.get("rows")
            raw_columns = table.get("columns")
            columns: list[str] = []
            if isinstance(raw_columns, list):
                columns = [str(item) for item in raw_columns if str(item).strip()]

            normalized_rows: list[dict[str, Any]] = []
            if isinstance(raw_rows, list):
                for row in raw_rows:
                    if isinstance(row, dict):
                        if not columns:
                            columns = [str(key) for key in row.keys()]
                        normalized_rows.append({column: row.get(column, "") for column in columns})
                    elif isinstance(row, list):
                        if not columns:
                            columns = [f"column_{i + 1}" for i in range(len(row))]
                        normalized_rows.append(
                            {
                                column: row[i] if i < len(row) else ""
                                for i, column in enumerate(columns)
                            }
                        )
            if not columns:
                columns = ["value"]
            tables.append({"name": name, "columns": columns, "rows": normalized_rows})
        return tables or fallback

    @staticmethod
    def _normalize_citations(raw: Any, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return fallback
        citations: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            citations.append(
                {
                    "label": str(item.get("label") or "Source"),
                    "url": str(item.get("url") or ""),
                    "note": str(item.get("note") or ""),
                }
            )
        return citations or fallback

    @staticmethod
    def _placeholder_payload(request: ResearchReportWorkerRequest) -> dict[str, Any]:
        areas = request.areas_of_interest or ["general"]
        boundaries = request.boundaries or ["no explicit boundaries provided"]
        area_text = ", ".join(areas[:3])
        boundary_text = ", ".join(boundaries[:3])
        previous_summary = ""
        if isinstance(request.previous_report, dict):
            previous_summary = str(request.previous_report.get("summary") or "").strip()
        findings = [
            {
                "headline": f"Scaffold finding for {request.theme}",
                "claim": f"Research should prioritize: {area_text}.",
                "inference": (
                    f"Boundaries currently constrain exploration to: {boundary_text}."
                    + (f" Prior revision summary: {previous_summary}" if previous_summary else "")
                ),
                "confidence": 0.35,
            }
        ]
        section_updates = [
            {
                "section": "Open Questions",
                "action": "append",
                "content": f"Investigate stronger evidence for {request.theme} across areas: {area_text}.",
            }
        ]
        table_rows = [
            {"metric": "areas_of_interest_count", "value": len(request.areas_of_interest)},
            {"metric": "boundaries_count", "value": len(request.boundaries)},
            {"metric": "retrieval_context_count", "value": len(request.retrieval_context)},
        ]
        citations = []
        for index, item in enumerate(request.retrieval_context[:3], start=1):
            citations.append(
                {
                    "label": str(item.get("id") or f"context-{index}"),
                    "url": str(item.get("url") or ""),
                    "note": str(item.get("text") or "")[:200],
                }
            )
        if not citations:
            citations.append({"label": "local-context", "url": "", "note": "No retrieval citations supplied."})

        return {
            "title": f"Research Report: {request.theme}",
            "summary": (
                f"Placeholder revision for report_key {request.report_key} generated deterministically "
                "because Codex structured output was unavailable."
            ),
            "findings": findings,
            "section_updates": section_updates,
            "tables": [
                {
                    "name": "research_scope",
                    "columns": ["metric", "value"],
                    "rows": table_rows,
                }
            ],
            "citations": citations,
        }

    def _materialize_csv_tables(self, *, run_dir: Path, tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for index, table in enumerate(tables, start=1):
            name = str(table.get("name") or f"table_{index}")
            columns = [str(item) for item in table.get("columns", [])]
            rows = list(table.get("rows", []))
            filename = f"table_{index:02d}_{self._slugify(name)}.csv"
            path = run_dir / filename
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns or ["value"])
                writer.writeheader()
                for row in rows:
                    if isinstance(row, dict):
                        writer.writerow({key: row.get(key, "") for key in writer.fieldnames})
            entries.append({"name": name, "path": path})
        return entries

    @staticmethod
    def _slugify(value: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
        return slug or "table"

    @staticmethod
    def _render_markdown(payload: dict[str, Any], request: ResearchReportWorkerRequest) -> str:
        title = str(payload.get("title") or f"Research Report: {request.theme}")
        update_lines = [
            "## Summary",
            str(payload.get("summary") or ""),
            "",
            "## Findings",
        ]
        for finding in payload.get("findings", []):
            headline = str(finding.get("headline") or "Finding")
            claim = str(finding.get("claim") or "")
            inference = str(finding.get("inference") or "")
            update_lines.extend(
                [
                    f"### {headline}",
                    f"- Claim: {claim}",
                    f"- Inference: {inference}",
                ]
            )

        update_lines.extend(["", "## Section Updates"])
        for section in payload.get("section_updates", []):
            update_lines.append(
                f"- [{section.get('action', 'update')}] {section.get('section', 'Section')}: {section.get('content', '')}"
            )

        update_lines.extend(["", "## Tables"])
        for table in payload.get("tables", []):
            update_lines.append(f"- {table.get('name', 'table')} ({len(table.get('rows', []))} rows)")

        update_lines.extend(["", "## Citations"])
        for citation in payload.get("citations", []):
            label = str(citation.get("label") or "source")
            url = str(citation.get("url") or "")
            note = str(citation.get("note") or "")
            if url:
                update_lines.append(f"- {label}: {url} - {note}")
            else:
                update_lines.append(f"- {label}: {note}")

        lines = [
            f"# {title}",
            "",
            f"- Report key: `{request.report_key}`",
            f"- Edit mode: `{request.edit_mode}`",
            f"- Revision: `{request.revision_number}`",
        ]
        previous_report = request.previous_report if isinstance(request.previous_report, dict) else None
        previous_title = str(previous_report.get("title") or "Prior Revision") if previous_report else ""
        previous_summary = str(previous_report.get("summary") or "").strip() if previous_report else ""
        previous_markdown = str(previous_report.get("content_markdown") or "").strip() if previous_report else ""

        if previous_report:
            lines.extend(["", "## Revision Strategy"])
            if request.edit_mode == "append":
                lines.append(f"This revision appends new material to `{previous_title}`.")
                lines.extend(
                    [
                        "",
                        "## Prior Revision",
                        previous_markdown or previous_summary or "Prior revision content unavailable.",
                        "",
                        f"## Revision {request.revision_number} Update",
                    ]
                )
            elif request.edit_mode == "refresh_section":
                lines.append(f"This revision refreshes targeted sections from `{previous_title}`.")
                if previous_summary:
                    lines.extend(["", "## Prior Revision Summary", previous_summary])
            else:
                lines.append(f"This revision merges new findings into the lineage from `{previous_title}`.")
                if previous_summary:
                    lines.extend(["", "## Prior Revision Summary", previous_summary])

        lines.extend(["", *update_lines])
        return "\n".join(lines)
