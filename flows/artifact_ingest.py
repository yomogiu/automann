from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from prefect import flow

from libs.config import get_settings
from libs.contracts.models import ArtifactIngestRequest
from libs.retrieval import RetrievalService

from .common import build_repository, execute_adapter


WORKER_KEY = "artifact_ingest_runner"


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_success_status(status: str, item: dict[str, Any]) -> bool:
    if status in {"completed", "success", "succeeded", "ok", "ingested"}:
        return True
    if item.get("normalized_text_artifact_id"):
        return True
    if item.get("chunk_count"):
        return True
    if item.get("raw_artifact_id"):
        return True
    return False


def _warning_codes(item: dict[str, Any]) -> list[str]:
    raw = item.get("warning_codes")
    if not isinstance(raw, list):
        raw = item.get("warnings")
    if not isinstance(raw, list):
        return []
    codes: list[str] = []
    for code in raw:
        text = str(code).strip()
        if text:
            codes.append(text)
    return list(dict.fromkeys(codes))


def _item_results(structured_outputs: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("items", "results", "item_results"):
        raw = structured_outputs.get(key)
        if isinstance(raw, list):
            return [_as_dict(item) for item in raw]
    return []


def _item_identity(item: dict[str, Any]) -> tuple[int | None, str]:
    input_index = item.get("input_index")
    if isinstance(input_index, int):
        index = input_index
    else:
        try:
            index = int(str(input_index))
        except Exception:
            index = None
    canonical_uri = str(
        item.get("canonical_uri")
        or _as_dict(item.get("metadata")).get("canonical_uri")
        or item.get("source_uri")
        or item.get("url")
        or item.get("file_path")
        or ""
    ).strip()
    return index, canonical_uri


def _artifact_role(artifact: dict[str, Any]) -> str:
    metadata = _as_dict(artifact.get("metadata"))
    role = str(metadata.get("role") or "").strip().lower()
    if role:
        return role
    kind = str(artifact.get("kind") or "").strip().lower()
    return kind


def _find_artifact(
    artifacts: list[dict[str, Any]],
    *,
    item_index: int | None,
    canonical_uri: str,
    roles: Iterable[str],
) -> dict[str, Any] | None:
    role_set = {str(role).strip().lower() for role in roles if str(role).strip()}
    for artifact in artifacts:
        role = _artifact_role(artifact)
        metadata = _as_dict(artifact.get("metadata"))
        metadata_index = metadata.get("input_index")
        if isinstance(metadata_index, int):
            artifact_index = metadata_index
        else:
            try:
                artifact_index = int(str(metadata_index))
            except Exception:
                artifact_index = None
        artifact_uri = str(
            metadata.get("canonical_uri")
            or artifact.get("path")
            or artifact.get("storage_uri")
            or ""
        ).strip()
        if role in role_set and (artifact_index == item_index or (canonical_uri and canonical_uri == artifact_uri)):
            return artifact
    return None


def _merge_metadata(*items: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for item in items:
        if isinstance(item, dict):
            merged.update(item)
    return merged


def _normalize_chunks(raw_chunks: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_chunks, list):
        return []
    chunks: list[dict[str, Any]] = []
    for ordinal, chunk in enumerate(raw_chunks):
        if isinstance(chunk, dict):
            payload = dict(chunk)
            payload.setdefault("ordinal", payload.get("index", ordinal))
            payload.setdefault("text", str(payload.get("text") or "").strip())
            payload.setdefault("token_count", int(payload.get("token_count") or 0))
            payload.setdefault("metadata", _as_dict(payload.get("metadata")))
            chunks.append(payload)
        else:
            text = str(chunk).strip()
            if not text:
                continue
            chunks.append(
                {
                    "ordinal": ordinal,
                    "text": text,
                    "token_count": max(1, len(text.split())),
                    "metadata": {},
                }
            )
    return chunks


def _call_repo_helper(repository: Any, names: Iterable[str], *args: Any, **kwargs: Any) -> Any:
    for name in names:
        helper = getattr(repository, name, None)
        if callable(helper):
            return helper(*args, **kwargs)
    return None


def _upsert_source_document(
    repository: Any,
    *,
    canonical_uri: str,
    source_type: str | None,
    title: str | None,
    author: str | None,
    published_at: Any,
    metadata: dict[str, Any],
) -> Any:
    return _call_repo_helper(
        repository,
        ("upsert_source_document", "get_or_create_source_document", "upsert_source"),
        canonical_uri=canonical_uri,
        source_type=source_type,
        title=title,
        author=author,
        published_at=published_at,
        metadata_json=metadata,
    )


def _assign_artifact_to_source(repository: Any, *, artifact_id: str, source_document_id: str) -> Any:
    return _call_repo_helper(
        repository,
        (
            "assign_artifact_to_source",
            "link_artifact_to_source_document",
            "set_artifact_source_document",
            "attach_artifact_to_source_document",
        ),
        artifact_id=artifact_id,
        source_document_id=source_document_id,
    )


def _set_current_text_artifact(repository: Any, *, source_document_id: str, artifact_id: str) -> Any:
    return _call_repo_helper(
        repository,
        (
            "set_source_document_current_text_artifact",
            "update_source_document_current_text_artifact",
            "promote_source_document_text_artifact",
        ),
        source_document_id=source_document_id,
        current_text_artifact_id=artifact_id,
    )


def _build_worker_payload(request: ArtifactIngestRequest) -> Any:
    try:
        from libs.contracts import workers as worker_contracts
    except Exception:
        return {
            "items": [item.model_dump(mode="json") for item in request.items],
            "metadata": dict(request.metadata),
        }

    model = getattr(worker_contracts, "ArtifactIngestRequest", None)
    payload = {
        "items": [item.model_dump(mode="json") for item in request.items],
        "metadata": dict(request.metadata),
    }
    if model is not None and hasattr(model, "model_validate"):
        try:
            return model.model_validate(payload)
        except Exception:
            return payload
    return payload


def _run_artifact_ingest_worker(settings: Any, request: ArtifactIngestRequest) -> Any:
    try:
        from workers.artifact_ingest_runner.runner import ArtifactIngestRunner
    except Exception as exc:  # pragma: no cover - imported at runtime by the worker lane
        raise RuntimeError("artifact ingest worker is unavailable") from exc

    runner = ArtifactIngestRunner(settings)
    payload = _build_worker_payload(request)
    if hasattr(runner, "run"):
        try:
            return runner.run(payload)
        except TypeError:
            return runner.run(request.model_dump(mode="json"))
    raise RuntimeError("artifact ingest worker does not expose a run method")


def _merge_item_outputs(
    *,
    repository: Any,
    artifacts: list[dict[str, Any]],
    item: dict[str, Any],
) -> dict[str, Any]:
    output = dict(item)
    item_index, canonical_uri = _item_identity(output)
    output.setdefault("input_index", item_index)
    if canonical_uri:
        output["canonical_uri"] = canonical_uri

    raw_artifact = _find_artifact(
        artifacts,
        item_index=item_index,
        canonical_uri=canonical_uri,
        roles=("raw_source", "raw-source", "source_raw", "source"),
    )
    normalized_artifact = _find_artifact(
        artifacts,
        item_index=item_index,
        canonical_uri=canonical_uri,
        roles=("normalized_text", "normalized-text", "text", "normalized"),
    )
    manifest_artifact = _find_artifact(
        artifacts,
        item_index=item_index,
        canonical_uri=canonical_uri,
        roles=("ingest_manifest", "manifest", "ingest-manifest"),
    )

    if raw_artifact is not None:
        output["raw_artifact_id"] = raw_artifact.get("id")
    if normalized_artifact is not None:
        output["normalized_text_artifact_id"] = normalized_artifact.get("id")
    if manifest_artifact is not None and output.get("ingest_manifest_artifact_id") is None:
        output["ingest_manifest_artifact_id"] = manifest_artifact.get("id")

    warning_codes = _warning_codes(output)
    output["warning_codes"] = warning_codes
    output["error"] = output.get("error")

    source_document_id = output.get("source_document_id")
    source_document = _as_dict(output.get("source_document"))
    source_metadata = _merge_metadata(
        _as_dict(output.get("metadata")),
        _as_dict(source_document.get("metadata")),
        {"tags": output.get("tags") or source_document.get("tags") or []},
        {"entities": output.get("entities") or source_document.get("entities") or []},
    )
    if canonical_uri and not source_document_id:
        source_record = _upsert_source_document(
            repository,
            canonical_uri=canonical_uri,
            source_type=str(output.get("source_type") or source_document.get("source_type") or output.get("input_kind") or "").strip() or None,
            title=str(output.get("title") or source_document.get("title") or "").strip() or None,
            author=str(output.get("author") or source_document.get("author") or "").strip() or None,
            published_at=output.get("published_at") or source_document.get("published_at"),
            metadata=source_metadata,
        )
        if source_record is not None:
            source_document_id = getattr(source_record, "id", None) or _as_dict(source_record).get("id") or source_document_id
            output["source_document_id"] = source_document_id

    if source_document_id:
        if raw_artifact is not None and raw_artifact.get("id"):
            _assign_artifact_to_source(repository, artifact_id=str(raw_artifact["id"]), source_document_id=str(source_document_id))
        if normalized_artifact is not None and normalized_artifact.get("id"):
            _assign_artifact_to_source(repository, artifact_id=str(normalized_artifact["id"]), source_document_id=str(source_document_id))
            _set_current_text_artifact(
                repository,
                source_document_id=str(source_document_id),
                artifact_id=str(normalized_artifact["id"]),
            )

    chunk_payloads = _normalize_chunks(
        output.get("chunk_payloads")
        or output.get("chunks")
        or output.get("normalized_chunks")
        or output.get("chunk_data")
    )
    if normalized_artifact is not None and normalized_artifact.get("id") and chunk_payloads:
        try:
            repository.upsert_chunks(artifact_id=str(normalized_artifact["id"]), chunks=chunk_payloads)
        except Exception:
            pass
    output["chunk_count"] = int(output.get("chunk_count") or len(chunk_payloads))
    return output


@flow(name="artifact-ingest")
def artifact_ingest_flow(request: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    repository = build_repository(settings)
    ingest_request = ArtifactIngestRequest.model_validate(request)

    if run_id is not None:
        repository.update_run_status(run_id, status="running")

    try:
        result = execute_adapter(
            flow_name="artifact_ingest_flow",
            worker_key=WORKER_KEY,
            input_payload=ingest_request.model_dump(mode="json"),
            runner=lambda: _run_artifact_ingest_worker(settings, ingest_request),
            parent_run_id=run_id,
        )

        structured_outputs = _as_dict(result.get("structured_outputs"))
        artifacts = [artifact for artifact in result.get("artifacts", []) if isinstance(artifact, dict)]
        item_outputs = _item_results(structured_outputs)
        if not item_outputs and isinstance(structured_outputs.get("items"), list):
            item_outputs = [_as_dict(item) for item in structured_outputs["items"]]

        merged_items = [
            _merge_item_outputs(repository=repository, artifacts=artifacts, item=item)
            for item in item_outputs
        ]

        if not merged_items:
            merged_items = []

        success_count = 0
        failure_count = 0
        warning_count = 0
        for item in merged_items:
            status = _normalize_status(item.get("status"))
            if _is_success_status(status, item):
                success_count += 1
            else:
                failure_count += 1
            warning_count += len(_warning_codes(item))

        if merged_items:
            final_status = "completed" if success_count > 0 else "failed"
        else:
            final_status = str(result.get("status") or "failed")

        flow_result = {
            **structured_outputs,
            "success_count": success_count,
            "failure_count": failure_count if merged_items else int(str(result.get("status") or "").lower() == "failed"),
            "warning_count": warning_count,
            "items": merged_items,
        }
        if success_count > 0:
            try:
                sync_warnings = RetrievalService(repository).sync_semantic_index()
            except Exception:
                sync_warnings = ["semantic_index_sync_failed"]
            if sync_warnings:
                flow_result["semantic_index_warnings"] = list(dict.fromkeys(sync_warnings))

        if run_id is not None:
            repository.update_run_status(
                run_id,
                status=final_status,
                stdout=result.get("stdout") or "",
                stderr=result.get("stderr") or "",
                structured_outputs=flow_result,
                artifact_manifest=artifacts,
                observation_summary=list(result.get("observations", [])),
                next_suggested_events=list(result.get("next_events", [])),
            )

        return {
            "status": final_status,
            **flow_result,
            "artifacts": artifacts,
            "observations": list(result.get("observations", [])),
            "reports": list(result.get("reports", [])),
            "next_events": list(result.get("next_events", [])),
        }
    except Exception as exc:
        if run_id is not None:
            repository.update_run_status(run_id, status="failed", stderr=str(exc))
        raise
