from __future__ import annotations

from typing import Any


def _estimate_tokens(text: str) -> int:
    return max(1, len(text.split()))


def chunk_text(
    text: str,
    *,
    target_chars: int = 1200,
    overlap_chars: int = 160,
) -> list[dict[str, Any]]:
    normalized = "\n".join(line.rstrip() for line in text.splitlines()).strip()
    if not normalized:
        return []

    paragraphs = [part.strip() for part in normalized.split("\n\n") if part.strip()]
    chunks: list[dict[str, Any]] = []
    buffer = ""

    def flush(current: str, ordinal: int) -> None:
        snippet = current.strip()
        if not snippet:
            return
        chunks.append(
            {
                "ordinal": ordinal,
                "text": snippet,
                "token_count": _estimate_tokens(snippet),
                "metadata": {"char_length": len(snippet)},
            }
        )

    ordinal = 0
    for paragraph in paragraphs:
        candidate = paragraph if not buffer else f"{buffer}\n\n{paragraph}"
        if len(candidate) <= target_chars:
            buffer = candidate
            continue
        if buffer:
            flush(buffer, ordinal)
            ordinal += 1
        if len(paragraph) <= target_chars:
            buffer = paragraph
            continue
        start = 0
        while start < len(paragraph):
            end = min(len(paragraph), start + target_chars)
            snippet = paragraph[start:end]
            flush(snippet, ordinal)
            ordinal += 1
            if end == len(paragraph):
                break
            start = max(end - overlap_chars, start + 1)
        buffer = ""

    flush(buffer, ordinal)
    return chunks
