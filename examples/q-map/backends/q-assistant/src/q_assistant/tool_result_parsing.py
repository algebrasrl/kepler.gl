from __future__ import annotations

import json
import re
from typing import Any


def _read_tool_message_content(content: Any) -> tuple[Any, str]:
    parsed: Any = None
    details = ""

    if isinstance(content, str):
        text = content.strip()
        if text:
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
                details = text
        return parsed, details

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text_piece = item.get("text")
                if isinstance(text_piece, str) and text_piece.strip():
                    parts.append(text_piece.strip())
        joined = "\n".join(parts).strip()
        if joined:
            try:
                parsed = json.loads(joined)
            except Exception:
                parsed = None
                details = joined
        return parsed, details

    return None, ""


def _extract_success_from_text(text: str) -> bool | None:
    body = str(text or "").strip()
    if not body:
        return None
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict) and isinstance(parsed.get("success"), bool):
            return bool(parsed.get("success"))
    except Exception:
        pass

    match = re.search(r'"success"\s*:\s*(true|false)', body, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).lower() == "true"


def _normalize_dataset_ref(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.lower().startswith("id:"):
        token = text[3:].strip()
    else:
        token = text
    if not token:
        return ""
    return f"id:{token}"


def _extract_dataset_ref_from_call(call: dict[str, Any] | None) -> str:
    if not isinstance(call, dict):
        return ""
    args = call.get("args")
    if not isinstance(args, dict):
        return ""
    for key in ("datasetRef", "sourceDatasetRef", "targetDatasetRef", "clipDatasetRef", "boundaryDatasetRef"):
        candidate = _normalize_dataset_ref(args.get(key))
        if candidate:
            return candidate
    return ""


def _build_dataset_hint(dataset_ref: str, dataset_name: str) -> str:
    if dataset_ref:
        return f' (datasetName="{dataset_ref}")'
    if dataset_name:
        return f' (datasetName="{dataset_name}")'
    return ""


def _build_source_dataset_hint(dataset_ref: str, dataset_name: str) -> str:
    if dataset_ref:
        return f' (sourceDatasetName="{dataset_ref}")'
    if dataset_name:
        return f' (sourceDatasetName="{dataset_name}")'
    return ""
