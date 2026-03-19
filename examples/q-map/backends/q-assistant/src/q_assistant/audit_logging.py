from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .config import Settings

_AUDIT_REDACTED = "[REDACTED]"
_AUDIT_SENSITIVE_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "apiKey",
    "token",
    "password",
    "secret",
    "x-api-key",
    "x_goog_api_key",
}
_AUDIT_PRUNE_MIN_INTERVAL_SECONDS = 60.0
_last_audit_prune_at = 0.0
_REQUEST_ID_HEADER = "x-q-assistant-request-id"
_CHAT_ID_HEADER = "x-q-assistant-chat-id"
_CHAT_AUDIT_SCHEMA = "qmap.chat_audit.v1"
_CHAT_AUDIT_EVENT_TYPE = "chat.audit"
_CHAT_AUDIT_SERVICE = "q-assistant"
_logger = logging.getLogger("q_assistant.audit")


def _parse_tool_arguments(raw_arguments: Any, *, none_on_failure: bool = False) -> dict[str, Any] | None:
    if isinstance(raw_arguments, dict):
        return raw_arguments
    if not isinstance(raw_arguments, str):
        return None if none_on_failure else {}
    text = raw_arguments.strip()
    if not text:
        return None if none_on_failure else {}
    try:
        parsed = json.loads(text)
    except Exception:
        return None if none_on_failure else {}
    if isinstance(parsed, dict):
        return parsed
    return None if none_on_failure else {}


def _derive_runtime_quality_metrics(
    request_tool_results: Any,
    response_tool_calls: Any,
    response_text: Any,
    request_payload: Any = None,
) -> dict[str, Any]:
    return {}


def _sanitize_qmap_context_payload(payload: Any) -> Any:
    return payload


def _configure_audit_runtime(
    *,
    parse_tool_arguments: Callable[..., dict[str, Any] | None] | None = None,
    derive_runtime_quality_metrics: Callable[[Any, Any, Any, Any], dict[str, Any]] | None = None,
    sanitize_qmap_context_payload: Callable[[Any], Any] | None = None,
    logger: logging.Logger | None = None,
) -> None:
    global _parse_tool_arguments
    global _derive_runtime_quality_metrics
    global _sanitize_qmap_context_payload
    global _logger

    if callable(parse_tool_arguments):
        _parse_tool_arguments = parse_tool_arguments
    if callable(derive_runtime_quality_metrics):
        _derive_runtime_quality_metrics = derive_runtime_quality_metrics
    if callable(sanitize_qmap_context_payload):
        _sanitize_qmap_context_payload = sanitize_qmap_context_payload
    if isinstance(logger, logging.Logger):
        _logger = logger


def _audit_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_for_audit(
    value: Any,
    *,
    max_depth: int = 8,
    max_list_items: int = 0,
    max_string_chars: int = 0,
) -> Any:
    if max_depth <= 0:
        return "[max-depth]"
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, raw_value in value.items():
            key_text = str(key or "")
            if key_text in _AUDIT_SENSITIVE_KEYS or key_text.lower() in _AUDIT_SENSITIVE_KEYS:
                out[key_text] = _AUDIT_REDACTED
                continue
            out[key_text] = _sanitize_for_audit(
                raw_value,
                max_depth=max_depth - 1,
                max_list_items=max_list_items,
                max_string_chars=max_string_chars,
            )
        return out
    if isinstance(value, list):
        sliced = value if max_list_items <= 0 else value[: max(0, int(max_list_items))]
        return [
            _sanitize_for_audit(
                item,
                max_depth=max_depth - 1,
                max_list_items=max_list_items,
                max_string_chars=max_string_chars,
            )
            for item in sliced
        ]
    if isinstance(value, str):
        text = value.strip()
        if max_string_chars <= 0:
            return text
        return (
            text
            if len(text) <= max_string_chars
            else f"{text[:max_string_chars]}...[truncated]"
        )
    return value


def _audit_json_preview(value: Any, max_chars: int = 1200) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False)
    except Exception:
        text = str(value)
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}...[truncated]"


def _compact_audit_event(event: dict[str, Any], *, max_chars: int) -> dict[str, Any]:
    if max_chars <= 0:
        return event
    try:
        line = json.dumps(event, ensure_ascii=False)
    except Exception:
        return {"ts": _audit_now_iso(), "event": "audit-serialization-error"}
    if len(line) <= max_chars:
        return event

    compact = dict(event)
    for key in ("requestPayload", "responsePayload", "qmapContext"):
        if key in compact:
            compact[f"{key}Preview"] = _audit_json_preview(compact.get(key))
            compact.pop(key, None)
    compact["truncated"] = True
    compact["truncatedFromChars"] = len(line)

    try:
        compact_line = json.dumps(compact, ensure_ascii=False)
    except Exception:
        compact_line = ""
    if compact_line and len(compact_line) <= max_chars:
        return compact

    return {
        "auditSchema": event.get("auditSchema") or _CHAT_AUDIT_SCHEMA,
        "eventType": event.get("eventType") or _CHAT_AUDIT_EVENT_TYPE,
        "service": event.get("service") or _CHAT_AUDIT_SERVICE,
        "ts": event.get("ts") or _audit_now_iso(),
        "sessionId": event.get("sessionId"),
        "chatId": event.get("chatId") or event.get("sessionId"),
        "requestId": event.get("requestId"),
        "endpoint": event.get("endpoint"),
        "status": event.get("status"),
        "durationMs": event.get("durationMs"),
        "outcome": event.get("outcome") or ("success" if int(event.get("status") or 0) < 400 else "error"),
        "usedProvider": event.get("usedProvider"),
        "usedModel": event.get("usedModel"),
        "error": event.get("error"),
        "truncated": True,
        "truncatedFromChars": len(line),
    }


def _extract_session_id(payload: Any, qmap_context_header: Any = None) -> str:
    keys = (
        "session_id",
        "sessionId",
        "conversation_id",
        "conversationId",
        "thread_id",
        "threadId",
        "chat_id",
        "chatId",
    )

    def _pick_from_dict(obj: Any) -> str:
        if not isinstance(obj, dict):
            return ""
        for key in keys:
            value = obj.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        metadata = obj.get("metadata")
        if isinstance(metadata, dict):
            for key in keys:
                value = metadata.get(key)
                if value is not None and str(value).strip():
                    return str(value).strip()
        return ""

    from_payload = _pick_from_dict(payload)
    if from_payload:
        return from_payload

    if isinstance(qmap_context_header, dict):
        from_ctx = _pick_from_dict(qmap_context_header)
        if from_ctx:
            return from_ctx
    raw_ctx = str(qmap_context_header or "").strip()
    if raw_ctx:
        try:
            parsed_ctx = json.loads(raw_ctx)
            from_ctx = _pick_from_dict(parsed_ctx)
            if from_ctx:
                return from_ctx
        except Exception:
            pass

    return "default"


def _normalize_session_id(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text[:128]


def _resolve_audit_session_id(
    explicit_session_id: str | None,
    payload: Any = None,
    qmap_context_header: Any = None,
) -> str:
    normalized = _normalize_session_id(explicit_session_id)
    if normalized:
        return normalized
    extracted = _extract_session_id(payload, qmap_context_header)
    normalized_extracted = _normalize_session_id(extracted)
    return normalized_extracted or "default"


def _set_trace_headers(headers: Any, request_id: str, chat_id: str | None) -> None:
    if headers is None:
        return
    headers[_REQUEST_ID_HEADER] = request_id
    if chat_id:
        headers[_CHAT_ID_HEADER] = str(chat_id)


def _audit_safe_token(value: Any, *, fallback: str = "default", max_len: int = 80) -> str:
    text = str(value or "").strip() or fallback
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    if not safe:
        safe = fallback
    return safe[:max_len]


def _resolve_chat_audit_dir(path_raw: str) -> Path:
    path = Path(str(path_raw or "").strip() or "/tmp/q-assistant-chat-audit")
    if path.suffix.lower() == ".jsonl":
        return path.parent / path.stem
    return path


def _prune_chat_audit_dir(audit_dir: Path, *, max_files: int, max_age_days: int) -> None:
    if max_files <= 0 and max_age_days <= 0:
        return
    try:
        now_ts = time.time()
        max_age_seconds = float(max_age_days) * 86400.0 if max_age_days > 0 else 0.0
        files: list[tuple[float, Path]] = []
        for path in audit_dir.glob("session-*.jsonl"):
            if not path.is_file():
                continue
            try:
                stat = path.stat()
                mtime = float(stat.st_mtime)
            except Exception:
                continue
            if max_age_seconds > 0 and (now_ts - mtime) > max_age_seconds:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue
            files.append((mtime, path))
        if max_files > 0 and len(files) > max_files:
            files.sort(key=lambda item: item[0], reverse=True)
            for _, stale_path in files[max_files:]:
                try:
                    stale_path.unlink(missing_ok=True)
                except Exception:
                    pass
    except Exception:
        return


def _maybe_prune_chat_audit(settings: Settings, audit_dir: Path) -> None:
    global _last_audit_prune_at
    if int(settings.chat_audit_max_files) <= 0 and int(settings.chat_audit_max_age_days) <= 0:
        return
    now_ts = time.time()
    if (now_ts - _last_audit_prune_at) < _AUDIT_PRUNE_MIN_INTERVAL_SECONDS:
        return
    _last_audit_prune_at = now_ts
    _prune_chat_audit_dir(
        audit_dir,
        max_files=max(0, int(settings.chat_audit_max_files)),
        max_age_days=max(0, int(settings.chat_audit_max_age_days)),
    )


def _audit_duration_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 1)


def _audit_payload_for_log(settings: Settings, payload: Any) -> Any | None:
    return payload if settings.chat_audit_include_payloads else None


def _audit_context_for_log(settings: Settings, qmap_context: str | None) -> Any | None:
    if not settings.chat_audit_include_context:
        return None
    raw = str(qmap_context or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        max_string_chars = max(0, int(settings.chat_audit_max_string_chars))
        preview = raw
        if max_string_chars > 0 and len(preview) > max_string_chars:
            preview = f"{preview[:max_string_chars]}...[truncated]"
        return {"invalidJson": True, "preview": preview}
    return _sanitize_for_audit(
        _sanitize_qmap_context_payload(parsed),
        max_list_items=max(0, int(settings.chat_audit_max_list_items)),
        max_string_chars=max(0, int(settings.chat_audit_max_string_chars)),
    )


def _normalize_response_tool_calls(response_tool_calls: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not isinstance(response_tool_calls, list):
        return out
    for item in response_tool_calls:
        if isinstance(item, str):
            name = item.strip()
            if not name:
                continue
            out.append(
                {
                    "id": None,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": "",
                        "parsedArguments": None,
                    },
                }
            )
            continue
        if not isinstance(item, dict):
            continue
        fn = item.get("function")
        fn_dict = fn if isinstance(fn, dict) else {}
        fn_name = str(fn_dict.get("name") or item.get("name") or "").strip()
        if not fn_name:
            continue
        raw_args = fn_dict.get("arguments")
        if isinstance(raw_args, str):
            args_text = raw_args
        elif raw_args is None:
            args_text = ""
        else:
            try:
                args_text = json.dumps(raw_args, ensure_ascii=False)
            except Exception:
                args_text = str(raw_args)
        parsed_args = fn_dict.get("parsedArguments")
        if parsed_args is None:
            parsed_args = _parse_tool_arguments(args_text, none_on_failure=True)
        out.append(
            {
                "id": item.get("id"),
                "type": item.get("type") or "function",
                "function": {
                    "name": fn_name,
                    "arguments": args_text,
                    "parsedArguments": parsed_args,
                },
            }
        )
    return out


def _collect_response_tool_call_names(response_tool_calls: Any) -> list[str]:
    names: list[str] = []
    for call in _normalize_response_tool_calls(response_tool_calls):
        fn = call.get("function")
        if not isinstance(fn, dict):
            continue
        name = str(fn.get("name") or "").strip()
        if name:
            names.append(name)
    return names


def _summarize_request_tool_results(request_tool_results: Any) -> dict[str, int]:
    rows = request_tool_results if isinstance(request_tool_results, list) else []
    items = [row for row in rows if isinstance(row, dict)]
    success_count = len([row for row in items if row.get("success") is True])
    failed_count = len([row for row in items if row.get("success") is False])
    unknown_count = len(items) - success_count - failed_count
    schema_mismatch_count = len([row for row in items if bool(row.get("contractSchemaMismatch"))])
    response_mismatch_count = len([row for row in items if bool(row.get("contractResponseMismatch"))])
    return {
        "total": len(items),
        "success": success_count,
        "failed": failed_count,
        "unknown": max(0, unknown_count),
        "contractSchemaMismatch": schema_mismatch_count,
        "contractResponseMismatch": response_mismatch_count,
    }


def _build_chat_audit_event(
    *,
    endpoint: str,
    status: int,
    started_at: float,
    session_id: str | None,
    request_id: str,
    **extra: Any,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "auditSchema": _CHAT_AUDIT_SCHEMA,
        "eventType": _CHAT_AUDIT_EVENT_TYPE,
        "service": _CHAT_AUDIT_SERVICE,
        "ts": _audit_now_iso(),
        "sessionId": session_id,
        "chatId": session_id,
        "requestId": request_id,
        "endpoint": endpoint,
        "status": int(status),
        "durationMs": _audit_duration_ms(started_at),
    }
    event.update(extra)
    if int(event.get("status") or 0) >= 400:
        event["outcome"] = "error"
    else:
        # Detect empty completions: status 200 but no tool calls and no response text.
        has_tool_calls = bool(event.get("responseToolCalls"))
        has_text = bool((event.get("responseText") or "").strip())
        if not has_tool_calls and not has_text:
            event["outcome"] = "empty_completion"
        else:
            event["outcome"] = "success"
    event["responseToolCallsNormalized"] = _normalize_response_tool_calls(event.get("responseToolCalls"))
    event["responseToolCallNames"] = _collect_response_tool_call_names(event.get("responseToolCalls"))
    event["requestToolResultsSummary"] = _summarize_request_tool_results(event.get("requestToolResults"))
    metrics = _derive_runtime_quality_metrics(
        event.get("requestToolResults"),
        event.get("responseToolCalls"),
        event.get("responseText"),
        event.get("requestPayload"),
    )
    if metrics:
        event["qualityMetrics"] = metrics
    return event


def _write_chat_audit_event(
    settings: Settings,
    *,
    endpoint: str,
    status: int,
    started_at: float,
    session_id: str | None,
    request_id: str,
    **extra: Any,
) -> None:
    if not settings.chat_audit_enabled:
        return

    event = _build_chat_audit_event(
        endpoint=endpoint,
        status=status,
        started_at=started_at,
        session_id=session_id,
        request_id=request_id,
        **extra,
    )
    max_chars = max(0, int(settings.chat_audit_max_chars))
    safe_event = _compact_audit_event(
        _sanitize_for_audit(
            event,
            max_list_items=max(0, int(settings.chat_audit_max_list_items)),
            max_string_chars=max(0, int(settings.chat_audit_max_string_chars)),
        ),
        max_chars=max_chars,
    )
    try:
        if not safe_event.get("sessionId"):
            safe_event["sessionId"] = _extract_session_id(
                safe_event.get("requestPayload"),
                safe_event.get("qmapContext"),
            )
        if not safe_event.get("chatId"):
            safe_event["chatId"] = safe_event.get("sessionId")
        chat_id = _audit_safe_token(
            safe_event.get("chatId") or safe_event.get("sessionId"),
            fallback="default",
        )
        audit_dir = _resolve_chat_audit_dir(settings.chat_audit_log_path)
        audit_dir.mkdir(parents=True, exist_ok=True)
        path = audit_dir / f"session-{chat_id}.jsonl"
        serialized = json.dumps(safe_event, ensure_ascii=False) + "\n"
        try:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(serialized)
        except Exception:
            # Common misconfiguration: mounted audit dir exists but is not writable by app user.
            fallback_dir = _resolve_chat_audit_dir("/tmp/q-assistant-chat-audit")
            fallback_dir.mkdir(parents=True, exist_ok=True)
            fallback_path = fallback_dir / f"session-{chat_id}.jsonl"
            safe_event["auditFallback"] = {"from": str(path), "to": str(fallback_path)}
            serialized = json.dumps(safe_event, ensure_ascii=False) + "\n"
            with fallback_path.open("a", encoding="utf-8") as handle:
                handle.write(serialized)
            audit_dir = fallback_dir
        if settings.chat_audit_stdout_enabled:
            try:
                _logger.info("%s", json.dumps(safe_event, ensure_ascii=False))
            except Exception:
                pass
        _maybe_prune_chat_audit(settings, audit_dir)
    except Exception:
        # Never break chat flow because audit logging failed.
        return
