from __future__ import annotations

from typing import Any, Callable

from .config import Settings


def _coerce_non_negative_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        coerced = int(value)
    except Exception:
        return None
    return max(0, coerced)


def infer_model_context_limit_tokens(
    model_hint: str | None,
    *,
    default_tokens: int,
    model_context_limit_hints: list[tuple[str, int]],
) -> int:
    model = str(model_hint or "").strip().lower()
    if "/" in model:
        model = model.split("/")[-1].strip()
    for token, size in model_context_limit_hints:
        if token in model:
            return int(size)
    return int(default_tokens)


def evaluate_payload_token_budget(
    settings: Settings,
    payload: dict[str, Any],
    *,
    model_hint: str | None = None,
    estimate_payload_token_usage: Callable[..., dict[str, Any]],
    model_context_limit_hints: list[tuple[str, int]],
) -> dict[str, Any]:
    estimate = estimate_payload_token_usage(payload, model_hint=model_hint)
    estimated_prompt_tokens = _coerce_non_negative_int(estimate.get("estimatedPromptTokens"))
    token_estimate_available = estimated_prompt_tokens is not None

    context_limit = int(settings.token_budget_context_limit_tokens or 0)
    if context_limit <= 0:
        context_limit = infer_model_context_limit_tokens(
            model_hint,
            default_tokens=max(16_000, int(settings.token_budget_default_context_limit_tokens)),
            model_context_limit_hints=model_context_limit_hints,
        )
    context_limit = max(16_000, context_limit)

    reserved_output_tokens = max(256, int(settings.token_budget_reserved_output_tokens))
    reserved_output_tokens = min(reserved_output_tokens, max(1024, context_limit // 2))
    prompt_budget_tokens = max(1024, context_limit - reserved_output_tokens)

    utilization = (
        (float(estimated_prompt_tokens) / float(prompt_budget_tokens))
        if token_estimate_available
        else None
    )
    warn_ratio = float(settings.token_budget_warn_ratio)
    compact_ratio = float(settings.token_budget_compact_ratio)
    hard_ratio = float(settings.token_budget_hard_ratio)

    decision = "unknown"
    if utilization is None:
        decision = "unknown"
    elif utilization >= hard_ratio:
        decision = "hard"
    elif utilization >= compact_ratio:
        decision = "compact"
    elif utilization >= warn_ratio:
        decision = "warn"
    else:
        decision = "ok"

    return {
        "decision": decision,
        "estimatedPromptTokens": estimated_prompt_tokens,
        "tokenEstimateAvailable": token_estimate_available,
        "utilizationRatio": round(utilization, 4) if utilization is not None else None,
        "contextLimitTokens": context_limit,
        "promptBudgetTokens": prompt_budget_tokens,
        "reservedOutputTokens": reserved_output_tokens,
        "warnRatio": round(warn_ratio, 4),
        "compactRatio": round(compact_ratio, 4),
        "hardRatio": round(hard_ratio, 4),
        "serializedChars": int(estimate.get("serializedChars") or 0),
        "messageCount": int(estimate.get("messageCount") or 0),
        "toolMessageCount": int(estimate.get("toolMessageCount") or 0),
        "toolCount": int(estimate.get("toolCount") or 0),
        "estimateMethod": str(estimate.get("method") or ""),
    }


def apply_payload_token_budget(
    settings: Settings,
    payload: dict[str, Any],
    *,
    model_hint: str | None = None,
    evaluate_payload_token_budget: Callable[..., dict[str, Any]],
    compact_chat_completions_payload: Callable[..., dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not settings.token_budget_enabled:
        unchanged = evaluate_payload_token_budget(settings, payload, model_hint=model_hint)
        return payload, {
            "enabled": False,
            "modelHint": str(model_hint or ""),
            "actions": [],
            "checks": [{"stage": "initial", **unchanged}],
            "finalDecision": unchanged.get("decision"),
        }

    working_payload = dict(payload or {})
    checks: list[dict[str, Any]] = []
    actions: list[str] = []
    # Context-preserving profile shared across providers/models.
    compact_profile = {
        "max_messages": 20,
        "max_tool_messages": 8,
        "max_message_content_chars": 3000,
        "compact_tool_messages": False,
        "max_tool_content_chars": 4000,
        "compact_tool_schemas": True,
        "aggressive_tool_schema_compaction": False,
    }
    hard_profile = {
        "max_messages": 14,
        "max_tool_messages": 6,
        "max_message_content_chars": 1500,
        "compact_tool_messages": True,
        "max_tool_content_chars": 2200,
        "compact_tool_schemas": True,
        "aggressive_tool_schema_compaction": False,
    }
    hard_trim_profile = {
        "start_messages": 10,
        "min_messages": 6,
        "max_tool_messages": 4,
        "max_message_content_chars": 1100,
        "compact_tool_messages": True,
        "max_tool_content_chars": 1200,
        "compact_tool_schemas": True,
        "aggressive_tool_schema_compaction": False,
    }

    initial = evaluate_payload_token_budget(settings, working_payload, model_hint=model_hint)
    checks.append({"stage": "initial", **initial})

    if initial.get("decision") == "unknown":
        actions.append("skip:unknown-estimate")
    elif initial.get("decision") in {"compact", "hard"}:
        working_payload = compact_chat_completions_payload(
            working_payload,
            max_messages=int(compact_profile["max_messages"]),
            max_tool_messages=int(compact_profile["max_tool_messages"]),
            max_message_content_chars=int(compact_profile["max_message_content_chars"]),
            compact_tool_messages=bool(compact_profile["compact_tool_messages"]),
            max_tool_content_chars=int(compact_profile["max_tool_content_chars"]),
            compact_tool_schemas=bool(compact_profile["compact_tool_schemas"]),
            aggressive_tool_schema_compaction=bool(compact_profile["aggressive_tool_schema_compaction"]),
        )
        actions.append("apply:compact-profile")
        compact_check = evaluate_payload_token_budget(settings, working_payload, model_hint=model_hint)
        checks.append({"stage": "compact", **compact_check})

        if compact_check.get("decision") == "hard":
            working_payload = compact_chat_completions_payload(
                working_payload,
                max_messages=int(hard_profile["max_messages"]),
                max_tool_messages=int(hard_profile["max_tool_messages"]),
                max_message_content_chars=int(hard_profile["max_message_content_chars"]),
                compact_tool_messages=bool(hard_profile["compact_tool_messages"]),
                max_tool_content_chars=int(hard_profile["max_tool_content_chars"]),
                compact_tool_schemas=bool(hard_profile["compact_tool_schemas"]),
                aggressive_tool_schema_compaction=bool(hard_profile["aggressive_tool_schema_compaction"]),
            )
            actions.append("apply:hard-profile")
            hard_check = evaluate_payload_token_budget(settings, working_payload, model_hint=model_hint)
            checks.append({"stage": "hard", **hard_check})

            trim_messages = int(hard_trim_profile["start_messages"])
            latest_check = hard_check
            trim_floor = int(hard_trim_profile["min_messages"])
            while latest_check.get("decision") == "hard" and trim_messages >= trim_floor:
                working_payload = compact_chat_completions_payload(
                    working_payload,
                    max_messages=trim_messages,
                    max_tool_messages=int(hard_trim_profile["max_tool_messages"]),
                    max_message_content_chars=int(hard_trim_profile["max_message_content_chars"]),
                    compact_tool_messages=bool(hard_trim_profile["compact_tool_messages"]),
                    max_tool_content_chars=int(hard_trim_profile["max_tool_content_chars"]),
                    compact_tool_schemas=bool(hard_trim_profile["compact_tool_schemas"]),
                    aggressive_tool_schema_compaction=bool(hard_trim_profile["aggressive_tool_schema_compaction"]),
                )
                actions.append(f"apply:hard-trim-{trim_messages}")
                latest_check = evaluate_payload_token_budget(settings, working_payload, model_hint=model_hint)
                checks.append({"stage": f"hard-trim-{trim_messages}", **latest_check})
                trim_messages -= 2

    final_check = checks[-1] if checks else initial
    return working_payload, {
        "enabled": True,
        "modelHint": str(model_hint or ""),
        "actions": actions,
        "checks": checks,
        "finalDecision": final_check.get("decision"),
        "finalEstimatedPromptTokens": final_check.get("estimatedPromptTokens"),
        "finalUtilizationRatio": final_check.get("utilizationRatio"),
        "contextLimitTokens": final_check.get("contextLimitTokens"),
        "promptBudgetTokens": final_check.get("promptBudgetTokens"),
        "reservedOutputTokens": final_check.get("reservedOutputTokens"),
    }
