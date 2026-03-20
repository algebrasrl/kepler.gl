from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from fastapi_mcp import FastApiMCP
try:
    from openai import AsyncOpenAI
except Exception:  # pragma: no cover - runtime dependency handled by container image
    AsyncOpenAI = None  # type: ignore[assignment]

from .audit_logging import (
    _CHAT_ID_HEADER,
    _REQUEST_ID_HEADER,
    _audit_context_for_log,
    _audit_payload_for_log,
    _collect_response_tool_call_names,
    _configure_audit_runtime,
    _resolve_audit_session_id,
    _set_trace_headers,
    _write_chat_audit_event,
)
from .agent_chain import (
    SUPPORTED_PROVIDERS,
    _build_chain_agents,
)
from .chat_response_normalization import _normalize
from .chat_payload_compaction import (
    _compact_chat_completions_payload,
    _deduplicate_discovery_tool_turns,
    _sanitize_openai_tools_for_gemini_model,
    _serialize_tool_call_args_for_signature,
)
from .objective_anchor import (
    _build_objective_focus_terms,
    _inject_objective_anchor_message,
    _normalize_openai_response_final_text,
)
from .objective_intent import (
    _extract_objective_required_focus_phrases,
    _objective_mentions_cloud_or_saved_maps,
    _objective_requests_cloud_load_sequence,
    _objective_requests_dataset_discovery,
    _objective_requests_population_transfer_modes,
    _objective_requires_clip_stats_coverage_validation,
    _objective_requires_ranked_output,
)
from .cloud_provider import (
    _normalize_cloud_provider,
    _resolve_cloud_provider_config,
)
from .chat_request_coercion import _coerce_chat_request
from .config import Settings, load_settings
from .ollama_urls import (
    _ollama_chat_url,
    _ollama_openai_chat_completions_url,
)
from .models import AgentConfig, ChatResponse
from .qmap_context import (
    _inject_qmap_context_message,
    _sanitize_qmap_context_payload,
)
from .openai_chat_payload import _coerce_openai_chat_payload
from .message_text import _extract_message_text, _extract_prompt_from_messages
from .payload_compaction import (
    apply_payload_token_budget as _apply_payload_token_budget,
    evaluate_payload_token_budget as _evaluate_payload_token_budget,
)
from .provider_retry import (
    _is_retryable_status,
    _openrouter_optional_headers,
)
from .provider_transport import (
    get_json as _get_json,
    post_openai_chat_completions_via_openai_sdk as _post_openai_chat_completions_via_openai_sdk,
    post_json as _post_json,
    post_openrouter_chat_completions_via_openai_sdk as _post_openrouter_chat_completions_via_openai_sdk,
    sanitize_openai_stream_chunks as _sanitize_openai_stream_chunks,
    stream_proxy as _stream_proxy,
    stream_openai_chat_completions_via_openai_sdk as _stream_openai_chat_completions_via_openai_sdk,
    stream_openrouter_chat_completions_via_openai_sdk as _stream_openrouter_chat_completions_via_openai_sdk,
)
from .request_routing import (
    _maybe_force_tool_choice,
    _should_skip_agent_for_payload,
)
from .request_tool_results import (
    _extract_recent_tool_results_since_last_user,
    _extract_request_tool_results,
    _has_assistant_text_since_last_user,
    _messages_since_last_user,
)
from .request_coercion import (
    _repair_openai_response_tool_call_arguments,
    _repair_qmap_metric_tool_call_arguments,
    _repair_qmap_validation_tool_call_arguments,
)
from .response_claims import (
    _response_claims_centering_success,
    _response_claims_operational_success,
    _response_claims_success,
)
from .tool_contracts import (
    _QMAP_TOOL_RESULT_SCHEMA,
)
from .tool_calls import (
    _extract_assistant_tool_calls,
    _extract_request_tool_names,
    _extract_response_tool_calls,
    _extract_tool_calls_from_assistant_message,
    _parse_tool_arguments,
)
from .tool_result_parsing import (
    _build_dataset_hint,
    _build_source_dataset_hint,
    _extract_dataset_ref_from_call,
    _extract_success_from_text,
    _read_tool_message_content,
)
from .usage_estimation import (
    _estimate_payload_token_usage,
    _extract_upstream_usage,
)
from .runtime_guardrails import (
    RuntimeToolLoopLimitBindings,
    enforce_runtime_tool_loop_limits as _enforce_runtime_tool_loop_limits,
    _inject_runtime_guardrail_message,
    is_likely_normalized_metric_field as _is_likely_normalized_metric_field,
    objective_requests_coloring as _objective_requests_coloring,
    objective_requests_map_centering as _objective_requests_map_centering,
    objective_requests_map_display as _objective_requests_map_display,
    objective_requests_normalized_metric as _objective_requests_normalized_metric,
    objective_requests_provider_discovery as _objective_requests_provider_discovery,
    prune_open_panel_only_chart_navigation as _prune_open_panel_only_chart_navigation,
    prune_forbidden_qmap_runtime_tools as _prune_forbidden_qmap_runtime_tools,
    prune_heavy_recompute_tools_after_low_distinct_color_failure as _prune_heavy_recompute_tools_after_low_distinct_color_failure,
    prune_population_style_tools_for_unresolved_value_coloring as _prune_population_style_tools_for_unresolved_value_coloring,
    prune_repeated_discovery_tools as _prune_repeated_discovery_tools,
    prune_sampling_preview_tools_for_superlatives as _prune_sampling_preview_tools_for_superlatives,
    summarize_runtime_tool_policy as _summarize_runtime_tool_policy,
    prune_uninformative_chart_tools_for_ranking as _prune_uninformative_chart_tools_for_ranking,
)
from .runtime_workflow_state import build_runtime_workflow_state
from .services.request_processor import (
    _MODEL_CONTEXT_LIMIT_HINTS,
    _with_context,
    _build_openai_stream_request_id_chunk,
    _DISCOVERY_TOOLS,
    _DISCOVERY_LOOP_PRUNE_TOOLS,
    _DISCOVERY_LOOP_PROGRESS_TOOLS,
    _FORBIDDEN_QMAP_RUNTIME_TOOLS,
    _latest_successful_tool_index,
    _is_metric_field_not_found_failure,
    _objective_explicit_population_metric,
    _has_unresolved_zonal_ui_freeze_failure,
    _has_recent_forest_clc_query_call,
    _is_low_distinct_color_failure,
    _objective_explicit_category_distribution,
    _has_repeated_discovery_loop,
    _runtime_guardrail_injection_bindings,
    _runtime_tool_loop_limit_bindings,
    _derive_runtime_quality_metrics,
)


_RUNTIME_POLICY_SUMMARY_HEADER = "x-q-assistant-runtime-policy-summary"



def _resolve_cloud_authorization_header(
    configured_cloud_token: str | None,
    caller_authorization: str | None,
) -> str | None:
    token = str(configured_cloud_token or "").strip()
    if not token:
        raw_authorization = str(caller_authorization or "").strip()
        if raw_authorization.lower().startswith("bearer "):
            token = raw_authorization[7:].strip()
    if not token:
        return None
    return f"Bearer {token}"


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or load_settings()
    app = FastAPI(title="q-assistant", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=app_settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["mcp-session-id", _REQUEST_ID_HEADER, _CHAT_ID_HEADER, _RUNTIME_POLICY_SUMMARY_HEADER]
    )

    # ─── q-cumber proxy ────────────────────────────────────────────────────
    from .services.qcumber_proxy import mount_qcumber_proxy
    mount_qcumber_proxy(
        app,
        base_url=app_settings.qcumber_cloud_api_base,
        token=app_settings.qcumber_cloud_token,
        timeout=app_settings.qcumber_cloud_timeout_seconds,
    )

    @app.get("/health")
    async def health() -> dict:
        return {
            "ok": True,
            "service": "q-assistant",
            "supportedProviders": sorted(SUPPORTED_PROVIDERS),
            "defaultProvider": app_settings.default_provider,
            "defaultModel": app_settings.default_model,
        }

    @app.get("/me")
    async def me() -> dict[str, Any]:
        """
        Lightweight profile endpoint used by q-map Profile panel.
        """
        return {
            "success": True,
            "profile": {
                "name": app_settings.profile_name,
                "email": app_settings.profile_email,
                "registeredAt": app_settings.profile_registered_at,
                "country": app_settings.profile_country,
            },
        }

    @app.get("/qmap/mcp/list-cloud-maps", operation_id="list_qmap_cloud_maps")
    async def list_qmap_cloud_maps(
        provider: str | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        """
        MCP tool: list available q-map cloud maps.
        """
        cloud = _resolve_cloud_provider_config(app_settings, provider)
        base = cloud["base"]
        if not base:
            raise HTTPException(
                status_code=503,
                detail=cloud["config_error"],
            )

        headers = {"accept": "application/json"}
        cloud_authorization = _resolve_cloud_authorization_header(
            cloud["token"],
            authorization,
        )
        if cloud_authorization:
            headers["authorization"] = cloud_authorization

        timeout = httpx.Timeout(cloud["timeout_seconds"])
        maps = []
        async with httpx.AsyncClient(timeout=timeout) as client:
            payload = await _get_json(client, f"{base}/maps", headers)
            items = payload if isinstance(payload, list) else (
                payload.get("items", payload.get("maps", []))
                if isinstance(payload, dict)
                else []
            )
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    map_id = str(item.get("id") or item.get("mapId") or "")
                    title = str(item.get("title") or item.get("name") or "Untitled map")
                    description = str(item.get("description") or "")
                    maps.append(
                        {
                            "id": map_id,
                            "title": title,
                            "description": description,
                            "updatedAt": item.get("updatedAt") or item.get("updated_at"),
                        }
                    )

        return {
            "success": True,
            "provider": cloud["provider"],
            "count": len(maps),
            "maps": maps,
            "sourceBaseUrl": base,
        }

    @app.get("/qmap/mcp/cloud-status", operation_id="qmap_cloud_status")
    async def qmap_cloud_status(
        provider: str | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        """
        MCP/debug tool: show q-assistant cloud config visibility (without secrets).
        """
        cloud = _resolve_cloud_provider_config(app_settings, provider)
        base = cloud["base"]
        has_token = bool(cloud["token"])
        if not base:
            return {
                "success": False,
                "configured": False,
                "provider": cloud["provider"],
                "sourceBaseUrl": "",
                "hasToken": has_token,
                "upstreamStatus": None,
                "mapCount": None,
                "sampleTitles": [],
            }

        headers = {"accept": "application/json"}
        cloud_authorization = _resolve_cloud_authorization_header(
            cloud["token"],
            authorization,
        )
        if cloud_authorization:
            headers["authorization"] = cloud_authorization

        timeout = httpx.Timeout(cloud["timeout_seconds"])
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(f"{base}/maps", headers=headers)
            upstream_status = response.status_code
            body = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            items = body if isinstance(body, list) else (
                body.get("items", body.get("maps", []))
                if isinstance(body, dict)
                else []
            )
            titles = []
            if isinstance(items, list):
                for item in items[:5]:
                    if isinstance(item, dict):
                        titles.append(str(item.get("title") or item.get("name") or item.get("id") or ""))

            return {
                "success": response.status_code < 400,
                "configured": True,
                "provider": cloud["provider"],
                "sourceBaseUrl": base,
                "hasToken": has_token,
                "upstreamStatus": upstream_status,
                "mapCount": len(items) if isinstance(items, list) else None,
                "sampleTitles": [t for t in titles if t],
            }
        except Exception as error:
            return {
                "success": False,
                "configured": True,
                "provider": cloud["provider"],
                "sourceBaseUrl": base,
                "hasToken": has_token,
                "upstreamStatus": None,
                "mapCount": None,
                "sampleTitles": [],
                "error": str(error),
            }

    @app.get("/qmap/mcp/get-cloud-map", operation_id="get_qmap_cloud_map")
    async def get_qmap_cloud_map(
        map_id: str,
        provider: str | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        """
        MCP tool: fetch a specific q-map cloud map payload by id.
        """
        if not map_id:
            raise HTTPException(status_code=400, detail="map_id is required")

        cloud = _resolve_cloud_provider_config(app_settings, provider)
        base = cloud["base"]
        if not base:
            raise HTTPException(
                status_code=503,
                detail=cloud["config_error"],
            )

        headers = {"accept": "application/json"}
        cloud_authorization = _resolve_cloud_authorization_header(
            cloud["token"],
            authorization,
        )
        if cloud_authorization:
            headers["authorization"] = cloud_authorization

        timeout = httpx.Timeout(cloud["timeout_seconds"])
        async with httpx.AsyncClient(timeout=timeout) as client:
            payload = await _get_json(client, f"{base}/maps/{map_id}", headers)

        return {
            "success": True,
            "provider": cloud["provider"],
            "mapId": map_id,
            "payload": payload,
        }

    @app.post(
        "/qmap/mcp/build-load-cloud-map-action",
        operation_id="build_load_cloud_map_action",
    )
    async def build_load_cloud_map_action(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        MCP tool: build a normalized q-map action payload to load a cloud map.
        """
        req = payload or {}
        map_id = str(req.get("mapId") or req.get("id") or "").strip()
        provider = _normalize_cloud_provider(str(req.get("provider") or "q-storage-backend"))
        if not map_id:
            raise HTTPException(status_code=400, detail="mapId is required")

        action = {
            "tool": "loadQMapCloudMap",
            "args": {
                "provider": provider,
                "mapId": map_id,
            },
        }
        return {"success": True, "action": action}

    @app.post(
        "/qmap/mcp/build-equals-filter-action",
        operation_id="build_equals_filter_action",
    )
    async def build_equals_filter_action(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        MCP tool: build a normalized q-map action payload to apply equality filter.
        """
        req = payload or {}
        dataset_name = str(req.get("datasetName") or "").strip()
        field_name = str(req.get("fieldName") or "").strip()
        if not dataset_name or not field_name:
            raise HTTPException(status_code=400, detail="datasetName and fieldName are required")
        value = req.get("value")
        if value is None:
            raise HTTPException(status_code=400, detail="value is required")

        action = {
            "tool": "setQMapFieldEqualsFilter",
            "args": {
                "datasetName": dataset_name,
                "fieldName": field_name,
                "value": value,
            },
        }
        return {"success": True, "action": action}

    @app.post("/chat", response_model=ChatResponse)
    async def chat(
        payload: dict[str, Any],
        response: Response,
        x_q_assistant_session_id: str | None = Header(default=None),
    ) -> ChatResponse:
        request_id = uuid.uuid4().hex
        started = time.perf_counter()
        audit_session_id = _resolve_audit_session_id(x_q_assistant_session_id, payload, None)
        _set_trace_headers(response.headers, request_id, audit_session_id)
        request_payload = _coerce_chat_request(payload)
        prompt = _with_context(request_payload.prompt, request_payload.context)
        timeout = httpx.Timeout(app_settings.request_timeout_seconds)
        agents = _build_chain_agents(app_settings, incoming=request_payload.agent, openai_compatible_only=False)
        attempts: list[str] = []
        switch_log: list[str] = []
        body: Any = None
        used_agent: AgentConfig | None = None
        last_exc: Exception | None = None

        async with httpx.AsyncClient(timeout=timeout) as client:
            for index, agent in enumerate(agents):
                if not agent.baseUrl:
                    last_exc = HTTPException(status_code=400, detail="Missing baseUrl for selected provider")
                    attempts.append(f"{agent.provider}:{agent.model} -> missing baseUrl")
                    continue
                base = agent.baseUrl.rstrip("/")
                try:
                    if agent.provider == "openrouter":
                        if not agent.apiKey:
                            raise HTTPException(status_code=400, detail="Missing apiKey for openrouter")
                        body = await _post_openrouter_chat_completions_via_openai_sdk(
                            async_openai_cls=AsyncOpenAI,
                            base_url=base,
                            api_key=agent.apiKey,
                            payload={
                                "model": agent.model,
                                "messages": [{"role": "user", "content": prompt}],
                                **({"temperature": agent.temperature} if agent.temperature is not None else {}),
                                **({"top_p": agent.topP} if agent.topP is not None else {}),
                            },
                            timeout_seconds=app_settings.request_timeout_seconds,
                            retry_attempts=app_settings.upstream_retry_attempts,
                            retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                            retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                            retry_jitter_ratio=app_settings.upstream_retry_jitter_ratio,
                            default_headers=_openrouter_optional_headers(agent.provider),
                        )
                    elif agent.provider == "openai":
                        if not agent.apiKey:
                            raise HTTPException(status_code=400, detail="Missing apiKey for openai")
                        body = await _post_openai_chat_completions_via_openai_sdk(
                            async_openai_cls=AsyncOpenAI,
                            base_url=base,
                            api_key=agent.apiKey,
                            payload={
                                "model": agent.model,
                                "messages": [{"role": "user", "content": prompt}],
                                **({"temperature": agent.temperature} if agent.temperature is not None else {}),
                                **({"top_p": agent.topP} if agent.topP is not None else {}),
                            },
                            timeout_seconds=app_settings.request_timeout_seconds,
                            retry_attempts=app_settings.upstream_retry_attempts,
                            retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                            retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                            retry_jitter_ratio=app_settings.upstream_retry_jitter_ratio,
                        )
                    elif agent.provider == "ollama":
                        body = await _post_json(
                            client,
                            _ollama_chat_url(base),
                            {"content-type": "application/json"},
                            {
                                "model": agent.model,
                                "stream": False,
                                "messages": [{"role": "user", "content": prompt}],
                                "options": {
                                    **({"temperature": agent.temperature} if agent.temperature is not None else {}),
                                    **({"top_p": agent.topP} if agent.topP is not None else {})
                                }
                            },
                            retry_attempts=app_settings.upstream_retry_attempts,
                            retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                            retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                        )
                    else:
                        raise HTTPException(status_code=400, detail=f"Unsupported provider '{agent.provider}'")

                    used_agent = agent
                    if index > 0:
                        switch_log.append(f"Cambio agente: {agents[index - 1].provider}/{agents[index - 1].model} -> {agent.provider}/{agent.model}")
                    break
                except Exception as exc:  # keep chain progression explicit
                    last_exc = exc
                    attempts.append(f"{agent.provider}:{agent.model} -> {exc}")
                    if index < len(agents) - 1:
                        switch_log.append(
                            f"Cambio agente: {agent.provider}/{agent.model} non disponibile, provo il successivo."
                        )

        if used_agent is None:
            detail = "Nessun agente disponibile per soddisfare la richiesta."
            if attempts:
                detail = f"{detail} Tentativi: {' | '.join(attempts)}"
            _write_chat_audit_event(
                app_settings,
                endpoint="/chat",
                status=int(last_exc.status_code) if isinstance(last_exc, HTTPException) else 503,
                started_at=started,
                session_id=audit_session_id,
                request_id=request_id,
                requestedProvider=getattr(request_payload.agent, "provider", None),
                requestedModel=getattr(request_payload.agent, "model", None),
                attempts=attempts,
                switchLog=switch_log,
                error=detail,
                requestPayload=_audit_payload_for_log(app_settings, payload),
            )
            if isinstance(last_exc, HTTPException):
                raise HTTPException(status_code=last_exc.status_code, detail=detail)
            raise HTTPException(status_code=503, detail=detail)

        answer = _normalize(body)
        _write_chat_audit_event(
            app_settings,
            endpoint="/chat",
            status=200,
            started_at=started,
            session_id=audit_session_id,
            request_id=request_id,
            requestedProvider=getattr(request_payload.agent, "provider", None),
            requestedModel=getattr(request_payload.agent, "model", None),
            usedProvider=used_agent.provider,
            usedModel=used_agent.model,
            attempts=attempts,
            switchLog=switch_log,
            requestPayload=_audit_payload_for_log(app_settings, payload),
            responsePayload=_audit_payload_for_log(
                app_settings,
                {"answer": answer, "provider": used_agent.provider, "model": used_agent.model},
            ),
        )

        return ChatResponse(
            answer=answer,
            provider=used_agent.provider or "",
            model=used_agent.model or "",
            switchLog=switch_log,
            requestId=request_id,
            chatId=audit_session_id,
        )

    @app.post("/chat/completions")
    async def chat_completions_proxy(
        payload: dict[str, Any],
        response: Response,
        authorization: str | None = Header(default=None),
        x_qmap_context: str | None = Header(default=None),
        x_q_assistant_session_id: str | None = Header(default=None),
    ) -> Any:
        """
        OpenAI-compatible endpoint for OpenAssistant/Kepler runtime.
        It forwards request body to upstream {baseUrl}/chat/completions and returns raw provider response.
        """
        request_id = uuid.uuid4().hex
        started = time.perf_counter()
        audit_session_id = _resolve_audit_session_id(x_q_assistant_session_id, payload, x_qmap_context)
        _set_trace_headers(response.headers, request_id, audit_session_id)

        agents = _build_chain_agents(app_settings, incoming=None, openai_compatible_only=True)
        if not agents:
            _write_chat_audit_event(
                app_settings,
                endpoint="/chat/completions",
                status=400,
                started_at=started,
                session_id=audit_session_id,
                request_id=request_id,
                error="q-assistant /chat/completions supports openai-compatible providers only",
            )
            raise HTTPException(
                status_code=400,
                detail="q-assistant /chat/completions supports openai-compatible providers only",
            )
        base_payload = _coerce_openai_chat_payload(payload or {})
        initial_tool_names = _extract_request_tool_names(base_payload)
        base_payload = _maybe_force_tool_choice(
            base_payload,
            enabled=app_settings.explicit_tool_routing_enabled,
        )
        base_payload = _inject_qmap_context_message(
            base_payload,
            x_qmap_context,
            enabled=app_settings.qmap_context_enabled,
            max_chars=app_settings.qmap_context_max_chars,
        )
        base_payload = _prune_forbidden_qmap_runtime_tools(
            base_payload,
            forbidden_qmap_runtime_tools=_FORBIDDEN_QMAP_RUNTIME_TOOLS,
        )
        base_payload = _prune_open_panel_only_chart_navigation(
            base_payload,
            extract_prompt_from_messages=_extract_prompt_from_messages,
        )
        base_payload = _prune_uninformative_chart_tools_for_ranking(
            base_payload,
            extract_prompt_from_messages=_extract_prompt_from_messages,
            objective_requires_ranked_output=_objective_requires_ranked_output,
            objective_explicit_category_distribution=_objective_explicit_category_distribution,
        )
        base_payload = _prune_sampling_preview_tools_for_superlatives(
            base_payload,
            extract_prompt_from_messages=_extract_prompt_from_messages,
            objective_requires_ranked_output=_objective_requires_ranked_output,
            extract_request_tool_names=_extract_request_tool_names,
            extract_request_tool_results=_extract_request_tool_results,
            latest_successful_tool_index=_latest_successful_tool_index,
            is_metric_field_not_found_failure=_is_metric_field_not_found_failure,
        )
        base_payload = _prune_population_style_tools_for_unresolved_value_coloring(
            base_payload,
            extract_prompt_from_messages=_extract_prompt_from_messages,
            objective_requests_coloring=_objective_requests_coloring,
            objective_explicit_population_metric=_objective_explicit_population_metric,
            extract_request_tool_results=_extract_request_tool_results,
            has_unresolved_zonal_ui_freeze_failure=_has_unresolved_zonal_ui_freeze_failure,
            extract_assistant_tool_calls=_extract_assistant_tool_calls,
            has_recent_forest_clc_query_call=_has_recent_forest_clc_query_call,
        )
        base_payload = _prune_heavy_recompute_tools_after_low_distinct_color_failure(
            base_payload,
            extract_prompt_from_messages=_extract_prompt_from_messages,
            objective_requests_coloring=_objective_requests_coloring,
            extract_request_tool_results=_extract_request_tool_results,
            is_low_distinct_color_failure=_is_low_distinct_color_failure,
        )
        base_payload = _inject_objective_anchor_message(base_payload)
        base_payload = _inject_runtime_guardrail_message(
            base_payload,
            bindings=_runtime_guardrail_injection_bindings(),
        )
        base_payload = _deduplicate_discovery_tool_turns(
            base_payload,
            discovery_tools=_DISCOVERY_TOOLS,
        )
        base_payload = _prune_repeated_discovery_tools(
            base_payload,
            extract_request_tool_results=_extract_request_tool_results,
            has_repeated_discovery_loop=_has_repeated_discovery_loop,
            extract_request_tool_names=_extract_request_tool_names,
            discovery_loop_progress_tools=_DISCOVERY_LOOP_PROGRESS_TOOLS,
            discovery_loop_prune_tools=_DISCOVERY_LOOP_PRUNE_TOOLS,
        )
        base_payload = _enforce_runtime_tool_loop_limits(
            base_payload,
            bindings=_runtime_tool_loop_limit_bindings(),
        )
        runtime_policy_summary = _summarize_runtime_tool_policy(
            initial_tool_names=initial_tool_names,
            final_tool_names=_extract_request_tool_names(base_payload),
        )
        model_hint_for_budget = str(agents[0].model or "") if agents else str(base_payload.get("model") or "")
        base_payload, token_budget_info = _apply_payload_token_budget(
            app_settings,
            base_payload,
            model_hint=model_hint_for_budget,
            evaluate_payload_token_budget=lambda settings, payload, *, model_hint=None: _evaluate_payload_token_budget(
                settings,
                payload,
                model_hint=model_hint,
                estimate_payload_token_usage=_estimate_payload_token_usage,
                model_context_limit_hints=_MODEL_CONTEXT_LIMIT_HINTS,
            ),
            compact_chat_completions_payload=_compact_chat_completions_payload,
        )
        base_payload_token_estimate = _estimate_payload_token_usage(
            base_payload,
            model_hint=model_hint_for_budget,
        )
        should_stream = bool(base_payload.get("stream"))
        attempts: list[str] = []
        switch_log: list[str] = []
        upstream_retry_traces: list[dict[str, Any]] = []
        last_exc: Exception | None = None

        timeout_seconds = app_settings.request_timeout_seconds + (
            app_settings.upstream_retry_timeout_increment_seconds
            * max(0, int(app_settings.upstream_retry_attempts))
        )
        timeout = httpx.Timeout(timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for index, agent in enumerate(agents):
                provider = str(agent.provider or "").lower()
                base = str(agent.baseUrl or "").rstrip("/")
                skip_reason = _should_skip_agent_for_payload(agent, base_payload)
                if skip_reason:
                    attempts.append(f"{provider}:{agent.model} -> skipped:{skip_reason}")
                    if index < len(agents) - 1:
                        switch_log.append(
                            f"Cambio agente: {provider}/{agent.model} saltato ({skip_reason}), provo il successivo."
                        )
                    continue
                if not base:
                    last_exc = HTTPException(status_code=400, detail="Missing baseUrl for provider")
                    attempts.append(f"{provider}:{agent.model} -> missing baseUrl")
                    continue

                headers: dict[str, str] = {"content-type": "application/json"}
                api_key = agent.apiKey
                if (
                    not api_key
                    and app_settings.allow_caller_api_key_fallback
                    and authorization
                    and authorization.lower().startswith("bearer ")
                ):
                    api_key = authorization[7:].strip() or api_key
                use_openai_sdk_transport = provider in {"openrouter", "openai"}
                if provider != "ollama":
                    if not api_key:
                        last_exc = HTTPException(status_code=400, detail="Missing apiKey for provider")
                        attempts.append(f"{provider}:{agent.model} -> missing apiKey")
                        continue
                    if not use_openai_sdk_transport:
                        headers["authorization"] = f"Bearer {api_key}"
                openrouter_headers = _openrouter_optional_headers(provider)
                if not use_openai_sdk_transport:
                    headers.update(openrouter_headers)

                upstream_url = (
                    _ollama_openai_chat_completions_url(base)
                    if provider == "ollama"
                    else f"{base}/chat/completions"
                )
                upstream_payload = dict(base_payload)
                upstream_payload["model"] = agent.model
                upstream_payload = _sanitize_openai_tools_for_gemini_model(
                    upstream_payload,
                    model_hint=str(agent.model or ""),
                )
                request_payload_token_estimate = _estimate_payload_token_usage(
                    upstream_payload,
                    model_hint=str(agent.model or ""),
                )

                try:
                    retry_trace: list[dict[str, Any]] = []
                    if should_stream:
                        stream_state: dict[str, Any] = {
                            "buffer": "",
                            "chunks": 0,
                            "bytes": 0,
                            "text_parts": [],
                            "tool_calls": {},
                            "upstream_usage": None,
                        }

                        def _consume_stream_chunk(chunk: bytes) -> None:
                            stream_state["chunks"] += 1
                            stream_state["bytes"] += len(chunk or b"")
                            piece = (chunk or b"").decode("utf-8", errors="ignore")
                            if not piece:
                                return
                            stream_state["buffer"] += piece
                            while "\n" in stream_state["buffer"]:
                                raw_line, remainder = stream_state["buffer"].split("\n", 1)
                                stream_state["buffer"] = remainder
                                line = raw_line.strip()
                                if not line.startswith("data:"):
                                    continue
                                data = line[5:].strip()
                                if not data or data == "[DONE]":
                                    continue
                                try:
                                    payload_obj = json.loads(data)
                                except Exception:
                                    continue
                                usage_metrics = _extract_upstream_usage(payload_obj)
                                if usage_metrics:
                                    stream_state["upstream_usage"] = usage_metrics
                                choices = payload_obj.get("choices")
                                if not isinstance(choices, list):
                                    continue
                                for choice in choices:
                                    delta = choice.get("delta") if isinstance(choice, dict) else None
                                    if not isinstance(delta, dict):
                                        continue
                                    content_piece = delta.get("content")
                                    if isinstance(content_piece, str) and content_piece:
                                        stream_state["text_parts"].append(content_piece)
                                    tool_calls = delta.get("tool_calls")
                                    if not isinstance(tool_calls, list):
                                        continue
                                    for call in tool_calls:
                                        if not isinstance(call, dict):
                                            continue
                                        idx = int(call.get("index") or 0)
                                        current = stream_state["tool_calls"].setdefault(
                                            idx,
                                            {"id": "", "type": "function", "function": {"name": "", "arguments": ""}},
                                        )
                                        call_id = call.get("id")
                                        if isinstance(call_id, str) and call_id:
                                            current["id"] = call_id
                                        fn_obj = call.get("function")
                                        if isinstance(fn_obj, dict):
                                            fn_name = fn_obj.get("name")
                                            fn_args = fn_obj.get("arguments")
                                            if isinstance(fn_name, str) and fn_name:
                                                current["function"]["name"] = fn_name
                                            if isinstance(fn_args, str) and fn_args:
                                                current["function"]["arguments"] += fn_args

                        def _finalize_stream_audit() -> None:
                            tool_calls_out: list[dict[str, Any]] = []
                            for idx in sorted(stream_state["tool_calls"].keys()):
                                item = stream_state["tool_calls"][idx]
                                if not isinstance(item, dict):
                                    continue
                                fn = item.get("function") if isinstance(item.get("function"), dict) else {}
                                args_raw = fn.get("arguments")
                                parsed_args = _parse_tool_arguments(args_raw, none_on_failure=True)
                                tool_calls_out.append(
                                    {
                                        "id": item.get("id") or None,
                                        "type": item.get("type") or "function",
                                        "function": {
                                            "name": fn.get("name") or "",
                                            "arguments": args_raw or "",
                                            "parsedArguments": parsed_args,
                                        },
                                    }
                                )

                            _raw_response_text = "".join(stream_state["text_parts"]).strip()
                            # Strip literal <ctrlNN> escape sequences leaked by some providers.
                            response_text = re.sub(r"<ctrl\d+>", "", _raw_response_text).strip() or None
                            _write_chat_audit_event(
                                app_settings,
                                endpoint="/chat/completions",
                                status=200,
                                started_at=started,
                                session_id=audit_session_id,
                                request_id=request_id,
                                stream=True,
                                usedProvider=provider,
                                usedModel=agent.model,
                                attempts=attempts,
                                switchLog=switch_log,
                                requestTools=_extract_request_tool_names(upstream_payload),
                                requestToolResults=_extract_request_tool_results(upstream_payload),
                                toolChoice=upstream_payload.get("tool_choice"),
                                responseToolCalls=tool_calls_out or None,
                                responseText=response_text,
                                streamStats={
                                    "chunks": stream_state["chunks"],
                                    "bytes": stream_state["bytes"],
                                },
                                requestPayloadTokenEstimate=request_payload_token_estimate,
                                tokenBudget=token_budget_info,
                                upstreamUsage=stream_state.get("upstream_usage"),
                                requestPayload=_audit_payload_for_log(app_settings, upstream_payload),
                                upstreamRetryTrace=retry_trace or None,
                                qmapContext=_audit_context_for_log(app_settings, x_qmap_context),
                            )

                        if provider == "openrouter":
                            response = await _stream_openrouter_chat_completions_via_openai_sdk(
                                async_openai_cls=AsyncOpenAI,
                                base_url=base,
                                api_key=str(api_key or ""),
                                payload=upstream_payload,
                                timeout_seconds=timeout_seconds,
                                retry_attempts=app_settings.upstream_retry_attempts,
                                retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                                retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                                retry_jitter_ratio=app_settings.upstream_retry_jitter_ratio,
                                retry_trace=retry_trace,
                                default_headers=openrouter_headers,
                                on_chunk=_consume_stream_chunk,
                                on_complete=_finalize_stream_audit,
                            )
                        elif provider == "openai":
                            response = await _stream_openai_chat_completions_via_openai_sdk(
                                async_openai_cls=AsyncOpenAI,
                                base_url=base,
                                api_key=str(api_key or ""),
                                payload=upstream_payload,
                                timeout_seconds=timeout_seconds,
                                retry_attempts=app_settings.upstream_retry_attempts,
                                retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                                retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                                retry_jitter_ratio=app_settings.upstream_retry_jitter_ratio,
                                retry_trace=retry_trace,
                                on_chunk=_consume_stream_chunk,
                                on_complete=_finalize_stream_audit,
                            )
                        else:
                            response = await _stream_proxy(
                                upstream_url,
                                headers,
                                upstream_payload,
                                timeout_seconds,
                                retry_attempts=app_settings.upstream_retry_attempts,
                                retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                                retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                                retry_jitter_ratio=app_settings.upstream_retry_jitter_ratio,
                                retry_trace=retry_trace,
                                on_chunk=_consume_stream_chunk,
                                on_complete=_finalize_stream_audit,
                            )
                        response.headers["x-q-assistant-provider"] = provider
                        response.headers["x-q-assistant-model"] = str(agent.model or "")
                        _set_trace_headers(response.headers, request_id, audit_session_id)
                        if switch_log:
                            response.headers["x-q-assistant-switch"] = " | ".join(switch_log)
                        if runtime_policy_summary:
                            response.headers[_RUNTIME_POLICY_SUMMARY_HEADER] = runtime_policy_summary

                        prelude_chunk = _build_openai_stream_request_id_chunk(request_id, agent.model)

                        async def _prepend_request_id_chunk():
                            yield prelude_chunk
                            async for chunk in _sanitize_openai_stream_chunks(response.body_iterator):
                                yield chunk

                        forwarded_headers: dict[str, str] = {
                            "cache-control": "no-cache",
                            "x-q-assistant-provider": provider,
                            "x-q-assistant-model": str(agent.model or ""),
                            _REQUEST_ID_HEADER: request_id,
                        }
                        if audit_session_id:
                            forwarded_headers[_CHAT_ID_HEADER] = audit_session_id
                        if switch_log:
                            forwarded_headers["x-q-assistant-switch"] = " | ".join(switch_log)
                        if runtime_policy_summary:
                            forwarded_headers[_RUNTIME_POLICY_SUMMARY_HEADER] = runtime_policy_summary
                        return StreamingResponse(
                            _prepend_request_id_chunk(),
                            status_code=response.status_code,
                            media_type="text/event-stream",
                            headers=forwarded_headers,
                        )

                    if provider == "openrouter":
                        body = await _post_openrouter_chat_completions_via_openai_sdk(
                            async_openai_cls=AsyncOpenAI,
                            base_url=base,
                            api_key=str(api_key or ""),
                            payload=upstream_payload,
                            timeout_seconds=timeout_seconds,
                            retry_attempts=app_settings.upstream_retry_attempts,
                            retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                            retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                            retry_jitter_ratio=app_settings.upstream_retry_jitter_ratio,
                            retry_trace=retry_trace,
                            default_headers=openrouter_headers,
                        )
                    elif provider == "openai":
                        body = await _post_openai_chat_completions_via_openai_sdk(
                            async_openai_cls=AsyncOpenAI,
                            base_url=base,
                            api_key=str(api_key or ""),
                            payload=upstream_payload,
                            timeout_seconds=timeout_seconds,
                            retry_attempts=app_settings.upstream_retry_attempts,
                            retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                            retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                            retry_jitter_ratio=app_settings.upstream_retry_jitter_ratio,
                            retry_trace=retry_trace,
                        )
                    else:
                        body = await _post_json(
                            client,
                            upstream_url,
                            headers,
                            upstream_payload,
                            retry_attempts=app_settings.upstream_retry_attempts,
                            retry_base_delay_seconds=app_settings.upstream_retry_base_delay_seconds,
                            retry_max_delay_seconds=app_settings.upstream_retry_max_delay_seconds,
                            retry_jitter_ratio=app_settings.upstream_retry_jitter_ratio,
                            retry_trace=retry_trace,
                        )
                    request_tool_results = _extract_request_tool_results(upstream_payload)
                    upstream_usage = None
                    response_tool_calls: list[dict[str, Any]] | list[str] = []
                    response_text = ""
                    if isinstance(body, dict):
                        upstream_usage = _extract_upstream_usage(body)
                        body = _repair_openai_response_tool_call_arguments(body)
                        body = _repair_qmap_validation_tool_call_arguments(
                            body,
                            request_tool_results=request_tool_results,
                        )
                        body = _repair_qmap_metric_tool_call_arguments(
                            body,
                            request_tool_results=request_tool_results,
                        )
                        body = _normalize_openai_response_final_text(
                            body,
                            objective_text=_extract_prompt_from_messages(upstream_payload.get("messages")),
                        )
                        response_message = {}
                        choices = body.get("choices")
                        if isinstance(choices, list) and choices:
                            first_choice = choices[0] if isinstance(choices[0], dict) else {}
                            response_message = (
                                first_choice.get("message")
                                if isinstance(first_choice.get("message"), dict)
                                else {}
                            )
                        response_text = _extract_message_text(response_message)
                        response_tool_calls = _extract_response_tool_calls(body)
                        quality_metrics = _derive_runtime_quality_metrics(
                            request_tool_results,
                            response_tool_calls,
                            response_text,
                            upstream_payload,
                        )
                        body["qAssistant"] = {
                            "usedProvider": provider,
                            "usedModel": agent.model,
                            "switchLog": switch_log,
                            "requestId": request_id,
                            "chatId": audit_session_id,
                            "upstreamUsage": upstream_usage,
                            "requestPayloadTokenEstimate": request_payload_token_estimate,
                            "tokenBudget": token_budget_info,
                            "qualityMetrics": quality_metrics,
                        }
                    if runtime_policy_summary:
                        response.headers[_RUNTIME_POLICY_SUMMARY_HEADER] = runtime_policy_summary
                    _write_chat_audit_event(
                        app_settings,
                        endpoint="/chat/completions",
                        status=200,
                        started_at=started,
                        session_id=audit_session_id,
                        request_id=request_id,
                        stream=False,
                        usedProvider=provider,
                        usedModel=agent.model,
                        attempts=attempts,
                        switchLog=switch_log,
                        requestTools=_extract_request_tool_names(upstream_payload),
                        requestToolResults=request_tool_results,
                        toolChoice=upstream_payload.get("tool_choice"),
                        responseToolCalls=response_tool_calls,
                        responseText=response_text,
                        requestPayloadTokenEstimate=request_payload_token_estimate,
                        tokenBudget=token_budget_info,
                        upstreamUsage=upstream_usage,
                        requestPayload=_audit_payload_for_log(app_settings, upstream_payload),
                        responsePayload=_audit_payload_for_log(app_settings, body),
                        upstreamRetryTrace=retry_trace or None,
                        qmapContext=_audit_context_for_log(app_settings, x_qmap_context),
                    )
                    return body
                except Exception as exc:
                    last_exc = exc
                    upstream_retry_traces.append(
                        {
                            "provider": provider,
                            "model": agent.model,
                            "trace": retry_trace,
                        }
                    )
                    attempts.append(f"{provider}:{agent.model} -> {exc}")
                    if index < len(agents) - 1:
                        switch_log.append(
                            f"Cambio agente: {provider}/{agent.model} non disponibile, provo il successivo."
                        )

        detail = "Nessun agente disponibile per soddisfare la richiesta."
        if attempts:
            detail = f"{detail} Tentativi: {' | '.join(attempts)}"
        if isinstance(last_exc, HTTPException) and _is_retryable_status(int(last_exc.status_code)):
            detail = (
                f"{detail} Errore temporaneo upstream ({int(last_exc.status_code)}). "
                "Riprova tra pochi secondi per riprendere il flusso dal passo corrente."
            )
        _write_chat_audit_event(
            app_settings,
            endpoint="/chat/completions",
            status=int(last_exc.status_code) if isinstance(last_exc, HTTPException) else 503,
            started_at=started,
            session_id=audit_session_id,
            request_id=request_id,
            stream=bool(base_payload.get("stream")),
            attempts=attempts,
            switchLog=switch_log,
            requestTools=_extract_request_tool_names(base_payload),
            requestToolResults=_extract_request_tool_results(base_payload),
            toolChoice=base_payload.get("tool_choice"),
            error=detail,
            requestPayloadTokenEstimate=base_payload_token_estimate,
            tokenBudget=token_budget_info,
            requestPayload=_audit_payload_for_log(app_settings, base_payload),
            upstreamRetryTrace=upstream_retry_traces or None,
            qmapContext=_audit_context_for_log(app_settings, x_qmap_context),
        )
        if isinstance(last_exc, HTTPException):
            raise HTTPException(status_code=last_exc.status_code, detail=detail)
        raise HTTPException(status_code=503, detail=detail)

    # Expose q-assistant as a real MCP server endpoint (HTTP transport at /mcp).
    mcp = FastApiMCP(
        app,
        include_operations=[
            "list_qmap_cloud_maps",
            "get_qmap_cloud_map",
            "build_load_cloud_map_action",
            "build_equals_filter_action",
            "qmap_cloud_status",
        ],
    )
    mcp.mount_http(mount_path="/mcp")

    return app


app = create_app()


def run() -> None:
    settings = load_settings()
    uvicorn.run("q_assistant.main:app", host=settings.host, port=settings.port, reload=True)


if __name__ == "__main__":
    run()
