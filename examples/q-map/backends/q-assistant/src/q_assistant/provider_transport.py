from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncIterator, Awaitable, Callable

import httpx
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from .provider_retry import (
    _compute_retry_delay,
    _extract_error_message,
    _is_retryable_status,
)


async def close_async_resource(resource: Any) -> None:
    if resource is None:
        return
    close_fn = getattr(resource, "aclose", None)
    if callable(close_fn):
        maybe = close_fn()
        if asyncio.iscoroutine(maybe):
            await maybe
        return
    close_fn = getattr(resource, "close", None)
    if callable(close_fn):
        maybe = close_fn()
        if asyncio.iscoroutine(maybe):
            await maybe


def openai_sdk_model_to_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    model_dump = getattr(payload, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="json")
        if isinstance(dumped, dict):
            return dumped
    to_dict = getattr(payload, "to_dict", None)
    if callable(to_dict):
        dumped = to_dict()
        if isinstance(dumped, dict):
            return dumped
    as_dict = getattr(payload, "dict", None)
    if callable(as_dict):
        dumped = as_dict()
        if isinstance(dumped, dict):
            return dumped
    return {"message": str(payload)}


def openrouter_sdk_exception_status_code(exc: Exception) -> int | None:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return int(status_code)
    response = getattr(exc, "response", None)
    if response is None:
        return None
    code = getattr(response, "status_code", None)
    if isinstance(code, int):
        return int(code)
    return None


def openrouter_sdk_exception_detail(exc: Exception) -> str:
    body = getattr(exc, "body", None)
    if isinstance(body, dict):
        detail = _extract_error_message(body)
        if detail:
            return detail
    response = getattr(exc, "response", None)
    if response is not None:
        json_fn = getattr(response, "json", None)
        if callable(json_fn):
            try:
                parsed = json_fn()
                detail = _extract_error_message(parsed)
                if detail:
                    return detail
            except Exception:
                pass
        text_value = getattr(response, "text", None)
        if isinstance(text_value, str) and text_value.strip():
            return text_value.strip()
    return str(exc)


async def post_openrouter_chat_completions_via_openai_sdk(
    *,
    async_openai_cls: Any,
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
    timeout_seconds: float,
    retry_attempts: int = 0,
    retry_base_delay_seconds: float = 1.0,
    retry_max_delay_seconds: float = 8.0,
    retry_jitter_ratio: float = 0.0,
    retry_trace: list[dict[str, Any]] | None = None,
    default_headers: dict[str, str] | None = None,
    provider_label: str = "openrouter",
) -> dict[str, Any]:
    if async_openai_cls is None:
        raise HTTPException(
            status_code=503,
            detail=f"OpenAI SDK dependency is missing for {provider_label} provider.",
        )

    attempts = max(1, int(retry_attempts) + 1)
    for attempt in range(1, attempts + 1):
        start_attempt = time.perf_counter()
        sdk_client = async_openai_cls(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout_seconds,
            max_retries=0,
            default_headers=default_headers or None,
        )
        try:
            response = await sdk_client.chat.completions.create(**payload)
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "response",
                        "status": 200,
                        "durationMs": round((time.perf_counter() - start_attempt) * 1000, 1),
                    }
                )
            return openai_sdk_model_to_dict(response)
        except Exception as exc:
            if isinstance(exc, (TypeError, ValueError)):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid {provider_label} chat payload: {exc}",
                ) from exc
            status_code = openrouter_sdk_exception_status_code(exc)
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "response" if status_code is not None else "network-error",
                        "status": int(status_code) if isinstance(status_code, int) else None,
                        "errorType": type(exc).__name__,
                        "error": str(exc),
                        "durationMs": round((time.perf_counter() - start_attempt) * 1000, 1),
                    }
                )

            retryable = status_code is None or _is_retryable_status(int(status_code))
            if retryable and attempt < attempts:
                delay = _compute_retry_delay(
                    attempt,
                    base_delay_seconds=retry_base_delay_seconds,
                    max_delay_seconds=retry_max_delay_seconds,
                    jitter_ratio=retry_jitter_ratio,
                )
                if isinstance(retry_trace, list):
                    retry_trace.append(
                        {
                            "attempt": attempt,
                            "phase": "sleep-before-retry",
                            "sleepMs": round(delay * 1000, 1),
                            "reason": "network-error" if status_code is None else f"http-{status_code}",
                        }
                    )
                await asyncio.sleep(delay)
                continue

            detail = openrouter_sdk_exception_detail(exc)
            if status_code is None:
                raise HTTPException(status_code=503, detail=f"Upstream network error: {detail}") from exc
            raise HTTPException(status_code=int(status_code), detail=detail or "Upstream request failed") from exc
        finally:
            await close_async_resource(sdk_client)

    raise HTTPException(status_code=503, detail="Upstream request failed after retries")


async def stream_openrouter_chat_completions_via_openai_sdk(
    *,
    async_openai_cls: Any,
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
    timeout_seconds: float,
    retry_attempts: int = 0,
    retry_base_delay_seconds: float = 1.0,
    retry_max_delay_seconds: float = 8.0,
    retry_jitter_ratio: float = 0.0,
    retry_trace: list[dict[str, Any]] | None = None,
    default_headers: dict[str, str] | None = None,
    on_chunk: Callable[[bytes], Awaitable[None] | None] | None = None,
    on_complete: Callable[[], Awaitable[None] | None] | None = None,
    provider_label: str = "openrouter",
) -> StreamingResponse:
    if async_openai_cls is None:
        raise HTTPException(
            status_code=503,
            detail=f"OpenAI SDK dependency is missing for {provider_label} provider.",
        )

    attempts = max(1, int(retry_attempts) + 1)
    stream: Any = None
    sdk_client: Any = None
    stream_payload = dict(payload or {})
    stream_payload["stream"] = True

    for attempt in range(1, attempts + 1):
        start_attempt = time.perf_counter()
        sdk_client = async_openai_cls(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout_seconds,
            max_retries=0,
            default_headers=default_headers or None,
        )
        try:
            stream = await sdk_client.chat.completions.create(**stream_payload)
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "response",
                        "status": 200,
                        "durationMs": round((time.perf_counter() - start_attempt) * 1000, 1),
                    }
                )
            break
        except Exception as exc:
            if isinstance(exc, (TypeError, ValueError)):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid {provider_label} chat payload: {exc}",
                ) from exc
            status_code = openrouter_sdk_exception_status_code(exc)
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "response" if status_code is not None else "network-error",
                        "status": int(status_code) if isinstance(status_code, int) else None,
                        "errorType": type(exc).__name__,
                        "error": str(exc),
                        "durationMs": round((time.perf_counter() - start_attempt) * 1000, 1),
                    }
                )
            await close_async_resource(sdk_client)
            sdk_client = None

            retryable = status_code is None or _is_retryable_status(int(status_code))
            if retryable and attempt < attempts:
                delay = _compute_retry_delay(
                    attempt,
                    base_delay_seconds=retry_base_delay_seconds,
                    max_delay_seconds=retry_max_delay_seconds,
                    jitter_ratio=retry_jitter_ratio,
                )
                if isinstance(retry_trace, list):
                    retry_trace.append(
                        {
                            "attempt": attempt,
                            "phase": "sleep-before-retry",
                            "sleepMs": round(delay * 1000, 1),
                            "reason": "network-error" if status_code is None else f"http-{status_code}",
                        }
                    )
                await asyncio.sleep(delay)
                continue

            detail = openrouter_sdk_exception_detail(exc)
            if status_code is None:
                raise HTTPException(status_code=503, detail=f"Upstream network error: {detail}") from exc
            raise HTTPException(status_code=int(status_code), detail=detail or "Upstream request failed") from exc

    if stream is None:
        raise HTTPException(status_code=503, detail="Upstream request failed after retries")

    async def _iter_stream() -> AsyncIterator[bytes]:
        try:
            async for chunk in stream:
                chunk_dict = openai_sdk_model_to_dict(chunk)
                raw = f"data: {json.dumps(chunk_dict, ensure_ascii=False)}\n\n".encode("utf-8")
                if on_chunk is not None:
                    maybe = on_chunk(raw)
                    if asyncio.iscoroutine(maybe):
                        await maybe
                yield raw
            done = b"data: [DONE]\n\n"
            if on_chunk is not None:
                maybe = on_chunk(done)
                if asyncio.iscoroutine(maybe):
                    await maybe
            yield done
        finally:
            try:
                await close_async_resource(stream)
            finally:
                try:
                    if on_complete is not None:
                        maybe = on_complete()
                        if asyncio.iscoroutine(maybe):
                            await maybe
                finally:
                    await close_async_resource(sdk_client)

    return StreamingResponse(
        _iter_stream(),
        status_code=200,
        media_type="text/event-stream",
    )


async def post_openai_chat_completions_via_openai_sdk(
    *,
    async_openai_cls: Any,
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
    timeout_seconds: float,
    retry_attempts: int = 0,
    retry_base_delay_seconds: float = 1.0,
    retry_max_delay_seconds: float = 8.0,
    retry_jitter_ratio: float = 0.0,
    retry_trace: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return await post_openrouter_chat_completions_via_openai_sdk(
        async_openai_cls=async_openai_cls,
        base_url=base_url,
        api_key=api_key,
        payload=payload,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
        retry_base_delay_seconds=retry_base_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        retry_jitter_ratio=retry_jitter_ratio,
        retry_trace=retry_trace,
        default_headers=None,
        provider_label="openai",
    )


async def stream_openai_chat_completions_via_openai_sdk(
    *,
    async_openai_cls: Any,
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
    timeout_seconds: float,
    retry_attempts: int = 0,
    retry_base_delay_seconds: float = 1.0,
    retry_max_delay_seconds: float = 8.0,
    retry_jitter_ratio: float = 0.0,
    retry_trace: list[dict[str, Any]] | None = None,
    on_chunk: Callable[[bytes], Awaitable[None] | None] | None = None,
    on_complete: Callable[[], Awaitable[None] | None] | None = None,
) -> StreamingResponse:
    return await stream_openrouter_chat_completions_via_openai_sdk(
        async_openai_cls=async_openai_cls,
        base_url=base_url,
        api_key=api_key,
        payload=payload,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
        retry_base_delay_seconds=retry_base_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        retry_jitter_ratio=retry_jitter_ratio,
        retry_trace=retry_trace,
        default_headers=None,
        on_chunk=on_chunk,
        on_complete=on_complete,
        provider_label="openai",
    )


async def post_json(
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    *,
    retry_attempts: int = 0,
    retry_base_delay_seconds: float = 1.0,
    retry_max_delay_seconds: float = 8.0,
    retry_jitter_ratio: float = 0.0,
    retry_trace: list[dict[str, Any]] | None = None,
) -> Any:
    attempts = max(1, retry_attempts + 1)
    for attempt in range(1, attempts + 1):
        start_attempt = time.perf_counter()
        try:
            response = await client.post(url, headers=headers, json=payload)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "network-error",
                        "errorType": type(exc).__name__,
                        "error": str(exc),
                        "durationMs": round((time.perf_counter() - start_attempt) * 1000, 1),
                    }
                )
            if attempt < attempts:
                delay = _compute_retry_delay(
                    attempt,
                    base_delay_seconds=retry_base_delay_seconds,
                    max_delay_seconds=retry_max_delay_seconds,
                    jitter_ratio=retry_jitter_ratio,
                )
                if isinstance(retry_trace, list):
                    retry_trace.append(
                        {
                            "attempt": attempt,
                            "phase": "sleep-before-retry",
                            "sleepMs": round(delay * 1000, 1),
                            "reason": "network-error",
                        }
                    )
                await asyncio.sleep(delay)
                continue
            raise HTTPException(status_code=503, detail=f"Upstream network error: {exc!s}") from exc

        text = response.text
        if isinstance(retry_trace, list):
            retry_trace.append(
                {
                    "attempt": attempt,
                    "phase": "response",
                    "status": int(response.status_code),
                    "durationMs": round((time.perf_counter() - start_attempt) * 1000, 1),
                }
            )
        try:
            body = response.json()
        except Exception:
            body = {"message": text}

        if response.status_code < 400:
            return body

        if response.status_code in {429, 500, 502, 503, 504} and attempt < attempts:
            delay = _compute_retry_delay(
                attempt,
                base_delay_seconds=retry_base_delay_seconds,
                max_delay_seconds=retry_max_delay_seconds,
                jitter_ratio=retry_jitter_ratio,
            )
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "sleep-before-retry",
                        "sleepMs": round(delay * 1000, 1),
                        "reason": f"http-{response.status_code}",
                    }
                )
            await asyncio.sleep(delay)
            continue

        detail = ""
        if isinstance(body, dict):
            detail = _extract_error_message(body)
        else:
            detail = str(body)
        raise HTTPException(status_code=response.status_code, detail=detail or response.reason_phrase)

    raise HTTPException(status_code=503, detail="Upstream request failed after retries")


async def get_json(client: httpx.AsyncClient, url: str, headers: dict[str, str]) -> Any:
    response = await client.get(url, headers=headers)
    text = response.text
    try:
        body = response.json()
    except Exception:
        body = {"message": text}

    if response.status_code >= 400:
        detail = ""
        if isinstance(body, dict):
            detail = _extract_error_message(body)
        else:
            detail = str(body)
        raise HTTPException(status_code=response.status_code, detail=detail or response.reason_phrase)

    return body


async def stream_proxy(
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout_seconds: float,
    *,
    retry_attempts: int = 0,
    retry_base_delay_seconds: float = 1.5,
    retry_max_delay_seconds: float = 12.0,
    retry_jitter_ratio: float = 0.0,
    retry_trace: list[dict[str, Any]] | None = None,
    on_chunk: Callable[[bytes], Awaitable[None] | None] | None = None,
    on_complete: Callable[[], Awaitable[None] | None] | None = None,
) -> StreamingResponse:
    """
    Transparent streaming passthrough for provider SSE responses.
    """
    client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds))
    attempts = max(1, retry_attempts + 1)
    response: httpx.Response | None = None

    for attempt in range(1, attempts + 1):
        start_attempt = time.perf_counter()
        try:
            request = client.build_request("POST", url, headers=headers, json=payload)
            response = await client.send(request, stream=True)
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "response",
                        "status": int(response.status_code),
                        "durationMs": round((time.perf_counter() - start_attempt) * 1000, 1),
                    }
                )
        except (httpx.TimeoutException, httpx.TransportError):
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "network-error",
                        "durationMs": round((time.perf_counter() - start_attempt) * 1000, 1),
                    }
                )
            if attempt >= attempts:
                break
            delay = _compute_retry_delay(
                attempt,
                base_delay_seconds=retry_base_delay_seconds,
                max_delay_seconds=retry_max_delay_seconds,
                jitter_ratio=retry_jitter_ratio,
            )
            if isinstance(retry_trace, list):
                retry_trace.append(
                    {
                        "attempt": attempt,
                        "phase": "sleep-before-retry",
                        "sleepMs": round(delay * 1000, 1),
                        "reason": "network-error",
                    }
                )
            await asyncio.sleep(delay)
            continue

        retryable_status = response.status_code in {429, 500, 502, 503, 504}
        if not retryable_status or attempt >= attempts:
            break

        delay = _compute_retry_delay(
            attempt,
            base_delay_seconds=retry_base_delay_seconds,
            max_delay_seconds=retry_max_delay_seconds,
            jitter_ratio=retry_jitter_ratio,
        )
        if isinstance(retry_trace, list):
            retry_trace.append(
                {
                    "attempt": attempt,
                    "phase": "sleep-before-retry",
                    "sleepMs": round(delay * 1000, 1),
                    "reason": f"http-{response.status_code}",
                }
            )
        await response.aclose()
        await asyncio.sleep(delay)

    if response is None:
        await client.aclose()
        raise HTTPException(status_code=503, detail="Unable to create upstream stream response")

    if response.status_code >= 400:
        text = await response.aread()
        await response.aclose()
        await client.aclose()
        detail = text.decode("utf-8", errors="ignore") or response.reason_phrase
        raise HTTPException(status_code=response.status_code, detail=detail)
    content_type = response.headers.get("content-type", "text/event-stream")

    async def _iter_stream():
        try:
            async for chunk in response.aiter_bytes():
                if on_chunk is not None:
                    maybe = on_chunk(chunk)
                    if asyncio.iscoroutine(maybe):
                        await maybe
                yield chunk
        finally:
            try:
                if on_complete is not None:
                    maybe = on_complete()
                    if asyncio.iscoroutine(maybe):
                        await maybe
            finally:
                await response.aclose()
                await client.aclose()

    return StreamingResponse(
        _iter_stream(),
        status_code=response.status_code,
        media_type=content_type,
    )


async def sanitize_openai_stream_chunks(chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """
    Normalize upstream stream to OpenAI-compatible SSE:
    - remove non-data/comment events
    - preserve data events
    - wrap non-SSE JSON payloads into SSE envelope.
    """

    def _normalize_openai_sse_event(event_bytes: bytes) -> bytes | None:
        event = (event_bytes or b"").strip(b"\r\n")
        if not event.strip():
            return None

        lines = event.splitlines()
        kept: list[bytes] = []
        has_data = False
        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith(b"data:"):
                kept.append(line)
                has_data = True
                continue
            if line.startswith(b"event:") or line.startswith(b"id:") or line.startswith(b"retry:"):
                kept.append(line)
                continue
        if not has_data:
            return None
        return b"\n".join(kept) + b"\n\n"

    buffer = b""
    emitted_any_event = False
    async for chunk in chunks:
        if not chunk:
            continue
        buffer += chunk
        while b"\n\n" in buffer:
            raw_event, remainder = buffer.split(b"\n\n", 1)
            buffer = remainder
            normalized = _normalize_openai_sse_event(raw_event)
            if normalized:
                emitted_any_event = True
                yield normalized

    tail = buffer.strip()
    if tail:
        if tail.startswith(b"data:"):
            emitted_any_event = True
            yield tail + b"\n\n"
        elif not emitted_any_event:
            # Upstream replied with plain JSON despite stream=true.
            yield b"data: " + tail + b"\n\n"
            yield b"data: [DONE]\n\n"
