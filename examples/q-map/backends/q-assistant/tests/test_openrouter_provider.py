import asyncio
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from q_assistant.config import load_settings
from q_assistant.provider_transport import (
    post_openai_chat_completions_via_openai_sdk as _post_openai_chat_completions_via_openai_sdk,
    post_openrouter_chat_completions_via_openai_sdk as _post_openrouter_chat_completions_via_openai_sdk,
    stream_openai_chat_completions_via_openai_sdk as _stream_openai_chat_completions_via_openai_sdk,
    stream_openrouter_chat_completions_via_openai_sdk as _stream_openrouter_chat_completions_via_openai_sdk,
)
from q_assistant.provider_retry import _provider_api_key


class _FakeChunk:
    def __init__(self, payload):
        self._payload = payload

    def model_dump(self, mode="json"):  # pragma: no cover - signature compatibility only
        return dict(self._payload)


class _FakeStream:
    def __init__(self, payloads):
        self._payloads = [_FakeChunk(payload) for payload in payloads]
        self._index = 0
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._payloads):
            raise StopAsyncIteration
        item = self._payloads[self._index]
        self._index += 1
        return item

    async def aclose(self):
        self.closed = True


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def model_dump(self, mode="json"):
        return dict(self._payload)


class _FakeAPIError(Exception):
    def __init__(self, status_code=None, body=None, message="fake error"):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class _FakeOpenAIClient:
    def __init__(self, behavior, kwargs):
        self._behavior = behavior
        self.kwargs = dict(kwargs)
        self.closed = False
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    async def _create(self, **kwargs):
        return await self._behavior(kwargs)

    async def close(self):
        self.closed = True


class _FakeOpenAIFactory:
    def __init__(self, behaviors):
        self._behaviors = list(behaviors)
        self.instances = []

    def __call__(self, **kwargs):
        if not self._behaviors:
            raise AssertionError("No fake behavior left for AsyncOpenAI constructor")
        behavior = self._behaviors.pop(0)
        client = _FakeOpenAIClient(behavior=behavior, kwargs=kwargs)
        self.instances.append(client)
        return client


async def _collect_stream_chunks(streaming_response):
    chunks = []
    async for chunk in streaming_response.body_iterator:
        chunks.append(chunk)
    return chunks


class OpenRouterProviderTests(unittest.TestCase):
    def test_provider_api_key_reads_openrouter_fallback(self):
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "or-key"}, clear=False):
            self.assertEqual(_provider_api_key("openrouter", ""), "or-key")

    def test_load_settings_openrouter_defaults(self):
        env = {
            "Q_ASSISTANT_PROVIDER": "openrouter",
            "Q_ASSISTANT_API_KEY": "",
            "OPENROUTER_API_KEY": "or-key",
            "Q_ASSISTANT_MODEL": "",
            "Q_ASSISTANT_BASE_URL": "",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = load_settings()
        self.assertEqual(settings.default_provider, "openrouter")
        self.assertEqual(settings.default_api_key, "or-key")
        self.assertEqual(settings.default_base_url, "https://openrouter.ai/api/v1")
        self.assertEqual(settings.default_model, "google/gemini-3-flash-preview")

    def test_load_settings_project_defaults_without_env(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = load_settings()
        self.assertEqual(settings.default_provider, "openrouter")
        self.assertEqual(settings.default_base_url, "https://openrouter.ai/api/v1")
        self.assertEqual(settings.default_model, "google/gemini-3-flash-preview")

    def test_openrouter_sdk_non_stream_success(self):
        async def _ok_behavior(kwargs):
            self.assertEqual(kwargs["model"], "google/gemini-3-flash-preview")
            self.assertFalse(kwargs.get("stream", False))
            return _FakeResponse({"id": "resp-1", "choices": [{"message": {"content": "ok"}}]})

        factory = _FakeOpenAIFactory([_ok_behavior])
        body = asyncio.run(
            _post_openrouter_chat_completions_via_openai_sdk(
                async_openai_cls=factory,
                base_url="https://openrouter.ai/api/v1",
                api_key="or-key",
                payload={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": "ciao"}]},
                timeout_seconds=15,
                default_headers={"X-Title": "q-map"},
            )
        )
        self.assertEqual(body.get("id"), "resp-1")
        self.assertEqual(len(factory.instances), 1)
        self.assertTrue(factory.instances[0].closed)

    def test_openrouter_sdk_non_stream_retries_on_429(self):
        async def _rate_limited(_kwargs):
            raise _FakeAPIError(status_code=429, body={"error": {"message": "rate limit"}})

        async def _ok_behavior(_kwargs):
            return _FakeResponse({"id": "resp-2", "choices": [{"message": {"content": "ok"}}]})

        factory = _FakeOpenAIFactory([_rate_limited, _ok_behavior])
        retry_trace = []
        body = asyncio.run(
            _post_openrouter_chat_completions_via_openai_sdk(
                async_openai_cls=factory,
                base_url="https://openrouter.ai/api/v1",
                api_key="or-key",
                payload={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": "ciao"}]},
                timeout_seconds=15,
                retry_attempts=1,
                retry_base_delay_seconds=0,
                retry_max_delay_seconds=0,
                retry_trace=retry_trace,
            )
        )
        self.assertEqual(body.get("id"), "resp-2")
        self.assertEqual(len(factory.instances), 2)
        self.assertTrue(all(client.closed for client in factory.instances))
        self.assertTrue(any(item.get("status") == 429 for item in retry_trace))
        self.assertTrue(any(item.get("phase") == "sleep-before-retry" for item in retry_trace))

    def test_openrouter_sdk_stream_emits_sse_chunks_and_done(self):
        async def _stream_behavior(kwargs):
            self.assertTrue(kwargs.get("stream"))
            return _FakeStream(
                [
                    {"id": "chunk-1", "choices": [{"delta": {"content": "ciao"}}]},
                    {"id": "chunk-2", "choices": [{"delta": {"content": "mondo"}}]},
                ]
            )

        factory = _FakeOpenAIFactory([_stream_behavior])
        seen = {"chunks": 0, "completed": False}

        def _on_chunk(_chunk: bytes):
            seen["chunks"] += 1

        def _on_complete():
            seen["completed"] = True

        response = asyncio.run(
            _stream_openrouter_chat_completions_via_openai_sdk(
                async_openai_cls=factory,
                base_url="https://openrouter.ai/api/v1",
                api_key="or-key",
                payload={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": "ciao"}]},
                timeout_seconds=15,
                on_chunk=_on_chunk,
                on_complete=_on_complete,
            )
        )
        chunks = asyncio.run(_collect_stream_chunks(response))

        self.assertGreaterEqual(len(chunks), 3)
        self.assertTrue(chunks[0].startswith(b"data: {"))
        self.assertEqual(chunks[-1], b"data: [DONE]\n\n")
        self.assertTrue(seen["completed"])
        self.assertGreaterEqual(seen["chunks"], 3)
        self.assertEqual(len(factory.instances), 1)
        self.assertTrue(factory.instances[0].closed)

    def test_openrouter_sdk_invalid_payload_returns_400(self):
        async def _bad_payload(_kwargs):
            raise TypeError("unexpected keyword argument: foo")

        factory = _FakeOpenAIFactory([_bad_payload])
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(
                _post_openrouter_chat_completions_via_openai_sdk(
                    async_openai_cls=factory,
                    base_url="https://openrouter.ai/api/v1",
                    api_key="or-key",
                    payload={"model": "google/gemini-3-flash-preview", "foo": "bar"},
                    timeout_seconds=15,
                    retry_attempts=2,
                )
            )
        self.assertEqual(ctx.exception.status_code, 400)

    def test_openrouter_sdk_missing_dependency_returns_503(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(
                _post_openrouter_chat_completions_via_openai_sdk(
                    async_openai_cls=None,
                    base_url="https://openrouter.ai/api/v1",
                    api_key="or-key",
                    payload={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": "ciao"}]},
                    timeout_seconds=15,
                )
            )
        self.assertEqual(ctx.exception.status_code, 503)

    def test_openai_sdk_non_stream_success(self):
        async def _ok_behavior(kwargs):
            self.assertEqual(kwargs["model"], "gpt-4o")
            self.assertFalse(kwargs.get("stream", False))
            return _FakeResponse({"id": "openai-1", "choices": [{"message": {"content": "ok"}}]})

        factory = _FakeOpenAIFactory([_ok_behavior])
        body = asyncio.run(
            _post_openai_chat_completions_via_openai_sdk(
                async_openai_cls=factory,
                base_url="https://api.openai.com/v1",
                api_key="oa-key",
                payload={"model": "gpt-4o", "messages": [{"role": "user", "content": "ciao"}]},
                timeout_seconds=15,
            )
        )
        self.assertEqual(body.get("id"), "openai-1")
        self.assertEqual(len(factory.instances), 1)
        self.assertIsNone(factory.instances[0].kwargs.get("default_headers"))
        self.assertTrue(factory.instances[0].closed)

    def test_openai_sdk_stream_emits_sse_chunks_and_done(self):
        async def _stream_behavior(kwargs):
            self.assertTrue(kwargs.get("stream"))
            return _FakeStream([{"id": "chunk-1", "choices": [{"delta": {"content": "ciao"}}]}])

        factory = _FakeOpenAIFactory([_stream_behavior])
        response = asyncio.run(
            _stream_openai_chat_completions_via_openai_sdk(
                async_openai_cls=factory,
                base_url="https://api.openai.com/v1",
                api_key="oa-key",
                payload={"model": "gpt-4o", "messages": [{"role": "user", "content": "ciao"}]},
                timeout_seconds=15,
            )
        )
        chunks = asyncio.run(_collect_stream_chunks(response))

        self.assertGreaterEqual(len(chunks), 2)
        self.assertTrue(chunks[0].startswith(b"data: {"))
        self.assertEqual(chunks[-1], b"data: [DONE]\n\n")
        self.assertTrue(factory.instances[0].closed)


if __name__ == "__main__":
    unittest.main()
