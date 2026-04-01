"""Tests for _filter_tool_calls_from_sse_chunk (tool_choice=none guard)."""

import json
import unittest

from q_assistant.main import _filter_tool_calls_from_sse_chunk


class FilterToolCallsFromSSEChunkTests(unittest.TestCase):
    """Verify that tool_call deltas are stripped from SSE chunks."""

    def _make_sse_chunk(self, data: dict) -> bytes:
        return f"data: {json.dumps(data)}\n\n".encode()

    def test_strips_tool_calls_from_delta(self):
        chunk = self._make_sse_chunk(
            {
                "id": "x",
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call_abc",
                                    "function": {"name": "someTool", "arguments": "{}"},
                                }
                            ]
                        }
                    }
                ],
            }
        )
        result = _filter_tool_calls_from_sse_chunk(chunk)
        self.assertIsNotNone(result)
        # Parse back the SSE data
        line = result.decode().strip()
        self.assertTrue(line.startswith("data: "))
        obj = json.loads(line[5:].strip())
        delta = obj["choices"][0]["delta"]
        self.assertNotIn("tool_calls", delta)

    def test_preserves_text_content(self):
        chunk = self._make_sse_chunk(
            {
                "id": "x",
                "choices": [{"delta": {"content": "Hello world"}}],
            }
        )
        result = _filter_tool_calls_from_sse_chunk(chunk)
        self.assertIsNotNone(result)
        obj = json.loads(result.decode().strip()[5:].strip())
        self.assertEqual(obj["choices"][0]["delta"]["content"], "Hello world")

    def test_preserves_done_event(self):
        chunk = b"data: [DONE]\n\n"
        result = _filter_tool_calls_from_sse_chunk(chunk)
        self.assertIsNotNone(result)
        self.assertIn(b"[DONE]", result)

    def test_mixed_text_and_tool_calls(self):
        chunk = self._make_sse_chunk(
            {
                "id": "x",
                "choices": [
                    {
                        "delta": {
                            "content": "Here is my response",
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "function": {"name": "badTool", "arguments": ""},
                                }
                            ],
                        }
                    }
                ],
            }
        )
        result = _filter_tool_calls_from_sse_chunk(chunk)
        self.assertIsNotNone(result)
        obj = json.loads(result.decode().strip()[5:].strip())
        delta = obj["choices"][0]["delta"]
        self.assertEqual(delta["content"], "Here is my response")
        self.assertNotIn("tool_calls", delta)

    def test_empty_chunk_returns_none(self):
        result = _filter_tool_calls_from_sse_chunk(b"")
        self.assertIsNone(result)

    def test_none_chunk_returns_none(self):
        result = _filter_tool_calls_from_sse_chunk(None)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
