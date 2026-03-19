import asyncio
import unittest

from q_assistant.provider_transport import sanitize_openai_stream_chunks as _sanitize_openai_stream_chunks


async def _iter_chunks(chunks):
    for chunk in chunks:
        yield chunk


async def _collect(chunks):
    out = []
    async for chunk in _sanitize_openai_stream_chunks(_iter_chunks(chunks)):
        out.append(chunk)
    return out


class OpenAIStreamNormalizationTests(unittest.TestCase):
    def test_filters_comment_events_and_keeps_data(self):
        chunks = [
            b": OPENROUTER PROCESSING\n\n",
            b"data: {\"id\":\"x\",\"choices\":[]}\n\n",
            b"data: [DONE]\n\n",
        ]
        out = asyncio.run(_collect(chunks))
        self.assertEqual(len(out), 2)
        self.assertTrue(out[0].startswith(b"data: "))
        self.assertEqual(out[1], b"data: [DONE]\n\n")

    def test_wraps_plain_json_payload_into_sse(self):
        chunks = [b"{\"id\":\"x\",\"object\":\"chat.completion\"}"]
        out = asyncio.run(_collect(chunks))
        self.assertEqual(len(out), 2)
        self.assertTrue(out[0].startswith(b"data: {"))
        self.assertEqual(out[1], b"data: [DONE]\n\n")

    def test_handles_split_sse_events_across_chunks(self):
        chunks = [
            b"data: {\"id\":\"x\",",
            b"\"choices\":[]}\n\n: keepalive\n\n",
            b"data: [DONE]\n\n",
        ]
        out = asyncio.run(_collect(chunks))
        self.assertEqual(len(out), 2)
        self.assertIn(b"\"id\":\"x\"", out[0])
        self.assertEqual(out[1], b"data: [DONE]\n\n")


if __name__ == "__main__":
    unittest.main()
