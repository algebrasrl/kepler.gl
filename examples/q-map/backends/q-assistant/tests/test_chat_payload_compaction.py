import json
import unittest

from q_assistant.chat_payload_compaction import (
    _compact_chat_completions_payload,
    _compact_openai_tool_schema,
    _compact_tool_message_content,
    _deduplicate_discovery_tool_turns,
    _repair_openai_tool_message_sequence,
    _sanitize_openai_tools_for_gemini_model,
)
from q_assistant.openai_chat_payload import _coerce_openai_chat_payload


class ChatPayloadCompactionTests(unittest.TestCase):
    def test_compact_tool_message_content_keeps_dataset_summary(self):
        payload = {
            "qmapToolResult": {
                "schema": "qmap.tool_result.v1",
                "success": True,
                "details": "dettaglio " + ("x" * 1000),
            },
            "llmResult": {
                "success": True,
                "providerId": "local-assets-it",
                "datasetId": "kontur-boundaries-italia",
                "datasetName": "Kontur Boundaries Italia (query) [abc123]",
                "returned": 563,
                "totalMatched": 563,
            },
        }
        compacted = _compact_tool_message_content(json.dumps(payload), max_chars=900)
        parsed = json.loads(compacted)
        self.assertIn("qmapToolResult", parsed)
        llm_section = parsed.get("llmResultSummary") if isinstance(parsed.get("llmResultSummary"), dict) else parsed.get("llmResult")
        self.assertIsInstance(llm_section, dict)
        self.assertEqual(llm_section.get("datasetId"), "kontur-boundaries-italia")

    def test_compact_tool_schema_drops_required_fields_not_in_properties(self):
        tool = {
            "type": "function",
            "function": {
                "name": "queryQCumberDataset",
                "description": "test",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "filters": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {"type": "string"},
                                },
                                "required": ["field", "op"],
                            },
                        }
                    },
                    "required": ["filters", "datasetName"],
                },
            },
        }
        compacted = _compact_openai_tool_schema(tool, aggressive=True)
        params = compacted["function"]["parameters"]
        self.assertEqual(params.get("required"), ["filters"])
        self.assertEqual(params["properties"]["filters"]["items"].get("required"), ["field"])

    def test_coerce_payload_strips_request_id_marker_from_assistant_string_content(self):
        payload = {
            "messages": [
                {"role": "user", "content": "ciao"},
                {
                    "role": "assistant",
                    "content": "[requestId: abc123]\n",
                    "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "x", "arguments": "{}"}}],
                },
            ]
        }
        coerced = _coerce_openai_chat_payload(payload)
        messages = coerced.get("messages", [])
        self.assertEqual(messages[1].get("content"), "")
        self.assertEqual(messages[1].get("tool_calls")[0]["id"], "call_1")

    def test_coerce_payload_strips_request_id_marker_from_assistant_parts_content(self):
        payload = {
            "messages": [
                {"role": "user", "content": "ciao"},
                {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "[requestId: xyz987]\npasso successivo"}],
                },
            ]
        }
        coerced = _coerce_openai_chat_payload(payload)
        messages = coerced.get("messages", [])
        parts = messages[1].get("content")
        self.assertIsInstance(parts, list)
        self.assertEqual(parts[0].get("text"), "passo successivo")

    def test_coerce_payload_normalizes_tool_required_fields_against_properties(self):
        payload = {
            "messages": [{"role": "user", "content": "ciao"}],
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "queryX",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "filters": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "field": {"type": "string"},
                                        },
                                        "required": ["field", "op"],
                                    },
                                }
                            },
                            "required": ["filters", "datasetName"],
                        },
                    },
                }
            ],
        }
        coerced = _coerce_openai_chat_payload(payload)
        tools = coerced.get("tools")
        self.assertIsInstance(tools, list)
        params = tools[0]["function"]["parameters"]
        self.assertEqual(params.get("required"), ["filters"])
        self.assertEqual(params["properties"]["filters"]["items"].get("required"), ["field"])

    def test_sanitize_openai_tools_for_gemini_repairs_required(self):
        payload = {
            "model": "google/gemini-3-flash-preview",
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "queryX",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "filters": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {"field": {"type": "string"}},
                                        "required": ["field", "op"],
                                    },
                                }
                            },
                            "required": ["filters", "datasetName"],
                        },
                    },
                }
            ],
        }
        sanitized = _sanitize_openai_tools_for_gemini_model(payload, model_hint=payload["model"])
        params = sanitized["tools"][0]["function"]["parameters"]
        self.assertEqual(params.get("required"), ["filters"])
        self.assertEqual(params["properties"]["filters"]["items"].get("required"), ["field"])

    def test_keeps_long_system_message_untruncated(self):
        long_system = "S" * 10000
        payload = {
            "messages": [
                {"role": "system", "content": long_system},
                {"role": "user", "content": "carica perimetro lombardia"},
            ]
        }

        compacted = _compact_chat_completions_payload(payload)
        messages = compacted.get("messages", [])
        self.assertTrue(messages)
        system_messages = [m for m in messages if m.get("role") == "system"]
        self.assertEqual(len(system_messages), 1)
        self.assertEqual(system_messages[0].get("content"), long_system)

    def test_drops_assistant_tool_calls_without_tool_response(self):
        messages = [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "u1"},
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "call_missing", "type": "function", "function": {"name": "x", "arguments": "{}"}}
                ],
            },
            {"role": "user", "content": "u2"},
        ]

        repaired = _repair_openai_tool_message_sequence(messages)
        self.assertEqual([m.get("role") for m in repaired], ["system", "user", "user"])

    def test_keeps_only_tool_calls_with_matching_tool_messages(self):
        messages = [
            {"role": "user", "content": "start"},
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "call_a", "type": "function", "function": {"name": "a", "arguments": "{}"}},
                    {"id": "call_b", "type": "function", "function": {"name": "b", "arguments": "{}"}},
                ],
            },
            {"role": "tool", "tool_call_id": "call_a", "content": "{\"ok\":true}"},
            {"role": "assistant", "content": "done"},
        ]

        repaired = _repair_openai_tool_message_sequence(messages)
        # With incomplete tool responses (call_b missing), the whole tool-call turn is dropped.
        self.assertEqual(repaired, [{"role": "user", "content": "start"}, {"role": "assistant", "content": "done"}])

    def test_keeps_tool_message_content_uncompacted_for_model_reasoning(self):
        payload = {
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "u1"},
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call_q",
                            "type": "function",
                            "function": {"name": "queryQCumberDataset", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_q",
                    "content": '{"qmapToolResult":{"schema":"qmap.tool_result.v1","success":true,'
                    '"details":"'
                    + ("x" * 5000)
                    + '"},"llmResult":{"success":true,"details":"ok","rows":['
                    + ",".join(['{"a":1}'] * 200)
                    + "]}}",
                },
            ]
        }
        compacted = _compact_chat_completions_payload(payload)
        tool_messages = [m for m in compacted.get("messages", []) if m.get("role") == "tool"]
        self.assertEqual(len(tool_messages), 1)
        tool_content = tool_messages[0].get("content")
        self.assertIsInstance(tool_content, str)
        self.assertGreater(len(tool_content), 1200)
        parsed = json.loads(tool_content)
        self.assertEqual(parsed.get("qmapToolResult", {}).get("schema"), "qmap.tool_result.v1")
        self.assertEqual(len(parsed.get("llmResult", {}).get("rows", [])), 200)
        self.assertIn("success", parsed.get("qmapToolResult", {}))

    def test_preserves_provider_ids_in_compacted_tool_results(self):
        payload = {
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "u1"},
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call_p",
                            "type": "function",
                            "function": {"name": "listQCumberProviders", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_p",
                    "content": json.dumps(
                        {
                            "qmapToolResult": {
                                "schema": "qmap.tool_result.v1",
                                "toolName": "listQCumberProviders",
                                "success": True,
                                "details": "Found 1 data providers.",
                            },
                            "llmResult": {
                                "success": True,
                                "providers": [
                                    {
                                        "id": "local-assets-it",
                                        "name": "Local Assets IT",
                                        "locale": "it",
                                        "category": "local",
                                        "apiType": "local",
                                        "apiBaseUrl": "http://q-cumber:3002",
                                        "capabilities": ["query"],
                                        "formats": ["geojson"],
                                        "tags": ["controllo"],
                                        "routingHint": "prefer territorial",
                                        "helperTools": ["listQCumberDatasets", "getQCumberDatasetHelp"],
                                    }
                                ],
                                "details": "Found 1 data providers.",
                            },
                        }
                    ),
                },
            ]
        }

        compacted = _compact_chat_completions_payload(payload)
        tool_messages = [m for m in compacted.get("messages", []) if m.get("role") == "tool"]
        self.assertEqual(len(tool_messages), 1)
        parsed = json.loads(tool_messages[0].get("content"))
        providers = parsed.get("llmResult", {}).get("providers")
        self.assertIsInstance(providers, list)
        self.assertTrue(providers)
        self.assertEqual(providers[0].get("id"), "local-assets-it")

    def test_compaction_preserves_latest_user_anchor_with_long_tool_chain(self):
        messages = [{"role": "system", "content": "sys"}, {"role": "user", "content": "obiettivo iniziale"}]
        for idx in range(1, 12):
            call_id = f"call_{idx}"
            messages.append(
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{"id": call_id, "type": "function", "function": {"name": "x", "arguments": "{}"}}],
                }
            )
            messages.append({"role": "tool", "tool_call_id": call_id, "content": "{\"ok\":true}"})

        compacted = _compact_chat_completions_payload(
            {"messages": messages},
            max_messages=8,
            max_tool_messages=6,
        )
        compacted_messages = compacted.get("messages", [])
        roles = [str(m.get("role")) for m in compacted_messages]
        self.assertIn("user", roles)
        first_non_system = next((m for m in compacted_messages if str(m.get("role")) != "system"), None)
        self.assertIsNotNone(first_non_system)
        self.assertEqual(first_non_system.get("role"), "user")

    def test_deduplicate_discovery_turns_removes_older_success_duplicates(self):
        discovery_payload = json.dumps(
            {
                "qmapToolResult": {
                    "schema": "qmap.tool_result.v1",
                    "toolName": "listQCumberDatasets",
                    "success": True,
                    "details": "Found 2 datasets.",
                },
                "llmResult": {
                    "success": True,
                    "details": "Found 2 datasets.",
                    "datasets": [{"id": "a"}, {"id": "b"}],
                },
            }
        )
        payload = {
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "u1"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_d1",
                            "type": "function",
                            "function": {"name": "listQCumberDatasets", "arguments": '{"providerId":"local-assets-it"}'},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_d1", "content": discovery_payload},
                {"role": "assistant", "content": "ok"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_d2",
                            "type": "function",
                            "function": {"name": "listQCumberDatasets", "arguments": '{"providerId":"local-assets-it"}'},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_d2", "content": discovery_payload},
            ]
        }

        out = _deduplicate_discovery_tool_turns(payload)
        messages = out.get("messages", [])
        tool_call_ids = [m.get("tool_call_id") for m in messages if isinstance(m, dict) and m.get("role") == "tool"]
        self.assertEqual(tool_call_ids, ["call_d2"])
        assistant_call_ids = []
        for message in messages:
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            for call in message.get("tool_calls", []) or []:
                if isinstance(call, dict):
                    assistant_call_ids.append(call.get("id"))
        self.assertIn("call_d2", assistant_call_ids)
        self.assertNotIn("call_d1", assistant_call_ids)

    def test_deduplicate_discovery_turns_keeps_failed_discovery(self):
        payload = {
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "u1"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_fail",
                            "type": "function",
                            "function": {"name": "listQCumberDatasets", "arguments": '{"providerId":"x"}'},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_fail",
                    "content": json.dumps(
                        {
                            "qmapToolResult": {
                                "schema": "qmap.tool_result.v1",
                                "success": False,
                                "details": "Invalid providerId.",
                            }
                        }
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_ok",
                            "type": "function",
                            "function": {"name": "listQCumberDatasets", "arguments": '{"providerId":"local-assets-it"}'},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_ok",
                    "content": json.dumps(
                        {
                            "qmapToolResult": {
                                "schema": "qmap.tool_result.v1",
                                "success": True,
                                "details": "Found 2 datasets.",
                            }
                        }
                    ),
                },
            ]
        }

        out = _deduplicate_discovery_tool_turns(payload)
        messages = out.get("messages", [])
        tool_call_ids = [m.get("tool_call_id") for m in messages if isinstance(m, dict) and m.get("role") == "tool"]
        self.assertIn("call_fail", tool_call_ids)
        self.assertIn("call_ok", tool_call_ids)


if __name__ == "__main__":
    unittest.main()
