import os
import json
import unittest
from functools import partial
from unittest.mock import patch

import q_assistant.usage_estimation as _usage_estimation
from q_assistant.chat_payload_compaction import _compact_chat_completions_payload
from q_assistant.config import load_settings
from q_assistant.payload_compaction import (
    apply_payload_token_budget as _apply_payload_token_budget,
    evaluate_payload_token_budget as _evaluate_payload_token_budget,
)
from q_assistant.services.request_processor import _MODEL_CONTEXT_LIMIT_HINTS


def _runtime_evaluate_payload_token_budget(settings, payload, *, model_hint=None):
    return _evaluate_payload_token_budget(
        settings,
        payload,
        model_hint=model_hint,
        estimate_payload_token_usage=_usage_estimation._estimate_payload_token_usage,
        model_context_limit_hints=_MODEL_CONTEXT_LIMIT_HINTS,
    )


_apply_payload_token_budget = partial(
    _apply_payload_token_budget,
    evaluate_payload_token_budget=_runtime_evaluate_payload_token_budget,
    compact_chat_completions_payload=_compact_chat_completions_payload,
)


def _deterministic_test_estimate_payload_token_usage(payload, *, model_hint=None):
    try:
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        serialized = str(payload)
    return {
        "estimatedPromptTokens": max(1, (len(serialized) + 3) // 4),
        "serializedChars": len(serialized),
        "method": "test:chars_div_4",
    }


class TokenBudgetCompactionTests(unittest.TestCase):
    def test_hard_trim_keeps_tool_messages_for_context(self):
        env = {
            "Q_ASSISTANT_TOKEN_BUDGET_ENABLED": "true",
            "Q_ASSISTANT_TOKEN_BUDGET_CONTEXT_LIMIT": "6000",
            "Q_ASSISTANT_TOKEN_BUDGET_RESERVED_OUTPUT_TOKENS": "1000",
            "Q_ASSISTANT_TOKEN_BUDGET_WARN_RATIO": "0.2",
            "Q_ASSISTANT_TOKEN_BUDGET_COMPACT_RATIO": "0.25",
            "Q_ASSISTANT_TOKEN_BUDGET_HARD_RATIO": "0.3",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = load_settings()

        messages = [
            {"role": "system", "content": "sys " + ("S" * 6000)},
            {"role": "user", "content": "u " + ("U" * 8000)},
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "queryQCumberDataset", "arguments": "{}"},
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_1",
                "content": '{"qmapToolResult":{"schema":"qmap.tool_result.v1","success":true,'
                '"details":"'
                + ("x" * 5000)
                + '"},"llmResult":{"success":true,"datasetId":"kontur-boundaries-italia","rows":['
                + ",".join(['{"a":1}'] * 100)
                + "]}}",
            },
        ]
        tools = []
        for i in range(60):
            tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": f"tool_{i}",
                        "description": "descrizione " + ("x" * 200),
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "p": {"type": "string", "description": "d" * 200},
                            },
                        },
                    },
                }
            )
        payload = {"messages": messages, "tools": tools}
        compacted, info = _apply_payload_token_budget(settings, payload, model_hint="openai/gpt-4.1")
        compacted_messages = compacted.get("messages", [])
        tool_messages = [m for m in compacted_messages if str(m.get("role") or "").lower() == "tool"]

        self.assertTrue(info.get("enabled"))
        self.assertGreaterEqual(len(tool_messages), 1)

    def test_budget_applies_compaction_profiles_and_reduces_payload(self):
        env = {
            "Q_ASSISTANT_TOKEN_BUDGET_ENABLED": "true",
            "Q_ASSISTANT_TOKEN_BUDGET_CONTEXT_LIMIT": "12000",
            "Q_ASSISTANT_TOKEN_BUDGET_RESERVED_OUTPUT_TOKENS": "1000",
            "Q_ASSISTANT_TOKEN_BUDGET_WARN_RATIO": "0.5",
            "Q_ASSISTANT_TOKEN_BUDGET_COMPACT_RATIO": "0.6",
            "Q_ASSISTANT_TOKEN_BUDGET_HARD_RATIO": "0.7",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = load_settings()

        tools = []
        for i in range(40):
            tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": f"tool_{i}",
                        "description": "descrizione " + ("x" * 300),
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "p": {"type": "string", "description": "d" * 300},
                            },
                        },
                    },
                }
            )
        payload = {
            "messages": [
                {"role": "system", "content": "sys " + ("S" * 5000)},
                {"role": "user", "content": "u " + ("U" * 30000)},
            ],
            "tools": tools,
        }

        with patch.object(_usage_estimation, "_estimate_payload_token_usage", side_effect=_deterministic_test_estimate_payload_token_usage):
            before = _deterministic_test_estimate_payload_token_usage(payload, model_hint="openai/gpt-4.1")
            compacted, info = _apply_payload_token_budget(
                settings,
                payload,
                model_hint="openai/gpt-4.1",
            )
            after = _deterministic_test_estimate_payload_token_usage(compacted, model_hint="openai/gpt-4.1")

        self.assertTrue(info.get("enabled"))
        self.assertTrue(info.get("actions"))
        self.assertLess(after.get("estimatedPromptTokens", 0), before.get("estimatedPromptTokens", 0))
        self.assertEqual(len(compacted.get("tools", [])), len(tools))
        compacted_names = [t.get("function", {}).get("name") for t in compacted.get("tools", [])]
        self.assertEqual(compacted_names[0], "tool_0")
        self.assertEqual(compacted_names[-1], "tool_39")

    def test_budget_can_be_disabled(self):
        env = {
            "Q_ASSISTANT_TOKEN_BUDGET_ENABLED": "false",
            "Q_ASSISTANT_TOKEN_BUDGET_CONTEXT_LIMIT": "12000",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = load_settings()

        payload = {
            "messages": [
                {"role": "user", "content": "ciao"},
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders", "parameters": {"type": "object"}}}
            ],
        }
        compacted, info = _apply_payload_token_budget(settings, payload, model_hint="openai/gpt-4.1")
        self.assertFalse(info.get("enabled"))
        self.assertEqual(compacted, payload)

    def test_budget_skips_compaction_when_estimate_is_unknown(self):
        env = {
            "Q_ASSISTANT_TOKEN_BUDGET_ENABLED": "true",
            "Q_ASSISTANT_TOKEN_BUDGET_CONTEXT_LIMIT": "12000",
            "Q_ASSISTANT_TOKEN_BUDGET_RESERVED_OUTPUT_TOKENS": "1000",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = load_settings()

        payload = {
            "messages": [
                {"role": "system", "content": "sys " + ("S" * 5000)},
                {"role": "user", "content": "u " + ("U" * 20000)},
            ],
            "tools": [{"type": "function", "function": {"name": "listQCumberProviders", "parameters": {"type": "object"}}}],
        }

        with patch.object(
            _usage_estimation,
            "_estimate_payload_token_usage",
            return_value={
                "estimatedPromptTokens": None,
                "serializedChars": 100000,
                "method": "unknown:tokenizer_unavailable",
            },
        ):
            compacted, info = _apply_payload_token_budget(settings, payload, model_hint="openai/gpt-4.1")

        self.assertEqual(compacted, payload)
        self.assertTrue(info.get("enabled"))
        self.assertEqual(info.get("finalDecision"), "unknown")
        self.assertIn("skip:unknown-estimate", info.get("actions") or [])


if __name__ == "__main__":
    unittest.main()
