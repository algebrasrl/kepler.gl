import unittest

from q_assistant.request_routing import _extract_explicit_tool_choice, _maybe_force_tool_choice


def _payload(user_text: str, tool_choice="auto"):
    return {
        "messages": [{"role": "user", "content": user_text}],
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "listQMapDatasets",
                    "description": "List loaded datasets",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "listQCumberDatasets",
                    "description": "List q-cumber datasets",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
        ],
        "tool_choice": tool_choice,
    }


class ExplicitToolRoutingTests(unittest.TestCase):
    def test_extract_explicit_tool_name_exact(self):
        name = _extract_explicit_tool_choice(_payload("listQCumberDatasets"))
        self.assertEqual(name, "listQCumberDatasets")

    def test_extract_explicit_tool_name_with_noise(self):
        name = _extract_explicit_tool_choice(_payload("<listQCumberDatasets,"))
        self.assertEqual(name, "listQCumberDatasets")

    def test_force_tool_choice_when_auto(self):
        out = _maybe_force_tool_choice(_payload("listQCumberDatasets", tool_choice="auto"), enabled=True)
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "listQCumberDatasets"}},
        )

    def test_keep_explicit_tool_choice(self):
        out = _maybe_force_tool_choice(
            _payload(
                "listQCumberDatasets",
                tool_choice={"type": "function", "function": {"name": "listQMapDatasets"}},
            ),
            enabled=True,
        )
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "listQMapDatasets"}},
        )

    def test_feature_flag_disabled(self):
        payload = _payload("listQCumberDatasets", tool_choice="auto")
        out = _maybe_force_tool_choice(payload, enabled=False)
        self.assertEqual(out.get("tool_choice"), "auto")


if __name__ == "__main__":
    unittest.main()
