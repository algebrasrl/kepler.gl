import unittest

from q_assistant.models import AgentConfig
from q_assistant.request_routing import _should_skip_agent_for_payload


class AgentSkipPolicyTests(unittest.TestCase):
    def test_skip_openai_gpt4_when_tools_present(self):
        agent = AgentConfig(provider="openai", model="gpt-4", baseUrl="https://api.openai.com/v1", apiKey="k")
        payload = {
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders", "parameters": {"type": "object"}}}
            ]
        }
        self.assertEqual(
            _should_skip_agent_for_payload(agent, payload),
            "context-likely-exceeded-with-tools",
        )

    def test_do_not_skip_openai_gpt4_without_tools(self):
        agent = AgentConfig(provider="openai", model="gpt-4", baseUrl="https://api.openai.com/v1", apiKey="k")
        payload = {"messages": [{"role": "user", "content": "hello"}]}
        self.assertIsNone(_should_skip_agent_for_payload(agent, payload))

    def test_do_not_skip_other_model(self):
        agent = AgentConfig(
            provider="openai",
            model="gpt-4o-mini",
            baseUrl="https://api.openai.com/v1",
            apiKey="k",
        )
        payload = {
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders", "parameters": {"type": "object"}}}
            ]
        }
        self.assertIsNone(_should_skip_agent_for_payload(agent, payload))


if __name__ == "__main__":
    unittest.main()
