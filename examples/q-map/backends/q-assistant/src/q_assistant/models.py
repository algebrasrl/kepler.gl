from __future__ import annotations

from pydantic import BaseModel


class AgentConfig(BaseModel):
    provider: str | None = None
    model: str | None = None
    baseUrl: str | None = None
    apiKey: str | None = None
    temperature: float | None = None
    topP: float | None = None


class ChatRequest(BaseModel):
    prompt: str
    context: dict | None = None
    agent: AgentConfig | None = None


class ChatResponse(BaseModel):
    answer: str
    provider: str
    model: str
    switchLog: list[str] | None = None
    requestId: str | None = None
    chatId: str | None = None
