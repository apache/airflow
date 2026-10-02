# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
from typing import Any

import httpx2
import pytest
from anthropic import AsyncAnthropic
from pydantic_ai import Agent
from pydantic_ai.messages import (
    CachePoint,
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.providers.anthropic import AnthropicProvider

from airflow.providers.common.ai.utils.prompt_cache import PROMPT_CACHE_SETTING_NAMES, PromptCaching

ALL_DEFAULTS = {
    "anthropic_cache_tool_definitions": True,
    "anthropic_cache_instructions": True,
    "anthropic_cache_messages": True,
    "bedrock_cache_tool_definitions": True,
    "bedrock_cache_instructions": True,
    "bedrock_cache_messages": True,
    "openrouter_cache_tool_definitions": True,
    "openrouter_cache_instructions": True,
    "openrouter_cache_messages": True,
}


def _run_and_capture_settings(
    *,
    model_settings: Any = None,
    run_model_settings: Any = None,
    model_own_settings: Any = None,
    capabilities: list[Any] | None = None,
    prompt: Any = "hi",
    message_history: list[ModelMessage] | None = None,
) -> dict[str, Any]:
    """Run a real agent against a FunctionModel and return the settings its request carried."""
    seen: dict[str, Any] = {}

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        seen.update(info.model_settings or {})
        return ModelResponse(parts=[TextPart("ok")])

    agent = Agent(
        FunctionModel(respond, settings=model_own_settings),
        instructions="Be brief.",
        model_settings=model_settings,
        capabilities=[PromptCaching()] if capabilities is None else capabilities,
    )
    agent.run_sync(prompt, model_settings=run_model_settings, message_history=message_history)
    return seen


class TestPromptCaching:
    def test_turns_on_every_provider_family_by_default(self):
        assert _run_and_capture_settings() == ALL_DEFAULTS

    def test_keeps_unrelated_agent_settings(self):
        settings = _run_and_capture_settings(model_settings={"temperature": 0})

        assert settings == {"temperature": 0, **ALL_DEFAULTS}

    @pytest.mark.parametrize(
        ("caller_settings", "skipped_prefix"),
        [
            pytest.param({"anthropic_cache": True}, "anthropic_cache", id="anthropic-automatic"),
            pytest.param({"anthropic_cache_instructions": "1h"}, "anthropic_cache", id="anthropic-ttl"),
            pytest.param({"anthropic_cache_messages": False}, "anthropic_cache", id="anthropic-off"),
            pytest.param({"bedrock_cache_messages": False}, "bedrock_cache", id="bedrock-off"),
            pytest.param({"openrouter_cache_messages": "1h"}, "openrouter_cache", id="openrouter-ttl"),
        ],
    )
    def test_a_caller_cache_setting_takes_over_that_provider_family(self, caller_settings, skipped_prefix):
        settings = _run_and_capture_settings(model_settings=caller_settings)

        untouched = {k: v for k, v in ALL_DEFAULTS.items() if not k.startswith(skipped_prefix)}
        assert settings == {**caller_settings, **untouched}

    def test_the_model_own_cache_settings_take_over_that_provider_family(self):
        settings = _run_and_capture_settings(model_own_settings={"anthropic_cache": "1h"})

        assert settings["anthropic_cache"] == "1h"
        assert "anthropic_cache_messages" not in settings
        assert settings["bedrock_cache_messages"] is True

    def test_callable_agent_settings_are_seen(self):
        settings = _run_and_capture_settings(model_settings=lambda ctx: {"anthropic_cache": True})

        assert settings["anthropic_cache"] is True
        assert "anthropic_cache_messages" not in settings

    def test_run_settings_still_win(self):
        settings = _run_and_capture_settings(run_model_settings={"anthropic_cache_messages": False})

        assert settings["anthropic_cache_messages"] is False

    def test_no_capability_means_no_cache_settings(self):
        assert _run_and_capture_settings(capabilities=[]) == {}

    def test_a_cache_point_in_the_prompt_leaves_caching_to_the_caller(self):
        settings = _run_and_capture_settings(prompt=["long document", CachePoint(ttl="1h"), "question"])

        assert settings == {}

    def test_a_cache_point_in_the_history_leaves_caching_to_the_caller(self):
        history: list[ModelMessage] = [
            ModelRequest(parts=[UserPromptPart(["long document", CachePoint(ttl="1h")])]),
            ModelResponse(parts=[TextPart("noted")]),
        ]

        settings = _run_and_capture_settings(prompt="question", message_history=history)

        assert settings == {}

    def test_setting_names_are_the_defaults_plus_automatic_anthropic_caching(self):
        assert {*ALL_DEFAULTS, "anthropic_cache"} == PROMPT_CACHE_SETTING_NAMES


class TestPromptCachingOnTheAnthropicWire:
    """What an Anthropic model actually sends, captured at the HTTP transport."""

    @staticmethod
    def _send(*, model_settings: Any = None, prompt: Any = "hi") -> dict[str, Any]:
        bodies: list[dict[str, Any]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            bodies.append(json.loads(request.content))
            return httpx2.Response(
                200,
                json={
                    "id": "msg_1",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-sonnet-4-5",
                    "content": [{"type": "text", "text": "ok"}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 1},
                },
            )

        client = AsyncAnthropic(
            api_key="test", http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
        )
        model = AnthropicModel("claude-sonnet-4-5", provider=AnthropicProvider(anthropic_client=client))
        agent = Agent(
            model, instructions="Be brief.", model_settings=model_settings, capabilities=[PromptCaching()]
        )

        @agent.tool_plain
        def lookup(key: str) -> str:
            """Look a key up."""
            return key

        agent.run_sync(prompt)
        (body,) = bodies
        return body

    def test_marks_tools_system_prompt_and_last_message(self):
        body = self._send()

        assert body["tools"][-1]["cache_control"] == {"type": "ephemeral", "ttl": "5m"}
        assert body["system"][-1]["cache_control"] == {"type": "ephemeral", "ttl": "5m"}
        assert body["messages"][-1]["content"][-1]["cache_control"] == {"type": "ephemeral", "ttl": "5m"}
        assert "cache_control" not in body

    def test_caller_automatic_caching_is_sent_instead_of_rejected(self):
        """``anthropic_cache`` cannot be combined with ``anthropic_cache_messages``, so ours step aside."""
        body = self._send(model_settings={"anthropic_cache": True})

        assert body["cache_control"] == {"type": "ephemeral", "ttl": "5m"}
        assert "cache_control" not in body["system"][-1]

    def test_caller_long_lived_cache_point_is_not_preceded_by_shorter_ones(self):
        """Anthropic needs a longer-lived entry ahead of a shorter one, so ours stay out."""
        body = self._send(prompt=["long document", CachePoint(ttl="1h"), "question"])

        marked = [
            block["cache_control"]
            for section in (body["tools"], body["system"], *(m["content"] for m in body["messages"]))
            for block in section
            if "cache_control" in block
        ]
        assert marked == [{"type": "ephemeral", "ttl": "1h"}]
