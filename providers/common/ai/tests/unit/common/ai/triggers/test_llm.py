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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.triggers.llm import LLMTrigger, deserialize_usage_limits, serialize_usage_limits
from airflow.triggers.base import TriggerEvent


class Summary(BaseModel):
    text: str


def _make_mock_run_result(output):
    mock_result = MagicMock()
    mock_result.output = output
    mock_result.usage = MagicMock(requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0)
    mock_result.response = MagicMock(model_name="test-model")
    mock_result.all_messages.return_value = []
    return mock_result


class TestLLMTrigger:
    def test_serialization(self):
        trigger = LLMTrigger(
            prompt="hello",
            llm_conn_id="my_llm",
            model_id="openai:gpt-5",
            system_prompt="be concise",
            output_type_path="builtins.str",
            agent_params={"retries": 1},
            usage_limits={"request_limit": 2},
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.common.ai.triggers.llm.LLMTrigger"
        assert kwargs == {
            "prompt": "hello",
            "llm_conn_id": "my_llm",
            "model_id": "openai:gpt-5",
            "system_prompt": "be concise",
            "output_type_path": "builtins.str",
            "agent_params": {"retries": 1},
            "usage_limits": {"request_limit": 2},
        }

    @pytest.mark.asyncio
    @patch("airflow.providers.common.ai.triggers.llm.PydanticAIHook", autospec=True)
    async def test_run_success(self, mock_hook_cls):
        mock_agent = MagicMock()
        mock_agent.run = AsyncMock(return_value=_make_mock_run_result("Paris"))
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        trigger = LLMTrigger(
            prompt="capital?",
            llm_conn_id="my_llm",
            output_type_path="builtins.str",
        )

        events = [event async for event in trigger.run()]
        assert len(events) == 1
        assert events[0].payload == {"status": "success", "output": "Paris"}
        mock_agent.run.assert_awaited_once_with("capital?", usage_limits=None)

    @pytest.mark.asyncio
    @patch("airflow.providers.common.ai.triggers.llm.PydanticAIHook", autospec=True)
    async def test_run_failure(self, mock_hook_cls):
        mock_hook_cls.get_hook.side_effect = RuntimeError("network down")
        trigger = LLMTrigger(
            prompt="capital?",
            llm_conn_id="my_llm",
            output_type_path="builtins.str",
        )

        events = [event async for event in trigger.run()]
        assert events[0].payload["status"] == "error"
        assert "network down" in events[0].payload["message"]


class TestUsageLimitsSerialization:
    def test_round_trip(self):
        pytest.importorskip("pydantic_ai")
        from pydantic_ai.usage import UsageLimits

        limits = UsageLimits(request_limit=2, output_tokens_limit=100)
        payload = serialize_usage_limits(limits)
        restored = deserialize_usage_limits(payload)
        assert restored.request_limit == 2
        assert restored.output_tokens_limit == 100
