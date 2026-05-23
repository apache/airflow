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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.hooks.base_ai import AgentRunResult, AgentUsage, BaseAIHook
from airflow.providers.common.compat.sdk import BaseHook


class TestBaseAIHookGetAgentHook:
    @patch("airflow.providers.common.ai.hooks.base_ai.BaseHook.get_hook", autospec=True)
    def test_returns_hook_when_instance_is_base_ai_hook(self, mock_get_hook):
        mock_hook = MagicMock(spec=BaseAIHook)
        mock_get_hook.return_value = mock_hook

        result = BaseAIHook.get_agent_hook("my_conn")

        assert result is mock_hook
        mock_get_hook.assert_called_once_with("my_conn", hook_params=None)

    @patch("airflow.providers.common.ai.hooks.base_ai.BaseHook.get_hook", autospec=True)
    def test_raises_when_hook_is_not_base_ai_hook(self, mock_get_hook):
        mock_get_hook.return_value = MagicMock(spec=BaseHook)

        with pytest.raises(TypeError, match="not a BaseAIHook"):
            BaseAIHook.get_agent_hook("my_conn")


class TestAgentRunResult:
    def test_dataclass_fields(self):
        usage = AgentUsage(requests=1, tool_calls=2, total_tokens=10)
        result = AgentRunResult(
            output="answer",
            message_history=["msg"],
            model_name="test-model",
            usage=usage,
            tool_names=["query"],
        )
        assert result.output == "answer"
        assert result.message_history == ["msg"]
        assert result.model_name == "test-model"
        assert result.usage == usage
        assert result.tool_names == ["query"]
