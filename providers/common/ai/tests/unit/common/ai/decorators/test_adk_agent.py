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

from airflow.providers.common.ai.decorators.adk_agent import _AdkAgentDecoratedOperator


class TestAdkAgentDecoratedOperator:
    def test_custom_operator_name(self):
        assert _AdkAgentDecoratedOperator.custom_operator_name == "@task.adk_agent"

    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_calls_callable_and_returns_output(self, mock_hook_cls):
        """The callable's return value becomes the agent prompt."""
        mock_hook_cls.return_value.run_agent_sync.return_value = "ADK result"

        def my_prompt():
            return "Who is our top customer?"

        op = _AdkAgentDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            model_id="gemini-2.5-flash",
        )
        result = op.execute(context={})

        assert result == "ADK result"
        assert op.prompt == "Who is our top customer?"
        mock_hook_cls.return_value.run_agent_sync.assert_called_once()

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None],
        ids=["non-string", "empty", "whitespace", "none"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns a non-string or blank string."""
        op = _AdkAgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            model_id="gemini-2.5-flash",
        )
        with pytest.raises(TypeError, match="non-empty string"):
            op.execute(context={})

    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_passes_tools(self, mock_hook_cls):
        """Tools are forwarded to create_agent."""
        mock_hook_cls.return_value.run_agent_sync.return_value = "done"

        def my_tool():
            """A test tool."""
            pass

        op = _AdkAgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Do something",
            model_id="gemini-2.5-flash",
            tools=[my_tool],
        )
        op.execute(context={})

        create_call = mock_hook_cls.return_value.create_agent.call_args
        assert create_call[1]["tools"] == [my_tool]

    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_merges_op_kwargs_into_callable(self, mock_hook_cls):
        """op_kwargs are resolved by the callable to build the prompt."""
        mock_hook_cls.return_value.run_agent_sync.return_value = "done"

        def my_prompt(topic):
            return f"Analyze {topic}"

        op = _AdkAgentDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            model_id="gemini-2.5-flash",
            op_kwargs={"topic": "revenue trends"},
        )
        op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Analyze revenue trends"
        mock_hook_cls.return_value.run_agent_sync.assert_called_once()
