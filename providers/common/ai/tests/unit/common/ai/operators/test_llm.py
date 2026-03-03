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

from pydantic import BaseModel

from airflow.providers.common.ai.operators.llm import LLMOperator


class TestLLMOperator:
    def test_template_fields(self):
        expected = {"prompt", "llm_conn_id", "model_id", "system_prompt", "agent_params"}
        assert set(LLMOperator.template_fields) == expected

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_returns_string_output(self, mock_hook_cls):
        """Default output_type=str returns the LLM string directly."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_result = MagicMock(spec=["output"])
        mock_result.output = "Paris is the capital of France."
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(task_id="test", prompt="What is the capital of France?", llm_conn_id="my_llm")
        result = op.execute(context=MagicMock())

        assert result == "Paris is the capital of France."
        mock_agent.run_sync.assert_called_once_with("What is the capital of France?")
        mock_hook_cls.return_value.create_agent.assert_called_once_with(output_type=str, instructions="")
        mock_hook_cls.assert_called_once_with(llm_conn_id="my_llm", model_id=None)

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_structured_output_with_all_params(self, mock_hook_cls):
        """Structured output via model_dump(), with model_id, system_prompt, and agent_params."""

        class Entities(BaseModel):
            names: list[str]

        mock_agent = MagicMock(spec=["run_sync"])
        mock_result = MagicMock(spec=["output"])
        mock_result.output = Entities(names=["Alice", "Bob"])
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="test",
            prompt="Extract entities",
            llm_conn_id="my_llm",
            model_id="openai:gpt-5",
            system_prompt="You are an extractor.",
            output_type=Entities,
            agent_params={"retries": 3, "model_settings": {"temperature": 0.9}},
        )
        result = op.execute(context=MagicMock())

        assert result == {"names": ["Alice", "Bob"]}
        mock_hook_cls.assert_called_once_with(llm_conn_id="my_llm", model_id="openai:gpt-5")
        mock_hook_cls.return_value.create_agent.assert_called_once_with(
            output_type=Entities,
            instructions="You are an extractor.",
            retries=3,
            model_settings={"temperature": 0.9},
        )
