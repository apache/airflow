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

from airflow.providers.common.ai.configs.datasource import DataSourceConfig
from airflow.providers.common.ai.exceptions import AgentResponseEvaluationFailure
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.operators.base_llm import BaseLLMOperator
from airflow.sdk import Connection

DATASOURCE_CONFIG = DataSourceConfig(
    conn_id="postgres_default",
    uri="postgres://postgres:postgres@localhost:5432/postgres",
    table_name="test_table",
    schema={"id": "integer", "name": "varchar"},
)
API_KEY = "gpt_api_key"

PROMPTS = ["generate query for distinct dept"]


class CustomLLMOperator(BaseLLMOperator):
    def get_prepared_prompt(self):
        return "prepared_prompt"


class TestBaseLLMOperator:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        conn = Connection(conn_id="pydantic_ai_default", conn_type=PydanticAIHook.conn_type, password=API_KEY)
        create_connection_without_db(conn)

    def test_init(self):
        base_llm_operator = BaseLLMOperator(
            prompts=PROMPTS, task_id="base_llm_task", datasource_configs=[DATASOURCE_CONFIG]
        )
        assert base_llm_operator.prompts == PROMPTS
        assert base_llm_operator.provider_model is None
        assert base_llm_operator.pydantic_ai_conn_id == "pydantic_ai_default"
        assert base_llm_operator.agent is None
        assert base_llm_operator.validate_result is True

    @patch("airflow.providers.common.ai.operators.base_llm.Agent")
    @patch("airflow.providers.common.ai.operators.base_llm.PydanticAIHook")
    def test_execute(self, mock_hook_cls, mock_agent_cls):
        mock_agent_instance = mock_agent_cls.return_value
        mock_agent_instance.run_sync.return_value = "mock_response"

        mock_hook_instance = mock_hook_cls.return_value
        mock_hook_instance.get_model.return_value = MagicMock(model_name="test_model")

        operator = CustomLLMOperator(
            prompts=PROMPTS, task_id="test_task", datasource_configs=[DATASOURCE_CONFIG]
        )
        result = operator.execute(context={})

        assert result == "mock_response"
        mock_agent_instance.run_sync.assert_called_once_with("prepared_prompt")
        mock_hook_instance.get_model.assert_called_once()

    def test_parse_schema_list(self):
        schema_list = [["col1", "int"], ["col2", "str"]]
        expected = {"col1": "int", "col2": "str"}
        assert BaseLLMOperator.parse_schema(schema_list) == expected

    def test_parse_schema_dict(self):
        schema_dict = {"col1": "int", "col2": "str"}
        assert BaseLLMOperator.parse_schema(schema_dict) == schema_dict

    def test_get_instruction(self):
        instruction = "custom instruction"
        operator = CustomLLMOperator(
            prompts=PROMPTS,
            task_id="test_task",
            instruction=instruction,
            datasource_configs=[DATASOURCE_CONFIG],
        )
        assert operator.get_instruction == instruction

    def test_evaluate_result(self):
        operator = CustomLLMOperator(
            prompts=PROMPTS, task_id="test_task", datasource_configs=[DATASOURCE_CONFIG]
        )

        # Should not raise exception
        operator.evaluate_result(
            response={
                "prompt1": "SELECT * from table where id = 1",
                "prompt2": "SELECT * from table where id = 2",
            }
        )

    def test_evaluate_result_error(self):
        operator = CustomLLMOperator(
            prompts=PROMPTS, task_id="test_task", datasource_configs=[DATASOURCE_CONFIG]
        )

        with pytest.raises(AgentResponseEvaluationFailure, match="Agent response evaluation failed"):
            operator.evaluate_result(
                response={"prompt1": "DROP TABLE t1", "prompt2": "SELECT * from table where id = 2"}
            )
