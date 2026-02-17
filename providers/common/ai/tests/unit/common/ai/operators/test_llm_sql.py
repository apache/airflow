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
from pydantic_ai import AgentRunResult

from airflow.providers.common.ai.exceptions import AgentResponseEvaluationFailure
from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator, SQLQueryResponseOutputType

from unit.common.ai.test_constants import (
    DATASOURCE_CONFIG,
    PROMPTS,
    TEST_MODEL_NAME,
    DBApiHookForTests,
)


class TestLLMSQLQueryOperator:
    def setup_method(self):
        self.llm_sql_query_operator = LLMSQLQueryOperator(
            datasource_configs=[DATASOURCE_CONFIG],
            prompts=PROMPTS,
            task_id="llm_sql_query_operator",
        )
        self.dbapi_hook = DBApiHookForTests()

    def test_init(self):
        llm_sql_query_operator = LLMSQLQueryOperator(
            datasource_configs=[DATASOURCE_CONFIG],
            prompts=PROMPTS,
            task_id="llm_sql_query_operator",
        )

        assert llm_sql_query_operator.datasource_configs == [DATASOURCE_CONFIG]
        assert llm_sql_query_operator.prompts == PROMPTS
        assert llm_sql_query_operator.provider_model is None
        assert llm_sql_query_operator.pydantic_ai_conn_id == "pydantic_ai_default"
        assert llm_sql_query_operator.agent is None
        assert llm_sql_query_operator.validate_result is True

    def test_init_with_non_default_values(self):
        llm_sql_query_operator = LLMSQLQueryOperator(
            datasource_configs=[DATASOURCE_CONFIG],
            prompts=PROMPTS,
            task_id="llm_sql_query_operator",
            provider_model=TEST_MODEL_NAME,
            validate_result=False,
            pydantic_ai_conn_id="pydantic_ai_with_extra_fields",
        )
        assert llm_sql_query_operator.provider_model == TEST_MODEL_NAME
        assert llm_sql_query_operator.validate_result is False
        assert llm_sql_query_operator.pydantic_ai_conn_id == "pydantic_ai_with_extra_fields"

    def test_get_output_type(self):
        assert self.llm_sql_query_operator.output_type is SQLQueryResponseOutputType

    def test_get_default_instruction(self):
        with patch.object(self.llm_sql_query_operator, "get_db_api_hook", return_value=self.dbapi_hook):
            assert "You are a SQL expert integrated with" in self.llm_sql_query_operator._instruction

    def test_get_instruction(self):
        instruction = "Your sql expert, your task is to generate Generate sql query"
        self.llm_sql_query_operator.instruction = instruction
        assert self.llm_sql_query_operator._instruction == instruction

    def test_process_llm_response(self):
        mock_response = MagicMock()
        mock_output = MagicMock()
        expected_dict = {"get sample data customers": "select * from customers"}
        mock_output.sql_query_prompt_dict = expected_dict
        mock_response.output = mock_output
        result = self.llm_sql_query_operator.process_llm_response(mock_response)

        assert result == expected_dict

    def test_process_llm_response_without_validation(self):
        self.llm_sql_query_operator.validate_result = False
        mock_response = MagicMock()
        mock_output = MagicMock()
        expected_dict = {"get sample data customers": "select * from customers"}
        mock_output.sql_query_prompt_dict = expected_dict
        mock_response.output = mock_output

        with patch.object(self.llm_sql_query_operator, "evaluate_result") as mock_evaluate:
            result = self.llm_sql_query_operator.process_llm_response(mock_response)

        assert result == expected_dict
        mock_evaluate.assert_not_called()

    def test_process_llm_response_validation_failed(self):
        sql_llm_query_operator = LLMSQLQueryOperator(
            datasource_configs=[DATASOURCE_CONFIG],
            prompts=PROMPTS,
            task_id="llm_sql_query_operator",
        )
        mock_response = MagicMock()
        mock_output = MagicMock()
        expected_dict = {"get sample data customers": "SELECT * FROM"}
        mock_output.sql_query_prompt_dict = expected_dict
        mock_response.output = mock_output
        with pytest.raises(AgentResponseEvaluationFailure, match="Agent response evaluation failed"):
            sql_llm_query_operator.process_llm_response(mock_response)

    @patch("airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator.evaluate_result")
    @patch("airflow.providers.common.ai.operators.base_llm.Agent")
    @patch("airflow.providers.common.ai.operators.base_llm.PydanticAIHook")
    def test_execute(self, mock_hook_cls, mock_agent_cls, mock_evaluate):
        sql_llm_query_operator = LLMSQLQueryOperator(
            datasource_configs=[DATASOURCE_CONFIG],
            prompts=PROMPTS,
            task_id="llm_sql_query_operator",
        )
        mock_agent_instance = mock_agent_cls.return_value
        agent_result = AgentRunResult(
            output=SQLQueryResponseOutputType(
                sql_query_prompt_dict={"prompt1": "SELECT * from table where id = 1"}
            )
        )
        mock_agent_instance.run_sync.return_value = agent_result

        mock_hook_instance = mock_hook_cls.return_value
        mock_hook_instance.get_model.return_value = MagicMock(model_name="test_model")

        with patch.object(sql_llm_query_operator, "get_db_api_hook", return_value=self.dbapi_hook):
            result = sql_llm_query_operator.execute(context={})

        expected_resp_prompt = 'TableName: test_table\nSchema: {\n    "id": "integer",\n    "name": "varchar"\n}\n\n\n1. generate query for distinct dept\n'

        assert result == {"prompt1": "SELECT * from table where id = 1"}
        mock_agent_instance.run_sync.assert_called_once_with(expected_resp_prompt)
        mock_hook_instance.get_model.assert_called_once()
        mock_evaluate.assert_called_once_with({"prompt1": "SELECT * from table where id = 1"})

    def test_evaluate_result(self):
        operator = LLMSQLQueryOperator(
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
        operator = LLMSQLQueryOperator(
            prompts=PROMPTS, task_id="test_task", datasource_configs=[DATASOURCE_CONFIG]
        )

        with pytest.raises(AgentResponseEvaluationFailure, match="Agent response evaluation failed"):
            operator.evaluate_result(
                response={"prompt1": "DROP TABLE t1", "prompt2": "SELECT * from table where id = 2"}
            )
