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

from typing import TYPE_CHECKING

from pydantic import BaseModel

from airflow.providers.common.ai.exceptions import AgentResponseEvaluationFailure
from airflow.providers.common.ai.operators.base_llm import BaseLLMOperator

if TYPE_CHECKING:
    from pydantic_ai.agent import AgentRunResult


class SQLQueryResponseOutputType(BaseModel):
    """Output type LLM Sql query generate."""

    sql_query_prompt_dict: dict[str, str]


class LLMSQLQueryOperator(BaseLLMOperator):
    """Operator to generate SQL queries based on prompts for multiple datasources."""

    BLOCKED_KEYWORDS = ["DROP", "TRUNCATE", "DELETE FROM", "ALTER TABLE", "GRANT", "REVOKE"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def output_type(self):
        """Output type for LLM Sql query generates."""
        return SQLQueryResponseOutputType

    @property
    def _instruction(self):
        """Instruction for LLM Agent."""
        db_names = []

        for config in self.datasource_configs:
            if config.db_name is None:
                hook = self.get_db_api_hook(config.conn_id)
                config.db_name = hook.dialect_name

            db_names.append(config.db_name)
        unique_db_names = set(db_names)
        db_name_str = ", ".join(unique_db_names)

        if self.instruction is None:
            self.instruction = (
                f"You are a SQL expert integrated with {db_name_str}, Your task is to generate SQL query's based on the prompts and "
                f"return the each query and its prompt in key value pair dict format. Make sure the generated query supports given dialect and It should not generate any queries without these dangerous keywords: {self.BLOCKED_KEYWORDS}."
            )
        return self.instruction

    def process_llm_response(self, response: AgentRunResult):
        """Process LLM response to return SQL query dict."""
        self.log.info("LLM response: %s", response.output)

        if self.validate_result:
            self.log.info("Evaluating generated responses")
            self.evaluate_result(response.output.sql_query_prompt_dict)
            self.log.info("Responses evaluated successfully")

        return response.output.sql_query_prompt_dict

    def evaluate_result(self, response: dict[str, str]):
        """Evaluate response for each query that generated."""
        try:
            from pydantic_evals import Dataset

            from airflow.providers.common.ai.evals.sql import ValidateSQL, build_test_case
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(e)

        eval_tcs = [build_test_case(query, f"Validate sql: {prompt}") for prompt, query in response.items()]

        dataset = Dataset(
            cases=eval_tcs,
            evaluators=[ValidateSQL(BLOCKED_KEYWORDS=self.BLOCKED_KEYWORDS)],
        )
        report = dataset.evaluate_sync(self._sql_evaluate_func)

        # Validate all the eval tests are passed here 1 -> 100%
        if report.averages().assertions != 1:
            report.print(include_input=True, include_durations=False)
            raise AgentResponseEvaluationFailure("Agent response evaluation failed")
