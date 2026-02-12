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

from airflow.providers.common.ai.operators.base_llm import BaseLLMOperator

if TYPE_CHECKING:
    from pydantic_ai.agent import AgentRunResult

    from airflow.providers.common.ai.configs.datasource import DataSourceConfig


class SQLQueryResponseOutputType(BaseModel):
    """Output type LLM Sql query generate."""

    sql_query_prompt_dict: dict[str, str]


class LLMSQLQueryOperator(BaseLLMOperator):
    """Operator to generate SQL queries based on prompts for multiple datasources."""

    def __init__(
        self, datasource_configs: list[DataSourceConfig], provider_model: str | None = None, **kwargs
    ):
        super().__init__(**kwargs)
        self.datasource_configs = datasource_configs
        self.provider_model = provider_model

    def execute(self, context):
        """Execute LLM Sql query operator."""
        return super().execute(context)

    @property
    def get_output_type(self):
        """Output type for LLM Sql query generates."""
        return SQLQueryResponseOutputType

    @property
    def get_instruction(self):
        """Instruction for LLM Agent."""
        db_names = []
        for config in self.datasource_configs:
            if config.db_name is None:
                config.db_name = config.uri.split("://")[1]
            db_names.append(config.db_name)
        unique_db_names = set(db_names)
        db_name_str = ", ".join(unique_db_names)
        if self.instruction is None:
            self.instruction = (
                f"You are a SQL expert integrated with {db_name_str}, Your task is to generate SQL query's based on the prompts and"
                f"return the each query and its prompt in key value pair dict format. Make sure the generated query supports given DatabaseType and It should not generate any query without these dangerous keywords: {self.BLOCKED_KEYWORDS} without where class"
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
