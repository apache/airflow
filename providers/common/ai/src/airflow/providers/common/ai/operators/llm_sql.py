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

from pydantic import BaseModel
from pydantic_ai import AgentRunResult

from airflow.providers.common.ai.configs.datasource import DataSourceConfig
from airflow.providers.common.ai.exceptions import PromptBuildError
from airflow.providers.common.ai.operators.base_llm import BaseLLMOperator


class SQLQueryResponseOutputType(BaseModel):
    """Output type LLM Sql query generate"""

    sql_query_prompt_dict: dict[str, str]


class LLMSQLQueryOperator(BaseLLMOperator):
    """
    Operator to generate SQL query based on prompts
    """

    def __init__(self,
                 datasource_config: DataSourceConfig,
                 provider_model: str | None = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.datasource_config = datasource_config
        self.provider_model = provider_model


    def execute(self, context):
        """Execute LLM Sql query operator"""
        return super().execute(context)

    def get_prepared_prompt(self) -> str:
        """ Prepare prompt for LLM based on datasource config """

        # TODO Add support for file storage like S3, GCS etc.

        try:
            if self.datasource_config.schema is None:
                hook = self.pydantic_hook._get_db_api_hook(self.datasource_config.conn_id)
                schema = hook.get_schema(table_name=self.datasource_config.table_name)
            else:
                schema = self.datasource_config.schema

            schema = self.parse_schema(schema)

            if not schema:
                raise ValueError("Schema cannot be empty")

            prompts = "\n".join(f"{i}. {p}" for i, p in enumerate(self.prompts, start=1))

            prompt = (
                f"TableName: {self.datasource_config.table_name}\n"
                f"Schema: {json.dumps(schema, indent=4)}\n\n"
                f"{prompts}\n"
            )

            self.log.info(f"Prepared prompt for LLM: {prompt}")
            return prompt
        except Exception as e:
            raise PromptBuildError(f"Error preparing prompt for LLM: {e}")


    @property
    def get_output_type(self):
        """Output type for LLM Sql query generates"""

        return SQLQueryResponseOutputType

    @property
    def get_instruction(self):
        """Instruction for LLM Agent"""

        if self.datasource_config.db_name is None:
            self.datasource_config.db_name = self.datasource_config.uri.split("://")[1]

        if self.instruction is None:
            self.instruction = (f"You are a SQL expert integrated with {self.datasource_config.db_name}, Your task is to generate SQL query's based on the prompts and"
                                f"return the each query and its prompt in key value pair dict format. Make sure the generated query supports given DatabaseType and It should not generate any query without these dangerous keywords: {self.BLOCKED_KEYWORDS} without where class")
        return self.instruction

    def process_llm_response(self, response: AgentRunResult):
        """Process LLM response to return SQL query dict"""

        self.log.info(f"LLM response: {response}")

        if self.validate_result:
            self.log.info("Evaluating generated responses")
            self.evaluate_result(response.output.sql_query_prompt_dict)
            self.log.info("Responses evaluated successfully")

        return response.output.sql_query_prompt_dict

