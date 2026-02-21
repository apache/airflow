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
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic_ai.agent import Agent

from airflow.providers.common.ai.exceptions import PromptBuildError
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.utils.mixins import CommonAIHookMixin
from airflow.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.ai.utils.config import DataSourceConfig


class BaseLLMOperator(BaseOperator, CommonAIHookMixin):
    """
    Base operator for LLM based tasks.

    :param prompts: List of prompts to be sent to the LLM.
    :param datasource_configs: List of configurations for the datasources to be used.
    :param instruction: Instruction for the LLM agent.
    :param provider_model: The provider model to use for the LLM.
    :param pydantic_ai_conn_id: Connection ID for the Pydantic AI hook.
    :param validate_result: Whether to validate the result of the LLM.
    """

    template_fields = (
        "prompts",
        "datasource_configs",
        "instruction",
        "provider_model",
        "pydantic_ai_conn_id",
        "validate_result",
    )

    def __init__(
        self,
        prompts: list[str],
        datasource_configs: list[DataSourceConfig],
        instruction: str | None = None,
        provider_model: str | None = None,
        pydantic_ai_conn_id: str = "pydantic_ai_default",
        validate_result: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.prompts = prompts
        self.datasource_configs = datasource_configs
        self.instruction = instruction
        self.provider_model = provider_model
        self.pydantic_ai_conn_id = pydantic_ai_conn_id
        self.agent: Agent | None = None
        self.validate_result = validate_result

        if self.provider_model and ":" not in self.provider_model:
            raise ValueError(
                "Provider model must be in the format provider:model_name, e.g. github:openai/gpt-4o-mini"
            )

    @cached_property
    def pydantic_hook(self):
        """Get Pydantic AI hook."""
        return PydanticAIHook(
            pydantic_ai_conn_id=self.pydantic_ai_conn_id, provider_model=self.provider_model
        )

    def execute(self, context) -> Any:
        """Execute LLM operator."""
        prompt = self.get_prepared_prompt()
        response = self._run_with_agent(prompt)
        return self.process_llm_response(response)

    def _create_llm_agent(self, output_type: Any, instruction: str):
        """Create Pydantic AI agent."""
        model = self.pydantic_hook.get_model()
        if self.agent is not None:
            return self.agent

        self.agent = Agent(model=model, output_type=output_type, instructions=instruction)

        self.log.info("Agent created with provider model: %s", model.model_name)
        return self.agent

    def _run_with_agent(self, prompt: str):
        """Run Pydantic AI agent."""
        instruction = self._instruction
        self.log.info("Running LLM agent with instruction: %s", instruction)
        return self._create_llm_agent(output_type=self.output_type, instruction=instruction).run_sync(prompt)

    def get_prepared_prompt(self) -> str:
        """Prepare prompt for LLM based on datasource configs."""
        # TODO Add support for file storage like S3, GCS etc.

        try:
            prompt_parts = []
            for config in self.datasource_configs:
                if config.schema is None:
                    hook = self.get_db_api_hook(config.conn_id)
                    schema = hook.get_schema(table_name=config.table_name)
                else:
                    schema = config.schema

                schema = self.parse_schema(schema)

                if not schema:
                    raise ValueError(f"Schema cannot be empty for table {config.table_name}")

                prompt_parts.append(
                    f"TableName: {config.table_name}\nSchema: {json.dumps(schema, indent=4, default=str)}\n"
                )

            prompts_str = "\n".join(f"{i}. {p}" for i, p in enumerate(self.prompts, start=1))
            prompt = "\n".join(prompt_parts) + f"\n\n{prompts_str}\n"

            self.log.info("Prepared prompt for LLM: \n\n%s", prompt)
            return prompt
        except Exception as e:
            raise PromptBuildError(f"Error preparing prompt for LLM: {e}")

    def process_llm_response(self, response: Any):
        """Process LLM response to return SQL query dict."""
        return response

    @property
    def output_type(self):
        """Return the output type of the LLM model."""
        return str

    @property
    def _instruction(self):
        """Return the instruction for the LLM model."""
        return self.instruction

    @staticmethod
    def parse_schema(schema: dict[str, str] | list):
        """Parse schema to dict."""
        schema_dict = {}
        if isinstance(schema, list):
            for item in schema:
                schema_dict[item.get("name")] = item.get("type")
            return schema_dict
        if isinstance(schema, dict):
            return schema
        return None

    @staticmethod
    def _sql_evaluate_func(query: str) -> str:
        """Fake function to pass through a query while running dataset evals."""
        return query

    def evaluate_result(self, response: dict[str, str]):
        raise NotImplementedError
