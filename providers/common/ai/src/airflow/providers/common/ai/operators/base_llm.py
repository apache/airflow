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

from functools import cached_property
from typing import Any

from pydantic_ai import Agent
from pydantic_evals import Dataset

from airflow.providers.common.ai.evals.llm_sql import build_test_case, ValidateSQL
from airflow.providers.common.ai.exceptions import AgentResponseEvaluationFailure
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.sdk import BaseOperator


class BaseLLMOperator(BaseOperator):
    """
    Base operator for LLM-based tasks.
    """
    BLOCKED_KEYWORDS = ["DROP", "TRUNCATE", "DELETE FROM", "ALTER TABLE", "GRANT", "REVOKE"]

    def __init__(
        self,
        prompts: list[str],
        instruction: str = "Your sql expert, generate a sql query based on the prompts",
        provider_model: str | None = None,
        pydantic_ai_conn_id: str = "pydantic_ai_default",
        validate_result: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.prompts = prompts
        self.instruction = instruction
        self.provider_model = provider_model
        self.pydantic_ai_conn_id = pydantic_ai_conn_id
        self.agent: Agent | None = None
        self.validate_result = validate_result

    @cached_property
    def pydantic_hook(self):
        return PydanticAIHook(pydantic_ai_conn_id=self.pydantic_ai_conn_id, provider_model=self.provider_model)

    def execute(self, context) -> Any:
        prompt = self.get_prepared_prompt()
        response = self._run_with_agent(prompt)
        return self.process_llm_response(response)

    def _create_llm_agent(self, output_type: Any, instruction: str):
        """Create Pydantic AI agent."""

        model = self.pydantic_hook.get_model()
        if self.agent is not None:
            return self.agent

        self.agent = Agent(model=model, output_type=output_type, instructions=instruction)

        self.log.info(f"Agent created with provider model: {model.model_name}")
        return self.agent

    def _run_with_agent(self, prompt: str):
        return self._create_llm_agent(output_type=self.get_output_type, instruction=self.get_instruction).run_sync(prompt)

    def get_prepared_prompt(self) -> Any:
        """Prepare prompt for LLM based on datasource config."""
        raise NotImplementedError

    def process_llm_response(self, response: Any):
        """Process LLM response to return SQL query dict."""
        return response

    @property
    def get_output_type(self):
        """Return the output type of the LLM model."""
        return str

    @property
    def get_instruction(self):
        return self.instruction

    @staticmethod
    def parse_schema(schema: dict[str, str] | list):
        schema_dict = {}
        if isinstance(schema, list):
            for item in schema:
                schema_dict[item[0]] = item[1]
            return schema_dict
        elif isinstance(schema, dict):
            return schema
        return None

    @staticmethod
    def _dummy_evaluate_func(query: str) -> str:
        """Fake function to pass through a query while running dataset evals"""
        return query

    def evaluate_result(self, response: dict[str, str]):
        """Evaluate response for each query that generated."""

        eval_tcs = [build_test_case(query, f"Validate sql: {prompt}") for prompt, query in response.items()]

        dataset = Dataset(
            cases=eval_tcs,
            evaluators=[ValidateSQL(BLOCKED_KEYWORDS=self.BLOCKED_KEYWORDS)],
        )
        report = dataset.evaluate_sync(self._dummy_evaluate_func)

        # Validate all the eval tests are passed here 1 -> 100%
        if report.averages().assertions != 1:
            report.print(include_input=True, include_durations=False)
            raise AgentResponseEvaluationFailure("Agent response evaluation failed")





