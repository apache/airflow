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

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.sdk import BaseOperator


class BaseLLMOperator(BaseOperator):
    """
    Base operator for LLM-based tasks.
    """

    def __init__(
        self, model_name: str | None = None, pydantic_ai_conn_id: str = "pydantic_ai_default", **kwargs
    ):
        super().__init__(**kwargs)
        self.model_name = model_name
        self.pydantic_ai_conn_id = pydantic_ai_conn_id
        self._agent: Agent | None = None

    @cached_property
    def _hook(self):
        return PydanticAIHook(pydantic_ai_conn_id=self.pydantic_ai_conn_id, model_name=self.model_name)

    def _create_llm_agent(self, output_type, instructions=None):
        """Create Pydantic AI agent."""
        model = self._hook.get_model()
        if self._agent is not None:
            return self._agent

        self._agent = Agent(model=model, output_type=output_type, instructions=instructions)
        return self._agent

    def _run_with_agent(self):
        pass

    def _prepare_prompts(self):
        pass

    def _process_llm_response(self, response):
        pass

    def execute(self, context) -> Any:
        self._prepare_prompts()
        response = self._run_with_agent()
        return self._process_llm_response(response)
