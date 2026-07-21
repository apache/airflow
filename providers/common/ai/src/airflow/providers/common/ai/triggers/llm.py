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

from collections.abc import AsyncIterator
from typing import Any

from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.common.ai.utils.output_type import (
    deserialize_output_type,
    serialize_llm_output,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


def serialize_usage_limits(usage_limits: Any | None) -> dict[str, Any] | None:
    """Serialize pydantic-ai ``UsageLimits`` for trigger persistence."""
    if usage_limits is None:
        return None
    return usage_limits.model_dump(exclude_defaults=True)


def deserialize_usage_limits(data: dict[str, Any] | None) -> UsageLimits | None:
    """Restore pydantic-ai ``UsageLimits`` from trigger persistence."""
    if data is None:
        return None
    return UsageLimits(**data)


class LLMTrigger(BaseTrigger):
    """
    Run a single-shot pydantic-ai LLM call in the triggerer.

    Frees the worker slot while the model request is in flight.
    """

    def __init__(
        self,
        *,
        prompt: str,
        llm_conn_id: str,
        model_id: str | None = None,
        system_prompt: str = "",
        output_type_path: str,
        agent_params: dict[str, Any] | None = None,
        usage_limits: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.output_type_path = output_type_path
        self.agent_params = agent_params or {}
        self.usage_limits = usage_limits

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize LLMTrigger arguments and classpath."""
        return (
            "airflow.providers.common.ai.triggers.llm.LLMTrigger",
            {
                "prompt": self.prompt,
                "llm_conn_id": self.llm_conn_id,
                "model_id": self.model_id,
                "system_prompt": self.system_prompt,
                "output_type_path": self.output_type_path,
                "agent_params": self.agent_params,
                "usage_limits": self.usage_limits,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the LLM call asynchronously and yield the result."""
        try:
            hook = PydanticAIHook.get_hook(
                self.llm_conn_id,
                hook_params={"model_id": self.model_id},
            )
            output_type = deserialize_output_type(self.output_type_path)
            usage_limits = deserialize_usage_limits(self.usage_limits)
            agent = hook.create_agent(
                output_type=output_type,
                instructions=self.system_prompt,
                **self.agent_params,
            )
            result = await agent.run(self.prompt, usage_limits=usage_limits)
            log_run_summary(self.log, result)
            yield TriggerEvent(
                {
                    "status": "success",
                    "output": serialize_llm_output(result.output),
                }
            )
        except Exception as exc:
            yield TriggerEvent({"status": "error", "message": str(exc)})
