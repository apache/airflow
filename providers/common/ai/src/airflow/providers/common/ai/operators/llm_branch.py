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
"""LLM-driven branching operator."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.standard.operators.branch import BranchMixIn

if TYPE_CHECKING:
    from airflow.sdk import Context


class LLMBranchOperator(LLMOperator, BranchMixIn):
    """
    Ask an LLM to choose which downstream task(s) to execute.

    Downstream task IDs are discovered automatically from the DAG topology
    and presented to the LLM as a constrained enum via pydantic-ai structured
    output. No text parsing or manual validation is needed.

    :param prompt: The prompt to send to the LLM.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"``).
        Overrides the model stored in the connection's extra field.
    :param system_prompt: System-level instructions for the LLM agent.
    :param allow_multiple_branches: When ``False`` (default) the LLM returns a
        single task ID. When ``True`` the LLM may return one or more task IDs.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``, ``tools``).
    """

    inherits_from_skipmixin = True

    template_fields: Sequence[str] = LLMOperator.template_fields

    def __init__(
        self,
        *,
        allow_multiple_branches: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("output_type", None)
        super().__init__(**kwargs)
        self.allow_multiple_branches = allow_multiple_branches

    def execute(self, context: Context) -> str | Iterable[str] | None:
        if not self.downstream_task_ids:
            raise ValueError(
                f"{self.task_id!r} has no downstream tasks. "
                "LLMBranchOperator requires at least one downstream task to branch into."
            )

        downstream_tasks_enum = Enum(  # type: ignore[misc]
            "DownstreamTasks",
            {task_id: task_id for task_id in self.downstream_task_ids},
        )
        output_type = list[downstream_tasks_enum] if self.allow_multiple_branches else downstream_tasks_enum

        agent = self.llm_hook.create_agent(
            output_type=output_type,
            instructions=self.system_prompt,
            **self.agent_params,
        )
        result = agent.run_sync(self.prompt)
        log_run_summary(self.log, result)
        output = result.output

        branches: str | list[str]
        if isinstance(output, list):
            branches = [item.value for item in output]
        elif isinstance(output, Enum):
            branches = output.value
        else:
            branches = str(output)

        return self.do_branch(context, branches)
