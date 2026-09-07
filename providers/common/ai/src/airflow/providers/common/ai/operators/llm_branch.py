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

import json
from collections.abc import Iterable, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.standard.exceptions import HITLRejectException
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
    :param fail_on_reject: If ``True``, a rejected review fails the task
        instead of skipping the downstream tasks. Generally discouraged,
        as for :class:`~airflow.providers.standard.operators.hitl.ApprovalOperator`.
        Default ``False``.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``, ``tools``).

    Human-in-the-Loop approval parameters are inherited from
    :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`
    (``require_approval``, ``approval_timeout``, ``allow_modifications``).
    The task pauses after the LLM chooses the branch(es) and only skips the
    unselected downstream tasks once a reviewer approves. Rejecting the
    review skips the direct downstream tasks except teardowns, matching
    :class:`~airflow.providers.standard.operators.hitl.ApprovalOperator`;
    set ``fail_on_reject=True`` to fail the task instead. The review form
    lists the valid downstream task IDs; with ``allow_modifications=True``
    the editable choice is rendered as a dropdown of those IDs (single-branch
    mode) or a multi-select of them (``allow_multiple_branches=True``), and
    the reviewed branch(es) are validated against the downstream task IDs
    before branching.
    """

    inherits_from_skipmixin = True

    template_fields: Sequence[str] = LLMOperator.template_fields

    def __init__(
        self,
        *,
        allow_multiple_branches: bool = False,
        fail_on_reject: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("output_type", None)
        super().__init__(**kwargs)
        self.allow_multiple_branches = allow_multiple_branches
        self.fail_on_reject = fail_on_reject

    def execute(self, context: Context) -> str | Iterable[str] | None:
        if self.require_approval:
            self.validate_approval_prompt()  # type: ignore[misc]

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
        result = agent.run_sync(self.prompt, usage_limits=self.usage_limits)
        log_run_summary(self.log, result)
        output = result.output

        branches: str | list[str]
        if isinstance(output, list):
            branches = [item.value for item in output]
        elif isinstance(output, Enum):
            branches = output.value
        else:
            branches = str(output)

        if not branches:
            raise ValueError(
                f"LLM selected no branches for {self.task_id!r}, which would skip every downstream task."
            )

        if self.require_approval:
            choices = sorted(self.downstream_task_ids)
            chosen = branches if isinstance(branches, str) else json.dumps(branches)
            body = (
                f"Valid branches: {', '.join(f'`{c}`' for c in choices)}\n\n"
                f"```\nPrompt: {self.prompt}\n\nChosen branch(es): {chosen}\n```"
            )
            modification_schema = (
                {"type": "array", "items": {"type": "string", "enum": choices}, "examples": choices}
                if self.allow_multiple_branches
                else {"type": "string", "enum": choices}
            )
            self.defer_for_approval(  # type: ignore[misc]
                context, branches, body=body, modification_schema=modification_schema
            )

        return self.do_branch(context, branches)

    def execute_complete(self, context: Context, generated_output: str, event: dict[str, Any]) -> Any:
        """Resume after human review, validating the reviewed choice before branching."""
        try:
            output = super().execute_complete(context, generated_output, event)
        except HITLRejectException:
            if self.fail_on_reject:
                raise
            self.log.info("Rejected by %s. Skipping downstream tasks...", event.get("responded_by_user"))
            tasks = context["task"].get_direct_relatives(upstream=False)
            self.skip(ti=context["ti"], tasks=(t for t in tasks if not t.is_teardown))
            return None
        branches = self._parse_reviewed_branches(output)
        selected = {branches} if isinstance(branches, str) else set(branches)
        invalid = selected - self.downstream_task_ids
        if invalid:
            raise ValueError(
                f"Reviewed branch(es) {sorted(invalid)} are not downstream tasks of "
                f"{self.task_id!r}. Valid choices: {sorted(self.downstream_task_ids)}."
            )
        return self.do_branch(context, branches)

    def _parse_reviewed_branches(self, output: str) -> str | list[str]:
        if not self.allow_multiple_branches:
            return output
        try:
            branches = json.loads(output)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Reviewed output {output!r} is not valid JSON. With "
                f"allow_multiple_branches=True the reviewed output must be a "
                f'JSON list of task IDs, e.g. ["task_a", "task_b"].'
            ) from e
        if not isinstance(branches, list) or not all(isinstance(b, str) for b in branches):
            raise ValueError(
                f"Reviewed output {output!r} must be a JSON list of task ID strings, "
                f'e.g. ["task_a", "task_b"].'
            )
        if not branches:
            raise ValueError(
                "Reviewed output selects no branches, which would skip every downstream "
                "task. Select at least one task ID, or reject the review instead."
            )
        return branches
