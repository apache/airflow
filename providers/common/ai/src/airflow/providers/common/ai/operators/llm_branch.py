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
from collections.abc import Iterable, Mapping, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.common.ai.utils.usage import coerce_usage_limits
from airflow.providers.standard.exceptions import HITLRejectException
from airflow.providers.standard.operators.branch import BranchMixIn

if TYPE_CHECKING:
    from airflow.sdk import Context


def _downstream_tasks_enum(
    task_id: str, downstream_task_ids: Iterable[str], descriptions: Mapping[str, str] | None
) -> type[Enum]:
    """
    Build the enum of branch options the model chooses from.

    Sorted so every worker sends the model the same option order: ``downstream_task_ids``
    is a set, and set order follows string hashing, which differs between processes.

    With ``descriptions``, the enum renders as ``anyOf`` of ``{const, description}`` instead
    of a bare ``enum`` list. That is the one JSON Schema shape that carries a description per
    value, and it is what both a text model's tool schema and pydantic-ai's TypeSafe adapter
    read an option's meaning from. Validation is unchanged: the model still has to answer
    with one of the task IDs, and the output is still an enum member.
    """
    task_ids = sorted(downstream_task_ids)
    if descriptions:
        unknown = sorted(set(descriptions) - set(task_ids))
        if unknown:
            raise ValueError(
                f"branch_descriptions for {task_id!r} names {unknown}, which are not downstream "
                f"tasks. Downstream tasks: {task_ids}."
            )
    enum_cls: type[Enum] = Enum("DownstreamTasks", {name: name for name in task_ids})  # type: ignore[misc]
    if not descriptions:
        return enum_cls

    described = {name: text for name, text in descriptions.items() if text}

    def json_schema(cls: type[Enum], core_schema: Any, handler: Any) -> dict[str, Any]:
        options: list[dict[str, Any]] = []
        for member in cls:
            option: dict[str, Any] = {"const": member.value, "type": "string"}
            if text := described.get(member.value):
                option["description"] = text
            options.append(option)
        return {"anyOf": options, "title": cls.__name__}

    # pydantic looks this hook up on the type when it builds the schema, so attaching it to the
    # functional-API enum is the same as defining it in a class body.
    setattr(enum_cls, "__get_pydantic_json_schema__", classmethod(json_schema))
    return enum_cls


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
    :param fallback_conn_ids: Connection IDs to fail over to, in order, when
        the primary provider is unavailable. Overrides the ``fallback_conn_ids``
        set in the connection's extra field. ``None`` (default) reads the
        connection's own extra field; an explicit ``[]`` disables a chain
        configured there. See
        :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
        for how blank entries in the list are dropped.
    :param system_prompt: System-level instructions for the LLM agent.
    :param branch_descriptions: Optional mapping of downstream task ID to a short
        description of what choosing that branch means. Descriptions travel in the
        output schema next to the option they describe, so the model reads "here is
        an option, here is what it means" rather than guessing from the task ID. A
        downstream task without an entry is presented by its ID alone, as today. A
        key that is not a downstream task ID fails the task before the model is
        called. Supports Jinja templating.
    :param allow_multiple_branches: When ``False`` (default) the LLM returns a
        single task ID. When ``True`` the LLM may return one or more task IDs.
    :param fail_on_reject: If ``True``, a rejected review fails the task
        instead of skipping the downstream tasks. Generally discouraged,
        as for :class:`~airflow.providers.standard.operators.hitl.ApprovalOperator`.
        Only takes effect with ``require_approval=True``. Default ``False``.
    :param ignore_downstream_trigger_rules: If ``True``, a rejected review skips
        every downstream task rather than only the direct ones, so a task whose
        trigger rule would still run it is skipped too. Only takes effect with
        ``require_approval=True``. Default ``False``.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``, ``tools``).

    ``usage_limits`` is inherited from
    :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`.

    Human-in-the-Loop approval parameters are inherited from
    :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`
    (``require_approval``, ``approval_timeout``, ``on_approval_timeout``,
    ``allow_modifications``, ``approval_notifiers``, ``approval_assigned_users``).
    The task pauses after the LLM chooses the branch(es) and only skips the
    unselected downstream tasks once a reviewer approves. Rejecting the
    review skips the direct downstream tasks except teardowns, matching
    :class:`~airflow.providers.standard.operators.hitl.ApprovalOperator`;
    set ``fail_on_reject=True`` to fail the task instead, or
    ``ignore_downstream_trigger_rules=True`` to skip every downstream task
    rather than only the direct ones. The review form
    lists the valid downstream task IDs; with ``allow_modifications=True``
    the editable choice is rendered as a dropdown of those IDs (single-branch
    mode) or a multi-select of them (``allow_multiple_branches=True``), and
    the reviewed branch(es) are validated against the downstream task IDs
    before branching.
    """

    inherits_from_skipmixin = True

    template_fields: Sequence[str] = (*LLMOperator.template_fields, "branch_descriptions")

    def __init__(
        self,
        *,
        branch_descriptions: Mapping[str, str] | None = None,
        allow_multiple_branches: bool = False,
        fail_on_reject: bool = False,
        ignore_downstream_trigger_rules: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("output_type", None)
        super().__init__(**kwargs)
        self.branch_descriptions = branch_descriptions
        self.allow_multiple_branches = allow_multiple_branches
        self.fail_on_reject = fail_on_reject
        self.ignore_downstream_trigger_rules = ignore_downstream_trigger_rules

    def execute(self, context: Context) -> str | Iterable[str] | None:
        if self.require_approval:
            self.validate_approval_prompt()  # type: ignore[misc]

        if not self.downstream_task_ids:
            raise ValueError(
                f"{self.task_id!r} has no downstream tasks. "
                "LLMBranchOperator requires at least one downstream task to branch into."
            )

        downstream_tasks_enum = _downstream_tasks_enum(
            self.task_id, self.downstream_task_ids, self.branch_descriptions
        )
        output_type: Any = (
            list[downstream_tasks_enum] if self.allow_multiple_branches else downstream_tasks_enum  # type: ignore[valid-type]
        )
        if self.branch_descriptions:
            undescribed = sorted(set(self.downstream_task_ids) - set(self.branch_descriptions))
            if undescribed:
                self.log.debug("Branches presented by task ID alone (no description): %s", undescribed)

        # Coerced first so a bad rendered value fails before the expensive setup below.
        usage_limits = coerce_usage_limits(self.usage_limits)

        agent = self.llm_hook.create_agent(
            output_type=output_type,
            instructions=self.system_prompt,
            **self.agent_params,
        )
        result = agent.run_sync(self.prompt, usage_limits=usage_limits)
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
            self.log.info("Rejected by %s. Skipping downstream tasks...", self._describe_responder(event))
            task = context["task"]
            tasks = (
                task.get_flat_relatives(upstream=False)
                if self.ignore_downstream_trigger_rules
                else task.get_direct_relatives(upstream=False)
            )
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
