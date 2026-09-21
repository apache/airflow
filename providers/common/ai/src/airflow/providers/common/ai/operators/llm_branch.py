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

try:
    from pydantic_ai import Choice, Choices  # type: ignore[attr-defined]
except ImportError:  # pydantic-ai < 2.46.0: build the same schema from an Enum instead
    Choice = Choices = None  # type: ignore[assignment,misc]

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.policies.decision import BranchOption, DecisionPolicy
from airflow.providers.common.ai.utils.decision import (
    ModelConfidence,
    check_uncertain_action,
    decision_record,
    describe_confidence,
    initial_decided_by,
    policy_record,
    review_reason,
    threshold_for,
)
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.common.ai.utils.usage import coerce_usage_limits
from airflow.providers.standard.exceptions import HITLRejectException
from airflow.providers.standard.operators.branch import BranchMixIn

if TYPE_CHECKING:
    from airflow.sdk import Context

__all__ = ["BranchOption", "DecisionPolicy", "LLMBranchOperator"]


def _branch_choices(
    task_id: str,
    downstream_task_ids: Iterable[str],
    descriptions: Mapping[str, str],
    configured: Iterable[str],
) -> type:
    """
    Build the type the model picks a branch from: one option per downstream task ID.

    Sorted so every worker sends the model the same option order: ``downstream_task_ids``
    is a set, and set order follows string hashing, which differs between processes.

    With a description on any branch the options render as ``anyOf`` of ``{const,
    description}`` instead of a bare ``enum`` list. That is the one JSON Schema shape that
    carries a description per value, and it is what both a text model's tool schema and
    pydantic-ai's TypeSafe adapter read an option's meaning from. On pydantic-ai 2.46+ the
    type is its ``Choices``; before that, an ``Enum`` whose schema hook emits the same shape.
    Either way the model has to answer with one of the task IDs.
    """
    task_ids = sorted(downstream_task_ids)
    unknown = sorted(set(configured) - set(task_ids))
    if unknown:
        raise ValueError(
            f"branches for {task_id!r} names {unknown}, which are not downstream tasks. "
            f"Downstream tasks: {task_ids}."
        )
    if Choices is not None:
        if not descriptions:
            return Choices(task_ids, name="DownstreamTasks")
        return Choices({name: Choice(descriptions.get(name)) for name in task_ids}, name="DownstreamTasks")

    enum_cls: type[Enum] = Enum("DownstreamTasks", {name: name for name in task_ids})  # type: ignore[misc]
    if not descriptions:
        return enum_cls

    def json_schema(cls: type[Enum], core_schema: Any, handler: Any) -> dict[str, Any]:
        options: list[dict[str, Any]] = []
        for member in cls:
            option: dict[str, Any] = {"const": member.value, "type": "string"}
            if text := descriptions.get(member.value):
                option["description"] = text
            options.append(option)
        return {"anyOf": options, "title": cls.__name__}

    # pydantic looks this hook up on the type when it builds the schema, so attaching it to the
    # functional-API enum is the same as defining it in a class body.
    setattr(enum_cls, "__get_pydantic_json_schema__", classmethod(json_schema))
    return enum_cls


def _picked(value: Any) -> str:
    """Return the task ID a picked option stands for: ``Choices`` answers with the key, the Enum fallback with a member."""
    return value.value if isinstance(value, Enum) else str(value)


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
    :param branches: Optional mapping of downstream task ID to a :class:`BranchOption`, or
        to a string as shorthand for its description. The description travels in the
        output schema next to the option, so the model reads "here is an option, here is
        what it means" rather than guessing from the task ID; ``min_confidence`` on an
        option is a bar for that branch alone. A downstream task without an entry is
        presented by its ID alone and takes the policy's bar. A key that is not a
        downstream task ID fails the task before the model is called. Descriptions
        support Jinja templating.
    :param allow_multiple_branches: When ``False`` (default) the LLM returns a
        single task ID. When ``True`` the LLM may return one or more task IDs.
    :param decision_policy: A :class:`~airflow.providers.common.ai.utils.decision.DecisionPolicy`:
        the confidence a pick needs for the operator to branch on it without a person
        (``min_confidence``) and what happens under it (``on_uncertain``: ``"review"`` or
        ``"fail"``). Confidence comes from models that report one, such as a classifier
        model (TypeSafe's); a text model reports none, which counts as uncertain, so
        swapping the connection does not silently switch off a control the author set.
        A branch's own ``min_confidence`` overrides the policy's for that pick; with
        ``allow_multiple_branches`` the strictest bar among the picked branches applies.
        ``require_approval=True`` still sends every pick to a person regardless.
        Default ``None``: no gate.
    :param fail_on_reject: If ``True``, a rejected review fails the task
        instead of skipping the downstream tasks. Generally discouraged,
        as for :class:`~airflow.providers.standard.operators.hitl.ApprovalOperator`.
        Only takes effect when a review is opened. Default ``False``.
    :param ignore_downstream_trigger_rules: If ``True``, a rejected review skips
        every downstream task rather than only the direct ones, so a task whose
        trigger rule would still run it is skipped too. Only takes effect when a
        review is opened. Default ``False``.
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

    template_fields: Sequence[str] = (*LLMOperator.template_fields, "branches")

    def __init__(
        self,
        *,
        branches: Mapping[str, BranchOption | str] | None = None,
        allow_multiple_branches: bool = False,
        fail_on_reject: bool = False,
        ignore_downstream_trigger_rules: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("output_type", None)
        super().__init__(**kwargs)
        self.branches = self._normalize_branches(branches)
        self.allow_multiple_branches = allow_multiple_branches
        self.fail_on_reject = fail_on_reject
        self.ignore_downstream_trigger_rules = ignore_downstream_trigger_rules

    def _normalize_branches(
        self, branches: Mapping[str, BranchOption | str] | None
    ) -> dict[str, BranchOption] | None:
        if not branches:
            return None
        if not isinstance(branches, Mapping):
            raise TypeError(
                f"branches must be a mapping of task ID to BranchOption or description, got "
                f"{type(branches).__name__}. Template the descriptions inside the mapping, not the mapping itself."
            )
        options = {
            str(name): option if isinstance(option, BranchOption) else BranchOption(description=option)
            for name, option in branches.items()
        }
        raised = sorted(name for name, option in options.items() if option.min_confidence is not None)
        if raised and self.decision_policy.min_confidence is None:
            raise ValueError(
                f"branches {raised} set min_confidence but decision_policy has none to inherit for the "
                "other branches. Set DecisionPolicy(min_confidence=...) to the bar the rest take."
            )
        return options

    @property
    def _branch_bars(self) -> dict[str, float]:
        return {
            name: option.min_confidence
            for name, option in (self.branches or {}).items()
            if option.min_confidence is not None
        }

    @property
    def _descriptions(self) -> dict[str, str]:
        return {
            name: option.description for name, option in (self.branches or {}).items() if option.description
        }

    def execute(self, context: Context) -> str | Iterable[str] | None:
        if self._may_review:
            self.validate_approval_prompt()  # type: ignore[misc]

        if not self.downstream_task_ids:
            raise ValueError(
                f"{self.task_id!r} has no downstream tasks. "
                "LLMBranchOperator requires at least one downstream task to branch into."
            )

        descriptions = self._descriptions
        choice_type = _branch_choices(
            self.task_id, self.downstream_task_ids, descriptions, configured=self.branches or ()
        )
        output_type: Any = list[choice_type] if self.allow_multiple_branches else choice_type  # type: ignore[valid-type]
        if self.branches:
            undescribed = sorted(set(self.downstream_task_ids) - set(descriptions))
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

        # The output type validated the pick, so it is a task ID (or IDs) in either encoding.
        branches: str | list[str] = (
            [_picked(item) for item in output] if isinstance(output, list) else _picked(output)
        )

        if not branches:
            raise ValueError(
                f"LLM selected no branches for {self.task_id!r}, which would skip every downstream task."
            )

        # The pick is one field, ``response``; its confidence is what the bar is compared against.
        model_confidence = ModelConfidence.from_result(result)
        picked = [branches] if isinstance(branches, str) else branches
        policy = self.decision_policy
        threshold = threshold_for(policy.min_confidence, self._branch_bars, picked)
        confidence = model_confidence.confidence.get("response")
        review = review_reason(
            require_approval=self.require_approval, threshold=threshold, confidence=confidence
        )
        record = {
            **decision_record(
                model_confidence=model_confidence,
                proposed=branches,
                action=None if review else branches,
                threshold=threshold,
                review=review,
                decided_by=initial_decided_by(review, policy.on_uncertain),
                policy=policy_record(policy, self._branch_bars),
            ),
            # On every record, not only reviewed ones: the record is read after the Dag file may have
            # changed, and a few hundred bytes per run is the price of it explaining itself.
            "descriptions": descriptions or None,
        }
        self._push_decision(context, record)
        check_uncertain_action(
            review,
            policy.on_uncertain,
            what=f"branch task {self.task_id!r}",
            confidence=confidence,
            threshold=threshold,
        )

        if review:
            self._log_review(review, confidence, threshold)
            choices = sorted(self.downstream_task_ids)
            chosen = branches if isinstance(branches, str) else json.dumps(branches)
            body = (
                f"Valid branches: {', '.join(f'`{c}`' for c in choices)}\n\n"
                f"```\nPrompt: {self.prompt}\n\nChosen branch(es): {chosen}\n```"
            )
            if review != "require_approval" or model_confidence.confidence:
                body += "\n\n" + describe_confidence(model_confidence, "response", threshold)
            modification_schema = (
                {"type": "array", "items": {"type": "string", "enum": choices}, "examples": choices}
                if self.allow_multiple_branches
                else {"type": "string", "enum": choices}
            )
            self.defer_for_approval(  # type: ignore[misc]
                context, branches, body=body, modification_schema=modification_schema, decision=record
            )

        return self.do_branch(context, branches)

    def execute_complete(
        self,
        context: Context,
        generated_output: str,
        event: dict[str, Any],
        decision: dict[str, Any] | None = None,
    ) -> Any:
        """Resume after human review, validating the reviewed choice before branching."""
        try:
            # Not LLMOperator.execute_complete: the branch finalizes the decision record itself, with
            # the branches that ran, and there is no Pydantic output to rehydrate. The helper still
            # finalizes a rejection or timeout before the exception leaves.
            output = self._resume_after_review(context, generated_output, event, decision)
        except HITLRejectException:
            if self.fail_on_reject:
                raise
            self.log.info("Rejected by %s. Skipping downstream tasks...", self._describe_responder(event))
            # The record was finalized before the exception; skip() hands the skip to the supervisor
            # by raising, so nothing after it runs.
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
        self._finalize_decision(context, event, decision, action=branches)
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
