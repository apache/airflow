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
"""Operator for general-purpose LLM calls."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import BaseModel

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin
from airflow.providers.common.ai.policies.decision import DecisionPolicy
from airflow.providers.common.ai.utils.decision import (
    DECISION_XCOM_KEY,
    ModelConfidence,
    ReviewReason,
    check_uncertain_action,
    decision_record,
    describe_confidence,
    finalize_record,
    initial_decided_by,
    policy_record,
    review_reason,
    timed_out_record,
    validate_decision_policy,
)
from airflow.providers.common.ai.utils.logging import log_run_summary
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output
from airflow.providers.common.ai.utils.usage import coerce_usage_limits
from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, BaseOperator
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.standard.exceptions import HITLRejectException, HITLTimeoutError

try:
    # New enough cores register an operator's declared ``output_type`` classes for
    # XCom deserialization from a worker-side walk over the loaded DAG. On those
    # cores the model instance flows through XCom unchanged. Older cores lack that
    # walk, so the operator dumps to a dict instead (still deserializable anywhere).
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:  # pragma: no cover - cores before the worker-side registration walk
    _CORE_WALKER = False

if TYPE_CHECKING:
    from pydantic_ai import Agent
    from pydantic_ai.usage import UsageLimits

    from airflow.sdk import Context
    from airflow.sdk.execution_time.hitl import HITLUser


__all__ = ["DecisionPolicy", "LLMOperator"]


class LLMOperator(BaseOperator, LLMApprovalMixin):
    """
    Call an LLM with a prompt and return the output.

    Uses a :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
    for LLM access. Supports plain string output (default) and structured output
    via a Pydantic ``BaseModel``. When ``output_type`` is a ``BaseModel`` subclass,
    the model instance is returned to XCom unchanged so downstream tasks can
    type-hint it directly (e.g. ``def downstream(result: MyModel) -> None``).
    The class is auto-registered for deserialization in each process that parses
    the DAG, so no edit to ``[core] allowed_deserialization_classes`` is required.
    The Pydantic class must be defined at module scope: classes nested inside
    a function or ``@dag``-decorated body cannot be deserialized from XCom.

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
    :param output_type: Expected output type. Default ``str``. Set to a Pydantic
        ``BaseModel`` subclass for structured output; the model instance is
        returned to XCom unchanged so downstream tasks can type-hint it
        directly. The class must be defined at module scope -- nested classes
        cannot be deserialized from XCom.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``, ``tools``).
        See `pydantic-ai Agent docs <https://ai.pydantic.dev/api/agent/>`__
        for the full list.
    :param usage_limits: Optional pydantic-ai
        :class:`~pydantic_ai.usage.UsageLimits` enforced on the run, or a dict
        of the same fields (e.g.
        ``{"cost_limit": "{{ params.budget }}", "request_limit": 5}``). The dict
        form is templated: each value is rendered by Jinja like any other
        ``template_fields`` entry, then coerced to that field's type (``Decimal``,
        ``int``, or ``bool``). A value that cannot be coerced -- a Variable
        that exists but is empty renders to ``""``, a typo renders to a
        non-numeric string -- fails the task with a ``ValueError`` naming the
        field and the rendered value, instead of silently disabling the
        limit. A ``UsageLimits`` instance passed directly is used as-is and
        is not templated or validated. ``None`` (default) means no
        enforcement.

        A dict that omits ``request_limit`` still gets pydantic-ai's default of
        ``50`` requests -- pass ``"request_limit": None`` explicitly for no
        request cap. This matches building a ``UsageLimits`` directly, but it is
        easy to miss when moving from ``usage_limits=None`` to a dict that only
        sets ``cost_limit``. See :ref:`howto/operator:llm` for the full set of
        caveats.
    :param require_approval: If ``True``, the task defers after generating
        output and waits for a human reviewer to approve or reject via the
        HITL interface.  Default ``False``. Needs Airflow 3.1+.
    :param approval_timeout: Maximum time to wait for a review.  When
        exceeded, ``on_approval_timeout`` decides the outcome.
    :param on_approval_timeout: What to do when ``approval_timeout`` expires
        without a review.  ``"fail"`` (default) fails the task with
        ``HITLTimeoutError``; ``"approve"`` and ``"reject"`` answer the review
        with that option, so the task resumes as if a reviewer had chosen it.
        The chosen option is also pre-highlighted for the reviewer in the HITL
        form.  Requires a review path (``require_approval=True`` or a
        ``decision_policy`` that reviews) and a positive
        ``approval_timeout``.
    :param allow_modifications: If ``True``, the reviewer can edit the output
        before approving.  The modified value is returned as the task result.
        Default ``False``.
    :param approval_notifiers: Notifiers called once the review is open, so a
        reviewer is told about it.  Only takes effect when a review is
        opened.  A retry re-notifies with the regenerated
        output while the open review keeps the original subject and body.
        Default ``None``.
    :param approval_assigned_users: Users allowed to answer the review, as
        ``{"id": ..., "name": ...}`` dicts where ``id`` is the auth manager's
        user id.  ``None`` (default) lets any user with the permission respond.
        The list is fixed when the review is first created.  Needs Airflow 3.1+.
    :param decision_policy: A :class:`~airflow.providers.common.ai.utils.decision.DecisionPolicy`
        saying how confident the model has to be for the operator to return its answer by
        itself (``min_confidence``) and what happens otherwise (``on_uncertain``: ``"review"``
        or ``"fail"``). Confidence comes from models that report one per output field, such as
        a classifier model (TypeSafe's), in ``provider_details``. A structured output is judged
        by its least confident field among the fields that reported one; a field whose type
        reports none (a bounded float, where the probability is the answer) is not gated, and
        the record's ``confidence`` map shows which fields were compared. When no field reports
        any confidence, as with a text model, the output counts as uncertain, so swapping the
        connection does not silently switch off a control the author set. Independent of
        ``require_approval``, which always asks. ``on_uncertain="review"`` needs Airflow 3.1+,
        like ``require_approval``. Default ``None``: no gate, behaviour unchanged.
    :param serialize_output: If ``True`` and ``output_type`` is a Pydantic
        ``BaseModel`` subclass, the model instance is dumped to a ``dict`` via
        ``model_dump()`` before being pushed to XCom. Default ``False`` --
        the Pydantic instance flows through XCom unchanged. Set to ``True``
        when a downstream consumer needs the dict shape (e.g. sending to an
        external system that expects JSON-style payloads).
    """

    deserialization_allowed_class_fields: ClassVar[tuple[str, ...]] = ("output_type",)

    # Subclasses that run their own ``execute`` without the confidence gate set this to False so a
    # ``decision_policy`` is rejected at construction rather than accepted and ignored.
    supports_decision_policy: ClassVar[bool] = True

    template_fields: Sequence[str] = (
        "prompt",
        "llm_conn_id",
        "model_id",
        "fallback_conn_ids",
        "system_prompt",
        "agent_params",
        "usage_limits",
    )

    def __init__(
        self,
        *,
        prompt: str,
        llm_conn_id: str,
        model_id: str | None = None,
        fallback_conn_ids: list[str] | None = None,
        system_prompt: str = "",
        output_type: type = str,
        agent_params: dict[str, Any] | None = None,
        usage_limits: UsageLimits | dict[str, Any] | None = None,
        require_approval: bool = False,
        approval_timeout: timedelta | None = None,
        on_approval_timeout: Literal["fail", "approve", "reject"] = "fail",
        allow_modifications: bool = False,
        approval_notifiers: BaseNotifier | Iterable[BaseNotifier] | None = None,
        approval_assigned_users: HITLUser | Iterable[HITLUser] | None = None,
        decision_policy: DecisionPolicy | None = None,
        serialize_output: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.decision_policy = validate_decision_policy(decision_policy)
        if self.decision_policy.gates and not self.supports_decision_policy:
            raise ValueError(
                f"{type(self).__name__} does not support decision_policy yet; it runs its own execute() "
                "without the confidence gate. Use require_approval=True for an unconditional review."
            )
        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.fallback_conn_ids = fallback_conn_ids
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.serialize_output = serialize_output
        # Return the Pydantic instance when the core can register ``output_type``
        # for deserialization (its worker-side DAG walk); otherwise, or when the
        # user opts in, dump to a dict so the value is deserializable anywhere.
        self._serialize_model_output = serialize_output or not _CORE_WALKER
        self.agent_params = agent_params or {}
        # No validation here -- see coerce_usage_limits() docstring for why.
        self.usage_limits = usage_limits
        if on_approval_timeout not in ("fail", *LLMApprovalMixin.TIMEOUT_DEFAULTS):
            raise ValueError(
                f"on_approval_timeout must be 'fail', 'approve', or 'reject', got {on_approval_timeout!r}."
            )
        # Checked before the combination rule so an old core reports the core version
        # rather than sending the user to drop an argument that was never the problem.
        if require_approval and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException("require_approval=True needs Airflow 3.1+.")
        if self.decision_policy.reviews and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException(
                "DecisionPolicy(on_uncertain='review') needs Airflow 3.1+; use on_uncertain='fail' on this core."
            )

        # A review can open either way; both settings make the approval flow reachable.
        self._may_review = require_approval or self.decision_policy.reviews
        if on_approval_timeout != "fail" and not (
            self._may_review and approval_timeout is not None and approval_timeout > timedelta(0)
        ):
            raise ValueError(
                f"on_approval_timeout={on_approval_timeout!r} needs a review path (require_approval=True or "
                "a decision_policy with on_uncertain='review') and a positive approval_timeout to fire. "
                "Set both, or leave on_approval_timeout as 'fail'."
            )
        self.require_approval = require_approval
        self.approval_timeout = approval_timeout
        self.on_approval_timeout = on_approval_timeout
        self.allow_modifications = allow_modifications
        if approval_notifiers is None:
            approval_notifiers = []
        elif isinstance(approval_notifiers, BaseNotifier):
            approval_notifiers = [approval_notifiers]
        elif isinstance(approval_notifiers, str) or not isinstance(approval_notifiers, Iterable):
            raise TypeError(
                "approval_notifiers must be a BaseNotifier or an iterable of BaseNotifier instances, "
                f"got {approval_notifiers!r}"
            )
        self.approval_notifiers = list(approval_notifiers)
        for notifier in self.approval_notifiers:
            if not isinstance(notifier, BaseNotifier):
                raise TypeError(f"approval_notifiers must contain BaseNotifier instances, got {notifier!r}")
        assigned_users: list[Any]
        if approval_assigned_users is None:
            assigned_users = []
        elif isinstance(approval_assigned_users, dict):
            assigned_users = [approval_assigned_users]
        elif isinstance(approval_assigned_users, str) or not isinstance(approval_assigned_users, Iterable):
            raise TypeError(
                "approval_assigned_users must be a {'id': str, 'name': str} dict or an iterable of them, "
                f"got {approval_assigned_users!r}"
            )
        else:
            assigned_users = list(approval_assigned_users)
        for user in assigned_users:
            if (
                not isinstance(user, dict)
                or not isinstance(user.get("id"), str)
                or not isinstance(user.get("name"), str)
            ):
                raise TypeError(
                    f"approval_assigned_users entries must be {{'id': str, 'name': str}} dicts, got {user!r}"
                )
        if assigned_users and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException("approval_assigned_users needs Airflow 3.1+.")
        self.approval_assigned_users: list[HITLUser] = assigned_users

    @cached_property
    def llm_hook(self) -> PydanticAIHook:
        """
        Return the correct PydanticAIHook subclass for the configured connection.

        Delegates to :meth:`~PydanticAIHook.get_hook` which looks up
        the connection's ``conn_type`` and instantiates the matching subclass
        (e.g. :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIAzureHook`
        for ``pydanticai_azure`` connections).
        """
        hook_params = {
            "model_id": self.model_id,
            "fallback_conn_ids": self.fallback_conn_ids,
        }
        return PydanticAIHook.get_hook(self.llm_conn_id, hook_params=hook_params)

    def execute(self, context: Context) -> Any:
        if self._may_review:
            self.validate_approval_prompt()  # type: ignore[misc]

        # Coerced first so a bad rendered value fails before the expensive setup below.
        usage_limits = coerce_usage_limits(self.usage_limits)

        agent: Agent[object, Any] = self.llm_hook.create_agent(
            output_type=self.output_type, instructions=self.system_prompt, **self.agent_params
        )
        result = agent.run_sync(self.prompt, usage_limits=usage_limits)
        log_run_summary(self.log, result)
        output = result.output

        model_confidence = ModelConfidence.from_result(result)
        # Gated by the least confident of the fields that reported a confidence; a field whose type
        # reports none is not gated. A bare output type is the one field ``response``.
        fields = list(model_confidence.confidence) or ["response"]
        policy = self.decision_policy
        threshold = policy.min_confidence
        confidence = model_confidence.lowest(fields)
        review = review_reason(
            require_approval=self.require_approval, threshold=threshold, confidence=confidence
        )
        record = decision_record(
            model_confidence=model_confidence,
            proposed=None,
            action=None,
            threshold=threshold,
            review=review,
            decided_by=initial_decided_by(review, policy.on_uncertain),
            policy=policy_record(policy),
        )
        self._push_decision(context, record)
        check_uncertain_action(
            review,
            policy.on_uncertain,
            what=f"task {self.task_id!r}",
            confidence=confidence,
            threshold=threshold,
        )

        if review:
            self._log_review(review, confidence, threshold)
            body = None
            if review != "require_approval":
                body = f"```\nPrompt: {self.prompt}\n\n{output}\n```\n\n" + "\n\n".join(
                    describe_confidence(model_confidence, name, threshold) for name in fields
                )
            self.defer_for_approval(context, output, body=body, decision=record)  # type: ignore[misc]

        if self._serialize_model_output and isinstance(output, BaseModel):
            # ``serialize_output=True``, or a core without the worker-side
            # deserialization-class walk: dump to a dict so XCom carries a plain
            # JSON payload that deserializes without an allow-list entry.
            output = output.model_dump()

        return output

    def execute_complete(
        self,
        context: Context,
        generated_output: str,
        event: dict[str, Any],
        decision: dict[str, Any] | None = None,
    ) -> Any:
        """Resume after human review and restore the Pydantic model for XCom consumers."""
        output = self._resume_after_review(context, generated_output, event, decision)
        self._finalize_decision(context, event, decision, action=None)
        return rehydrate_pydantic_output(
            self.output_type, output, serialize_output=self._serialize_model_output
        )

    def _log_review(self, review: ReviewReason, confidence: float | None, threshold: float | None) -> None:
        if review == "below_threshold":
            self.log.info(
                "Sending the output to review: confidence %.2f is below min_confidence=%.2f.",
                confidence,
                threshold,
            )
        elif review == "missing_confidence":
            self.log.info(
                "Sending the output to review: min_confidence=%.2f is set but the model reported no "
                "confidence for its answer.",
                threshold,
            )

    def _resume_after_review(
        self, context: Context, generated_output: str, event: dict[str, Any], decision: dict[str, Any] | None
    ) -> str:
        """
        Run the mixin's resume and, when it raises, finalize the record before the exception leaves.

        A rejection and a timeout with no default both end in an exception from the mixin; without
        this the ``decision`` XCom would stay pending, indistinguishable from a review still open.
        """
        try:
            return LLMApprovalMixin.execute_complete(self, context, generated_output, event, decision)
        except HITLRejectException:
            self._finalize_decision(context, event, decision, action=None)
            raise
        except HITLTimeoutError:
            if decision is not None:
                self._push_decision(context, timed_out_record(decision))
            raise

    def _push_decision(self, context: Context, record: dict[str, Any]) -> None:
        """Expose what the model proposed, its confidence, and what the gate decided, on XCom."""
        if not self.do_xcom_push:
            return
        try:
            ti = context["task_instance"]
        except (KeyError, TypeError):
            ti = None
        push = getattr(ti, "xcom_push", None)
        if not callable(push):
            # A hand-built context (a dict, or no task instance at all) has nowhere to push to; the
            # record is inspection output, so the run goes on without it.
            self.log.warning("No task instance in the context; the decision record was not pushed to XCom.")
            return
        push(key=DECISION_XCOM_KEY, value=record)

    def _finalize_decision(
        self, context: Context, event: dict[str, Any], decision: dict[str, Any] | None, *, action: Any
    ) -> None:
        """
        After a review, overwrite the pending ``decision`` XCom with who decided and what was done.

        ``decision`` is the record carried in the continuation since the pause, so the final record
        does not depend on the pending XCom still being there or unchanged.
        """
        if decision is None:
            return
        self._push_decision(context, finalize_record(decision, event, action=action))
